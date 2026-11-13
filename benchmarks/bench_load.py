"""Finite, credential-free synthetic runtime capacity experiments.

Example (run the same file in each source checkout)::

    python benchmarks/bench_load.py --sessions 1 2 4 --journals shared separate \
        --mode all --jobs 12 --rate 500 --seconds 2 --provider-ms 20 \
        --work-ms 5 --repeats 1 --seed 7 --json results.json

Worker jobs use real PythonHost processes, FULL durable commits and exact marker
verification. Admission uses aggregate open-loop arrivals with ordinary batching.
Provider sleep is synthetic async delay, not inference. Defaults for capacity,
context, compaction and activation limits are unchanged. Warm-up is excluded from
active timing; shutdown and offline verification have separate timing boundaries.
All accepted inputs are drained or reported unfinished; rejections are not retried.
JSON contains configuration, independent trials, per-session jobs/events/row counts,
aggregate rates and latency distributions (milliseconds, nearest-rank percentiles).
Only offline-verified completions contribute goodput. Instrumentation retains event
and scripted request history, so these finite experiments are not production SLOs.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
import platform
from pathlib import Path
import random
import sqlite3
import sys
import tempfile
import time
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from nervipulsa.config import Settings
from nervipulsa.events import Event
from nervipulsa.ports import ModelRequest
from nervipulsa.providers import ScriptedBackend, text_response, tool_response
from nervipulsa.runtime import Runtime
import nervipulsa.runtime as runtime_module


def distribution(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    return {
        "samples": len(ordered),
        **{
            f"p{percentile}": (
                ordered[max(0, math.ceil(len(ordered) * percentile / 100) - 1)]
                if ordered else None
            )
            for percentile in (50, 95, 99)
        },
        "max": ordered[-1] if ordered else None,
        "mean": sum(ordered) / len(ordered) if ordered else None,
    }


@dataclass
class Job:
    marker: str
    warmup: bool = False
    scheduled_at: float | None = None
    attempted_at: float | None = None
    accepted_at: float | None = None
    input_id: str | None = None
    rejection: str | None = None
    activation_id: str | None = None
    activation_status: str | None = None
    tool_call_id: str | None = None
    execution_id: str | None = None
    execution_accepted_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    terminal_id: str | None = None
    terminal_status: str | None = None
    followup_activation_id: str | None = None
    settled_at: float | None = None
    error: str | None = None
    verified: bool = False


class Session:
    def __init__(self, root: Path, journal: Path, session_id: str, mode: str,
                 provider_ms: float, work_ms: float) -> None:
        self.mode = mode
        self.provider_ms = provider_ms
        self.work_ms = work_ms
        self.workspace = root
        self.marker_file = root / "job-markers.txt"
        self.jobs: list[Job] = []
        self.events: list[dict[str, Any]] = []
        self.changed = asyncio.Event()
        self.current: Job | None = None
        self.codes: dict[str, Job] = {}
        self.executions: dict[str, Job] = {}
        self.inputs: dict[str, Job] = {}
        self.settlements: dict[str, dict[str, Any]] = {}
        self.request_purposes: Counter[str] = Counter()
        self.context_samples: list[int] = []
        self.errors: list[str] = []
        self.backend = ScriptedBackend(self.respond)
        settings = Settings(model="scripted", workspace=str(root))
        self.runtime = Runtime(settings, self.backend, root, journal_path=journal,
                               session_id=session_id, python_host=mode == "worker",
                               echo=lambda _text: None)
        self.runtime.add_listener(self.observe)
        self.initial_context_chars = self.runtime.status()["transcript_chars"]
        self.active_start_context_chars: int | None = None

    async def respond(self, request: ModelRequest):
        self.request_purposes[request.purpose] += 1
        if self.provider_ms:
            await asyncio.sleep(self.provider_ms / 1000)
        if request.purpose != "activation":
            return text_response("Synthetic benchmark summary; prior jobs were observed.")
        job = self.current
        if self.mode == "worker" and job is not None and job.tool_call_id is None:
            job.tool_call_id = f"call-{job.marker}"
            code = (
                "import time\n"
                f"time.sleep({self.work_ms / 1000!r})\n"
                f"with open({str(self.marker_file)!r}, 'a', encoding='utf-8') as marker_file:\n"
                f"    marker_file.write({job.marker + chr(10)!r})\n"
                "    marker_file.flush()\n"
            )
            self.codes[code] = job
            return tool_response((job.tool_call_id, code, None))
        return text_response("Synthetic benchmark input observed.")

    def observe(self, event: Event) -> None:
        # Listener exceptions are swallowed by Runtime; preserve any explicitly.
        try:
            record = {"id": event.id, "type": event.type, "reply_to": event.reply_to,
                      "accepted_at": event.accepted_at, "payload": dict(event.payload)}
            self.events.append(record)
            if event.type == "python.requested":
                job = self.codes.get(str(event.payload.get("code")))
                if job is not None:
                    if job.execution_id is not None:
                        self.errors.append(f"duplicate execution for {job.marker}")
                    job.execution_id = event.id
                    job.execution_accepted_at = event.accepted_at
                    self.executions[event.id] = job
            elif event.type in {"python.started", "python.finished"}:
                job = self.executions.get(event.reply_to or "")
                if job is not None:
                    if event.type == "python.started":
                        job.started_at = event.accepted_at
                    else:
                        job.finished_at = event.accepted_at
                        job.terminal_id = event.id
                        job.terminal_status = str(event.payload.get("status"))
            elif event.type == "agent.activation_finished":
                payload = event.payload
                for event_id in payload.get("input_event_ids", []):
                    self.settlements[event_id] = record
                    job = self.inputs.get(event_id)
                    if job is not None:
                        job.activation_id = str(payload["activation_id"])
                        job.activation_status = str(payload["status"])
                        if self.mode == "admission":
                            job.settled_at = event.accepted_at
                self.context_samples.append(self.runtime.status()["transcript_chars"])
            self.changed.set()
        except Exception as exc:
            self.errors.append(f"listener: {type(exc).__name__}: {exc}")
            self.changed.set()

    async def until(self, predicate: Callable[[], bool], timeout: float) -> None:
        async def wait() -> None:
            while True:
                self.changed.clear()
                if predicate():
                    return
                await self.changed.wait()
        await asyncio.wait_for(wait(), timeout)

    def submit(self, marker: str, *, warmup: bool = False,
               scheduled_at: float | None = None) -> Job:
        job = Job(marker, warmup=warmup, scheduled_at=scheduled_at)
        self.jobs.append(job)
        self.current = job
        job.attempted_at = asyncio.get_running_loop().time()
        delivery = self.runtime.submit_text(f"bench_load job {marker}")
        if delivery.accepted and delivery.event_id is not None:
            job.input_id = delivery.event_id
            job.accepted_at = next(event["accepted_at"] for event in reversed(self.events)
                                   if event["id"] == job.input_id)
            self.inputs[job.input_id] = job
        else:
            job.rejection = delivery.reason or "accepted_without_event_id"
            if delivery.accepted:
                self.errors.append("accepted input has no correlation ID")
        return job

    async def worker_job(self, marker: str, timeout: float, *, warmup: bool = False) -> None:
        job = self.submit(marker, warmup=warmup)
        if job.input_id is None:
            return
        try:
            activation = await self.runtime.wait_for_activation(
                input_event_id=job.input_id, timeout=timeout)
            if activation is None:
                raise TimeoutError("input activation did not settle")
            job.activation_id = activation.id
            job.activation_status = activation.status
            if activation.status != "committed":
                raise RuntimeError(f"input activation {activation.status}: {activation.error_kind}")
            await self.until(lambda: job.terminal_id is not None, timeout)
            followup = await self.runtime.wait_for_activation(
                input_event_id=job.terminal_id, timeout=timeout)
            if followup is None:
                raise TimeoutError("terminal follow-up did not settle")
            job.followup_activation_id = followup.id
            if followup.status != "committed":
                raise RuntimeError(f"follow-up {followup.status}: {followup.error_kind}")
            job.settled_at = asyncio.get_running_loop().time()
            if job.terminal_status != "succeeded":
                raise RuntimeError(f"worker terminal {job.terminal_status}")
        except Exception as exc:
            job.error = f"{type(exc).__name__}: {exc}"

    async def drain_admission(self, timeout: float) -> None:
        try:
            await self.until(lambda: all(job.input_id in self.settlements
                                        for job in self.jobs if job.input_id), timeout)
        except TimeoutError:
            self.errors.append("accepted input drain timed out")
        for job in self.jobs:
            if job.input_id and job.activation_status != "committed":
                job.error = f"input activation {job.activation_status or 'unfinished'}"


class LoopLag:
    """Absolute 10 ms timer deadlines; includes the final delayed callback."""
    def __init__(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.interval = 0.01
        self.target = self.loop.time() + self.interval
        self.samples: list[float] = []
        self.handle = self.loop.call_at(self.target, self.tick)

    def tick(self) -> None:
        now = self.loop.time()
        self.samples.append(max(0, now - self.target) * 1000)
        # Skip missed deadlines instead of generating a burst of callbacks.
        self.target += (max(0, int((now - self.target) / self.interval)) + 1) * self.interval
        self.handle = self.loop.call_at(self.target, self.tick)

    def stop(self) -> dict[str, Any]:
        self.handle.cancel()
        now = self.loop.time()
        if now >= self.target:
            self.samples.append((now - self.target) * 1000)
        return distribution(self.samples)


def rows(connection: sqlite3.Connection, table: str, session_id: str) -> list[dict[str, Any]]:
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                              (table,)).fetchone():
        return []
    return [dict(row) for row in connection.execute(
        f"SELECT * FROM {table} WHERE session_id=?", (session_id,))]


def reconcile(session: Session) -> dict[str, Any]:
    """Offline, exact-set inspection after journal shutdown; never replay anything."""
    errors = list(session.errors)
    runtime = session.runtime
    data: dict[str, list[dict[str, Any]]] = {}
    tables = ("events", "activations", "executions", "durable_activation_commits",
              "durable_transcript_snapshots", "durable_tool_receipts", "durable_effects")
    try:
        with sqlite3.connect(runtime.journal_path) as connection:
            connection.row_factory = sqlite3.Row
            data = {table: rows(connection, table, runtime.bus.session_id) for table in tables}
    except Exception as exc:
        errors.append(f"journal inspection: {type(exc).__name__}: {exc}")
        data = {table: [] for table in tables}
    by_type: dict[str, list[dict[str, Any]]] = {}
    for event in session.events:
        by_type.setdefault(event["type"], []).append(event)
    stored_events = {row["id"]: row for row in data["events"]}
    commits = {row["activation_id"]: row for row in data["durable_activation_commits"]}
    snapshots = {row["activation_id"]: row for row in data["durable_transcript_snapshots"]}
    observed_activations = {row["id"]: row for row in data["activations"]}
    effects = {row["execution_id"]: row for row in data["durable_effects"]}
    executions = {row["execution_id"]: row for row in data["executions"]}
    accepted = {job.input_id for job in session.jobs if job.input_id}
    expected_executions = {job.execution_id for job in session.jobs if job.execution_id}

    def exact(label: str, expected: set, actual: set) -> None:
        if expected != actual:
            errors.append(f"{label}: missing={sorted(expected - actual, key=str)} unexpected={sorted(actual - expected, key=str)}")

    exact("listener versus persisted event IDs", {event["id"] for event in session.events},
          set(stored_events))
    exact("accepted user inputs", accepted, {row["id"] for row in by_type.get("user.message", [])})
    exact("durable accepted user inputs", accepted,
          {row["id"] for row in data["events"] if row["type"] == "user.message"})
    exact("requested executions", expected_executions,
          {row["id"] for row in by_type.get("python.requested", [])})
    for event_type in ("python.started", "python.finished"):
        exact(f"{event_type} execution IDs", expected_executions,
              {row["reply_to"] for row in by_type.get(event_type, [])})
    exact("effect rows", expected_executions, set(effects))
    exact("execution rows", expected_executions, set(executions))
    exact("snapshot activation IDs", set(commits), set(snapshots))
    exact("observation activation IDs", set(commits), set(observed_activations))
    settled_ids = {str(row["payload"]["activation_id"])
                   for row in by_type.get("agent.activation_finished", [])}
    exact("settled activation IDs", set(commits), settled_ids)
    for event in by_type.get("agent.activation_finished", []):
        payload = event["payload"]
        commit = commits.get(str(payload["activation_id"]))
        if commit and (payload["status"] != commit["status"] or
                       payload["input_event_ids"] != json.loads(commit["input_event_ids_json"])):
            errors.append(f"settlement differs from durable commit: {payload['activation_id']}")
    input_commit_counts = Counter(event_id for commit in commits.values()
                                  for event_id in json.loads(commit["input_event_ids_json"]))
    for event_id in accepted:
        if input_commit_counts[event_id] != 1:
            errors.append(f"accepted input committed {input_commit_counts[event_id]} times: {event_id}")
    receipts = data["durable_tool_receipts"]
    exact("receipt execution IDs", expected_executions,
          {row["execution_id"] for row in receipts if row["execution_id"]})
    if len(receipts) != len(expected_executions):
        errors.append("receipt count differs from expected execution count")
    markers = session.marker_file.read_text(encoding="utf-8").splitlines() if session.marker_file.exists() else []
    marker_counts = Counter(markers)
    expected_markers = {job.marker for job in session.jobs if job.input_id} if session.mode == "worker" else set()
    exact("worker marker set", expected_markers, set(markers))
    if any(count != 1 for count in marker_counts.values()):
        errors.append("duplicate worker markers")
    for activation_id, commit in commits.items():
        snapshot = snapshots.get(activation_id)
        observation = observed_activations.get(activation_id)
        if snapshot is None or observation is None:
            continue
        if snapshot["transcript_version"] != commit["transcript_version"]:
            errors.append(f"snapshot version mismatch: {activation_id}")
        if observation["status"] != commit["status"]:
            errors.append(f"activation observation status mismatch: {activation_id}")
        if commit["status"] != "committed":
            errors.append(f"durable activation {activation_id}: {commit['status']}")
        if not set(json.loads(commit["input_event_ids_json"])).issubset(
                set(json.loads(snapshot["event_ids_json"]))):
            errors.append(f"snapshot missing activation inputs: {activation_id}")
    for event in session.events:
        stored = stored_events.get(event["id"])
        if stored and (stored["type"] != event["type"] or stored["reply_to"] != event["reply_to"]
                       or json.loads(stored["payload_json"]) != event["payload"]):
            errors.append(f"event payload mismatch: {event['id']}")
    journal = {"dropped": runtime.journal.dropped, "error": runtime.journal.error,
               "incomplete": runtime.journal.incomplete}
    if journal["incomplete"] or journal["dropped"] or journal["error"]:
        errors.append("journal observations incomplete")
    exact_set_consistent = not errors

    def committed(activation_id: str | None, input_id: str | None) -> bool:
        commit = commits.get(activation_id or "")
        return bool(commit and commit["status"] == "committed" and input_id
                    and input_id in json.loads(commit["input_event_ids_json"])
                    and activation_id in snapshots and activation_id in settled_ids)

    for job in session.jobs:
        if not job.input_id:
            continue
        valid = bool(exact_set_consistent and committed(job.activation_id, job.input_id)
                     and job.settled_at is not None and job.error is None)
        if session.mode == "worker":
            effect = effects.get(job.execution_id or "", {})
            execution = executions.get(job.execution_id or "", {})
            matching_receipts = [row for row in receipts if row["execution_id"] == job.execution_id]
            terminal_events = [row for row in by_type.get("python.finished", [])
                               if row["reply_to"] == job.execution_id]
            started_events = [row for row in by_type.get("python.started", [])
                              if row["reply_to"] == job.execution_id]
            valid = bool(valid and marker_counts[job.marker] == 1
                         and job.started_at is not None and job.finished_at is not None
                         and job.terminal_status == "succeeded"
                         and len(started_events) == len(terminal_events) == len(matching_receipts) == 1
                         and terminal_events[0]["payload"].get("effect_state") == "observed"
                         and committed(job.followup_activation_id, job.terminal_id)
                         and effect.get("state") == "observed" and effect.get("outcome") == "succeeded"
                         and execution.get("status") == "succeeded"
                         and matching_receipts[0]["status"] == "accepted"
                         and matching_receipts[0]["activation_id"] == job.activation_id
                         and matching_receipts[0]["tool_call_id"] == job.tool_call_id
                         and json.loads(matching_receipts[0]["receipt_json"]).get("execution_id") == job.execution_id)
        job.verified = valid
        if not valid:
            errors.append(f"unverified accepted job: {job.marker}")
    return {"ok": not errors, "errors": errors, "journal": journal,
            "row_counts": {table: len(value) for table, value in data.items()},
            "accepted_input_ids": sorted(accepted), "execution_ids": sorted(expected_executions),
            "marker_counts": dict(marker_counts)}


def metrics(jobs: list[Job], seconds: float) -> dict[str, Any]:
    measured = [job for job in jobs if not job.warmup]
    accepted = [job for job in measured if job.input_id]
    completed = [job for job in accepted if job.verified]
    unfinished = [job for job in accepted if job.settled_at is None]
    return {
        "attempted": len(measured), "accepted": len(accepted),
        "rejected": len(measured) - len(accepted),
        "rejection_reasons": dict(Counter(job.rejection for job in measured if not job.input_id)),
        "committed_inputs": sum(job.activation_status == "committed" for job in accepted),
        "observed_completions": sum(job.settled_at is not None and job.error is None for job in accepted),
        "completed_verified": len(completed),
        "failed": sum(job.error is not None or (job.settled_at is not None and not job.verified)
                      for job in accepted),
        "unfinished": len(unfinished),
        "attempted_per_s": len(measured) / seconds if seconds else None,
        "accepted_per_s": len(accepted) / seconds if seconds else None,
        "verified_tasks_per_s": len(completed) / seconds if seconds else None,
        "accepted_to_committed_ms": distribution([
            (job.settled_at - job.accepted_at) * 1000 for job in completed]),
        "scheduled_to_committed_ms": distribution([
            (job.settled_at - job.scheduled_at) * 1000 for job in completed if job.scheduled_at is not None]),
        "producer_lateness_ms": distribution([
            max(0, job.attempted_at - job.scheduled_at) * 1000 for job in measured if job.scheduled_at is not None]),
        "worker_accepted_to_started_ms": distribution([
            (job.started_at - job.execution_accepted_at) * 1000 for job in accepted
            if job.started_at is not None and job.execution_accepted_at is not None]),
        "worker_accepted_to_finished_ms": distribution([
            (job.finished_at - job.execution_accepted_at) * 1000 for job in accepted
            if job.finished_at is not None and job.execution_accepted_at is not None]),
    }


async def trial(args: argparse.Namespace, mode: str, count: int, journals: str,
                repeat: int, directory: Path) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    sessions = [Session(directory / f"session-{index}",
                        directory / ("shared.sqlite" if journals == "shared" else f"journal-{index}.sqlite"),
                        f"{directory.name}-s{index}", mode, args.provider_ms, args.work_ms)
                for index in range(count)]
    errors: list[str] = []
    total_start, total_cpu = time.perf_counter(), time.process_time()
    timing: dict[str, float] = {}
    active_seconds = 0.0
    lag: dict[str, Any] = distribution([])
    timeout = max(60.0, Settings().default_timeout + 4 * args.provider_ms / 1000 + args.work_ms / 1000)
    calls_before: list[int] = [0] * count
    purposes_before: list[Counter] = [Counter() for _ in sessions]
    active_cpu = 0.0
    try:
        phase_start = time.perf_counter()
        await asyncio.gather(*(session.runtime.start() for session in sessions))
        timing["startup_s"] = time.perf_counter() - phase_start
        phase_start = time.perf_counter()
        if mode == "worker":
            await asyncio.gather(*(session.worker_job(f"warmup-{index}", timeout, warmup=True)
                                   for index, session in enumerate(sessions)))
        else:
            for index, session in enumerate(sessions):
                session.submit(f"warmup-{index}", warmup=True)
            await asyncio.gather(*(session.drain_admission(timeout) for session in sessions))
        timing["warmup_s"] = time.perf_counter() - phase_start
        if any(job.rejection or job.error or job.settled_at is None
               for session in sessions for job in session.jobs):
            raise RuntimeError("warm-up did not complete; see session jobs")
        calls_before = [session.backend.calls for session in sessions]
        purposes_before = [session.request_purposes.copy() for session in sessions]
        for session in sessions:
            session.active_start_context_chars = session.runtime.status()["transcript_chars"]
        active_start, cpu_start = loop.time(), time.process_time()
        timing["active_start_loop_s"] = active_start
        monitor = LoopLag()
        try:
            if mode == "worker":
                async def produce_worker(index: int, session: Session) -> None:
                    for job_index in range(args.jobs):
                        await session.worker_job(f"job-{index}-{job_index}", timeout)
                        # Do not submit into a failed closed-loop chain.
                        if session.jobs[-1].error:
                            break
                await asyncio.gather(*(produce_worker(index, session)
                                       for index, session in enumerate(sessions)))
                timing["producer_s"] = loop.time() - active_start
                timing["drain_s"] = 0.0
            else:
                order = list(range(count))
                random.Random(args.seed + repeat).shuffle(order)
                arrivals = math.ceil(args.rate * args.seconds)
                for index in range(arrivals):
                    scheduled = active_start + index / args.rate
                    await asyncio.sleep(max(0, scheduled - loop.time()))
                    sessions[order[index % count]].submit(f"input-{index}", scheduled_at=scheduled)
                # Retain the full nominal arrival window even for a short last interval.
                await asyncio.sleep(max(0, active_start + args.seconds - loop.time()))
                timing["producer_s"] = loop.time() - active_start
                drain_start = loop.time()
                await asyncio.gather(*(session.drain_admission(timeout) for session in sessions))
                timing["drain_s"] = loop.time() - drain_start
        finally:
            active_seconds = loop.time() - active_start
            active_cpu = time.process_time() - cpu_start
            lag = monitor.stop()
    except Exception as exc:
        errors.append(f"trial: {type(exc).__name__}: {exc}")
    finally:
        phase_start = time.perf_counter()
        closed = await asyncio.gather(*(session.runtime.shutdown() for session in sessions), return_exceptions=True)
        for index, outcome in enumerate(closed):
            if isinstance(outcome, BaseException):
                errors.append(f"session {index} shutdown: {type(outcome).__name__}: {outcome}")
        timing["shutdown_s"] = time.perf_counter() - phase_start
    timing["active_s"] = active_seconds
    timing["active_parent_cpu_s"] = active_cpu
    reconciliation_start = time.perf_counter()
    reports = []
    for index, session in enumerate(sessions):
        try:
            integrity = reconcile(session)
        except Exception as exc:
            integrity = {"ok": False, "errors": [f"reconciliation: {type(exc).__name__}: {exc}"]}
        jobs = [asdict(job) for job in session.jobs]
        reports.append({
            "session_id": session.runtime.bus.session_id, "workspace": str(session.workspace),
            "journal_path": str(session.runtime.journal_path), "integrity": integrity,
            "metrics": metrics(session.jobs, active_seconds), "jobs": jobs,
            "backend_requests_total": session.backend.calls,
            "backend_requests_active": session.backend.calls - calls_before[index],
            "backend_request_purposes_total": dict(session.request_purposes),
            "backend_request_purposes_active": dict(session.request_purposes - purposes_before[index]),
            "settled_activations": len({event["payload"]["activation_id"] for event in session.events
                                        if event["type"] == "agent.activation_finished"}),
            "settled_activations_active": len({event["payload"]["activation_id"] for event in session.events
                                               if event["type"] == "agent.activation_finished"
                                               and event["accepted_at"] >= timing.get("active_start_loop_s", math.inf)}),
            "context_chars": {"initial": session.initial_context_chars,
                              "active_start": session.active_start_context_chars,
                              "final": session.runtime.status()["transcript_chars"],
                              "max_at_settlement": max(session.context_samples, default=0)},
        })
    timing["offline_reconciliation_s"] = time.perf_counter() - reconciliation_start
    timing["total_s"] = time.perf_counter() - total_start
    timing["total_parent_cpu_s"] = time.process_time() - total_cpu
    all_jobs = [job for session in sessions for job in session.jobs]
    aggregate = metrics(all_jobs, active_seconds)
    producer_seconds = timing.get("producer_s", 0.0)
    aggregate["attempted_per_producer_s"] = aggregate["attempted"] / producer_seconds if producer_seconds else None
    aggregate["accepted_per_producer_s"] = aggregate["accepted"] / producer_seconds if producer_seconds else None
    aggregate["settled_activations_active"] = sum(report["settled_activations_active"] for report in reports)
    if mode == "admission":
        aggregate["configured_aggregate_arrivals_per_s"] = args.rate
        aggregate["nominal_arrival_window_s"] = args.seconds
        aggregate["scheduled_arrivals"] = math.ceil(args.rate * args.seconds)
        if aggregate["attempted"] != aggregate["scheduled_arrivals"]:
            errors.append("producer did not attempt every scheduled arrival")
    if mode == "worker" and aggregate["attempted"] != count * args.jobs:
        errors.append("closed-loop workload stopped before all configured jobs were attempted")
    return {
        "mode": mode, "sessions": count, "journals": journals, "repeat": repeat,
        "directory": str(directory), "ok": not errors and all(report["integrity"]["ok"] for report in reports),
        "errors": errors, "timing": timing, "loop_lag_ms": lag,
        "aggregate": aggregate, "session_results": reports,
        "backend_requests_active": sum(report["backend_requests_active"] for report in reports),
        "backend_requests_total": sum(report["backend_requests_total"] for report in reports),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sessions", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--journals", choices=("shared", "separate"), nargs="+", default=["shared", "separate"])
    parser.add_argument("--mode", choices=("worker", "admission", "all"), default="all")
    parser.add_argument("--jobs", type=int, default=12, help="measured worker jobs per session")
    parser.add_argument("--rate", type=float, default=500, help="aggregate admission arrivals/second")
    parser.add_argument("--seconds", type=float, default=2, help="nominal admission arrival window")
    parser.add_argument("--provider-ms", type=float, default=20, help="synthetic async scripted-provider delay")
    parser.add_argument("--work-ms", type=float, default=5, help="real worker sleep per job")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--json", type=Path, help="new output file; existing files are never overwritten")
    parser.add_argument("--workspace-root", type=Path, help="parent directory for a fresh isolated run")
    args = parser.parse_args(argv)
    if any(value <= 0 for value in args.sessions) or args.jobs <= 0 or args.repeats <= 0:
        parser.error("sessions, jobs and repeats must be positive")
    if not all(math.isfinite(value) for value in (args.rate, args.seconds, args.provider_ms, args.work_ms)):
        parser.error("rate, seconds and delays must be finite")
    if args.rate <= 0 or args.seconds <= 0 or args.provider_ms < 0 or args.work_ms < 0:
        parser.error("rate and seconds must be positive; delays must be nonnegative")
    if args.json and args.json.exists():
        parser.error(f"output already exists: {args.json}")
    if args.workspace_root:
        args.workspace_root.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="bench-load-", dir=args.workspace_root)).resolve()
    target = args.json.resolve() if args.json else root / "results.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    settings = Settings()
    imported_package = Path(runtime_module.__file__).resolve().parent
    source_fingerprints = {
        name: hashlib.sha256((imported_package / name).read_bytes()).hexdigest()
        for name in ("llm.py", "journal.py", "python_host.py", "events.py")
    }
    output: dict[str, Any] = {
        "environment": {
            "python": sys.version.split()[0], "platform": sys.platform,
            "os": platform.platform(), "machine": platform.machine(),
            "processor": platform.processor(), "logical_cpu_count": os.cpu_count(),
            "windows_appcontainer": os.name == "nt" and os.environ.get(
                "NERVIPULSA_WINDOWS_APPCONTAINER", "").lower() in {"1", "true", "yes"},
        },
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "schema_version": 1, "label": "synthetic runtime capacity; no production SLO or inference measurement",
        "repository_root": str(REPO_ROOT), "run_directory": str(root),
        "runtime_source_root": str(imported_package.parent),
        "runtime_module_path": str(Path(runtime_module.__file__).resolve()),
        "source_sha256": source_fingerprints,
        "configuration": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "runtime_defaults": {key: getattr(settings, key) for key in (
            "ordinary_limit", "result_limit", "batch_limit", "context_limit", "max_activations",
            "default_timeout", "max_timeout", "max_output_tokens")},
        "methodology": {
            "worker": "one outstanding external input/session; actual python_exec, observed terminal, committed follow-up, exact marker check",
            "admission": "aggregate absolute-rate ordinary inputs; real batching; no rejection retries; accepted-input drain",
            "durability": "runtime defaults: FULL activation/snapshot/receipt and effect commits; asynchronous observation journal",
            "latency_clock": "asyncio loop monotonic seconds; latency distributions in milliseconds",
            "percentiles": "nearest rank; samples counts included; null when empty",
            "rate_denominator": "active seconds including warm-worker execution and accepted drain; excludes startup, warm-up, shutdown and offline checks",
            "cpu": "parent process only; worker process CPU excluded",
            "lag": "10 ms absolute timer; skipped missed timer deadlines; final overdue deadline sampled",
            "failure_counts": "failed includes explicit errors or unverified settled work; unfinished means no completion settlement; counts can overlap",
            "goodput_integrity": "only accepted jobs with independent evidence in sessions passing exact-set/row/event/journal checks contribute goodput",
            "limits": "finite synthetic scripts; retained listener events and ScriptedBackend request history add overhead; no user workload or latency SLO supplied",
        },
        "trials": [], "ok": False,
    }
    modes = ("worker", "admission") if args.mode == "all" else (args.mode,)
    try:
        for repeat in range(args.repeats):
            for mode in modes:
                for count in args.sessions:
                    for journals in args.journals:
                        directory = Path(tempfile.mkdtemp(prefix=f"{mode}-{count}-{journals}-", dir=root))
                        result = asyncio.run(trial(args, mode, count, journals, repeat, directory))
                        output["trials"].append(result)
                        summary = result["aggregate"]
                        print(f"{mode} sessions={count} journals={journals} repeat={repeat}: "
                              f"verified={summary['completed_verified']}/{summary['accepted']} "
                              f"rejected={summary['rejected']} goodput={summary['verified_tasks_per_s']} "
                              f"ok={result['ok']}", flush=True)
        output["ok"] = all(result["ok"] for result in output["trials"])
    except BaseException as exc:
        output["fatal_error"] = f"{type(exc).__name__}: {exc}"
    finally:
        with target.open("x", encoding="utf-8") as handle:
            json.dump(output, handle, indent=2, ensure_ascii=False, allow_nan=False)
            handle.write("\n")
        print(f"JSON: {target}", flush=True)
    return 0 if output["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
