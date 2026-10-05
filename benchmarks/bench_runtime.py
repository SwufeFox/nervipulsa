"""Micro + end-to-end benchmarks for the v0.4 runtime.

These measure mechanism, not model quality:

* transcript / projector measurement cost as history grows;
* end-to-end turn cost for a tool-calling session with a growing transcript;
* Python host idle-wake latency: accepted -> python.started;
* idle cost of an empty runtime (thread churn / wakeups per second).

Run it before and after a change and keep both numbers in the record:

    python benchmarks/bench_runtime.py
    python benchmarks/bench_runtime.py --json before.json
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nervipulsa.config import Settings  # noqa: E402
from nervipulsa.events import Event, Lane  # noqa: E402
from nervipulsa.llm import ContextProjector, Transcript  # noqa: E402
from nervipulsa.providers import (  # noqa: E402
    ModelRequest,
    ScriptedBackend,
    text_response,
    tool_response,
)
from nervipulsa.runtime import Runtime  # noqa: E402

# A payload close to what a real read/edit/verify loop produces.
BLOB = ("x" * 96 + "\n") * 80  # ~7.8 KiB of stdout per execution


def _event(name: str, seq: int, text: str) -> Event:
    return Event(
        session_id="bench",
        id=name,
        seq=seq,
        type="user.message",
        source="cli",
        target="llm",
        reply_to=None,
        payload={"text": text},
        accepted_at=1.0,
    )


def bench_transcript_measurement(message_target: int = 400) -> dict[str, Any]:
    """Cost of one context measurement as the transcript grows."""
    transcript = Transcript()
    projector = ContextProjector(
        transcript,
        lambda: {
            "worker_epoch": 3,
            "namespace": "current_epoch_only",
            "current_execution_id": None,
            "queued_execution_ids": ["evt_1", "evt_2"],
            "unfinished_execution_ids": ["evt_1", "evt_2"],
        },
    )
    seq = 0
    while len(transcript.messages) < message_target:
        seq += 1
        transcript.project(_event(f"u{seq}", seq, User := f"user requirement number {seq}"))
        transcript.add_assistant(tool_response((f"c{seq}", "print(1)", None)))
        transcript.add_tool(f"c{seq}", {"status": "accepted", "execution_id": f"evt_{seq}"})
        finished = Event(
            session_id="bench",
            id=f"f{seq}",
            seq=seq,
            type="python.finished",
            source="python_host",
            target="llm",
            reply_to=f"evt_{seq}",
            payload={
                "status": "succeeded",
                "stdout": BLOB,
                "stderr": "",
                "stdout_bytes": len(BLOB),
                "stderr_bytes": 0,
                "duration_ms": 12,
                "worker_epoch": 3,
                "namespace_reset": False,
            },
            accepted_at=1.0,
        )
        transcript.project(finished)

    ground_truth = len(json.dumps(transcript.snapshot(), ensure_ascii=False))
    rounds = 200
    samples: list[float] = []
    legacy: list[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        # One activation asks for both plus the projector view, several times.
        transcript.serialized_length()
        projector.view_length()
        transcript.serialized_length()
        projector.view_length()
        samples.append((time.perf_counter() - start) * 1000)
    for _ in range(rounds):
        start = time.perf_counter()
        # The pre-optimization path: a full json.dumps per measurement.
        len(json.dumps(transcript.snapshot(), ensure_ascii=False))
        len(json.dumps(projector.snapshot(), ensure_ascii=False))
        len(json.dumps(transcript.snapshot(), ensure_ascii=False))
        len(json.dumps(projector.snapshot(), ensure_ascii=False))
        legacy.append((time.perf_counter() - start) * 1000)
    gc.collect()
    return {
        "messages": len(transcript.messages),
        "transcript_chars": transcript.serialized_length(),
        "chars_match_full_dump": transcript.serialized_length() == ground_truth,
        "view_chars": projector.view_length(),
        "ms_per_4_measurements_mean": round(statistics.mean(samples), 4),
        "ms_per_4_measurements_p95": round(sorted(samples)[int(rounds * 0.95) - 1], 4),
        "legacy_full_dump_ms_mean": round(statistics.mean(legacy), 4),
    }


def bench_tool_session(workspace: Path, activations: int = 24) -> dict[str, Any]:
    """Wall cost of a tool-calling session whose transcript keeps growing."""
    issued: list[str] = []

    def respond(request: ModelRequest):
        users = [str(m.get("content") or "") for m in request.messages if m.get("role") == "user"]
        last = users[-1] if users else ""
        if '"kind":"runtime_event"' in last:
            return text_response("observed")
        index = len(issued)
        issued.append(index)
        code = f"import sys\nsys.stdout.write({BLOB!r})\nprint('RUN-{index}')\n"
        return tool_response((f"call-{index}", code, None))

    async def body() -> dict[str, Any]:
        backend = ScriptedBackend(respond)
        settings = Settings(model="scripted", workspace=str(workspace), max_timeout=30, default_timeout=30)
        # Keep compaction out of the measured window: this isolates measurement
        # cost from summary generation.
        settings.context_limit = 20_000_000
        runtime = Runtime(settings, backend, workspace, echo=lambda _t: None)
        await runtime.start()
        try:
            wall_start = time.perf_counter()
            cpu_start = time.process_time()
            for index in range(activations):
                runtime.submit_text(f"do step {index}")
                await _until_finished(runtime, index)
            cpu = time.process_time() - cpu_start
            wall = time.perf_counter() - wall_start
            return {
                "tool_activations": len(issued),
                "model_calls": backend.calls,
                "transcript_chars": runtime.actor.transcript.serialized_length(),
                "wall_ms": round(wall * 1000, 1),
                "cpu_ms": round(cpu * 1000, 1),
            }
        finally:
            await runtime.shutdown()

    return asyncio.run(body())


async def _until_finished(runtime: Runtime, index: int) -> None:
    import time as _t

    deadline = _t.monotonic() + 20
    target = index + 1
    while _t.monotonic() < deadline:
        if sum(e.type == "python.finished" for e in runtime.trace) >= target and not runtime.actor.busy:
            # let the follow-up text activation land, then leave it settled
            await asyncio.sleep(0.05)
            if not runtime.actor.busy and runtime.llm_box.size == 0:
                return
        await asyncio.sleep(0.02)
    raise AssertionError("execution did not finish")


def bench_host_idle_wake(workspace: Path, rounds: int = 12) -> dict[str, Any]:
    """Latency from accepting python.requested to the code actually starting."""
    PY = "print('ping')\n"

    async def body() -> dict[str, Any]:
        backend = ScriptedBackend(lambda _r: text_response("unused"))
        settings = Settings(model="scripted", workspace=str(workspace), max_timeout=30, default_timeout=30)
        runtime = Runtime(settings, backend, workspace, echo=lambda _t: None)
        await runtime.start()
        try:
            # Warm the worker so startup cost is not counted.
            await _run_once(runtime, PY)
            await runtime.wait_until_idle(10)
            latencies: list[float] = []
            for _ in range(rounds):
                await runtime.wait_until_idle(10)
                delivery = runtime.llm_emitter.call(
                    "python.requested",
                    {"code": PY, "timeout": 30.0},
                    reserve_result_for="llm",
                )
                assert delivery.accepted, delivery.reason
                accepted_loop_time = None
                for event in reversed(runtime.trace):
                    if event.id == delivery.event_id:
                        accepted_loop_time = event.accepted_at
                        break
                assert accepted_loop_time is not None
                await _waitStarted(runtime, delivery.event_id)
                for event in runtime.trace:
                    if event.type == "python.started" and event.reply_to == delivery.event_id:
                        latencies.append((event.accepted_at - accepted_loop_time) * 1000)
                        break
                runtime.llm_box.drain_available(64)
                for item in runtime.llm_box._reservations.copy():
                    runtime.llm_box.release_terminal(item)
            await runtime.shutdown()
            return {
                "rounds": len(latencies),
                "accepted_to_started_ms_mean": round(statistics.mean(latencies), 2),
                "accepted_to_started_ms_max": round(max(latencies), 2),
                "accepted_to_started_ms_p95": round(
                    sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)], 2
                ),
            }
        finally:
            if runtime.bus.state.value != "CLOSED":
                await runtime.shutdown()

    return asyncio.run(body())


async def _waitStarted(runtime: Runtime, execution_id: str) -> None:
    import time as _t

    deadline = _t.monotonic() + 15
    while _t.monotonic() < deadline:
        for event in runtime.trace:
            if event.type == "python.started" and event.reply_to == execution_id:
                return
        for event in runtime.trace:
            if event.type == "python.finished" and event.reply_to == execution_id:
                return
        await asyncio.sleep(0.005)


async def _run_once(runtime: Runtime, code: str) -> None:
    delivery = runtime.llm_emitter.call(
        "python.requested", {"code": code, "timeout": 30.0}, reserve_result_for="llm"
    )
    assert delivery.accepted, delivery.reason
    await _waitStarted(runtime, delivery.event_id)
    await _waitFinished(runtime, delivery.event_id)
    runtime.llm_box.drain_available(64)
    for item in runtime.llm_box._reservations.copy():
        runtime.llm_box.release_terminal(item)


async def _waitFinished(runtime: Runtime, execution_id: str) -> None:
    import time as _t

    deadline = _t.monotonic() + 15
    while _t.monotonic() < deadline:
        for event in runtime.trace:
            if event.type == "python.finished" and event.reply_to == execution_id:
                return
        await asyncio.sleep(0.01)


def bench_idle_overhead(workspace: Path, seconds: float = 2.0) -> dict[str, Any]:
    """CPU cost of simply existing: an idle runtime must not burn resources."""

    async def body() -> dict[str, Any]:
        backend = ScriptedBackend(lambda _r: text_response("unused"))
        settings = Settings(model="scripted", workspace=str(workspace), max_timeout=30, default_timeout=30)
        runtime = Runtime(settings, backend, workspace, echo=lambda _t: None)
        await runtime.start()
        try:
            await runtime.wait_until_idle(10)
            cpu_start = time.process_time()
            wakeups = [len(runtime.host._threads_snapshot())]
            await asyncio.sleep(seconds)
            cpu = time.process_time() - cpu_start
            wakeups.append(runtime.host.scheduler_wakeups)
            await runtime.shutdown()
            return {
                "seconds": seconds,
                "cpu_ms": round(cpu * 1000, 2),
                "scheduler_wakeups": runtime.host.scheduler_wakeups,
                "thread_count_start": wakeups[0],
            }
        finally:
            if runtime.bus.state.value != "CLOSED":
                await runtime.shutdown()

    return asyncio.run(body())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bench_runtime")
    parser.add_argument("--json", dest="json_path", default=None, help="write results as JSON")
    parser.add_argument("--messages", type=int, default=400)
    parser.add_argument("--activations", type=int, default=24)
    parser.add_argument("--host-rounds", type=int, default=12)
    parser.add_argument("--idle-seconds", type=float, default=2.0)
    parser.add_argument("--quick", action="store_true", help="shorter run for smoke use")
    args = parser.parse_args(argv)
    if args.quick:
        args.messages = 200
        args.activations = 8
        args.host_rounds = 6
        args.idle_seconds = 1.0

    root = Path(__file__).resolve().parents[1] / ".scratch" / "bench"
    root.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {"python": sys.version.split()[0], "platform": sys.platform}
    results["transcript_measurement"] = bench_transcript_measurement(args.messages)
    print(f"transcript measurement: {results['transcript_measurement']}")
    results["tool_session"] = bench_tool_session(root / "session", args.activations)
    print(f"tool session:           {results['tool_session']}")
    results["host_idle_wake"] = bench_host_idle_wake(root / "host", args.host_rounds)
    print(f"host idle wake:         {results['host_idle_wake']}")
    results["idle_overhead"] = bench_idle_overhead(root / "idle", args.idle_seconds)
    print(f"idle overhead:          {results['idle_overhead']}")
    if args.json_path:
        Path(args.json_path).write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"wrote {args.json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
