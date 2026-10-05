"""Real-model experiments from Nervipulsa_Design_v0.4.md section 13.3.

Run one experiment or all three against a live OpenAI-compatible endpoint:

    NV_BASE_URL=http://127.0.0.1:8317/v1 \
    NV_API_KEY=... NV_MODEL=deepseek-flash \
    python examples/real_model_experiments.py 1 2 3

Credentials come only from the environment and are never written to a file.
Each experiment keeps its workspace under .scratch/, its event trace, and a
JSON plus text report under .scratch/reports (override with NV_REPORT_DIR).

These are not part of `pytest`: they need credentials, they cost tokens, and
they measure whether a model can use the asynchronous protocol, which the
scripted backend cannot show.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from nervipulsa.config import Settings
from nervipulsa.providers import OpenAICompatibleBackend
from nervipulsa.runtime import Runtime

SCRATCH = ROOT / ".scratch" / f"real-model-{uuid.uuid4().hex[:12]}"
REPORTS = Path(os.environ.get("NV_REPORT_DIR") or (SCRATCH / "reports"))

EXP1_ASK = (
    "Use python_exec to run one program: import time, then time.sleep(20), then print('MARKER-EXP1'). "
    "Submit it and do not wait for it. Do not run it a second time and do not start anything else."
)
EXP1_INTERJECT = "Reply that you received this. The original execution keeps running; do not run it again."

EXP2_ASK = (
    "Work in two steps using python_exec.\n"
    "Step 1: run a program that sleeps 15 seconds and then prints 7, then ends. "
    "Do not do any multiplication inside that same program.\n"
    "Step 2: after I confirm, run a second program that multiplies 7 by the current multiplier and prints the result.\n"
    "The current multiplier is 2, but do not run step 2 yet."
)
EXP2_INTERJECT = "The multiplier is now 3. You may run step 2 with the new multiplier."

EXP3_ASK = (
    "This workspace contains calc.py and its tests. Run `python -m pytest -q` to reproduce the failure, "
    "read calc.py, fix add() so it returns the sum, then run the tests again and report the result."
)
EXP3_INTERJECT = (
    "Extra requirement: keep add() taking exactly two positional parameters (a, b). "
    "Do not switch to *args and do not add default values."
)

SLOW_TEST = '''import time


def test_slow_regression():
    time.sleep(15)
    assert True
'''

PYTEST_INI = "[pytest]\naddopts = -p no:cacheprovider\n"


class Counting:
    """Counts model requests without changing adapter behavior."""

    def __init__(self, inner) -> None:
        self.inner = inner
        self.calls = 0
        self.requests = []
        self.responses = []

    async def complete(self, request):
        self.calls += 1
        self.requests.append(request)
        response = await self.inner.complete(request)
        self.responses.append(response)
        return response


class Recorder:
    def __init__(self) -> None:
        self.lines: list[tuple[float, str]] = []
        self.events: list[dict] = []
        self._request_waiters: list[tuple[str, asyncio.Event]] = []
        self._type_waiters: dict[str, asyncio.Event] = {}

    def note(self, text: str) -> None:
        now = time.time()
        self.lines.append((now, text))
        print(f"    {time.strftime('%H:%M:%S', time.localtime(now))} {text}", flush=True)

    def request_waiter(self, needle: str) -> asyncio.Event:
        event = asyncio.Event()
        self._request_waiters.append((needle, event))
        return event

    def type_waiter(self, event_type: str) -> asyncio.Event:
        return self._type_waiters.setdefault(event_type, asyncio.Event())

    def observe(self, event) -> None:
        payload = dict(event.payload)
        code = payload.get("code") if isinstance(payload.get("code"), str) else None
        record = {
            "wall": time.time(),
            "seq": event.seq,
            "id": event.id,
            "type": event.type,
            "source": event.source,
            "target": event.target,
            "reply_to": event.reply_to,
            "payload": {key: value for key, value in payload.items() if key != "code"},
        }
        if code is not None:
            record["payload"]["code"] = code
        self.events.append(record)
        waiter = self._type_waiters.get(event.type)
        if waiter is not None:
            waiter.set()
        if event.type == "python.requested" and code:
            for needle, pending in self._request_waiters:
                if needle in code:
                    pending.set()


def build_runtime(workspace: Path, recorder: Recorder) -> tuple[Runtime, Counting]:
    settings = Settings(
        provider="openai",
        base_url=os.environ["NV_BASE_URL"],
        api_key=os.environ["NV_API_KEY"],
        model=os.environ.get("NV_MODEL", "deepseek-flash"),
        workspace=str(workspace),
        max_timeout=120,
        default_timeout=60,
    )
    backend = Counting(
        OpenAICompatibleBackend(
            provider=settings.provider,
            base_url=settings.base_url,
            api_key=settings.api_key,
            model=settings.model,
            timeout=120,
        )
    )
    runtime = Runtime(settings, backend, workspace, echo=recorder.note)
    runtime.add_listener(recorder.observe)
    return runtime, backend


def fresh_workspace(name: str) -> Path:
    path = SCRATCH / f"{name}-{uuid.uuid4().hex[:8]}"
    path.mkdir(parents=True)
    return path


def summarize(runtime: Runtime, backend: Counting, recorder: Recorder, checks: list[dict]) -> dict:
    prompt_tokens = completion_tokens = total_tokens = 0
    activations = []
    for activation in runtime.actor.activations:
        usage = activation.response.usage if activation.response else None
        if isinstance(usage, dict):
            prompt_tokens += int(usage.get("prompt_tokens") or 0)
            completion_tokens += int(usage.get("completion_tokens") or 0)
            total_tokens += int(usage.get("total_tokens") or 0)
        activations.append(
            {
                "id": activation.id,
                "status": activation.status,
                "input_event_ids": list(activation.input_event_ids),
                "input_high_water_seq": activation.input_high_water_seq,
                "tool_calls": dict(activation.tool_calls),
                "error": activation.error_kind,
                "usage": usage,
            }
        )
    executions = [
        {
            "execution_id": event.reply_to,
            "status": event.payload.get("status"),
            "namespace_reset": event.payload.get("namespace_reset"),
            "stdout": event.payload.get("stdout"),
            "stderr": (event.payload.get("stderr") or "")[:2000],
            "wall": event.accepted_at,
        }
        for event in runtime.trace
        if event.type == "python.finished"
    ]
    return {
        "model_calls": backend.calls,
        "tokens": {
            "prompt": prompt_tokens,
            "completion": completion_tokens,
            "total": total_tokens,
        },
        "activations": activations,
        "executions": executions,
        "events": recorder.events,
        "timeline": [{"wall": stamp, "text": text} for stamp, text in recorder.lines],
        "checks": checks,
        "ok": all(check["ok"] for check in checks),
    }


def journal_is_clean(runtime: Runtime) -> tuple[bool, str]:
    key = os.environ["NV_API_KEY"]
    path = runtime.journal_path
    if not path.exists():
        return False, "journal file missing"
    blob = path.read_bytes()
    if key.encode() in blob:
        return False, "API key found in the journal"
    return True, f"journal {path.stat().st_size} bytes, no key"


def model_saw(runtime: Runtime, activation, needle: str) -> bool:
    by_id = {event.id: event for event in runtime.trace}
    for event_id in activation.input_event_ids:
        event = by_id.get(event_id)
        if event is None:
            continue
        text = json.dumps(dict(event.payload), ensure_ascii=False)
        if needle in text:
            return True
    return False


# --------------------------------------------------------------------------- exp 1


async def experiment_one() -> dict:
    print("\n=== experiment 1: interjection during a running execution ===", flush=True)
    workspace = fresh_workspace("exp1")
    recorder = Recorder()
    runtime, backend = build_runtime(workspace, recorder)
    checks: list[dict] = []
    await runtime.start()
    marker_requested = recorder.request_waiter("MARKER-EXP1")
    try:
        runtime.submit_text(EXP1_ASK)
        try:
            await asyncio.wait_for(marker_requested.wait(), timeout=90)
        except asyncio.TimeoutError:
            checks.append({"name": "model submitted the sleep program", "ok": False, "detail": "no request in 90s"})
            return summarize(runtime, backend, recorder, checks)
        recorder.note("marker program accepted; waiting 3s before the interjection")
        await asyncio.sleep(3)
        submit_at = time.time()
        runtime.submit_text(EXP1_INTERJECT)
        if not await runtime.wait_until_idle(timeout=150):
            checks.append({"name": "session reached idle", "ok": False, "detail": "still busy after 150s"})
            return summarize(runtime, backend, recorder, checks)

        events = recorder.events
        marker_runs = [
            event
            for event in events
            if event["type"] == "python.requested" and "MARKER-EXP1" in (event["payload"].get("code") or "")
        ]
        checks.append(
            {
                "name": "the marker program was submitted exactly once",
                "ok": len(marker_runs) == 1,
                "detail": f"{len(marker_runs)} request(s)",
            }
        )
        finished = [event for event in events if event["type"] == "python.finished"]
        checks.append(
            {
                "name": "exactly one terminal for the whole run",
                "ok": len(finished) == 1,
                "detail": f"{len(finished)} terminal(s): {[item['payload'].get('status') for item in finished]}",
            }
        )
        if not finished:
            return summarize(runtime, backend, recorder, checks)
        terminal = finished[0]
        replies = [event for event in events if event["type"] == "assistant.message"]
        before = [event for event in replies if event["wall"] < terminal["wall"]]
        after = [event for event in replies if event["wall"] >= terminal["wall"]]
        checks.append(
            {
                "name": "a reply arrived before the marker finished",
                "ok": bool(before),
                "detail": f"{len(before)} repl(ies) before, {len(after)} after; "
                f"gap={terminal['wall'] - submit_at:.1f}s after the interjection",
            }
        )
        checks.append(
            {
                "name": "the marker printed and the terminal reports it",
                "ok": terminal["payload"].get("status") == "succeeded"
                and "MARKER-EXP1" in (terminal["payload"].get("stdout") or ""),
                "detail": repr((terminal["payload"].get("stdout") or "")[:120]),
            }
        )
        consuming = [
            activation
            for activation in runtime.actor.activations
            if terminal["id"] in activation.input_event_ids
        ]
        checks.append(
            {
                "name": "a later activation consumed the terminal",
                "ok": bool(consuming),
                "detail": f"{len(consuming)} activation(s)",
            }
        )
        reported_after = [
            event
            for event in replies
            if event["wall"] > terminal["wall"] and consuming
        ]
        checks.append(
            {
                "name": "the model reported the result after the marker",
                "ok": bool(reported_after),
                "detail": f"{len(reported_after)} reply(ies) after the terminal",
            }
        )
        clean, detail = journal_is_clean(runtime)
        checks.append({"name": "journal has no API key", "ok": clean, "detail": detail})
    finally:
        await runtime.shutdown()
    return summarize(runtime, backend, recorder, checks)


# --------------------------------------------------------------------------- exp 2


async def experiment_two() -> dict:
    print("\n=== experiment 2: new information changes the next action ===", flush=True)
    workspace = fresh_workspace("exp2")
    recorder = Recorder()
    runtime, backend = build_runtime(workspace, recorder)
    checks: list[dict] = []
    await runtime.start()
    step_one = recorder.request_waiter("sleep")
    try:
        runtime.submit_text(EXP2_ASK)
        try:
            await asyncio.wait_for(step_one.wait(), timeout=90)
        except asyncio.TimeoutError:
            checks.append({"name": "model submitted step 1", "ok": False, "detail": "no request in 90s"})
            return summarize(runtime, backend, recorder, checks)
        recorder.note("step 1 accepted; changing the multiplier in 3s")
        await asyncio.sleep(3)
        runtime.submit_text(EXP2_INTERJECT)
        if not await runtime.wait_until_idle(timeout=150):
            checks.append({"name": "session reached idle", "ok": False, "detail": "still busy after 150s"})
            return summarize(runtime, backend, recorder, checks)

        events = recorder.events
        requests = [event for event in events if event["type"] == "python.requested"]
        checks.append(
            {
                "name": "step 1 did not fold the multiplication into its own program",
                "ok": not any(
                    "multipl" in (event["payload"].get("code") or "").lower()
                    for event in requests
                    if "sleep" in (event["payload"].get("code") or "")
                ),
                "detail": f"{len(requests)} request(s)",
            }
        )
        finished = [event for event in events if event["type"] == "python.finished"]
        stdout = "".join((event["payload"].get("stdout") or "") for event in finished)
        checks.append(
            {
                "name": "the final arithmetic used the updated multiplier",
                "ok": "21" in stdout,
                "detail": f"stdout={stdout!r}",
            }
        )
        multiplier_activation = [
            activation
            for activation in runtime.actor.activations
            if model_saw(runtime, activation, "multiplier is now 3")
        ]
        checks.append(
            {
                "name": "the activation that computed the product had seen the change",
                "ok": bool(multiplier_activation),
                "detail": f"{len(multiplier_activation)} activation(s) saw the message",
            }
        )
        product_activation = [
            activation
            for activation in runtime.actor.activations
            if any(call_id for call_id, state in activation.tool_calls.items() if state == "accepted")
            and model_saw(runtime, activation, "multiplier is now 3")
        ]
        checks.append(
            {
                "name": "the product ran in an activation after the change, not the first one",
                "ok": bool(product_activation),
                "detail": f"{len(product_activation)} activation(s)",
            }
        )
        clean, detail = journal_is_clean(runtime)
        checks.append({"name": "journal has no API key", "ok": clean, "detail": detail})
    finally:
        await runtime.shutdown()
    return summarize(runtime, backend, recorder, checks)


# --------------------------------------------------------------------------- exp 3


async def experiment_three() -> dict:
    print("\n=== experiment 3: real bug fix with a compatibility note ===", flush=True)
    workspace = fresh_workspace("exp3")
    shutil.copy(ROOT / "examples" / "bugfix" / "calc.py", workspace / "calc.py")
    shutil.copy(ROOT / "examples" / "bugfix" / "test_calc.py", workspace / "test_calc.py")
    (workspace / "test_slow.py").write_text(SLOW_TEST, encoding="utf-8")
    (workspace / "pytest.ini").write_text(PYTEST_INI, encoding="utf-8")
    before = (workspace / "calc.py").read_text(encoding="utf-8")

    recorder = Recorder()
    runtime, backend = build_runtime(workspace, recorder)
    checks: list[dict] = []
    await runtime.start()
    pytest_run = recorder.request_waiter("pytest")
    try:
        runtime.submit_text(EXP3_ASK)
        try:
            await asyncio.wait_for(pytest_run.wait(), timeout=120)
        except asyncio.TimeoutError:
            checks.append({"name": "model ran the tests", "ok": False, "detail": "no pytest request in 120s"})
            return summarize(runtime, backend, recorder, checks)
        recorder.note("test run started; sending the compatibility note in 4s")
        await asyncio.sleep(4)
        runtime.submit_text(EXP3_INTERJECT)
        if not await runtime.wait_until_idle(timeout=300):
            checks.append({"name": "session reached idle", "ok": False, "detail": "still busy after 300s"})
            return summarize(runtime, backend, recorder, checks)
        await asyncio.sleep(0.5)

        after = (workspace / "calc.py").read_text(encoding="utf-8")
        checks.append(
            {
                "name": "calc.py was actually changed",
                "ok": after != before,
                "detail": after.replace("\n", " | "),
            }
        )
        checks.append(
            {
                "name": "the signature stayed two positional parameters",
                "ok": "def add(a, b)" in after,
                "detail": [line for line in after.splitlines() if line.startswith("def ")],
            }
        )
        fresh = subprocess.run(
            [sys.executable, "-m", "pytest", "-q"],
            cwd=str(workspace),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,
        )
        checks.append(
            {
                "name": "a fresh test process passes",
                "ok": fresh.returncode == 0,
                "detail": (fresh.stdout or "").strip().splitlines()[-1] if fresh.stdout else fresh.stderr[:200],
            }
        )
        checks.append(
            {
                "name": "the compatibility note reached an activation",
                "ok": any(model_saw(runtime, activation, "exactly two positional parameters") for activation in runtime.actor.activations),
                "detail": "checked activation inputs",
            }
        )
        clean, detail = journal_is_clean(runtime)
        checks.append({"name": "journal has no API key", "ok": clean, "detail": detail})
    finally:
        await runtime.shutdown()
    report = summarize(runtime, backend, recorder, checks)
    report["diff"] = {"before": before, "after": (workspace / "calc.py").read_text(encoding="utf-8")}
    return report


EXPERIMENTS = {"1": experiment_one, "2": experiment_two, "3": experiment_three}


def write_report(name: str, report: dict) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / f"exp{name}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    lines = [f"experiment {name}: {'PASS' if report['ok'] else 'FAIL'}",
             f"model calls: {report['model_calls']}",
             f"tokens: {report['tokens']}",
             "checks:"]
    for check in report["checks"]:
        lines.append(f"  [{'ok' if check['ok'] else 'FAIL'}] {check['name']} :: {check['detail']}")
    lines.append("executions:")
    for execution in report["executions"]:
        lines.append(f"  {execution['execution_id']} {execution['status']} stdout={execution['stdout']!r}")
        if execution["stderr"]:
            lines.append(f"      stderr={execution['stderr'][:300]!r}")
    lines.append("assistant replies:")
    for event in report["events"]:
        if event["type"] == "assistant.message":
            lines.append(f"  {event['id']} {str(event['payload'].get('text'))[:400]}")
    lines.append("python requests:")
    for event in report["events"]:
        if event["type"] == "python.requested":
            lines.append(f"  {event['id']} {str(event['payload'].get('code'))[:400]!r}")
    (REPORTS / f"exp{name}.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


async def main(numbers: list[str]) -> int:
    failures = 0
    for number in numbers:
        report = await EXPERIMENTS[number]()
        write_report(number, report)
        print(f"\n--- experiment {number}: {'PASS' if report['ok'] else 'FAIL'} "
              f"({report['model_calls']} model calls, {report['tokens']['total']} tokens)", flush=True)
        for check in report["checks"]:
            print(f"    [{'ok' if check['ok'] else 'FAIL'}] {check['name']} :: {check['detail']}", flush=True)
        if not report["ok"]:
            failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("experiments", nargs="*", default=["1", "2", "3"])
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.experiments)))
