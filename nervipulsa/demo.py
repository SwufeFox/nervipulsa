"""No-credential trace of the v0.4 asynchronous contract.

Passing this command shows the scripted protocol only. It does not validate a
real model or provider.
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
import tempfile
import uuid
from pathlib import Path

from .config import Settings
from .providers import ModelRequest, ScriptedBackend, text_response, tool_response
from .runtime import Runtime

FIX_CODE = """\
from pathlib import Path
import time
path = Path("sample.py")
file_text = path.read_text(encoding="utf-8")
path.write_text(file_text.replace("return a - b", "return a + b"), encoding="utf-8")
time.sleep(0.6)
print("CHECKED")
"""

RECORD_CODE = """\
from pathlib import Path
print("recorded")
Path("result.txt").write_text("CHECKED\\n", encoding="utf-8")
"""

EXPLAIN = "Explain the change in one sentence. Do not run the code again."


def scripted_responder(request: ModelRequest):
    users = [str(message.get("content") or "") for message in request.messages if message.get("role") == "user"]

    def runtime_event(text: str, needle: str) -> bool:
        return '"kind":"runtime_event"' in text and needle in text

    if any(runtime_event(text, "recorded") for text in users):
        return text_response("Done. The check finished and the result was recorded.")
    if any(runtime_event(text, "CHECKED") for text in users):
        return tool_response(("rec", RECORD_CODE, None))
    if any(EXPLAIN in text for text in users):
        return text_response(
            "The edit changes subtraction to addition in add(). The running check was left untouched."
        )
    return tool_response(("fix", FIX_CODE, 30), text="I'll fix sample.py and run a slow check.")


def _fresh_workspace() -> Path:
    """Prefer a system temp dir, and fall back when that filesystem is not writable."""
    candidate = Path(tempfile.mkdtemp(prefix="nervipulsa-demo-"))
    probe = candidate / ".write-probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return candidate
    except OSError:
        shutil.rmtree(candidate, ignore_errors=True)
        root = Path(__file__).resolve().parents[1] / ".scratch" / "demo"
        root.mkdir(parents=True, exist_ok=True)
        path = root / uuid.uuid4().hex[:8]
        path.mkdir()
        return path


async def run_scripted(workspace: Path | None = None) -> tuple[bool, str]:
    owns_workspace = workspace is None
    if workspace is None:
        workspace = _fresh_workspace()
    lines: list[str] = []
    backend = ScriptedBackend(scripted_responder)
    settings = Settings(model="scripted", workspace=str(workspace), max_timeout=30, default_timeout=30)
    runtime = Runtime(settings, backend, workspace, echo=lines.append)
    injected = False

    def on_event(event) -> None:
        nonlocal injected
        if event.type == "python.started" and not injected:
            injected = True
            runtime.submit_text(EXPLAIN)

    try:
        (workspace / "sample.py").write_text("def add(a, b):\n    return a - b\n", encoding="utf-8")
        await runtime.start()
        runtime.add_listener(on_event)
        delivery = runtime.submit_text("Read sample.py and fix add() so it adds. Then run a slow check.")
        if not delivery.accepted:
            return False, f"initial message rejected: {delivery.reason}"
        if not await runtime.wait_until_idle(timeout=20):
            return False, "runtime did not become idle\n" + _debug(runtime, lines)
        await asyncio.sleep(0.2)
        if backend.calls != 4:
            return False, f"expected 4 model calls, got {backend.calls}\n" + _debug(runtime, lines)
        sample = (workspace / "sample.py").read_text(encoding="utf-8")
        if "return a + b" not in sample:
            return False, f"sample.py was not fixed:\n{sample}"
        result = (workspace / "result.txt").read_text(encoding="utf-8")
        if "CHECKED" not in result:
            return False, f"result.txt missing CHECKED: {result!r}"
        requested = [event for event in runtime.trace if event.type == "python.requested"]
        if len(requested) != 2:
            return False, f"expected 2 executions, saw {len(requested)}\n" + _debug(runtime, lines)
        meaningful = [item for item in runtime.actor.activations if _meaningful(runtime, item.input_event_ids)]
        if len(meaningful) < 4:
            return False, f"expected 4 activations, saw {len(meaningful)}\n" + _debug(runtime, lines)
        first, second, third, fourth = meaningful[:4]
        first_events = _events(runtime, first.input_event_ids)
        second_events = _events(runtime, second.input_event_ids)
        third_events = _events(runtime, third.input_event_ids)
        fourth_events = _events(runtime, fourth.input_event_ids)
        if [event.type for event in first_events] != ["user.message"]:
            return False, "first activation did not see only the original request\n" + _debug(runtime, lines)
        if not second_events or second_events[0].payload.get("text") != EXPLAIN:
            return False, "interjection was not its own later activation\n" + _debug(runtime, lines)
        if any(event.id in first.input_event_ids for event in second_events):
            return False, "interjection leaked into the first snapshot"
        if second_events[0].seq <= first.input_high_water_seq:
            return False, "interjection was not ordered after the first snapshot"
        if [event.type for event in third_events] != ["python.finished"]:
            return False, "third activation did not consume the execution result\n" + _debug(runtime, lines)
        if [event.type for event in fourth_events] != ["python.finished"]:
            return False, "fourth activation did not consume the follow-up result\n" + _debug(runtime, lines)
        tool_messages = [message for message in runtime.actor.transcript.messages if message.get("role") == "tool"]
        if any("CHECKED" in str(message.get("content")) for message in tool_messages):
            return False, "tool receipt included the final stdout"
        observations = [
            message
            for message in runtime.actor.transcript.messages
            if message.get("role") == "user" and "runtime_event" in str(message.get("content"))
        ]
        if len(observations) < 2:
            return False, "runtime observations were not projected into the transcript"
        calls_after_idle = backend.calls
        await asyncio.sleep(0.2)
        if backend.calls != calls_after_idle:
            return False, "model was called while idle"
        await runtime.shutdown()
        if runtime.host.cleanup_failed or runtime.host.managed_pids():
            return False, "shutdown left a managed process behind"
        summary = "\n".join(lines)
        return True, summary
    except Exception as exc:
        try:
            await runtime.shutdown()
        except Exception:
            pass
        return False, f"{type(exc).__name__}: {exc}\n" + _debug(runtime, lines)
    finally:
        if owns_workspace:
            shutil.rmtree(workspace, ignore_errors=True)


def _events(runtime: Runtime, ids: tuple[str, ...]):
    by_id = {event.id: event for event in runtime.trace}
    return [by_id[event_id] for event_id in ids if event_id in by_id]


def _meaningful(runtime: Runtime, ids: tuple[str, ...]) -> bool:
    return any(event.type != "agent.retry" for event in _events(runtime, ids))


def _debug(runtime: Runtime, lines: list[str]) -> str:
    rendered = "\n".join(lines)
    activations = []
    for activation in runtime.actor.activations:
        activations.append(
            f"{activation.id} {activation.status} inputs={activation.input_event_ids} error={activation.error_kind}"
        )
    events = [f"{event.seq} {event.type} {event.id} reply_to={event.reply_to}" for event in runtime.trace]
    return rendered + "\nACTIVATIONS\n" + "\n".join(activations) + "\nEVENTS\n" + "\n".join(events)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nervipulsa.demo")
    parser.add_argument("--scripted", action="store_true", help="Run the no-credential contract trace")
    args = parser.parse_args(argv)
    scope = "real model: NOT VERIFIED by this command; see examples/real_model_experiments.py"
    if not args.scripted:
        print("usage: python -m nervipulsa.demo --scripted")
        print(scope)
        return 2
    ok, detail = asyncio.run(run_scripted())
    if detail.strip():
        print(detail)
    print("scripted contract: PASS" if ok else "scripted contract: FAIL")
    print(scope)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
