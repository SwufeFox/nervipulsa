"""Abrupt OS-process recovery probes for Runtime and its observation Journal."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest


_CHILD = r'''
import asyncio, json, os, sys, time
from pathlib import Path
from nervipulsa.config import Settings
from nervipulsa.providers import ScriptedBackend, text_response
from nervipulsa.runtime import Runtime

root, mode, ready_path = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
async def main():
    runtime = Runtime(Settings(), ScriptedBackend(lambda _: text_response("fresh")), root)
    if mode == "request":
        gate = asyncio.Event()
        original = runtime.python_box.take_batch
        async def held(limit):
            await gate.wait()
            return await original(limit)
        runtime.python_box.take_batch = held
    elif mode == "terminal":
        gate = asyncio.Event()
        async def held(_limit):
            await gate.wait()
            return []
        runtime.llm_box.take_batch = held
    await runtime.start()
    if mode in ("request", "terminal"):
        delivery = runtime.host_emitter.call("python.requested", {
            "code": "from pathlib import Path\nPath('crash-effect.marker').write_text('effect', encoding='utf-8')",
            "timeout": 30,
        }, reserve_result_for="llm")
        assert delivery.accepted, delivery
        if mode == "terminal":
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if (root / "crash-effect.marker").exists() and runtime.llm_box.size:
                    break
                await asyncio.sleep(.01)
            assert (root / "crash-effect.marker").exists()
            assert runtime.llm_box.size == 1
        runtime.journal.flush(5)
        deadline = time.monotonic() + 10
        while getattr(runtime.host, "_managed", None) is None and time.monotonic() < deadline:
            await asyncio.sleep(.01)
        report = {
            "session_id": runtime.bus.session_id,
            "request_id": delivery.event_id,
            "worker_pid": getattr(getattr(runtime.host, "_managed", None), "pid", None),
            "marker": (root / "crash-effect.marker").exists(),
            "mailbox_size": runtime.python_box.size if mode == "request" else runtime.llm_box.size,
            "journal": runtime.journal.recent(runtime.bus.session_id, 100),
        }
        ready_path.write_text(json.dumps(report), encoding="utf-8")
        while True:
            await asyncio.sleep(1)
    else:
        await asyncio.sleep(.25)
        runtime.journal.flush(5)
        ready_path.write_text(json.dumps({
            "session_id": runtime.bus.session_id,
            "trace_types": [event.type for event in runtime.trace],
            "marker": (root / "crash-effect.marker").exists(),
            "journal": runtime.journal.recent(runtime.bus.session_id, 100),
        }), encoding="utf-8")
        await runtime.shutdown()
asyncio.run(main())
'''


def _wait_json(path: Path, proc: subprocess.Popen[str], timeout: float = 25) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            # Existence is not completeness: `write_text` creates the file
            # before the bytes land, so a fast reader can see an empty or
            # half-written file and fail to parse it. Retry instead of
            # treating that as a crashed child.
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                pass
        if proc.poll() is not None:
            # Only a genuinely finished child is an error; it may also have
            # exited after a successful write, so try one last read.
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                raise AssertionError(
                    f"child exited {proc.returncode}: {proc.stderr.read()}"
                ) from None
        time.sleep(.02)
    raise AssertionError(f"timed out waiting for child sentinel {path.name}")


def _kill_tree(proc: subprocess.Popen[str], worker_pid: int | None) -> None:
    was_running = proc.poll() is None
    tree_terminated = False
    if was_running:
        if os.name == "nt":
            result = subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                capture_output=True,
                check=False,
                timeout=10,
            )
            tree_terminated = result.returncode == 0
        else:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
                tree_terminated = True
            except ProcessLookupError:
                tree_terminated = True
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
    # Only use the recorded PID fallback when tree termination failed while
    # the Runtime parent was still live. A successful tree kill must not be
    # followed by a kill of a potentially recycled PID.
    if was_running and worker_pid and worker_pid != proc.pid and not tree_terminated:
        try:
            os.kill(worker_pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass


@pytest.mark.parametrize("fault", ["request", "terminal"])
def test_os_process_death_does_not_replay_journaled_work(workspace: Path, fault: str) -> None:
    ready = workspace / "child-ready.json"
    restart_ready = workspace / "restart-ready.json"
    child = subprocess.Popen(
        [sys.executable, "-c", _CHILD, str(workspace), fault, str(ready)],
        cwd=Path(__file__).resolve().parents[1], text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        **({"start_new_session": True} if os.name != "nt" else {}),
    )
    worker_pid = None
    try:
        before = _wait_json(ready, child)
        worker_pid = before["worker_pid"]
        assert before["session_id"]
        assert before["request_id"]
        assert before["worker_pid"] and before["worker_pid"] != child.pid
        assert before["mailbox_size"] == 1
        recorded = {event["type"]: event for event in before["journal"]["events"]}
        assert "python.requested" in recorded
        assert recorded["python.requested"]["id"] == before["request_id"]
        assert ("python.finished" in recorded) is (fault == "terminal")
        assert before["marker"] is (fault == "terminal")
        _kill_tree(child, worker_pid)
        worker_pid = None
        restart = subprocess.Popen(
            [sys.executable, "-c", _CHILD, str(workspace), "restart", str(restart_ready)],
            cwd=Path(__file__).resolve().parents[1], text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        **({"start_new_session": True} if os.name != "nt" else {}),
        )
        after = _wait_json(restart_ready, restart)
        assert after["session_id"] != before["session_id"]
        assert "python.requested" not in after["trace_types"]
        assert "python.finished" not in after["trace_types"]
        assert after["marker"] is (fault == "terminal")
    finally:
        _kill_tree(child, worker_pid)
        # The restart child exits normally after writing its sentinel.
        if "restart" in locals():
            try:
                restart.wait(timeout=10)
            except subprocess.TimeoutExpired:
                _kill_tree(restart, None)
        ready.unlink(missing_ok=True)
        restart_ready.unlink(missing_ok=True)
