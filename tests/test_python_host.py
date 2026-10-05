"""Real worker process: persistence, timeout, cancel, and reset."""

from __future__ import annotations

import asyncio
import ctypes
import os
import time
from ctypes import wintypes
from pathlib import Path

from nervipulsa.events import MAX_HANDLER_RESULTS_PER_EXECUTION, Bus, Event, Lane, Mailbox
from nervipulsa.python_host import PythonHost


def _alive(pid: int) -> bool:
    if os.name != "nt":
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(0x1000, False, pid)
    if not handle:
        return False
    code = wintypes.DWORD()
    kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
    kernel32.CloseHandle(handle)
    return code.value == 259


class HostRig:
    def __init__(self, workspace: Path) -> None:
        _BUFFERS.clear()
        self.workspace = workspace
        self.bus = Bus()
        self.llm = Mailbox("llm", result_limit=4)
        self.python_box = Mailbox("python_host", ordinary_limit=8)
        self.control = Mailbox("python_host.control", ordinary_limit=16)
        self.ui = Mailbox("ui", ordinary_limit=256)
        routes = {
            "python.requested": self.python_box,
            "python.cancel": self.control,
            "python.started": self.ui,
            "python.finished": self.llm,
            "agent.handler_fired": self.llm,
            "python.cancel_result": self.ui,
            "python.environment_changed": self.llm,
            "agent.error": self.ui,
        }
        for event_type, box in routes.items():
            self.bus.register(event_type, box)
        self.bus.start()
        emitter = self.bus.emitter("python_host")
        self.host = PythonHost(
            requests=self.python_box,
            control=self.control,
            ui=emitter,
            results=emitter,
            workspace=workspace,
            output_dir=workspace / "outputs",
            max_timeout=30,
            session_id=self.bus.session_id,
        )
        self.caller = self.bus.emitter("test")
        self.host.start()

    def request(self, code: str, timeout: float = 30):
        return self.caller.call(
            "python.requested",
            {"code": code, "timeout": timeout},
            reserve_result_for="llm",
            reserve_handler_slots=MAX_HANDLER_RESULTS_PER_EXECUTION,
        )

    def cancel(self, execution_id: str, reason: str = "stop"):
        return self.caller.call("python.cancel", {"execution_id": execution_id, "reason": reason})

    async def close(self) -> None:
        self.bus.begin_draining()
        await self.host.close()
        self.bus.close()


_BUFFERS: dict[int, list[Event]] = {}


async def _wait(box: Mailbox, event_type: str, *, reply_to: str | None = None, timeout: float = 10) -> Event:
    """Keep unmatched events. One drain can contain several terminals."""
    buffer = _BUFFERS.setdefault(id(box), [])
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        buffer.extend(box.drain_available(32))
        for index, event in enumerate(buffer):
            if event.type == event_type and (reply_to is None or event.reply_to == reply_to):
                match = buffer.pop(index)
                box.mark_consumed(match)
                return match
        await asyncio.sleep(0.02)
    seen = [f"{event.type}:{event.reply_to}" for event in buffer]
    raise AssertionError(f"timed out waiting for {event_type} reply_to={reply_to}; held {seen}")


def test_namespace_cwd_streams_and_immediate_accept(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        try:
            started = time.monotonic()
            first = rig.request("import time\ntime.sleep(0.3)\nprint('FIRST')")
            second = rig.request("print('SECOND')")
            assert time.monotonic() - started < 0.2
            assert first.accepted and second.accepted
            done1 = await _wait(rig.llm, "python.finished", reply_to=first.event_id)
            done2 = await _wait(rig.llm, "python.finished", reply_to=second.event_id)
            assert done1.payload["status"] == "succeeded"
            assert done2.payload["status"] == "succeeded"
            assert done1.payload["stdout"] == "FIRST\n"
            assert "SECOND" in done2.payload["stdout"]
            assigned = rig.request("value = 7")
            await _wait(rig.llm, "python.finished", reply_to=assigned.event_id)
            broken = rig.request("import sys\nprint('OUT')\nprint('ERR', file=sys.stderr)\nraise RuntimeError('boom')")
            failed = await _wait(rig.llm, "python.finished", reply_to=broken.event_id)
            assert failed.payload["status"] == "failed"
            assert failed.payload["namespace_reset"] is False
            assert "OUT" in failed.payload["stdout"]
            assert "ERR" in failed.payload["stderr"] and "boom" in failed.payload["stderr"]
            assert failed.payload["exception"] == "RuntimeError: boom"
            large_failure = rig.request("raise RuntimeError('x' * 10000)")
            large_failed = await _wait(rig.llm, "python.finished", reply_to=large_failure.event_id)
            assert large_failed.payload["status"] == "failed"
            assert len(large_failed.payload["exception"]) <= 512
            assert "exception text truncated" in large_failed.payload["exception"]
            still = rig.request("print(value)")
            kept = await _wait(rig.llm, "python.finished", reply_to=still.event_id)
            assert kept.payload["stdout"] == "7\n"
            (workspace / "sub").mkdir()
            moved = rig.request("import os\nos.chdir('sub')\nprint(os.getcwd())")
            await _wait(rig.llm, "python.finished", reply_to=moved.event_id)
            restored = rig.request("import os\nprint(os.path.basename(os.getcwd()))")
            back = await _wait(rig.llm, "python.finished", reply_to=restored.event_id)
            assert back.payload["stdout"].strip() == workspace.name
            child = rig.request(
                "import subprocess, sys\n"
                "subprocess.run([sys.executable, '-c', 'print(\"from-child\")'])\n"
            )
            captured = await _wait(rig.llm, "python.finished", reply_to=child.event_id)
            assert "from-child" in captured.payload["stdout"]
        finally:
            await rig.close()

    asyncio.run(body())


def test_shutdown_closes_mailbox_handler_budget(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        try:
            pending = rig.request("print('shutdown-budget')")
            assert pending.accepted
            assert rig.llm.handler_reserved_size == MAX_HANDLER_RESULTS_PER_EXECUTION
            await rig.close()
            assert rig.llm.handler_reserved_size == 0
            late = rig.bus.call(
                "python_host",
                "agent.handler_fired",
                {
                    "handler_id": "late-after-close",
                    "trigger": {"request_id": pending.event_id},
                    "result": "late",
                    "worker_epoch": rig.host.worker_epoch,
                },
                reply_to=pending.event_id,
                lane=Lane.HANDLER_RESULT,
                lane_key=pending.event_id,
            )
            assert late.reason == "runtime_closing"
            assert rig.llm.handler_reserved_size == 0
        finally:
            if rig.bus.state.value != "CLOSED":
                await rig.close()

    asyncio.run(body())


def test_timeout_and_cancel_kill_the_process_tree(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        try:
            spinning = rig.request("while True:\n    pass", timeout=0.4)
            timed = await _wait(rig.llm, "python.finished", reply_to=spinning.event_id, timeout=8)
            assert timed.payload["status"] == "timeout"
            assert timed.payload["namespace_reset"] is True
            assert rig.llm.handler_reserved_size == 0
            late = rig.bus.call(
                "python_host",
                "agent.handler_fired",
                {
                    "handler_id": "late-after-timeout",
                    "trigger": {"request_id": spinning.event_id},
                    "result": "late",
                    "worker_epoch": timed.payload["worker_epoch"],
                },
                reply_to=spinning.event_id,
                lane=Lane.HANDLER_RESULT,
                lane_key=spinning.event_id,
            )
            assert not late.accepted
            assert rig.llm.handler_reserved_size == 0
            follow = rig.request("print('after-timeout')")
            after = await _wait(rig.llm, "python.finished", reply_to=follow.event_id)
            assert after.payload["status"] == "succeeded"
            assert "after-timeout" in after.payload["stdout"]

            sleeping = rig.request("import time\ntime.sleep(30)\nprint('should-not')", timeout=30)
            await _wait(rig.ui, "python.started", reply_to=sleeping.event_id)
            queued = rig.request("print('QUEUED_MARKER')")
            assert queued.accepted
            rig.cancel(queued.event_id)
            queued_done = await _wait(rig.llm, "python.finished", reply_to=queued.event_id)
            assert queued_done.payload["status"] == "cancelled"
            assert queued_done.payload["namespace_reset"] is False

            running = rig.request(
                "import subprocess, sys, pathlib, time\n"
                "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])\n"
                "pathlib.Path('child.pid').write_text(str(child.pid))\n"
                "time.sleep(30)\n"
            )
            # The sleeper above is still the current execution; queue the tree test after it.
            rig.cancel(sleeping.event_id)
            stopped = await _wait(rig.llm, "python.finished", reply_to=sleeping.event_id, timeout=8)
            assert stopped.payload["status"] == "cancelled"
            assert stopped.payload["namespace_reset"] is True
            # Cancelling the running worker also drops the not-yet-started tree request.
            dropped = await _wait(rig.llm, "python.finished", reply_to=running.event_id, timeout=8)
            assert dropped.payload["status"] == "cancelled"
            assert dropped.payload["reason"] == "namespace_reset_before_start"
            assert rig.llm.handler_reserved_size == 0

            tree = rig.request(
                "import subprocess, sys, pathlib, time\n"
                "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])\n"
                "pathlib.Path('child.pid').write_text(str(child.pid))\n"
                "time.sleep(30)\n"
            )
            await _wait(rig.ui, "python.started", reply_to=tree.event_id)
            deadline = time.monotonic() + 5
            pid_path = workspace / "child.pid"
            while not pid_path.exists() and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            child_pid = int(pid_path.read_text(encoding="utf-8").strip())
            assert _alive(child_pid)
            rig.cancel(tree.event_id, reason="stop-tree")
            tree_done = await _wait(rig.llm, "python.finished", reply_to=tree.event_id, timeout=8)
            assert tree_done.payload["status"] == "cancelled"
            await asyncio.sleep(0.3)
            assert _alive(child_pid) is False
            assert rig.host.managed_pids() == [] or rig.host.worker_epoch >= 1
        finally:
            await rig.close()
            await asyncio.sleep(0.1)

    asyncio.run(body())


def test_worker_crash_resets_and_idle_death_notifies(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        try:
            os.environ["NERVIPULSA_API_KEY"] = "sekret-value"
            os.environ["OPENAI_API_KEY"] = "sekret-openai"
            hidden = rig.request("import os\nprint('NERVIPULSA' in os.environ.get('NERVIPULSA_API_KEY', ''))\nprint('OPENAI_API_KEY' in os.environ)")
            secret = await _wait(rig.llm, "python.finished", reply_to=hidden.event_id)
            assert secret.payload["stdout"] == "False\nFalse\n"
            rig.request("marker = 1")
            # The assignment shares the queue with nothing else; wait via a print.
            shown = rig.request("print('marker-set')")
            await _wait(rig.llm, "python.finished", reply_to=shown.event_id)
            crashed = rig.request("import os\nos._exit(3)")
            crash = await _wait(rig.llm, "python.finished", reply_to=crashed.event_id, timeout=8)
            assert crash.payload["status"] == "failed"
            assert crash.payload["namespace_reset"] is True
            gone = rig.request("print(marker)")
            missing = await _wait(rig.llm, "python.finished", reply_to=gone.event_id)
            assert missing.payload["status"] == "failed"
            assert "marker" in missing.payload["stderr"]

            alive = rig.request("print('idle-ok')")
            await _wait(rig.llm, "python.finished", reply_to=alive.event_id)
            assert rig.host._managed is not None
            rig.host._managed.terminate_tree()
            changed = await _wait(rig.llm, "python.environment_changed", timeout=8)
            assert changed.payload["new_epoch"] != changed.payload["old_epoch"]
            again = rig.request("print('restarted')")
            restarted = await _wait(rig.llm, "python.finished", reply_to=again.event_id)
            assert "restarted" in restarted.payload["stdout"]
            assert rig.llm.handler_reserved_size == 0

            before = rig.host.late_frames
            rig.host._frame_q.put(
                (
                    "frame",
                    rig.host.worker_epoch,
                    {"kind": "finished", "request_id": "stale", "status": "succeeded"},
                )
            )
            await asyncio.sleep(0.4)
            assert rig.host.late_frames > before
        finally:
            os.environ.pop("NERVIPULSA_API_KEY", None)
            os.environ.pop("OPENAI_API_KEY", None)
            await rig.close()

    asyncio.run(body())



def test_finished_handler_bridge(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        try:
            registered = rig.request("on_finished(lambda event: 'observed:' + event['status'])")
            await _wait(rig.llm, "python.finished", reply_to=registered.event_id)
            trigger = rig.request("print('bridge-trigger')")
            await _wait(rig.llm, "python.finished", reply_to=trigger.event_id)
            fired = await _wait(rig.llm, "agent.handler_fired", timeout=5)
            if fired.payload["trigger"]["request_id"] != trigger.event_id:
                fired = await _wait(rig.llm, "agent.handler_fired", timeout=5)
            assert fired.payload["trigger"]["request_id"] == trigger.event_id
            assert fired.payload["result"] == "observed:succeeded"
            assert rig.host.late_frames == 0
        finally:
            await rig.close()

    asyncio.run(body())


def test_worker_enforces_handler_registration_limit(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        try:
            code = (
                "for index in range(16):\n"
                "    on_finished(lambda event, index=index: f'result-{index}')\n"
                "try:\n"
                "    on_finished(lambda event: 'overflow')\n"
                "except RuntimeError:\n"
                "    print('cap-enforced')\n"
            )
            request = rig.request(code)
            finished = await _wait(rig.llm, "python.finished", reply_to=request.event_id)
            assert "cap-enforced" in finished.payload["stdout"]
            assert finished.payload["expected_handler_count"] == MAX_HANDLER_RESULTS_PER_EXECUTION
            assert finished.payload["handler_result_status"] == "complete"
            assert finished.payload["missing_handler_ids"] == []
            events = [
                await _wait(rig.llm, "agent.handler_fired", reply_to=request.event_id)
                for _ in range(MAX_HANDLER_RESULTS_PER_EXECUTION)
            ]
            assert len({event.payload["handler_id"] for event in events}) == MAX_HANDLER_RESULTS_PER_EXECUTION
        finally:
            await rig.close()

    asyncio.run(body())


def test_off_finished_and_callback_snapshot_mutations_apply_later(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        try:
            setup = rig.request(
                "state = {}\n"
                "def rotate(event):\n"
                "    off_finished(state['active'])\n"
                "    state['next'] = on_finished(lambda event: 'next-result')\n"
                "    return 'current-result'\n"
                "state['active'] = on_finished(rotate)\n"
            )
            first_terminal = await _wait(rig.llm, "python.finished", reply_to=setup.event_id)
            assert first_terminal.payload["expected_handler_count"] == 1
            first = await _wait(rig.llm, "agent.handler_fired", reply_to=setup.event_id)
            assert first.payload["result"] == "current-result"

            trigger = rig.request("print('later-snapshot')")
            second_terminal = await _wait(rig.llm, "python.finished", reply_to=trigger.event_id)
            assert second_terminal.payload["expected_handler_count"] == 1
            second = await _wait(rig.llm, "agent.handler_fired", reply_to=trigger.event_id)
            assert second.payload["result"] == "next-result"

            removed = rig.request("assert off_finished(state['next']) is True")
            removed_terminal = await _wait(rig.llm, "python.finished", reply_to=removed.event_id)
            assert removed_terminal.payload["expected_handler_count"] == 0
            assert removed_terminal.payload["handler_result_status"] == "complete"
            assert removed_terminal.payload["missing_handler_ids"] == []
        finally:
            await rig.close()

    asyncio.run(body())


def test_output_streams_and_artifacts(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        try:
            big = rig.request(
                "import sys\nprint('A' * 100000)\nprint('B' * 120000, file=sys.stderr)"
            )
            finished = await _wait(rig.llm, "python.finished", reply_to=big.event_id, timeout=8)
            assert finished.payload["status"] == "succeeded"
            assert finished.payload["truncated"] is True
            assert finished.payload["stdout_bytes"] == 100001
            assert finished.payload["stderr_bytes"] == 120001
            assert len(finished.payload["stdout"].encode("utf-8")) <= 4 * 1024
            assert len(finished.payload["stderr"].encode("utf-8")) <= 4 * 1024
            stdout_artifact = Path(str(finished.payload["stdout_artifact_path"]))
            stderr_artifact = Path(str(finished.payload["stderr_artifact_path"]))
            assert stdout_artifact.exists()
            assert stderr_artifact.exists()
            assert stdout_artifact.stat().st_size == 100001
            assert stderr_artifact.stat().st_size == 120001
            nxt = rig.request("print('still-alive')")
            after = await _wait(rig.llm, "python.finished", reply_to=nxt.event_id)
            assert "still-alive" in after.payload["stdout"]
        finally:
            await rig.close()

    asyncio.run(body())


def test_handler_registry_resets_at_worker_epoch_boundary(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        try:
            first = rig.request("on_finished(lambda event: 'old:' + event['status'])")
            await _wait(rig.llm, "python.finished", reply_to=first.event_id)
            old_fired = await _wait(rig.llm, "agent.handler_fired", timeout=5)
            assert old_fired.payload["trigger"]["request_id"] == first.event_id
            old_epoch = rig.host.worker_epoch
            old_handler_id = next(iter(rig.host._handlers))
            assert rig.host._handlers[old_handler_id] == old_epoch

            timed_out = rig.request("while True:\n    pass", timeout=0.4)
            done = await _wait(rig.llm, "python.finished", reply_to=timed_out.event_id, timeout=8)
            assert done.payload["namespace_reset"] is True
            deadline = time.monotonic() + 5
            while rig.host.worker_epoch == old_epoch and time.monotonic() < deadline:
                await asyncio.sleep(0.02)
            assert rig.host.worker_epoch != old_epoch
            assert rig.host._handlers == {}

            rig.host._route_handler_frame(
                {
                    "kind": "handler.fired",
                    "handler_id": old_handler_id,
                    "trigger": {"request_id": "stale"},
                    "result": "must be rejected",
                },
                old_epoch,
            )
            await asyncio.sleep(0.05)
            assert rig.llm.drain_available(32) == []

            fresh = rig.request("on_finished(lambda event: 'new:' + event['status'])")
            await _wait(rig.llm, "python.finished", reply_to=fresh.event_id)
            registration_fired = await _wait(rig.llm, "agent.handler_fired", timeout=5)
            assert registration_fired.payload["handler_id"] != old_handler_id
            assert registration_fired.payload["trigger"]["request_id"] == fresh.event_id
            new_handler_id = registration_fired.payload["handler_id"]
            assert rig.host._handlers[new_handler_id] == rig.host.worker_epoch

            trigger = rig.request("print('new-epoch-trigger')")
            await _wait(rig.llm, "python.finished", reply_to=trigger.event_id)
            fired = await _wait(rig.llm, "agent.handler_fired", timeout=5)
            assert fired.payload["handler_id"] == new_handler_id
            assert fired.payload["trigger"]["request_id"] == trigger.event_id
            assert fired.payload["result"] == "new:succeeded"
        finally:
            await rig.close()

    asyncio.run(body())


def test_queued_request_waits_for_handler_frames(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        observed: list[Event] = []
        rig.bus.add_observer(observed.append)
        try:
            register = rig.request("on_finished(lambda event: (__import__('time').sleep(0.2), 'handled')[1])")
            await _wait(rig.llm, "python.finished", reply_to=register.event_id)
            await _wait(rig.llm, "agent.handler_fired", timeout=5)

            first = rig.request("print('first')")
            await _wait(rig.ui, "python.started", reply_to=first.event_id)
            second = rig.request("print('second')")
            assert second.accepted

            finished = await _wait(rig.llm, "python.finished", reply_to=first.event_id)
            fired = await _wait(rig.llm, "agent.handler_fired", timeout=5)
            assert fired.payload["trigger"]["request_id"] == first.event_id
            next_started = await _wait(rig.ui, "python.started", reply_to=second.event_id)
            assert finished.payload["status"] == "succeeded"

            lifecycle = [
                (event.type, event.reply_to, event.payload.get("trigger", {}).get("request_id"))
                for event in observed
                if (event.type == "python.finished" and event.reply_to == first.event_id)
                or (event.type == "agent.handler_fired" and event.payload.get("trigger", {}).get("request_id") == first.event_id)
                or (event.type == "python.started" and event.reply_to == second.event_id)
            ]
            assert [item[0] for item in lifecycle] == [
                "agent.handler_fired", "python.finished", "python.started"
            ]
            assert next_started.reply_to == second.event_id
        finally:
            await rig.close()

    asyncio.run(body())



def test_handler_timeout_discards_fired_frames_from_real_worker(workspace: Path) -> None:
    async def body() -> None:
        rig = HostRig(workspace)
        try:
            register = rig.request("on_finished(lambda event: (__import__('time').sleep(2), 'late')[1])")
            registered = await _wait(rig.llm, "python.finished", reply_to=register.event_id)
            assert registered.payload["expected_handler_count"] == 1
            assert registered.payload["handler_result_status"] == "complete"
            await _wait(rig.llm, "agent.handler_fired", reply_to=register.event_id)
            timed_out = rig.request("print('callback-will-time-out')", timeout=0.4)
            terminal = await _wait(rig.llm, "python.finished", reply_to=timed_out.event_id, timeout=8)
            assert terminal.payload["status"] == "timeout"
            assert terminal.payload["expected_handler_count"] == 1
            assert terminal.payload["handler_result_status"] == "incomplete"
            assert len(terminal.payload["missing_handler_ids"]) == 1
            assert rig.llm.handler_reserved_size == 0
            late = rig.bus.call(
                "python_host",
                "agent.handler_fired",
                {
                    "handler_id": "late-after-callback-timeout",
                    "trigger": {"request_id": timed_out.event_id},
                    "result": "late",
                    "worker_epoch": terminal.payload["worker_epoch"],
                },
                reply_to=timed_out.event_id,
                lane=Lane.HANDLER_RESULT,
                lane_key=timed_out.event_id,
            )
            assert not late.accepted
            assert rig.llm.handler_reserved_size == 0
            await asyncio.sleep(0.2)
            assert not any(event.type == "agent.handler_fired" for event in rig.llm.drain_available(32))
        finally:
            await rig.close()

    asyncio.run(body())
