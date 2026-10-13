"""Deterministic probes for effect uncertainty and handler-capacity failures."""
from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from pathlib import Path

import pytest

import nervipulsa.python_host as python_host_module
from nervipulsa.config import Settings
from nervipulsa.events import MAX_HANDLER_RESULTS_PER_EXECUTION, Bus, Event, Lane, Mailbox
from nervipulsa.providers import ScriptedBackend, text_response, tool_response
from nervipulsa.python_host import PythonHost
from nervipulsa.runtime import Runtime


class FaultRig:
    def __init__(self, workspace: Path, *, handler_result_limit: int = 16) -> None:
        self.bus = Bus()
        self.requests = Mailbox("python_host", ordinary_limit=8)
        self.control = Mailbox("python_host.control", ordinary_limit=16)
        self.llm = Mailbox("llm", result_limit=4, handler_result_limit=handler_result_limit)
        self.ui = Mailbox("ui", ordinary_limit=64)
        routes = {
            "python.requested": self.requests,
            "python.cancel": self.control,
            "python.started": self.ui,
            "python.finished": self.llm,
            "agent.handler_fired": self.llm,
            "python.cancel_result": self.ui,
            "agent.error": self.ui,
        }
        for event_type, mailbox in routes.items():
            self.bus.register(event_type, mailbox)
        self.bus.start()
        emitter = self.bus.emitter("python_host")
        self.host = PythonHost(
            requests=self.requests,
            control=self.control,
            ui=emitter,
            results=emitter,
            workspace=workspace,
            output_dir=workspace / "outputs",
            max_timeout=30,
            session_id=self.bus.session_id,
        )
        self.host.start()
        self.client = self.bus.emitter("fault-test")

    def request(self, code: str, *, timeout: float = 20):
        return self.client.call(
            "python.requested",
            {"code": code, "timeout": timeout},
            reserve_result_for="llm",
            reserve_handler_slots=MAX_HANDLER_RESULTS_PER_EXECUTION,
        )

    async def close(self) -> None:
        self.bus.begin_draining()
        await self.host.close()
        self.bus.close()


async def next_event(
    mailbox: Mailbox,
    event_type: str,
    *,
    reply_to: str | None = None,
    timeout: float = 15,
) -> Event:
    """Keep unrelated dequeued events leased until their own assertion consumes them."""
    held: list[Event] = getattr(mailbox, "_fault_semantics_stash", [])
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        held.extend(mailbox.drain_available(32))
        for index, event in enumerate(held):
            if event.type == event_type and (reply_to is None or event.reply_to == reply_to):
                result = held.pop(index)
                setattr(mailbox, "_fault_semantics_stash", held)
                mailbox.mark_consumed(result)
                return result
        setattr(mailbox, "_fault_semantics_stash", held)
        await asyncio.sleep(0.02)
    seen = [f"{event.type}:{event.reply_to}" for event in held]
    raise AssertionError(f"timed out waiting for {event_type} reply_to={reply_to}; held {seen}")


@pytest.mark.parametrize(
    ("write_effect_first", "effect_observed"),
    [(False, False), (True, True)],
    ids=["exit-before-effect", "effect-before-exit"],
)
def test_worker_exit_does_not_identify_external_effect(
    workspace: Path,
    write_effect_first: bool,
    effect_observed: bool,
) -> None:
    """The worker terminal can be identical while the external effect differs."""

    async def body() -> None:
        rig = FaultRig(workspace)
        marker = workspace / "external-effect.marker"
        try:
            if write_effect_first:
                code = (
                    "from pathlib import Path\n"
                    "with Path('external-effect.marker').open('a', encoding='utf-8') as output:\n"
                    "    output.write('effect\\n')\n"
                    "import os\n"
                    "os._exit(73)\n"
                )
            else:
                code = "import os\nos._exit(73)\n"
            request = rig.request(code)
            assert request.accepted
            terminal = await next_event(
                rig.llm, "python.finished", reply_to=request.event_id, timeout=20
            )
            assert terminal.payload["status"] == "failed"
            assert terminal.payload["reason"] == "worker_exit"
            assert terminal.payload["namespace_reset"] is True
            assert "effect_state" not in terminal.payload
            assert marker.exists() is effect_observed
            # Check for duplicate writes during a short, live-host observation window.
            await asyncio.sleep(0.15)
            if effect_observed:
                assert marker.read_text(encoding="utf-8") == "effect\n"
        finally:
            await rig.close()

    asyncio.run(body())


def test_frame_delivered_before_send_error_can_run_despite_prestart_terminal(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller-side send error after full delivery cannot prove non-execution."""
    async def body() -> None:
        rig = FaultRig(workspace)
        marker = workspace / "post-send-effect.marker"
        original_write_frame = python_host_module.write_frame
        injected = False
        try:
            def deliver_then_raise(sock, frame) -> None:
                nonlocal injected
                original_write_frame(sock, frame)
                if frame.get("kind") != "execute" or injected:
                    return
                injected = True
                deadline = time.monotonic() + 10
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                if not marker.exists():
                    raise AssertionError("worker did not execute the fully delivered frame")
                raise OSError("injected error after complete frame delivery")

            monkeypatch.setattr(python_host_module, "write_frame", deliver_then_raise)
            request = rig.request(
                "from pathlib import Path\n"
                "Path('post-send-effect.marker').write_text('ran', encoding='utf-8')\n"
            )
            assert request.accepted
            terminal = await next_event(
                rig.llm, "python.finished", reply_to=request.event_id, timeout=15
            )
            assert injected
            assert marker.read_text(encoding="utf-8") == "ran"
            assert terminal.payload["status"] == "cancelled"
            assert terminal.payload["reason"] == "namespace_reset_before_start"
            assert terminal.payload["namespace_reset"] is True
        finally:
            await rig.close()

    asyncio.run(body())


def test_closed_terminal_mailbox_records_undelivered_result(workspace: Path) -> None:
    async def body() -> None:
        rig = FaultRig(workspace)
        closed = False
        try:
            request = rig.request("import time\ntime.sleep(0.15)\nprint('done')")
            assert request.accepted
            await next_event(rig.ui, "python.started", reply_to=request.event_id)
            rig.llm.close()
            closed = True

            error = await next_event(rig.ui, "agent.error", timeout=15)
            assert error.payload["kind"] == "result_undelivered"
            assert request.event_id in error.payload["message"]
            assert rig.host.undelivered_terminals == 1
            assert rig.llm.size == 0
            assert rig.llm.reserved_size == 0
            assert rig.llm.handler_reserved_size == 0
        finally:
            if not closed:
                await rig.close()
            else:
                rig.bus.begin_draining()
                await rig.host.close()
                rig.bus.close()

    asyncio.run(body())


def test_accepted_request_waiting_before_host_dequeue_is_not_replayed(
    workspace: Path,
) -> None:
    async def body() -> None:
        code = (
            "from pathlib import Path\n"
            "Path('never-dispatched.marker').write_text('ran', encoding='utf-8')"
        )
        first_backend = ScriptedBackend(lambda _request: tool_response(("lost", code, None)))
        first = Runtime(Settings(), first_backend, workspace)
        dequeue_gate = asyncio.Event()
        original_take_batch = first.python_box.take_batch

        async def hold_before_dequeue(limit: int):
            await dequeue_gate.wait()
            return await original_take_batch(limit)

        first.python_box.take_batch = hold_before_dequeue
        await first.start()
        try:
            assert first.submit_text("run the Python request").accepted
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                accepted = any(event.type == "python.requested" for event in first.trace)
                if accepted and first.python_box.size == 1 and not first.actor.busy:
                    break
                await asyncio.sleep(0.01)
            assert any(event.type == "python.requested" for event in first.trace)
            assert first.python_box.size == 1
            assert first.host.queue_size == 0
            assert first.host.current_execution_id is None
            assert not (workspace / "never-dispatched.marker").exists()
        finally:
            await first.shutdown()

        second_backend = ScriptedBackend(lambda _request: text_response("fresh session"))
        second = Runtime(Settings(), second_backend, workspace)
        await second.start()
        try:
            assert second.submit_text("start after restart").accepted
            assert await second.wait_until_idle(timeout=10)
            assert not any(event.type == "python.requested" for event in second.trace)
            assert not (workspace / "never-dispatched.marker").exists()
        finally:
            await second.shutdown()

    asyncio.run(body())


def test_queued_terminal_is_not_replayed_after_runtime_restart(workspace: Path) -> None:
    async def body() -> None:
        runtime = Runtime(
            Settings(), ScriptedBackend(lambda _request: text_response("unused")), workspace
        )
        actor_gate = asyncio.Event()

        async def hold_actor_batch(_limit: int):
            await actor_gate.wait()
            return []

        runtime.llm_box.take_batch = hold_actor_batch
        await runtime.start()
        marker = workspace / "terminal-effect.marker"
        try:
            request = runtime.host_emitter.call(
                "python.requested",
                {
                    "code": "from pathlib import Path\n"
                    "Path('terminal-effect.marker').write_text('done', encoding='utf-8')",
                    "timeout": 10,
                },
                reserve_result_for="llm",
                reserve_handler_slots=MAX_HANDLER_RESULTS_PER_EXECUTION,
            )
            assert request.accepted
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                terminal_observed = any(
                    event.type == "python.finished" for event in runtime.trace
                )
                if terminal_observed and runtime.llm_box.size == 1:
                    break
                await asyncio.sleep(0.01)
            assert terminal_observed
            assert runtime.llm_box.size == 1
            assert runtime.llm_box.reserved_size == 1
            assert marker.read_text(encoding="utf-8") == "done"
        finally:
            await runtime.shutdown()

        restarted = Runtime(
            Settings(), ScriptedBackend(lambda _request: text_response("fresh")), workspace
        )
        await restarted.start()
        try:
            assert restarted.submit_text("new runtime").accepted
            assert await restarted.wait_until_idle(timeout=10)
            assert not any(event.type == "python.finished" for event in restarted.trace)
            assert not any(
                message.get("role") == "user"
                and isinstance(message.get("content"), str)
                and "python.finished" in message["content"]
                for message in restarted.actor.transcript.messages
            )
        finally:
            await restarted.shutdown()

    asyncio.run(body())


def test_consumed_terminal_is_projected_before_interrupted_provider_call(
    workspace: Path,
) -> None:
    async def body() -> None:
        second_call_entered = asyncio.Event()
        release_provider = asyncio.Event()
        seen_terminal = False
        call_count = 0

        async def responder(request):
            nonlocal call_count, seen_terminal
            call_count += 1
            if call_count == 1:
                return tool_response(("complete", "print('finished once')", None))
            if call_count == 2:
                for message in request.messages:
                    content = message.get("content")
                    if not isinstance(content, str):
                        continue
                    try:
                        envelope = __import__("json").loads(content)
                    except (TypeError, ValueError):
                        continue
                    if (
                        isinstance(envelope, dict)
                        and envelope.get("kind") == "runtime_event"
                        and envelope.get("type") == "python.finished"
                    ):
                        seen_terminal = True
                second_call_entered.set()
                await release_provider.wait()
                return text_response("completed")
            return text_response("unexpected extra call")

        backend = ScriptedBackend(responder)
        runtime = Runtime(Settings(), backend, workspace)
        await runtime.start()
        try:
            assert runtime.submit_text("run and report").accepted
            await asyncio.wait_for(second_call_entered.wait(), timeout=15)
            assert seen_terminal
            assert runtime.llm_box.size == 0
            assert runtime.llm_box.reserved_size == 0
            assert any(event.type == "python.finished" for event in runtime.trace)
            # The terminal was consumed and projected, but the provider response
            # has not arrived. Shutdown cannot restore the in-memory transcript.
        finally:
            await runtime.shutdown()

        restart_backend = ScriptedBackend(lambda _request: text_response("new process"))
        restarted = Runtime(Settings(), restart_backend, workspace)
        await restarted.start()
        try:
            assert restarted.submit_text("new process turn").accepted
            assert await restarted.wait_until_idle(timeout=10)
            assert restart_backend.calls == 1
            assert not any(event.type == "python.finished" for event in restarted.trace)
            assert not any(
                message.get("role") == "user"
                and isinstance(message.get("content"), str)
                and "python.finished" in message["content"]
                for message in restarted.actor.transcript.messages
            )
        finally:
            await restarted.shutdown()

    asyncio.run(body())


def test_near_capacity_cancel_releases_handler_reservations(workspace: Path) -> None:
    async def body() -> None:
        rig = FaultRig(workspace, handler_result_limit=4)
        try:
            running = rig.request("import time\ntime.sleep(30)")
            assert running.accepted
            await next_event(rig.ui, "python.started", reply_to=running.event_id)
            assert rig.llm.handler_reserved_size == MAX_HANDLER_RESULTS_PER_EXECUTION

            blocked = rig.request("print('capacity-must-be-released')")
            assert not blocked.accepted
            assert blocked.reason == "capacity_exceeded"
            assert rig.llm.reserved_size == 1

            cancellation = rig.client.call(
                "python.cancel",
                {"execution_id": running.event_id, "reason": "fault-matrix-cancel"},
            )
            assert cancellation.accepted
            await next_event(rig.ui, "python.cancel_result", reply_to=cancellation.event_id)
            terminal = await next_event(rig.llm, "python.finished", reply_to=running.event_id)
            assert terminal.payload["status"] == "cancelled"
            assert terminal.payload["reason"] == "fault-matrix-cancel"
            assert terminal.payload["namespace_reset"] is True
            assert rig.llm.handler_reserved_size == 0
            assert rig.llm.reserved_size == 0

            recovered = rig.request("print('capacity-restored')")
            assert recovered.accepted
            recovered_terminal = await next_event(
                rig.llm, "python.finished", reply_to=recovered.event_id
            )
            assert recovered_terminal.payload["status"] == "succeeded"
            assert rig.llm.handler_reserved_size == 0
        finally:
            await rig.close()

    asyncio.run(body())


def test_near_capacity_timeout_releases_slots_and_rejects_late_handler(
    workspace: Path,
) -> None:
    async def body() -> None:
        rig = FaultRig(workspace, handler_result_limit=4)
        try:
            registration = rig.request(
                "on_finished(lambda event: (__import__('time').sleep(2), 'late')[1])"
            )
            registered = await next_event(
                rig.llm, "python.finished", reply_to=registration.event_id
            )
            assert registered.payload["expected_handler_count"] == 1
            await next_event(rig.llm, "agent.handler_fired", reply_to=registration.event_id)
            assert rig.llm.handler_reserved_size == 0

            timed = rig.request("print('timeout-trigger')", timeout=0.4)
            assert timed.accepted
            await next_event(rig.ui, "python.started", reply_to=timed.event_id)
            assert rig.llm.handler_reserved_size == MAX_HANDLER_RESULTS_PER_EXECUTION

            blocked = rig.request("print('capacity-must-be-released-after-timeout')")
            assert not blocked.accepted
            assert blocked.reason == "capacity_exceeded"
            assert rig.llm.reserved_size == 1

            terminal = await next_event(rig.llm, "python.finished", reply_to=timed.event_id)
            assert terminal.payload["status"] == "timeout"
            assert terminal.payload["namespace_reset"] is True
            assert terminal.payload["expected_handler_count"] == 1
            assert terminal.payload["handler_result_status"] == "incomplete"
            assert len(terminal.payload["missing_handler_ids"]) == 1
            assert rig.llm.handler_reserved_size == 0
            assert rig.llm.reserved_size == 0

            late = rig.client.call(
                "agent.handler_fired",
                {
                    "handler_id": "late00000001",
                    "trigger": {"request_id": timed.event_id},
                    "result": "late",
                    "worker_epoch": terminal.payload["worker_epoch"],
                },
                reply_to=timed.event_id,
                lane=Lane.HANDLER_RESULT,
                lane_key=timed.event_id,
            )
            assert not late.accepted
            assert late.reason == "capacity_exceeded"
            assert rig.llm.handler_reserved_size == 0

            recovered = rig.request("print('admission-restored-after-timeout')")
            assert recovered.accepted
            recovered_terminal = await next_event(
                rig.llm, "python.finished", reply_to=recovered.event_id
            )
            assert recovered_terminal.payload["status"] == "succeeded"
        finally:
            await rig.close()

    asyncio.run(body())


def test_host_close_clears_near_capacity_handler_reservations(workspace: Path) -> None:
    async def body() -> None:
        rig = FaultRig(workspace, handler_result_limit=4)
        closed = False
        try:
            running = rig.request("import time\ntime.sleep(30)")
            assert running.accepted
            await next_event(rig.ui, "python.started", reply_to=running.event_id)
            assert rig.llm.handler_reserved_size == MAX_HANDLER_RESULTS_PER_EXECUTION
            assert rig.llm.reserved_size == 1

            await rig.close()
            closed = True
            assert rig.llm.handler_reserved_size == 0
            assert rig.llm.reserved_size == 0

            late = rig.client.call(
                "agent.handler_fired",
                {
                    "handler_id": "late00000002",
                    "trigger": {"request_id": running.event_id},
                    "result": "after-close",
                    "worker_epoch": rig.host.worker_epoch,
                },
                reply_to=running.event_id,
                lane=Lane.HANDLER_RESULT,
                lane_key=running.event_id,
            )
            assert not late.accepted
            assert late.reason == "runtime_closing"
        finally:
            if not closed:
                await rig.close()

    asyncio.run(body())


def test_duplicate_handler_frame_is_delivered_once_and_counted_late(
    workspace: Path,
) -> None:
    async def body() -> None:
        rig = FaultRig(workspace, handler_result_limit=4)
        try:
            registration = rig.request("on_finished(lambda event: 'once')")
            await next_event(rig.llm, "python.finished", reply_to=registration.event_id)
            await next_event(rig.llm, "agent.handler_fired", reply_to=registration.event_id)
            assert rig.llm.handler_reserved_size == 0

            original_apply = rig.host._apply_result

            def apply_with_duplicate(record, result) -> None:
                frames = result.handler_fired or []
                if frames:
                    result = replace(result, handler_fired=[*frames, dict(frames[0])])
                original_apply(record, result)

            rig.host._apply_result = apply_with_duplicate
            late_before = rig.host.late_frames
            trigger = rig.request("print('duplicate-frame-trigger')")
            assert trigger.accepted
            terminal = await next_event(rig.llm, "python.finished", reply_to=trigger.event_id)
            fired = await next_event(
                rig.llm, "agent.handler_fired", reply_to=trigger.event_id
            )
            assert terminal.payload["handler_result_status"] == "complete"
            assert terminal.payload["expected_handler_count"] == 1
            assert fired.payload["trigger"]["request_id"] == trigger.event_id
            assert rig.host.late_frames == late_before + 1
            assert rig.llm.handler_reserved_size == 0
            assert rig.llm.size == 0
        finally:
            await rig.close()

    asyncio.run(body())
