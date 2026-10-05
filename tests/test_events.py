"""Kernel capacity, routing, FIFO, reserved terminals, and lifecycle."""

from __future__ import annotations

import asyncio

from nervipulsa.events import MAX_HANDLER_RESULTS_PER_EXECUTION, Bus, Lane, Mailbox, RuntimeState


def _finished(execution_id: str = "exec-1", *, expected_handler_count: int = 0) -> dict:
    return {
        "status": "succeeded",
        "stdout": "",
        "stderr": "",
        "duration_ms": 1,
        "worker_epoch": 1,
        "namespace_reset": False,
        "expected_handler_count": expected_handler_count,
    }


def test_created_running_and_unknown_route() -> None:
    async def body() -> None:
        bus = Bus()
        box = Mailbox("llm", ordinary_limit=2)
        bus.register("user.message", box)
        rejected = bus.call("cli", "user.message", {"text": "hi"})
        assert rejected.accepted is False
        assert rejected.reason == "receiver_unavailable"
        bus.start()
        missing = bus.call("cli", "no.such", {"text": "x"})
        assert missing.reason == "unknown_route"
        invalid = bus.call("cli", "user.message", {"text": ""})
        assert invalid.reason == "invalid_payload"
        ok = bus.call("cli", "user.message", {"text": "one"})
        assert ok.accepted and ok.event_id == "evt_00000001"
        payload = {"text": "two"}
        second = bus.call("cli", "user.message", payload)
        payload["text"] = "mutated"
        assert second.accepted
        third = bus.call("cli", "user.message", {"text": "three"})
        assert third.reason == "capacity_exceeded"
        batch = await box.take_batch(10)
        assert [event.payload["text"] for event in batch] == ["one", "two"]
        assert [event.seq for event in batch] == [1, 2]

    asyncio.run(body())


def test_reserved_results_survive_a_full_ordinary_lane() -> None:
    async def body() -> None:
        bus = Bus()
        box = Mailbox("llm", ordinary_limit=1, result_limit=2)
        bus.register("user.message", box)
        bus.register("python.finished", box)
        bus.start()
        assert bus.call("cli", "user.message", {"text": "fill"}).accepted
        assert bus.call("cli", "user.message", {"text": "nope"}).reason == "capacity_exceeded"
        assert box.reserve_terminal("exec-1")
        finished = bus.call(
            "python_host",
            "python.finished",
            _finished(),
            reply_to="exec-1",
            lane=Lane.RESERVED_RESULT,
            lane_key="exec-1",
        )
        assert finished.accepted
        assert box.reserve_terminal("exec-1") is False

    asyncio.run(body())


def test_routes_freeze_and_close_rejects_everything() -> None:
    async def body() -> None:
        bus = Bus()
        llm = Mailbox("llm")
        other = Mailbox("other")
        bus.register("user.message", llm)
        bus.register("python.finished", llm)
        bus.start()
        try:
            bus.register("assistant.message", other)
            raise AssertionError("route registration should freeze")
        except RuntimeError:
            pass
        bus.begin_draining()
        assert bus.state is RuntimeState.DRAINING
        assert bus.call("cli", "user.message", {"text": "late"}).reason == "runtime_closing"
        assert llm.reserve_terminal("exec-9")
        assert bus.call(
            "python_host",
            "python.finished",
            _finished("exec-9"),
            reply_to="exec-9",
            lane=Lane.RESERVED_RESULT,
            lane_key="exec-9",
        ).accepted
        bus.close()
        assert bus.call(
            "python_host",
            "python.finished",
            _finished("exec-9"),
            reply_to="exec-9",
            lane=Lane.RESERVED_RESULT,
            lane_key="exec-9",
        ).reason == "runtime_closing"

    asyncio.run(body())


def test_observer_failure_does_not_undo_delivery() -> None:
    async def body() -> None:
        bus = Bus()
        box = Mailbox("llm")
        bus.register("user.message", box)

        def explode(_event) -> None:
            raise RuntimeError("observer failed")

        bus.add_observer(explode)
        bus.start()
        delivery = bus.call("cli", "user.message", {"text": "kept"})
        assert delivery.accepted
        assert box.size == 1

    asyncio.run(body())



def test_handler_slots_survive_dequeue_until_consumed_and_terminal_closes_unused() -> None:
    async def body() -> None:
        bus = Bus()
        box = Mailbox("llm", ordinary_limit=1, result_limit=1, handler_result_limit=2)
        bus.register("python.requested", box)
        bus.register("python.finished", box)
        bus.register("agent.handler_fired", box)
        bus.register("user.message", box)
        bus.start()

        admitted = bus.call(
            "llm",
            "python.requested",
            {"code": "pass", "timeout": 1, "activation_id": "a", "tool_call_id": "t"},
            reserve_result_for="llm",
            reserve_handler_slots=2,
        )
        assert admitted.accepted
        assert box.handler_reserved_size == 2
        await box.take_batch(1)
        assert bus.call("cli", "user.message", {"text": "fills ordinary lane"}).accepted
        bad_reply = bus.call(
            "python_host",
            "agent.handler_fired",
            {
                "handler_id": "handler-bad",
                "trigger": {"request_id": admitted.event_id},
                "result": "bad correlation",
                "worker_epoch": 1,
            },
            reply_to="wrong-execution",
            lane=Lane.HANDLER_RESULT,
            lane_key=admitted.event_id,
        )
        assert bad_reply.reason == "invalid_payload"
        assert box.handler_reserved_size == 2

        fired = bus.call(
            "python_host",
            "agent.handler_fired",
            {
                "handler_id": "handler-1",
                "trigger": {"request_id": admitted.event_id},
                "result": "ok",
                "worker_epoch": 1,
            },
            reply_to=admitted.event_id,
            lane=Lane.HANDLER_RESULT,
            lane_key=admitted.event_id,
        )
        assert fired.accepted
        mixed_batch = box.drain_available(8)
        handler_event = next(event for event in mixed_batch if event.type == "agent.handler_fired")
        assert box.handler_reserved_size == 2
        box.mark_consumed(handler_event)
        assert box.handler_reserved_size == 1

        finished = bus.call(
            "python_host",
            "python.finished",
            _finished("finished", expected_handler_count=1),
            reply_to=admitted.event_id,
            lane=Lane.RESERVED_RESULT,
            lane_key=admitted.event_id,
        )
        assert finished.accepted
        terminal_batch = box.drain_available(8)
        terminal_event = next(event for event in terminal_batch if event.type == "python.finished")
        box.mark_consumed(terminal_event)
        assert box.handler_reserved_size == 0

        late = bus.call(
            "python_host",
            "agent.handler_fired",
            {
                "handler_id": "handler-2",
                "trigger": {"request_id": admitted.event_id},
                "result": "late",
                "worker_epoch": 1,
            },
            reply_to=admitted.event_id,
            lane=Lane.HANDLER_RESULT,
            lane_key=admitted.event_id,
        )
        assert late.reason == "capacity_exceeded"
        bus.close()

    asyncio.run(body())


def test_ordinary_fallback_for_handler_results_keeps_handler_accounting() -> None:
    async def body() -> None:
        bus = Bus()
        box = Mailbox("llm", result_limit=1, handler_result_limit=2)
        bus.register("python.requested", box)
        bus.register("python.finished", box)
        bus.register("agent.handler_fired", box)
        bus.start()
        admitted = bus.call(
            "llm",
            "python.requested",
            {"code": "pass", "timeout": 1, "activation_id": "a", "tool_call_id": "t"},
            reserve_result_for="llm",
            reserve_handler_slots=1,
        )
        assert admitted.accepted
        await box.take_batch(1)

        payload = {
            "handler_id": "handler-1",
            "trigger": {"request_id": admitted.event_id},
            "result": "fallback",
            "worker_epoch": 1,
        }
        rejected_lane = bus.call(
            "python_host",
            "agent.handler_fired",
            payload,
            reply_to=admitted.event_id,
            lane=Lane.HANDLER_RESULT,
            lane_key="wrong-key",
        )
        assert not rejected_lane.accepted
        fallback = bus.call(
            "python_host",
            "agent.handler_fired",
            payload,
            reply_to=admitted.event_id,
        )
        assert fallback.accepted
        assert box.handler_reserved_size == 1

        terminal = bus.call(
            "python_host",
            "python.finished",
            _finished("finished", expected_handler_count=1),
            reply_to=admitted.event_id,
            lane=Lane.RESERVED_RESULT,
            lane_key=admitted.event_id,
        )
        assert terminal.accepted
        for event in box.drain_available(8):
            box.mark_consumed(event)
        assert box.handler_reserved_size == 0

        late = bus.call(
            "python_host",
            "agent.handler_fired",
            payload,
            reply_to=admitted.event_id,
        )
        assert not late.accepted
        assert box.handler_reserved_size == 0
        bus.close()

    asyncio.run(body())
    async def body() -> None:
        bus = Bus()
        python_box = Mailbox("python_host", ordinary_limit=4)
        llm_box = Mailbox("llm", result_limit=2, handler_result_limit=8)
        bus.register("python.requested", python_box)
        bus.register("python.finished", llm_box)
        bus.start()

        first = bus.call(
            "llm",
            "python.requested",
            {"code": "pass", "timeout": 1, "activation_id": "a", "tool_call_id": "t1"},
            reserve_result_for="llm",
            reserve_handler_slots=MAX_HANDLER_RESULTS_PER_EXECUTION,
        )
        assert first.accepted
        before_second = bus.seq
        second = bus.call(
            "llm",
            "python.requested",
            {"code": "pass", "timeout": 1, "activation_id": "a", "tool_call_id": "t2"},
            reserve_result_for="llm",
            reserve_handler_slots=MAX_HANDLER_RESULTS_PER_EXECUTION,
        )
        assert not second.accepted
        assert second.reason == "capacity_exceeded"
        assert bus.seq == before_second
        assert llm_box.reserved_size == 1
        assert llm_box.handler_reserved_size == MAX_HANDLER_RESULTS_PER_EXECUTION
        assert not llm_box.reserve_handlers("oversized", MAX_HANDLER_RESULTS_PER_EXECUTION + 1)
        bus.close()

    asyncio.run(body())


def test_terminal_wrong_reply_key_does_not_settle_other_execution_slots() -> None:
    async def body() -> None:
        bus = Bus()
        box = Mailbox("llm", result_limit=2, handler_result_limit=2)
        bus.register("python.finished", box)
        bus.start()
        assert box.reserve_terminal("exec-1")
        assert box.reserve_handlers("exec-2", 2)

        rejected = bus.call(
            "python_host",
            "python.finished",
            _finished("exec-2", expected_handler_count=0),
            reply_to="exec-2",
            lane=Lane.RESERVED_RESULT,
            lane_key="exec-1",
        )
        assert not rejected.accepted
        assert rejected.reason == "invalid_payload"
        assert box.handler_reserved_size == 2
        assert box.reserved_size == 1

        bus.close()

    asyncio.run(body())


    async def body() -> None:
        bus = Bus()
        box = Mailbox("llm", ordinary_limit=1, result_limit=1)
        bus.register("user.message", box)
        bus.start()
        assert box.reserve_terminal("exec-1")
        assert bus.call("cli", "user.message", {"text": "still fits"}).accepted
        bus.close()

    asyncio.run(body())


def test_call_during_handling_stays_in_the_next_batch() -> None:
    async def body() -> None:
        bus = Bus()
        box = Mailbox("llm")
        bus.register("user.message", box)
        bus.start()
        bus.call("cli", "user.message", {"text": "first"})

        async def consume():
            batch = await box.take_batch(10)
            bus.call("cli", "user.message", {"text": "second"})
            return batch

        batch = await consume()
        assert [event.payload["text"] for event in batch] == ["first"]
        nxt = await box.take_batch(10)
        assert [event.payload["text"] for event in nxt] == ["second"]

    asyncio.run(body())
