"""Input→output correlation and the activation wait.

A host that submits an event needs to know which activation consumed it and
when that activation stopped running. That answer used to be a scan of
`actor.activations`; these tests pin the supported replacement.
"""
import asyncio

from nervipulsa.config import Settings
from nervipulsa.events import Event
from nervipulsa.llm import ACTIVATION_LEDGER_RETENTION
from nervipulsa.ports import ModelResponse, ProviderFailure
from nervipulsa.runtime import Runtime, TRACE_EVENT_RETENTION

class Backend:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    async def complete(self, request):
        self.requests.append(request)
        reply = self.responses.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


async def _runtime(tmp_path, *responses):
    runtime = Runtime(Settings(), Backend(*responses), tmp_path, python_host=False)
    await runtime.start()
    return runtime


def test_wait_returns_the_activation_that_consumed_the_event(tmp_path):
    async def run():
        runtime = await _runtime(tmp_path, ModelResponse(content="one"),
                                 ModelResponse(content="two"))
        try:
            first = runtime.submit_text("hello")
            activated = await runtime.wait_for_activation(
                input_event_id=first.event_id, timeout=5
            )
            assert activated is not None
            assert activated.status == "committed"
            assert first.event_id in activated.input_event_ids
            assert runtime.activation_for_event(first.event_id) is activated
            assert (activated.response.content if activated.response else None) == "one"

            second = runtime.submit_text("again")
            activated_again = await runtime.wait_for_activation(
                input_event_id=second.event_id, timeout=5
            )
            assert activated_again is not None
            assert activated_again is not activated
            assert second.event_id in activated_again.input_event_ids
        finally:
            await runtime.shutdown()

    asyncio.run(run())


def test_wait_times_out_without_consuming_the_input(tmp_path):
    async def run():
        runtime = await _runtime(tmp_path, ModelResponse(content="one"))
        try:
            unknown = await runtime.wait_for_activation(
                input_event_id="evt_99999999", timeout=0.05
            )
            assert unknown is None
            assert runtime.activation_for_event("evt_99999999") is None
        finally:
            await runtime.shutdown()

    asyncio.run(run())


def test_paused_activation_settles_the_wait(tmp_path):
    async def run():
        runtime = await _runtime(tmp_path, ProviderFailure("timeout", "provider stalled"))
        try:
            delivery = runtime.submit_text("hello")
            activated = await runtime.wait_for_activation(
                input_event_id=delivery.event_id, timeout=5
            )
            assert activated is not None
            assert activated.status == "paused"
            assert activated.error_kind == "timeout"
            assert runtime.actor.paused is activated
        finally:
            await runtime.shutdown()

    asyncio.run(run())


def test_retry_keeps_the_same_correlation(tmp_path):
    async def run():
        runtime = await _runtime(
            tmp_path,
            ProviderFailure("timeout", "provider stalled"),
            ModelResponse(content="recovered"),
        )
        try:
            delivery = runtime.submit_text("hello")
            paused = await runtime.wait_for_activation(
                input_event_id=delivery.event_id, timeout=5
            )
            assert paused is not None and paused.status == "paused"
            paused_version = paused.settle_version

            runtime.retry()
            committed = await runtime.wait_for_activation(
                input_event_id=delivery.event_id, timeout=5,
                after_version=paused_version,
            )
            assert committed is paused
            assert committed.status == "committed"
            assert committed.settle_version > paused_version
            assert runtime.activation_for_event(delivery.event_id) is committed
        finally:
            await runtime.shutdown()

    asyncio.run(run())


def test_settle_event_carries_the_correlation(tmp_path):
    async def run():
        runtime = await _runtime(tmp_path, ModelResponse(content="one"))
        try:
            delivery = runtime.submit_text("hello")
            activated = await runtime.wait_for_activation(
                input_event_id=delivery.event_id, timeout=5
            )
            await runtime.wait_until_idle(timeout=5)
            settled = [
                event for event in runtime.trace
                if event.type == "agent.activation_finished"
            ]
            assert settled, "no settle event was traced"
            payload = settled[-1].payload
            assert payload["activation_id"] == activated.id
            assert payload["input_event_ids"] == list(activated.input_event_ids)
            assert payload["status"] == "committed"
            assert payload["error_kind"] is None
            assert isinstance(payload["ended_at"], float)
        finally:
            await runtime.shutdown()

    asyncio.run(run())


def test_only_recent_request_views_stay_in_memory(tmp_path):
    async def run():
        runtime = await _runtime(
            tmp_path, *[ModelResponse(content=f"a{turn}") for turn in range(40)]
        )
        try:
            for turn in range(40):
                delivery = runtime.submit_text(f"m{turn}")
                settled = await runtime.wait_for_activation(
                    input_event_id=delivery.event_id, timeout=5
                )
                assert settled is not None and settled.status == "committed"

            activations = runtime.actor.activations
            kept = [activation for activation in activations if activation.snapshot is not None]
            assert len(kept) == 32, "the exact request views are not bounded"
            assert activations[0].snapshot is None
            assert activations[-1].snapshot is not None
            # Correlation outlives the view: the id stays the answer.
            assert activations[0].id
        finally:
            await runtime.shutdown()

    asyncio.run(run())


def test_paused_activation_keeps_its_request_view_for_retry(tmp_path):
    async def run():
        runtime = await _runtime(tmp_path, ProviderFailure("timeout", "stalled"))
        try:
            delivery = runtime.submit_text("hello")
            paused = await runtime.wait_for_activation(
                input_event_id=delivery.event_id, timeout=5
            )
            assert paused is not None and paused.status == "paused"
            assert paused.snapshot is not None, "a retry must replay the exact view"
        finally:
            await runtime.shutdown()

    asyncio.run(run())


def test_activation_ledger_and_indexes_expire_together(tmp_path):
    async def run():
        count = ACTIVATION_LEDGER_RETENTION + 10
        runtime = await _runtime(
            tmp_path, *[ModelResponse(content=f"a{turn}") for turn in range(count)]
        )
        try:
            first_event_id = None
            first_activation = None
            latest_event_id = None
            latest_activation = None
            for turn in range(count):
                delivery = runtime.submit_text(f"m{turn}")
                settled = await runtime.wait_for_activation(
                    input_event_id=delivery.event_id, timeout=5
                )
                assert settled is not None and settled.status == "committed"
                if turn == 0:
                    first_event_id = delivery.event_id
                    first_activation = settled
                latest_event_id = delivery.event_id
                latest_activation = settled

            actor = runtime.actor
            assert actor.activation_count == count
            assert len(actor.activations) <= ACTIVATION_LEDGER_RETENTION
            assert first_activation is not None and first_event_id is not None
            assert actor.activation_by_id(first_activation.id) is None
            assert actor.activation_for_event(first_event_id) is None
            assert latest_activation is not None and latest_event_id is not None
            assert actor.activation_by_id(latest_activation.id) is latest_activation
            assert actor.activation_for_event(latest_event_id) is latest_activation
            assert await runtime.wait_for_activation(
                input_event_id=first_event_id
            ) is None
            assert await runtime.wait_for_activation(
                activation_id=first_activation.id
            ) is None
        finally:
            await runtime.shutdown()

    asyncio.run(run())


def test_trace_window_keeps_cumulative_event_counts(tmp_path):
    runtime = Runtime(Settings(), Backend(), tmp_path, python_host=False)
    for sequence in range(1, TRACE_EVENT_RETENTION + 2):
        runtime._observe(
            Event(
                session_id=runtime.bus.session_id,
                id=f"evt_{sequence:08d}",
                seq=sequence,
                type="test.event",
                source="test",
                target="runtime",
                reply_to=None,
                payload={"sequence": sequence},
                accepted_at=float(sequence),
            )
        )

    assert len(runtime.trace) <= TRACE_EVENT_RETENTION
    assert runtime.trace[0].seq == 257
    assert runtime.trace[-1].seq == TRACE_EVENT_RETENTION + 1
    assert runtime.event_count() == TRACE_EVENT_RETENTION + 1
    assert runtime.event_count("test.event") == TRACE_EVENT_RETENTION + 1
    assert runtime.event_count("missing.event") == 0


def test_wait_requires_exactly_one_selector(tmp_path):
    async def run():
        runtime = await _runtime(tmp_path, ModelResponse(content="one"))
        try:
            for kwargs in ({}, {"input_event_id": "evt_00000001", "activation_id": "act_x"}):
                try:
                    await runtime.wait_for_activation(**kwargs, timeout=0.01)
                except TypeError as exc:
                    assert "exactly one" in str(exc)
                else:
                    raise AssertionError("ambiguous wait was accepted")
        finally:
            await runtime.shutdown()

    asyncio.run(run())
