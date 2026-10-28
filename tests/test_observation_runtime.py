import asyncio
import json
from collections import UserDict

from nervipulsa.config import Settings
from nervipulsa.events import MAX_EVENT_BYTES
from nervipulsa.ports import ModelResponse, ToolCall
from nervipulsa.runtime import Runtime


class Backend:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    async def complete(self, request):
        self.requests.append(request)
        return self.responses.pop(0)


class Tool:
    name = "no_action"

    def __init__(self, effects):
        self.effects = effects

    def validate(self, arguments, *, max_timeout):
        return None

    async def invoke(self, arguments, *, context, activation, call):
        self.effects.append("tool")
        return {"status": "accepted"}, "accepted"


class Catalog:
    def __init__(self, tool):
        self.tool = tool

    def schemas(self, *, workspace):
        return []

    def get(self, name):
        return self.tool if name == self.tool.name else None


async def until(predicate):
    for _ in range(500):
        if predicate():
            return
        await asyncio.sleep(.01)
    assert predicate(), "timed out"


def test_observation_projection_validation_and_batch(tmp_path):
    async def run():
        backend = Backend(ModelResponse(content="ok"))
        runtime = Runtime(Settings(), backend, tmp_path, python_host=False)
        await runtime.start()
        try:
            observation = UserDict({"message": {"text": "hello"}, "items": [1, True]})
            delivery = runtime.submit_observation(observation)
            assert delivery.accepted
            observation["message"]["text"] = "changed"
            assert runtime.submit_observation({"second": 2}).accepted
            for invalid in ([], {"bad": float("nan")}, {1: "key"}, {"x": object()}, {"x": "a" * MAX_EVENT_BYTES}):
                assert runtime.submit_observation(invalid).reason == "invalid_payload"
            assert await runtime.wait_until_idle()
            assert len(backend.requests) == 1
            messages = backend.requests[0].messages
            envelopes = [json.loads(m["content"]) for m in messages if m["role"] == "user" and m["content"].startswith('{"kind":"runtime_event"')]
            assert [e["type"] for e in envelopes] == ["runtime.observation", "runtime.observation"]
            assert envelopes[0]["event_id"] == delivery.event_id
            assert envelopes[0]["payload"]["message"]["text"] == "hello"
            assert runtime.actor.activations[0].context_metrics["input_events"][0]["projected"]
        finally:
            await runtime.shutdown()
    asyncio.run(run())


def test_observation_capacity_while_paused(tmp_path):
    async def run():
        backend = Backend(ModelResponse(content="ok"))
        async def hook(activation, response):
            raise RuntimeError("unavailable")
        runtime = Runtime(Settings(ordinary_limit=2), backend, tmp_path, python_host=False, response_commit_hook=hook)
        await runtime.start()
        try:
            assert runtime.submit_observation({"initial": 1}).accepted
            await until(lambda: runtime.actor.paused is not None)
            assert runtime.submit_observation({"held": 1}).accepted
            await until(lambda: len(runtime.actor.held) == 1)
            assert runtime.submit_observation({"held": 2}).accepted
            await until(lambda: len(runtime.actor.held) == 2)
            assert runtime.submit_observation({"held": 3}).reason == "capacity_exceeded"
            assert len(backend.requests) == 1
        finally:
            await runtime.shutdown()
    asyncio.run(run())


def test_hook_failure_retry_and_tool_order(tmp_path):
    async def run():
        effects = []
        attempts = []
        response = ModelResponse(content="visible", tool_calls=[ToolCall("call", "no_action", {})])
        backend = Backend(response)
        async def hook(activation, received):
            assert received is response
            assert not activation.response_recorded
            assert not effects
            attempts.append(activation.id)
            if len(attempts) == 1:
                raise RuntimeError("storage unavailable")
            effects.append("persisted")
        runtime = Runtime(Settings(), backend, tmp_path, python_host=False, tool_catalog=Catalog(Tool(effects)), response_commit_hook=hook)
        await runtime.start()
        try:
            assert runtime.submit_observation({"x": 1}).accepted
            await until(lambda: runtime.actor.paused is not None)
            assert runtime.status()["pause_reason"] == "response_commit_failed"
            assert not effects
            assert not any(e.type == "assistant.message" for e in runtime.trace)
            assert not any(m["role"] == "assistant" for m in runtime.actor.transcript.messages)
            assert runtime.retry().accepted
            assert await runtime.wait_until_idle()
            assert effects == ["persisted", "tool"]
            assert len(backend.requests) == 1
            assert len(set(attempts)) == 1
        finally:
            await runtime.shutdown()
    asyncio.run(run())


def test_empty_response_recovery_and_budget(tmp_path):
    async def run():
        backend = Backend(ModelResponse(), ModelResponse(content="ok"), ModelResponse(content="unexpected"))
        runtime = Runtime(Settings(max_activations=2), backend, tmp_path, python_host=False)
        await runtime.start()
        try:
            assert runtime.submit_text("hello").accepted
            await until(lambda: runtime.actor.paused is not None)
            assert runtime.status()["pause_reason"] == "response_invalid"
            assert runtime.submit_observation({"new": "information"}).accepted
            assert await runtime.wait_until_idle()
            assert any('"new":"information"' in m.get("content", "") for m in backend.requests[1].messages)
            assert runtime.submit_observation({"next": 1}).accepted
            assert await runtime.wait_until_idle()
            assert len(backend.requests) == 3
        finally:
            await runtime.shutdown()
    asyncio.run(run())


def test_observation_only_session_outlives_activation_budget(tmp_path):
    """A caller that never sends user.message must not be stranded by the cap."""
    async def run():
        total = 6
        backend = Backend(*(ModelResponse(content=f"turn {i}") for i in range(total)))
        runtime = Runtime(Settings(max_activations=2), backend, tmp_path, python_host=False)
        await runtime.start()
        try:
            for index in range(total):
                assert runtime.submit_observation({"seq": index}).accepted
                assert await runtime.wait_until_idle()
                assert runtime.actor.paused is None, (
                    f"observation {index} left the actor paused: "
                    f"{runtime.status()['pause_reason']}"
                )
            assert len(backend.requests) == total
            assert not any(u.type == "user.message" for u in runtime.trace)
        finally:
            await runtime.shutdown()
    asyncio.run(run())


def test_internal_events_still_consume_the_activation_budget(tmp_path):
    """Tool results and handler feedback must remain bounded.

    One external observation is allowed to start a turn. The internal events
    that follow it are what the budget is meant to cap, so they must not reset
    the counter the way a fresh observation does.
    """
    async def run():
        backend = Backend(ModelResponse(content="first"), ModelResponse(content="second"))
        runtime = Runtime(Settings(max_activations=1), backend, tmp_path, python_host=False)
        await runtime.start()
        try:
            assert runtime.submit_observation({"first": 1}).accepted
            assert await runtime.wait_until_idle()
            activation = runtime.actor.activations[0]
            assert runtime.actor.paused is None
            runtime.llm_emitter.call(
                "agent.feedback",
                {"activation_id": activation.id, "rejections": []},
            )
            await until(lambda: runtime.actor.paused is not None)
            assert runtime.status()["pause_reason"] == "budget"
            assert len(backend.requests) == 1
        finally:
            await runtime.shutdown()
    asyncio.run(run())
