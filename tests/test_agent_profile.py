"""Contract: an embedding application owns its own agent profile.

Nervipulsa currently hard-codes the system prompt in three places (transcript
size accounting, the projector snapshot, and the compaction floor). A host
application that is not a coding CLI — a chat participant, a support agent, a
scheduler — cannot state who it is, and cannot even omit the coding-assistant
identity the runtime asserts on its behalf.

These tests fix the public shape of the fix: one string, carried by the
Transcript so that size accounting, the model view and the compaction floor
cannot disagree.
"""

from __future__ import annotations

import asyncio
import json

from nervipulsa.config import Settings
from nervipulsa.events import SYSTEM_PROMPT, Event
from nervipulsa.llm import ContextProjector, LLMActor, Transcript
from nervipulsa.providers import ModelResponse, ScriptedBackend
from nervipulsa.runtime import Runtime

PROFILE = "You are a terse group-chat participant. Never claim to be human."


class _NoTools:
    def schemas(self, *, workspace: str | None = None):
        return []

    def get(self, name: str):
        return None


def _responder(request):
    return ModelResponse(content="ok", model="scripted")


def test_transcript_carries_the_system_prompt():
    transcript = Transcript(system_prompt=PROFILE)
    assert transcript.system_prompt == PROFILE
    assert transcript.snapshot()[0] == {"role": "system", "content": PROFILE}


def test_default_system_prompt_is_unchanged():
    transcript = Transcript()
    assert transcript.system_prompt == SYSTEM_PROMPT
    assert transcript.snapshot()[0]["content"] == SYSTEM_PROMPT


def test_size_accounting_matches_the_custom_prompt():
    transcript = Transcript(system_prompt=PROFILE)
    transcript.messages.append({"role": "user", "content": "hello"})
    transcript.event_ids.append("e1")
    transcript.event_types.append("user.message")
    # Exact accounting must follow the prompt actually sent, not the default.
    assert transcript.serialized_length() == len(
        json.dumps(transcript.snapshot(), ensure_ascii=False)
    )


def test_projector_snapshot_and_compaction_floor_use_the_same_prompt():
    transcript = Transcript(system_prompt=PROFILE)
    projector = ContextProjector(transcript)
    assert projector.snapshot()[0] == {"role": "system", "content": PROFILE}
    assert projector.view_length() == len(json.dumps(projector.snapshot(), ensure_ascii=False))
    # The floor is derived from the prompt; a long profile must not be measured
    # against the short default.
    assert projector.minimum_floor() >= len(
        json.dumps({"role": "system", "content": PROFILE}, ensure_ascii=False)
    )


def test_runtime_accepts_a_host_supplied_prompt(workspace):
    async def body() -> None:
        backend = ScriptedBackend(_responder)
        settings = Settings(model="scripted", workspace=str(workspace))
        runtime = Runtime(
            settings,
            backend,
            workspace,
            tool_catalog=_NoTools(),
            system_prompt=PROFILE,
        )
        await runtime.start()
        try:
            runtime.submit_text("ping")
            await runtime.wait_until_idle(timeout=5)
        finally:
            await runtime.shutdown()
        assert backend.requests, "the runtime did not reach the provider"
        system_messages = [
            message
            for message in backend.requests[0].messages
            if message.get("role") == "system"
        ]
        assert system_messages == [{"role": "system", "content": PROFILE}]
        assert PROFILE not in json.dumps(backend.requests[0].model or "")

    asyncio.run(body())


def test_runtime_rejects_a_non_string_prompt(workspace):
    settings = Settings(model="scripted", workspace=str(workspace))
    try:
        Runtime(
            settings,
            ScriptedBackend(_responder),
            workspace,
            tool_catalog=_NoTools(),
            system_prompt=42,  # type: ignore[arg-type]
        )
    except TypeError:
        return
    raise AssertionError("a non-string system prompt must be rejected at construction")


def test_actor_prompt_reaches_the_request(workspace):
    """LLMActor is constructible on its own; the prompt must travel with it."""

    async def body() -> None:
        backend = ScriptedBackend(_responder)
        from nervipulsa.events import Bus, Mailbox

        bus = Bus()
        box = Mailbox("llm")
        bus.register("user.message", box)
        bus.start()
        actor = LLMActor(
            bus_state=lambda: bus.state,
            bus_seq=lambda: bus.seq,
            inbox=box,
            backend=backend,
            emitter=bus.emitter("llm"),
            ui=bus.emitter("llm"),
            workspace=str(workspace),
            model="scripted",
            session_id=bus.session_id,
            tool_catalog=_NoTools(),
            system_prompt=PROFILE,
        )
        assert actor.transcript.system_prompt == PROFILE
        task = asyncio.create_task(actor.run())
        bus.call("test", "user.message", {"text": "ping"})
        for _ in range(200):
            if backend.requests:
                break
            await asyncio.sleep(0.01)
        actor.request_stop()
        # request_stop() only flags the loop and cancels the in-flight request;
        # a blocked mailbox wait is released by closing the mailbox (Runtime
        # shutdown does the same by cancelling its tasks).
        box.close()
        await asyncio.wait_for(task, timeout=5)
        bus.close()
        assert backend.requests[0].messages[0] == {"role": "system", "content": PROFILE}

    asyncio.run(body())
