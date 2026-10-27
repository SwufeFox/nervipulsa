"""Contract: an unusable model response must not stall the actor forever.

Why this exists (evidence from a real application, not a hypothetical):

A Telegram group agent submitted a follow-up question and got an empty
completion back — the provider spent its whole output budget on reasoning and
returned `content: null`. `LLMActor` correctly treated that as
`response_invalid` and paused. The defect was what happened *next*:

    {message, reaction} → held, not projected
    retry of the paused activation  → re-sent the *same* frozen snapshot
    new user.message                → also held, still frozen

So every later turn re-sent one stale request. In a 49-turn group run this
showed up as ten model calls with an identical `prompt_tokens` (1485) and six
byte-identical replies, while the transcript never grew past 1485 tokens.

(`nervipulsa-telegram-chatbot`, experiments/long_conversation.py, 2026-10-27.)

Two distinct bugs are covered here:

1. A pause of kind `response_invalid` answered a new user message by retrying a
   frozen snapshot. A new message is new information the snapshot does not
   contain, so the retry can only reproduce the old failure.
2. `paused` was exposed as *the* result even when it belonged to an earlier
   call, so a failed activation was reported as the answer to a fresh prompt.

The pause itself is still deliberate and is still honoured: an operator-driven
retry (`agent.retry`) keeps retrying the frozen snapshot, and
`response_unknown` (the request may have been delivered) is never auto-resumed.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from nervipulsa.config import Settings
from nervipulsa.ports import ModelResponse
from nervipulsa.providers import ScriptedBackend
from nervipulsa.runtime import Runtime


class _NoTools:
    def schemas(self, *, workspace: str | None = None) -> list[dict]:
        return []

    def get(self, name: str):
        return None


def _settings(workspace: Path) -> Settings:
    return Settings(model="scripted", workspace=str(workspace),
                    max_timeout=10, default_timeout=10)


def _run(coro):
    return asyncio.run(coro)


async def _settle(runtime: Runtime, *, expect: int = 1, timeout: float = 10.0) -> None:
    """Wait until the actor stops working, including stopping *on* a pause.

    `wait_until_idle()` deliberately returns False when the actor is paused, so
    it cannot be used to observe the state this suite is about. This waits for
    the absence of work instead: no running activation, nothing in flight.

    `expect` is the number of activations that must exist before "no running
    activation" means anything — otherwise the check passes at t=0, when the
    actor has not picked the message up yet and there is simply nothing there.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        started = len(runtime.actor.activations) >= expect
        running = any(a.status == "running" for a in runtime.actor.activations)
        if started and not running and not runtime.actor.busy:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(
        f"actor did not settle within {timeout}s: "
        f"{[(a.id, a.status) for a in runtime.actor.activations]}"
    )


def _texts(runtime: Runtime) -> list[str]:
    return [
        event.payload["text"]
        for event in runtime.trace
        if event.type == "assistant.message"
    ]


def _prompts(backend: ScriptedBackend) -> list[str]:
    """Every user line the model was actually shown, per call."""
    return [
        str((request.messages or [{}])[-1].get("content") or "")
        for request in backend.requests
    ]


def test_empty_response_does_not_freeze_the_conversation(workspace: Path) -> None:
    """The second question must reach the model, with its own text."""
    calls = {"n": 0}

    def responder(request):
        calls["n"] += 1
        if calls["n"] == 1:
            # What the provider returns when it spends the whole budget thinking.
            return ModelResponse(content=None, usage={"completion_tokens": 2030},
                                 model="scripted")
        return ModelResponse(content=f"answer {calls['n']}", model="scripted")

    async def body() -> None:
        backend = ScriptedBackend(responder)
        runtime = Runtime(_settings(workspace), backend, workspace,
                          tool_catalog=_NoTools(), python_host=False)
        await runtime.start()
        try:
            first = runtime.submit_text("first question")
            assert first.accepted
            await _settle(runtime)
            assert runtime.actor.paused is not None, "the empty reply should pause"
            assert runtime.actor.paused.error_kind == "response_invalid"

            second = runtime.submit_text("second question")
            assert second.accepted
            # `expect=2`: the paused activation already exists, so waiting for
            # "one settled activation" would return before the new one starts.
            await _settle(runtime, expect=2)

            prompts = _prompts(backend)
            # The decisive assertion: the follow-up was projected and sent.
            assert any("second question" in prompt for prompt in prompts), (
                f"the second message never reached the model; prompts={prompts!r}"
            )
            assert any("first question" in prompt for prompt in prompts)
            assert "answer 2" in _texts(runtime), _texts(runtime)
        finally:
            await runtime.shutdown()

    _run(body())


def test_a_new_message_creates_a_new_activation_not_a_retry(workspace: Path) -> None:
    """Recovery must be a fresh activation, so its snapshot includes the input."""
    state = {"n": 0}

    def responder(request):
        state["n"] += 1
        if state["n"] == 1:
            return ModelResponse(content=None, model="scripted")
        return ModelResponse(content="fine", model="scripted")

    async def body() -> None:
        backend = ScriptedBackend(responder)
        runtime = Runtime(_settings(workspace), backend, workspace,
                          tool_catalog=_NoTools(), python_host=False)
        await runtime.start()
        try:
            runtime.submit_text("alpha")
            await _settle(runtime)
            paused_id = runtime.actor.paused.id if runtime.actor.paused else None
            assert paused_id is not None

            runtime.submit_text("beta")
            await _settle(runtime, expect=2)

            activations = runtime.actor.activations
            assert len(activations) == 2, [(a.id, a.status) for a in activations]
            assert activations[1].id != paused_id
            assert activations[1].status == "committed"
            # The stale activation keeps its own terminal state; it is history,
            # not a result that can be handed to a later caller.
            assert runtime.actor.activations[0].status == "paused"
        finally:
            await runtime.shutdown()

    _run(body())


def test_an_operator_retry_still_retries_the_frozen_snapshot(workspace: Path) -> None:
    """An explicit retry keeps the original request; only new input resets it.

    This is the behaviour the fix must not break: after an empty reply the
    request that failed is still recoverable without re-deriving it, and a retry
    with no new user message must not invent a new snapshot.

    A separate, pre-existing defect is visible here and is asserted rather than
    hidden: `agent.retry` completes the paused activation *and* records a second
    activation for the same attempt (two rows for one retried request, the first
    keeping `error_kind="response_invalid"` although it finished). It does not
    lose or duplicate model output, so it is noted, not repaired, in this change.
    """
    state = {"n": 0}

    def responder(request):
        state["n"] += 1
        if state["n"] == 1:
            return ModelResponse(content=None, model="scripted")
        return ModelResponse(content="recovered", model="scripted")

    async def body() -> None:
        backend = ScriptedBackend(responder)
        runtime = Runtime(_settings(workspace), backend, workspace,
                          tool_catalog=_NoTools(), python_host=False)
        await runtime.start()
        try:
            runtime.submit_text("one question")
            await _settle(runtime)
            assert runtime.actor.paused is not None

            runtime.retry()
            await _settle(runtime, expect=2)

            # The retried request is the same prompt, by design.
            prompts = _prompts(backend)
            assert len(prompts) == 2
            assert prompts[0] == prompts[1]
            # Exactly one answer was produced and delivered.
            assert _texts(runtime) == ["recovered"]
            assert runtime.actor.paused is None
            assert len(runtime.actor.activations) == 2, [
                (a.id, a.status, a.error_kind) for a in runtime.actor.activations
            ]
        finally:
            await runtime.shutdown()

    _run(body())
