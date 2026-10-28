"""Transcript restore: the model view survives a restart, safely.

What must hold:

* a new Runtime with the same stable session id sees the previous conversation,
* the live system prompt wins over the snapshot (a changed persona is not
  silently reverted),
* redelivered observations project as duplicates instead of being appended
  twice,
* only the transcript is restored - no mailbox, no namespace, no effects,
* a session with no snapshot restores nothing rather than inventing state.
"""
import asyncio
import json

from nervipulsa.config import Settings
from nervipulsa.ports import ModelResponse
from nervipulsa.runtime import Runtime


class Backend:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    async def complete(self, request):
        self.requests.append(request)
        return self.responses.pop(0)


async def until(predicate):
    for _ in range(500):
        if predicate():
            return
        await asyncio.sleep(.01)
    assert predicate(), "timed out"


def answer(text=""):
    """A response with usable content (empty responses pause as invalid)."""
    return ModelResponse(content=text or "ok", model="scripted")


async def run_turn(runtime, payload, backend):
    assert runtime.submit_observation(payload).accepted
    before = len(backend.requests)
    await until(lambda: len(backend.requests) == before)
    await runtime.wait_until_idle()


def test_restart_restores_previous_conversation(tmp_path):
    async def run():
        sid = "scope-chat-1-topic-7"
        backend = Backend(answer("a"), answer("b"), answer("c"))
        first = Runtime(Settings(), backend, tmp_path, python_host=False,
                        session_id=sid)
        await first.start()
        try:
            await run_turn(first, {"text": "第一条：我们在查内存问题"}, backend)
            await run_turn(first, {"text": "第二条：部署刚发布"}, backend)
            await until(lambda: first.journal.flush(timeout=2))
        finally:
            await first.shutdown()
        second = Runtime(Settings(), backend, tmp_path, python_host=False,
                         session_id=sid)
        await second.start()
        try:
            report = second.restore_transcript()
            assert report["restored"] is True, report
            assert report["restored_messages"] >= 4, report

            restored = json.dumps(second.actor.transcript.messages,
                                  ensure_ascii=False)
            assert "内存问题" in restored
            assert "部署刚发布" in restored

            await run_turn(second, {"text": "第三条：查到是缓存没上限"}, backend)
            request = backend.requests[-1]
            body = json.dumps(request.messages, ensure_ascii=False)
            # The new turn must carry its history, not start from zero.
            assert "内存问题" in body
            assert "部署刚发布" in body
            assert "缓存没上限" in body
        finally:
            await second.shutdown()

    asyncio.run(run())


def test_live_system_prompt_wins_over_snapshot(tmp_path):
    async def run():
        sid = "persona-scope"
        backend = Backend(answer("a"), answer("b"))
        first = Runtime(Settings(), backend, tmp_path, python_host=False,
                        system_prompt="OLD PROMPT", session_id=sid)
        await first.start()
        try:
            await run_turn(first, {"text": "hi"}, backend)
            await until(lambda: first.journal.flush(timeout=2))
        finally:
            await first.shutdown()

        second = Runtime(Settings(), backend, tmp_path, python_host=False,
                         system_prompt="NEW PROMPT", session_id=sid)
        await second.start()
        try:
            assert second.restore_transcript()["restored"] is True
            await run_turn(second, {"text": "again"}, backend)
            messages = backend.requests[-1].messages
            assert messages[0] == {"role": "system", "content": "NEW PROMPT"}
            # No stale system message from the snapshot may compete with it.
            assert sum(1 for m in messages if m.get("role") == "system") == 1
        finally:
            await second.shutdown()

    asyncio.run(run())


def test_redelivered_observation_is_not_appended_twice_on_turn_one(tmp_path):
    """The app's own cursor may lag the transcript; restore must absorb that."""
    async def run():
        sid = "redelivery-scope"
        backend = Backend(answer("a"), answer("b"), answer("c"))
        first = Runtime(Settings(), backend, tmp_path, python_host=False,
                        session_id=sid)
        await first.start()
        seen_event_id = None
        try:
            assert first.submit_observation({"text": "replayed message"}).accepted
            await until(lambda: len(backend.requests) == 1)
            await first.wait_until_idle()
            seen_event_id = first.actor.activations[0].input_event_ids[0]
            await until(lambda: first.journal.flush(timeout=2))
        finally:
            await first.shutdown()

        second = Runtime(Settings(), backend, tmp_path, python_host=False,
                         session_id=sid)
        await second.start()
        try:
            assert second.restore_transcript()["restored"] is True
            before_messages = len(second.actor.transcript.messages)
            # A brand-new Runtime would normally project this again; the
            # restored `seen` set must recognise it as a duplicate.
            assert seen_event_id in second.actor.transcript.seen
            duplicate = second.actor.transcript.project(
                _fake_event(seen_event_id, "replayed message"))
            assert duplicate["duplicate"] is True
            assert len(second.actor.transcript.messages) == before_messages
        finally:
            await second.shutdown()

    asyncio.run(run())


def _fake_event(event_id, text):
    from nervipulsa.events import Event

    return Event(
        session_id="s", id=event_id, seq=1, type="runtime.observation",
        source="app", target="llm", reply_to=None,
        payload={"text": text}, accepted_at=0.0,
    )


def test_no_snapshot_restores_nothing(tmp_path):
    async def run():
        backend = Backend(answer("a"))
        runtime = Runtime(Settings(), backend, tmp_path, python_host=False,
                          session_id="never-used")
        await runtime.start()
        try:
            report = runtime.restore_transcript()
            assert report["restored"] is False
            assert report["reason"] == "no_snapshot"
            assert runtime.actor.transcript.messages == []
        finally:
            await runtime.shutdown()

    asyncio.run(run())


def test_default_session_id_is_still_random(tmp_path):
    """Restore must not silently leak between unrelated callers."""
    async def run():
        backend = Backend(answer("a"), answer("b"))
        a = Runtime(Settings(), backend, tmp_path, python_host=False)
        b = Runtime(Settings(), backend, tmp_path, python_host=False)
        assert a.bus.session_id != b.bus.session_id
        await a.start()
        try:
            await run_turn(a, {"text": "private to a"}, backend)
            await until(lambda: a.journal.flush(timeout=2))
        finally:
            await a.shutdown()

        await b.start()
        try:
            # Different session id: b cannot see a's conversation.
            assert b.restore_transcript()["restored"] is False
            assert b.actor.transcript.messages == []
        finally:
            await b.shutdown()

    asyncio.run(run())
