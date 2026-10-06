"""Scripted actor: batches, receipts, ledger, and pauses. No real provider."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path

from nervipulsa.cli import handle_line
from nervipulsa.config import Settings
from nervipulsa.events import Delivery, Event, Lane, Mailbox
from nervipulsa.llm import ContextProjector, LLMActor, Transcript
from nervipulsa.providers import (
    ModelResponse,
    ProviderError,
    ScriptedBackend,
    ToolCall,
    text_response,
    tool_response,
)
from nervipulsa.runtime import Runtime


def test_handler_wait_keeps_all_events_in_sequence_order() -> None:
    async def body() -> None:
        box = Mailbox("llm")
        actor = LLMActor.__new__(LLMActor)
        actor.inbox = box
        actor._held = []
        actor.batch_limit = 32

        finished = Event(
            "session", "finished", 10, "python.finished", "python_host", "llm",
            "request-1", {"status": "succeeded", "stdout": "ok", "stderr": ""}, 0.0,
        )
        user = Event(
            "session", "user", 11, "user.message", "cli", "llm",
            None, {"text": "next"}, 0.0,
        )
        matching = Event(
            "session", "matching", 12, "agent.handler_fired", "python_host", "llm",
            "request-1", {"handler_id": "h1", "trigger": {"request_id": "request-1"},
                           "result": "handler-result", "worker_epoch": 1}, 0.0,
        )
        assert box.reserve_handlers("request-1", 1)
        assert box.offer(user, Lane.ORDINARY).accepted

        async def publish_handler() -> None:
            await asyncio.sleep(0.005)
            box.offer(matching, Lane.ORDINARY)

        publish = asyncio.create_task(publish_handler())
        batch = await actor._coalesce_handler_results([finished])
        await publish
        # This batch becomes the activation input, preserving every received event.
        assert [(event.seq, event.type) for event in batch] == [
            (10, "python.finished"), (11, "user.message"), (12, "agent.handler_fired")
        ]
        assert actor._held == []
        # If the handler misses the bounded window, the finished event still
        # proceeds; a later handler remains available to a later activation.
        timed_out = await actor._coalesce_handler_results([finished])
        assert [event.id for event in timed_out] == ["finished"]
        late = Event(
            "session", "late", 13, "agent.handler_fired", "python_host", "llm",
            "request-1", {"handler_id": "h1", "trigger": {"request_id": "request-1"},
                           "result": "late-result", "worker_epoch": 1}, 0.0,
        )
        assert [event.id for event in await actor._coalesce_handler_results([late])] == ["late"]

    asyncio.run(body())


def test_counted_handler_coalescing_is_bounded_and_uses_initial_batch() -> None:
    async def body() -> None:
        class Inbox:
            def __init__(self) -> None:
                self.events: list[Event] = []
                self.calls = 0

            async def take_batch(self, _limit: int) -> list[Event]:
                self.calls += 1
                while not self.events:
                    await asyncio.sleep(0)
                events, self.events = self.events, []
                return events

        def finished(count: int) -> Event:
            return Event(
                "session", f"finished-{count}", 10, "python.finished", "host", "llm",
                "request", {"expected_handler_count": count}, 0.0,
            )

        def handler(seq: int, handler_id: str | None = None) -> Event:
            return Event(
                "session", f"handler-{seq}", seq, "agent.handler_fired", "host", "llm",
                None, {"handler_id": handler_id or f"h{seq}", "trigger": {"request_id": "request"}}, 0.0,
            )

        inbox = Inbox()
        actor = LLMActor.__new__(LLMActor)
        actor.inbox = inbox
        actor.batch_limit = 32

        # Results already in the first batch satisfy the count without reading inbox.
        first = await actor._coalesce_handler_results([finished(1), handler(11)])
        assert [event.seq for event in first] == [10, 11]
        assert inbox.calls == 0

        # A partial first batch waits for the remaining result and retains every event.
        async def publish() -> None:
            await asyncio.sleep(0.003)
            inbox.events.extend([handler(13), Event(
                "session", "interleaved", 12, "user.message", "cli", "llm",
                None, {"text": "during wait"}, 0.0,
            )])

        task = asyncio.create_task(publish())
        partial = await actor._coalesce_handler_results([finished(2), handler(11)])
        await task
        assert [event.seq for event in partial] == [10, 11, 12, 13]

        # Finished events found in an extra batch are registered before its
        # handler results are counted, and all events remain in seq order.
        nested = Event(
            "session", "nested-finished", 14, "python.finished", "host", "llm",
            "nested-request", {"expected_handler_count": 1}, 0.0,
        )
        nested_handler = Event(
            "session", "nested-handler", 15, "agent.handler_fired", "host", "llm",
            None, {"trigger": {"request_id": "nested-request"}}, 0.0,
        )
        inbox.events.extend([nested_handler, nested])
        nested_batch = await actor._coalesce_handler_results([finished(1)])
        assert [event.seq for event in nested_batch] == [10, 14, 15]

        # Duplicate handler IDs do not satisfy the expected count twice, even
        # though each event remains in the activation batch for audit.
        async def publish_duplicate_then_distinct() -> None:
            await asyncio.sleep(0.003)
            inbox.events.extend([
                handler(11, "same-id"),
                Event("session", "between", 12, "user.message", "cli", "llm", None, {"text": "during duplicate"}, 0.0),
            ])
            await asyncio.sleep(0.003)
            inbox.events.append(handler(13, "second-id"))

        dedup_task = asyncio.create_task(publish_duplicate_then_distinct())
        deduped = await actor._coalesce_handler_results([finished(2), handler(10, "same-id")])
        await dedup_task
        assert [event.seq for event in deduped] == [10, 10, 11, 12, 13]
        assert inbox.calls >= 2

        started = asyncio.get_running_loop().time()
        missing = await actor._coalesce_handler_results([finished(3)])
        elapsed = asyncio.get_running_loop().time() - started
        assert [event.id for event in missing] == ["finished-3"]
        assert 0.020 <= elapsed < 0.15

        # A declared zero has no inbox wait at all.
        before = inbox.calls
        zero = await actor._coalesce_handler_results([finished(0)])
        assert [event.id for event in zero] == ["finished-0"]
        assert inbox.calls == before

    asyncio.run(body())


    transcript = Transcript()
    events = [
        Event(
            "session", "finished", 10, "python.finished", "python_host", "llm",
            "request-1", {"status": "succeeded", "stdout": "ok", "stderr": ""}, 0.0,
        ),
        Event(
            "session", "handler", 12, "agent.handler_fired", "python_host", "llm",
            "request-1", {
                "handler_id": "h1", "trigger": {"request_id": "request-1"},
                "result": "opaque-handler-proof", "worker_epoch": 1,
            }, 0.0,
        ),
    ]

    for event in events:
        assert transcript.project(event)["projected"] is True

    projected = [json.loads(str(message["content"])) for message in transcript.messages]
    assert [item["type"] for item in projected] == ["python.finished", "agent.handler_fired"]
    assert projected[0]["payload"]["status"] == "succeeded"
    assert projected[1]["payload"]["result"] == "opaque-handler-proof"
    assert projected[1]["payload"]["trigger"]["request_id"] == "request-1"


def _user_texts(request) -> list[str]:
    texts = []
    for message in request.messages:
        if message.get("role") != "user":
            continue
        content = str(message.get("content") or "")
        if '"kind":"runtime_status"' in content:
            continue
        texts.append(content)
    return texts


def _runtime(workspace: Path, backend, **overrides) -> Runtime:
    settings = Settings(model="scripted", workspace=str(workspace), max_timeout=30, default_timeout=30)
    for key, value in overrides.items():
        setattr(settings, key, value)
    return Runtime(settings, backend, workspace, echo=lambda _text: None)


async def _until(predicate, timeout: float = 8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition was not met")



def test_incomplete_handler_status_reaches_provider_activation(workspace: Path) -> None:
    async def body() -> None:
        def respond(request):
            for message in request.messages:
                content = str(message.get("content") or "")
                if '"type":"python.finished"' not in content:
                    continue
                envelope = json.loads(content)
                payload = envelope.get("payload", {})
                if payload.get("handler_result_status") == "incomplete":
                    missing = payload.get("missing_handler_ids", [])
                    return text_response(f"Acknowledged missing handler: {missing[0]}")
            return text_response("no incomplete handler status")

        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace, backend)
        request_id = "execution-incomplete"
        try:
            await runtime.start()
            assert runtime.llm_box.reserve_terminal(request_id)
            terminal = runtime.bus.call(
                "python_host",
                "python.finished",
                {
                    "status": "timeout",
                    "stdout": "",
                    "stderr": "",
                    "duration_ms": 400,
                    "namespace_reset": True,
                    "expected_handler_count": 2,
                    "handler_result_status": "incomplete",
                    "missing_handler_ids": ["handler-missing-17"],
                    "worker_epoch": 4,
                },
                reply_to=request_id,
                lane=Lane.RESERVED_RESULT,
                lane_key=request_id,
            )
            assert terminal.accepted
            assert await runtime.wait_until_idle(8)

            # Assert the exact messages passed to the mock provider, not just the
            # journal, actor transcript, or activation ledger.
            payloads = [
                json.loads(str(message["content"]))
                for message in backend.requests[0].messages
                if message.get("role") == "user"
                and '"type":"python.finished"' in str(message.get("content"))
            ]
            assert len(payloads) == 1
            event_payload = payloads[0]["payload"]
            assert event_payload["handler_result_status"] == "incomplete"
            assert event_payload["missing_handler_ids"] == ["handler-missing-17"]
            assert any(
                message.get("content") == "Acknowledged missing handler: handler-missing-17"
                for message in runtime.actor.transcript.messages
            )
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_messages_batch_and_arrivals_during_inference_wait(workspace: Path) -> None:
    async def body() -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def respond(request):
            if backend.calls == 1:
                started.set()
                await release.wait()
                return text_response("first")
            return text_response("second")

        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace, backend)
        try:
            await runtime.start()
            await asyncio.sleep(0.05)
            assert backend.calls == 0
            runtime.submit_text("one")
            runtime.submit_text("two")
            runtime.submit_text("three")
            await started.wait()
            runtime.submit_text("during")
            release.set()
            assert await runtime.wait_until_idle(8)
            assert backend.calls == 2
            first_users = _user_texts(backend.requests[0])
            second_users = _user_texts(backend.requests[1])
            assert first_users == ["one", "two", "three"]
            assert second_users[-1] == "during"
            assert "during" not in first_users
            await asyncio.sleep(0.25)
            assert backend.calls == 2
            assert handle_line(runtime, "/status") is False
            assert handle_line(runtime, "/cancel") is False
            assert handle_line(runtime, "/nope") is False
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_python_exec_default_adapter_emits_request_and_correlated_terminal(workspace: Path) -> None:
    backend = ScriptedBackend(
        lambda request: tool_response(("exec-call", "print('default adapter')", None))
        if backend.calls == 1
        else text_response("result received")
    )
    runtime = _runtime(workspace, backend)

    async def body() -> None:
        try:
            await runtime.start()
            runtime.submit_text("run python")
            assert await runtime.wait_until_idle(8)
            requested = [event for event in runtime.trace if event.type == "python.requested"]
            finished = [event for event in runtime.trace if event.type == "python.finished"]
            assert len(requested) == len(finished) == 1
            request = requested[0]
            terminal = finished[0]
            assert request.payload == {
                "code": "print('default adapter')",
                "timeout": 30.0,
                "activation_id": runtime.actor.activations[0].id,
                "tool_call_id": "exec-call",
            }
            assert terminal.reply_to == request.id
            assert terminal.payload["stdout"] == "default adapter\n"
            receipt = next(
                json.loads(message["content"])
                for message in runtime.actor.transcript.messages
                if message.get("role") == "tool"
            )
            assert receipt == {"status": "accepted", "execution_id": request.id}
            assert runtime.llm_box.reserved_size == 0
            assert runtime.llm_box.handler_reserved_size == 0
        finally:
            await runtime.shutdown()

    asyncio.run(body())


    async def invalid() -> None:
        def respond(request):
            if any("runtime_event" in text for text in _user_texts(request)):
                return text_response("corrected")
            return ModelResponse(
                content="",
                tool_calls=[
                    ToolCall(id="bad-type", name="python_exec", arguments={"code": 1}),
                    ToolCall(id="bad-name", name="other", arguments={"code": "print('NO')"}),
                ],
            )

        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace / "invalid", backend)
        (workspace / "invalid").mkdir()
        try:
            await runtime.start()
            runtime.submit_text("go")
            assert await runtime.wait_until_idle(8)
            assert not any(event.type == "python.requested" for event in runtime.trace)
            assert backend.calls == 2
        finally:
            await runtime.shutdown()

    async def partial() -> None:
        def respond(request):
            if any("runtime_event" in text for text in _user_texts(request)):
                return text_response("corrected")
            return tool_response(("a", "print('ONCE')", None), ("b", "print('TWICE')", None))

        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace / "partial", backend, result_limit=1)
        (workspace / "partial").mkdir()
        try:
            await runtime.start()
            runtime.submit_text("go")
            assert await runtime.wait_until_idle(8)
            requested = [event for event in runtime.trace if event.type == "python.requested"]
            finished = [event for event in runtime.trace if event.type == "python.finished"]
            assert len(requested) == 1
            assert len(finished) == 1
            assert finished[0].payload["stdout"] == "ONCE\n"
            assert "TWICE" not in "".join(event.payload.get("stdout", "") for event in finished if isinstance(event.payload.get("stdout"), str))
        finally:
            await runtime.shutdown()

    asyncio.run(invalid())
    asyncio.run(partial())


def test_commit_interruption_does_not_repeat_an_accepted_call(workspace: Path) -> None:
    async def body() -> None:
        def respond(request):
            if any("runtime_event" in text for text in _user_texts(request)):
                return text_response("saw-result")
            return tool_response(("a", "print('A')", None), ("b", "print('B')", None))

        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace, backend)

        def boom(index, _call) -> None:
            if index == 0:
                raise RuntimeError("boom")

        runtime.actor.after_tool = boom
        try:
            await runtime.start()
            runtime.submit_text("go")
            await _until(lambda: runtime.actor.paused is not None and runtime.actor.paused.error_kind == "commit_interrupted")
            assert backend.calls == 1
            runtime.actor.after_tool = None
            assert runtime.retry().accepted
            assert await runtime.wait_until_idle(10)
            finished = [event for event in runtime.trace if event.type == "python.finished"]
            stdout = "".join(str(event.payload.get("stdout") or "") for event in finished)
            assert stdout.count("A") == 1
            assert stdout.count("B") == 1
            assert len([event for event in runtime.trace if event.type == "python.requested"]) == 2
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_unknown_response_is_not_retried_until_asked(workspace: Path) -> None:
    async def body() -> None:
        def respond(_request):
            if backend.calls == 1:
                raise ProviderError("response_unknown", "socket dropped")
            return text_response("resumed")

        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace, backend)
        try:
            await runtime.start()
            runtime.submit_text("hello")
            await _until(lambda: runtime.actor.paused is not None)
            assert runtime.actor.paused.error_kind == "response_unknown"
            runtime.submit_text("please continue")
            await asyncio.sleep(0.3)
            assert backend.calls == 1
            assert runtime.retry().accepted
            assert await runtime.wait_until_idle(8)
            assert backend.calls >= 2
            assert any(message.get("content") == "resumed" for message in runtime.actor.transcript.messages)
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_safe_retry_does_not_duplicate_the_projected_batch(workspace: Path) -> None:
    async def body() -> None:
        seen: list[list[str]] = []

        def respond(request):
            seen.append(_user_texts(request))
            if len(seen) == 1:
                raise ProviderError("request_not_sent", "missing API key")
            return text_response("ok")

        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace, backend)
        try:
            await runtime.start()
            runtime.submit_text("hello")
            await _until(lambda: runtime.actor.paused is not None)
            runtime.submit_text("again")
            assert await runtime.wait_until_idle(8)
            assert seen[0] == ["hello"]
            assert seen[1] == ["hello"]
            assert "again" in seen[2][-1]
            assert sum(text == "hello" for text in (message.get("content") for message in runtime.actor.transcript.messages)) == 1
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_budget_and_context_pause_without_dropping_history(workspace: Path) -> None:
    async def budget() -> None:
        def respond(request):
            last = _user_texts(request)[-1]
            if "runtime_event" not in last and "stop" in last:
                return text_response("stopped")
            return tool_response(("loop", "print('x')", None))

        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace / "budget", backend, max_activations=2)
        (workspace / "budget").mkdir()
        try:
            await runtime.start()
            runtime.submit_text("loop")
            await _until(lambda: runtime.actor.paused is not None and runtime.actor.paused.error_kind == "budget", timeout=10)
            assert backend.calls == 2
            runtime.submit_text("stop")
            assert await runtime.wait_until_idle(8)
            assert backend.calls == 3
        finally:
            await runtime.shutdown()

    async def context() -> None:
        backend = ScriptedBackend(lambda _request: text_response("nope"))
        runtime = _runtime(workspace / "context", backend, context_limit=40)
        (workspace / "context").mkdir()
        try:
            await runtime.start()
            runtime.submit_text("hello")
            await _until(lambda: runtime.actor.paused is not None)
            activation = runtime.actor.paused
            assert activation is not None
            assert activation.error_kind == "context"
            assert backend.calls == 0
            assert any(message.get("content") == "hello" for message in runtime.actor.transcript.messages)
            metrics = activation.context_metrics
            actual = metrics["transcript_chars_after_compaction"]
            assert actual > 40
            assert metrics["configured_limit_chars"] == 40
            assert metrics["input_added_chars"] > 0
            assert metrics["input_events"][0]["added_chars"] > 0
            assert f"{actual:,} chars" in str(activation.error_message)
            assert "limit 40 chars" in str(activation.error_message)
            recent = runtime.logs()
            stored = json.loads(recent["activations"][0]["context_json"])
            assert stored["input_added_chars"] == metrics["input_added_chars"]
            assert json.loads(recent["activations"][0]["usage_json"] or "null") is None
        finally:
            await runtime.shutdown()

    asyncio.run(budget())
    asyncio.run(context())




def test_python_environment_runtime_error_becomes_failed_event(workspace: Path, monkeypatch) -> None:
    import nervipulsa.tools as tools_module

    def explode(*_args, **_kwargs):
        raise RuntimeError("synthetic path resolver failure")

    monkeypatch.setattr(tools_module, "discover_python_environment", explode)
    responses = [
        ModelResponse(tool_calls=[ToolCall(id="env-fail", name="python_environment", arguments={})]),
        text_response("Discovery failed safely."),
    ]
    backend = ScriptedBackend(lambda _request: responses.pop(0))
    runtime = _runtime(workspace, backend)

    async def body() -> None:
        try:
            await runtime.start()
            runtime.submit_text("Inspect the Python environment")
            assert await runtime.wait_until_idle(8)
            event_payloads = [
                json.loads(str(message["content"]))["payload"]
                for message in backend.requests[1].messages
                if message.get("role") == "user"
                and '"type":"python.environment_discovered"' in str(message.get("content"))
            ]
            assert len(event_payloads) == 1
            assert event_payloads[0]["status"] == "failed"
            assert event_payloads[0]["error"] == "environment discovery failed (RuntimeError)"
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_python_environment_scan_does_not_block_actor_loop(workspace: Path, monkeypatch) -> None:
    import nervipulsa.tools as tools_module

    scan_started = threading.Event()
    release_scan = threading.Event()
    loop_progressed = asyncio.Event()

    def blocked_scan(*_args, **_kwargs):
        scan_started.set()
        if not release_scan.wait(timeout=5):
            raise RuntimeError("test scan release timed out")
        return {
            "read_only": True,
            "python": "3.13",
            "workspace": ".",
            "project_files": [],
            "modules": [],
            "install_supported": False,
        }

    monkeypatch.setattr(tools_module, "discover_python_environment", blocked_scan)
    backend = ScriptedBackend(
        lambda _request: ModelResponse(
            tool_calls=[ToolCall(id="env-blocked", name="python_environment", arguments={})]
        ) if backend.calls == 1 else text_response("scan completed")
    )
    runtime = _runtime(workspace, backend)

    async def body() -> None:
        async def prove_loop_progress() -> None:
            await asyncio.sleep(0.02)
            loop_progressed.set()

        try:
            await runtime.start()
            runtime.submit_text("Inspect the environment")
            assert await asyncio.to_thread(scan_started.wait, 5)
            progress_task = asyncio.create_task(prove_loop_progress())
            await asyncio.wait_for(loop_progressed.wait(), timeout=1)
            assert not release_scan.is_set()
            delivery = runtime.submit_text("follow-up during environment scan")
            assert delivery.accepted
            release_scan.set()
            assert await runtime.wait_until_idle(8)
            assert any(event.type == "python.environment_discovered" for event in runtime.trace)
            assert backend.calls == 2
            second_request = backend.requests[1]
            assert "follow-up during environment scan" in _user_texts(second_request)
            assert any(
                '"type":"python.environment_discovered"' in str(message.get("content"))
                for message in second_request.messages
                if message.get("role") == "user"
            )
            await progress_task
        finally:
            release_scan.set()
            await runtime.shutdown()

    asyncio.run(body())


def test_python_environment_tool_runs_before_smoke_test(workspace: Path) -> None:
    (workspace / "customlib.py").write_text('__version__ = "1.4"\nclass Widget: pass\n', encoding="utf-8")
    import sys
    sys.path.insert(0, str(workspace))

    async def body() -> None:
        responses = [
            ModelResponse(tool_calls=[ToolCall(
                id="env-1", name="python_environment",
                arguments={"modules": ["customlib"], "api_names": ["Widget"]},
            )]),
            text_response("Import and API verified; now smoke test."),
        ]
        backend = ScriptedBackend(lambda _request: responses.pop(0))
        runtime = _runtime(workspace, backend)
        try:
            await runtime.start()
            runtime.submit_text("Use unfamiliar customlib")
            assert await runtime.wait_until_idle(8)
            request = backend.requests[0]
            assert [tool["function"]["name"] for tool in request.tools] == ["python_exec", "python_environment"]
            receipts = [message for message in runtime.actor.transcript.messages if message.get("role") == "tool"]
            assert len(receipts) == 1
            receipt = json.loads(receipts[0]["content"])
            assert receipt["status"] == "accepted"
            event_messages = [
                json.loads(message["content"])
                for message in runtime.actor.transcript.messages
                if message.get("role") == "user" and '"python.environment_discovered"' in str(message.get("content"))
            ]
            assert len(event_messages) == 1
            result = event_messages[0]["payload"]
            assert event_messages[0]["reply_to"] == runtime.actor.activations[0].id
            assert result["modules"][0]["version"] == "1.4"
            assert result["modules"][0]["api"] == {"Widget": True}
            assert result["install_supported"] is False
        finally:
            await runtime.shutdown()

    try:
        asyncio.run(body())
    finally:
        sys.path.remove(str(workspace))
        sys.modules.pop("customlib", None)


def test_provider_usage_and_context_measurements_are_journaled(workspace: Path) -> None:
    async def body() -> None:
        usage = {"prompt_tokens": 41, "completion_tokens": 5, "total_tokens": 46}
        backend = ScriptedBackend(
            lambda _request: ModelResponse(content="ready", model="local-proxy-model", usage=usage)
        )
        runtime = _runtime(workspace, backend)
        try:
            await runtime.start()
            runtime.submit_text("record context")
            assert await runtime.wait_until_idle(8)
            activation = runtime.actor.activations[0]
            metrics = activation.context_metrics
            request = backend.requests[0]
            assert metrics["request_transcript_chars"] == len(
                json.dumps(request.messages, ensure_ascii=False)
            )
            assert metrics["provider_usage"] == usage
            assert metrics["attempts"][0]["usage"] == usage
            assert metrics["attempts"][0]["tool_schema_chars"] == len(
                json.dumps(request.tools, ensure_ascii=False)
            )
            recent = runtime.logs()
            stored = recent["activations"][0]
            assert json.loads(stored["usage_json"]) == usage
            assert json.loads(stored["context_json"])["provider_model"] == "local-proxy-model"
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_compaction_removes_only_complete_tool_interactions() -> None:
    transcript = Transcript()
    latest_ids: set[str] = set()
    sequence = 0
    for index in range(5):
        sequence += 1
        user = Event(
            session_id="test-session",
            id=f"user-{index}",
            seq=sequence,
            type="user.message",
            source="cli",
            target="llm",
            reply_to=None,
            payload={"text": f"task-{index}"},
            accepted_at=float(sequence),
        )
        transcript.project(user)
        call_id = f"call-{index}"
        execution_id = f"exec-{index}"
        transcript.add_assistant(tool_response((call_id, f"print('result-{index}')", None)))
        transcript.add_tool(call_id, {"status": "accepted", "execution_id": execution_id})
        sequence += 1
        result = Event(
            session_id="test-session",
            id=f"finished-{index}",
            seq=sequence,
            type="python.finished",
            source="python_host",
            target="llm",
            reply_to=execution_id,
            payload={
                "status": "succeeded",
                "duration_ms": 2,
                "stdout": f"result-{index}\n",
                "stderr": "",
                "stdout_bytes": len(f"result-{index}\n".encode()),
                "stderr_bytes": 0,
            },
            accepted_at=float(sequence),
        )
        transcript.project(result)
    sequence += 1
    pending_user = Event(
        session_id="test-session",
        id="pending-user",
        seq=sequence,
        type="user.message",
        source="cli",
        target="llm",
        reply_to=None,
        payload={"text": "execution still running"},
        accepted_at=float(sequence),
    )
    transcript.project(pending_user)
    transcript.add_assistant(tool_response(("pending-call", "print('pending')", None)))
    transcript.add_tool(
        "pending-call",
        {"status": "accepted", "execution_id": "exec-pending"},
    )
    current = Event(
        session_id="test-session",
        id="current-user",
        seq=sequence + 1,
        type="user.message",
        source="cli",
        target="llm",
        reply_to=None,
        payload={"text": "current requirement"},
        accepted_at=float(sequence + 1),
    )
    transcript.project(current)
    latest_ids.add(current.id)
    before = transcript.serialized_length()

    compacted = transcript.compact(current_event_ids=latest_ids, limit=before - 100)
    assert compacted["pending_tool_interactions_kept"] == 1

    assert compacted["completed_tool_interactions"] >= 1
    assert compacted["chars_after"] <= before - 100
    assert any(message.get("content") == "current requirement" for message in transcript.messages)
    assert any("do not replay it automatically" in str(message.get("content")) for message in transcript.messages)
    calls = [
        call["id"]
        for message in transcript.messages
        if message.get("role") == "assistant"
        for call in message.get("tool_calls", [])
    ]
    receipts = [
        message["tool_call_id"]
        for message in transcript.messages
        if message.get("role") == "tool"
    ]
    assert calls == receipts
    assert "pending-call" in calls
    assert "finished-0" in transcript.seen



def test_context_compaction_does_not_replay_completed_executions(workspace: Path) -> None:
    issued: list[str] = []

    def respond(request):
        if request.purpose == "context_summary":
            return text_response(
                "Earlier Python interactions already ran. Do not replay them. This is not current runtime status."
            )
        calls = [
            call["id"]
            for message in request.messages
            if message.get("role") == "assistant"
            for call in message.get("tool_calls", [])
        ]
        receipts = [
            item["tool_call_id"]
            for item in request.messages
            if item.get("role") == "tool"
        ]
        assert calls == receipts

        last_user = _user_texts(request)[-1]
        try:
            envelope = json.loads(last_user)
        except json.JSONDecodeError:
            envelope = {}
        if isinstance(envelope, dict) and envelope.get("type") == "python.finished":
            return text_response("execution result observed")

        index = len(issued)
        code = f"print('run-{index}')"
        issued.append(code)
        return tool_response((f"call-{index}", code, None))

    async def body() -> None:
        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace, backend)
        initial_chars = runtime.actor.transcript.serialized_length()
        runtime.actor.context_limit = initial_chars + 5000
        try:
            await runtime.start()
            for index in range(12):
                runtime.submit_text(f"run request {index}")
                await _until(
                    lambda: sum(event.type == "python.finished" for event in runtime.trace)
                    >= index + 1
                )
                assert await runtime.wait_until_idle(8)

            assert len(issued) == 12
            task_requests = [item for item in backend.requests if item.purpose != "context_summary"]
            assert len(task_requests) == 24
            assert any(item.purpose == "context_summary" for item in backend.requests)
            finished = [event for event in runtime.trace if event.type == "python.finished"]
            assert [event.payload["stdout"] for event in finished] == [
                f"run-{index}\n" for index in range(12)
            ]
            activations = runtime.logs(100)["activations"]
            contexts = [json.loads(item["context_json"]) for item in activations]
            assert any(
                (context.get("compaction") or {}).get("completed_tool_interactions", 0) > 0
                for context in contexts
            )
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_transcript_projection_accounts_for_duplicate_events() -> None:
    transcript = Transcript()
    event = Event(
        session_id="test-session",
        id="same-event",
        seq=1,
        type="user.message",
        source="cli",
        target="llm",
        reply_to=None,
        payload={"text": "same content"},
        accepted_at=1.0,
    )
    before = transcript.serialized_length()

    first = transcript.project(event)
    after_first = transcript.serialized_length()
    version = transcript.version
    duplicate = transcript.project(event)

    assert first["projected"] is True
    assert first["added_chars"] == after_first - before
    assert duplicate == {"projected": False, "duplicate": True, "added_chars": 0}
    assert transcript.version == version
    assert len(transcript.messages) == 1


def test_incremental_length_matches_a_full_serialization() -> None:
    """The cached accounting must stay bit-exact with a full json.dumps."""
    transcript = Transcript()
    projector = ContextProjector(
        transcript,
        lambda: {
            "worker_epoch": 7,
            "namespace": "current_epoch_only",
            "current_execution_id": None,
            "queued_execution_ids": [],
            "unfinished_execution_ids": [],
        },
    )
    seq = 0
    for step in range(40):
        seq += 1
        transcript.project(
            Event(
                session_id="s",
                id=f"u{seq}",
                seq=seq,
                type="user.message",
                source="cli",
                target="llm",
                reply_to=None,
                payload={"text": f"requirement {seq} with unicode 中文"},
                accepted_at=1.0,
            )
        )
        transcript.add_assistant(tool_response((f"c{seq}", f"print({seq})", None)))
        transcript.add_tool(f"c{seq}", {"status": "accepted", "execution_id": f"e{seq}"})
        transcript.project(
            Event(
                session_id="s",
                id=f"f{seq}",
                seq=seq,
                type="python.finished",
                source="python_host",
                target="llm",
                reply_to=f"e{seq}",
                payload={
                    "status": "succeeded",
                    "stdout": "x" * (step * 37),
                    "stderr": "",
                    "stdout_bytes": step * 37,
                    "stderr_bytes": 0,
                    "duration_ms": 1,
                    "worker_epoch": 7,
                    "namespace_reset": False,
                },
                accepted_at=1.0,
            )
        )
        assert transcript.serialized_length() == len(
            json.dumps(transcript.snapshot(), ensure_ascii=False)
        )
        assert projector.view_length() == len(
            json.dumps(projector.snapshot(), ensure_ascii=False)
        )

    # Removing and replacing must also keep the cache exact.
    transcript.messages.append({"role": "user", "content": "直接写入"})
    transcript.event_ids.append("raw")
    transcript.event_types.append("user.message")
    assert transcript.serialized_length() == len(
        json.dumps(transcript.snapshot(), ensure_ascii=False)
    )

    plan = {"remove_indices": [len(transcript.messages) - 3, len(transcript.messages) - 2], "insert_at": len(transcript.messages) - 3}
    summary = "x" * 20
    assert transcript.apply_model_summary(plan, summary)["status"] == "applied"
    assert transcript.serialized_length() == len(
        json.dumps(transcript.snapshot(), ensure_ascii=False)
    )
    assert projector.view_length() == len(
        json.dumps(projector.snapshot(), ensure_ascii=False)
    )


def test_shutdown_stops_a_running_worker_and_journal_omits_the_key(workspace: Path) -> None:
    async def body() -> None:
        def respond(request):
            if any("runtime_event" in text for text in _user_texts(request)):
                return text_response("done")
            return tool_response(("sleep", "import time\ntime.sleep(30)\n", 30))

        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace, backend)
        runtime.settings.api_key = "super-secret-key-zz"
        try:
            await runtime.start()
            runtime.submit_text("run")
            await _until(lambda: any(event.type == "python.started" for event in runtime.trace))
            pid = runtime.host.managed_pids()[0]
            await runtime.shutdown()
            assert runtime.host.managed_pids() == []
            _assert_dead(pid)
            blob = runtime.journal_path.read_bytes()
            assert b"super-secret-key-zz" not in blob
        finally:
            if runtime.bus.state.value != "CLOSED":
                await runtime.shutdown()

    asyncio.run(body())


def test_session_shutdown_event_closes_runtime_resources(workspace: Path) -> None:
    async def body() -> None:
        runtime = _runtime(workspace, ScriptedBackend(lambda _request: text_response("unused")))
        try:
            await runtime.start()
            await _until(
                lambda: bool(runtime.host.managed_pids()) and not runtime.host._starting,
                timeout=12,
            )
            pid = runtime.host.managed_pids()[0]
            delivery = runtime.user_emitter.call("session.shutdown", {})
            assert delivery.accepted
            await _until(
                lambda: runtime.bus.state.value == "CLOSED"
                and not runtime.host.managed_pids()
                and not runtime.journal._thread.is_alive(),
                timeout=12,
            )
            _assert_dead(pid)
        finally:
            managed = runtime.host._managed
            if managed is not None and managed.poll() is None:
                await asyncio.to_thread(runtime.host._destroy_worker_blocking)
            if runtime.bus.state.value != "CLOSED":
                runtime.bus.close()
            current = asyncio.current_task()
            pending = [task for task in runtime._tasks if task is not current and not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                try:
                    await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=5)
                except asyncio.TimeoutError:
                    pass
            if runtime.journal._thread.is_alive():
                runtime.journal.close(timeout=5)

    asyncio.run(body())


def _assert_dead(pid: int) -> None:
    import os

    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return
        code = wintypes.DWORD()
        kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
        kernel32.CloseHandle(handle)
        assert code.value != 259
        return
    try:
        os.kill(pid, 0)
    except OSError:
        return
    raise AssertionError(f"pid {pid} still alive")


def test_projector_does_not_hide_a_duplicate_append_or_a_tiny_budget() -> None:
    duplicate = Transcript()
    duplicate.event_ids.extend(["same", "same"])
    assert ContextProjector(duplicate).diagnose(context_limit=200_000) == "duplicate_append"

    projector = ContextProjector(
        Transcript(),
        lambda: {
            "worker_epoch": 2,
            "namespace": "current_epoch_only",
            "current_execution_id": None,
            "queued_execution_ids": [],
            "unfinished_execution_ids": [],
        },
    )
    assert projector.diagnose(context_limit=40) == "budget_config"
    snapshot = projector.snapshot()
    assert snapshot[0]["role"] == "system"
    assert snapshot[0]["content"] == "You are a helpful software engineer assistant."
    status = json.loads(snapshot[1]["content"])
    assert status["kind"] == "runtime_status"
    assert status["worker_epoch"] == 2
    assert status["unfinished_execution_ids"] == []
    assert "succeeded" not in status

    kept = Transcript()
    kept.messages.append({"role": "user", "content": "keep me"})
    refused = kept.apply_model_summary({"remove_indices": [0], "insert_at": 0}, "x" * 5000)
    assert refused["status"] == "kept_original"
    assert kept.messages == [{"role": "user", "content": "keep me"}]
    assert kept.archive == []


def test_cross_file_edit_survives_compression_and_a_new_requirement(workspace: Path) -> None:
    """Small budget, one real summary, and a requirement that arrives during it."""
    new_requirement = "Also set LABEL = 'ready' in right.py. Do not redo the earlier edit."
    left_code = (
        "from pathlib import Path\n"
        "Path('left.py').write_text('def value():\\n    return 2\\n', encoding='utf-8')\n"
        "print('LEFT-DONE ' + ('x' * 4000))\n"
    )
    right_code = (
        "import time\n"
        "from pathlib import Path\n"
        "time.sleep(0.4)\n"
        "Path('right.py').write_text(\n"
        "    'from left import value\\n\\n'\n"
        "    'def total():\\n'\n"
        "    '    return value()\\n',\n"
        "    encoding='utf-8',\n"
        ")\n"
        "print('RIGHT-DONE')\n"
    )
    label_code = (
        "from pathlib import Path\n"
        "path = Path('right.py')\n"
        "file_text = path.read_text(encoding='utf-8')\n"
        "if 'LABEL' not in file_text:\n"
        "    file_text += \"\\nLABEL = 'ready'\\n\"\n"
        "path.write_text(file_text, encoding='utf-8')\n"
        "print('LABEL-DONE')\n"
    )
    test_code = (
        "import test_app\n"
        "test_app.test_total()\n"
        "test_app.test_label()\n"
        "print('TESTS-PASSED')\n"
    )
    (workspace / "left.py").write_text("def value():\n    return 1\n", encoding="utf-8")
    (workspace / "right.py").write_text("def total():\n    return 1\n", encoding="utf-8")
    (workspace / "test_app.py").write_text(
        "from left import value\n"
        "from right import total, LABEL\n"
        "\n"
        "def test_total():\n"
        "    assert value() == 2\n"
        "    assert total() == 2\n"
        "\n"
        "def test_label():\n"
        "    assert LABEL == 'ready'\n",
        encoding="utf-8",
    )
    issued: list[str] = []
    summary_started = asyncio.Event()
    release_summary = asyncio.Event()
    lines: list[str] = []

    async def respond(request):
        if request.purpose == "context_summary":
            summary_started.set()
            await release_summary.wait()
            return text_response(
                "Earlier completed edits already ran. This summary is not the live execution status."
            )
        blob = "\n".join(_user_texts(request))
        if "left" not in issued:
            issued.append("left")
            return tool_response(("left", left_code, None), text="Updating left.py so value() returns 2.")
        if "right" not in issued:
            issued.append("right")
            return tool_response(("right", right_code, None), text="Updating right.py so total() uses value().")
        if new_requirement in blob and "label" not in issued:
            issued.append("label")
            return tool_response(("label", label_code, None), text="Setting LABEL to ready.")
        if "label" in issued and "LABEL-DONE" in blob and "tests" not in issued:
            issued.append("tests")
            return tool_response(("tests", test_code, None), text="Running test_app.py.")
        if "TESTS-PASSED" in blob:
            return text_response("Edited left.py and right.py, set LABEL to ready, and the tests passed.")
        if "right" in issued and "label" not in issued:
            return text_response("Files are updated. A later requirement will be applied before tests.")
        return text_response("Edited left.py and right.py, set LABEL to ready, and the tests passed.")

    async def body() -> None:
        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace, backend)
        runtime.echo = lines.append
        runtime.actor.note = lines.append
        try:
            await runtime.start()
            runtime.submit_text(
                "Make value() return 2 and total() return value() by editing left.py and right.py. "
                "Then be ready to run test_app.py."
            )
            await _until(lambda: sum(event.type == "python.requested" for event in runtime.trace) >= 2)
            runtime.actor.context_limit = runtime.actor.projector.view_length() + 50
            await asyncio.wait_for(summary_started.wait(), timeout=8)
            runtime.submit_text(new_requirement)
            release_summary.set()
            idle = await runtime.wait_until_idle(20)
            if not idle:
                paused = runtime.actor.paused
                detail = (
                    f"phase={runtime.actor.phase} busy={runtime.actor.busy} "
                    f"pause={None if paused is None else paused.error_kind} "
                    f"view={runtime.actor.projector.view_length()} "
                    f"limit={runtime.actor.context_limit} issued={issued} "
                    f"calls={backend.calls}\n"
                    + "\n".join(lines[-30:])
                )
                raise AssertionError(detail)

            requested = [event.payload.get("code") for event in runtime.trace if event.type == "python.requested"]
            assert requested.count(left_code) == 1
            assert requested.count(right_code) == 1
            assert requested.count(label_code) == 1
            assert requested.count(test_code) == 1
            assert any(
                "TESTS-PASSED" in str(event.payload.get("stdout") or "")
                for event in runtime.trace
                if event.type == "python.finished"
            )
            assert "return 2" in (workspace / "left.py").read_text(encoding="utf-8")
            right_text = (workspace / "right.py").read_text(encoding="utf-8")
            assert "def total" in right_text
            assert "LABEL = 'ready'" in right_text

            summaries = [item for item in backend.requests if item.purpose == "context_summary"]
            assert summaries
            assert new_requirement not in json.dumps(summaries[0].messages, ensure_ascii=False)
            task_requests = [item for item in backend.requests if item.purpose != "context_summary"]
            assert any(new_requirement in text for item in task_requests for text in _user_texts(item))
            final = task_requests[-1]
            assert "TESTS-PASSED" in "\n".join(_user_texts(final))
            assert new_requirement in "\n".join(_user_texts(final))
            status = json.loads(
                next(
                    str(message.get("content") or "")
                    for message in final.messages
                    if '"kind":"runtime_status"' in str(message.get("content") or "")
                )
            )
            assert status["unfinished_execution_ids"] == []
            assert status["current_execution_id"] is None
            assert "succeeded" not in status
            assert any("LEFT-DONE" in json.dumps(item, ensure_ascii=False) for item in runtime.actor.transcript.archive)
            assert any(
                "ready" in str(message.get("content") or "")
                for message in runtime.actor.transcript.messages
                if message.get("role") == "assistant"
            )
            assert any("compressing" in line for line in lines)
            assert any(line.startswith("  context") for line in lines)
            assert any("LEFT-DONE" in line for line in lines)
            handle_line(runtime, "/logs")
            assert any("reply_to=" in line and "→" in line for line in lines)
        finally:
            await runtime.shutdown()

    asyncio.run(body())


def test_host_delivery_rejection_status_reaches_provider_and_is_acknowledged(workspace: Path) -> None:
    async def body() -> None:
        def respond(request):
            if backend.calls == 1:
                return tool_response(("call-handler", "on_finished(lambda result: 'callback seen')\nimport time\ntime.sleep(0.15)\nprint('EXECUTION DONE')", None))
            terminal = next(
                (json.loads(str(message.get("content") or "")) for message in request.messages
                 if message.get("role") == "user" and '"type":"python.finished"' in str(message.get("content") or "")),
                None,
            )
            if terminal is None:
                return text_response("terminal missing")
            missing = terminal["payload"].get("missing_handler_ids", [])
            return text_response(f"Acknowledged host rejection: {missing[0] if missing else 'none'}")

        backend = ScriptedBackend(respond)
        runtime = _runtime(workspace, backend)
        original_call = runtime.host_emitter.call

        def reject_handler_delivery(event_type, payload, **kwargs):
            if event_type == "agent.handler_fired":
                return Delivery(False, reason="capacity_exceeded")
            return original_call(event_type, payload, **kwargs)

        runtime.host_emitter.call = reject_handler_delivery
        try:
            await runtime.start()
            runtime.submit_text("Run Python and report handler delivery status.")
            assert await runtime.wait_until_idle(8)

            terminals = [event for event in runtime.trace if event.type == "python.finished"]
            assert len(terminals) == 1
            terminal = terminals[0]
            assert terminal.payload["handler_result_status"] == "incomplete"
            assert len(terminal.payload["missing_handler_ids"]) == 1
            provider_events = [
                json.loads(str(message.get("content") or ""))
                for message in backend.requests[1].messages
                if message.get("role") == "user"
                and '"type":"python.finished"' in str(message.get("content") or "")
            ]
            assert len(provider_events) == 1
            assert provider_events[0]["payload"]["missing_handler_ids"] == terminal.payload["missing_handler_ids"]
            assert any(
                message.get("content") == f"Acknowledged host rejection: {terminal.payload['missing_handler_ids'][0]}"
                for message in runtime.actor.transcript.messages
            )
        finally:
            await runtime.shutdown()

    asyncio.run(body())
