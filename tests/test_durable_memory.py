"""Durable Transcript/Activation/ToolReceipt: explicit failure, restart-readable, no replay."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

import pytest

from nervipulsa.config import Settings
from nervipulsa.journal import Journal
from nervipulsa.ports import ModelRequest, ModelResponse
from nervipulsa.providers import ScriptedBackend
from nervipulsa.runtime import Runtime


def _inject_commit_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    import nervipulsa.journal as journal_module

    connect = sqlite3.connect
    failed = False

    class ConnectionProxy:
        def __init__(self, connection: sqlite3.Connection) -> None:
            self._connection = connection

        def __getattr__(self, name: str):
            return getattr(self._connection, name)

        def commit(self) -> None:
            nonlocal failed
            if not failed:
                failed = True
                raise sqlite3.OperationalError("injected commit failure")
            self._connection.commit()

    monkeypatch.setattr(
        journal_module.sqlite3,
        "connect",
        lambda *args, **kwargs: ConnectionProxy(connect(*args, **kwargs)),
    )


def _sample_messages() -> list[dict]:
    return [{"role": "user", "content": "hello"}]


def test_durable_activation_commit_failure_raises_and_leaves_no_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        assert journal.flush()
        _inject_commit_failure(monkeypatch)
        with pytest.raises(sqlite3.OperationalError, match="injected commit failure"):
            journal.commit_activation_snapshot(
                session_id="sess-fail",
                activation_id="act-1",
                status="committed",
                input_event_ids=["evt-1"],
                transcript_version=1,
                context={"note": "x"},
                error=None,
                transcript_messages=_sample_messages(),
                transcript_event_ids=["evt-1"],
                transcript_event_types=["user.message"],
            )
        monkeypatch.undo()
        inspected = journal.durable_inspect("sess-fail")
        assert inspected["activations"] == []
        assert inspected["transcript_snapshots"] == []
        assert inspected["tool_receipts"] == []
        assert "READ-ONLY" in inspected["notice"]
        assert journal.durable_sessions() == []
    finally:
        assert journal.close()


def test_durable_tool_receipt_commit_failure_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        assert journal.flush()
        _inject_commit_failure(monkeypatch)
        with pytest.raises(sqlite3.OperationalError, match="injected commit failure"):
            journal.commit_tool_receipt(
                session_id="sess-r",
                activation_id="act-r",
                tool_call_id="call-1",
                execution_id="exec-1",
                status="accepted",
                receipt={"status": "accepted", "execution_id": "exec-1"},
            )
        monkeypatch.undo()
        inspected = journal.durable_inspect("sess-r")
        assert inspected["tool_receipts"] == []
    finally:
        assert journal.close()


def test_durable_commit_and_inspect_roundtrip(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        assert journal.flush()
        journal.commit_activation_snapshot(
            session_id="sess-a",
            activation_id="act-a",
            status="committed",
            input_event_ids=["evt-1"],
            transcript_version=2,
            context={"measurement": "chars"},
            error=None,
            transcript_messages=_sample_messages(),
            transcript_event_ids=["evt-1"],
            transcript_event_types=["user.message"],
        )
        journal.commit_tool_receipt(
            session_id="sess-a",
            activation_id="act-a",
            tool_call_id="call-1",
            execution_id="exec-1",
            status="accepted",
            receipt={"status": "accepted", "execution_id": "exec-1"},
        )
        inspected = journal.durable_inspect("sess-a")
        assert "not resumable" in inspected["notice"]
        assert len(inspected["activations"]) == 1
        assert inspected["activations"][0]["activation_id"] == "act-a"
        assert len(inspected["transcript_snapshots"]) == 1
        messages = json.loads(str(inspected["transcript_snapshots"][0]["messages_json"]))
        assert messages == _sample_messages()
        assert len(inspected["tool_receipts"]) == 1
        assert inspected["tool_receipts"][0]["tool_call_id"] == "call-1"
        sessions = journal.durable_sessions()
        assert any(row["session_id"] == "sess-a" for row in sessions)
    finally:
        assert journal.close()


def _plain_backend() -> ScriptedBackend:
    async def behavior(request: ModelRequest) -> ModelResponse:
        return ModelResponse(content="ack", tool_calls=[])

    return ScriptedBackend(behavior)


def _runtime(workspace: Path, backend) -> Runtime:
    settings = Settings(model="scripted", workspace=str(workspace), max_timeout=30, default_timeout=30)
    return Runtime(settings, backend, workspace, echo=lambda _text: None)


def test_restart_inspectable_without_replay(tmp_path: Path) -> None:
    async def body() -> None:
        workspace = tmp_path / "ws"
        backend = _plain_backend()
        runtime1 = _runtime(workspace, backend)
        await runtime1.start()
        try:
            session1 = runtime1.bus.session_id
            delivery = runtime1.submit_text("hello durable")
            assert delivery.accepted
            assert await runtime1.wait_until_idle(timeout=15)
            inspected_current = runtime1.inspect_durable(limit=10)
            assert len(inspected_current["activations"]) >= 1
            assert len(inspected_current["transcript_snapshots"]) >= 1
            assert "READ-ONLY" in inspected_current["notice"]
        finally:
            await runtime1.shutdown()

        # Restart against the same workspace/journal file: new session, empty memory.
        runtime2 = _runtime(workspace, _plain_backend())
        await runtime2.start()
        try:
            assert runtime2.bus.session_id != session1
            # New session starts with empty in-memory transcript/activations: nothing restored.
            assert runtime2.actor.transcript.messages == []
            assert runtime2.actor.activations == []
            assert runtime2.llm_box.size == 0
            assert runtime2.host.idle
            # Old session remains inspectable read-only.
            old = runtime2.inspect_durable(session_id=session1, limit=10)
            assert "not resumable" in old["notice"]
            assert len(old["activations"]) >= 1
            assert len(old["transcript_snapshots"]) >= 1
            messages = json.loads(str(old["transcript_snapshots"][0]["messages_json"]))
            assert any("hello durable" in str(m.get("content", "")) for m in messages)
            # No auto-replay: no python execution was enqueued or started by the restart.
            assert runtime2.host.current_execution_id is None
            assert runtime2.host.queue_size == 0
            assert runtime2.actor.paused is None
        finally:
            await runtime2.shutdown()

    asyncio.run(body())


def test_durable_commit_failure_pauses_explicitly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def body() -> None:
        workspace = tmp_path / "ws-fail"
        runtime = _runtime(workspace, _plain_backend())
        await runtime.start()
        try:
            _inject_commit_failure(monkeypatch)
            delivery = runtime.submit_text("this commit must fail loudly")
            assert delivery.accepted
            assert not await runtime.wait_until_idle(timeout=15)
            paused = runtime.actor.paused
            assert paused is not None
            assert paused.error_kind == "commit_interrupted"
            assert "durable" in str(paused.error_message)
        finally:
            monkeypatch.undo()
            await runtime.shutdown()

    asyncio.run(body())
