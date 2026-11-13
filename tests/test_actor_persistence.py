"""Actor FULL commits stay off-loop, ordered, detached, and owned until complete."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from pathlib import Path

import pytest

from nervipulsa.config import Settings
from nervipulsa.llm import Activation
from nervipulsa.ports import ModelResponse, ProviderFailure, ToolCall
from nervipulsa.providers import ScriptedBackend
from nervipulsa.runtime import Runtime
from nervipulsa.session_lock import SessionBusyError, SessionLease


class _CommitGate:
    """Pause a real Journal operation before allowing its FULL transaction."""

    def __init__(self, original, predicate=lambda payload: True):
        self.original = original
        self.predicate = predicate
        self.loop_thread = threading.get_ident()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        self.payload = None

    def __call__(self, **payload):
        assert threading.get_ident() != self.loop_thread
        gated = self.payload is None and self.predicate(payload)
        if gated:
            self.payload = payload
            self.entered.set()
            assert self.release.wait(15), "commit gate was not released"
        try:
            return self.original(**payload)
        finally:
            if gated:
                self.finished.set()

    async def wait(self):
        assert await asyncio.to_thread(self.entered.wait, 10), "commit was not reached"


class _FileTool:
    name = "record"

    def __init__(self, path):
        self.path = path
        self.receipts = {}
        self.before_invoke = None

    def schema(self, *, workspace):
        return {"type": "function", "function": {
            "name": self.name, "description": "Record a call in a file",
            "parameters": {"type": "object", "properties": {}},
        }}

    def validate(self, arguments, *, max_timeout):
        return None

    async def invoke(self, arguments, *, context, activation, call):
        if self.before_invoke is not None:
            await self.before_invoke(call)

        def append():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(call.id + "\n")

        await asyncio.to_thread(append)
        receipt = {"status": "accepted", "execution_id": "exec-" + call.id,
                   "details": {"value": "original"}}
        self.receipts[call.id] = receipt
        return receipt, "accepted"


class _Catalog:
    def __init__(self, tool=None):
        self.tool = tool

    def schemas(self, *, workspace):
        return [] if self.tool is None else [self.tool.schema(workspace=workspace)]

    def get(self, name):
        return self.tool if self.tool is not None and name == self.tool.name else None


def _runtime(path, backend=None, *, tool=None, session_id="owner", journal_path=None):
    return Runtime(
        Settings(model="scripted"),
        backend or ScriptedBackend(lambda request: ModelResponse(content="ack")),
        path, python_host=False, tool_catalog=_Catalog(tool),
        session_id=session_id, journal_path=journal_path,
    )


def _tools():
    return ModelResponse(tool_calls=[
        ToolCall("a", "record", {}), ToolCall("b", "record", {}),
    ])


def _install_gate(monkeypatch, runtime, phase):
    if phase == "receipt":
        gate = _CommitGate(runtime.journal.commit_tool_receipt)
        monkeypatch.setattr(runtime.journal, "commit_tool_receipt", gate)
    else:
        status = {"input": "running", "final": "committed",
                  "pause": "paused", "reject": "committed"}[phase]
        gate = _CommitGate(runtime.journal.commit_activation_snapshot,
                           lambda payload: payload["status"] == status)
        monkeypatch.setattr(runtime.journal, "commit_activation_snapshot", gate)
    return gate


def _backend_for(phase):
    def respond(request):
        if backend.calls == 1:
            if phase == "pause":
                raise ProviderFailure("timeout", "provider stopped")
            if phase == "reject":
                return ModelResponse(tool_calls=[ToolCall("bad", "unknown", {})])
            if phase == "receipt":
                return _tools()
        return ModelResponse(content="ack")

    backend = ScriptedBackend(respond)
    return backend


@pytest.mark.parametrize("phase", ["input", "final", "pause", "reject", "receipt"])
def test_commits_allow_admission_and_independent_session_progress(
    tmp_path: Path, monkeypatch, phase,
):
    async def run():
        tool = _FileTool(tmp_path / "calls.txt")
        runtime = _runtime(tmp_path / "owner", _backend_for(phase), tool=tool)
        other = _runtime(tmp_path / "other", session_id="other",
                         journal_path=runtime.journal_path)
        await asyncio.gather(runtime.start(), other.start())
        gate = _install_gate(monkeypatch, runtime, phase)
        waiting = None
        try:
            first = runtime.submit_text("original input")
            assert first.accepted
            waiting = asyncio.create_task(runtime.wait_for_activation(
                input_event_id=first.event_id, timeout=10,
            ))
            await gate.wait()
            activation = runtime.activation_for_event(first.event_id)
            assert activation is not None and activation.status == "running"
            assert activation.settle_version == 0
            assert runtime.actor.paused is None
            assert not waiting.done()
            if phase == "input":
                assert runtime.backend.calls == 0
            if phase == "receipt":
                assert tool.path.read_text(encoding="utf-8").splitlines() == ["a"]

            # This session shares the same SQLite file. A blocked FULL commit
            # must not monopolize the loop or stop another session's commits.
            independent = other.submit_text("independent input")
            settled = await other.wait_for_activation(
                input_event_id=independent.event_id, timeout=5,
            )
            assert settled is not None and settled.status == "committed"
            other_rows = await asyncio.to_thread(other.inspect_durable)
            assert other_rows["activations"][0]["status"] == "committed"
            assert not gate.finished.is_set()
            assert not waiting.done()
            late = None
            if phase != "pause":
                late = runtime.submit_text("admitted while commit is blocked")
                assert late.accepted
                assert runtime.activation_for_event(late.event_id) is None

            before = await asyncio.to_thread(runtime.inspect_durable)
            assert all(row["status"] != "committed" for row in before["activations"])
            assert before["tool_receipts"] == []
            gate.release.set()
            settled = await waiting
            assert settled is not None
            assert gate.finished.is_set()
            if phase == "pause":
                assert settled.status == "paused" and settled.error_kind == "timeout"
                assert runtime.actor.paused is settled
            else:
                assert settled.status == "committed"
                assert await runtime.wait_until_idle(timeout=10)
                assert late is not None
                assert runtime.activation_for_event(late.event_id) is not None
            after = await asyncio.to_thread(runtime.inspect_durable)
            row = next(row for row in after["activations"]
                       if row["activation_id"] == activation.id)
            assert row["status"] == ("paused" if phase == "pause" else "committed")
            snapshot = next(row for row in after["transcript_snapshots"]
                            if row["activation_id"] == activation.id)
            assert first.event_id in json.loads(snapshot["event_ids_json"])
            if phase == "receipt":
                assert tool.path.read_text(encoding="utf-8").splitlines() == ["a", "b"]
                assert {row["tool_call_id"] for row in after["tool_receipts"]} == {"a", "b"}
        finally:
            gate.release.set()
            if waiting is not None:
                await waiting
            await asyncio.gather(runtime.shutdown(), other.shutdown())

    asyncio.run(run())


@pytest.mark.parametrize("phase", ["input", "final", "pause", "receipt"])
def test_shutdown_cancellation_keeps_lease_until_real_commit_finishes(
    tmp_path: Path, monkeypatch, phase,
):
    async def run():
        tool = _FileTool(tmp_path / "calls.txt")
        runtime = _runtime(tmp_path / "owner", _backend_for(phase), tool=tool)
        await runtime.start()
        gate = _install_gate(monkeypatch, runtime, phase)
        closing = stopped = None
        close_entered = asyncio.Event()
        original_close = runtime.host.close

        async def close_host():
            close_entered.set()
            await original_close()

        monkeypatch.setattr(runtime.host, "close", close_host)
        try:
            first = runtime.submit_text("cancel during a commit")
            await gate.wait()
            activation = runtime.activation_for_event(first.event_id)
            closing = asyncio.create_task(runtime.shutdown())
            await asyncio.wait_for(close_entered.wait(), 5)
            closing.cancel()
            await asyncio.sleep(0)
            closing.cancel()
            await asyncio.sleep(0)
            stopped = asyncio.create_task(runtime.actor.wait_stopped())
            await asyncio.sleep(0)
            assert not closing.done()
            assert not stopped.done()
            assert not gate.finished.is_set()
            assert activation.status == "running" and activation.settle_version == 0
            with pytest.raises(SessionBusyError):
                await asyncio.to_thread(SessionLease.acquire, runtime.journal_path, "owner")
        finally:
            gate.release.set()
            if closing is not None:
                with pytest.raises(asyncio.CancelledError):
                    await closing
            if stopped is not None:
                await stopped
            await runtime.shutdown()
        assert gate.finished.is_set()
        assert not runtime.journal._thread.is_alive()
        rows = await asyncio.to_thread(runtime.inspect_durable)
        assert rows["activations"]
        if phase in {"final", "pause"}:
            status = "committed" if phase == "final" else "paused"
            assert rows["activations"][0]["status"] == status
            assert activation.status == status and activation.settle_version == 1
        if phase == "input":
            assert runtime.backend.calls == 0
        if phase == "receipt":
            assert tool.path.read_text(encoding="utf-8").splitlines() == ["a"]
            assert [row["tool_call_id"] for row in rows["tool_receipts"]] == ["a"]
        lease = await asyncio.to_thread(SessionLease.acquire, runtime.journal_path, "owner")
        await asyncio.to_thread(lease.close)

    asyncio.run(run())


@pytest.mark.parametrize("failed_attempts", [1, 2])
def test_input_snapshot_failure_only_retries_on_explicit_retry(
    tmp_path: Path, monkeypatch, failed_attempts,
):
    async def run():
        runtime = _runtime(tmp_path)
        await runtime.start()
        original = runtime.journal.commit_activation_snapshot
        gate = _CommitGate(original, lambda payload: payload["status"] == "running")
        attempts = 0

        def fail_then_commit(**payload):
            nonlocal attempts
            attempts += 1
            if attempts <= failed_attempts:
                raise sqlite3.OperationalError("injected input snapshot failure")
            return gate(**payload)

        monkeypatch.setattr(runtime.journal, "commit_activation_snapshot", fail_then_commit)
        waiting = None
        try:
            first = runtime.submit_text("durable before inference")
            previous = None
            for number in range(failed_attempts):
                if number:
                    assert runtime.retry().accepted
                paused = await runtime.wait_for_activation(
                    input_event_id=first.event_id, after_version=previous, timeout=5,
                )
                assert paused is not None and paused.error_kind == "commit_interrupted"
                previous = paused.settle_version
                assert attempts == number + 1
                assert runtime.backend.calls == 0
                assert (await asyncio.to_thread(runtime.inspect_durable))["activations"] == []
            assert runtime.retry().accepted
            waiting = asyncio.create_task(runtime.wait_for_activation(
                input_event_id=first.event_id, after_version=previous, timeout=10,
            ))
            await gate.wait()
            assert attempts == failed_attempts + 1
            assert runtime.backend.calls == 0
            assert not waiting.done()
            gate.release.set()
            settled = await waiting
            assert settled is paused and settled.status == "committed"
            assert runtime.backend.calls == 1
            rows = await asyncio.to_thread(runtime.inspect_durable)
            assert rows["activations"][0]["status"] == "committed"
            assert first.event_id in json.loads(rows["activations"][0]["input_event_ids_json"])
        finally:
            gate.release.set()
            if waiting is not None:
                await waiting
            await runtime.shutdown()

    asyncio.run(run())


def test_receipt_retry_commits_detached_receipt_before_next_tool_without_repeat(
    tmp_path: Path, monkeypatch,
):
    async def run():
        tool = _FileTool(tmp_path / "calls.txt")
        backend = ScriptedBackend(lambda request: _tools())
        runtime = _runtime(tmp_path / "owner", backend, tool=tool)
        await runtime.start()
        original = runtime.journal.commit_tool_receipt
        gate = _CommitGate(original, lambda payload: payload["tool_call_id"] == "a")
        attempts = 0

        def fail_once(**payload):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise sqlite3.OperationalError("injected receipt failure")
            return gate(**payload)

        async def before_invoke(call):
            if call.id == "b":
                rows = await asyncio.to_thread(runtime.inspect_durable)
                assert [row["tool_call_id"] for row in rows["tool_receipts"]] == ["a"]

        tool.before_invoke = before_invoke
        monkeypatch.setattr(runtime.journal, "commit_tool_receipt", fail_once)
        waiting = None
        try:
            first = runtime.submit_text("record each call once")
            paused = await runtime.wait_for_activation(input_event_id=first.event_id, timeout=5)
            assert paused is not None and paused.error_kind == "commit_interrupted"
            version = paused.settle_version
            assert attempts == 1 and backend.calls == 1
            assert tool.path.read_text(encoding="utf-8").splitlines() == ["a"]
            assert (await asyncio.to_thread(runtime.inspect_durable))["tool_receipts"] == []
            # The tool still owns the object it returned; a later mutation must
            # not change the actor's retained receipt on explicit retry.
            tool.receipts["a"]["details"]["value"] = "mutated by tool"
            assert runtime.retry().accepted
            waiting = asyncio.create_task(runtime.wait_for_activation(
                input_event_id=first.event_id, after_version=version, timeout=10,
            ))
            await gate.wait()
            assert attempts == 2 and backend.calls == 1
            assert not waiting.done()
            assert tool.path.read_text(encoding="utf-8").splitlines() == ["a"]
            gate.release.set()
            settled = await waiting
            assert settled is paused and settled.status == "committed"
            assert tool.path.read_text(encoding="utf-8").splitlines() == ["a", "b"]
            assert backend.calls == 1 and attempts == 3
            rows = await asyncio.to_thread(runtime.inspect_durable)
            receipts = {row["tool_call_id"]: json.loads(row["receipt_json"])
                        for row in rows["tool_receipts"]}
            assert receipts["a"]["details"] == {"value": "original"}
            assert set(receipts) == {"a", "b"}
        finally:
            gate.release.set()
            if waiting is not None:
                await waiting
            await runtime.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("kind", ["snapshot", "receipt"])
def test_thread_payload_is_detached_before_cancellation(tmp_path: Path, monkeypatch, kind):
    async def run():
        runtime = _runtime(tmp_path)
        await runtime.start()
        activation = Activation("detached", ("evt_1",), 7, "running", 1,
                                context_metrics={"nested": {"value": "original"}})
        runtime.actor.transcript.messages[:] = [
            {"role": "user", "content": [{"text": "original"}]},
        ]
        runtime.actor.transcript.event_ids[:] = ["evt_1"]
        runtime.actor.transcript.event_types[:] = ["user.message"]
        runtime.actor.transcript.version = 7
        receipt = {"status": "accepted", "details": {"value": "original"}}
        if kind == "snapshot":
            gate = _CommitGate(runtime.journal.commit_activation_snapshot)
            monkeypatch.setattr(runtime.journal, "commit_activation_snapshot", gate)
            operation = runtime.actor._durable_commit_activation(activation)
        else:
            gate = _CommitGate(runtime.journal.commit_tool_receipt)
            monkeypatch.setattr(runtime.journal, "commit_tool_receipt", gate)
            operation = runtime.actor._durable_commit_tool_receipt(
                activation, "call", "exec", "accepted", receipt,
            )
        committing = asyncio.create_task(operation)
        try:
            await gate.wait()
            activation.id = "changed"
            activation.context_metrics["nested"]["value"] = "changed"
            runtime.actor.transcript.messages[0]["content"][0]["text"] = "changed"
            runtime.actor.transcript.event_ids[0] = "evt_2"
            runtime.actor.transcript.event_types[0] = "runtime.observation"
            receipt["details"]["value"] = "changed"
            committing.cancel()
            await asyncio.sleep(0)
            committing.cancel()
            await asyncio.sleep(0)
            assert not committing.done()
            assert not gate.finished.is_set()
        finally:
            gate.release.set()
            with pytest.raises(asyncio.CancelledError):
                await committing
            await runtime.shutdown()
        assert gate.finished.is_set()
        rows = await asyncio.to_thread(runtime.inspect_durable)
        if kind == "snapshot":
            assert rows["activations"][0]["activation_id"] == "detached"
            assert json.loads(rows["activations"][0]["context_json"]) == {
                "nested": {"value": "original"},
            }
            snapshot = rows["transcript_snapshots"][0]
            assert json.loads(snapshot["messages_json"])[0]["content"] == [{"text": "original"}]
            assert json.loads(snapshot["event_ids_json"]) == ["evt_1"]
            assert json.loads(snapshot["event_types_json"]) == ["user.message"]
        else:
            assert rows["tool_receipts"][0]["activation_id"] == "detached"
            assert json.loads(rows["tool_receipts"][0]["receipt_json"])["details"] == {
                "value": "original",
            }

    asyncio.run(run())
