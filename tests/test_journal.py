from __future__ import annotations


import pytest
import sqlite3
import time
from pathlib import Path

from nervipulsa.journal import Journal


def _inject_commit_failure(monkeypatch: pytest.MonkeyPatch, path: Path) -> None:
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


def test_effect_intent_commit_failure_rolls_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        assert journal.flush()
        _inject_commit_failure(monkeypatch, journal.path)
        with pytest.raises(sqlite3.OperationalError, match="injected commit failure"):
            journal.prepare_effect("session-a", "exec-intent-fail")
        monkeypatch.undo()
        assert journal.unresolved_effects() == []
    finally:
        assert journal.close()


def test_effect_dispatch_commit_failure_leaves_intent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        assert journal.flush()
        journal.prepare_effect("session-a", "exec-dispatch-fail")
        _inject_commit_failure(monkeypatch, journal.path)
        with pytest.raises(sqlite3.OperationalError, match="injected commit failure"):
            journal.mark_effect_dispatching("session-a", "exec-dispatch-fail")
        monkeypatch.undo()
        pending = journal.unresolved_effects("session-a")
        assert [(item["execution_id"], item["state"]) for item in pending] == [
            ("exec-dispatch-fail", "intent")
        ]
    finally:
        assert journal.close()


def test_effect_observed_commit_failure_leaves_dispatching(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        assert journal.flush()
        journal.prepare_effect("session-a", "exec-observed-fail")
        journal.mark_effect_dispatching("session-a", "exec-observed-fail")
        _inject_commit_failure(monkeypatch, journal.path)
        with pytest.raises(sqlite3.OperationalError, match="injected commit failure"):
            journal.observe_effect("session-a", "exec-observed-fail", "succeeded")
        monkeypatch.undo()
        pending = journal.unresolved_effects("session-a")
        assert [(item["execution_id"], item["state"]) for item in pending] == [
            ("exec-observed-fail", "dispatching")
        ]
    finally:
        assert journal.close()


def test_flush_preserves_per_table_upsert_order_when_grouping(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        journal.activation(
            {
                "id": "activation-1",
                "session_id": "session-1",
                "input_event_ids": [],
                "transcript_version": 1,
                "status": "running",
                "started_at": 1.0,
                "ended_at": None,
                "model": None,
                "usage": None,
                "context": {},
                "error": None,
            }
        )
        journal.execution(
            {
                "execution_id": "execution-1",
                "session_id": "session-1",
                "activation_id": "activation-1",
                "worker_epoch": None,
                "status": "queued",
                "queued_at": 1.0,
                "started_at": None,
                "ended_at": None,
                "namespace_reset": 0,
            }
        )
        journal.activation(
            {
                "id": "activation-1",
                "session_id": "session-1",
                "input_event_ids": [],
                "transcript_version": 2,
                "status": "complete",
                "started_at": 1.0,
                "ended_at": 2.0,
                "model": None,
                "usage": None,
                "context": {},
                "error": None,
            }
        )
        journal.execution(
            {
                "execution_id": "execution-1",
                "session_id": "session-1",
                "activation_id": "activation-1",
                "worker_epoch": None,
                "status": "complete",
                "queued_at": 1.0,
                "started_at": 1.1,
                "ended_at": 2.0,
                "namespace_reset": 1,
            }
        )
        journal._submit(("unrecognized", {}))
        assert journal.flush()
        assert journal.error is None

        with sqlite3.connect(journal.path) as connection:
            activation = connection.execute(
                "SELECT transcript_version,status FROM activations WHERE id='activation-1'"
            ).fetchone()
            execution = connection.execute(
                "SELECT status,namespace_reset FROM executions WHERE execution_id='execution-1'"
            ).fetchone()
        assert activation == (2, "complete")
        assert execution == ("complete", 1)
        assert journal.error is None
    finally:
        assert journal.close()


def test_durable_effect_intent_and_observed_commit(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        assert journal.flush()
        journal.prepare_effect("session-a", "exec-pending")
        journal.prepare_effect("session-a", "exec-observed")
        journal.mark_effect_dispatching("session-a", "exec-observed")
        journal.observe_effect("session-a", "exec-observed", "succeeded")
        assert [(item["execution_id"], item["state"]) for item in journal.unresolved_effects("session-a")] == [
            ("exec-pending", "intent")
        ]
        assert journal.unresolved_effects("session-b") == []
        with sqlite3.connect(journal.path) as connection:
            assert connection.execute(
                "SELECT state,outcome FROM durable_effects WHERE execution_id='exec-observed'"
            ).fetchone() == ("observed", "succeeded")
    finally:
        assert journal.close()

def test_durable_effect_dispatching_is_unresolved(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        assert journal.flush()
        journal.prepare_effect("session-a", "exec-dispatching")
        journal.mark_effect_dispatching("session-a", "exec-dispatching")
        pending = journal.unresolved_effects("session-a")
        assert len(pending) == 1
        assert pending[0]["state"] == "dispatching"
        with pytest.raises(RuntimeError, match="already dispatched"):
            journal.mark_effect_dispatching("session-a", "exec-dispatching")
    finally:
        assert journal.close()


def test_effect_ids_are_scoped_by_session(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        assert journal.flush()
        journal.prepare_effect("session-a", "same-execution")
        journal.prepare_effect("session-b", "same-execution")
        records = journal.unresolved_effects()
        assert {
            (item["session_id"], item["execution_id"], item["state"])
            for item in records
        } == {
            ("session-a", "same-execution", "intent"),
            ("session-b", "same-execution", "intent"),
        }
    finally:
        assert journal.close()


def test_session_scoped_event_and_execution_ids(tmp_path: Path) -> None:
    from nervipulsa.events import Event

    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        journal.observe_event(Event("s1", "same", 1, "notice", "a", "b", None, {}, 1.0))
        journal.observe_event(Event("s2", "same", 1, "notice", "a", "b", None, {}, 2.0))
        for session, status in (("s1", "running"), ("s2", "queued")):
            journal.execution({
                "execution_id": "same-execution", "session_id": session,
                "activation_id": None, "worker_epoch": None, "status": status,
                "queued_at": 1.0, "started_at": None, "ended_at": None,
                "namespace_reset": 0,
            })
        assert journal.flush()
        with sqlite3.connect(journal.path) as connection:
            assert connection.execute("SELECT session_id,id FROM events ORDER BY session_id").fetchall() == [("s1", "same"), ("s2", "same")]
            assert connection.execute("SELECT session_id,status FROM executions ORDER BY session_id").fetchall() == [("s1", "running"), ("s2", "queued")]
    finally:
        assert journal.close()


def test_flushes_partial_batch_after_idle(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "journal.sqlite")
    journal.start()
    try:
        assert journal.flush()  # Wait for schema initialization before polling SQLite.
        journal.execution(
            {
                "execution_id": "idle-flush",
                "session_id": "session-idle",
                "activation_id": None,
                "worker_epoch": None,
                "status": "queued",
                "queued_at": time.time(),
                "started_at": None,
                "ended_at": None,
                "namespace_reset": 0,
            }
        )
        deadline = time.monotonic() + 3.0
        row = None
        while time.monotonic() < deadline:
            with sqlite3.connect(journal.path) as connection:
                row = connection.execute(
                    "SELECT status FROM executions WHERE session_id=? AND execution_id=?",
                    ("session-idle", "idle-flush"),
                ).fetchone()
            if row is not None:
                break
            time.sleep(0.02)
        assert row == ("queued",)
    finally:
        assert journal.close()


def test_migrates_legacy_primary_keys_preserving_rows(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite"
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE events (
                id TEXT PRIMARY KEY, session_id TEXT NOT NULL, seq INTEGER NOT NULL,
                type TEXT NOT NULL, source TEXT NOT NULL, target TEXT NOT NULL,
                reply_to TEXT, payload_json TEXT NOT NULL, accepted_at REAL NOT NULL,
                UNIQUE(session_id, seq)
            );
            CREATE TABLE executions (
                execution_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                activation_id TEXT, worker_epoch INTEGER, status TEXT NOT NULL,
                queued_at REAL NOT NULL, started_at REAL, ended_at REAL,
                namespace_reset INTEGER NOT NULL DEFAULT 0, metadata_json TEXT
            );
            INSERT INTO events VALUES ('legacy-event','s',7,'notice','src','dst',NULL,'{"x":1}',3.5);
            INSERT INTO executions VALUES ('legacy-exec','s','act',4,'complete',1,2,3,1,'{"keep":true}');
        """)
    journal = Journal(path)
    journal.start()
    try:
        assert journal.flush()
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT * FROM events").fetchone() == ("legacy-event", "s", 7, "notice", "src", "dst", None, '{"x":1}', 3.5)
            assert connection.execute("SELECT * FROM executions").fetchone() == ("legacy-exec", "s", "act", 4, "complete", 1.0, 2.0, 3.0, 1, '{"keep":true}')
            assert [row[1] for row in sorted(connection.execute("PRAGMA table_info(events)"), key=lambda row: row[5]) if row[5]] == ["session_id", "id"]
            assert [row[1] for row in sorted(connection.execute("PRAGMA table_info(executions)"), key=lambda row: row[5]) if row[5]] == ["session_id", "execution_id"]
            assert connection.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='events'").fetchall()
    finally:
        assert journal.close()
