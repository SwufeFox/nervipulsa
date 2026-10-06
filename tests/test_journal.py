from __future__ import annotations

import sqlite3
from pathlib import Path

from nervipulsa.journal import Journal


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
