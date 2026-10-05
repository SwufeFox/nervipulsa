"""Asynchronous SQLite observation journal (never on the event acceptance path)."""

from __future__ import annotations

import json
import queue
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from .events import Delivery, Event


_STOP = object()


class Journal:
    def __init__(self, path: Path, *, queue_limit: int = 4096) -> None:
        self.path = path
        self._queue: queue.Queue[object] = queue.Queue(maxsize=queue_limit)
        self._thread = threading.Thread(target=self._writer, name="nervipulsa-journal", daemon=True)
        self._started = False
        self._error: str | None = None
        self._dropped = 0

    @property
    def incomplete(self) -> bool:
        return self._dropped > 0 or self._error is not None

    @property
    def error(self) -> str | None:
        return self._error

    @property
    def dropped(self) -> int:
        return self._dropped

    def start(self) -> None:
        if self._started:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._thread.start()
        self._started = True

    def _submit(self, item: object) -> None:
        if not self._started:
            return
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            self._dropped += 1

    def observe_event(self, event: Event) -> None:
        self._submit(("event", event))

    def observe_delivery(self, session_id: str, event: Event | None, receiver: str, delivery: Delivery) -> None:
        self._submit(
            (
                "delivery",
                {
                    "session_id": session_id,
                    "event_id": event.id if event else delivery.event_id,
                    "receiver": receiver,
                    "accepted": int(delivery.accepted),
                    "reason": delivery.reason,
                    "observed_at": time.time(),
                },
            )
        )

    def activation(self, record: dict[str, Any]) -> None:
        self._submit(("activation", dict(record)))

    def execution(self, record: dict[str, Any]) -> None:
        self._submit(("execution", dict(record)))

    def tool_call(self, record: dict[str, Any]) -> None:
        self._submit(("tool_call", dict(record)))

    def _writer(self) -> None:
        connection: sqlite3.Connection | None = None
        pending: list[object] = []
        try:
            connection = sqlite3.connect(self.path, timeout=5)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    source TEXT NOT NULL,
                    target TEXT NOT NULL,
                    reply_to TEXT,
                    payload_json TEXT NOT NULL,
                    accepted_at REAL NOT NULL,
                    UNIQUE(session_id, seq)
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    event_id TEXT,
                    receiver TEXT NOT NULL,
                    accepted INTEGER NOT NULL,
                    reason TEXT,
                    observed_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS activations (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    input_event_ids_json TEXT NOT NULL,
                    transcript_version INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    started_at REAL NOT NULL,
                    ended_at REAL,
                    model TEXT,
                    usage_json TEXT,
                    context_json TEXT,
                    error_json TEXT
                );
                CREATE TABLE IF NOT EXISTS executions (
                    execution_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    activation_id TEXT,
                    worker_epoch INTEGER,
                    status TEXT NOT NULL,
                    queued_at REAL NOT NULL,
                    started_at REAL,
                    ended_at REAL,
                    namespace_reset INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT
                );
                CREATE TABLE IF NOT EXISTS tool_calls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    activation_id TEXT NOT NULL,
                    tool_call_id TEXT NOT NULL,
                    execution_id TEXT,
                    status TEXT NOT NULL,
                    reason TEXT,
                    observed_at REAL NOT NULL
                );
                """
            )
            columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(activations)")
            }
            if "context_json" not in columns:
                connection.execute("ALTER TABLE activations ADD COLUMN context_json TEXT")
            connection.commit()
            while True:
                try:
                    item = self._queue.get(timeout=0.2)
                except queue.Empty:
                    item = None
                if item is _STOP:
                    self._flush(connection, pending)
                    break
                if isinstance(item, tuple) and item and item[0] == "barrier":
                    self._flush(connection, pending)
                    item[1].set()
                    continue
                if item is not None:
                    pending.append(item)
                if len(pending) >= 64:
                    self._flush(connection, pending)
        except Exception as exc:
            self._error = f"{type(exc).__name__}: {exc}"
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

    def _flush(self, connection: sqlite3.Connection, pending: list[object]) -> None:
        if not pending:
            return
        try:
            for item in pending:
                if not isinstance(item, tuple):
                    continue
                kind = item[0]
                if kind == "event":
                    event: Event = item[1]
                    connection.execute(
                        "INSERT OR IGNORE INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            event.id,
                            event.session_id,
                            event.seq,
                            event.type,
                            event.source,
                            event.target,
                            event.reply_to,
                            json.dumps(event.payload, ensure_ascii=False, separators=(",", ":")),
                            event.accepted_at,
                        ),
                    )
                elif kind == "delivery":
                    record = item[1]
                    connection.execute(
                        "INSERT INTO deliveries(session_id,event_id,receiver,accepted,reason,observed_at) "
                        "VALUES(?,?,?,?,?,?)",
                        (
                            record["session_id"],
                            record["event_id"],
                            record["receiver"],
                            record["accepted"],
                            record["reason"],
                            record["observed_at"],
                        ),
                    )
                elif kind == "activation":
                    record = item[1]
                    connection.execute(
                        """
                        INSERT INTO activations
                        (id,session_id,input_event_ids_json,transcript_version,status,started_at,ended_at,model,usage_json,context_json,error_json)
                        VALUES(:id,:session_id,:input_event_ids_json,:transcript_version,:status,:started_at,:ended_at,:model,:usage_json,:context_json,:error_json)
                        ON CONFLICT(id) DO UPDATE SET
                          transcript_version=excluded.transcript_version,
                          status=excluded.status,
                          ended_at=excluded.ended_at,
                          model=excluded.model,
                          usage_json=excluded.usage_json,
                          context_json=excluded.context_json,
                          error_json=excluded.error_json
                        """,
                        {
                            **record,
                            "input_event_ids_json": record.get("input_event_ids_json")
                            or json.dumps(record.get("input_event_ids", [])),
                            "usage_json": record.get("usage_json")
                            or (json.dumps(record["usage"]) if record.get("usage") is not None else None),
                            "context_json": record.get("context_json")
                            or json.dumps(record.get("context", {}), ensure_ascii=False),
                            "error_json": record.get("error_json")
                            or (json.dumps(record["error"]) if record.get("error") is not None else None),
                        },
                    )
                elif kind == "execution":
                    record = item[1]
                    connection.execute(
                        """
                        INSERT INTO executions
                        (execution_id,session_id,activation_id,worker_epoch,status,queued_at,started_at,ended_at,namespace_reset,metadata_json)
                        VALUES(:execution_id,:session_id,:activation_id,:worker_epoch,:status,:queued_at,:started_at,:ended_at,:namespace_reset,:metadata_json)
                        ON CONFLICT(execution_id) DO UPDATE SET
                          activation_id=excluded.activation_id,
                          worker_epoch=excluded.worker_epoch,
                          status=excluded.status,
                          started_at=excluded.started_at,
                          ended_at=excluded.ended_at,
                          namespace_reset=excluded.namespace_reset,
                          metadata_json=excluded.metadata_json
                        """,
                        {
                            **record,
                            "metadata_json": record.get("metadata_json")
                            or json.dumps(record.get("metadata", {}), ensure_ascii=False),
                        },
                    )
                elif kind == "tool_call":
                    record = item[1]
                    connection.execute(
                        "INSERT INTO tool_calls(session_id,activation_id,tool_call_id,execution_id,status,reason,observed_at) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (
                            record["session_id"],
                            record["activation_id"],
                            record["tool_call_id"],
                            record.get("execution_id"),
                            record["status"],
                            record.get("reason"),
                            record.get("observed_at", time.time()),
                        ),
                    )
            connection.commit()
        except Exception as exc:
            connection.rollback()
            self._error = f"{type(exc).__name__}: {exc}"
            self._dropped += len(pending)
        pending.clear()

    def flush(self, timeout: float = 5.0) -> bool:
        if not self._started or not self._thread.is_alive():
            return False
        event = threading.Event()
        try:
            self._queue.put(("barrier", event), timeout=timeout)
        except queue.Full:
            self._dropped += 1
            return False
        return event.wait(timeout)

    def recent(self, session_id: str, limit: int = 30) -> dict[str, list[dict[str, Any]]]:
        if not self.path.exists():
            return {"events": [], "activations": [], "executions": []}
        connection = sqlite3.connect(self.path, timeout=2)
        connection.row_factory = sqlite3.Row
        try:
            events = [
                dict(row)
                for row in connection.execute(
                    "SELECT id,seq,type,source,target,reply_to,payload_json,accepted_at "
                    "FROM events WHERE session_id=? ORDER BY seq DESC LIMIT ?",
                    (session_id, limit),
                )
            ]
            activations = [
                dict(row)
                for row in connection.execute(
                    "SELECT id,input_event_ids_json,transcript_version,status,started_at,ended_at,model,usage_json,context_json,error_json "
                    "FROM activations WHERE session_id=? ORDER BY started_at DESC LIMIT ?",
                    (session_id, limit),
                )
            ]
            executions = [
                dict(row)
                for row in connection.execute(
                    "SELECT execution_id,activation_id,worker_epoch,status,queued_at,started_at,ended_at,namespace_reset,metadata_json "
                    "FROM executions WHERE session_id=? ORDER BY queued_at DESC LIMIT ?",
                    (session_id, limit),
                )
            ]
            return {"events": events, "activations": activations, "executions": executions}
        finally:
            connection.close()

    def execution_event(self, session_id: str, execution_id: str) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        connection = sqlite3.connect(self.path, timeout=2)
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                "SELECT id,reply_to,payload_json FROM events "
                "WHERE session_id=? AND type='python.finished' AND reply_to=? "
                "ORDER BY seq DESC LIMIT 1",
                (session_id, execution_id),
            ).fetchone()
            return dict(row) if row is not None else None
        finally:
            connection.close()

    def close(self, timeout: float = 8.0) -> bool:
        if not self._started:
            return True
        deadline = time.monotonic() + timeout
        while True:
            try:
                self._queue.put(_STOP, timeout=min(0.1, max(0.01, deadline - time.monotonic())))
                break
            except queue.Full:
                if time.monotonic() >= deadline:
                    self._dropped += 1
                    return False
        self._thread.join(max(0.0, deadline - time.monotonic()))
        return not self._thread.is_alive()
