"""Python host: one serial worker, a separate control port, and one terminal each.

The host never calls the model. Timeouts and cancellation kill the worker
process tree and start a fresh interpreter; queued code from the old
namespace is cancelled instead of being replayed.
"""

from __future__ import annotations

import asyncio
import queue
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .events import MAX_HANDLER_RESULTS_PER_EXECUTION, Emitter, Event, Lane, Mailbox
from .framing import read_frame, write_frame
from .process_tree import ManagedProcess, spawn_worker

_PREVIEW = 4 * 1024


_IDLE_TICK = 0.25  # safety net only; real wakeups arrive via _signal_wake()


@dataclass
class Execution:
    event_id: str
    code: str
    timeout: float
    activation_id: str | None
    tool_call_id: str | None
    state: str = "queued"
    queued_at: float = 0.0
    started_at: float | None = None
    ended_at: float | None = None
    worker_epoch: int | None = None
    cancel: bool = False
    cancel_reason: str = ""
    timed_out: bool = False
    started_monotonic: float | None = None
    cleanup_failed: bool = False


@dataclass
class ExecResult:
    status: str
    stdout: str
    stderr: str
    duration_ms: int
    namespace_reset: bool
    reason: str
    worker_epoch: int
    truncated: bool = False
    stdout_bytes: int = 0
    stderr_bytes: int = 0
    expected_handler_count: int = 0
    stdout_artifact_path: str | None = None
    stderr_artifact_path: str | None = None
    exception: str | None = None
    handler_fired: list[dict[str, Any]] | None = None
    handler_ids: list[str] | None = None
    missing_handler_ids: list[str] | None = None
    cleanup_failed: bool = False


class PythonHost:
    def __init__(
        self,
        *,
        requests: Mailbox,
        control: Mailbox,
        ui: Emitter,
        results: Emitter,
        workspace: Path,
        output_dir: Path,
        max_timeout: float = 120,
        journal: Any | None = None,
        session_id: str = "",
    ) -> None:
        self._requests = requests
        self._control = control
        self._ui = ui
        self._results = results
        self.workspace = workspace
        self.output_dir = output_dir
        self.max_timeout = max_timeout
        self._journal = journal
        self.session_id = session_id
        self._pending: deque[Execution] = deque()
        self._records: dict[str, Execution] = {}
        self._done: set[str] = set()
        self._handlers: dict[str, int] = {}
        self._handler_snapshots: dict[int, dict[str, set[str]]] = {}
        self._current: Execution | None = None
        self._epoch = 0
        self._managed: ManagedProcess | None = None
        self._reader: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._stderr_buf = b""
        self._frame_q: queue.Queue[tuple[Any, ...]] = queue.Queue()
        self._alive = False
        self._starting = False
        self._executing = False
        self._closing = False
        self._wake: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._accept_task: asyncio.Task[None] | None = None
        self._control_task: asyncio.Task[None] | None = None
        self._scheduler_task: asyncio.Task[None] | None = None
        self.late_frames = 0
        self.cleanup_failed = False
        self.undelivered_terminals = 0
        self.undelivered_handler_results = 0
        # Diagnostics only: counts how often the scheduler left its idle wait.
        self.scheduler_wakeups = 0

    def _threads_snapshot(self) -> list[str]:
        return sorted(thread.name for thread in threading.enumerate())

    @property
    def idle(self) -> bool:
        return not self._executing and not self._pending and not self._starting and not self._closing

    @property
    def queue_size(self) -> int:
        return len(self._pending)

    @property
    def current_execution_id(self) -> str | None:
        current = self._current
        return current.event_id if current else None

    @property
    def worker_epoch(self) -> int:
        return self._epoch

    def runtime_facts(self) -> dict[str, Any]:
        """Live execution state. Summaries must not invent or replace this."""
        queued = [item.event_id for item in self._pending]
        current = self.current_execution_id
        unfinished = ([current] if current else []) + [item for item in queued if item != current]
        return {
            "worker_epoch": self._epoch,
            "cleanup_failed": self.cleanup_failed,
            "namespace": "not_started" if self._epoch == 0 else "current_epoch_only",
            "current_execution_id": current,
            "queued_execution_ids": queued,
            "unfinished_execution_ids": unfinished,
        }

    def managed_pids(self) -> list[int]:
        managed = self._managed
        if managed is None or managed.poll() is not None:
            return []
        return [managed.pid]

    def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._wake = asyncio.Event()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._accept_task = asyncio.create_task(self._accept_loop(), name="python-accept")
        self._control_task = asyncio.create_task(self._control_loop(), name="python-control")
        self._scheduler_task = asyncio.create_task(self._scheduler(), name="python-scheduler")

    def _signal_wake(self) -> None:
        """Wake the scheduler. Thread-safe; a no-op before start() or after close."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(self._set_wake)
        except RuntimeError:
            pass

    def _set_wake(self) -> None:
        if self._wake is not None:
            self._wake.set()

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._signal_wake()
        current = self._current
        if current is not None and current.event_id not in self._done:
            current.cancel = True
            current.cancel_reason = "shutdown"
            await asyncio.to_thread(self._kill_current, current)
        if self._scheduler_task is not None:
            try:
                await asyncio.wait_for(self._scheduler_task, timeout=8)
            except asyncio.TimeoutError:
                self.cleanup_failed = True
                self._scheduler_task.cancel()
        while self._pending:
            record = self._pending.popleft()
            if record.event_id not in self._done:
                self._emit_finished(
                    record,
                    ExecResult("cancelled", "", "execution cancelled: shutdown\n", 0, False, "shutdown", self._epoch),
                )
        await asyncio.to_thread(self._destroy_worker_blocking)
        if self._managed is not None and self._managed.poll() is None:
            self.cleanup_failed = True
        for task in (self._accept_task, self._control_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *[task for task in (self._accept_task, self._control_task) if task is not None],
            return_exceptions=True,
        )

    async def _accept_loop(self) -> None:
        while not self._closing:
            batch = await self._requests.take_batch(16)
            if not batch:
                return
            for event in batch:
                self._enqueue(event)

    async def _control_loop(self) -> None:
        while not self._closing:
            batch = await self._control.take_batch(16)
            if not batch:
                return
            for event in batch:
                await self._on_cancel(event)

    def _enqueue(self, event: Event) -> None:
        timeout = float(event.payload["timeout"])
        if timeout > self.max_timeout:
            timeout = self.max_timeout
        activation = event.payload.get("activation_id")
        tool_call = event.payload.get("tool_call_id")
        record = Execution(
            event_id=event.id,
            code=str(event.payload["code"]),
            timeout=timeout,
            activation_id=activation if isinstance(activation, str) else None,
            tool_call_id=tool_call if isinstance(tool_call, str) else None,
            queued_at=time.time(),
        )
        self._records[event.id] = record
        self._pending.append(record)
        self._journal_execution(record, "queued", reset=False)
        self._signal_wake()

    async def _on_cancel(self, event: Event) -> None:
        execution_id = str(event.payload["execution_id"])
        reason = str(event.payload.get("reason") or "cancel")
        record = self._records.get(execution_id)
        if record is None:
            self._emit_cancel_result(event, "not_found", execution_id)
            return
        if execution_id in self._done:
            self._emit_cancel_result(event, "already_finished", execution_id)
            return
        if record.state == "queued":
            self._pending = deque(item for item in self._pending if item.event_id != execution_id)
            self._emit_cancel_result(event, "requested", execution_id)
            self._emit_finished(
                record,
                ExecResult("cancelled", "", f"execution cancelled: {reason}\n", 0, False, "queued_cancel", self._epoch),
            )
            return
        if record.state == "running":
            self._emit_cancel_result(event, "requested", execution_id)
            record.cancel = True
            record.cancel_reason = reason
            await asyncio.to_thread(self._kill_current, record)

    async def _scheduler(self) -> None:
        try:
            await self._ensure_worker()
            while not self._closing:
                if self._pending and self._alive:
                    record = self._pending.popleft()
                    if record.event_id in self._done:
                        continue
                    record.state = "running"
                    self._current = record
                    self._executing = True
                    result = None
                    try:
                        result = await asyncio.to_thread(self._execute_blocking, record)
                        self._apply_result(record, result)
                    except Exception as exc:
                        result = ExecResult(
                            "failed",
                            "",
                            f"host error: {exc}\n",
                            0,
                            True,
                            "worker_exit",
                            self._epoch,
                        )
                        self._apply_result(record, result)
                    finally:
                        self._executing = False
                        self._current = None
                    if result is not None and result.namespace_reset and not self._closing:
                        self._cancel_queued("namespace_reset_before_start")
                        self._alive = False
                        await self._ensure_worker()
                    continue
                item = await self._wait_idle()
                if item == "stop" or self._closing:
                    break
                if item == "eof":
                    await self._handle_idle_death()
                elif self._pending and not self._alive:
                    # Pending work with no worker: recover instead of stalling in
                    # the idle wait if a spawn failed and nothing signalled.
                    await self._ensure_worker()
        finally:
            while self._pending:
                record = self._pending.popleft()
                if record.event_id not in self._done:
                    self._emit_finished(
                        record,
                        ExecResult(
                            "cancelled", "", "execution cancelled: shutdown\n", 0, False, "shutdown", self._epoch
                        ),
                    )

    async def _handle_idle_death(self) -> None:
        old = self._epoch
        self._alive = False
        await asyncio.to_thread(self._destroy_worker_blocking)
        if self._closing:
            return
        if self._pending:
            self._cancel_queued("namespace_reset_before_start")
            await self._ensure_worker()
            return
        await self._ensure_worker()
        self._results.call(
            "python.environment_changed",
            {"old_epoch": old, "new_epoch": self._epoch, "reason": "worker_exited"},
        )

    def _cancel_queued(self, reason: str) -> None:
        queued = list(self._pending)
        self._pending.clear()
        for record in queued:
            if record.event_id in self._done:
                continue
            self._emit_finished(
                record,
                ExecResult("cancelled", "", f"execution cancelled: {reason}\n", 0, True, reason, self._epoch),
            )

    async def _ensure_worker(self) -> None:
        if self._alive and self._managed is not None and self._managed.poll() is None:
            return
        self._starting = True
        try:
            await asyncio.to_thread(self._spawn_and_wait_ready)
            self._alive = True
        finally:
            self._starting = False

    def _spawn_and_wait_ready(self) -> None:
        self._destroy_worker_blocking()
        self._epoch += 1
        self._handlers.clear()
        self._handler_snapshots.clear()
        epoch = self._epoch
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._managed = spawn_worker(self.workspace, epoch, self.output_dir)
        managed = self._managed
        self._reader = threading.Thread(
            target=self._read_control,
            args=(managed.control, epoch),
            name="nervipulsa-worker-io",
            daemon=True,
        )
        self._reader.start()
        if managed.proc.stderr is not None:
            self._stderr_thread = threading.Thread(
                target=self._read_stderr,
                args=(managed.proc.stderr,),
                name="nervipulsa-worker-err",
                daemon=True,
            )
            self._stderr_thread.start()
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                item = self._frame_q.get(timeout=0.2)
            except queue.Empty:
                if managed.poll() is not None:
                    raise RuntimeError(self._boot_text() or "worker exited during startup")
                continue
            if item[0] == "eof":
                raise RuntimeError(self._boot_text() or "worker exited during startup")
            frame = item[2]
            if isinstance(frame, dict) and frame.get("kind") == "ready" and frame.get("worker_epoch") == epoch:
                return
            self.late_frames += 1
        raise RuntimeError("worker did not become ready")

    def _destroy_worker_blocking(self) -> None:
        self._alive = False
        managed = self._managed
        reader = self._reader
        if managed is not None:
            if not managed.terminate_tree():
                self.cleanup_failed = True
            try:
                managed.control.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                managed.control.close()
            except OSError:
                pass
            if managed.proc.stderr is not None:
                try:
                    managed.proc.stderr.close()
                except OSError:
                    pass
        if reader is not None and reader is not threading.current_thread():
            reader.join(timeout=2)
        if self._stderr_thread is not None and self._stderr_thread is not threading.current_thread():
            self._stderr_thread.join(timeout=1)
        self._drain_frames()
        self._reader = None
        self._stderr_thread = None

    def _drain_frames(self) -> None:
        while True:
            try:
                self._frame_q.get_nowait()
            except queue.Empty:
                return

    def _read_control(self, sock: socket.socket, epoch: int) -> None:
        try:
            while True:
                frame = read_frame(sock)
                if frame is None:
                    break
                self._frame_q.put(("frame", epoch, frame))
                self._signal_wake()
        except Exception as exc:
            self._frame_q.put(("eof", epoch, str(exc)))
            self._signal_wake()
            return
        self._frame_q.put(("eof", epoch, ""))
        self._signal_wake()

    def _read_stderr(self, pipe: Any) -> None:
        try:
            data = pipe.read()
        except Exception:
            data = b""
        if isinstance(data, str):
            data = data.encode("utf-8", "replace")
        self._stderr_buf = data or b""

    def _boot_text(self) -> str:
        if not self._stderr_buf:
            return ""
        return self._stderr_buf.decode("utf-8", "replace")[-4000:]

    def _route_handler_frame(self, frame: dict[str, Any], epoch: int) -> bool:
        handler_id = frame.get("handler_id")
        snapshot_id = frame.get("snapshot_id")
        trigger = frame.get("trigger")
        if (
            not isinstance(handler_id, str)
            or not isinstance(snapshot_id, str)
            or not isinstance(trigger, dict)
            or trigger.get("request_id") != snapshot_id
            or frame.get("worker_epoch") != epoch
            or epoch != self._epoch
            or handler_id not in self._handler_snapshots.get(epoch, {}).get(snapshot_id, set())
            or self._handlers.get(handler_id) != epoch
        ):
            self.late_frames += 1
            return False
        payload = {
            "handler_id": handler_id,
            "trigger": trigger,
            "result": str(frame.get("result", ""))[:4096],
            "worker_epoch": epoch,
        }
        delivery = self._results.call(
            "agent.handler_fired",
            payload,
            reply_to=snapshot_id,
            lane=Lane.HANDLER_RESULT,
            lane_key=snapshot_id,
        )
        if delivery.accepted:
            return True
        fallback = self._results.call("agent.handler_fired", payload, reply_to=snapshot_id)
        if fallback.accepted:
            return True
        self.undelivered_handler_results += 1
        self._ui.call(
            "agent.error",
            {
                "kind": "handler_result_undelivered",
                "message": (
                    f"handler result {handler_id} for {snapshot_id} was not delivered: "
                    f"{delivery.reason}; fallback: {fallback.reason}"
                ),
            },
        )
        return False

    def _drain_idle(self) -> str | None:
        """Consume worker signals with nothing executing. Never blocks.

        Returns "eof" when the current worker epoch has exited, "stop" when the
        host is closing, and None otherwise. Frames that arrive with no request
        in flight are unmatched by definition and counted as late.
        """
        saw_eof = False
        while True:
            try:
                item = self._frame_q.get_nowait()
            except queue.Empty:
                break
            if item[0] == "eof":
                if item[1] == self._epoch:
                    saw_eof = True
                continue
            frame = item[2]
            if isinstance(frame, dict) and frame.get("kind") == "handler.fired":
                self._route_handler_frame(frame, item[1])
            else:
                self.late_frames += 1
        if self._closing:
            return "stop"
        return "eof" if saw_eof else None

    async def _wait_idle(self) -> str | None:
        """Wait for work, a worker signal, or the bounded safety tick."""
        while True:
            if self._wake is not None:
                self._wake.clear()
            drained = self._drain_idle()
            if drained is not None:
                return drained
            if self._pending:
                return None
            self.scheduler_wakeups += 1
            try:
                if self._wake is not None:
                    await asyncio.wait_for(self._wake.wait(), timeout=_IDLE_TICK)
                else:  # pragma: no cover - start() always creates the event
                    await asyncio.sleep(_IDLE_TICK)
            except (asyncio.TimeoutError, TimeoutError):
                pass

    def _kill_current(self, record: Execution | None = None) -> bool:
        managed = self._managed
        if managed is None:
            return True
        cleanup_ok = managed.terminate_tree()
        if not cleanup_ok:
            self.cleanup_failed = True
            if record is not None:
                record.cleanup_failed = True
        try:
            managed.control.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        return cleanup_ok

    def _execute_blocking(self, record: Execution) -> ExecResult:
        epoch = self._epoch
        record.worker_epoch = epoch
        send_at = time.monotonic()
        try:
            assert self._managed is not None
            write_frame(
                self._managed.control,
                {
                    "kind": "execute",
                    "request_id": record.event_id,
                    "code": record.code,
                    "timeout": record.timeout,
                },
            )
        except OSError as exc:
            # sendall failed, so the worker never received a complete frame and
            # the code cannot have started: this is a namespace reset, not a run.
            return ExecResult(
                "cancelled",
                "",
                f"execution cancelled: namespace_reset_before_start ({exc})\n",
                0,
                True,
                "namespace_reset_before_start",
                epoch,
            )
        started = False
        handler_fired: list[dict[str, Any]] = []
        handler_snapshot_ids: list[str] | None = None
        start = send_at
        deadline = send_at + 10
        while True:
            if record.cancel:
                self._kill_current(record)
                return self._cancelled_result(
                    record,
                    start if started else send_at,
                    epoch,
                    handler_snapshot_ids,
                    cleanup_failed=record.cleanup_failed,
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if not started:
                    self._kill_current(record)
                    return ExecResult(
                        "failed",
                        "",
                        self._boot_text() or "worker did not start\n",
                        self._elapsed_ms(send_at),
                        True,
                        "worker_exit",
                        epoch,
                        cleanup_failed=record.cleanup_failed,
                    )
                record.timed_out = True
                self._kill_current(record)
                return self._timeout_result(
                    record,
                    start,
                    epoch,
                    handler_snapshot_ids,
                    cleanup_failed=record.cleanup_failed,
                )
            try:
                item = self._frame_q.get(timeout=min(0.5, max(0.05, remaining)))
            except queue.Empty:
                continue
            if item[0] == "eof" or item[1] != epoch:
                if item[0] != "eof":
                    self.late_frames += 1
                    continue
                if record.cancel:
                    return self._cancelled_result(
                        record,
                        start if started else send_at,
                        epoch,
                        handler_snapshot_ids,
                        cleanup_failed=record.cleanup_failed,
                    )
                if record.timed_out:
                    return self._timeout_result(
                        record,
                        start,
                        epoch,
                        handler_snapshot_ids,
                        cleanup_failed=record.cleanup_failed,
                    )
                return ExecResult(
                    "failed",
                    "",
                    self._boot_text() or "worker process exited\n",
                    self._elapsed_ms(start if started else send_at),
                    True,
                    "worker_exit",
                    epoch,
                    expected_handler_count=(
                        len(handler_snapshot_ids) if handler_snapshot_ids is not None else 0
                    ),
                    handler_ids=handler_snapshot_ids,
                    missing_handler_ids=(
                        list(handler_snapshot_ids) if handler_snapshot_ids is not None else None
                    ),
                )
            frame = item[2]
            if isinstance(frame, dict) and frame.get("kind") == "handler.fired":
                handler_fired.append(frame)
                continue
            if not isinstance(frame, dict) or frame.get("request_id") != record.event_id:
                self.late_frames += 1
                continue
            kind = frame.get("kind")
            if kind == "handler.snapshot":
                snapshot = frame.get("handler_ids")
                expected = frame.get("expected_handler_count")
                valid_snapshot = (
                    handler_snapshot_ids is None
                    and frame.get("worker_epoch") == epoch
                    and isinstance(snapshot, list)
                    and len(snapshot) <= MAX_HANDLER_RESULTS_PER_EXECUTION
                    and all(
                        isinstance(item, str) and len(item) == 12 and item.isalnum()
                        for item in snapshot
                    )
                    and len(set(snapshot)) == len(snapshot)
                    and isinstance(expected, int)
                    and not isinstance(expected, bool)
                    and expected == len(snapshot)
                )
                if valid_snapshot:
                    handler_snapshot_ids = list(snapshot)
                else:
                    self.late_frames += 1
                continue
            if kind == "started" and not started:
                started = True
                start = time.monotonic()
                record.started_monotonic = start
                deadline = start + record.timeout
                self._emit_started(record, epoch)
                continue
            if kind == "finished":
                if record.cancel:
                    self.late_frames += 1
                    continue
                status = frame.get("status") if frame.get("status") in {"succeeded", "failed"} else "failed"
                stdout = frame.get("stdout") if isinstance(frame.get("stdout"), str) else ""
                stderr = frame.get("stderr") if isinstance(frame.get("stderr"), str) else ""
                stdout_bytes = frame.get("stdout_bytes")
                stderr_bytes = frame.get("stderr_bytes")
                if not isinstance(stdout_bytes, int) or isinstance(stdout_bytes, bool):
                    stdout_bytes = len(stdout.encode("utf-8"))
                if not isinstance(stderr_bytes, int) or isinstance(stderr_bytes, bool):
                    stderr_bytes = len(stderr.encode("utf-8"))
                stdout_artifact = frame.get("stdout_artifact_path")
                stderr_artifact = frame.get("stderr_artifact_path")
                snapshot = frame.get("handler_ids")
                expected = frame.get("expected_handler_count")
                valid_ids = (
                    isinstance(snapshot, list)
                    and len(snapshot) <= MAX_HANDLER_RESULTS_PER_EXECUTION
                    and all(isinstance(item, str) and len(item) == 12 and item.isalnum() for item in snapshot)
                    and len(set(snapshot)) == len(snapshot)
                    and isinstance(expected, int)
                    and not isinstance(expected, bool)
                    and expected == len(snapshot)
                    and (handler_snapshot_ids is None or snapshot == handler_snapshot_ids)
                )
                handler_ids = list(snapshot) if valid_ids else handler_snapshot_ids
                valid_handler_frames: list[dict[str, Any]] = []
                seen_handler_ids: set[str] = set()
                if handler_ids is not None:
                    if valid_ids:
                        for fired in handler_fired:
                            handler_id = fired.get("handler_id")
                            trigger = fired.get("trigger")
                            if (
                                isinstance(handler_id, str)
                                and handler_id in handler_ids
                                and handler_id not in seen_handler_ids
                                and fired.get("snapshot_id") == record.event_id
                                and fired.get("worker_epoch") == epoch
                                and isinstance(trigger, dict)
                                and trigger.get("request_id") == record.event_id
                            ):
                                valid_handler_frames.append(fired)
                                seen_handler_ids.add(handler_id)
                            else:
                                self.late_frames += 1
                    else:
                        self.late_frames += len(handler_fired)
                    missing_handler_ids = [
                        handler_id for handler_id in handler_ids if handler_id not in seen_handler_ids
                    ]
                    self._handler_snapshots.setdefault(epoch, {})[record.event_id] = set(handler_ids)
                    self._handlers = {
                        handler_id: handler_epoch
                        for handler_id, handler_epoch in self._handlers.items()
                        if handler_epoch != epoch
                    }
                    self._handlers.update({handler_id: epoch for handler_id in handler_ids})
                else:
                    missing_handler_ids = None
                    self.late_frames += len(handler_fired)
                return ExecResult(
                    status,
                    stdout,
                    stderr,
                    self._elapsed_ms(start),
                    False,
                    "exception" if status == "failed" else "",
                    epoch,
                    bool(frame.get("truncated")),
                    stdout_bytes=stdout_bytes,
                    stderr_bytes=stderr_bytes,
                expected_handler_count=(len(handler_ids) if handler_ids is not None else 0),
                    stdout_artifact_path=(
                        stdout_artifact if isinstance(stdout_artifact, str) else None
                    ),
                    stderr_artifact_path=(
                        stderr_artifact if isinstance(stderr_artifact, str) else None
                    ),
                    handler_fired=valid_handler_frames,
                    handler_ids=handler_ids,
                    missing_handler_ids=missing_handler_ids,
                    exception=(
                        frame.get("exception")
                        if isinstance(frame.get("exception"), str)
                        else None
                    ),
                )

    def _cancelled_result(
        self,
        record: Execution,
        start: float,
        epoch: int,
        handler_ids: list[str] | None = None,
        *,
        cleanup_failed: bool = False,
    ) -> ExecResult:
        reason = record.cancel_reason or "cancel"
        return ExecResult(
            "cancelled",
            "",
            f"execution cancelled: {reason}\n",
            self._elapsed_ms(start),
            True,
            reason,
            epoch,
            expected_handler_count=(len(handler_ids) if handler_ids is not None else 0),
            handler_ids=handler_ids,
            missing_handler_ids=(list(handler_ids) if handler_ids is not None else None),
            cleanup_failed=cleanup_failed,
        )

    def _timeout_result(
        self,
        record: Execution,
        start: float,
        epoch: int,
        handler_ids: list[str] | None = None,
        *,
        cleanup_failed: bool = False,
    ) -> ExecResult:
        return ExecResult(
            "timeout",
            "",
            "execution timed out; the worker was restarted and the namespace was reset\n",
            self._elapsed_ms(start),
            True,
            "timeout",
            epoch,
            expected_handler_count=(len(handler_ids) if handler_ids is not None else 0),
            handler_ids=handler_ids,
            missing_handler_ids=(list(handler_ids) if handler_ids is not None else None),
            cleanup_failed=cleanup_failed,
        )

    @staticmethod
    def _elapsed_ms(start: float) -> int:
        return max(0, int((time.monotonic() - start) * 1000))

    def _emit_started(self, record: Execution, epoch: int) -> None:
        def _do() -> None:
            record.started_at = time.time()
            self._ui.call(
                "python.started",
                {"execution_id": record.event_id, "worker_epoch": epoch},
                reply_to=record.event_id,
            )
            self._journal_execution(record, "running", reset=False, epoch=epoch)

        loop = self._loop
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(_do)
        except RuntimeError:
            pass

    def _apply_result(self, record: Execution, result: ExecResult) -> None:
        delivered_handler_ids: set[str] = set()
        try:
            if result.status not in {"timeout", "cancelled"}:
                seen_handler_ids: set[str] = set()
                for frame in result.handler_fired or []:
                    handler_id = frame.get("handler_id")
                    if not isinstance(handler_id, str) or handler_id in seen_handler_ids:
                        self.late_frames += 1
                        continue
                    seen_handler_ids.add(handler_id)
                    if self._route_handler_frame(frame, result.worker_epoch):
                        delivered_handler_ids.add(handler_id)
            if result.handler_ids is not None:
                result.missing_handler_ids = [
                    handler_id
                    for handler_id in result.handler_ids
                    if handler_id not in delivered_handler_ids
                ]
        finally:
            snapshots = self._handler_snapshots.get(result.worker_epoch)
            if snapshots is not None:
                snapshots.pop(record.event_id, None)
                if not snapshots:
                    self._handler_snapshots.pop(result.worker_epoch, None)
        self._emit_finished(record, result)

    def _emit_finished(self, record: Execution, result: ExecResult) -> None:
        if record.event_id in self._done:
            self.late_frames += 1
            return
        self._done.add(record.event_id)
        record.state = "done"
        record.ended_at = time.time()
        stdout = result.stdout[:_PREVIEW]
        stderr = result.stderr[:_PREVIEW]
        stdout_bytes = result.stdout_bytes or len(result.stdout.encode("utf-8"))
        stderr_bytes = result.stderr_bytes or len(result.stderr.encode("utf-8"))
        truncated = bool(
            result.truncated
            or result.stdout_artifact_path
            or result.stderr_artifact_path
            or len(result.stdout) > _PREVIEW
            or len(result.stderr) > _PREVIEW
        )
        payload: dict[str, Any] = {
            "status": result.status,
            "stdout": stdout,
            "stderr": stderr,
            "stdout_bytes": stdout_bytes,
            "stderr_bytes": stderr_bytes,
            "duration_ms": int(result.duration_ms),
            "worker_epoch": int(result.worker_epoch),
            "namespace_reset": bool(result.namespace_reset),
            "expected_handler_count": max(0, int(result.expected_handler_count)),
            "handler_result_status": (
                "unknown"
                if result.handler_ids is None
                else "incomplete"
                if result.missing_handler_ids
                else "complete"
            ),
            "truncated": truncated,
        }
        if result.cleanup_failed:
            payload["process_tree_cleanup_failed"] = True
        if result.reason:
            payload["reason"] = result.reason
        if result.missing_handler_ids is not None:
            payload["missing_handler_ids"] = list(result.missing_handler_ids)
        if result.stdout_artifact_path:
            payload["stdout_artifact_path"] = result.stdout_artifact_path
        if result.stderr_artifact_path:
            payload["stderr_artifact_path"] = result.stderr_artifact_path
        if result.exception:
            payload["exception"] = result.exception
        delivery = self._results.call(
            "python.finished",
            payload,
            reply_to=record.event_id,
            lane=Lane.RESERVED_RESULT,
            lane_key=record.event_id,
        )
        if not delivery.accepted:
            self.undelivered_terminals += 1
            self._ui.call(
                "agent.error",
                {
                    "kind": "result_undelivered",
                    "message": f"terminal for {record.event_id} was not delivered: {delivery.reason}",
                },
            )
        self._journal_execution(
            record,
            result.status,
            reset=result.namespace_reset,
            epoch=result.worker_epoch,
            metadata={
                "reason": result.reason,
                "truncated": truncated,
                "stdout_bytes": stdout_bytes,
                "stderr_bytes": stderr_bytes,
                "stdout_artifact_path": result.stdout_artifact_path,
                "stderr_artifact_path": result.stderr_artifact_path,
                "tool_call_id": record.tool_call_id,
            },
        )

    def _emit_cancel_result(self, event: Event, status: str, execution_id: str) -> None:
        self._ui.call(
            "python.cancel_result",
            {"status": status, "execution_id": execution_id},
            reply_to=event.id,
        )

    def _journal_execution(
        self,
        record: Execution,
        status: str,
        *,
        reset: bool,
        epoch: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        journal = self._journal
        if journal is None:
            return
        journal.execution(
            {
                "execution_id": record.event_id,
                "session_id": self.session_id,
                "activation_id": record.activation_id,
                "worker_epoch": self._epoch if epoch is None else epoch,
                "status": status,
                "queued_at": record.queued_at,
                "started_at": record.started_at,
                "ended_at": record.ended_at,
                "namespace_reset": int(bool(reset)),
                "metadata": metadata or {},
            }
        )
