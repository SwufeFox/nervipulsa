"""Persistent Python worker.

Control frames travel on an inherited socket. User stdout and stderr are
redirected onto private pipes so print() and child-process output cannot
corrupt the control channel. The namespace survives successful calls and
ordinary exceptions. The host, not this process, decides timeouts.
"""

from __future__ import annotations

import argparse
import io
import os
import socket
import sys
import threading
import time
import traceback
import uuid
from pathlib import Path

from nervipulsa.events import MAX_HANDLER_RESULTS_PER_EXECUTION
from nervipulsa.framing import read_frame, write_frame

PREVIEW_BYTES = 4 * 1024
EXCEPTION_PREVIEW_CHARS = 512


class PipeCapture:
    """Drain a pipe continuously, including after the stored preview is full."""

    def __init__(self, read_fd: int, limit: int = PREVIEW_BYTES) -> None:
        self.read_fd = read_fd
        self.limit = limit
        self._lock = threading.Lock()
        self._memory = bytearray()
        self._total = 0
        self._pending = bytearray()
        self._sentinel: bytes | None = None
        self._found = threading.Event()
        self._file: io.BufferedWriter | None = None
        self._path: Path | None = None
        self._stop = False
        self._thread = threading.Thread(target=self._run, name="nervipulsa-capture", daemon=True)
        self._thread.start()

    def begin(self, path: Path | None) -> None:
        with self._lock:
            leftover = bytes(self._pending)
            self._pending.clear()
            self._close_file_locked()
            self._memory.clear()
            self._total = 0
            self._sentinel = None
            self._path = path
            self._found.clear()
            if leftover:
                self._store_locked(leftover)

    def collect(self, write_fd: int) -> tuple[str, int, bool, str | None]:
        token = b"\0NV" + uuid.uuid4().bytes + b"\0"
        with self._lock:
            self._sentinel = token
            self._found.clear()
        os.write(write_fd, token)
        if not self._found.wait(timeout=5):
            with self._lock:
                if self._sentinel and self._sentinel in self._pending:
                    self._consume_locked(bytes(self._pending))
                    self._pending.clear()
            if not self._found.is_set():
                raise TimeoutError("output pipe was not drained")
        with self._lock:
            preview = bytes(self._memory)
            total = self._total
            artifact = str(self._path) if self._path is not None and total > self.limit else None
            self._close_file_locked()
            self._memory.clear()
            self._total = 0
            self._sentinel = None
            self._path = None
            self._found.clear()
        return preview.decode("utf-8", errors="replace"), total, total > self.limit, artifact

    def _run(self) -> None:
        while not self._stop:
            try:
                data = os.read(self.read_fd, 65536)
            except OSError:
                break
            if not data:
                break
            self._add(data)

    def _add(self, data: bytes) -> None:
        with self._lock:
            if self._sentinel is None:
                self._store_locked(data)
                return
            self._pending.extend(data)
            if self._sentinel not in self._pending:
                keep = len(self._sentinel) - 1
                if len(self._pending) > keep:
                    self._store_locked(bytes(self._pending[:-keep]))
                    self._pending = self._pending[-keep:]
                return
            self._consume_locked(bytes(self._pending))

    def _consume_locked(self, pending: bytes) -> None:
        assert self._sentinel is not None
        index = pending.find(self._sentinel)
        if index < 0:
            self._store_locked(pending)
            self._pending.clear()
            return
        self._store_locked(pending[:index])
        self._pending = bytearray(pending[index + len(self._sentinel) :])
        self._found.set()

    def _store_locked(self, data: bytes) -> None:
        if not data:
            return
        if self._file is None and self._path is not None and len(self._memory) + len(data) > self.limit:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self._path.open("wb")
            self._file.write(self._memory)
        if self._file is not None:
            self._file.write(data)
        room = self.limit - len(self._memory)
        if room > 0:
            self._memory.extend(data[:room])
        self._total += len(data)

    def _close_file_locked(self) -> None:
        if self._file is not None:
            self._file.flush()
            self._file.close()
            self._file = None


def _install_stdio() -> tuple[PipeCapture, PipeCapture]:
    out_read, out_write = os.pipe()
    err_read, err_write = os.pipe()
    os.dup2(out_write, 1)
    os.dup2(err_write, 2)
    os.close(out_write)
    os.close(err_write)
    sys.stdout = io.TextIOWrapper(
        io.BufferedWriter(io.FileIO(1, mode="wb", closefd=False)),
        encoding="utf-8",
        errors="replace",
        newline="\n",
        write_through=True,
        line_buffering=True,
    )
    sys.stderr = io.TextIOWrapper(
        io.BufferedWriter(io.FileIO(2, mode="wb", closefd=False)),
        encoding="utf-8",
        errors="replace",
        newline="\n",
        write_through=True,
        line_buffering=True,
    )
    return PipeCapture(out_read), PipeCapture(err_read)


def _control_socket() -> socket.socket:
    fd = int(os.environ["NERVIPULSA_CONTROL_FD"])
    sock = socket.socket(fileno=fd)
    try:
        sock.set_inheritable(False)
    except OSError:
        pass
    sock.setblocking(True)
    return sock


def run_worker(workspace: Path, epoch: int, output_dir: Path) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    sock = _control_socket()
    socket_write_lock = threading.Lock()

    def send_frame(frame: dict[str, object]) -> None:
        with socket_write_lock:
            write_frame(sock, frame)
    stdout, stderr = _install_stdio()
    handlers: dict[str, object] = {}

    def on_finished(callback: object) -> str:
        if not callable(callback):
            raise TypeError("handler must be callable")
        if len(handlers) >= MAX_HANDLER_RESULTS_PER_EXECUTION:
            raise RuntimeError(
                f"at most {MAX_HANDLER_RESULTS_PER_EXECUTION} active finished handlers are allowed"
            )
        handler_id = uuid.uuid4().hex[:12]
        handlers[handler_id] = callback
        return handler_id

    def off_finished(handler_id: str) -> bool:
        """Unregister a handler previously returned by on_finished()."""
        if not isinstance(handler_id, str):
            return False
        return handlers.pop(handler_id, None) is not None

    namespace: dict[str, object] = {"__name__": "__main__", "on_finished": on_finished, "off_finished": off_finished}
    send_frame({"kind": "ready", "worker_epoch": epoch})
    while True:
        message = read_frame(sock)
        if message is None:
            return
        if message.get("kind") != "execute":
            continue
        request_id = str(message.get("request_id") or "")
        code = message.get("code")
        if not isinstance(code, str):
            code = ""
        try:
            os.chdir(workspace)
        except OSError as exc:
            send_frame(
                {
                    "kind": "finished",
                    "request_id": request_id,
                    "status": "failed",
                    "stdout": "",
                    "stderr": f"{exc}\n",
                    "duration_ms": 0,
                    "truncated": False,
                    "stdout_bytes": 0,
                    "stderr_bytes": len(f"{exc}\n".encode("utf-8")),
                    "stdout_artifact_path": None,
                    "stderr_artifact_path": None,
                    "worker_epoch": epoch,
                },
            )
            continue
        stdout.begin(output_dir / f"{request_id}.stdout.log")
        stderr.begin(output_dir / f"{request_id}.stderr.log")
        send_frame({"kind": "started", "request_id": request_id, "worker_epoch": epoch})
        status = "succeeded"
        exception: str | None = None
        started = time.monotonic()
        try:
            compiled = compile(code, "<python_exec>", "exec")
            exec(compiled, namespace, namespace)
        except BaseException as exc:
            status = "failed"
            try:
                name = type(exc).__name__
                prefix = f"{name}: "
                if len(prefix) > 128:
                    prefix = f"{prefix[:127]}…"
                message = str(exc)
                message_budget = EXCEPTION_PREVIEW_CHARS - len(prefix)
                if len(message) <= message_budget:
                    exception = f"{prefix}{message}"
                else:
                    marker = "\n…[exception text truncated]…\n"
                    content_budget = message_budget - len(marker)
                    head = content_budget // 2
                    tail = content_budget - head
                    exception = f"{prefix}{message[:head]}{marker}{message[-tail:]}"
            except BaseException:
                exception = type(exc).__name__[:EXCEPTION_PREVIEW_CHARS]
            traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
        sys.stdout.flush()
        sys.stderr.flush()
        try:
            out_text, out_bytes, out_truncated, out_artifact = stdout.collect(1)
            err_text, err_bytes, err_truncated, err_artifact = stderr.collect(2)
        except TimeoutError as exc:
            out_text, out_bytes, out_truncated, out_artifact = "", 0, False, None
            err_text, err_bytes, err_truncated, err_artifact = (
                f"{exc}\n",
                len(f"{exc}\n".encode("utf-8")),
                False,
                None,
            )
            status = "failed"
        handler_snapshot = tuple(handlers.items())
        handler_ids = [handler_id for handler_id, _ in handler_snapshot]
        expected_handler_count = len(handler_snapshot)
        send_frame(
            {
                "kind": "handler.snapshot",
                "request_id": request_id,
                "worker_epoch": epoch,
                "handler_ids": handler_ids,
                "expected_handler_count": expected_handler_count,
            }
        )
        observation = {"request_id": request_id, "status": status, "stdout": out_text[:1024]}
        fired_frames: list[dict[str, object]] = []
        for handler_id, callback in handler_snapshot:
            try:
                result = callback(observation)  # type: ignore[operator]
                fired_frames.append({"kind": "handler.fired", "handler_id": handler_id, "snapshot_id": request_id, "worker_epoch": epoch, "trigger": observation, "result": str(result)[:4096]})
            except BaseException as exc:
                fired_frames.append({"kind": "handler.fired", "handler_id": handler_id, "snapshot_id": request_id, "worker_epoch": epoch, "trigger": observation, "result": f"handler error: {type(exc).__name__}: {exc}"[:4096]})
        for frame in fired_frames:
            send_frame(frame)
        duration_ms = max(0, int((time.monotonic() - started) * 1000))
        send_frame({
            "kind": "finished", "request_id": request_id, "status": status,
            "exception": exception, "stdout": out_text, "stderr": err_text,
            "duration_ms": duration_ms, "expected_handler_count": expected_handler_count,
            "handler_ids": handler_ids, "stdout_bytes": out_bytes, "stderr_bytes": err_bytes,
            "truncated": bool(out_truncated or err_truncated),
            "stdout_artifact_path": out_artifact, "stderr_artifact_path": err_artifact,
            "worker_epoch": epoch,
        })


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Nervipulsa persistent Python worker")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--epoch", required=True, type=int)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)
    try:
        run_worker(Path(args.workspace), args.epoch, Path(args.output_dir))
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
