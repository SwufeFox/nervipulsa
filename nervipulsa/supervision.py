"""Private process supervision for opt-in, one-shot CLI deadlines.

The parent owns the deadline, captures stdout until the interpreter has exited,
starts the runtime only after containment is installed, and never runs a model.
A POSIX worker is registered while still in the runtime's original group, then
released to establish its own session. Windows uses a strict, parent-held job.
This is process cleanup, not a sandbox or a hard real-time scheduling guarantee.
"""

from __future__ import annotations

import argparse
import asyncio
import atexit
import base64
import codecs
import json
import math
import os
import select
import signal
import socket
import struct
import subprocess
import sys
import threading
import tempfile
import time
import uuid
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from .config import Settings
from .framing import FrameError, MAX_FRAME_BYTES, read_frame, write_frame
from .process_tree import (
    _assign_windows_job,
    close_windows_job,
    filtered_environment,
    peek_returncode,
    terminate_windows_job,
    windows_job_active_processes,
)

_CLEANUP_SECONDS = 5.0
_active_supervisor: _SupervisorClient | None = None


def worker_supervisor() -> _SupervisorClient | None:
    """Return the private runtime-child supervisor; ordinary runs have none."""
    return _active_supervisor


class _SupervisorClient:
    def __init__(self, control: socket.socket) -> None:
        self.control = control
        self.lock = threading.Lock()
        self.workers: dict[int, Any] = {}

    def _request(self, kind: str, pid: int) -> None:
        with self.lock:
            write_frame(self.control, {"kind": kind, "pid": pid})
            reply = read_frame(self.control)
            if reply != {"kind": "ack", "operation": kind, "pid": pid}:
                raise RuntimeError("process supervisor refused worker group ownership")

    def register(self, managed: Any) -> None:
        self.workers[managed.pid] = managed
        self._request("register", managed.pid)

    def unregister(self, pid: int) -> None:
        self._request("unregister", pid)
        self.workers.pop(pid, None)

    def stop_worker(self, managed: Any) -> bool:
        deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            try:
                os.killpg(managed.pid, signal.SIGKILL)
            except ProcessLookupError:
                # A failed/gated bootstrap has not left the root group yet.
                pass
            # Popen.kill/send_signal can internally poll and reap on POSIX;
            # signal directly so the leader identity stays pinned instead.
            try:
                os.kill(managed.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            while peek_returncode(managed.proc) is None or _live_posix_groups({managed.pid}):
                if time.monotonic() >= deadline:
                    return False
                time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
            self.unregister(managed.pid)
            managed._supervisor = None
            managed.proc.wait(timeout=max(0.0, deadline - time.monotonic()))
            return managed._cleanup_sandbox()
        except (OSError, RuntimeError, subprocess.TimeoutExpired):
            # Keep ownership and the unreaped leader on failure. The outer
            # supervisor must still be able to terminate this group safely.
            return False

    def close(self) -> None:
        for managed in list(self.workers.values()):
            managed.terminate_tree()
        self.control.close()


class _Messages:
    """Incremental framing: a partial frame must not defeat the deadline."""

    def __init__(self) -> None:
        self.buffer = bytearray()
        self.closed = False

    def receive(self, control: socket.socket) -> list[dict[str, Any]]:
        data = control.recv(65536)
        if not data:
            self.closed = True
            if self.buffer:
                raise FrameError("supervisor channel closed within a frame")
            return []
        self.buffer.extend(data)
        messages = []
        while len(self.buffer) >= 4:
            size = struct.unpack_from(">I", self.buffer)[0]
            if size > MAX_FRAME_BYTES:
                raise FrameError("supervisor control frame exceeds 1 MiB")
            if len(self.buffer) < size + 4:
                break
            payload = json.loads(self.buffer[4:size + 4])
            del self.buffer[:size + 4]
            if not isinstance(payload, dict):
                raise FrameError("supervisor control frame must be an object")
            messages.append(payload)
        return messages


class _Output:
    def __init__(self, pipe: Any, *, stderr: bool = False) -> None:
        self.pipe = pipe
        self.stderr = stderr
        self.capture = tempfile.TemporaryFile(buffering=0) if not stderr else None
        self.stopped = threading.Event()
        self.complete = threading.Event()
        self.thread = threading.Thread(target=self._read, name="nervipulsa-supervised-output", daemon=True)
        self.thread.start()

    def _read(self) -> None:
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        try:
            while not self.stopped.is_set():
                data = os.read(self.pipe.fileno(), 65536)
                if not data:
                    if self.stderr:
                        remainder = decoder.decode(b"", final=True)
                        if remainder:
                            sys.stderr.write(remainder)
                            sys.stderr.flush()
                    return
                if self.stderr:
                    sys.stderr.write(decoder.decode(data))
                    sys.stderr.flush()
                else:
                    self.capture.write(data)
        except (OSError, ValueError):
            return
        finally:
            self.complete.set()

    def close(self, deadline: float) -> bool:
        self.thread.join(timeout=max(0.0, deadline - time.monotonic()))
        finished = self.complete.is_set()
        self.stopped.set()
        try:
            self.pipe.close()
        except OSError:
            return False
        return finished


def _linux_identity(pid: int) -> str | None:
    if not sys.platform.startswith("linux"):
        return None
    try:
        text = Path(f"/proc/{pid}/stat").read_text()
        return text[text.rfind(")") + 2:].split()[19]
    except (OSError, IndexError):
        return None


def _live_posix_groups(groups: set[int]) -> bool:
    """Distinguish stopped trees from unreaped orphan zombies on Linux."""
    if sys.platform.startswith("linux"):
        try:
            with os.scandir("/proc") as entries:
                for entry in entries:
                    if not entry.name.isdecimal():
                        continue
                    try:
                        text = Path(entry.path, "stat").read_text()
                        fields = text[text.rfind(")") + 2:].split()
                        if int(fields[2]) in groups and fields[0] not in {"Z", "X"}:
                            return True
                    except FileNotFoundError:
                        continue
                    except (OSError, IndexError, ValueError):
                        return True
            return False
        except OSError:
            return True
    for pgid in groups:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            continue
        except OSError:
            return True
        return True
    return False


class _Tree:
    def __init__(self) -> None:
        self.proc: subprocess.Popen[bytes] | None = None
        self.job: Any = None
        self.groups: dict[int, str | None] = {}
        self.result: bool | None = None

    def message(self, message: dict[str, Any], control: socket.socket, deadline: float) -> None:
        if self.proc is None or os.name == "nt":
            raise RuntimeError("unexpected worker registration")
        kind = message.get("kind")
        pid = message.get("pid")
        if type(pid) is not int or pid <= 0 or pid == self.proc.pid:
            raise RuntimeError("invalid worker group registration")
        if kind == "register":
            if pid in self.groups or os.getpgid(pid) != self.proc.pid or os.getsid(pid) != self.proc.pid:
                raise RuntimeError("worker left the root group before registration")
            self.groups[pid] = _linux_identity(pid)
        elif kind == "unregister":
            if pid not in self.groups:
                raise RuntimeError("worker group was not registered")
            # Retire ownership before acknowledging. Only then may the runtime
            # reap the leader and allow that numeric PGID to be reused.
            del self.groups[pid]
        else:
            raise RuntimeError("unexpected process supervisor message")
        control.settimeout(max(0.001, deadline - time.monotonic()))
        write_frame(control, {"kind": "ack", "operation": kind, "pid": pid})

    def stop(self, deadline: float) -> bool:
        if self.result is not None:
            return self.result
        self.result = False
        proc = self.proc
        if proc is None:
            self.result = True
            return True
        ok = True
        if os.name == "nt":
            if self.job is not None:
                try:
                    ok = terminate_windows_job(self.job)
                    while windows_job_active_processes(self.job):
                        if time.monotonic() >= deadline:
                            ok = False
                            break
                        time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
                except OSError:
                    ok = False
                finally:
                    try:
                        close_windows_job(self.job)
                    except OSError:
                        ok = False
                    self.job = None
            else:
                # Strict job assignment failed with the runtime gate closed;
                # this child has never imported or started the runtime.
                try:
                    proc.kill()
                except OSError:
                    ok = False
        else:
            groups = {proc.pid, *self.groups}
            # Kill the root group first: unregistered and gated workers still
            # belong to it. Then stop every acknowledged independent group.
            for pgid in [proc.pid, *self.groups]:
                identity = self.groups.get(pgid)
                current = _linux_identity(pgid) if identity is not None else None
                if identity is not None and current is not None and current != identity:
                    # This identity has already gone; never signal a reused PID.
                    groups.discard(pgid)
                    continue
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
                    groups.discard(pgid)
                except OSError:
                    ok = False
            while _live_posix_groups(groups):
                if time.monotonic() >= deadline:
                    ok = False
                    break
                time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
            self.groups.clear()
        try:
            proc.wait(timeout=max(0.0, deadline - time.monotonic()))
        except (OSError, subprocess.TimeoutExpired):
            ok = False
        self.result = ok and proc.returncode is not None
        return self.result


def _send_configuration(control: socket.socket, configuration: dict[str, Any], deadline: float) -> None:
    # Chunk the private configuration so long prompts do not acquire the
    # worker protocol's per-frame limit. Nothing sensitive enters argv/files.
    data = json.dumps(configuration, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    for offset in range(0, len(data), 49152):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("process deadline reached during child startup")
        control.settimeout(remaining)
        write_frame(control, {"kind": "configuration", "data": base64.b64encode(data[offset:offset + 49152]).decode("ascii")})
    control.settimeout(max(0.001, deadline - time.monotonic()))
    write_frame(control, {"kind": "start"})


def _receive_configuration(control: socket.socket) -> dict[str, Any]:
    data = bytearray()
    while True:
        frame = read_frame(control)
        if frame is None:
            raise RuntimeError("supervisor closed the runtime startup gate")
        if frame.get("kind") == "start":
            configuration = json.loads(data)
            if not isinstance(configuration, dict):
                raise RuntimeError("invalid private runtime configuration")
            return configuration
        if frame.get("kind") != "configuration" or not isinstance(frame.get("data"), str):
            raise RuntimeError("invalid private runtime configuration frame")
        data.extend(base64.b64decode(frame["data"], validate=True))


def _failure_payload(settings: Settings, workspace: Path, session_id: str, *, code: int, error: str) -> dict[str, Any]:
    return {
        "ok": False, "status": "timeout" if code == 124 else "failed", "exit_code": code,
        "session_id": session_id, "provider": settings.provider, "model": settings.model,
        "workspace": str(workspace), "duration_seconds": 0.0,
        "delivery": {"accepted": None, "event_id": None, "reason": None},
        "activation": None, "pause": None, "error": error,
        "final_message": None, "last_message": None,
        "transcript_chars": None, "python": None, "resume": None,
    }


class _Interrupted(BaseException):
    def __init__(self, signum: int) -> None:
        self.signum = signum


def run_supervised(
    settings: Settings,
    workspace: Path,
    *,
    prompt: str,
    json_output: bool,
    run_timeout: float,
    process_timeout: float,
    session_id: str | None,
    resume: bool,
) -> int:
    """Run the actual CLI in a contained child with an elapsed process deadline.

    Argument/configuration resolution belongs to the caller. OS process creation
    and scheduling are not hard real-time; cleanup/reaping receives one shared,
    bounded five-second allowance after the deadline or interruption.
    """
    if not math.isfinite(process_timeout) or process_timeout <= 0 or prompt is None:
        raise ValueError("process supervision requires a finite positive timeout and a prompt")
    started = time.monotonic()
    deadline = started + process_timeout
    chosen_session = session_id or uuid.uuid4().hex
    tree = _Tree()
    control: socket.socket | None = None
    child_control: socket.socket | None = None
    outputs: list[_Output] = []
    timed_out = False
    error: str | None = None
    code = 1
    tree_stopped = False
    output_complete = True
    previous_term: Any = None
    term_installed = False

    def emergency_stop() -> None:
        tree.stop(time.monotonic() + _CLEANUP_SECONDS)

    def interrupt(signum: int, _frame: Any) -> None:
        raise _Interrupted(signum)

    atexit.register(emergency_stop)
    try:
        if threading.current_thread() is threading.main_thread() and hasattr(signal, "SIGTERM"):
            previous_term = signal.signal(signal.SIGTERM, interrupt)
            term_installed = True
        if os.name == "nt" and os.environ.get("NERVIPULSA_WINDOWS_APPCONTAINER", "").lower() in {"1", "true", "yes"}:
            raise RuntimeError("--process-timeout is incompatible with the experimental Windows AppContainer: ACL/profile teardown cannot meet its bounded cleanup deadline")
        if os.name != "nt" and not all(hasattr(os, name) for name in ("waitid", "WNOWAIT", "WEXITED", "WNOHANG", "P_PID")):
            raise RuntimeError("process supervision requires POSIX waitid(WNOWAIT) to retain process group ownership")
        from .providers import PROVIDER_KEY_ENV

        key_env = PROVIDER_KEY_ENV.get(settings.provider)
        child_settings = replace(settings, api_key=settings.api_key or (os.environ.get(key_env, "") if key_env else ""))
        configuration = {
            "settings": asdict(child_settings), "workspace": str(workspace),
            "prompt": prompt, "json_output": json_output, "run_timeout": run_timeout,
            "session_id": chosen_session, "resume": resume,
        }
        control, child_control = socket.socketpair()
        child_control.set_inheritable(True)
        # This process replaces the CLI host, so preserve provider proxy/TLS
        # settings and startup extensions. Only Python workers use filtering.
        env = dict(os.environ)
        env.update(filtered_environment())
        argv = [sys.executable, "-P", "-m", "nervipulsa.supervision", "--runtime-child", "--control-fd", str(child_control.fileno())]
        options: dict[str, Any] = {}
        if os.name == "nt":
            startup = subprocess.STARTUPINFO()
            startup.lpAttributeList = {"handle_list": [child_control.fileno()]}
            options["startupinfo"] = startup
        else:
            options.update(pass_fds=(child_control.fileno(),), start_new_session=True)
        if time.monotonic() >= deadline:
            raise TimeoutError("process deadline reached before child launch")
        tree.proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, bufsize=0, env=env, close_fds=True, **options)
        child_control.close()
        child_control = None
        assert tree.proc.stdout is not None and tree.proc.stderr is not None
        outputs = [_Output(tree.proc.stdout), _Output(tree.proc.stderr, stderr=True)]
        if os.name == "nt":
            tree.job = _assign_windows_job(tree.proc, strict=True)
        # No runtime import/model call can occur before this message: on
        # Windows the job is assigned; POSIX Popen has established its session.
        _send_configuration(control, configuration, deadline)
        messages = _Messages()
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("process deadline reached")
            if peek_returncode(tree.proc) is not None:
                # Messages already queued may retire worker ownership. Do not
                # reap the root until all remaining groups have been stopped.
                while not messages.closed and select.select([control], [], [], 0)[0]:
                    for message in messages.receive(control):
                        tree.message(message, control, deadline)
                break
            if messages.closed:
                time.sleep(min(0.02, remaining))
            elif select.select([control], [], [], min(0.02, remaining))[0]:
                for message in messages.receive(control):
                    tree.message(message, control, deadline)
        code = int(peek_returncode(tree.proc) or 0)
    except (TimeoutError, socket.timeout):
        timed_out = True
        code = 124
        error = (
            f"process timeout after {process_timeout:g}s; forced interruption can leave journal "
            "observations incomplete and external effects unknown"
        )
    except KeyboardInterrupt:
        code = 130
        error = "process interrupted; journal observations may be incomplete and external effects unknown"
    except _Interrupted as exc:
        code = 128 + exc.signum
        error = "process interrupted; journal observations may be incomplete and external effects unknown"
    except Exception as exc:
        code = 1
        error = f"process supervision failed: {type(exc).__name__}: {exc}"
    finally:
        # Ignore repeated terminal interruptions while enforcing the single
        # cleanup bound. Restore caller signal handling before returning.
        previous_int: Any = None
        int_ignored = False
        if threading.current_thread() is threading.main_thread():
            previous_int = signal.signal(signal.SIGINT, signal.SIG_IGN)
            int_ignored = True
            if term_installed:
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            tree_stopped = tree.stop(cleanup_deadline)
            for output in outputs:
                if not output.close(cleanup_deadline):
                    output_complete = False
        finally:
            if control is not None:
                control.close()
            if child_control is not None:
                child_control.close()
            atexit.unregister(emergency_stop)
            if int_ignored:
                signal.signal(signal.SIGINT, previous_int)
            if term_installed:
                signal.signal(signal.SIGTERM, previous_term)
    if error is None and json_output:
        try:
            outputs[0].capture.seek(0)
            payload = json.load(outputs[0].capture)
            if not isinstance(payload, dict) or "status" not in payload or "exit_code" not in payload:
                raise ValueError("child did not produce a completion object")
            if code != payload["exit_code"]:
                raise ValueError(f"child exit code {code} disagrees with completion exit code")
        except (ValueError, IndexError, UnicodeError) as exc:
            code = 1
            error = f"supervised child exited without a usable completion: {exc}"
    if error is not None or not json_output:
        payload = _failure_payload(settings, workspace, chosen_session, code=code, error=error or "supervised process failed")
    if not tree_stopped or not output_complete:
        payload["cleanup_incomplete"] = True
        payload["ok"] = False
        payload["final_message"] = None
        if code == 0:
            code = 1
            payload["status"] = "failed"
            payload["error"] = "supervised process cleanup incomplete"
        payload["exit_code"] = code
    payload["duration_seconds"] = round(time.monotonic() - started, 3)
    payload["supervision"] = {
        "process_timeout": float(process_timeout), "timed_out": timed_out,
        "tree_stopped": tree_stopped,
        "child_exit_code": None if tree.proc is None else tree.proc.returncode,
    }
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)
    else:
        if not timed_out and error is None and outputs:
            outputs[0].capture.seek(0)
            decoder = codecs.getincrementaldecoder("utf-8")("replace")
            while data := outputs[0].capture.read(65536):
                sys.stdout.write(decoder.decode(data))
            sys.stdout.write(decoder.decode(b"", final=True))
            sys.stdout.flush()
        if error is not None:
            print(error, file=sys.stderr, flush=True)
        if payload.get("cleanup_incomplete"):
            print("supervised process cleanup incomplete", file=sys.stderr, flush=True)
    for output in outputs:
        if output.capture is not None:
            output.capture.close()
    return code


def _runtime_child(control_fd: int) -> int:
    global _active_supervisor
    control = socket.socket(fileno=control_fd)
    control.set_inheritable(False)
    try:
        configuration = _receive_configuration(control)
        _active_supervisor = _SupervisorClient(control)
        # Lazy import is essential: a closed gate must fail before constructing
        # the runtime, creating its worker, or making a provider/model call.
        from .cli import run_cli

        settings = Settings(**configuration.pop("settings"))
        workspace = Path(configuration.pop("workspace"))
        return asyncio.run(run_cli(settings, workspace, **configuration))
    finally:
        if _active_supervisor is not None:
            _active_supervisor.close()
            _active_supervisor = None
        else:
            control.close()


def _worker_child(argv: list[str]) -> int:
    control = socket.socket(fileno=int(os.environ["NERVIPULSA_CONTROL_FD"]))
    control.set_inheritable(False)
    release = read_frame(control)
    if release != {"kind": "worker-release"}:
        control.close()
        return 1
    os.setsid()
    write_frame(control, {"kind": "worker-group-ready", "pid": os.getpid()})
    # python_worker takes ownership of the same descriptor after this preamble.
    # Keep its imports protected by -P and the host source-root PYTHONPATH.
    control.detach()
    from .python_worker import main

    main(argv)
    return 0


def _main() -> int:
    if "--worker-child" in sys.argv[1:]:
        arguments = sys.argv[1:]
        arguments.remove("--worker-child")
        return _worker_child(arguments)
    parser = argparse.ArgumentParser(description="Private Nervipulsa process supervisor bootstrap")
    parser.add_argument("--runtime-child", action="store_true", required=True)
    parser.add_argument("--control-fd", type=int, required=True)
    args = parser.parse_args()
    return _runtime_child(args.control_fd)


if __name__ == "__main__":
    # process_tree's lazy import must see this process's active client rather
    # than a second module instance created by Python's -m entry mechanism.
    sys.modules["nervipulsa.supervision"] = sys.modules[__name__]
    raise SystemExit(_main())
