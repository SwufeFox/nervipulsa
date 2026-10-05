"""Spawn and stop the worker process tree.

The worker runs with a filtered environment. Provider credentials and
NERVIPULSA_* values are not inherited. A Windows job object (or a POSIX
process group) makes timeout and cancel stop managed child processes too.
This is process cleanup, not a sandbox.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

_ALLOW = {
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "TEMP",
    "TMP",
    "TMPDIR",
    "HOME",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LANGUAGE",
    "PYTHONIOENCODING",
    "PYTHONUTF8",
    "PYTHONUNBUFFERED",
    "PYTHONPATH",
    "PYTHONHOME",
    "VIRTUAL_ENV",
    "APPDATA",
    "LOCALAPPDATA",
    "SYSTEMDRIVE",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
    "PROGRAMDATA",
    "PROGRAMW6432",
    "USERDOMAIN",
    "USERNAME",
    "COMPUTERNAME",
    "NUMBER_OF_PROCESSORS",
    "PROCESSOR_ARCHITECTURE",
    "PROCESSOR_IDENTIFIER",
    "PUBLIC",
    "OS",
    "SESSIONNAME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TERM",
}
_DENY_PARTS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "PASSWD")


def source_root() -> Path:
    return Path(__file__).resolve().parents[1]


def filtered_environment() -> dict[str, str]:
    env: dict[str, str] = {}
    for name, value in os.environ.items():
        upper = name.upper()
        if upper.startswith("NERVIPULSA_"):
            continue
        if any(part in upper for part in _DENY_PARTS):
            continue
        if upper in _ALLOW or upper.startswith("LC_"):
            env[name] = value
    root = str(source_root())
    previous = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = root if not previous else root + os.pathsep + previous
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return env


class ManagedProcess:
    def __init__(self, proc: subprocess.Popen[bytes], control: socket.socket, job: Any) -> None:
        self.proc = proc
        self.control = control
        self.job = job
        self._lock = threading.Lock()
        self._termination_result: bool | None = None

    @property
    def pid(self) -> int:
        return int(self.proc.pid)

    def poll(self) -> int | None:
        return self.proc.poll()

    def terminate_tree(self) -> bool:
        with self._lock:
            if self._termination_result is None:
                self._termination_result = self._terminate_tree()
            return self._termination_result

    def _terminate_tree(self) -> bool:
        tree_stopped = True
        if os.name == "nt":
            if self.job is not None:
                try:
                    tree_stopped = _terminate_job(self.job)
                except OSError:
                    tree_stopped = False
                self.job = None
                if not tree_stopped:
                    tree_stopped = _taskkill(self.pid)
            else:
                tree_stopped = _taskkill(self.pid)
        else:
            try:
                os.killpg(self.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError:
                tree_stopped = False
                try:
                    self.proc.kill()
                except OSError:
                    pass
        if self.poll() is None:
            try:
                self.proc.kill()
            except OSError:
                pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                if not _taskkill(self.pid):
                    tree_stopped = False
                try:
                    self.proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    return False
            else:
                return False
        return tree_stopped and self.poll() is not None


def spawn_worker(workspace: Path, epoch: int, output_dir: Path) -> ManagedProcess:
    parent, child = socket.socketpair()
    child.set_inheritable(True)
    env = filtered_environment()
    env["NERVIPULSA_CONTROL_FD"] = str(child.fileno())
    argv = [
        sys.executable,
        "-m",
        "nervipulsa.python_worker",
        "--workspace",
        str(workspace),
        "--epoch",
        str(epoch),
        "--output-dir",
        str(output_dir),
    ]
    try:
        if os.name == "nt":
            startup = subprocess.STARTUPINFO()
            startup.lpAttributeList = {"handle_list": [child.fileno()]}
            proc = subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                env=env,
                cwd=str(workspace),
                startupinfo=startup,
                close_fds=True,
            )
        else:
            proc = subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                env=env,
                cwd=str(workspace),
                pass_fds=(child.fileno(),),
                start_new_session=True,
                close_fds=True,
            )
    except Exception:
        parent.close()
        child.close()
        raise
    else:
        child.close()
    job = _assign_windows_job(proc) if os.name == "nt" else None
    parent.setblocking(True)
    return ManagedProcess(proc, parent, job)


def _assign_windows_job(proc: subprocess.Popen[bytes]) -> Any:
    try:
        kernel32, job = _create_kill_on_close_job()
        handle = int(getattr(proc, "_handle"))
        from ctypes import wintypes

        assigned = kernel32.AssignProcessToJobObject(job, wintypes.HANDLE(handle))
        if not assigned:
            kernel32.CloseHandle(job)
            return None
        return job
    except Exception:
        return None


def _create_kill_on_close_job() -> tuple[Any, Any]:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_uint64),
            ("WriteOperationCount", ctypes.c_uint64),
            ("OtherOperationCount", ctypes.c_uint64),
            ("ReadTransferCount", ctypes.c_uint64),
            ("WriteTransferCount", ctypes.c_uint64),
            ("OtherTransferCount", ctypes.c_uint64),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        raise OSError("CreateJobObjectW failed")
    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not kernel32.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info)):
        kernel32.CloseHandle(job)
        raise OSError("SetInformationJobObject failed")
    return kernel32, job


def _terminate_job(job: Any) -> bool:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateJobObject.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    try:
        return bool(kernel32.TerminateJobObject(job, 1))
    finally:
        kernel32.CloseHandle(job)


def _taskkill(pid: int) -> bool:
    try:
        completed = subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            capture_output=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        # Minimal Windows images may omit taskkill.exe. The caller still kills
        # and waits for the worker itself; descendant cleanup is unavailable.
        return False
    return completed.returncode == 0
