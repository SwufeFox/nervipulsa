"""Experimental Windows AppContainer process launcher (no network capabilities)."""
from __future__ import annotations

import ctypes
import logging
import os
import secrets
import stat
import subprocess
import threading
from ctypes import wintypes
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)
_ACL_TIMEOUT_SECONDS = 30
_MAX_ACL_TREE_ENTRIES = 100_000


class AppContainerUnavailable(RuntimeError):
    """The requested Windows isolation could not be established."""


class _SID_AND_ATTRIBUTES(ctypes.Structure):
    _fields_ = [("Sid", ctypes.c_void_p), ("Attributes", wintypes.DWORD)]


class _SECURITY_CAPABILITIES(ctypes.Structure):
    _fields_ = [("AppContainerSid", ctypes.c_void_p), ("Capabilities", ctypes.POINTER(_SID_AND_ATTRIBUTES)), ("CapabilityCount", wintypes.DWORD), ("Reserved", wintypes.DWORD)]


class _SECURITY_ATTRIBUTES(ctypes.Structure):
    _fields_ = [("nLength", wintypes.DWORD), ("lpSecurityDescriptor", ctypes.c_void_p), ("bInheritHandle", wintypes.BOOL)]


class _STARTUPINFO(ctypes.Structure):
    _fields_ = [("cb", wintypes.DWORD), ("lpReserved", wintypes.LPWSTR), ("lpDesktop", wintypes.LPWSTR), ("lpTitle", wintypes.LPWSTR), ("dwX", wintypes.DWORD), ("dwY", wintypes.DWORD), ("dwXSize", wintypes.DWORD), ("dwYSize", wintypes.DWORD), ("dwXCountChars", wintypes.DWORD), ("dwYCountChars", wintypes.DWORD), ("dwFillAttribute", wintypes.DWORD), ("dwFlags", wintypes.DWORD), ("wShowWindow", wintypes.WORD), ("cbReserved2", wintypes.WORD), ("lpReserved2", ctypes.c_void_p), ("hStdInput", wintypes.HANDLE), ("hStdOutput", wintypes.HANDLE), ("hStdError", wintypes.HANDLE)]


class _STARTUPINFOEX(ctypes.Structure):
    _fields_ = [("StartupInfo", _STARTUPINFO), ("lpAttributeList", ctypes.c_void_p)]


class _PROCESS_INFORMATION(ctypes.Structure):
    _fields_ = [("hProcess", wintypes.HANDLE), ("hThread", wintypes.HANDLE), ("dwProcessId", wintypes.DWORD), ("dwThreadId", wintypes.DWORD)]


class _WindowsProcess:
    """Small Popen-compatible owner for the native CreateProcess handles."""
    def __init__(self, pi: _PROCESS_INFORMATION, stderr: Any):
        self._handle = pi.hProcess
        self._thread = pi.hThread
        self.pid = int(pi.dwProcessId)
        self.stderr = stderr
        self.returncode: int | None = None
        self._lock = threading.Lock()

    def poll(self) -> int | None:
        with self._lock:
            if self.returncode is not None:
                return self.returncode
            code = wintypes.DWORD()
            if not _k32.GetExitCodeProcess(self._handle, ctypes.byref(code)):
                raise ctypes.WinError(ctypes.get_last_error())
            if code.value != 259:
                self.returncode = int(code.value)
                _k32.CloseHandle(self._handle)
                _k32.CloseHandle(self._thread)
            return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        with self._lock:
            if self.returncode is not None:
                return self.returncode
            handle = self._handle
        ms = 0xFFFFFFFF if timeout is None else max(0, int(timeout * 1000))
        result = _k32.WaitForSingleObject(handle, ms)
        if result == 0x102:
            returncode = self.poll()
            if returncode is not None:
                return returncode
            raise subprocess.TimeoutExpired("AppContainer worker", timeout)
        if result != 0:
            returncode = self.poll()
            if returncode is not None:
                return returncode
            raise ctypes.WinError(ctypes.get_last_error())
        return self.poll()  # type: ignore[return-value]

    def kill(self) -> None:
        with self._lock:
            if self.returncode is None:
                _k32.TerminateProcess(self._handle, 1)


_k32 = ctypes.WinDLL("kernel32", use_last_error=True) if os.name == "nt" else None
if _k32 is not None:
    _k32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    _k32.GetExitCodeProcess.restype = wintypes.BOOL
    _k32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    _k32.WaitForSingleObject.restype = wintypes.DWORD
    _k32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    _k32.TerminateProcess.restype = wintypes.BOOL
    _k32.CloseHandle.argtypes = [wintypes.HANDLE]
    _k32.CloseHandle.restype = wintypes.BOOL
    _k32.CreatePipe.argtypes = [ctypes.POINTER(wintypes.HANDLE), ctypes.POINTER(wintypes.HANDLE), ctypes.c_void_p, wintypes.DWORD]
    _k32.CreatePipe.restype = wintypes.BOOL
    _k32.SetHandleInformation.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD]
    _k32.SetHandleInformation.restype = wintypes.BOOL
    _k32.InitializeProcThreadAttributeList.argtypes = [ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(ctypes.c_size_t)]
    _k32.InitializeProcThreadAttributeList.restype = wintypes.BOOL
    _k32.UpdateProcThreadAttribute.argtypes = [ctypes.c_void_p, wintypes.DWORD, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_void_p]
    _k32.UpdateProcThreadAttribute.restype = wintypes.BOOL
    _k32.DeleteProcThreadAttributeList.argtypes = [ctypes.c_void_p]
    _k32.CreateProcessW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, ctypes.c_void_p, ctypes.c_void_p, wintypes.BOOL, wintypes.DWORD, ctypes.c_void_p, wintypes.LPCWSTR, ctypes.POINTER(_STARTUPINFOEX), ctypes.POINTER(_PROCESS_INFORMATION)]
    _k32.CreateProcessW.restype = wintypes.BOOL
    _k32.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    _k32.CreateFileW.restype = wintypes.HANDLE
    _k32.GetStdHandle.argtypes = [wintypes.DWORD]
    _k32.GetStdHandle.restype = wintypes.HANDLE
    _k32.LocalFree.argtypes = [ctypes.c_void_p]
    _k32.LocalFree.restype = ctypes.c_void_p


def _check(ok: Any, operation: str) -> None:
    if not ok:
        raise ctypes.WinError(ctypes.get_last_error(), operation)


def _within(path: Path, root: Path) -> bool:
    try:
        candidate = os.path.normcase(os.path.abspath(path))
        boundary = os.path.normcase(os.path.abspath(root))
        return os.path.commonpath((candidate, boundary)) == boundary
    except ValueError:
        return False


def _preflight_tree(root: Path) -> None:
    """Reject links that could make recursive ACL grants reach outside a root."""
    try:
        root_info = os.stat(root, follow_symlinks=False)
    except OSError as exc:
        raise AppContainerUnavailable(f"cannot inspect sandbox root {root}: {exc}") from exc
    root_attributes = getattr(root_info, "st_file_attributes", 0)
    if stat.S_ISLNK(root_info.st_mode) or root_attributes & 0x400:
        raise AppContainerUnavailable(f"sandbox root is a reparse point: {root}")
    if not stat.S_ISDIR(root_info.st_mode):
        raise AppContainerUnavailable(f"sandbox root is not a directory: {root}")
    pending = [root]
    visited = 0
    while pending:
        directory = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError as exc:
            raise AppContainerUnavailable(f"cannot inspect sandbox root {directory}: {exc}") from exc
        for entry in entries:
            visited += 1
            if visited > _MAX_ACL_TREE_ENTRIES:
                raise AppContainerUnavailable(
                    f"sandbox root exceeds {_MAX_ACL_TREE_ENTRIES} entries: {root}"
                )
            try:
                info = os.stat(entry.path, follow_symlinks=False)
            except OSError as exc:
                raise AppContainerUnavailable(f"cannot inspect sandbox entry {entry.path}: {exc}") from exc
            attributes = getattr(info, "st_file_attributes", 0)
            if stat.S_ISLNK(info.st_mode) or attributes & 0x400:
                raise AppContainerUnavailable(f"sandbox root contains a reparse point: {entry.path}")
            if not stat.S_ISDIR(info.st_mode) and info.st_nlink > 1:
                raise AppContainerUnavailable(f"sandbox root contains a hard-linked file: {entry.path}")
            if stat.S_ISDIR(info.st_mode):
                pending.append(Path(entry.path))


def _icacls() -> str:
    import shutil

    found = shutil.which("icacls")
    if found:
        return found
    system_root = os.environ.get("SystemRoot") or os.environ.get("SYSTEMROOT")
    if system_root:
        candidate = Path(system_root) / "System32" / "icacls.exe"
        if candidate.is_file():
            return str(candidate)
    raise AppContainerUnavailable("required ACL tool icacls is unavailable")

def launch(argv: list[str], env: dict[str, str], cwd: Path, output_dir: Path, inherited_socket: int) -> tuple[Any, str, Any]:
    """Create an AppContainer process; caller owns returned ACL/profile cleanup."""
    if os.name != "nt":
        raise AppContainerUnavailable("AppContainer is available only on Windows")
    package_root = Path(os.path.abspath(__file__)).parent
    workspace_root = Path(os.path.abspath(cwd))
    output_root = Path(os.path.abspath(output_dir))
    output_root.mkdir(parents=True, exist_ok=True)
    write_roots = [workspace_root]
    if not _within(output_root, workspace_root):
        write_roots.append(output_root)
    for root in write_roots:
        _preflight_tree(root)
    root_rules: list[tuple[Path, str, bool]] = [(root, "(OI)(CI)M", True) for root in write_roots]
    source_root = package_root.parent
    if not any(_within(source_root, root) for root in write_roots):
        root_rules.append((source_root, "RX", False))
    if not any(_within(package_root, root) for root in write_roots):
        root_rules.append((package_root, "(OI)(CI)RX", True))
    userenv = ctypes.WinDLL("userenv", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    userenv.CreateAppContainerProfile.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.LPCWSTR, ctypes.POINTER(_SID_AND_ATTRIBUTES), wintypes.DWORD, ctypes.POINTER(ctypes.c_void_p)]
    userenv.CreateAppContainerProfile.restype = ctypes.c_long
    userenv.DeleteAppContainerProfile.argtypes = [wintypes.LPCWSTR]
    userenv.DeleteAppContainerProfile.restype = ctypes.c_long
    advapi.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(wintypes.LPWSTR)]
    advapi.ConvertSidToStringSidW.restype = wintypes.BOOL
    advapi.FreeSid.argtypes = [ctypes.c_void_p]
    advapi.FreeSid.restype = ctypes.c_void_p
    profile = "Nervipulsa-" + secrets.token_hex(12)
    sid = ctypes.c_void_p()
    hr = userenv.CreateAppContainerProfile(profile, "Nervipulsa worker", "Experimental isolated worker", None, 0, ctypes.byref(sid))
    if hr & 0x80000000:
        raise AppContainerUnavailable(f"CreateAppContainerProfile failed: HRESULT 0x{hr & 0xffffffff:08x}")
    sid_text = wintypes.LPWSTR()
    if not advapi.ConvertSidToStringSidW(sid, ctypes.byref(sid_text)):
        error = ctypes.get_last_error()
        userenv.DeleteAppContainerProfile(profile)
        advapi.FreeSid(sid)
        raise ctypes.WinError(error, "ConvertSidToStringSidW")
    sid_string = sid_text.value
    authorized: list[tuple[Path, bool]] = []
    stderr_read = stderr_write = None
    attrs = None
    attrs_initialized = False
    nul = None
    process_info: _PROCESS_INFORMATION | None = None
    try:
        icacls = _icacls()
        for path, rights, recursive in root_rules:
            path = Path(os.path.abspath(path))
            authorized.append((path, recursive))
            command = [icacls, str(path), "/grant", f"*{sid_string}:{rights}"]
            if recursive:
                command.extend(["/T"])
            try:
                result = subprocess.run(command, capture_output=True, text=True, timeout=_ACL_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired as exc:
                raise AppContainerUnavailable(f"ACL grant timed out for {path} after {_ACL_TIMEOUT_SECONDS}s") from exc
            if result.returncode:
                detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
                raise AppContainerUnavailable(f"ACL grant failed for {path}: {detail}")
        read_h, write_h = ctypes.c_void_p(), ctypes.c_void_p()
        _check(_k32.CreatePipe(ctypes.byref(read_h), ctypes.byref(write_h), None, 0), "CreatePipe")
        stderr_read, stderr_write = read_h.value, write_h.value
        _check(_k32.SetHandleInformation(wintypes.HANDLE(stderr_read), 1, 0), "SetHandleInformation(stderr read)")
        _check(_k32.SetHandleInformation(wintypes.HANDLE(stderr_write), 1, 1), "SetHandleInformation(stderr write)")
        null_security = _SECURITY_ATTRIBUTES(ctypes.sizeof(_SECURITY_ATTRIBUTES), None, True)
        nul = _k32.CreateFileW("NUL", 0xC0000000, 3, ctypes.byref(null_security), 3, 0x80, None)
        if ctypes.c_void_p(nul).value in (None, ctypes.c_void_p(-1).value):
            raise ctypes.WinError(ctypes.get_last_error(), "CreateFileW(NUL)")
        handles = (ctypes.c_void_p * 3)(int(inherited_socket), stderr_write, nul)
        size = ctypes.c_size_t()
        _k32.InitializeProcThreadAttributeList(None, 2, 0, ctypes.byref(size))
        attrs = ctypes.create_string_buffer(size.value)
        _check(_k32.InitializeProcThreadAttributeList(attrs, 2, 0, ctypes.byref(size)), "InitializeProcThreadAttributeList")
        attrs_initialized = True
        caps = _SECURITY_CAPABILITIES(sid, None, 0, 0)
        _check(_k32.UpdateProcThreadAttribute(attrs, 0, 0x00020002, ctypes.byref(handles), ctypes.sizeof(handles), None, None), "handle-list attribute")
        _check(_k32.UpdateProcThreadAttribute(attrs, 0, 0x00020009, ctypes.byref(caps), ctypes.sizeof(caps), None, None), "security-capabilities attribute")
        si = _STARTUPINFOEX()
        si.StartupInfo.cb = ctypes.sizeof(si)
        si.StartupInfo.dwFlags = 0x100
        si.StartupInfo.hStdInput = wintypes.HANDLE(nul)
        si.StartupInfo.hStdOutput = wintypes.HANDLE(nul)
        si.StartupInfo.hStdError = wintypes.HANDLE(stderr_write)
        si.lpAttributeList = ctypes.cast(attrs, ctypes.c_void_p)
        pi = _PROCESS_INFORMATION()
        process_info = pi
        command = ctypes.create_unicode_buffer(subprocess.list2cmdline(argv))
        sandbox_env = dict(env)
        sandbox_env["PYTHONDONTWRITEBYTECODE"] = "1"
        sandbox_env["NERVIPULSA_APPCONTAINER"] = "1"
        previous_pythonpath = sandbox_env.get("PYTHONPATH")
        sandbox_env["PYTHONPATH"] = (
            str(source_root) + (os.pathsep + previous_pythonpath if previous_pythonpath else "")
        )
        envblock = ctypes.create_unicode_buffer(
            "\0".join(f"{k}={v}" for k, v in sorted(sandbox_env.items())) + "\0\0"
        )
        _check(_k32.CreateProcessW(argv[0], command, None, None, True, 0x00080000 | 0x00000400 | 0x08000000, envblock, str(cwd), ctypes.byref(si), ctypes.byref(pi)), "CreateProcessW(AppContainer)")
        _k32.CloseHandle(wintypes.HANDLE(nul))
        nul = None
        _k32.CloseHandle(wintypes.HANDLE(stderr_write))
        stderr_write = None
        import msvcrt
        stderr_file = os.fdopen(msvcrt.open_osfhandle(stderr_read, os.O_RDONLY), "rb", buffering=0)
        stderr_read = None
        return _WindowsProcess(pi, stderr_file), profile, (sid_string, authorized)
    except Exception as exc:
        cleanup_ok = True
        process_stopped = True
        if process_info is not None and process_info.hProcess:
            _k32.TerminateProcess(process_info.hProcess, 1)
            process_stopped = _k32.WaitForSingleObject(process_info.hProcess, 5000) == 0
            if not process_stopped:
                logger.error("AppContainer process %s did not stop during startup rollback", process_info.dwProcessId)
                cleanup_ok = False
            _k32.CloseHandle(process_info.hProcess)
            if process_info.hThread:
                _k32.CloseHandle(process_info.hThread)
        if process_stopped:
            for path, recursive in reversed(authorized):
                try:
                    command = [icacls, str(path), "/remove:g", f"*{sid_string}"]
                    if recursive:
                        command.extend(["/T", "/C"])
                    removed = subprocess.run(
                        command,
                        capture_output=True,
                        text=True,
                        timeout=_ACL_TIMEOUT_SECONDS,
                    )
                    if removed.returncode:
                        detail = removed.stderr.strip() or removed.stdout.strip() or f"exit code {removed.returncode}"
                        logger.error("AppContainer ACL rollback failed for %s: %s", path, detail)
                        cleanup_ok = False
                except Exception as cleanup_exc:
                    logger.error("AppContainer ACL rollback failed for %s: %s", path, cleanup_exc)
                    cleanup_ok = False
            try:
                profile_result = userenv.DeleteAppContainerProfile(profile)
                if profile_result != 0:
                    logger.error("DeleteAppContainerProfile failed for %s: error %s", profile, profile_result)
                    cleanup_ok = False
            except Exception as cleanup_exc:
                logger.exception("DeleteAppContainerProfile raised for %s", profile)
                cleanup_ok = False
        if not cleanup_ok:
            raise AppContainerUnavailable(f"{exc}; AppContainer process/ACL/profile cleanup failed; see diagnostics") from exc
        raise
    finally:
        if attrs_initialized:
            _k32.DeleteProcThreadAttributeList(attrs)
        if stderr_read:
            _k32.CloseHandle(stderr_read)
        if stderr_write:
            _k32.CloseHandle(stderr_write)
        if nul:
            _k32.CloseHandle(wintypes.HANDLE(nul))
        _k32.LocalFree(sid_text)
        advapi.FreeSid(sid)
