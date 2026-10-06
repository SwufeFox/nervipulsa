from __future__ import annotations

import json
import os
import socket
import sys
from pathlib import Path

import pytest

from nervipulsa.appcontainer import AppContainerUnavailable, launch
from nervipulsa.process_tree import _cleanup_appcontainer, filtered_environment


@pytest.mark.skipif(os.name != "nt", reason="Windows AppContainer API")
def test_appcontainer_isolation_boundaries(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    output = tmp_path / "output"
    workspace.mkdir()
    output.mkdir()
    (workspace / "inside.txt").write_text("inside", encoding="utf-8")
    outside = tmp_path / "outside.txt"
    outside.write_text("secret", encoding="utf-8")
    result_path = workspace / "result.json"
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    control_parent, control_child = socket.socketpair()
    control_child.set_inheritable(True)
    code = r"""
import ctypes, json, os, socket
from pathlib import Path
k=ctypes.WinDLL('kernel32', use_last_error=True)
a=ctypes.WinDLL('advapi32', use_last_error=True)
k.GetCurrentProcess.restype=ctypes.c_void_p
a.OpenProcessToken.argtypes=[ctypes.c_void_p, ctypes.c_uint, ctypes.POINTER(ctypes.c_void_p)]
a.OpenProcessToken.restype=ctypes.c_int
a.GetTokenInformation.argtypes=[ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint, ctypes.POINTER(ctypes.c_uint)]
a.GetTokenInformation.restype=ctypes.c_int
token=ctypes.c_void_p()
assert a.OpenProcessToken(k.GetCurrentProcess(), 8, ctypes.byref(token))
value=ctypes.c_uint()
size=ctypes.c_uint()
assert a.GetTokenInformation(token, 29, ctypes.byref(value), ctypes.sizeof(value), ctypes.byref(size))
inside=Path(r'%s')
inside.write_text('worker-write', encoding='utf-8')
read_inside=inside.read_text(encoding='utf-8')
try:
    Path(r'%s').read_text(encoding='utf-8')
    outside_denied=False
except OSError:
    outside_denied=True
try:
    socket.create_connection(('127.0.0.1', %d), timeout=1).close()
    network_denied=False
except OSError:
    network_denied=True
Path(r'%s').write_text(json.dumps({'appcontainer': bool(value.value), 'inside': read_inside, 'outside_denied': outside_denied, 'network_denied': network_denied}), encoding='utf-8')
""" % (str(workspace / "inside.txt"), str(outside), listener.getsockname()[1], str(result_path))
    env = filtered_environment()
    argv = [sys.executable, "-c", code]
    proc = None
    profile = ""
    metadata = None
    try:
        proc, profile, metadata = launch(argv, env, workspace, output, control_child.fileno())
        control_child.close()
        assert proc.wait(timeout=20) == 0, proc.stderr.read().decode("utf-8", "replace")
        state = json.loads(result_path.read_text(encoding="utf-8"))
        assert state == {"appcontainer": True, "inside": "worker-write", "outside_denied": True, "network_denied": True}
    except Exception:
        raise
    finally:
        listener.close()
        control_parent.close()
        control_child.close()
        if proc is not None:
            proc.stderr.close()
        if profile and metadata is not None:
            assert _cleanup_appcontainer(profile, metadata)


@pytest.mark.skipif(os.name != "nt", reason="Windows AppContainer API")
def test_appcontainer_rejects_workspace_hardlinks(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    output = tmp_path / "output"
    workspace.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    os.link(outside, workspace / "linked.txt")

    with pytest.raises(AppContainerUnavailable, match="hard-linked file"):
        launch([sys.executable, "-c", "pass"], filtered_environment(), workspace, output, 0)

    assert outside.read_text(encoding="utf-8") == "outside"


@pytest.mark.skipif(os.name != "nt", reason="Windows AppContainer API")
def test_appcontainer_createprocess_failure_rolls_back_acl_and_profile(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    output = tmp_path / "output"
    workspace.mkdir()

    with pytest.raises(OSError, match="2"):
        launch(
            [str(tmp_path / "missing-worker.exe")],
            filtered_environment(),
            workspace,
            output,
            0,
        )
