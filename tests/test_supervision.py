"""Hard CLI deadlines exercised through real processes and a loopback provider."""
from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
import ctypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from typing import Any

import pytest

from nervipulsa.session_lock import SessionLease


_ROOT = Path(__file__).resolve().parents[1]


def _response(text: str = "local completion", *, code: str | None = None,
              timeout: float = 120) -> dict:
    message: dict[str, Any] = {"role": "assistant", "content": text}
    if code is not None:
        message["tool_calls"] = [{
            "id": "local-python", "type": "function",
            "function": {"name": "python_exec", "arguments": json.dumps({
                "code": code, "timeout": timeout,
            })},
        }]
    return {"model": "local-test", "choices": [{
        "index": 0, "message": message,
        "finish_reason": "tool_calls" if code is not None else "stop",
    }]}


@contextmanager
def _provider(
    answer: Callable[[dict, int, threading.Event], dict],
) -> Iterator[tuple[str, list[dict], threading.Event]]:
    requests: list[dict] = []
    entered = threading.Event()
    release = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            entered.set()
            payload = json.dumps(answer(body, len(requests), release)).encode("utf-8")
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass  # A deadline is expected to close the requesting socket.

        def log_message(self, _format: str, *_args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1", requests, entered
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def _environment(tmp_path: Path) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items()
           if not key.startswith("NERVIPULSA_") and key not in {
               "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "DEEPSEEK_API_KEY",
           }}
    env["NERVIPULSA_CONFIG_DIR"] = str(tmp_path / "config")
    env["PYTHONPATH"] = os.pathsep.join(filter(None, (str(_ROOT), env.get("PYTHONPATH"))))
    env["PYTHONIOENCODING"] = "utf-8"
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = env["NO_PROXY"]
    return env


@contextmanager
def _cli(tmp_path: Path, url: str, *options: str,
         environment: dict[str, str] | None = None) -> Iterator[subprocess.Popen[str]]:
    process = subprocess.Popen(
        [sys.executable, "-m", "nervipulsa", "--dir", str(tmp_path),
         "--provider", "ollama", "--base-url", url, "--model", "local-test",
         "--api-key", "", *options],
        cwd=_ROOT, env={**_environment(tmp_path), **(environment or {})}, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8",
        start_new_session=os.name != "nt",
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                               capture_output=True, timeout=10, check=False)
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if process.poll() is None:
                process.kill()
        process.wait(timeout=10)
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                stream.close()


def _result(process: subprocess.Popen[str], timeout: float = 30) -> dict:
    stdout, stderr = process.communicate(timeout=timeout)
    # json.loads also rejects an escaped early result followed by a second result.
    try:
        result = json.loads(stdout)
    except ValueError:
        pytest.fail(f"expected one JSON result, got {stdout!r}; stderr={stderr!r}")
    assert isinstance(result, dict)
    assert result["exit_code"] == process.returncode, stderr
    return result


def _timeout_result(process: subprocess.Popen[str], deadline: float) -> dict:
    result = _result(process, timeout=deadline + 15)
    assert process.returncode == 124
    assert result["status"] == "timeout"
    assert result["ok"] is False
    assert result["final_message"] is None
    assert result["last_message"] is None
    assert result["delivery"]["accepted"] is None
    assert result["supervision"]["process_timeout"] == deadline
    assert result["supervision"]["timed_out"] is True
    assert result["supervision"]["tree_stopped"] is True
    assert not result.get("cleanup_incomplete", False)
    return result


def _wait_json(path: Path, process: subprocess.Popen[str], timeout: float = 15) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            pass
        assert process.poll() is None, "CLI exited before the tool reached its gate"
        time.sleep(0.025)
    pytest.fail(f"tool did not reach gate {path}")


def _running(pid: int) -> bool:
    if os.name == "nt":
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
        kernel.OpenProcess.restype = ctypes.c_void_p
        kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
        kernel.WaitForSingleObject.restype = ctypes.c_ulong
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        handle = kernel.OpenProcess(0x00100000, False, pid)  # SYNCHRONIZE
        if not handle:
            assert ctypes.get_last_error() == 87, "cannot inspect test process"
            return False
        try:
            return kernel.WaitForSingleObject(handle, 0) == 258  # WAIT_TIMEOUT
        finally:
            kernel.CloseHandle(handle)
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        if sys.platform.startswith("linux"):
            return False
    else:
        if stat.rsplit(")", 1)[1].split()[0] == "Z":
            return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_stopped(pids: list[int]) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and any(_running(pid) for pid in pids):
        time.sleep(0.025)
    assert not [pid for pid in pids if _running(pid)], "managed process survived deadline"


def test_supervised_cli_reports_one_complete_success(tmp_path: Path) -> None:
    with _provider(lambda *_: _response("finished locally")) as (url, requests, _):
        with _cli(tmp_path, url, "--prompt", "hello", "--json",
                  "--session-id", "normal", "--process-timeout", "20") as process:
            result = _result(process)
    assert result["status"] == "succeeded"
    assert result["ok"] is True
    assert result["exit_code"] == 0
    assert result["session_id"] == "normal"
    assert result["final_message"] == "finished locally"
    assert result["last_message"]["text"] == "finished locally"
    supervision = result["supervision"]
    assert supervision["process_timeout"] == 20.0
    assert supervision["timed_out"] is False
    assert supervision["tree_stopped"] is True
    assert supervision["child_exit_code"] == 0
    assert len(requests) == 1


def test_supervised_worker_timeout_allows_replacement_and_completion(tmp_path: Path) -> None:
    def answer(_body: dict, number: int, _release: threading.Event) -> dict:
        if number == 1:
            return _response(code="import time\nwhile True: time.sleep(.1)", timeout=0.3)
        if number == 2:
            return _response(code="from pathlib import Path\nPath('replacement.txt').write_text('ready')")
        return _response("replacement completed")

    with _provider(answer) as (url, requests, _):
        with _cli(tmp_path, url, "--prompt", "run and recover", "--json",
                  "--process-timeout", "25") as process:
            result = _result(process)
    assert result["status"] == "succeeded"
    assert result["final_message"] == "replacement completed"
    assert result["supervision"]["tree_stopped"] is True
    assert (tmp_path / "replacement.txt").read_text() == "ready"
    assert len(requests) == 3


def test_success_is_withheld_while_child_executor_prevents_exit(tmp_path: Path) -> None:
    # A real Python startup extension leaves an executor alive in the runtime
    # child. Observe its successful receipt before interpreter thread teardown.
    startup = tmp_path / "startup"
    startup.mkdir()
    observed = tmp_path / "child-success.json"
    (startup / "sitecustomize.py").write_text(
        "import concurrent.futures, json, os, sys, time\n"
        "if 'nervipulsa.supervision' in sys.orig_argv:\n"
        "    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)\n"
        "    pool.submit(time.sleep, 90)\n"
        "    original = sys.stdout\n"
        "    class ObservedOutput:\n"
        "        def __getattr__(self, name): return getattr(original, name)\n"
        "        def write(self, text):\n"
        "            try: result = json.loads(text)\n"
        "            except ValueError: result = None\n"
        "            if isinstance(result, dict) and result.get('status') == 'succeeded':\n"
        "                with open(os.environ['NV_TEST_CHILD_RESULT'], 'w') as record:\n"
        "                    json.dump({'pid': os.getpid(), 'result': result}, record)\n"
        "            return original.write(text)\n"
        "    sys.stdout = ObservedOutput()\n",
        encoding="utf-8",
    )
    environment = {
        "PYTHONPATH": os.pathsep.join((str(_ROOT), str(startup))),
        "NV_TEST_CHILD_RESULT": str(observed),
    }
    try:
        with _provider(lambda *_: _response("answer before teardown")) as (url, _, _):
            with _cli(tmp_path, url, "--prompt", "hello", "--json",
                      "--process-timeout", "8", environment=environment) as process:
                _timeout_result(process, 8)
        receipt = json.loads(observed.read_text(encoding="utf-8"))
        assert receipt["result"]["final_message"] == "answer before teardown"
        assert receipt["result"]["exit_code"] == 0
        _wait_stopped([receipt["pid"]])
    finally:
        if observed.exists():
            pid = json.loads(observed.read_text(encoding="utf-8"))["pid"]
            if _running(pid):
                os.kill(pid, signal.SIGKILL if os.name != "nt" else signal.SIGTERM)


def test_hard_deadline_includes_blocked_http_after_soft_timeout(tmp_path: Path) -> None:
    def blocked(_body: dict, _number: int, release: threading.Event) -> dict:
        release.wait(90)
        return _response("too late")

    with _provider(blocked) as (url, requests, entered):
        with _cli(tmp_path, url, "--prompt", "wait", "--json", "--run-timeout", "1",
                  "--process-timeout", "8") as process:
            assert entered.wait(6), "CLI never contacted the local provider"
            result = _timeout_result(process, 8)
    assert len(requests) == 1
    assert result["duration_seconds"] >= 1


def test_hard_deadline_kills_tool_descendant_and_releases_session_without_replay(
    tmp_path: Path,
) -> None:
    ready = tmp_path / "descendant-ready.json"
    descendant_ready = tmp_path / "descendant-started.json"
    gate = tmp_path / "allow-late-effect"
    marker = tmp_path / "late-effect"
    executions = tmp_path / "executions"
    descendant_code = (
        "import json, os, time\nfrom pathlib import Path\n"
        f"target = Path({str(descendant_ready)!r})\n"
        "target.with_suffix('.tmp').write_text(json.dumps({'descendant': os.getpid(), "
        "'worker': os.getppid()}))\n"
        "target.with_suffix('.tmp').replace(target)\n"
        f"while not Path({str(gate)!r}).exists(): time.sleep(.025)\n"
        f"Path({str(marker)!r}).write_text('late effect')\n"
    )
    code = (
        "import json, os, subprocess, sys, time\nfrom pathlib import Path\n"
        f"with Path({str(executions)!r}).open('a') as log: log.write('executed\\n')\n"
        f"child = subprocess.Popen([sys.executable, '-c', {descendant_code!r}])\n"
        f"while not Path({str(descendant_ready)!r}).exists(): time.sleep(.025)\n"
        f"data = json.loads(Path({str(descendant_ready)!r}).read_text())\n"
        "data['runtime'] = os.getppid()\n"
        f"Path({str(ready)!r}).write_text(json.dumps(data))\n"
        "while True: time.sleep(.1)\n"
    )
    def tool_then_text(_body: dict, number: int, _release: threading.Event) -> dict:
        return _response(code=code) if number == 1 else _response("unexpected continuation")

    pids: list[int] = []
    try:
        with _provider(tool_then_text) as (url, _, _):
            with _cli(tmp_path, url, "--prompt", "run the gated tool", "--json",
                      "--session-id", "forced-session", "--process-timeout", "15") as process:
                report = _wait_json(ready, process, timeout=12)
                pids = [report["runtime"], report["worker"], report["descendant"]]
                assert all(_running(pid) for pid in pids)
                _timeout_result(process, 15)
        _wait_stopped(pids)
        pids = []
        # Releasing the real side-effect gate after termination must do nothing.
        gate.touch()
        assert not marker.exists()
        lease = SessionLease.acquire(tmp_path / ".nervipulsa" / "journal.sqlite", "forced-session")
        lease.close()
        with _provider(lambda *_: _response("fresh continuation")) as (url, requests, _):
            with _cli(tmp_path, url, "--prompt", "continue without repeating effects", "--json",
                      "--session-id", "forced-session", "--resume",
                      "--process-timeout", "20") as process:
                resumed = _result(process)
        assert resumed["status"] == "succeeded"
        assert resumed["final_message"] == "fresh continuation"
        assert len(requests) == 1
        assert executions.read_text().splitlines() == ["executed"]
        assert not marker.exists()
    finally:
        # On assertion failure, unblock only after killing recorded live processes.
        for pid in pids:
            if _running(pid):
                try:
                    os.kill(pid, signal.SIGKILL if os.name != "nt" else signal.SIGTERM)
                except ProcessLookupError:
                    pass
        gate.touch()


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "-1"])
def test_process_timeout_rejects_invalid_values(tmp_path: Path, value: str) -> None:
    with _cli(tmp_path, "http://127.0.0.1:1/v1", "--prompt", "hello",
              f"--process-timeout={value}") as process:
        stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 2, stderr
    assert not stdout


def test_process_timeout_requires_one_shot_prompt(tmp_path: Path) -> None:
    with _cli(tmp_path, "http://127.0.0.1:1/v1", "--process-timeout", "5") as process:
        stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 2, stderr
    assert not stdout
