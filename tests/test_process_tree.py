from __future__ import annotations

import subprocess
from typing import Any

import pytest

from nervipulsa import process_tree


class _FakeProcess:
    pid = 123

    def __init__(self) -> None:
        self.returncode: int | None = None

    def poll(self) -> int | None:
        return self.returncode

    def kill(self) -> None:
        self.returncode = -9

    def wait(self, timeout: float) -> int:
        assert self.returncode is not None
        return self.returncode


@pytest.mark.parametrize(("taskkill_code", "expected"), [(0, True), (1, False)])
def test_windows_cleanup_reports_taskkill_tree_result(
    monkeypatch: pytest.MonkeyPatch, taskkill_code: int, expected: bool
) -> None:
    process = _FakeProcess()
    calls: list[list[str]] = []

    def run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        calls.append(args)
        return subprocess.CompletedProcess(args, taskkill_code)

    monkeypatch.setattr(process_tree.os, "name", "nt")
    monkeypatch.setattr(process_tree.subprocess, "run", run)
    managed = process_tree.ManagedProcess(process, None, None)  # type: ignore[arg-type]

    assert managed.terminate_tree() is expected
    assert calls == [["taskkill", "/F", "/T", "/PID", "123"]]


def test_failed_job_termination_falls_back_to_taskkill(monkeypatch: pytest.MonkeyPatch) -> None:
    process = _FakeProcess()
    calls: list[list[str]] = []

    def run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(process_tree.os, "name", "nt")
    monkeypatch.setattr(process_tree, "_terminate_job", lambda job: False)
    monkeypatch.setattr(process_tree.subprocess, "run", run)
    managed = process_tree.ManagedProcess(process, None, object())  # type: ignore[arg-type]

    assert managed.terminate_tree() is True
    assert calls == [["taskkill", "/F", "/T", "/PID", "123"]]


def test_taskkill_missing_reports_tree_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakeProcess()

    def run(*args: Any, **kwargs: Any) -> None:
        raise FileNotFoundError

    monkeypatch.setattr(process_tree.os, "name", "nt")
    monkeypatch.setattr(process_tree.subprocess, "run", run)
    managed = process_tree.ManagedProcess(process, None, None)  # type: ignore[arg-type]

    assert managed.terminate_tree() is False
    assert process.poll() is not None


