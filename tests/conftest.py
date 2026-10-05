"""Workspace-local scratch dirs. System temp is not always writable for files."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / ".scratch" / "pytest"


@pytest.fixture
def workspace(request: pytest.FixtureRequest) -> Path:
    path = ROOT / request.node.name
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True)
    yield path
    shutil.rmtree(path, ignore_errors=True)
