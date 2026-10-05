from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from nervipulsa.events import _valid_payload
from nervipulsa.python_environment import (
    MAX_MODULES,
    MAX_RESULT_BYTES,
    MAX_SEARCH_ROOTS,
    _search_roots,
    discover_python_environment,
)


def test_static_discovery_finds_custom_package_without_running_top_level(tmp_path: Path) -> None:
    package = tmp_path / "quirk"
    package.mkdir()
    marker = tmp_path / "executed.txt"
    (package / "__init__.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('ran')\nclass Client: pass\ndef connect(): pass\n", encoding="utf-8"
    )
    (tmp_path / "pyproject.toml").write_text('[project]\ndependencies = ["quirk"]\n', encoding="utf-8")
    result = discover_python_environment(str(tmp_path), ["quirk"], ["Client", "connect", "missing"])
    assert not marker.exists()
    assert result["modules"][0]["found"] is True
    assert result["modules"][0]["api"] == {"Client": True, "connect": True, "missing": False}
    assert "quirk/__init__.py" in result["modules"][0]["candidates"]
    assert result["read_only"] and result["install_supported"] is False


def test_discovery_does_not_call_import_resolution_or_mutate_sys_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "safe.py"
    source.write_text("VALUE = 3\n", encoding="utf-8")
    before = list(sys.path)
    monkeypatch.setattr("importlib.util.find_spec", lambda *_: pytest.fail("find_spec called"))
    monkeypatch.setattr("importlib.import_module", lambda *_: pytest.fail("import_module called"))
    discover_python_environment(str(tmp_path), ["safe"], ["VALUE"])
    assert sys.path == before


def test_request_budget_and_names_are_strict(tmp_path: Path) -> None:
    accepted = discover_python_environment(str(tmp_path), [f"pkg{i}" for i in range(MAX_MODULES)], [f"Api{i}" for i in range(40)])
    assert len(accepted["modules"]) == MAX_MODULES
    assert len(json.dumps(accepted, ensure_ascii=False, separators=(",", ":")).encode()) <= MAX_RESULT_BYTES
    with pytest.raises(ValueError):
        discover_python_environment(str(tmp_path), ["x"] * (MAX_MODULES + 1))
    for invalid in (["../escape"], ["pkg;import os"], ["pkg..child"]):
        with pytest.raises(ValueError):
            discover_python_environment(str(tmp_path), invalid)
    with pytest.raises(ValueError):
        discover_python_environment(str(tmp_path), [], ["x"] * 41)


def test_project_files_are_reported_by_name_without_exposing_contents(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("README private prose", encoding="utf-8")
    (tmp_path / "CONTRIBUTING.md").write_text("contributor private prose", encoding="utf-8")
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "architecture.md").write_text("architecture private prose", encoding="utf-8")
    secret = "private requirement marker"
    for filename in ("pyproject.toml", "requirements.txt", "setup.py", "setup.cfg"):
        (tmp_path / filename).write_text(secret, encoding="utf-8")

    result = discover_python_environment(str(tmp_path))

    assert result["project_files"] == [
        {"file": "pyproject.toml", "kind": "dependency_manifest"},
        {"file": "requirements.txt", "kind": "dependency_manifest"},
        {"file": "setup.py", "kind": "dependency_manifest"},
        {"file": "setup.cfg", "kind": "dependency_manifest"},
    ]
    serialized = json.dumps(result)
    assert secret not in serialized
    assert "private prose" not in serialized
    assert "architecture.md" not in serialized
    assert "README.md" not in serialized



def test_result_budget_is_enforced(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("x" * 5000, encoding="utf-8")
    result = discover_python_environment(str(tmp_path))
    assert len(json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()) <= MAX_RESULT_BYTES
    assert result["project_files"] == []


def test_workspace_candidate_is_statically_recognized(tmp_path: Path) -> None:
    nested = tmp_path / "custom" / "maths"
    nested.mkdir(parents=True)
    (nested / "__init__.py").write_text("__version__ = '0.7'\ndef clamp(x): return x\n", encoding="utf-8")
    result = discover_python_environment(str(tmp_path), ["custom.maths"], ["clamp"])
    item = result["modules"][0]
    assert item["found"] is True
    assert item["api"]["clamp"] is True





def test_search_roots_are_bounded_and_paths_are_redacted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = []
    for index in range(MAX_SEARCH_ROOTS + 10):
        item = tmp_path / f"root-{index}"
        item.mkdir()
        paths.append(str(item))
    monkeypatch.setattr(sys, "path", paths)

    roots = _search_roots(tmp_path)
    assert len(roots) == MAX_SEARCH_ROOTS
    source = tmp_path / "custom.py"
    source.write_text("def scale(x): return x\n", encoding="utf-8")
    result = discover_python_environment(str(tmp_path), ["custom"], ["scale"])
    serialized = json.dumps(result)
    assert str(tmp_path.resolve()) not in serialized
    assert result["workspace"] == "."
    assert result["modules"][0]["candidates"] == ["custom.py"]


def test_resolved_workspace_escape_is_rejected_without_symlink_privilege(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "escape.py"
    outside = tmp_path.parent / f"{tmp_path.name}-outside.py"
    source.write_text("class Hidden: pass\n", encoding="utf-8")
    outside.write_text("class Hidden: pass\n", encoding="utf-8")
    original_resolve = Path.resolve

    def redirect_candidate(path: Path, *args: Any, **kwargs: Any) -> Path:
        if path == source:
            return outside
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", redirect_candidate)
    result = discover_python_environment(str(tmp_path), ["escape"], ["Hidden"])
    assert result["modules"][0]["found"] is False
    assert result["modules"][0]["candidates"] == []
    assert str(outside) not in json.dumps(result)




def test_distribution_alias_mapping_uses_import_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import nervipulsa.python_environment as environment

    monkeypatch.setattr(
        environment,
        "_package_distribution_map",
        lambda _roots: {"import_alias": [{"name": "actual-distribution", "version": "2.4.1"}]},
    )
    result = discover_python_environment(str(tmp_path), ["import_alias"])
    assert result["modules"][0]["distributions"] == [{"name": "actual-distribution", "version": "2.4.1"}]
    assert result["modules"][0]["version"] == "2.4.1"



def test_runtime_error_resolving_candidate_is_ignored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "loop.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    original_resolve = Path.resolve

    def fail_candidate(path: Path, *args: Any, **kwargs: Any) -> Path:
        if path == source:
            raise RuntimeError("symlink loop")
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", fail_candidate)
    result = discover_python_environment(str(tmp_path), ["loop"], ["VALUE"])
    assert result["modules"][0]["found"] is False

def test_workspace_symlinks_cannot_disclose_external_source_or_docs(tmp_path: Path, request: pytest.FixtureRequest) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    request.addfinalizer(lambda: shutil.rmtree(outside, ignore_errors=True))
    secret_package = outside / "leaked"
    secret_package.mkdir()
    (secret_package / "__init__.py").write_text("__version__ = 'secret-version'\nclass SecretAPI: pass\n", encoding="utf-8")
    secret_docs = outside / "docs"
    secret_docs.mkdir()
    (secret_docs / "secret.md").write_text("external secret documentation", encoding="utf-8")

    def link_directory(link: Path, target: Path) -> None:
        if os.name == "nt":
            completed = subprocess.run([os.environ.get("COMSPEC", r"C:\\Windows\\System32\\cmd.exe"), "/c", "mklink", "/J", str(link), str(target)], capture_output=True, text=True)
            if completed.returncode:
                pytest.skip(f"directory junction unavailable: {completed.stderr or completed.stdout}")
        else:
            link.symlink_to(target, target_is_directory=True)

    link_directory(tmp_path / "leaked", secret_package)
    link_directory(tmp_path / "docs", secret_docs)
    result = discover_python_environment(str(tmp_path), ["leaked"], ["SecretAPI"])
    item = result["modules"][0]
    assert item["found"] is False
    assert item["version"] is None
    assert item["api"] == {"SecretAPI": False}
    assert result["project_files"] == []
    assert all("secret.py" not in candidate for candidate in item["candidates"])


def test_environment_event_schema_accepts_success_and_failure_only() -> None:
    base = {"activation_id": "a", "tool_call_id": "t", "read_only": True, "python": "3.13", "workspace": ".", "project_files": [], "modules": [], "install_supported": False}
    assert _valid_payload("python.environment_discovered", {**base, "status": "succeeded"})
    assert _valid_payload("python.environment_discovered", {**base, "status": "failed", "error": "invalid request"})
    assert not _valid_payload("python.environment_discovered", {**base, "status": "failed"})
    assert not _valid_payload("python.environment_discovered", {**base, "status": "unknown"})
