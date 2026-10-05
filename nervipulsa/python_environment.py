"""Static, bounded discovery of Python package candidates."""
from __future__ import annotations

import ast
import importlib.metadata
import json
import os
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

DEPENDENCY_FILES = (
    "pyproject.toml",
    "requirements.txt",
    "requirements-dev.txt",
    "setup.py",
    "setup.cfg",
    "Pipfile",
    "environment.yml",
    "conda.yml",
)
MAX_MODULES = 20
MAX_API_NAMES = 40
MAX_SEARCH_ROOTS = 32
MAX_DISTRIBUTIONS_PER_ROOT = 512
MAX_CANDIDATES_PER_MODULE = 8
MAX_RESULT_BYTES = 48_000
MAX_SOURCE_BYTES = 256_000
MAX_TOTAL_SOURCE_BYTES = 1_000_000
_NAME = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$", re.ASCII)


def _valid_list(value: Any, limit: int, *, dotted: bool = True) -> bool:
    if not isinstance(value, list) or len(value) > limit:
        return False
    for item in value:
        if not isinstance(item, str) or len(item) > 200 or not _NAME.fullmatch(item):
            return False
        if not dotted and "." in item:
            return False
    return True


def _resolve(path: Path) -> Path | None:
    try:
        return path.resolve()
    except (OSError, RuntimeError):
        return None


def _is_within(root: Path, path: Path) -> bool:
    resolved_root = _resolve(root)
    resolved_path = _resolve(path)
    if resolved_root is None or resolved_path is None:
        return False
    try:
        resolved_path.relative_to(resolved_root)
        return True
    except ValueError:
        return False


def _search_roots(workspace: Path) -> list[Path]:
    roots: list[Path] = [workspace]
    for entry in sys.path:
        if not entry:
            continue
        candidate = _resolve(Path(entry))
        if candidate is None:
            continue
        try:
            if not candidate.is_dir() or candidate in roots:
                continue
        except OSError:
            continue
        roots.append(candidate)
        if len(roots) >= MAX_SEARCH_ROOTS:
            break
    return roots[:MAX_SEARCH_ROOTS]


def _contains_resolved(roots: list[Path], target: Path) -> bool:
    for root in roots:
        try:
            target.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def _module_candidates(module_name: str, roots: list[Path]) -> list[Path]:
    parts = module_name.split(".")
    found: list[Path] = []
    for base in roots:
        current = base.joinpath(*parts)
        for candidate in (current.with_suffix(".py"), current / "__init__.py"):
            resolved = _resolve(candidate)
            if resolved is None or not _contains_resolved(roots, resolved):
                continue
            try:
                if not resolved.is_file() or resolved.stat().st_size > MAX_SOURCE_BYTES:
                    continue
            except OSError:
                continue
            if resolved not in found:
                found.append(resolved)
            if len(found) >= MAX_CANDIDATES_PER_MODULE:
                return found
        if parts[:-1]:
            resolved_directory = _resolve(current)
            if resolved_directory is not None and _contains_resolved(roots, resolved_directory):
                try:
                    if resolved_directory.is_dir() and resolved_directory not in found:
                        found.append(resolved_directory)
                except OSError:
                    pass
        if len(found) >= MAX_CANDIDATES_PER_MODULE:
            break
    return found


def _display_path(path: Path, workspace: Path, roots: list[Path]) -> str:
    try:
        return path.relative_to(workspace).as_posix()
    except ValueError:
        pass
    for index, root in enumerate(roots):
        if root == workspace:
            continue
        try:
            return f"<python-path-{index}>/{path.relative_to(root).as_posix()}"
        except ValueError:
            continue
    return "<unavailable>"


def _read_source(path: Path, remaining: int) -> str | None:
    if remaining <= 0:
        return None
    try:
        with path.open("rb") as stream:
            data = stream.read(min(MAX_SOURCE_BYTES, remaining) + 1)
    except OSError:
        return None
    if len(data) > min(MAX_SOURCE_BYTES, remaining):
        return None
    return data.decode("utf-8", errors="replace")


def _static_metadata(source: str | None, requested: list[str]) -> tuple[str | None, dict[str, bool]]:
    if source is None:
        return None, {name: False for name in requested}
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError, MemoryError):
        return None, {name: False for name in requested}
    version = None
    visible: set[str] = set()
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, ast.AnnAssign) else []
        if any(isinstance(target, ast.Name) and target.id == "__version__" for target in targets):
            value = node.value
            if isinstance(value, ast.Constant) and isinstance(value.value, (str, int, float)):
                version = str(value.value)[:100]
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            visible.add(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            names = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in names:
                visible.update(n.id for n in ast.walk(target) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store))
        elif isinstance(node, ast.Import):
            visible.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            visible.update(alias.asname or alias.name for alias in node.names if alias.name != "*")
    return version, {name: name in visible for name in requested}


@lru_cache(maxsize=4)
def _package_distribution_map(search_roots: tuple[str, ...]) -> dict[str, list[dict[str, str]]]:
    """Index distribution metadata only beneath the bounded interpreter search roots."""
    mapping: dict[str, list[dict[str, str]]] = {}
    for root in search_roots[:MAX_SEARCH_ROOTS]:
        try:
            distributions = importlib.metadata.distributions(path=[root])
            for distribution_index, dist in enumerate(distributions):
                if distribution_index >= MAX_DISTRIBUTIONS_PER_ROOT:
                    break
                try:
                    name = str(dist.metadata.get("Name") or "")[:200]
                    version = str(dist.metadata.get("Version") or "")[:100]
                    if not name or not version:
                        continue
                    top_level = dist.read_text("top_level.txt") or ""
                except Exception:
                    continue
                names = {
                    line.strip()
                    for line in top_level.splitlines()
                    if _NAME.fullmatch(line.strip()) and "." not in line.strip()
                }
                if not names:
                    fallback = re.sub(r"[-.]+", "_", name).lower()
                    if _NAME.fullmatch(fallback):
                        names.add(fallback)
                record = {"name": name, "version": version}
                for import_name in names:
                    bucket = mapping.setdefault(import_name, [])
                    if record not in bucket and len(bucket) < 8:
                        bucket.append(record)
        except Exception:
            continue
    return mapping


def _distribution_info(module_name: str, package_map: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    return package_map.get(module_name.split(".", 1)[0], [])


def discover_python_environment(
    workspace: str,
    modules: list[str] | None = None,
    api_names: list[str] | None = None,
) -> dict[str, Any]:
    """Inspect metadata and source text only; never resolve or execute target modules."""
    modules = [] if modules is None else modules
    api_names = [] if api_names is None else api_names
    if not _valid_list(modules, MAX_MODULES):
        raise ValueError(f"modules must contain at most {MAX_MODULES} valid dotted names")
    if not _valid_list(api_names, MAX_API_NAMES, dotted=False):
        raise ValueError(f"api_names must contain at most {MAX_API_NAMES} valid identifiers")

    root = _resolve(Path(workspace))
    if root is None:
        raise ValueError("workspace path cannot be resolved")
    roots = _search_roots(root)

    # Return only manifest filenames. Never inject arbitrary README, requirements,
    # setup.py, or Markdown prose into model context.
    project_files: list[dict[str, str]] = []
    for name in DEPENDENCY_FILES:
        path = root / name
        if _is_within(root, path):
            try:
                if path.is_file():
                    project_files.append({"file": name, "kind": "dependency_manifest"})
            except OSError:
                continue

    package_map = _package_distribution_map(tuple(str(path) for path in roots[1:]))

    results: list[dict[str, Any]] = []
    total_source_bytes = 0
    for name in modules:
        candidates = _module_candidates(name, roots)
        primary = next((path for path in candidates if path.is_file()), None)
        source = None
        if primary is not None:
            source = _read_source(primary, MAX_TOTAL_SOURCE_BYTES - total_source_bytes)
            if source is not None:
                total_source_bytes += len(source.encode("utf-8", errors="replace"))
        static_version, api = _static_metadata(source, api_names)
        distributions = _distribution_info(name, package_map)
        versions = {dist["version"] for dist in distributions}
        version = next(iter(versions)) if len(versions) == 1 else static_version
        results.append(
            {
                "module": name,
                "found": bool(candidates or distributions),
                "version": version,
                "distributions": distributions,
                "version_ambiguous": len(versions) > 1,
                "candidates": [_display_path(path, root, roots) for path in candidates],
                "api": api,
            }
        )

    result: dict[str, Any] = {
        "read_only": True,
        "python": sys.version.split()[0],
        "executable": Path(sys.executable).name,
        "virtual_env_active": bool(os.environ.get("VIRTUAL_ENV")),
        "workspace": ".",
        "project_files": project_files,
        "modules": results,
        "install_supported": False,
    }
    encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_RESULT_BYTES:
        raise ValueError(f"discovery result exceeds {MAX_RESULT_BYTES} byte limit")
    return result
