from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path
from typing import Any


SCHEMA = "workspace-fingerprint-v1"
_CHUNK_SIZE = 1024 * 1024


def _frame(hasher: Any, value: bytes) -> None:
    hasher.update(len(value).to_bytes(8, "big"))
    hasher.update(value)


def _ignored(name: str) -> bool:
    candidate = os.path.normcase(name)
    return candidate in {os.path.normcase(".nervipulsa"), os.path.normcase(".git")}


def _hash_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_CHUNK_SIZE):
            digest.update(chunk)
    return digest.digest()


def workspace_fingerprint(workspace: Path) -> dict[str, Any]:
    """Hash visible path names, node kinds and regular file bytes; never follow links."""
    root = Path(workspace)
    try:
        root_stat = root.lstat()
    except FileNotFoundError:
        return {"status": "unavailable", "error": "workspace does not exist"}
    except OSError as exc:
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
    if not stat.S_ISDIR(root_stat.st_mode):
        return {"status": "unavailable", "error": "workspace is not a directory"}

    try:
        entries: list[tuple[str, str, Path]] = []
        stack = [(root, "")]
        while stack:
            directory, relative_parent = stack.pop()
            children = list(os.scandir(directory))
            children.sort(key=lambda item: os.path.normcase(item.name))
            for child in children:
                if _ignored(child.name):
                    continue
                relative = f"{relative_parent}/{child.name}" if relative_parent else child.name
                mode = child.stat(follow_symlinks=False).st_mode
                if stat.S_ISLNK(mode):
                    kind = "symlink"
                elif stat.S_ISDIR(mode):
                    kind = "directory"
                    stack.append((Path(child.path), relative))
                elif stat.S_ISREG(mode):
                    kind = "file"
                else:
                    kind = "other"
                entries.append((relative.replace(os.sep, "/"), kind, Path(child.path)))

        tree = hashlib.sha256()
        tree.update(b"nervipulsa-workspace-merkle-v1\0")
        for relative, kind, path in sorted(entries, key=lambda item: os.path.normcase(item[0])):
            leaf = hashlib.sha256()
            leaf.update(b"entry-v1\0")
            _frame(leaf, relative.encode("utf-8", "surrogateescape"))
            _frame(leaf, kind.encode("ascii"))
            if kind == "file":
                leaf.update(_hash_file(path))
            elif kind == "symlink":
                _frame(leaf, os.fsencode(os.readlink(path)))
            tree.update(leaf.digest())
        return {"status": "available", "algorithm": SCHEMA, "sha256": tree.hexdigest()}
    except Exception as exc:
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}


def fingerprint_metadata(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    changed = None
    if before.get("status") == "available" and after.get("status") == "available":
        changed = before.get("sha256") != after.get("sha256")
    return {
        "schema": SCHEMA,
        "before": before,
        "after": after,
        "changed": changed,
        "observation": "non_atomic_window",
    }
