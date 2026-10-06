"""OMP-style, workspace-scoped Hashline view and PUT editing."""
from __future__ import annotations

import os
import re
import stat
import tempfile
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath

_MASK32 = 0xFFFFFFFF
_P1, _P2, _P3, _P4, _P5 = 2654435761, 2246822519, 3266489917, 668265263, 374761393
_MAX_FILE_BYTES = 4 * 1024 * 1024
_MAX_SNAPSHOT_ITEMS = 128
_MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024
_MAX_VERSIONS_PER_PATH = 4
_TAG_RE = re.compile(r"^[0-9A-F]{4}$")
_RANGE_RE = re.compile(r"^PUT ([1-9][0-9]*)\.=([1-9][0-9]*):$")
_BEFORE_RE = re.compile(r"^PUT <([1-9][0-9]*):$")
_AFTER_RE = re.compile(r"^PUT >([1-9][0-9]*):$")

_workspace_root: Path | None = None
_snapshots: OrderedDict[tuple[str, str, str, str], "_Snapshot"] = OrderedDict()
_snapshot_bytes = 0


@dataclass(frozen=True)
class _Snapshot:
    relative_path: str
    tag: str
    text: str
    bom: bool
    line_ending: str
    size_bytes: int


def xxhash32(data: bytes | str, seed: int = 0) -> int:
    """Return xxHash32 using only the Python standard library."""
    if isinstance(data, str):
        data = data.encode("utf-8")

    def rotl(value: int, count: int) -> int:
        return ((value << count) | (value >> (32 - count))) & _MASK32

    def round_(acc: int, word: int) -> int:
        return (rotl((acc + word * _P2) & _MASK32, 13) * _P1) & _MASK32

    length = len(data)
    index = 0
    if length >= 16:
        v1 = (seed + _P1 + _P2) & _MASK32
        v2 = (seed + _P2) & _MASK32
        v3 = seed & _MASK32
        v4 = (seed - _P1) & _MASK32
        while index <= length - 16:
            v1 = round_(v1, int.from_bytes(data[index:index + 4], "little")); index += 4
            v2 = round_(v2, int.from_bytes(data[index:index + 4], "little")); index += 4
            v3 = round_(v3, int.from_bytes(data[index:index + 4], "little")); index += 4
            v4 = round_(v4, int.from_bytes(data[index:index + 4], "little")); index += 4
        result = (rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18)) & _MASK32
    else:
        result = (seed + _P5) & _MASK32
    result = (result + length) & _MASK32
    while index + 4 <= length:
        result = (rotl((result + int.from_bytes(data[index:index + 4], "little") * _P3) & _MASK32, 17) * _P4) & _MASK32
        index += 4
    while index < length:
        result = (rotl((result + data[index] * _P5) & _MASK32, 11) * _P1) & _MASK32
        index += 1
    result ^= result >> 15
    result = (result * _P2) & _MASK32
    result ^= result >> 13
    result = (result * _P3) & _MASK32
    return (result ^ (result >> 16)) & _MASK32


def _configure_workspace(path: str | os.PathLike[str]) -> None:
    """Fix the workspace root for this worker epoch and clear old snapshots."""
    global _workspace_root, _snapshot_bytes
    if os.environ.get("NERVIPULSA_APPCONTAINER") == "1":
        # The host canonicalizes the workspace before launch. Avoid probing its
        # protected parent directories from inside the AppContainer token.
        root = Path(os.path.abspath(path))
    else:
        root = Path(path).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("worker workspace must be a directory")
    if _workspace_root != root:
        _snapshots.clear()
        _snapshot_bytes = 0
    _workspace_root = root


def _root() -> Path:
    if _workspace_root is None:
        raise RuntimeError("Hashline library is not attached to a worker workspace")
    return _workspace_root


def _relative_path(path: str | os.PathLike[str]) -> tuple[str, Path]:
    raw = os.fspath(path)
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        raise ValueError("path must be a non-empty workspace-relative string")
    candidate = Path(raw)
    if candidate.is_absolute() or PureWindowsPath(raw).drive or raw.startswith(("\\\\", "//")):
        raise ValueError("absolute paths are not allowed; use a workspace-relative path")
    if ".." in candidate.parts:
        raise ValueError("parent-directory traversal is not allowed")
    if os.environ.get("NERVIPULSA_APPCONTAINER") == "1":
        root = _root()
        target = Path(os.path.abspath(root / candidate))
        current = root
        for part in candidate.parts:
            current = current / part
            try:
                info = current.lstat()
            except FileNotFoundError:
                break
            attributes = getattr(info, "st_file_attributes", 0)
            if stat.S_ISLNK(info.st_mode) or attributes & 0x400:
                raise ValueError("Hashline does not follow workspace symlinks or reparse points")
    else:
        target = ( _root() / candidate ).resolve(strict=True)
    try:
        target.relative_to(_root())
    except ValueError as exc:
        raise ValueError("path resolves outside the worker workspace") from exc
    if not target.is_file():
        raise ValueError("Hashline can only view existing regular files")
    relative = target.relative_to(_root()).as_posix()
    return relative, target


def _normalize(raw: bytes) -> tuple[str, bool, str]:
    if len(raw) > _MAX_FILE_BYTES:
        raise ValueError(f"file exceeds Hashline size limit ({_MAX_FILE_BYTES} bytes)")
    has_bom = raw.startswith(b"\xef\xbb\xbf")
    text = raw.decode("utf-8-sig")
    first_lf = text.find("\n")
    first_crlf = text.find("\r\n")
    line_ending = "\r\n" if first_lf >= 0 and first_crlf >= 0 and first_crlf < first_lf else "\n"
    return text.replace("\r\n", "\n").replace("\r", "\n"), has_bom, line_ending


def _read_raw(target: Path) -> bytes:
    with target.open("rb") as stream:
        raw = stream.read(_MAX_FILE_BYTES + 1)
    if len(raw) > _MAX_FILE_BYTES:
        raise ValueError(f"file exceeds Hashline size limit ({_MAX_FILE_BYTES} bytes)")
    return raw


def _tag(text: str) -> str:
    # Match OMP file_hash: trim trailing ASCII space, tab and CR per line,
    # retaining LF separators and the original final-newline state.
    normalized = "\n".join(line.rstrip(" \t\r") for line in text.split("\n"))
    return f"{xxhash32(normalized) & 0xFFFF:04X}"


def _snapshot_key(snapshot: _Snapshot) -> tuple[str, str, str, str]:
    return (str(_root()), snapshot.relative_path, snapshot.tag, snapshot.text)


def _remember(snapshot: _Snapshot) -> None:
    global _snapshot_bytes
    key = _snapshot_key(snapshot)
    previous = _snapshots.pop(key, None)
    if previous is not None:
        _snapshot_bytes -= previous.size_bytes
    _snapshots[key] = snapshot
    _snapshot_bytes += snapshot.size_bytes
    path_keys = [k for k in _snapshots if k[:2] == key[:2]]
    for old_key in path_keys[:-_MAX_VERSIONS_PER_PATH]:
        removed = _snapshots.pop(old_key)
        _snapshot_bytes -= removed.size_bytes
    while len(_snapshots) > _MAX_SNAPSHOT_ITEMS or _snapshot_bytes > _MAX_SNAPSHOT_BYTES:
        _, removed = _snapshots.popitem(last=False)
        _snapshot_bytes -= removed.size_bytes


def _render(snapshot: _Snapshot) -> str:
    lines = snapshot.text.split("\n")
    if snapshot.text.endswith("\n"):
        lines.pop()
    rows = [f"[{snapshot.relative_path}#{snapshot.tag}]"]
    rows.extend(f"{number}:{line}" for number, line in enumerate(lines, 1))
    return "\n".join(rows)


def view_file(path: str | os.PathLike[str]) -> str:
    """Return OMP-style ``[path#TAG]`` and ``N:text`` rows; records a snapshot."""
    relative, target = _relative_path(path)
    raw = _read_raw(target)
    text, bom, line_ending = _normalize(raw)
    size_bytes = len(text.encode("utf-8"))
    snapshot = _Snapshot(relative, _tag(text), text, bom, line_ending, size_bytes)
    _remember(snapshot)
    return _render(snapshot)


def _parse_patch(patch: str) -> tuple[str, str, list[tuple[str, int, int, tuple[str, ...]]]]:
    if not isinstance(patch, str):
        raise TypeError("Hashline patch must be a string")
    rows = patch.splitlines()
    if rows and rows[0].strip() == "*** Begin Patch":
        rows = rows[1:]
    if rows and rows[-1].strip() == "*** End Patch":
        rows = rows[:-1]
    if not rows or not (rows[0].startswith("[") and rows[0].endswith("]")):
        raise ValueError("patch must start with [workspace/path#TAG]")
    header = rows.pop(0)[1:-1]
    if "#" not in header:
        raise ValueError("file header must include a #TAG")
    path, tag = header.rsplit("#", 1)
    if not path or not _TAG_RE.fullmatch(tag):
        raise ValueError("file header requires a relative path and four uppercase hex digits")
    operations: list[tuple[str, int, int, tuple[str, ...]]] = []
    index = 0
    while index < len(rows):
        line = rows[index]
        if not line.startswith("PUT "):
            raise ValueError(f"expected PUT hunk, got {line!r}")
        range_match = _RANGE_RE.fullmatch(line)
        before_match = _BEFORE_RE.fullmatch(line)
        after_match = _AFTER_RE.fullmatch(line)
        if range_match:
            start, end = map(int, range_match.groups())
            kind = "replace"
            a, b = start, end
        elif before_match:
            kind = "before"
            a = b = int(before_match.group(1))
        elif after_match:
            kind = "after"
            a = b = int(after_match.group(1))
        elif line == "PUT >$:":
            kind = "eof"
            a = b = 0
        else:
            raise ValueError(f"unsupported or malformed PUT header: {line!r}")
        index += 1
        payload: list[str] = []
        while index < len(rows) and not rows[index].startswith("PUT "):
            if not rows[index].startswith("+"):
                raise ValueError("PUT body rows must begin with + (use a lone + for an empty line)")
            payload.append(rows[index][1:])
            index += 1
        operations.append((kind, a, b, tuple(payload)))
    if not operations:
        raise ValueError("patch must contain at least one PUT hunk")
    return path, tag, operations


def edit(patch: str) -> str:
    """Apply OMP-style PUT hunks to the exact snapshot named in ``patch``.

    Supported headers: ``PUT N.=M:``, ``PUT <N:``, ``PUT >N:``, ``PUT >$:``.
    Coordinates refer to the same original snapshot; each replacement line is
    prefixed with ``+``. Returns a freshly rendered view with the new tag.
    """
    path, tag, operations = _parse_patch(patch)
    relative, target = _relative_path(path)
    if relative != path.replace("\\", "/"):
        raise ValueError("use the canonical workspace-relative path shown by view_file")
    candidates = [s for key, s in _snapshots.items() if key[:3] == (str(_root()), relative, tag)]
    if not candidates:
        raise RuntimeError("snapshot tag is unknown or expired; call view_file(path) again")
    snapshot = candidates[-1]
    current_text, current_bom, current_ending = _normalize(_read_raw(target))
    if current_text != snapshot.text or current_bom != snapshot.bom or current_ending != snapshot.line_ending:
        raise RuntimeError("stale snapshot: file changed since view_file; view it again")

    original = snapshot.text.split("\n")
    if snapshot.text.endswith("\n"):
        original.pop()
    count = len(original)
    planned: list[tuple[str, int, int, tuple[str, ...]]] = []
    occupied: list[tuple[int, int]] = []
    insertion_points: set[int] = set()
    for kind, a, b, payload in operations:
        if kind == "replace":
            if a > b or b > count:
                raise ValueError(f"replacement range {a}..{b} is outside the {count}-line snapshot")
            start, end = a - 1, b
            if any(start < old_end and old_start < end for old_start, old_end in occupied):
                raise ValueError("duplicate or overlapping PUT ranges")
            if any(start <= point <= end for point in insertion_points):
                raise ValueError("PUT insertion overlaps a replacement boundary")
            occupied.append((start, end))
            planned.append((kind, start, end, payload))
            continue
        if kind == "before":
            if a > count and not (count == 0 and a == 1):
                raise ValueError(f"PUT <{a} is outside the {count}-line snapshot")
            point = a - 1
        elif kind == "after":
            if a > count:
                raise ValueError(f"PUT >{a} is outside the {count}-line snapshot")
            point = a
        else:
            point = count
        if point in insertion_points:
            raise ValueError("duplicate PUT insertion point")
        if any(start <= point <= end for start, end in occupied):
            raise ValueError("PUT insertion overlaps a replacement boundary")
        insertion_points.add(point)
        planned.append((kind, point, point, payload))

    result = list(original)
    for kind, start, end, payload in sorted(planned, key=lambda op: (op[1], op[2]), reverse=True):
        if kind == "replace":
            result[start:end] = payload
        else:
            result[start:start] = payload
    final_text = "\n".join(result)
    if result and snapshot.text.endswith("\n"):
        final_text += "\n"
    encoded_text = final_text.replace("\n", snapshot.line_ending)
    raw = ("\ufeff" if snapshot.bom else "") + encoded_text
    data = raw.encode("utf-8")
    if len(data) > _MAX_FILE_BYTES:
        raise ValueError(f"edited file exceeds Hashline size limit ({_MAX_FILE_BYTES} bytes)")

    # Re-resolve and compare immediately before atomic replacement.
    relative_now, target_now = _relative_path(path)
    if relative_now != relative or target_now != target:
        raise RuntimeError("workspace path changed while applying Hashline edit")
    latest, latest_bom, latest_ending = _normalize(_read_raw(target))
    if latest != snapshot.text or latest_bom != snapshot.bom or latest_ending != snapshot.line_ending:
        raise RuntimeError("stale snapshot: file changed before write")
    mode = stat.S_IMODE(target.stat().st_mode)
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temp_name, mode)
        os.replace(temp_name, target)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise
    return view_file(path)


__all__ = ["view_file", "edit"]
