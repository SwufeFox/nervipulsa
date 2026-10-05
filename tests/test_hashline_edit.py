from __future__ import annotations

from pathlib import Path

import pytest

from nervipulsa import hashline_edit


def test_hashline_put_subset_and_snapshot(workspace: Path) -> None:
    target = workspace / "sample.py"
    target.write_text("one\ntwo\nthree\n", encoding="utf-8")
    hashline_edit._configure_workspace(workspace)

    view = hashline_edit.view_file("sample.py")
    header = view.splitlines()[0]
    assert view == f"[{header[1:-1]}]\n1:one\n2:two\n3:three"

    updated = hashline_edit.edit(
        f"[{header[1:-1]}]\nPUT 2.=2:\n+replacement\nPUT >3:\n+last"
    )
    assert updated.splitlines()[1:] == ["1:one", "2:replacement", "3:three", "4:last"]
    assert target.read_text(encoding="utf-8") == "one\nreplacement\nthree\nlast\n"


def test_hashline_rejects_unsupported_omp_operations(workspace: Path) -> None:
    target = workspace / "sample.py"
    target.write_text("one\n", encoding="utf-8")
    hashline_edit._configure_workspace(workspace)
    view = hashline_edit.view_file("sample.py")
    header = view.splitlines()[0]

    for operation in ("CUT 1", "MV sample.py", "REM 1"):
        with pytest.raises(ValueError):
            hashline_edit.edit(f"{header}\n{operation}")


def test_hashline_rejects_stale_snapshot(workspace: Path) -> None:
    target = workspace / "sample.py"
    target.write_text("before\n", encoding="utf-8")
    hashline_edit._configure_workspace(workspace)
    header = hashline_edit.view_file("sample.py").splitlines()[0]
    target.write_text("external\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="stale snapshot"):
        hashline_edit.edit(f"{header}\nPUT 1.=1:\n+after")


def test_xxhash32_omp_reference_vector_and_file_tag(workspace: Path) -> None:
    target = workspace / "sample.py"
    target.write_bytes(b"abc")
    hashline_edit._configure_workspace(workspace)

    assert hashline_edit.xxhash32(b"") == 0x02CC5D05
    assert hashline_edit.xxhash32(b"abc") == 0x32D153FF
    assert hashline_edit.view_file("sample.py") == "[sample.py#53FF]\n1:abc"


def test_file_tag_normalizes_bom_line_endings_and_trailing_ascii_space(workspace: Path) -> None:
    legacy = workspace / "legacy.py"
    normalized = workspace / "normalized.py"
    legacy.write_bytes(b"\xef\xbb\xbffirst \t\r\nsecond  \r")
    normalized.write_bytes(b"first\nsecond\n")
    hashline_edit._configure_workspace(workspace)

    legacy_header = hashline_edit.view_file("legacy.py").splitlines()[0]
    normalized_header = hashline_edit.view_file("normalized.py").splitlines()[0]
    assert legacy_header.rsplit("#", 1)[1] == normalized_header.rsplit("#", 1)[1]


def test_hashline_put_before_and_end_of_file(workspace: Path) -> None:
    target = workspace / "sample.py"
    target.write_text("one\ntwo\nthree\n", encoding="utf-8")
    hashline_edit._configure_workspace(workspace)
    header = hashline_edit.view_file("sample.py").splitlines()[0]

    updated = hashline_edit.edit(
        f"{header}\nPUT <2:\n+before\nPUT >$:\n+after"
    )

    assert updated.splitlines()[1:] == [
        "1:one", "2:before", "3:two", "4:three", "5:after"
    ]
    assert target.read_text(encoding="utf-8") == "one\nbefore\ntwo\nthree\nafter\n"


def test_hashline_rejects_workspace_escape(workspace: Path) -> None:
    hashline_edit._configure_workspace(workspace)

    with pytest.raises(ValueError, match="parent-directory traversal"):
        hashline_edit.view_file("../outside.py")
