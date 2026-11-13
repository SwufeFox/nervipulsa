from __future__ import annotations

import ast
import re
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


def _continuation(page: str) -> dict | None:
    footer = page.splitlines()[-1]
    if not footer.startswith("[More: "):
        return None
    # Parse the public continuation call without executing file content.
    printing = ast.parse(footer[len("[More: "):-1], mode="eval").body
    call = printing.args[0]
    return {keyword.arg: ast.literal_eval(keyword.value) for keyword in call.keywords}


def test_paged_view_reconstructs_unicode_and_long_lines(workspace: Path) -> None:
    lines = ["first", "\U00020000界" * 1600]
    lines.extend(f"row {number}: " + "ä界" * 25 for number in range(150))
    target = workspace / "source's.txt"
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    hashline_edit._configure_workspace(workspace)
    cursor = {}
    reconstructed: dict[int, str] = {}
    header = None
    seen_cursors = set()
    for _ in range(100):
        page = hashline_edit.view_file(target.name, **cursor)
        assert len(page.encode("utf-8")) <= 3072
        rows = page.splitlines()
        if header is None:
            header = rows[0]
        assert rows[0] == header
        for row in rows[1:]:
            if row.startswith("[More: "):
                continue
            match = re.fullmatch(r"(\d+)(?:\+(\d+))?:(.*)", row)
            assert match is not None, row
            number, offset, content = match.groups()
            number = int(number)
            previous = reconstructed.get(number, "")
            assert int(offset or 0) == len(previous)
            reconstructed[number] = previous + content
        cursor = _continuation(page)
        if cursor is None:
            break
        position = (cursor["start"], cursor["column"])
        assert position not in seen_cursors
        seen_cursors.add(position)
    else:
        pytest.fail("source reader did not reach the end of the file")
    assert [reconstructed[i] for i in range(1, len(lines) + 1)] == lines


def test_targeted_page_keeps_whole_file_edit_identity(workspace: Path) -> None:
    lines = [f"line {number}: " + "content " * 8 for number in range(1, 201)]
    target = workspace / "large.txt"
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    hashline_edit._configure_workspace(workspace)
    first_header = hashline_edit.view_file(target.name).splitlines()[0]
    page = hashline_edit.view_file(target.name, start=150, end=152)
    assert page.splitlines() == [first_header] + [
        f"{number}:{lines[number - 1]}" for number in range(150, 153)
    ]
    result = hashline_edit.edit(f"{first_header}\nPUT 151.=151:\n+changed")
    assert len(result.encode("utf-8")) <= 3072
    assert target.read_text(encoding="utf-8").splitlines()[150] == "changed"
    header = hashline_edit.view_file(target.name, start=150, end=152).splitlines()[0]
    contents = target.read_text(encoding="utf-8").splitlines()
    contents[0] = "changed outside the displayed range"
    target.write_text("\n".join(contents) + "\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="stale snapshot"):
        hashline_edit.edit(f"{header}\nPUT 151.=151:\n+unsafe")
    assert target.read_text(encoding="utf-8").splitlines()[150] == "changed"


@pytest.mark.parametrize("cursor", [{"start": 0}, {"start": 3}, {"column": -1},
                                    {"column": 99}, {"start": 2, "end": 1}])
def test_invalid_view_cursor_cannot_change_file(workspace: Path, cursor: dict) -> None:
    target = workspace / "cursor.txt"
    target.write_text("first\nsecond\n", encoding="utf-8")
    hashline_edit._configure_workspace(workspace)
    with pytest.raises(ValueError):
        hashline_edit.view_file(target.name, **cursor)
    assert target.read_text(encoding="utf-8") == "first\nsecond\n"
