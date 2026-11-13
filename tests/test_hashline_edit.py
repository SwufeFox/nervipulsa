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


def test_batch_views_keep_full_snapshot_edit_identity(workspace: Path) -> None:
    left = [f"left {number}" for number in range(1, 201)]
    right = [f"right {number}" for number in range(1, 201)]
    (workspace / "left.py").write_text("\n".join(left) + "\n", encoding="utf-8")
    (workspace / "right.py").write_text("\n".join(right) + "\n", encoding="utf-8")
    hashline_edit._configure_workspace(workspace)

    blocks = hashline_edit.view_files([
        {"path": "left.py", "start": 150, "end": 152},
        {"path": "right.py", "start": 50, "end": 52},
    ]).split("\n\n")
    assert blocks[0].splitlines()[1:] == [f"{n}:{left[n - 1]}" for n in range(150, 153)]
    assert blocks[1].splitlines()[1:] == [f"{n}:{right[n - 1]}" for n in range(50, 53)]
    hashline_edit.edit(f"{blocks[0].splitlines()[0]}\nPUT 151.=151:\n+updated")
    left[150] = "updated"
    assert (workspace / "left.py").read_text(encoding="utf-8").splitlines() == left

    right[0] = "changed outside the displayed range"
    (workspace / "right.py").write_text("\n".join(right) + "\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="stale snapshot"):
        hashline_edit.edit(f"{blocks[1].splitlines()[0]}\nPUT 51.=51:\n+unsafe")
    assert (workspace / "right.py").read_text(encoding="utf-8").splitlines() == right


def test_batch_unicode_continuations_reconstruct_requested_ranges(workspace: Path) -> None:
    sources = {
        "source's.py": "\U00020000界" * 1200 + "\nlast\nnot requested\n",
        "other.py": "ä" * 2200 + "\nnot requested\n",
        "empty.py": "",
    }
    requests = [
        {"path": "source's.py", "end": 2},
        {"path": "other.py", "end": 1},
        {"path": "empty.py"},
    ]
    for path, text in sources.items():
        (workspace / path).write_text(text, encoding="utf-8")
    hashline_edit._configure_workspace(workspace)
    batch = hashline_edit.view_files(requests)
    assert len(batch.encode("utf-8")) <= 3072
    blocks = batch.split("\n\n")
    assert len(blocks) == len(requests)
    for request, first_page in zip(requests, blocks):
        page = first_page
        header = page.splitlines()[0]
        reconstructed: dict[int, str] = {}
        seen = set()
        for _ in range(20):
            assert len(page.encode("utf-8")) <= 3072
            assert page.splitlines()[0] == header
            for row in page.splitlines()[1:]:
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
            assert position not in seen
            seen.add(position)
            page = hashline_edit.view_file(request["path"], **cursor)
        else:
            pytest.fail("batch continuation did not reach the requested end")
        expected = sources[request["path"]].splitlines()[:request.get("end")] or [""]
        assert [reconstructed[n] for n in sorted(reconstructed)] == expected


def test_batch_repeated_file_uses_one_consistent_snapshot(workspace: Path, monkeypatch) -> None:
    source = workspace / "source.py"
    source.write_text("alpha\nbeta\n", encoding="utf-8")
    hashline_edit._configure_workspace(workspace)
    original_read = hashline_edit._read_raw
    changed = False

    def change_after_read(target: Path) -> bytes:
        nonlocal changed
        raw = original_read(target)
        if not changed:
            changed = True
            source.write_text("external\nexternal\n", encoding="utf-8")
        return raw

    monkeypatch.setattr(hashline_edit, "_read_raw", change_after_read)
    blocks = hashline_edit.view_files([
        {"path": "source.py", "start": 1, "end": 1},
        {"path": "source.py", "start": 2, "end": 2},
    ]).split("\n\n")
    assert blocks[0].splitlines()[0] == blocks[1].splitlines()[0]
    assert blocks[0].splitlines()[1:] == ["1:alpha"]
    assert blocks[1].splitlines()[1:] == ["2:beta"]
    with pytest.raises(RuntimeError, match="stale snapshot"):
        hashline_edit.edit(f"{blocks[0].splitlines()[0]}\nPUT 1.=1:\n+unsafe")
    assert source.read_text(encoding="utf-8") == "external\nexternal\n"


def test_batch_rejects_snapshots_that_would_expire_each_other(workspace: Path, monkeypatch) -> None:
    for path in ("first.py", "second.py"):
        (workspace / path).write_text("x" * 80, encoding="utf-8")
    hashline_edit._configure_workspace(workspace)
    monkeypatch.setattr(hashline_edit, "_MAX_SNAPSHOT_BYTES", 100)
    with pytest.raises(ValueError, match="snapshot cache byte limit"):
        hashline_edit.view_files([{"path": "first.py"}, {"path": "second.py"}])


@pytest.mark.parametrize("requests", [
    [],
    [{"path": "source.py"}] * 9,
    [{"path": "source.py", "start": 0}],
    [{"path": "source.py", "start_line": 2}],
    [{"path": "source.py"}, {"path": "../outside.py"}],
])
def test_invalid_batch_cannot_change_files(workspace: Path, requests: list) -> None:
    source = workspace / "source.py"
    source.write_text("first\nsecond\n", encoding="utf-8")
    hashline_edit._configure_workspace(workspace)
    with pytest.raises(ValueError):
        hashline_edit.view_files(requests)
    assert source.read_text(encoding="utf-8") == "first\nsecond\n"
