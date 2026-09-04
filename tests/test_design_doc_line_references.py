"""A `file:line` citation nobody checks decays into a confident wrong number.

The commit-addressed-checkout design cites the code it has to move: the three
places the submitter's absolute path is bound into the action key, the ref
lock it must qualify, the closure check it must keep honest.  Those citations
have already rotted twice on this branch -- the design was written against one
arrangement of ``pbrun.py``, the work it describes moved those lines, and two
separate commits went to re-pointing them by hand.  A third would have been a
pattern.

So the doc repeats every citation in a table beside the line it names, and
this reads both.  Prose and table must agree on the set of citations, and each
cited line must still be the line the table quotes.  Nothing here judges
whether the citation is *apt* -- that is the reader's job, and it is the job
this check exists to make possible.
"""

from __future__ import annotations

from pathlib import Path
import re

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCS = sorted((ROOT / "docs").glob("*.md"))

#: `pbrun.py:198-202` or `pool.py:319`, in prose or in a table cell.
CITATION = re.compile(r"`([A-Za-z0-9_./]+\.(?:py|md)):(\d+)(?:-(\d+))?`")
#: One table row: | `citation` | `the line` |
ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|\s*(.+?)\s*\|\s*$")


def _resolve(name: str) -> Path:
    """Where a citation's short name lives.  The doc cites files, not paths."""

    for candidate in (ROOT / name, ROOT / "src" / "prismabuild" / name,
                      ROOT / "tools" / "fleet" / name):
        if candidate.is_file():
            return candidate
    raise AssertionError(f"cited file does not exist: {name}")


SECTION = "## Line references"


def _table(text: str) -> dict[str, str]:
    """The rows under ``## Line references``, and only those.

    Scoped to the section rather than to "any row whose first cell is in
    backticks", because other docs here keep tables of file names and would
    otherwise be read as making citations they do not make.
    """

    if SECTION not in text:
        return {}
    out: dict[str, str] = {}
    for line in text.split(SECTION, 1)[1].splitlines():
        match = ROW.match(line)
        if not match or match.group(1).startswith("citation"):
            continue
        cell = match.group(2)
        if not (cell.startswith("`") and cell.endswith("`")):
            continue
        # The quoted source line may itself contain backticks, so take
        # everything between the first and the last -- and un-escape the pipe,
        # which is the one character a markdown row cannot carry raw.
        out[match.group(1)] = cell[1:-1].replace(r"\|", "|")
    return out


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_every_cited_line_is_still_the_line_the_doc_quotes(doc: Path) -> None:
    text = doc.read_text()
    table = _table(text)
    if not table:
        pytest.skip(f"{doc.name} keeps no line-reference table")

    body = text.split(SECTION, 1)[0]
    prose = {m.group(0).strip("`") for m in CITATION.finditer(body)}
    assert prose == set(table), (
        f"{doc.name}: prose and the line-reference table disagree; "
        f"only in prose {sorted(prose - set(table))}, "
        f"only in the table {sorted(set(table) - prose)}"
    )

    for citation, quoted in sorted(table.items()):
        name, first = citation.split(":", 1)
        start = int(first.split("-", 1)[0])
        lines = _resolve(name).read_text().splitlines()
        assert start <= len(lines), f"{citation}: file has {len(lines)} lines"
        assert lines[start - 1] == quoted, (
            f"{citation} has moved\n  doc says: {quoted!r}\n  file has: "
            f"{lines[start - 1]!r}"
        )
