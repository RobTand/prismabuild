"""A code reference nobody checks decays into a confident wrong statement.

The commit-addressed-checkout design turns on particular lines of particular
files: the three places the submitter's absolute path is bound into the action
key, the ref lock it must qualify on NFS, the closure check it must keep
honest.  If one of those lines changes, the design needs re-reading before it
is built.

It cited `file:line` twice, and both versions went stale inside an hour --
the work the design describes moves those very lines, and two separate
commits went to re-pointing them by hand.  A line-number check would then
fail this suite on every unrelated edit to ``pbrun.py``, which five branches
edit at once; that is a check people delete, not a check people keep.  So the
doc quotes the line instead, and this asserts the quotation still occurs in
the named file, exactly once.  It fires when the cited code actually changes
-- which is exactly when a reader of the design needs to know.

It judges nothing about whether a reference is *apt*.  That is the reader's
job, and it is the job an unchecked reference quietly takes away.
"""

from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
DOCS = sorted((ROOT / "docs").rglob("*.md"))
SECTION = "## Line references"

#: One table row: | where | `the quoted line` |
ROW = re.compile(r"^\|\s*(.+?)\s*\|\s*(`.*`)\s*\|\s*$")
#: The first backticked token of a row's "where" -- a path, or a module and
#: optionally a symbol inside it (``pool.PoolQueue.publish``).
WHERE = re.compile(r"`([A-Za-z0-9_./]+)`")


def _resolve(where: str) -> Path:
    """The file a row's "where" names.  Paths as written, modules by name.

    The token is tried whole first, then with successive trailing
    dot-components dropped, so ``docs/design.md`` resolves as a path while
    ``pool.PoolQueue.publish`` walks back to ``pool``.
    """

    match = WHERE.search(where)
    assert match, f"row names no file: {where}"
    token = match.group(1)
    while True:
        for candidate in (ROOT / token, ROOT / f"{token}.py",
                          ROOT / "src" / "prismabuild" / f"{token}.py",
                          ROOT / "tools" / "fleet" / f"{token}.py"):
            if candidate.is_file():
                return candidate
        if "." not in token:
            raise AssertionError(f"no such file for {where!r}")
        token = token.rsplit(".", 1)[0]


def _table(text: str) -> list[tuple[str, str]]:
    """The rows under ``## Line references``, and only those.

    Scoped to the section rather than to "any two-cell row", because other
    docs here keep tables that are not references and must not be read as
    making claims they do not make.
    """

    if SECTION not in text:
        return []
    out: list[tuple[str, str]] = []
    for line in text.split(SECTION, 1)[1].splitlines():
        match = ROW.match(line)
        if not match or set(match.group(1)) <= set("- "):
            continue
        cell = match.group(2)
        # The quoted line may itself contain backticks, so take everything
        # between the first and the last -- and un-escape the pipe, the one
        # character a markdown row cannot carry raw.
        out.append((match.group(1), cell[1:-1].replace(r"\|", "|")))
    return out


def test_every_quoted_line_is_still_in_the_file_the_doc_names() -> None:
    checked = 0
    for doc in DOCS:
        rows = _table(doc.read_text())
        if not rows:
            continue                  # this doc keeps no line-reference table
        for where, quoted in rows:
            path = _resolve(where)
            hits = [i + 1 for i, line in enumerate(path.read_text().splitlines())
                    if line == quoted]
            assert hits, (
                f"{doc.name}: {where} quotes a line that is no longer in "
                f"{path.relative_to(ROOT)}:\n  {quoted!r}"
            )
            assert len(hits) == 1, (
                f"{doc.name}: {where} quotes a line that now occurs "
                f"{len(hits)} times in {path.relative_to(ROOT)} (lines "
                f"{hits}), so it no longer names one place"
            )
            checked += 1
    # One test rather than one per doc, so a suite that suddenly checks
    # nothing says so instead of reporting a handful of green skips -- and a
    # skip is exactly what a reader scanning for a CUDA-gated one reads past.
    assert checked, "no doc keeps a line-reference table; the check is vacuous"
