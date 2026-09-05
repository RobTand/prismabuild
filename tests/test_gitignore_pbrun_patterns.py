"""``.gitignore`` names pbrun's generated basenames and nothing else.

``core.pbrun_git_exclude_patterns`` is the grammar: a fixed prefix, sixteen hex
characters, a fixed suffix.  ``pbrun`` writes exactly those two patterns into a
checkout's ``.git/info/exclude`` and strips the broad globs a previous version
left there.  The tracked ``.gitignore`` has to say the same thing, because a
broad glob here drops a repository file whose name happens to start with
``pbrun_result.`` out of every bundle the sealer builds, silently.
"""
from __future__ import annotations

from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import core as pb  # noqa: E402


def _patterns_in_gitignore() -> list[str]:
    lines = (REPOSITORY / ".gitignore").read_text(encoding="utf-8").splitlines()
    return [
        line.strip()
        for line in lines
        if line.strip() and not line.lstrip().startswith("#")
    ]


def test_gitignore_carries_pbruns_exact_patterns() -> None:
    """The two exact patterns, and no broad glob standing in for them."""

    listed = _patterns_in_gitignore()
    for pattern in pb.pbrun_git_exclude_patterns():
        assert pattern in listed, (
            f".gitignore does not carry {pattern!r}, which pbrun writes into "
            f"every checkout's .git/info/exclude"
        )


def test_gitignore_carries_no_broad_pbrun_glob() -> None:
    """The globs ``pbrun`` removes from ``.git/info/exclude`` are gone here too.

    ``pbrun_result.*`` matches a tracked ``pbrun_result.notes.py``, and a
    tracked file the sealer cannot see is a bundle that does not build the tree
    the action ran in.
    """

    broad = {f"{pb.PBRUN_STAMP_PREFIX}*", f"{pb.PBRUN_RESULT_PREFIX}*"}
    assert not broad.intersection(_patterns_in_gitignore())
