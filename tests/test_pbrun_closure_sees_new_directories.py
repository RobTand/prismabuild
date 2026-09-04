"""An untracked directory hides its files from the action's identity.

``git status --porcelain`` collapses a wholly-untracked directory to a single
``?? scratch/`` line, and the closure loop skipped that line because it names a
directory.  Every file under a newly created directory therefore contributed
nothing to the action key, so editing one left the key unmoved and the CAS
replayed the previous run's stdout -- a stale result indistinguishable from a
fresh one.

Measured on 2026-09-04 while porting the memory cap: two edited scripts under
an untracked ``scratch/`` produced action key ``579c3dc891ff`` twice, and the
second run replayed the first run's traceback verbatim.  This is the same bug
the comment above the loop already described, reached from the other side.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun


def _repo(root: Path) -> Path:
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(["git", "-C", str(root), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(root), "config", "user.name", "t"], check=True)
    (root / "tracked.txt").write_text("one\n")
    subprocess.run(["git", "-C", str(root), "add", "."], check=True)
    subprocess.run(["git", "-C", str(root), "commit", "-qm", "first"], check=True)
    return root


def test_editing_a_script_in_a_new_directory_moves_the_identity(tmp_path):
    root = _repo(tmp_path / "checkout")
    scratch = root / "scratch"
    scratch.mkdir()
    (scratch / "probe.sh").write_text("echo one\n")
    before = pbrun._git_identity(root)

    (scratch / "probe.sh").write_text("echo two\n")
    after = pbrun._git_identity(root)

    assert before["head"] == after["head"]
    assert before["dirty_sha256"] != after["dirty_sha256"], (
        "an edit under an untracked directory left the closure unmoved, so the "
        "CAS would replay the previous run"
    )


def test_a_new_file_beside_the_tracked_ones_still_moves_it(tmp_path):
    """The already-working case, kept so the fix cannot regress it."""

    root = _repo(tmp_path / "checkout")
    before = pbrun._git_identity(root)
    (root / "loose.sh").write_text("echo one\n")
    assert pbrun._git_identity(root)["dirty_sha256"] != before["dirty_sha256"]


def test_the_stamp_and_the_result_log_are_still_excluded(tmp_path):
    """Both are written INTO the tree by the submit computing this digest."""

    root = _repo(tmp_path / "checkout")
    before = pbrun._git_identity(root)
    (root / f"{pbrun.STAMP_PREFIX}deadbeef.json").write_text("{}")
    (root / f"{pbrun.RESULT_PREFIX}deadbeef.txt").write_text("output\n")
    assert pbrun._git_identity(root)["dirty_sha256"] == before["dirty_sha256"]


def test_an_ignored_directory_is_not_walked(tmp_path):
    """``-uall`` lists what git considers untracked, so a venv stays out."""

    root = _repo(tmp_path / "checkout")
    (root / ".gitignore").write_text("venv/\n")
    subprocess.run(["git", "-C", str(root), "add", ".gitignore"], check=True)
    subprocess.run(["git", "-C", str(root), "commit", "-qm", "ignore"], check=True)
    (root / "venv").mkdir()
    (root / "venv" / "big.bin").write_bytes(b"\x00" * 1024)
    before = pbrun._git_identity(root)
    (root / "venv" / "big.bin").write_bytes(b"\x01" * 1024)
    assert pbrun._git_identity(root)["dirty_sha256"] == before["dirty_sha256"]
