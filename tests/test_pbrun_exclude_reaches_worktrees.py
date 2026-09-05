"""pbrun's droppings must stay out of git in the checkouts agents actually make.

``pbrun`` writes a closure stamp into the checkout it submits from, and adds
the exact generated stamp and result-log grammars to git's local exclude file
so they cannot make a clean tree look dirty to anything else. It computed that
file as
``cwd/.git/info/exclude``, which is a directory only in a repository root
that is not a linked worktree.  In a ``git worktree`` checkout -- which is
what every agent on this fleet is given -- ``.git`` is a FILE, the write was
skipped, and the comment recorded that as "not an error".

It is an error.  An untracked file in a tree several agents stage broadly in
is a file that gets committed, and one was: a stamp landed on this branch
under a ``git add -A``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "pbrun_exclude", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", "-C", str(cwd), *args],
                          capture_output=True, text=True)


def _repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "t@example.invalid")
    _git(root, "config", "user.name", "t")
    (root / "a.txt").write_text("hi\n")
    _git(root, "add", "a.txt")
    _git(root, "commit", "-qm", "init")
    return root


def _ignored(cwd: Path, name: str) -> bool:
    return _git(cwd, "check-ignore", "-q", name).returncode == 0


def test_a_linked_worktree_is_taught_to_ignore_the_stamp(tmp_path: Path) -> None:
    """The case that was silently skipped, and the one every agent submits from."""

    root = _repo(tmp_path)
    tree = tmp_path / "wt"
    _git(root, "worktree", "add", "-q", str(tree), "-b", "probe")
    stamp = f"{pbrun.STAMP_PREFIX}{'a' * 16}.json"
    (tree / stamp).write_text("{}\n")
    assert not _ignored(tree, stamp)              # the state that let it be committed

    written = pbrun.keep_droppings_out_of_git(tree)

    assert written is not None
    assert _ignored(tree, stamp)
    assert _ignored(tree, f"{pbrun.RESULT_PREFIX}{'a' * 16}.txt")


def test_it_writes_the_exclude_git_actually_reads(tmp_path: Path) -> None:
    """--git-common-dir, not --git-dir: in a worktree only one of them is read.

    Measured rather than reasoned: a pattern in
    ``.git/worktrees/<name>/info/exclude`` does not match.  So the test names
    the file, not just the effect -- a fix that happened to work through some
    other path would be a fix nobody could keep.
    """

    root = _repo(tmp_path)
    tree = tmp_path / "wt"
    _git(root, "worktree", "add", "-q", str(tree), "-b", "probe")
    stamp = f"{pbrun.STAMP_PREFIX}{'a' * 16}.json"
    (tree / stamp).write_text("{}\n")

    # The measurement itself, staged rather than reasoned about: the same
    # pattern in the per-worktree exclude ignores nothing.  Asserting only
    # that the fix stayed out of this file left the premise unread, and a Git
    # that started reading it would make the choice of file arbitrary.
    per_worktree = root / ".git" / "worktrees" / "wt" / "info" / "exclude"
    per_worktree.parent.mkdir(parents=True, exist_ok=True)
    # Use the exact stamp so an accidental append of the general pattern to
    # this wrong file remains observable, even after staging the premise.
    per_worktree.write_text(f"{stamp}\n")
    assert not _ignored(tree, stamp)

    written = pbrun.keep_droppings_out_of_git(tree)

    assert written == root / ".git" / "info" / "exclude"
    assert pbrun.STAMP_PREFIX in written.read_text()   # named, and written
    assert _ignored(tree, stamp)                       # and read back by git
    assert per_worktree.read_text() == f"{stamp}\n"


def test_a_repository_root_still_works_and_is_not_written_twice(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    stamp = f"{pbrun.STAMP_PREFIX}{'a' * 16}.json"
    (root / stamp).write_text("{}\n")

    first = pbrun.keep_droppings_out_of_git(root)
    assert first == root / ".git" / "info" / "exclude"
    assert _ignored(root, stamp)
    once = first.read_text()

    assert pbrun.keep_droppings_out_of_git(root) == first
    assert first.read_text() == once           # appending on every submit grows a file


def test_only_the_exact_generated_basename_grammar_is_ignored(tmp_path: Path) -> None:
    """A legacy prefix glob must not hide a legitimate untracked payload."""

    root = _repo(tmp_path)
    exclude = root / ".git" / "info" / "exclude"
    with exclude.open("a", encoding="utf-8") as handle:
        handle.write(f"{pbrun.STAMP_PREFIX}*\n")
        handle.write(f"{pbrun.RESULT_PREFIX}*\n")

    assert pbrun.keep_droppings_out_of_git(root) == exclude

    assert _ignored(root, f"{pbrun.STAMP_PREFIX}{'0' * 16}.json")
    assert _ignored(root, f"{pbrun.RESULT_PREFIX}{'f' * 16}.txt")
    assert not _ignored(root, f"{pbrun.STAMP_PREFIX}notes.json")
    assert not _ignored(root, f"{pbrun.RESULT_PREFIX}notes.py")


def test_legacy_exclude_migration_fails_closed_when_it_cannot_publish(
    tmp_path: Path, monkeypatch,
) -> None:
    """A broad legacy glob must not survive behind a reported success."""

    root = _repo(tmp_path)
    exclude = root / ".git" / "info" / "exclude"
    with exclude.open("a", encoding="utf-8") as handle:
        handle.write(f"{pbrun.RESULT_PREFIX}*\n")
    real_replace = pbrun.os.replace

    def refuse_replace(source, destination):
        if Path(destination) == exclude:
            raise PermissionError("simulated unwritable common exclude")
        return real_replace(source, destination)

    monkeypatch.setattr(pbrun.os, "replace", refuse_replace)
    with pytest.raises(SystemExit, match="cannot update pbrun Git excludes"):
        pbrun.keep_droppings_out_of_git(root)


def test_git_exclude_setup_refuses_failed_initial_repository_detection(
    tmp_path: Path, monkeypatch,
) -> None:
    """A checkout marker plus failed rev-parse is uncertainty, not no-Git."""

    root = _repo(tmp_path)
    real_run = pbrun.subprocess.run

    def fail_common_dir(argv, *args, **kwargs):
        if (
            argv[:3] == ["git", "-C", str(root)]
            and "--git-common-dir" in argv
        ):
            return subprocess.CompletedProcess(
                argv, 1, stdout="", stderr="simulated initial Git failure"
            )
        return real_run(argv, *args, **kwargs)

    monkeypatch.setattr(pbrun.subprocess, "run", fail_common_dir)
    with pytest.raises(SystemExit, match="cannot inspect local Git excludes"):
        pbrun.keep_droppings_out_of_git(root)


def test_a_checkout_that_is_not_a_git_repository_is_not_an_error(tmp_path: Path) -> None:
    """Submitting from a plain directory is supported; it just has nothing to tell."""

    plain = tmp_path / "plain"
    plain.mkdir()

    assert pbrun.keep_droppings_out_of_git(plain) is None
