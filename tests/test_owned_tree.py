"""Owner locks decide what PrismaBuild may reap; names and age never do.

``owned_tree`` is the one implementation behind pbtest's attempt base temps,
private checkouts and pbrun's snapshot workspaces. A tree goes only when its
owner lock is free (the kernel dropped it with the owner), names this host,
and its retention clock has run; the removal never follows a link, never
leaves the tree's mount, and restores owner rwx to read-only directories.
"""
from __future__ import annotations

import os
import re
import socket
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from prismabuild import owned_tree  # noqa: E402

PATTERN = re.compile(r"pb-[a-z0-9_]{8}")
WINDOW_S = owned_tree.RETENTION_S
FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC


class Parent:
    """A directory held open, as every caller of the module holds one."""

    def __init__(self, path: Path):
        path.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.fd = os.open(path, FLAGS)

    def close(self):
        os.close(self.fd)

    def own(self, name: str) -> int:
        """Create ``name`` and hold its inside lock; returns the lock fd."""

        (self.path / name).mkdir()
        held = os.open(name, FLAGS, dir_fd=self.fd)
        try:
            return owned_tree.take_owner_lock(held, owned_tree.LOCK_NAME)
        finally:
            os.close(held)

    def reap(self, **options) -> list[str]:
        return owned_tree.reap_dead_trees(self.fd, PATTERN, **options)


@pytest.fixture
def parent(tmp_path):
    held = Parent(tmp_path / "tmpdir")
    yield held
    held.close()
    for path in tmp_path.rglob("*"):
        if path.is_dir() and not path.is_symlink():
            path.chmod(0o700)


def _age(path: Path, seconds: float) -> None:
    then = time.time() - seconds
    os.utime(path, (then, then), follow_symlinks=False)


def _later(seconds: float = WINDOW_S + 60) -> float:
    return time.time() + seconds


def test_a_live_owner_is_never_reaped_at_any_age(parent):
    lock = parent.own("pb-livelive")
    try:
        _age(parent.path / "pb-livelive" / owned_tree.LOCK_NAME, 365 * 86400)
        assert parent.reap(now=_later(365 * 86400)) == []
        assert (parent.path / "pb-livelive").is_dir()
        body = (parent.path / "pb-livelive" / owned_tree.LOCK_NAME).read_bytes()
        assert body.endswith(b"running\n"), "a live owner's clock was started"
    finally:
        os.close(lock)


def test_a_dead_owner_starts_its_clock_then_goes_after_the_window(parent):
    lock = parent.own("pb-deadbeef")
    (parent.path / "pb-deadbeef" / "pytest").mkdir()
    os.close(lock)                                   # the owner is gone
    assert parent.reap(now=_later()) == [], "removed before its clock started"
    assert parent.reap() == [], "removed inside its window"
    assert parent.reap(now=_later()) == ["pb-deadbeef"]
    assert list(parent.path.iterdir()) == []


def test_an_ended_owner_keeps_its_tree_for_the_window(parent):
    lock = parent.own("pb-failed00")
    owned_tree.release_owned_tree(parent.fd, "pb-failed00", lock, remove=False)
    assert parent.reap(now=_later(WINDOW_S - 60)) == []
    assert parent.reap(now=_later()) == ["pb-failed00"]


def test_a_passing_owner_removes_its_tree_with_read_only_parts(parent):
    lock = parent.own("pb-passing0")
    tree = parent.path / "pb-passing0"
    for mode in (0o555, 0o000, 0o111, 0o444):
        nested = tree / "pytest" / f"m{mode:o}" / "deeper"
        nested.mkdir(parents=True)
        (nested / "file.txt").write_text("x")
        (nested.parent / "kept.txt").write_text("x")
        nested.chmod(mode)
        nested.parent.chmod(mode)
    owned_tree.release_owned_tree(parent.fd, "pb-passing0", lock, remove=True)
    assert list(parent.path.iterdir()) == []


def test_removal_never_follows_a_link_out_of_the_tree(parent, tmp_path):
    outside = tmp_path / "outside"
    (outside / "dir").mkdir(parents=True)
    (outside / "dir" / "precious.txt").write_text("keep")
    (outside / "file.txt").write_text("keep")
    outside.chmod(0o555)
    lock = parent.own("pb-symlinks")
    tree = parent.path / "pb-symlinks"
    (tree / "to-dir").symlink_to(outside / "dir")
    (tree / "to-file").symlink_to(outside / "file.txt")
    (tree / "to-parent").symlink_to(parent.path)
    owned_tree.release_owned_tree(parent.fd, "pb-symlinks", lock, remove=True)
    assert list(parent.path.iterdir()) == []
    assert (outside / "dir" / "precious.txt").read_text() == "keep"
    assert (outside / "file.txt").read_text() == "keep"
    assert stat.S_IMODE(outside.stat().st_mode) == 0o555


def test_removal_never_crosses_into_another_mount(parent, monkeypatch):
    lock = parent.own("pb-mounted0")
    tree = parent.path / "pb-mounted0"
    (tree / "mountpoint" / "inside").mkdir(parents=True)
    (tree / "mountpoint" / "inside" / "data").write_text("another filesystem")
    (tree / "ordinary").mkdir()
    (tree / "ordinary" / "data").write_text("x")
    mountpoint = os.stat(tree / "mountpoint").st_ino
    actual = owned_tree._mount_id_of

    def mount_of(fd):
        return actual(fd) + (1 if os.fstat(fd).st_ino == mountpoint else 0)

    monkeypatch.setattr(owned_tree, "_mount_id_of", mount_of)
    with pytest.raises(OSError, match="could not be removed"):
        owned_tree.release_owned_tree(parent.fd, "pb-mounted0", lock, remove=True)
    assert (tree / "mountpoint" / "inside" / "data").read_text() == "another filesystem"
    assert not (tree / "ordinary").exists()
    assert (tree / owned_tree.LOCK_NAME).is_file(), "a cut-short removal lost its mark"
    # The failed removal started the clock: a reaper retries it a window on.
    assert parent.reap(now=_later(WINDOW_S - 60)) == []
    monkeypatch.setattr(owned_tree, "_mount_id_of", actual)
    assert parent.reap(now=_later()) == ["pb-mounted0"]


def test_names_alone_never_select_a_tree(parent, tmp_path):
    """Agent folders matching the shape (``pb-cachetop``, ``pb-receipts``)
    and every near miss stay, however old: only PB's own lock selects."""

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / owned_tree.LOCK_NAME).write_bytes(owned_tree._owner_header())
    other = owned_tree._owner_header().replace(
        f"host={socket.gethostname()}".encode(), b"host=elsewhere")
    trees = {
        "pb-cachetop": None,                          # an agent's folder
        "pb-receipts": None,                          # another agent's folder
        "pb-foreign0": b"not a prismabuild lock\n",
        "pb-otherhst": other,                         # another host's lock
        "pb-emptylck": b"",
        "pb-version2": b"prismabuild owner lock v2 host=x\n",
    }
    for name, body in trees.items():
        (parent.path / name / "pytest").mkdir(parents=True)
        (parent.path / name / "pytest" / "data").write_text("keep")
        if body is not None:
            (parent.path / name / owned_tree.LOCK_NAME).write_bytes(body)
    (parent.path / "pb-linklock").mkdir()
    (parent.path / "pb-linklock" / owned_tree.LOCK_NAME).symlink_to(
        outside / owned_tree.LOCK_NAME)
    (parent.path / "pb-hardlink").mkdir()
    os.link(outside / owned_tree.LOCK_NAME,
            parent.path / "pb-hardlink" / owned_tree.LOCK_NAME)
    (parent.path / "pb-symlink0").symlink_to(outside)
    (parent.path / "pb-regular0").write_text("a file")
    (parent.path / "pb-toolong12").mkdir()
    (parent.path / "pb-toolong12" / owned_tree.LOCK_NAME).write_bytes(
        owned_tree._owner_header())
    for path in [*parent.path.rglob("*"), *outside.rglob("*")]:
        _age(path, 365 * 86400)
    before = sorted(str(path) for path in parent.path.rglob("*"))

    for _ in range(2):
        assert parent.reap(now=_later(365 * 86400)) == []

    assert sorted(str(path) for path in parent.path.rglob("*")) == before
    assert (outside / owned_tree.LOCK_NAME).read_bytes() == owned_tree._owner_header()
    assert (parent.path / "pb-otherhst" / owned_tree.LOCK_NAME).read_bytes() == other


def test_a_parent_held_open_since_before_its_trees_still_finds_them(parent):
    """btrfs lists, through one open directory, only what existed when it was
    opened or last rewound: a caller's old descriptor must not hide a tree."""
    os.listdir(parent.fd)                            # read and rewound while empty
    os.close(parent.own("pb-heldopen"))
    assert parent.reap() == []
    lock = parent.path / "pb-heldopen" / owned_tree.LOCK_NAME
    assert lock.read_bytes() == owned_tree._owner_header(), "the dead tree went unseen"


def test_one_call_inspects_and_removes_a_bounded_number(parent, monkeypatch):
    for index in range(7):
        os.close(parent.own(f"pb-dead000{index}"))
    monkeypatch.setattr(owned_tree, "REMOVE_PER_CALL", 3)
    assert parent.reap() == []                       # clocks start
    assert len(parent.reap(now=_later())) == 3
    assert len(parent.reap(now=_later())) == 3
    assert len(parent.reap(now=_later())) == 1
    monkeypatch.setattr(owned_tree, "INSPECT_PER_CALL", 2)
    for index in range(5):
        os.close(parent.own(f"pb-later00{index}"))
    parent.reap()
    started = [path for path in parent.path.iterdir()
               if not (path / owned_tree.LOCK_NAME).read_bytes().endswith(b"running\n")]
    assert len(started) == 2, "one call inspected more than its bound"


def test_a_lock_beside_its_tree_and_a_lock_whose_tree_is_gone(parent):
    tree = parent.path / "0123456789ab.abcdefgh"
    (tree / "checkout").mkdir(parents=True)
    (tree / "checkout" / "payload").write_text("x")
    pattern = re.compile(r"[0-9a-f]{12}\.[a-z0-9_]{8}")
    lock = owned_tree.take_owner_lock(parent.fd, tree.name + owned_tree.LOCK_SUFFIX)
    assert not (tree / owned_tree.LOCK_NAME).exists(), "the tree carries a lock"
    assert owned_tree.reap_dead_trees(parent.fd, pattern, beside=True,
                                      now=_later()) == []
    os.close(lock)
    assert owned_tree.reap_dead_trees(parent.fd, pattern, beside=True) == []
    assert owned_tree.reap_dead_trees(parent.fd, pattern, beside=True,
                                      now=_later()) == [tree.name]
    assert list(parent.path.iterdir()) == []

    # A tree its owner (or the SLURM epilog) already removed leaves only the
    # free lock, which goes at once.
    gone = owned_tree.take_owner_lock(parent.fd, "fedcba987654.zyxwvuts.lock")
    os.close(gone)
    assert owned_tree.reap_dead_trees(parent.fd, pattern, beside=True) == [
        "fedcba987654.zyxwvuts"]
    assert list(parent.path.iterdir()) == []


def test_drop_owner_lock_follows_its_tree(parent):
    for removed in (True, False):
        name = f"0123456789ab.{'removed0' if removed else 'remained'}"
        (parent.path / name).mkdir()
        lock = owned_tree.take_owner_lock(parent.fd, name + owned_tree.LOCK_SUFFIX)
        if removed:
            owned_tree.remove_tree(parent.fd, name)
        owned_tree.drop_owner_lock(parent.fd, name, lock)
        sibling = parent.path / (name + owned_tree.LOCK_SUFFIX)
        assert sibling.exists() is not removed
        if not removed:
            assert sibling.read_bytes() == owned_tree._owner_header(), (
                "a tree left behind has not started its clock")


def test_nested_attempts_go_and_take_their_emptied_key_directory(parent):
    key, live_key = "a" * 64, "b" * 64
    for owner, attempt in ((key, "1" * 32), (live_key, "2" * 32), (live_key, "3" * 32)):
        directory = parent.path / owner / attempt
        directory.mkdir(parents=True)
    holders = {}
    for owner, attempt in ((key, "1" * 32), (live_key, "2" * 32), (live_key, "3" * 32)):
        held = os.open(parent.path / owner / attempt, FLAGS)
        holders[attempt] = owned_tree.take_owner_lock(held, owned_tree.LOCK_NAME)
        os.close(held)
    os.close(holders.pop("1" * 32))
    os.close(holders.pop("2" * 32))
    inner = re.compile(r"[0-9a-f]{32}")
    outer = re.compile(r"[0-9a-f]{64}")
    owned_tree.reap_dead_trees(parent.fd, outer, inner=inner)
    gone = owned_tree.reap_dead_trees(parent.fd, outer, inner=inner, now=_later())
    try:
        assert sorted(gone) == [f"{key}/{'1' * 32}", f"{live_key}/{'2' * 32}"]
        assert not (parent.path / key).exists(), "an emptied key directory stayed"
        assert sorted(p.name for p in (parent.path / live_key).iterdir()) == ["3" * 32]
    finally:
        os.close(holders.pop("3" * 32))


def test_a_live_owner_refuses_a_second_taker(parent):
    lock = parent.own("pb-takeover")
    held = os.open(parent.path / "pb-takeover", FLAGS)
    try:
        with pytest.raises(BlockingIOError):
            owned_tree.take_owner_lock(held, owned_tree.LOCK_NAME)
    finally:
        os.close(held)
        os.close(lock)


def test_the_owner_lock_dies_with_a_killed_owner(parent):
    (parent.path / "pb-killedby").mkdir()
    holder = subprocess.Popen(
        [sys.executable, "-c",
         "import os, sys\n"
         f"sys.path.insert(0, {str(ROOT / 'src')!r})\n"
         "from prismabuild import owned_tree\n"
         f"held = os.open({str(parent.path / 'pb-killedby')!r}, os.O_RDONLY)\n"
         "owned_tree.take_owner_lock(held, owned_tree.LOCK_NAME)\n"
         "print('held', flush=True)\n"
         "sys.stdin.readline()\n"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    assert holder.stdout.readline().strip() == "held"
    assert parent.reap(now=_later()) == [] and holder.poll() is None
    holder.kill()
    holder.wait(timeout=30)
    assert parent.reap() == []                       # first seen dead
    assert parent.reap(now=_later()) == ["pb-killedby"]
