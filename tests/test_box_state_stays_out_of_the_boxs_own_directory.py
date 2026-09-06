"""The suite must not file lock files into the directory the fleet's loops use.

``box_state`` keys a box's host-local admission state on the queue root the
loops share, and creates a ``<digest>.lock`` per identity that is never
unlinked -- correctly, since unlinking one a live loop may be holding would
lapse the mutual exclusion it exists for.

Every test builds its own queue under a fresh ``tmp_path``, so every test was a
new identity, and every identity left a permanent file behind in the *fleet's*
directory.  Measured 2026-09-06: dl380g10 held 3780 of them against sparky's 2,
874 of those minted in a single 20-minute run.  Nothing read them and nothing
broke; a box's own admission state simply lived in a directory the suite was
filling.

These tests pin the property both ways round: the guard moves the directory,
and the identity it is keyed on is unchanged by the move.
"""
from __future__ import annotations

import fcntl
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import adaptive_cpu

#: The directory a real loop on this box uses.  Named here from the same two
#: parts the module names it from, rather than read back from the module, so
#: that redirecting the module cannot redirect the thing being asserted about.
FLEETS_OWN = Path('/tmp') / f'prismabuild-admission-{os.getuid()}'


def test_a_fresh_queue_root_files_nothing_in_the_fleets_directory(tmp_path):
    """The defect itself: one test, one permanent file, on the real box.

    Asserted on the exact name this identity would have taken rather than on
    the directory's contents, because the fleet's own loops are running on this
    box while the suite runs and may legitimately write there.
    """

    directory, digest = adaptive_cpu.box_state(tmp_path / 'reservations' / 'h')

    assert not (FLEETS_OWN / f'{digest}.lock').exists(), (
        'this test just minted a lock file in the directory the box uses')
    assert directory != FLEETS_OWN, 'box state was taken from the live box'
    assert directory == Path(adaptive_cpu.BOX_STATE_ROOT)


def test_the_lock_is_really_created_where_the_guard_points(tmp_path):
    """Redirected, not disabled: the mechanism still has to work."""

    directory, digest = adaptive_cpu.box_state(tmp_path / 'reservations' / 'h')
    descriptor = os.open(directory / f'{digest}.lock',
                         os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(descriptor)

    assert (directory / f'{digest}.lock').is_file()


def test_the_directory_is_still_private(tmp_path, monkeypatch):
    """The mode check is the reason this is not just a `mkdir`.

    The repoint is done here as well as by the autouse guard, and that is not
    belt-and-braces for its own sake.  This test makes a directory unsafe on
    purpose, and the directory it makes unsafe is whatever ``box_state``
    returns -- so if the guard ever failed to apply, this would `chmod 0770`
    the *fleet's* admission directory on the box the shard ran on.  That is
    not hypothetical: `box_state` refuses that mode, the refusal reaches the
    worker loop as an ordinary item error, and the box then announces a fresh
    offer at full capacity forever while claiming nothing.  A test must not be
    one fixture away from taking a box out of service.
    """

    root = tmp_path / 'box-state'
    monkeypatch.setattr(adaptive_cpu, 'BOX_STATE_ROOT', root)

    directory, _ = adaptive_cpu.box_state(tmp_path / 'reservations' / 'h')
    assert directory == root, 'the repoint did not take; refusing to chmod'
    assert directory.stat().st_mode & 0o077 == 0

    directory.chmod(0o770)
    with pytest.raises(RuntimeError, match='unsafe'):
        adaptive_cpu.box_state(tmp_path / 'reservations' / 'h')


def test_the_identity_is_unchanged_by_the_move(tmp_path):
    """Two roots still hash apart, and one root still hashes the same.

    The identity is what makes two boxes serving one pool never meet here and
    two generations on one box always meet.  Moving the directory must not
    have touched it.
    """

    _, first = adaptive_cpu.box_state(tmp_path / 'one')
    _, again = adaptive_cpu.box_state(tmp_path / 'one')
    _, other = adaptive_cpu.box_state(tmp_path / 'two')

    assert first == again, 'one queue root stopped hashing to one identity'
    assert first != other, 'two queue roots collided'


def test_the_probe_reads_the_directory_the_lock_writes(tmp_path):
    """``mount_latency`` globs what ``adaptive_cpu`` creates.

    They are two spellings of one directory, so a guard that moved only one
    would leave the probe measuring somewhere nothing is written -- which
    reads as a healthy, empty gate.
    """

    mount_latency = sys.modules.get('mount_latency')
    if mount_latency is None:
        pytest.skip('mount_latency is not imported in this session')

    directory, _ = adaptive_cpu.box_state(tmp_path / 'reservations' / 'h')
    assert Path(mount_latency.ADMISSION_LOCK_DIR) == directory
