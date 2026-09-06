"""The pid named as holding admission must be holding *this* lock.

``_holder_of`` reads ``/proc/locks`` to answer "who is in there", which is the
one question a refusal has to answer for an operator.  It matched on the inode
number alone, and an inode number is only unique within a filesystem: any other
mount with a lock on the same inode number could be reported as the holder of
this one.

So the inode names the lock and the device breaks a tie -- in that order, which
#276 had the other way round.  Requiring both to match is blind on btrfs, where
a subvolume has its own anonymous device: measured on dl380g10 on 2026-09-06,
one lock under ``/home/rob/tmp`` was ``00:1f:14164446`` to ``fstat`` and
``00:1e:14164446`` in ``/proc/locks``.  Same inode, minor off by one, no holder
reported for a lock the process was itself holding.  That box is where the test
suite's ``tmp_path`` lives, so it was the suite that found it.

The device is parsed rather than formatted for a related reason.  The kernel
prints ``%02x:%02x``, so a key built by formatting must reproduce that padding
exactly, and #264 did not: it matched nothing on dl380g10, whose ``/tmp`` is
tmpfs with major 0, while looking correct on sparky, whose major 259 prints the
same either way.  Both bugs are false negatives that read as good news, so
these tests pin the properties (the right lock is found, a look-alike is not)
rather than any particular spelling of a key.

The synthetic ``/proc/locks`` lines below are the kernel's real format, taken
from a capture on dl380g10 during the 2026-09-06 admission incident:

    1: FLOCK  ADVISORY  WRITE 1036396 00:33:6884 0 EOF
"""
from __future__ import annotations

import fcntl
import io
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import adaptive_cpu


@pytest.fixture
def held(tmp_path):
    """A real flock this process holds, with its real device and inode."""

    descriptor = os.open(tmp_path / 'admission.lock',
                         os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


def _proc_locks(monkeypatch, text):
    """Answer ``/proc/locks`` with *text*, leaving every other open alone."""

    real_open = open

    def fake_open(path, *args, **kwargs):
        if str(path) == '/proc/locks':
            return io.StringIO(text)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(adaptive_cpu, 'open', fake_open, raising=False)


def test_the_real_holder_is_named(held):
    """Against the kernel's own ``/proc/locks``, not a fixture."""

    assert adaptive_cpu._holder_of(held) == os.getpid()


def test_a_lock_on_another_device_is_not_our_holder(held, monkeypatch):
    """Same inode number, different filesystem: not this lock.

    Reporting it would name a process that has nothing to do with admission,
    and an operator would go and look at it.

    Both rows are present, which is the reachable shape: this is only asked
    after an acquisition failed, so our own lock is held and the kernel is
    listing it.  The look-alike is the extra one, and the device is what tells
    them apart.
    """

    info = os.fstat(held)
    ours = (f'{os.major(info.st_dev):02x}:{os.minor(info.st_dev):02x}'
            f':{info.st_ino}')
    elsewhere = (f'{os.major(info.st_dev) + 1:02x}:'
                 f'{os.minor(info.st_dev):02x}:{info.st_ino}')
    _proc_locks(monkeypatch,
                f'1: FLOCK  ADVISORY  WRITE 4242 {elsewhere} 0 EOF\n'
                f'2: FLOCK  ADVISORY  WRITE 4243 {ours} 0 EOF\n')

    assert adaptive_cpu._holder_of(held) == 4243, (
        'a lock on another filesystem was named as holding this one')


def test_a_subvolumes_own_device_does_not_hide_the_holder(held, monkeypatch):
    """The btrfs shape, taken from the dl380g10 measurement.

    One row, our inode, a device that is not the one ``fstat`` reports.  There
    is nothing else it could be: the acquisition failed, so this lock is held,
    and the kernel listed exactly one lock on this inode.  #276 answered
    ``None`` here and the refusal could not name anyone.
    """

    info = os.fstat(held)
    superblock = (f'{os.major(info.st_dev):02x}:'
                  f'{max(os.minor(info.st_dev) - 1, 0):02x}:{info.st_ino}')
    _proc_locks(monkeypatch,
                f'1: FLOCK  ADVISORY  WRITE 4242 {superblock} 0 EOF\n')

    assert adaptive_cpu._holder_of(held) == 4242, (
        'the holder of a lock on a btrfs subvolume was not named')


def test_an_inode_two_mounts_disagree_about_names_nobody(held, monkeypatch):
    """Two candidates, neither carrying our device: we do not know which.

    Naming either would be a guess, and the operator would go and look at a
    process picked by file order.
    """

    info = os.fstat(held)
    minor = os.minor(info.st_dev)
    first = f'{os.major(info.st_dev):02x}:{minor + 1:02x}:{info.st_ino}'
    second = f'{os.major(info.st_dev) + 1:02x}:{minor:02x}:{info.st_ino}'
    _proc_locks(monkeypatch,
                f'1: FLOCK  ADVISORY  WRITE 4242 {first} 0 EOF\n'
                f'2: FLOCK  ADVISORY  WRITE 4243 {second} 0 EOF\n')

    assert adaptive_cpu._holder_of(held) is None, (
        'one of two look-alikes was named as the holder')


def test_the_device_is_matched_by_value_not_by_padding(held, monkeypatch):
    """The #264 regression, pinned as a property.

    The same device written without the kernel's zero padding must still be
    recognised.  A comparison that formats a key of its own passes this only if
    it happens to pick the same padding; reading the numbers back cannot fail.
    """

    info = os.fstat(held)
    unpadded = f'{os.major(info.st_dev):x}:{os.minor(info.st_dev):x}'
    _proc_locks(monkeypatch, f'1: FLOCK  ADVISORY  WRITE 4242 '
                             f'{unpadded}:{info.st_ino} 0 EOF\n')

    assert adaptive_cpu._holder_of(held) == 4242, (
        'the holder was missed because the device was spelled differently')


def test_a_waiter_is_never_named_as_the_holder(held, monkeypatch):
    """Both rows carry the same device and inode; only one is holding it."""

    info = os.fstat(held)
    where = (f'{os.major(info.st_dev):02x}:{os.minor(info.st_dev):02x}'
             f':{info.st_ino}')
    _proc_locks(monkeypatch,
                f'1: FLOCK  ADVISORY  WRITE 4242 {where} 0 EOF\n'
                f'1: -> FLOCK  ADVISORY  WRITE 9999 {where} 0 EOF\n')

    assert adaptive_cpu._holder_of(held) == 4242, (
        'the process queued behind the lock was reported as holding it')


@pytest.mark.parametrize('field', ['', 'garbage', '00:33', 'zz:33:6884',
                                   '00:33:6884:7'])
def test_an_unparsable_device_field_is_skipped_not_raised(held, monkeypatch,
                                                          field):
    """A diagnostic may not turn a refusal into a crash."""

    _proc_locks(monkeypatch, f'1: FLOCK  ADVISORY  WRITE 4242 {field} 0 EOF\n')

    assert adaptive_cpu._holder_of(held) is None
