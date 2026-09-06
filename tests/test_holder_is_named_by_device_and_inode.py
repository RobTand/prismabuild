"""The pid named as holding admission must be holding *this* lock.

``_holder_of`` reads ``/proc/locks`` to answer "who is in there", which is the
one question a refusal has to answer for an operator.  It matched on the inode
number alone, and an inode number is only unique within a filesystem: any other
mount with a lock on the same inode number could be reported as the holder of
this one.

The device half is why this is parsed rather than formatted.  The kernel prints
``%02x:%02x``, so a key built by formatting must reproduce that padding
exactly, and #264 did not: it matched nothing on dl380g10, whose ``/tmp`` is
tmpfs with major 0, while looking correct on sparky, whose major 259 prints the
same either way.  That is a false negative that reads as good news, so these
tests pin the property (the right lock is found, a look-alike is not) rather
than any particular spelling of a key.

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
    """

    info = os.fstat(held)
    elsewhere = os.major(info.st_dev) + 1
    _proc_locks(monkeypatch, f'1: FLOCK  ADVISORY  WRITE 4242 '
                             f'{elsewhere:02x}:{os.minor(info.st_dev):02x}:'
                             f'{info.st_ino} 0 EOF\n')

    assert adaptive_cpu._holder_of(held) is None, (
        'a lock on another filesystem was named as holding this one')


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
