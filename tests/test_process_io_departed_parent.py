"""A reaped child's work must survive its parent departing in the same window.

Real process trees and the real ``/proc/<pid>/io`` reader: the premise under
test is a kernel one, that a reaped child's counters are folded into its
parent's, and no fake can show whether that actually happened.  Only scope
membership is supplied, because that part needs a cgroup this test has no
business creating.

The two directions are tested together on purpose.  ``read_process_io``
records a measured fleet regression in the other direction, a 64 MiB write
reported as 129 MiB, so a change that stops losing a departed child's bytes
must not start counting a live parent's absorbed child twice.
"""
import os
from pathlib import Path

import pytest

from prismabuild import resource_scope


PAYLOAD = 64 << 20


def _io(pid):
    return {line.split(': ')[0]: int(line.split(': ')[1])
            for line in Path(f'/proc/{pid}/io').read_text().splitlines()}


def _tree(tmp_path, *, parent_outlives_child):
    """Fork parent -> child.  The child writes, then both are steered by pipes.

    Returns ``(parent_pid, child_pid, step, stop)``.  ``step`` lets the child
    exit and has the parent reap it; ``stop`` ends the parent.  Every phase is
    acknowledged over a pipe, so no phase of this test waits on a sleep.
    """
    to_child, child_go = os.pipe()
    child_done_r, child_done_w = os.pipe()
    to_parent, parent_go = os.pipe()
    parent_ready_r, parent_ready_w = os.pipe()
    parent = os.fork()
    if parent == 0:                                    # the scope's root
        try:
            child = os.fork()
            if child == 0:                             # the departing worker
                os.close(child_done_r)
                with open(tmp_path / 'child.bin', 'wb') as handle:
                    handle.write(b'x' * PAYLOAD)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.write(child_done_w, b'w')           # written, still alive
                os.read(to_child, 1)                   # hold until released
                os._exit(0)
            os.close(child_done_w)
            os.read(child_done_r, 1)
            os.write(parent_ready_w, bytes([child & 0xFF, (child >> 8) & 0xFF,
                                            (child >> 16) & 0xFF, (child >> 24) & 0xFF]))
            os.read(to_parent, 1)                      # sample 1 has been taken
            os.waitpid(child, 0)                       # reap: absorb its bytes
            os.write(parent_ready_w, b'r')
            if parent_outlives_child:
                os.read(to_parent, 1)
            os._exit(0)
        except BaseException:
            os._exit(97)
    os.close(child_done_w)
    raw = os.read(parent_ready_r, 4)
    child = raw[0] | (raw[1] << 8) | (raw[2] << 16) | (raw[3] << 24)

    def step():
        os.write(child_go, b'x')                       # child exits
        os.write(parent_go, b'x')                      # parent reaps it
        assert os.read(parent_ready_r, 1) == b'r'

    def stop():
        if parent_outlives_child:
            os.write(parent_go, b'x')
        os.waitpid(parent, 0)

    return parent, child, step, stop


@pytest.fixture
def scope(monkeypatch, tmp_path):
    """A scope whose membership is exactly the pids the test names."""
    instance = resource_scope.ResourceScope('c' * 64, 'd' * 32, 1 << 40,
                                            tmp_path / 'telemetry.json')
    instance.cgroup_path = tmp_path / 'job.slice'
    monkeypatch.setattr(resource_scope, 'CGROUP_ROOT', tmp_path)
    monkeypatch.setattr(resource_scope, '_process_in_scope', lambda *a, **k: True)
    return instance


def test_reaped_child_survives_its_parent_departing_in_the_same_window(scope, monkeypatch, tmp_path):
    """Both depart between samples.  The child's bytes must not vanish."""
    parent, child, step, stop = _tree(tmp_path, parent_outlives_child=False)
    monkeypatch.setattr(resource_scope, 'scope_pids', lambda *a, **k: [parent, child])
    first = scope.sample_process_io()
    assert first['wchar'] >= PAYLOAD, 'the live child was never observed writing'
    # The parent has not yet reaped, so its own reading does not hold the bytes.
    assert _io(parent)['wchar'] < PAYLOAD

    step()
    stop()
    monkeypatch.setattr(resource_scope, 'scope_pids', lambda *a, **k: [])
    final = scope.sample_process_io()

    assert final['processes_live'] == 0
    assert final['wchar'] >= PAYLOAD, (
        'a departed child was discarded on the strength of a parent that had '
        f'also departed: {final["wchar"]} < {PAYLOAD}')
    assert final['wchar'] < 2 * PAYLOAD, 'the same write was counted twice'


def test_live_parent_still_absorbs_its_reaped_child_exactly_once(scope, monkeypatch, tmp_path):
    """The other direction: the recorded 64 MiB reported as 129 MiB."""
    parent, child, step, stop = _tree(tmp_path, parent_outlives_child=True)
    monkeypatch.setattr(resource_scope, 'scope_pids', lambda *a, **k: [parent, child])
    assert scope.sample_process_io()['wchar'] >= PAYLOAD

    step()                                   # child reaped, parent still alive
    assert _io(parent)['wchar'] >= PAYLOAD, 'kernel did not fold the child in'
    monkeypatch.setattr(resource_scope, 'scope_pids', lambda *a, **k: [parent])
    during = scope.sample_process_io()
    stop()

    assert during['processes_live'] == 1
    assert during['retired'] == {name: 0 for name in resource_scope.IO_COUNTERS}, (
        'a child whose parent is still counting it was retired as well')
    assert during['wchar'] < 2 * PAYLOAD, (
        f'the absorbed child was counted twice: {during["wchar"]}')


@pytest.fixture
def synthetic(monkeypatch, tmp_path):
    """Drive the accounting directly, for shapes a real tree cannot stage."""
    instance = resource_scope.ResourceScope('e' * 64, 'f' * 32, 1 << 40,
                                            tmp_path / 'telemetry.json')
    instance.cgroup_path = tmp_path / 'job.slice'
    monkeypatch.setattr(resource_scope, 'CGROUP_ROOT', tmp_path)
    monkeypatch.setattr(resource_scope, '_process_in_scope', lambda *a, **k: True)
    world = {}

    def counters(pid):
        entry = world.get(pid)
        if entry is None:
            return None
        start, ppid, value = entry
        return f'{pid}:{start}', ppid, {name: value for name in resource_scope.IO_COUNTERS}

    monkeypatch.setattr(resource_scope, 'read_process_io', counters)
    monkeypatch.setattr(resource_scope, 'scope_pids', lambda *a, **k: sorted(world))
    return instance, world


def test_two_independent_roots_each_keep_their_own_departed_children(synthetic):
    """Multiple roots: one tree departing must not retire the other's work."""
    scope, world = synthetic
    world.update({10: (1, 1, 100), 11: (1, 10, 700),        # root 10, child 11
                  20: (1, 1, 200), 21: (1, 20, 900)})       # root 20, child 21
    assert scope.sample_process_io()['wchar'] == 1900

    del world[11], world[10]                     # the first tree departs whole
    during = scope.sample_process_io()
    assert during['retired']['wchar'] == 800, 'a whole departed tree was lost'
    assert during['wchar'] == 1900, 'the surviving tree was recounted or dropped'

    del world[21], world[20]
    assert scope.sample_process_io()['wchar'] == 1900


def test_a_departed_child_is_kept_when_only_its_sibling_survives(synthetic):
    """The parent is gone; a live sibling must not stand in for it."""
    scope, world = synthetic
    world.update({30: (1, 1, 50), 31: (1, 30, 400), 32: (1, 30, 600)})
    assert scope.sample_process_io()['wchar'] == 1050

    del world[30], world[31]              # parent and one child depart together
    during = scope.sample_process_io()
    assert during['retired']['wchar'] == 450, (
        'the departed child was discarded although no live process absorbs it')
    assert during['wchar'] == 1050


def test_recycled_parent_pid_can_still_hide_a_departed_child(synthetic):
    """A known bound of a pid-keyed ppid, unchanged here and stated as one.

    ``live_before`` records a departed process's parent as a bare pid, so a
    recycled pid belonging to an unrelated live process is indistinguishable
    from the real parent and the child is skipped.  The old ``members`` test
    had the same bound.  Closing it means recording the parent's *identity*,
    pid and starttime together, which changes the persisted record shape; it
    is out of scope for a lost-counter fix and is stated rather than hidden.
    """
    scope, world = synthetic
    world.update({40: (1, 1, 100), 41: (1, 40, 800)})
    assert scope.sample_process_io()['wchar'] == 900

    del world[41]
    world[40] = (999, 1, 100)            # same pid, a new and unrelated process
    during = scope.sample_process_io()
    # The original pid 40 is retired correctly: identity separates it from its
    # recycled successor, and its own parent is outside the scope.  What is
    # lost is its child, whose recorded ppid of 40 now matches a live stranger.
    assert during['retired']['wchar'] == 100, 'the departed original was not retired'
    assert during['wchar'] == 200, (
        'behaviour changed: a recycled parent pid no longer hides the 800 the '
        'child earned, which is an improvement that needs its own reasoning '
        'recorded rather than arriving as a surprise')
