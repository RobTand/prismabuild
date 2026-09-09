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
