"""Independent critical QA: the canary owns each real reader to fd close."""
import errno
import os
from pathlib import Path

import pytest

from test_canary_leg3_staged_read import (
    CONSUMER, _build_fleet, _child_env, leg3, pool, reader_lease,
)
import stage_release


def _configure(monkeypatch, fleet):
    fleet.file_claim()
    fleet.publish(range(3))
    env = _child_env(fleet, fleet.write_manifest())
    for name in list(os.environ):
        if name.startswith('PRISMABUILD_'):
            monkeypatch.delenv(name)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    stage_release.register_stage_root(
        fleet.queue, stage_root=str(fleet.stage), tier_id='prismabuild-stage:testbox')


def test_each_descriptor_is_pinned_against_egress_until_it_is_closed(tmp_path, monkeypatch):
    fleet = _build_fleet(tmp_path)
    _configure(monkeypatch, fleet)
    original_open = reader_lease.open_pinned
    original_release = reader_lease.release
    opened = {}
    released = []

    def observing_open(queue, pin, ref_id, key, **kwargs):
        fd, serving = original_open(queue, pin, ref_id, key, **kwargs)
        index = fleet.keys.index(key)
        held = (queue.root / pool.RESIDENCY / reader_lease.LEASES_SUBDIR
                / CONSUMER / (pin['pin_id'] + '.lease.json'))
        assert held.is_file(), 'reader ref missing while its fd is live'
        result = stage_release.evict(
            queue, f'{index+1:064x}', consumer_action_key=CONSUMER,
            stage_root=str(fleet.stage))
        assert result['entries_deleted'] == 0, result
        assert result['entries_deferred'] == 1, result
        assert Path(fleet.fragments[index]['stage_path']).is_file()
        opened[pin['pin_id']] = fd
        return fd, serving

    def observing_release(queue, pin_id, ref_id, **kwargs):
        assert pin_id in opened, 'release without a successful pinned open'
        with pytest.raises(OSError) as error:
            os.fstat(opened[pin_id])
        assert error.value.errno == errno.EBADF, 'pin released before fd close'
        result = original_release(queue, pin_id, ref_id, **kwargs)
        assert result is True
        released.append(pin_id)
        return result

    monkeypatch.setattr(reader_lease, 'open_pinned', observing_open)
    monkeypatch.setattr(reader_lease, 'release', observing_release)
    assert leg3.run_action() == 0
    assert len(opened) == len(released) == 3
    assert not list((fleet.root / reader_lease.LEASES_SUBDIR).glob('*/*.lease.json'))


def test_a_failed_pinned_open_releases_its_acquired_ref(tmp_path, monkeypatch):
    fleet = _build_fleet(tmp_path)
    _configure(monkeypatch, fleet)
    calls = []

    def fail_open(queue, pin, ref_id, key, **kwargs):
        path = (queue.root / pool.RESIDENCY / reader_lease.LEASES_SUBDIR
                / CONSUMER / (pin['pin_id'] + '.lease.json'))
        assert path.is_file()
        calls.append(pin['pin_id'])
        raise reader_lease.ReaderLeaseError('injected descriptor identity refusal')

    monkeypatch.setattr(reader_lease, 'open_pinned', fail_open)
    assert leg3.run_action() == 1
    assert len(calls) == 1
    assert not list((fleet.root / reader_lease.LEASES_SUBDIR).glob('*/*.lease.json'))
