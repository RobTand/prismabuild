"""Portable CPU placement uses whole-host load beyond its token ledger."""
import json
import socket

import pytest

from prismabuild import pool


def _placement(tmp_path, monkeypatch, *, tags=()):
    monkeypatch.setattr(pool, '_now', lambda: 1000.0)
    monkeypatch.setattr(pool.time, 'monotonic', lambda: 10.0)
    queue = pool.PoolQueue(tmp_path / 'queue')
    local = socket.gethostname()

    def announce(host, *, cpus, load, gpu=False, detail=None, observed=None):
        capacity = {'cpu': cpus, 'mem_gb': 8, **({'gpu': 1} if gpu else {})}
        tiers = {'preferred': list(range(cpus)), 'fallback': []}
        ledger = queue.ledger(host)
        ledger.configure_cpu_tiers(tiers)
        ledger.ensure_capacity(capacity)
        queue.announce(host=host, tags=[host, 'class'], has_gpu=gpu, capacity=capacity,
                       observed_capacity=capacity if observed is None else observed, cpu_tiers=tiers,
                       observed_detail={'observed_unix': 1000.0, 'load1': load,
                                        'gpu_power_fraction': 0.0,
                                        'gpu_power_sampled_unix': 1000.0, **(detail or {})})
        return capacity, tiers

    announce(local, cpus=80, load=88.0)
    announce('spare', cpus=20, load=0.0, gpu=True)
    key = 'a' * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 4, 'mem_gb': 1}, tags=tags)

    def claim(host, *, cpus, gpu=False):
        monkeypatch.setattr(pool.socket, 'gethostname', lambda: host)
        return queue.claim(tags=[host, 'class'], has_gpu=gpu,
                           capacity={'cpu': cpus, 'mem_gb': 8, **({'gpu': 1} if gpu else {})},
                           cpu_tiers={'preferred': list(range(cpus)), 'fallback': []})

    return queue, announce, claim, local, key


@pytest.fixture
def placement(tmp_path, monkeypatch):
    return _placement(tmp_path, monkeypatch)


def test_portable_cpu_row_uses_spare_host_when_external_load_fills_cpu_host(placement):
    queue, announce, claim, local, key = placement
    assert claim(local, cpus=80) is None
    assert not queue.ledger(local).held_keys()
    claimed = claim('spare', cpus=20, gpu=True)
    assert claimed is not None
    assert claimed['action_key'] == key


def test_cpu_load_preference_expires_at_twenty_seconds(placement, monkeypatch):
    queue, announce, claim, local, key = placement
    assert claim(local, cpus=80) is None
    monkeypatch.setattr(pool.time, 'monotonic', lambda: 29.999)
    assert claim(local, cpus=80) is None
    monkeypatch.setattr(pool.time, 'monotonic', lambda: 30.0)
    assert claim(local, cpus=80)['action_key'] == key


@pytest.mark.parametrize('scope', ['hostname', 'class'])
def test_explicit_placement_keeps_its_existing_claim(tmp_path, monkeypatch, scope):
    local = socket.gethostname()
    queue, announce, claim, local, key = _placement(
        tmp_path, monkeypatch, tags=[local if scope == 'hostname' else 'class'])
    assert claim(local, cpus=80)['action_key'] == key


@pytest.mark.parametrize('host', ['local', 'peer'])
@pytest.mark.parametrize('detail', [
    {'load1': None}, {'load1': True}, {'load1': -1},
    {'observed_unix': None}, {'observed_unix': 1001.0},
    {'observed_unix': 1000.0 - pool.box_capacity.GPU_SAMPLE_MAX_AGE_S - 1},
])
def test_unknown_load_supplies_no_preference(placement, host, detail):
    queue, announce, claim, local, key = placement
    peer = host == 'peer'
    announce('spare' if peer else local, cpus=20 if peer else 80,
             load=0.0 if peer else 88.0, gpu=peer, detail=detail)
    assert claim(local, cpus=80)['action_key'] == key


@pytest.mark.parametrize('local_load,peer_load,yields', [
    (76.0, 0.0, False), (76.1, 0.0, True),
    (88.0, 16.0, True), (88.0, 16.1, False), (88.0, 88.0, False),
])
def test_headroom_must_cover_the_whole_cpu_request(placement, local_load, peer_load, yields):
    queue, announce, claim, local, key = placement
    announce(local, cpus=80, load=local_load)
    announce('spare', cpus=20, load=peer_load, gpu=True)
    assert (claim(local, cpus=80) is None) is yields


@pytest.mark.parametrize('constraint', ['reserved_memory', 'unmeasured_memory', 'cpu_map'])
def test_peer_must_fit_the_complete_measured_request(placement, constraint):
    queue, announce, claim, local, key = placement
    if constraint == 'reserved_memory':
        assert queue.ledger('spare').acquire('b' * 64, {'mem_gb': 8})
    elif constraint == 'unmeasured_memory':
        announce('spare', cpus=20, load=0.0, gpu=True, observed={'cpu': 20, 'gpu': 1})
    else:
        pool.immutable_files.write_json_atomic(queue.ledger('spare').base / 'cpu-map.json',
                                               {'preferred': [99], 'fallback': []})
    assert claim(local, cpus=80)['action_key'] == key


def replay(root, monkeypatch):
    """Compare actual claims with a declared synthetic service model."""
    results = {}
    for enabled in (False, True):
        with monkeypatch.context() as patch:
            queue, announce, claim, local, key = _placement(root / str(enabled), patch)
            if not enabled:
                patch.setattr(queue, '_defer_for_cpu_load', lambda *args, **kwargs: None)
            for letter in 'bcde':
                queue.publish(action_key=letter * 64, cas_root=str(root / 'cas'),
                              checkout_root=str(root), worker_script='worker.py',
                              resources={'cpu': 4, 'mem_gb': 1})
            hosts = []
            for _ in range(5):
                row = claim(local, cpus=80)
                host = local
                if row is None:
                    row = claim('spare', cpus=20, gpu=True)
                    host = 'spare'
                assert row is not None
                hosts.append(host)
            waits = [0 if host == 'spare' else 20 for host in hosts]
            results['enabled' if enabled else 'disabled'] = {
                'placements': hosts, 'completed_units_at_tick_1': hosts.count('spare'),
                'mean_cpu_start_wait_ticks': sum(waits) / len(waits),
                'all_units_complete_tick': max(waits) + 1,
            }
    return {'model': 'Five independent four-CPU units each need one service tick. '
                     'Foreign load occupies every local CPU until tick 20; spare load stays zero. '
                     'Claims are real; service times are synthetic, not fleet measurements.',
            'results': results}


def test_replay_moves_units_away_from_the_saturated_host(tmp_path, monkeypatch):
    result = replay(tmp_path, monkeypatch)
    assert result['results']['disabled']['completed_units_at_tick_1'] == 0
    assert result['results']['enabled']['completed_units_at_tick_1'] == 5
    print('cpu-placement-replay: ' + json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as directory, pytest.MonkeyPatch.context() as patch:
        print(json.dumps(replay(Path(directory), patch), sort_keys=True))
