"""Portable CPU placement accounts for load outside its token ledger."""
import socket

import pytest

from prismabuild import pool


@pytest.fixture
def placement(tmp_path, monkeypatch):
    monkeypatch.setattr(pool, '_now', lambda: 1000.0)
    monkeypatch.setattr(pool.time, 'monotonic', lambda: 10.0)
    queue = pool.PoolQueue(tmp_path / 'queue')
    local = socket.gethostname()

    def announce(host, *, cpus, load, gpu=False):
        capacity = {'cpu': cpus, 'mem_gb': 8, **({'gpu': 1} if gpu else {})}
        tiers = {'preferred': list(range(cpus)), 'fallback': []}
        ledger = queue.ledger(host)
        ledger.configure_cpu_tiers(tiers)
        ledger.ensure_capacity(capacity)
        queue.announce(host=host, tags=[host], has_gpu=gpu, capacity=capacity,
                       observed_capacity=capacity, cpu_tiers=tiers,
                       observed_detail={'observed_unix': 1000.0, 'load1': load,
                                        'gpu_power_fraction': 0.0,
                                        'gpu_power_sampled_unix': 1000.0})
        return capacity, tiers

    announce(local, cpus=80, load=88.0)
    announce('spare', cpus=20, load=0.0, gpu=True)
    key = 'a' * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 4, 'mem_gb': 1})

    def claim(host, *, cpus, gpu=False):
        monkeypatch.setattr(pool.socket, 'gethostname', lambda: host)
        return queue.claim(tags=[host], has_gpu=gpu,
                           capacity={'cpu': cpus, 'mem_gb': 8, **({'gpu': 1} if gpu else {})},
                           cpu_tiers={'preferred': list(range(cpus)), 'fallback': []})

    return queue, announce, claim, local, key


def test_portable_cpu_row_uses_spare_host_when_external_load_fills_cpu_host(placement):
    queue, announce, claim, local, key = placement
    assert claim(local, cpus=80) is None
    assert not queue.ledger(local).held_keys()
    claimed = claim('spare', cpus=20, gpu=True)
    assert claimed is not None
    assert claimed['action_key'] == key
