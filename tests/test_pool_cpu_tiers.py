"""Real affinity inherited by launchers and concurrent token-backed CPU tiers."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool


def publish(queue, tmp_path, index, cpus=1, tags=()):
    worker = tmp_path / 'affinity.py'
    worker.write_text(
        'import os, subprocess, sys\n'
        'child = subprocess.check_output([sys.executable, \"-c\", '
        '\"import json,os; print(json.dumps(sorted(os.sched_getaffinity(0))))\"])\n'
        'print(child.decode().strip())\n')
    key = f'{index:064x}'
    queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script=str(worker),
                  resources={'cpu': cpus}, tags=tags)
    return key


def test_claims_use_preferred_then_fallback_and_release_reuses_preferred(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    cpus = sorted(os.sched_getaffinity(0))[:2]
    if len(cpus) < 2:
        pytest.skip("real affinity overflow proof requires two admitted CPUs")
    tiers = {'preferred': [cpus[1]], 'fallback': [cpus[0]]}
    items = []
    for i in range(2):
        publish(queue, tmp_path, i)
        item = queue.claim(capacity={'cpu': 2}, cpu_tiers=tiers)
        items.append(item)
        expected = [cpus[1], cpus[0]][i]
        result = queue.execute(item, python=sys.executable)
        assert result['status'] == 'executed', result
        assert json.loads(result['stdout']) == [expected]
        assert result['cpu_allocation'] == item['cpu_allocation']
    publish(queue, tmp_path, 2)
    assert queue.claim(capacity={'cpu': 2}, cpu_tiers=tiers) is None
    queue.finish(items[0]['action_key'], status='executed', detail={})
    reused = queue.claim(capacity={'cpu': 2}, cpu_tiers=tiers)
    assert reused['cpu_allocation'] == {'preferred': [cpus[1]], 'fallback': []}
    assert os.sched_getaffinity(0) >= set(cpus), 'executor narrowed parent affinity'


def test_concurrent_claimants_have_disjoint_cpu_reservations(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [9, 3], 'fallback': [8, 2]}
    queue.ledger().configure_cpu_tiers(tiers)
    queue.ledger().ensure_capacity({'cpu': 4})
    for i in range(8):
        publish(queue, tmp_path, i)
    barrier = threading.Barrier(8)
    def claim(_):
        barrier.wait()
        return queue.claim(capacity={'cpu': 4}, cpu_tiers=tiers)
    with ThreadPoolExecutor(max_workers=8) as workers:
        items = [x for x in workers.map(claim, range(8)) if x]
    assert len(items) == 4
    allocated = [cpu for x in items for values in x['cpu_allocation'].values()
                 for cpu in values]
    assert sorted(allocated) == [2, 3, 8, 9]
    assert queue.ledger().available().get('cpu', 0) == 0


def test_conflicting_worker_affinity_cannot_reinterpret_held_tokens(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': [1]}
    publish(queue, tmp_path, 0)
    item = queue.claim(capacity={'cpu': 2}, cpu_tiers=tiers)
    with pytest.raises(pool.PoolContractError, match='CPU tier map differs'):
        queue.claim(capacity={'cpu': 2}, cpu_tiers={'preferred': [1], 'fallback': [0]})
    assert queue.ledger().cpu_allocation(item['action_key'], tiers) == item['cpu_allocation']


def test_executor_refuses_allocation_outside_current_affinity(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    cpu = min(os.sched_getaffinity(0))
    publish(queue, tmp_path, 0)
    item = queue.claim(capacity={'cpu': 1}, cpu_tiers={'preferred': [cpu], 'fallback': []})
    monkeypatch.setattr(os, 'sched_getaffinity', lambda _: {cpu + 1})
    with pytest.raises(pool.PoolContractError, match='exceeds current affinity'):
        queue.execute(item)


def remote_offer(queue, *, tags=(), cpus=2):
    tiers = {'preferred': list(range(cpus)), 'fallback': []}
    remote = queue.ledger('another-host')
    remote.configure_cpu_tiers(tiers)
    remote.ensure_capacity({'cpu': cpus})
    queue.announce(host='another-host', tags=tags, has_gpu=False,
                   capacity={'cpu': cpus}, cpu_tiers=tiers)


def test_remote_preferred_deferral_is_compatible_whole_demand_and_bounded(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    remote_offer(queue, tags=['portable'])
    publish(queue, tmp_path, 0, cpus=2, tags=['portable'])
    tiers = {'preferred': [4], 'fallback': [5]}
    monkeypatch.setattr(pool.time, 'monotonic', lambda: 10.)
    assert queue.claim(tags=['portable'], capacity={'cpu': 2}, cpu_tiers=tiers) is None
    assert queue.ledger().available()['cpu'] == 2
    monkeypatch.setattr(pool.time, 'monotonic', lambda: 31.)
    assert queue.claim(tags=['portable'], capacity={'cpu': 2}, cpu_tiers=tiers)


@pytest.mark.parametrize('remote_tags,remote_cpus', [(['wrong-host'], 2), (['portable'], 1)])
def test_incompatible_or_too_small_remote_does_not_delay(tmp_path, remote_tags, remote_cpus):
    queue = pool.PoolQueue(tmp_path / 'queue')
    remote_offer(queue, tags=remote_tags, cpus=remote_cpus)
    publish(queue, tmp_path, 0, cpus=2, tags=['portable'])
    assert queue.claim(tags=['portable'], capacity={'cpu': 2},
                       cpu_tiers={'preferred': [4], 'fallback': [5]})


def test_finished_remote_generation_drops_local_deferral(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    remote_offer(queue)
    key = publish(queue, tmp_path, 0)
    tiers = {'preferred': [], 'fallback': [4]}
    assert queue.claim(capacity={'cpu': 1}, cpu_tiers=tiers) is None
    assert queue._cpu_deferrals
    queue.item_path(pool.READY, key).unlink()
    assert queue.claim(capacity={'cpu': 1}, cpu_tiers=tiers) is None
    assert queue._cpu_deferrals == {}


def test_legacy_cpu_reservation_prevents_map_activation(tmp_path):
    ledger = pool.PoolQueue(tmp_path / 'queue').ledger()
    ledger.ensure_capacity({'cpu': 1})
    assert ledger.acquire('a' * 64, {'cpu': 1})
    with pytest.raises(pool.PoolContractError, match='drain legacy CPU'):
        ledger.configure_cpu_tiers({'preferred': [0], 'fallback': []})
    assert ledger.held()['cpu'] == 1


def test_cpu_map_validation_only_never_initializes_legacy_ledger(tmp_path):
    ledger = pool.PoolQueue(tmp_path / 'queue').ledger()
    ledger.ensure_capacity({'cpu': 1})
    assert ledger.acquire('a' * 64, {'cpu': 1})
    tiers = {'preferred': [0], 'fallback': []}
    assert ledger.configure_cpu_tiers(tiers, initialize=False) is None
    assert not (ledger.base / 'cpu-map.json').exists()
    assert ledger.held()['cpu'] == 1
    with pytest.raises(pool.PoolContractError, match='drain legacy CPU'):
        ledger.configure_cpu_tiers(tiers)
