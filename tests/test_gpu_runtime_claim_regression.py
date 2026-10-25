"""A broker fence stops sibling GPU claims until verified recovery."""
import json
from pathlib import Path

from prismabuild import adaptive_cpu, adaptive_gpu, gpu_capacity, pool
from admitted_queue_fixture import AdmittedQueueFixture


def test_broker_fence_denies_gpu_claim_and_recovery_restores_it(tmp_path, monkeypatch):
    mirror = tmp_path / 'gpu-container-runtime.json'
    state = {'schema': 'prismabuild.gpu-container-runtime.v1', 'latched': True,
             'reason': 'gpu_container_runtime_unready', 'subtype': 'cdi_mount_missing'}
    mirror.write_text(json.dumps(state))
    monkeypatch.setattr(gpu_capacity, 'RUNTIME_GATE', mirror, raising=False)
    monkeypatch.setattr(gpu_capacity, 'runtime_fence_enabled', lambda: True, raising=False)
    trusted_sample = adaptive_gpu.trusted_sample
    monkeypatch.setattr(adaptive_gpu, 'trusted_sample', lambda path: (
        json.loads(mirror.read_text()) if Path(path) == mirror else trusted_sample(path)))
    queue = AdmittedQueueFixture(pool.PoolQueue(tmp_path / 'queue'),
        capacity={'cpu': 2, 'mem_gb': 4, 'gpu': 1}, default_demand={'cpu': 1, 'mem_gb': 1})
    gpu_key, cpu_key = 'a' * 64, 'b' * 64
    for key, resources, priority in [
        (gpu_key, {'cpu': 1, 'gpu': 1, 'mem_gb': 1}, 1),
        (cpu_key, {'cpu': 1, 'mem_gb': 1}, 0),
    ]:
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                      checkout_root=str(tmp_path), worker_script='/worker.py',
                      resources=resources, priority=priority)

    assert queue.claim(has_gpu=True)['action_key'] == cpu_key
    sibling = pool.PoolQueue(queue.root)
    assert sibling.claim(has_gpu=True, capacity=queue.capacity) is None
    local = adaptive_cpu.local_state_base(queue.ledger().base)
    denials = adaptive_cpu.read_json(local / pool.CLAIM_DENIALS)['records']
    denial = next(row for row in denials.values() if row['action_key'] == gpu_key)
    assert denial['reason'] == 'gpu_container_runtime_unready'
    assert denial['evidence']['subtype'] == 'cdi_mount_missing'

    mirror.write_text(json.dumps({**state, 'latched': False}))
    assert sibling.claim(has_gpu=True, capacity=queue.capacity)['action_key'] == gpu_key
