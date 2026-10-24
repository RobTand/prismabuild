"""Historical GPU sharing slots do not crash CPU borrowing at queue admission."""
from prismabuild import adaptive_cpu
from test_adaptive_gpu import gpu_rig


def test_b4_five_consecutive_multislot_gpu_claims_borrow_without_loop_errors(gpu_rig):
    queue, clock, sample, capacity, _, tick, _ = gpu_rig
    tiers = {'preferred': [0, 1, 2, 3], 'fallback': [4, 5, 6, 7]}
    cpu_sample = lambda: {'sampled_unix': clock[0], 'busy_cpus': .1, 'psi_some': 0.,
                         'cpu_count': 8, 'interval_s': 1.}
    options = dict(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True, has_gpu=True,
                   cpu_sampler=cpu_sample, gpu_sampler=lambda: sample.copy())
    donor = 'a' * 64
    queue.publish(action_key=donor, cas_root='/cas', checkout_root='/co', worker_script='/w.py',
                  resources={'cpu': 4, 'mem_gb': 1})
    assert queue.claim(**options)['action_key'] == donor
    for _ in range(3):
        tick()
        controller = adaptive_cpu.Controller(queue.ledger(), tiers, sampler=cpu_sample)
        with controller.locked():
            controller.decision({'action_key': '9' * 64}, {'cpu': 1},
                                identity=('warm', False), owner=None)
    for attempt in range(5):
        tick()
        key = f'{attempt + 1:064x}'
        queue.publish(action_key=key, cas_root='/cas', checkout_root='/co', worker_script='/w.py',
                      resources={'cpu': 2, 'mem_gb': 1, 'gpu': 2}, needs_gpu=True)
        claimed = queue.claim(**options)
        assert claimed and claimed['action_key'] == key
        assert claimed['cpu_admission']['borrowed_cpus'] == [0, 1]
        assert queue.ledger().holder_tokens(key) == {'gpu': 1, 'mem_gb': 1}
        queue.finish(key, status='executed', detail={'returncode': 0}, claim_snapshot=claimed)
    assert set(queue.ledger().held_keys()) == {donor}
