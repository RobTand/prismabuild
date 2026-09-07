"""A lease census cannot reclaim a key whose ownership is changing."""
from concurrent.futures import ThreadPoolExecutor
import socket

from prismabuild import pool


def test_lease_sweep_skips_an_active_key_and_recovers_an_independent_key(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.ensure_layout()
    keys = ['a' * 64, 'b' * 64]
    ledger = queue.ledger()
    ledger.ensure_capacity({'cpu': 2})
    for key in keys:
        assert ledger.acquire(key, {'cpu': 1})
        pool._write_json_atomic(queue.lease_path(key), {
            'action_key': key, 'host': socket.gethostname(),
            'heartbeat_unix': pool._now() - 10_000,
        })
    original = queue.lease_path(keys[0]).read_bytes()
    # The claimant owns the key while publishing its claim and first lease.
    # A concurrent sweep must defer this key without blocking other recovery.
    with queue._transition_locked(keys[0]):
        with ThreadPoolExecutor(max_workers=1) as executor:
            swept = executor.submit(queue.sweep_widowed_leases, timeout_s=60).result(timeout=5)
    assert swept == [keys[1]], 'the sweep crossed an active ownership transition'
    assert queue.lease_path(keys[0]).read_bytes() == original
    assert ledger.held() == {'cpu': 1}
    assert ledger.available() == {'cpu': 1}
    assert queue.sweep_widowed_leases(timeout_s=60) == [keys[0]]
    assert ledger.held() == {}
