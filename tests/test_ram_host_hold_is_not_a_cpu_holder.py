"""A RAM fill's host memory hold is not a CPU holder (#1260).

#1245 made a RAM-tier fill take host ``mem_gb`` tokens under a
``ram-host:<grant>`` holder in the storage host's ledger, so rows and tier
fills draw on one memory pool.  That holder runs no process and holds no CPU
token, but the CPU admission census read every held directory as an action.
A holder with no declared CPU and non-empty content refuses
``holder_reservation_unknown``, so after the 4c2bcd95 publish every row on
dl380g10 was refused for as long as the RAM tier held any bytes.

The memory side must still count the hold: a row that needs the memory the
fill took is refused, and admitted once the fill releases it.
"""
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import pool  # noqa: E402

TIERS = {'preferred': [0], 'fallback': [1]}
CAPACITY = {'cpu': 2, 'mem_gb': 8}
GRANT = 'g' * 64


def _idle_host(monkeypatch, *, measurement=False):
    from prismabuild import adaptive_cpu
    monkeypatch.setattr(adaptive_cpu, 'action_identity',
                        lambda item: ('shape', measurement))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'cpu_count': 2, 'interval_s': 1.,
        'busy_cpus': 0., 'psi_some': 0.})


def _queue_with_ram_fill(tmp_path, gib):
    queue = pool.PoolQueue(tmp_path / 'queue')
    ledger = queue.ledger()
    ledger.configure_cpu_tiers(TIERS)
    ledger.ensure_capacity(CAPACITY)
    # The real writer, on the host the claim will read.
    assert queue.hold_tier_host_memory(ledger.host, GRANT, gib) == ('taken', '')
    return queue


def _publish(queue, tmp_path, key, resources):
    queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources=resources)


@pytest.mark.parametrize('measurement', [False, True])
def test_a_ram_fill_hold_does_not_refuse_a_cpu_row(tmp_path, monkeypatch, measurement):
    """The refusal dl380g10 hit: a bounded row beside a RAM fill's hold."""
    from prismabuild import adaptive_cpu
    queue = _queue_with_ram_fill(tmp_path, 2)
    _idle_host(monkeypatch, measurement=measurement)
    controller = adaptive_cpu.Controller(queue.ledger(), TIERS)
    with controller.locked():
        decision = controller.decision({'action_key': 'a' * 64, 'cas_root': str(tmp_path)},
                                       {'cpu': 1, 'mem_gb': 1})
    assert controller.last_decision.get('reason') != 'holder_reservation_unknown'
    assert decision is not None, controller.last_decision

    _publish(queue, tmp_path, 'b' * 64, {'cpu': 1, 'mem_gb': 1})
    claimed = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True)
    assert claimed and claimed['action_key'] == 'b' * 64


def test_a_ram_fill_hold_does_not_block_an_unbounded_row(tmp_path, monkeypatch):
    """An undeclared-CPU row needs an empty host; a memory hold leaves it empty."""
    queue = _queue_with_ram_fill(tmp_path, 2)
    _idle_host(monkeypatch)
    _publish(queue, tmp_path, 'c' * 64, {'mem_gb': 1})
    assert queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True)


def test_the_memory_the_fill_holds_is_still_counted(tmp_path, monkeypatch):
    """Exempt from the CPU census, never from the memory pool."""
    queue = _queue_with_ram_fill(tmp_path, 7)
    _idle_host(monkeypatch)
    _publish(queue, tmp_path, 'd' * 64, {'cpu': 1, 'mem_gb': 2})
    assert queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True) is None
    assert queue.release_tier_host_memory(queue.ledger().host, GRANT) == 7
    claimed = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True)
    assert claimed and claimed['action_key'] == 'd' * 64
