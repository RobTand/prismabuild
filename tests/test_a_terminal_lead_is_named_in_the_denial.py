"""A consumer whose leads can never become resident says so in its denial (#595).

The item stays ``ready`` by documented intent -- ``record_denial`` is
host-local and coalesced, and the branch skips ``record_pass`` -- but until
now nothing told a terminal lead (failed, withdrawn, dropped, unpinned, or
bound to another manifest) apart from a mover that simply has not finished.
When every pending lead ended somewhere no later poll repairs, the denial
reason is ``residency_lead_terminal``; admission is unchanged.  The verdict
itself keeps the per-lead detail, so the reason is the signal and the
evidence is the diagnosis.

Nothing here touches the live queue, a real pool or a real device.
"""

from __future__ import annotations

from admitted_queue_fixture import simulated_admission

from admitted_queue_fixture import simulated_capacity
from claim_denial_reader import latest_claim_denial
from fleet_world import ActionRows

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

MOVER = "1" * 64
SECOND_MOVER = "3" * 64
CONSUMER = "2" * 64


from fleet_world import FleetWorld


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return FleetWorld(tmp_path).queue


def _publish_consumer(q: pool.PoolQueue, leads: list[str]) -> None:
    ActionRows(q).publish(
        CONSUMER,
        cas_root=q.root / 'cas',
        checkout_root=q.root / 'co',
        worker_script=q.root / 'worker.py',
        resources={'cpu': 1},
        residency={
            'schema': pool.RESIDENCY_SCHEMA_V1,
            'manifest_sha256': 'a' * 64,
            'manifest_bytes': 4096,
            'leads': leads,
        },
    )


def _fail(q: pool.PoolQueue, key: str) -> None:
    ActionRows(q).publish(
        key,
        cas_root=q.root / 'cas',
        checkout_root=q.root / 'co',
        worker_script=q.root / 'worker.py',
        resources={'cpu': 1},
        max_attempts=1,
        retry_safe=False,
    )
    claim = q.claim(owner="mover", **simulated_admission(q, simulated_capacity({"cpu": 4})))
    assert claim is not None and claim["action_key"] == key
    q.finish(key, status="failed", claim_snapshot=claim)


def test_a_failed_lead_is_terminal_not_pending(queue: pool.PoolQueue) -> None:
    _fail(queue, MOVER)
    _publish_consumer(queue, [MOVER])
    assert queue.claim(owner="worker", **simulated_admission(queue, simulated_capacity({"cpu": 4}))) is None
    denial = latest_claim_denial(queue, CONSUMER)
    assert denial is not None and denial["reason"] == "residency_lead_terminal"
    assert denial["evidence"]["residency"]["pending"] == [
        {"lead": MOVER, "status": "failed"}]
    # The signal changed; the disposition did not: still ready, still unaged.
    assert queue.records.item_path(pool.record_store.READY, CONSUMER).exists()
    assert queue.passes(CONSUMER) == 0


def test_a_failed_lead_beside_a_queued_one_stays_pending(
    queue: pool.PoolQueue,
) -> None:
    """One terminal lead does not speak for a lead that may still arrive."""

    _fail(queue, MOVER)
    _publish_consumer(queue, [MOVER, SECOND_MOVER])
    assert queue.claim(owner="worker", **simulated_admission(queue, simulated_capacity({"cpu": 4}))) is None
    denial = latest_claim_denial(queue, CONSUMER)
    assert denial is not None
    assert denial["reason"] == "residency_lead_not_resident"
    assert denial["evidence"]["residency"]["pending"] == [
        {"lead": MOVER, "status": "failed"},
        {"lead": SECOND_MOVER, "status": "absent"},
    ]
