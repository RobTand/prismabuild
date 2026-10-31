"""Review-only #1543 reproduction. Real queue; fault only the plan's stat."""
from pathlib import Path

import pytest

from prismabuild import pool, residency_plan
from test_gang_plan_confirmation import _waiting_plan
from test_gang_residency_members import _compose_map
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401


@pytest.mark.parametrize("stat_unavailable", [False, True], ids=["control", "stat-failure"])
def test_retired_resident_gang_does_not_claim(gang_fleet, monkeypatch, tmp_path,
                                            stat_unavailable):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, first, second, lead, refill, plan = _waiting_plan(
        gang_fleet, monkeypatch, tmp_path, "review-stat-failure")
    _compose_map(monkeypatch, queue, first, [lead])
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, first))
    assert queue.residency_verdict(row)["state"] == "resident"
    residency_plan.mark_superseded(queue, first, reason="review retired current plan")
    assert queue._terminal_plan_signature(first) is not None
    faulted = []
    if stat_unavailable:
        actual_incarnation = residency_plan.incarnation
        actual_stat = Path.stat
        plan_path = queue.records.residency_plan_path(first)

        def unavailable_stat(path, *args, **kwargs):
            if path == plan_path:
                faulted.append(str(path))
                raise OSError("review injected plan stat unavailable")
            return actual_stat(path, *args, **kwargs)

        def unavailable_incarnation(path):
            with monkeypatch.context() as fault:
                fault.setattr(Path, "stat", unavailable_stat)
                return actual_incarnation(path)

        monkeypatch.setattr(residency_plan, "incarnation", unavailable_incarnation)
    claimed = gclaim("sparklina")
    print({"stat_unavailable": stat_unavailable, "faults": len(faulted),
           "retired_member": first, "claimed": claimed,
           "held": queue.ledger("sparklina").held_keys()}, flush=True)
    if stat_unavailable:
        assert faulted, "the intended real Path.stat fault did not execute"
    assert claimed is None, "a resident gang with a retired plan claimed after an unavailable stat"
    assert first not in queue.ledger("sparklina").held_keys()
