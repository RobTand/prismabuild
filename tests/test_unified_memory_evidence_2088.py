"""Trusted residency bounds one shared physical memory pool."""
from __future__ import annotations

import pytest

from prismabuild import box_capacity as bc
from prismabuild import gpu_sample_facts as facts
from test_box_capacity import gpu_sample

GIB = 1 << 30
NOW = 100.0
CAPACITY = {"cpu": 8, "gpu": 1, "mem_gb": 104}


def unified_sample(*, foreign=(), jobs=(), domain="shared_system", sampled=NOW,
                   available=None):
    value = gpu_sample(foreign=foreign, jobs=jobs, domain=domain)
    if available is None:
        known = sum(row["used_bytes"] for row in foreign
                    if type(row.get("used_bytes")) is int and row["used_bytes"] >= 0)
        available = min(120, 128 - known / GIB)
    value.update(sample_id="unified-sample", sampled_unix=sampled,
                 host_total_bytes=128 * GIB, host_available_bytes=int(available * GIB))
    return value


def foreign(used_bytes):
    return {"pid": 7, "gpu_uuid": "GPU-0", "used_bytes": used_bytes}


@pytest.mark.parametrize("rows,state,used", [
    ([], facts.ForeignProcesses.CLEAR, 0),
    ([foreign(0)], facts.ForeignProcesses.PRESENT, 0),
    ([foreign(32 * GIB)], facts.ForeignProcesses.PRESENT, 32 * GIB),
    ([foreign(None)], facts.ForeignProcesses.UNKNOWN, 0),
    ([foreign(32 * GIB), foreign(None)], facts.ForeignProcesses.UNKNOWN, 32 * GIB),
    ([foreign(-1)], facts.ForeignProcesses.UNKNOWN, 0),
    ([foreign(True)], facts.ForeignProcesses.UNKNOWN, 0),
    ([{"gpu_uuid": [], "used_bytes": None}], facts.ForeignProcesses.UNKNOWN, 0),
])
def test_classification_preserves_known_bytes_and_unknown_residency(rows, state, used):
    evidence = facts.unified_memory_evidence(
        unified_sample(foreign=rows), now=NOW, max_age_s=5)
    assert evidence is not None
    assert evidence.sample_id == "unified-sample"
    assert evidence.sampled_unix == NOW
    assert evidence.foreign_residency is state
    assert evidence.foreign_bytes == used
    assert evidence.unknown is (state is facts.ForeignProcesses.UNKNOWN)


@pytest.mark.parametrize("mutate", [
    lambda value: value.update(sampled_unix=NOW - 6),
    lambda value: value.update(sampled_unix=NOW + 1),
    lambda value: value.update(complete=False),
    lambda value: value.update(attributed=False),
    lambda value: value.update(sample_id=""),
    lambda value: value.update(foreign_processes=None),
])
def test_untrusted_or_stale_samples_supply_no_unified_credit(mutate):
    value = unified_sample(foreign=[foreign(32 * GIB)])
    mutate(value)
    assert facts.unified_memory_evidence(value, now=NOW, max_age_s=5) is None


@pytest.mark.parametrize("used", [None, 32 * GIB])
def test_discrete_foreign_residency_does_not_change_host_budget(used):
    value = unified_sample(foreign=[foreign(used)], domain="discrete")
    assert facts.unified_memory_evidence(value, now=NOW, max_age_s=5) is None
    seen = bc.observe(CAPACITY, {}, gpu_sample=value, mem_gb=80, load1=0, now=NOW)
    assert seen.capacity["mem_gb"] == 72


@pytest.mark.parametrize("held,available,expected", [
    (0, 96, 88),
    (80, 96, 88),
    (0, 80, 72),
    (80, 80, 72),
])
def test_host_and_gpu_readings_are_reconciled_without_double_charge(held, available, expected):
    seen = bc.observe(CAPACITY, {"mem_gb": held},
                      gpu_sample=unified_sample(foreign=[foreign(32 * GIB)],
                                               available=available),
                      mem_gb=available, load1=0, now=NOW)
    assert seen.capacity["mem_gb"] == expected


def test_foreign_charge_rounds_up_without_changing_the_operating_margin():
    seen = bc.observe(CAPACITY, {"mem_gb": 80},
                      gpu_sample=unified_sample(foreign=[foreign(32 * GIB + 1)]),
                      mem_gb=95, load1=0, now=NOW)
    assert seen.capacity["mem_gb"] == 87
    assert bc.MEMORY_MARGIN_GB == 8


def test_pb_host_and_cuda_residency_leave_foreign_bytes_counted_once():
    job = {"complete": True, "host_bytes": 4 * GIB, "budget_bytes": 80 * GIB,
           "memory_domain": "shared_system", "gpu_lower_bound_bytes": 60 * GIB,
           "gpu_reported_bytes": 120 * GIB}
    value = unified_sample(foreign=[foreign(32 * GIB)], jobs=[job], available=32)

    seen = bc.observe(CAPACITY, {"mem_gb": 80}, gpu_sample=value,
                      mem_gb=32, load1=0, now=NOW)

    assert seen.capacity["mem_gb"] == 88
    assert seen.unified_memory.resident_bytes == 64 * GIB
    assert seen.unified_memory.external_bytes == 32 * GIB


@pytest.mark.parametrize("update,expected", [
    ({"gpu_lower_bound_bytes": None}, 28),
    ({"gpu_lower_bound_bytes": -1}, 28),
    ({"gpu_lower_bound_bytes": True}, 28),
    ({"host_bytes": None}, 84),
    ({"complete": False}, 24),
    ({"memory_domain": "discrete"}, 28),
    ({"memory_domain": "unknown"}, 28),
])
def test_unproven_pb_residency_does_not_earn_memory_credit(update, expected):
    job = {"complete": True, "host_bytes": 4 * GIB, "budget_bytes": 80 * GIB,
           "memory_domain": "shared_system", "gpu_lower_bound_bytes": 60 * GIB}
    job.update(update)
    seen = bc.observe(CAPACITY, {"mem_gb": 80},
                      gpu_sample=unified_sample(jobs=[job], available=32),
                      mem_gb=32, load1=0, now=NOW)

    assert seen.capacity["mem_gb"] == expected


def test_latest_foreign_ceiling_bounds_delayed_observer_history():
    observer = bc.CapacityObserver(samples=3)
    for sampled, used in [(NOW - 2, 0), (NOW - 1, 16 * GIB), (NOW, 40 * GIB)]:
        offered = observer.offer(
            CAPACITY, {"mem_gb": 80},
            gpu_sample=unified_sample(foreign=[foreign(used)], sampled=sampled),
            mem_gb=128 - max(8, used // GIB), load1=0, now=NOW)
    assert offered["mem_gb"] == 80
    assert observer.last.unified_memory.sampled_unix == NOW
    unchanged = observer.offer(
        CAPACITY, {"mem_gb": 80},
        gpu_sample=unified_sample(foreign=[foreign(40 * GIB)]),
        mem_gb=88, load1=0, now=NOW)
    assert unchanged["mem_gb"] == 80


def test_unknown_ceiling_bypasses_history_and_clear_sample_recovers():
    observer = bc.CapacityObserver(samples=3)
    clear = unified_sample()
    observer.offer(CAPACITY, {}, gpu_sample=clear, mem_gb=120, load1=0, now=NOW)
    unknown = observer.offer(CAPACITY, {"mem_gb": 80},
                             gpu_sample=unified_sample(foreign=[foreign(None)]),
                             mem_gb=120, load1=0, now=NOW)
    assert unknown["mem_gb"] == 0
    assert observer.last.unified_memory.unknown
    recovered = observer.offer(CAPACITY, {"mem_gb": 80}, gpu_sample=clear,
                               mem_gb=120, load1=0, now=NOW)
    assert recovered["mem_gb"] == 104
    assert not observer.last.unified_memory.unknown


@pytest.mark.parametrize("budget", [
    {}, {"budget_bytes": None}, {"budget_bytes": -1}, {"budget_bytes": True},
    {"budget_bytes": float(80 * GIB)}, {"budget_bytes": str(80 * GIB)},
    {"budget_bytes": 0},
])
def test_unreserved_scope_residency_earns_no_credit(budget):
    job = {"complete": True, "host_bytes": 4 * GIB,
           "memory_domain": "shared_system", "gpu_lower_bound_bytes": 60 * GIB,
           **budget}
    seen = bc.observe(CAPACITY, {"mem_gb": 80},
                      gpu_sample=unified_sample(jobs=[job], available=32),
                      mem_gb=32, load1=0, now=NOW)

    assert seen.capacity["mem_gb"] == 24
    assert seen.unified_memory.resident_bytes == 0
