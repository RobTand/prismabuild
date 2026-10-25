"""Trusted residency bounds one shared physical memory pool."""
from __future__ import annotations

import pytest

from prismabuild import box_capacity as bc
from prismabuild import gpu_sample_facts as facts
from test_box_capacity import gpu_sample

GIB = 1 << 30
NOW = 100.0
CAPACITY = {"cpu": 8, "gpu": 1, "mem_gb": 104}


def unified_sample(*, foreign=(), domain="shared_system", sampled=NOW):
    value = gpu_sample(foreign=foreign, domain=domain)
    value.update(sample_id="unified-sample", sampled_unix=sampled,
                 host_total_bytes=128 * GIB, host_available_bytes=120 * GIB)
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
    (0, 120, 88),
    (80, 120, 88),
    (0, 80, 72),
    (80, 80, 88),
])
def test_host_and_gpu_readings_are_reconciled_without_double_charge(held, available, expected):
    seen = bc.observe(CAPACITY, {"mem_gb": held},
                      gpu_sample=unified_sample(foreign=[foreign(32 * GIB)]),
                      mem_gb=available, load1=0, now=NOW)
    assert seen.capacity["mem_gb"] == expected


def test_foreign_charge_rounds_up_without_changing_the_operating_margin():
    seen = bc.observe(CAPACITY, {"mem_gb": 80},
                      gpu_sample=unified_sample(foreign=[foreign(32 * GIB + 1)]),
                      mem_gb=120, load1=0, now=NOW)
    assert seen.capacity["mem_gb"] == 87
    assert bc.MEMORY_MARGIN_GB == 8


def test_latest_foreign_ceiling_bounds_delayed_observer_history():
    observer = bc.CapacityObserver(samples=3)
    for sampled, used in [(NOW - 2, 0), (NOW - 1, 16 * GIB), (NOW, 40 * GIB)]:
        offered = observer.offer(
            CAPACITY, {"mem_gb": 80},
            gpu_sample=unified_sample(foreign=[foreign(used)], sampled=sampled),
            mem_gb=120, load1=0, now=NOW)
    assert offered["mem_gb"] == 80
    assert observer.last.unified_memory.sampled_unix == NOW
    unchanged = observer.offer(
        CAPACITY, {"mem_gb": 80},
        gpu_sample=unified_sample(foreign=[foreign(40 * GIB)]),
        mem_gb=120, load1=0, now=NOW)
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
