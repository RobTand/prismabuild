"""An 80-candidate pass with a free fence scans the queue once (#1571).

The census reads the whole queue, not the candidate, but each candidate
took the reader fence and ran both bounded-child scans: the discovery
read outside host admission and the refresh read under it. A free fence
hides the cost in waits and shows it in scans. A GPU holder takes the
box's device, so every candidate of the pass reaches the census and is
refused after it; the pass then evaluates all of them. This test counts
the scans across that pass and requires one locked census, which is two
scans: discovery plus refresh.
"""
from __future__ import annotations

from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool

from test_census_tmpfs_state_1451 import (  # noqa: F401  (fixtures)
    tmpfs_mount, tmpfs_state)
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture

fleet = fleet_fixture

CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
TIERS = {"preferred": list(range(20)), "fallback": []}
CANDIDATES = 6


def test_a_free_fence_scans_once_per_pass_not_twice_per_candidate(
        fleet, tmpfs_state, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    holder = publish("reuse-holder", priority=10, cpu=2, gpu=0, mem_gb=112)
    assert claim() == holder
    keys = [publish(f"reuse-candidate-{index}", priority=-10,
                    cpu=2, gpu=0, mem_gb=16)
            for index in range(CANDIDATES)]
    records = [pool._read_json(queue.item_path(pool.READY, key)) for key in keys]

    scans: list[int] = []
    real_census = reservation.CensusReader._census

    def counted(self, directory):
        scans.append(1)
        return real_census(self, directory)

    monkeypatch.setattr(reservation.CensusReader, "_census", counted)

    assert queue.claim(
        capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
        has_gpu=True, tags=["gb10", "sparklina"], ready=records) is None

    assert len(scans) == 2, (
        "one locked census is two scans (discovery plus refresh); "
        f"a pass of {CANDIDATES} candidates must not rescan: {len(scans)} scans")
