"""A residency reservation is arithmetic over the manifest, and it gates the claim (#583).

Two properties, both cheap to check and both mutation-tested here.

**Derived, never typed.**  A movement node declares the byte range of its
consumer's read order it makes resident.  Its ``stage_gib`` demand on that tier
has to be at least the range's own ceiling in GiB, and the range comes out of
the manifest the consumer already sealed -- ``storage_tiers.manifest_phase_ranges``
reads the same running byte sum ``prewarm_loop.manifest_phases`` reads.  So the
number in the claim record traces back to the read set the action declared, not
to a habit (``pb_demand_must_be_measured_not_habitual``).

**Gating, not hoping.**  A compute node that names lead movers is refused until
each lead's ``done/`` record says ``executed``.  A ``cache_hit`` finished
without moving a byte and must not satisfy the gate: the residency descriptor is
deliberately deterministic so a consumer can bind it before the mover runs,
which is exactly what makes a cached mover look finished.

Nothing here touches the live queue, a real pool or a real device.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import adaptive_cpu, pool, storage_tiers  # noqa: E402

MOVER = "1" * 64
CONSUMER = "2" * 64
SECOND_MOVER = "3" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE = f"stage_gib@{TIER}"
GIB = storage_tiers.GIB


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, resources: dict[str, int], **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources=resources,
        **kw,
    )


def _denial(q: pool.PoolQueue, key: str) -> dict[str, object] | None:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    matching = [entry for entry in records.values()
                if isinstance(entry, dict) and entry.get("action_key") == key]
    if not matching:
        return None
    return max(matching, key=lambda entry: float(entry.get("denied_unix", 0.0)))


def _manifest_v1(phase_bytes: list[int]) -> dict[str, object]:
    """A v1 manifest of one entry per phase, with the running sum v1 declares."""

    entries = [{"path": f"/mnt/shared/part-{i}", "offset": 0, "bytes": size,
                "sha256": None} for i, size in enumerate(phase_bytes)]
    phases = []
    running = 0
    for index, size in enumerate(phase_bytes):
        running += size
        phases.append({"name": f"phase-{index}", "bytes": size,
                       "cumulative_bytes": running})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {"phases": phases},
        "mount_prefix": "/mnt/shared", "entries": entries,
        "entry_count": len(entries), "total_bytes": running,
    }


def _residency(*, range_start: int, range_end: int) -> dict[str, object]:
    return {
        "schema": pool.RESIDENCY_SCHEMA_V1,
        "tier_id": TIER,
        "manifest_sha256": "a" * 64,
        "manifest_bytes": 4096,
        "range_start_bytes": range_start,
        "range_end_bytes": range_end,
    }


# -- derived ---------------------------------------------------------------


def test_the_range_comes_out_of_the_manifests_own_read_order() -> None:
    manifest = _manifest_v1([3 * GIB, 7 * GIB, GIB // 2])
    ranges = storage_tiers.manifest_phase_ranges(manifest)
    assert ranges == [
        {"name": "phase-0", "start_bytes": 0, "end_bytes": 3 * GIB},
        {"name": "phase-1", "start_bytes": 3 * GIB, "end_bytes": 10 * GIB},
        {"name": "phase-2", "start_bytes": 10 * GIB, "end_bytes": 10 * GIB + GIB // 2},
    ]
    # The demand for each range is the range, in whole GiB, rounded up.
    assert storage_tiers.residency_demand(
        tier_id=TIER, range_start_bytes=ranges[0]["start_bytes"],
        range_end_bytes=ranges[0]["end_bytes"]) == {STAGE: 3}
    assert storage_tiers.residency_demand(
        tier_id=TIER, range_start_bytes=ranges[2]["start_bytes"],
        range_end_bytes=ranges[2]["end_bytes"]) == {STAGE: 1}
    # A pool-side fill ask rides along, named for the side it is measured on.
    assert storage_tiers.residency_demand(
        tier_id=TIER, range_start_bytes=0, range_end_bytes=GIB,
        fill_mb_s_pool_side=242) == {
            STAGE: 1, f"fill_mb_s_pool_side@{TIER}": 242}


def test_the_tier_id_decides_the_capacity_kind_for_demand_and_for_refusal(
    queue: pool.PoolQueue,
) -> None:
    """One rule, so a derived demand is never a demand ``publish`` refuses."""

    arc = "arc:dl380g10"
    derived = storage_tiers.residency_demand(
        tier_id=arc, range_start_bytes=0, range_end_bytes=2 * GIB)
    assert derived == {f"arc_gib@{arc}": 2}
    block = dict(_residency(range_start=0, range_end=2 * GIB), tier_id=arc)
    _publish(queue, MOVER, {"cpu": 1, **derived}, residency=block)
    with pytest.raises(pool.PoolContractError, match=f"arc_gib@{arc}"):
        _publish(queue, MOVER, {"cpu": 1, f"arc_gib@{arc}": 1}, residency=block)
    # A stated tier that contradicts the id is a mistake, not a preference.
    with pytest.raises(ValueError):
        storage_tiers.residency_demand(
            tier_id=arc, tier="stage", range_start_bytes=0, range_end_bytes=GIB)


def test_a_phase_table_that_does_not_describe_this_manifest_yields_no_ranges() -> None:
    """Windowing on the wrong boundaries would reserve for bytes nobody reads."""

    manifest = _manifest_v1([GIB, GIB])
    manifest["annotations"]["phases"][0]["cumulative_bytes"] = GIB // 2  # cuts an entry
    assert storage_tiers.manifest_phase_ranges(manifest) == []
    absent = _manifest_v1([GIB])
    absent["annotations"] = {}
    assert storage_tiers.manifest_phase_ranges(absent) == []


def test_publish_refuses_a_demand_below_the_range_it_declares(
    queue: pool.PoolQueue,
) -> None:
    ranges = storage_tiers.manifest_phase_ranges(_manifest_v1([3 * GIB, 7 * GIB]))
    lead = ranges[0]
    derived = storage_tiers.residency_demand(
        tier_id=TIER, range_start_bytes=lead["start_bytes"],
        range_end_bytes=lead["end_bytes"])
    block = _residency(range_start=lead["start_bytes"], range_end=lead["end_bytes"])

    with pytest.raises(pool.PoolContractError, match="below the 3 GiB"):
        _publish(queue, MOVER, {"cpu": 1, STAGE: derived[STAGE] - 1}, residency=block)
    with pytest.raises(pool.PoolContractError, match="below the 3 GiB"):
        _publish(queue, MOVER, {"cpu": 1}, residency=block)
    # The derived demand publishes, and so does more than it (a mover may
    # reserve headroom; it may never reserve less than it will write).
    _publish(queue, MOVER, {"cpu": 1, **derived}, residency=block)
    item = pool._read_json(queue.item_path(pool.READY, MOVER))
    assert item is not None and item["residency"]["range_end_bytes"] == 3 * GIB
    _publish(queue, MOVER, {"cpu": 1, STAGE: derived[STAGE] + 1}, residency=block)


def test_publish_refuses_a_malformed_residency_block(queue: pool.PoolQueue) -> None:
    good = _residency(range_start=0, range_end=GIB)
    for mutate in (
        lambda b: b.update(schema="prismabuild.residency.v0"),
        lambda b: b.update(leads=["short"]),
        lambda b: b.update(leads=[MOVER, MOVER]),
        lambda b: b.update(tier_id="stage_gib@x"),
        lambda b: b.update(range_end_bytes=0),
        lambda b: b.pop("range_end_bytes"),
        lambda b: b.update(unexpected=1),
    ):
        block = dict(good)
        mutate(block)
        with pytest.raises(pool.PoolContractError):
            _publish(queue, MOVER, {"cpu": 1, STAGE: 1}, residency=block)
    # A block that asks for nothing at all is a mistake, not a no-op.
    with pytest.raises(pool.PoolContractError):
        _publish(queue, MOVER, {"cpu": 1},
                 residency={"schema": pool.RESIDENCY_SCHEMA_V1})


# -- gating ----------------------------------------------------------------


def _publish_consumer(q: pool.PoolQueue, leads: list[str]) -> None:
    _publish(q, CONSUMER, {"cpu": 1}, residency={
        "schema": pool.RESIDENCY_SCHEMA_V1,
        "manifest_sha256": "a" * 64, "manifest_bytes": 4096, "leads": leads,
    })


def test_a_consumer_waits_until_its_lead_mover_has_moved_the_bytes(
    queue: pool.PoolQueue,
) -> None:
    _publish_consumer(queue, [MOVER])
    assert queue.claim(owner="worker", capacity={"cpu": 4}) is None
    denial = _denial(queue, CONSUMER)
    assert denial is not None and denial["reason"] == "residency_lead_not_resident"
    assert denial["evidence"]["residency"]["pending"] == [
        {"lead": MOVER, "status": "absent"}]
    # Refused before any token moved, and without aging the item: the box
    # goes and does other work rather than withholding capacity for bytes
    # that are not on their way.
    assert queue.ledger().held() == {}
    assert queue.passes(CONSUMER) == 0

    # The mover runs and finishes; now the consumer is admitted.
    _publish(queue, MOVER, {"cpu": 1})
    mover_claim = queue.claim(owner="mover", capacity={"cpu": 4})
    assert mover_claim is not None and mover_claim["action_key"] == MOVER
    queue.finish(MOVER, status="executed", claim_snapshot=mover_claim)
    claimed = queue.claim(owner="worker", capacity={"cpu": 4})
    assert claimed is not None and claimed["action_key"] == CONSUMER
    assert claimed["residency_verdict"] == {"state": "resident", "leads": [MOVER]}


def test_a_cache_hit_lead_moved_no_bytes_and_does_not_satisfy_the_gate(
    queue: pool.PoolQueue,
) -> None:
    """The descriptor is deterministic on purpose, so a cached mover looks done."""

    _publish(queue, MOVER, {"cpu": 1})
    mover_claim = queue.claim(owner="mover", capacity={"cpu": 4})
    assert mover_claim is not None
    queue.finish(MOVER, status="cache_hit", claim_snapshot=mover_claim)
    done = pool._read_json(queue.item_path(pool.DONE, MOVER))
    assert done is not None and done["status"] == "cache_hit"

    _publish_consumer(queue, [MOVER])
    assert queue.claim(owner="worker", capacity={"cpu": 4}) is None
    denial = _denial(queue, CONSUMER)
    assert denial is not None and denial["reason"] == "residency_lead_not_resident"
    assert denial["evidence"]["residency"]["pending"] == [
        {"lead": MOVER, "status": "cache_hit"}]


def test_every_lead_must_be_resident_not_merely_the_first(
    queue: pool.PoolQueue,
) -> None:
    for key in (MOVER, SECOND_MOVER):
        # One attempt each: a mover that fails must stay failed, or the
        # requeued row is what the next claim returns and the gate is not
        # what this test observed.
        _publish(queue, key, {"cpu": 1}, max_attempts=1, retry_safe=False)
        claim = queue.claim(owner="mover", capacity={"cpu": 4})
        assert claim is not None and claim["action_key"] == key
        if key == MOVER:
            queue.finish(key, status="executed", claim_snapshot=claim)
        else:
            queue.finish(key, status="failed", claim_snapshot=claim)
    _publish_consumer(queue, [MOVER, SECOND_MOVER])
    assert queue.claim(owner="worker", capacity={"cpu": 4}) is None
    denial = _denial(queue, CONSUMER)
    assert denial is not None
    # ...and the denial says the lead ended, not that it has yet to start:
    # a consumer waiting on a mover that will never run is a different
    # situation from one waiting on a mover that is queued.
    assert denial["evidence"]["residency"]["pending"] == [
        {"lead": SECOND_MOVER, "status": "failed"}]


def test_mutating_the_gate_out_lets_an_unserved_consumer_claim(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bite test: with the verdict forced, the lead is never checked."""

    monkeypatch.setattr(
        pool.PoolQueue, "residency_verdict",
        lambda self, item: {"state": "not_requested"})
    _publish_consumer(queue, [MOVER])
    claimed = queue.claim(owner="worker", capacity={"cpu": 4})
    assert claimed is not None and claimed["action_key"] == CONSUMER
    assert "residency_verdict" not in claimed
