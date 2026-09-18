"""The rows a submission seals are the rows that keep the stage (#583, #602).

Every other test of the pin publishes a mover by calling ``queue.publish`` with
a residency block written out in the test.  That proves the pool keeps tokens
for a record that carries one; it cannot prove that the record ``pbrun`` seals
and the tiers loop publishes *has* one -- and it did not.  ``publication_row``
returns no ``residency`` key, only the consumer's row was stamped, so every
live mover's queue record was missing the block, ``residency_pin_holds``
returned False at its second check, and the tier tokens went back at ``finish``
while 34 GB sat on the stage.  Nothing but the ledger could see it: the mover
ends ``executed``, the files are there, and the consumer's gate then waits
forever for a lead that can never read as pinned.

So this file drives the real path end to end and asserts on the ledger:
``residency_stage_rows`` seals, ``residency_plan.freeze`` writes the plan,
``tier_loop.cycle`` publishes the window, a worker claims and finishes the
mover with a complete receipt, and the tokens stay held.  The CAS and the ZFS
discovery are stubbed -- they are the two things a test may not have -- and
nothing else is.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
GIB = storage_tiers.GIB
PHASE_BYTES = 2 * GIB


def _manifest(phases: int = 2) -> dict[str, object]:
    """A v1 manifest of one entry per phase, with the running sum v1 declares."""

    entries, table, running = [], [], 0
    for index in range(phases):
        entries.append({"path": f"/mnt/shared/part-{index}", "offset": 0,
                        "bytes": PHASE_BYTES, "sha256": None})
        running += PHASE_BYTES
        table.append({"name": f"phase-{index}", "bytes": PHASE_BYTES,
                      "cumulative_bytes": running})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {"phases": table},
        "mount_prefix": "/mnt/shared", "entries": entries,
        "entry_count": len(entries), "total_bytes": running,
    }


class _Cas:
    """Only the two things ``residency_stage_rows`` asks of a CAS."""

    def __init__(self, manifest_path: Path) -> None:
        self._manifest = manifest_path
        self.requested: list[str] = []

    def input_path(self, entry):
        return self._manifest

    def publish_action_request(self, action) -> None:
        self.requested.append(str(action["action_key"]))


def _template(digest: str, size: int) -> dict[str, object]:
    import pbrun

    return {
        "cas": None, "marker_root": Path("/home/rob/tmp/markers"),
        "checkout_identity": {"commit": "a" * 40},
        "log_name": "x.log", "stamp_name": "pbrun.stamp",
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "working_directory": "."},
        "inputs": [{"id": "pbrun.checkout-snapshot", "sha256": "b" * 64,
                    "bytes": 4096}],
        "code_closure": pbrun.build_stamp_closure("pbrun.stamp", "{}"),
        "params": {"command": ["true"], "cwd": "/home/rob", "demand": {"cpu": 1},
                   "placement": {"required_tags": []},
                   "checkout_snapshot": {
                       "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                       "commit": "a" * 40, "subdirectory": ".",
                       "input": {"id": pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID,
                                 "sha256": "b" * 64, "bytes": 4096}},
                   "retry_policy": {"max_attempts": 1},
                   "data_manifest": {"input": {"sha256": digest, "bytes": size}}},
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }


@pytest.fixture()
def fleet(tmp_path: Path):
    """A queue, an announced stage tier, and a submission sealed against them."""

    import pbrun

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_manifest()))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    cas = _Cas(manifest_path)

    # One cycle to announce the tier: the mountpoint, the interpreter and the
    # tool root all come off the box, and the submission reads them there.
    def discover(**_kwargs):
        return {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                       "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                       "mountpoint": str(tmp_path / "stage"),
                       # Exactly one phase fits, so the window's own bound is
                       # exercised rather than assumed.
                       "capacity_bytes": PHASE_BYTES}}

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    tier = pbrun.resolve_stage_tier(queue, None)

    args = types.SimpleNamespace(
        priority=-10, max_attempts=1, retry_safe=True,
        residency="stage", residency_tier=None, residency_mover_mem_gb=1,
        # A mover's own, since #603: the width it copies at and the attempts it
        # gets, neither of them the consumer's.
        residency_mover_readers=4, residency_mover_max_attempts=3)
    staged = pbrun.residency_stage_rows(
        _template(digest, manifest_path.stat().st_size),
        consumer_action_key=CONSUMER, tier=tier, args=args, queue=queue, cas=cas)
    residency_plan.freeze(queue, staged["plan"])
    queue.publish(action_key=CONSUMER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1}, tags=["dl380g10"],
                  residency=staged["residency"])
    return types.SimpleNamespace(queue=queue, staged=staged, discover=discover,
                                 stage=tmp_path / "stage")


def _cycle(fleet) -> list[dict[str, object]]:
    return tier_loop.cycle(fleet.queue, host="dl380g10", source_pool="storage_pool",
                           receipts=tier_loop.ReceiptCache(),
                           discover=fleet.discover)


def _lead(fleet) -> str:
    return str(fleet.staged["plan"]["phases"][0]["mover_row"]["action_key"])


def _stage_the_lead(fleet) -> dict[str, object]:
    """Publish the window, claim the lead mover, finish it with a full receipt."""

    queue, mover = fleet.queue, _lead(fleet)
    _cycle(fleet)
    claim = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claim is not None and claim["action_key"] == mover
    queue.record_move(mover, {
        "tier_id": TIER, "consumer_action_key": CONSUMER, "complete": True,
        "bytes_staged": PHASE_BYTES, "entries_staged": 1,
        "range_start_bytes": 0, "range_end_bytes": PHASE_BYTES,
        "stage_root": str(fleet.stage),
        "disk_pacing": {"mean_self_read_mb_s": 0.0}})
    queue.finish(mover, status="executed", claim_snapshot=claim)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 2}
    return claim


def test_a_mover_the_loop_published_keeps_its_tokens_when_it_finishes(fleet) -> None:
    """The defect, from the submitter's own rows to the ledger.

    Without the block on the mover's row this fails on the last two asserts:
    the mover ends ``executed`` with its range on the stage, and the tier reads
    its whole supply free.
    """

    queue, mover = fleet.queue, _lead(fleet)
    _cycle(fleet)

    # The loop published exactly the phase the tier's free tokens covered...
    assert queue.item_path(pool.READY, mover).exists()
    others = [k for k in residency_plan.mover_keys(fleet.staged["plan"]) if k != mover]
    assert others and not any(
        queue.item_path(pool.READY, k).exists() for k in others)
    # ...and the row it published carries the pin the pool reads.
    published = pool._read_json(queue.item_path(pool.READY, mover))
    block = published.get("residency")
    assert isinstance(block, dict), "the published mover row carries no residency block"
    assert block["tier_id"] == TIER
    assert (block["range_start_bytes"], block["range_end_bytes"]) == (0, PHASE_BYTES)

    claim = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claim is not None and claim["action_key"] == mover
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 2}

    queue.record_move(mover, {
        "tier_id": TIER, "consumer_action_key": CONSUMER, "complete": True,
        "bytes_staged": PHASE_BYTES, "entries_staged": 1,
        "range_start_bytes": 0, "range_end_bytes": PHASE_BYTES,
        "stage_root": str(fleet.stage),
        "disk_pacing": {"mean_self_read_mb_s": 0.0}})
    queue.finish(mover, status="executed", claim_snapshot=claim)

    # The invariant: the bytes are on the stage, so the tokens are still held.
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 2}
    assert queue.tier_ledger(TIER).available().get("stage_gib", 0) == 0
    assert queue.residency_pin_holds(
        pool._read_json(queue.item_path(pool.DONE, mover)), mover) is True


def test_the_consumer_reads_that_pin_as_a_resident_lead(fleet) -> None:
    """All the way to an admission, which is the only reason the pin exists."""

    queue, mover = fleet.queue, _lead(fleet)
    _cycle(fleet)
    claim = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claim is not None and claim["action_key"] == mover
    queue.record_move(mover, {
        "tier_id": TIER, "consumer_action_key": CONSUMER, "complete": True,
        "bytes_staged": PHASE_BYTES, "entries_staged": 1,
        "range_start_bytes": 0, "range_end_bytes": PHASE_BYTES,
        "stage_root": str(fleet.stage),
        "disk_pacing": {"mean_self_read_mb_s": 0.0}})
    queue.finish(mover, status="executed", claim_snapshot=claim)

    assert queue._lead_is_pinned(fleet.staged["residency"], mover) is True

    # A mover files its fragment; the next cycle composes the map, which is the
    # other half of the gate.
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(fleet.stage),
        "manifest_sha256": str(fleet.staged["residency"]["manifest_sha256"]),
        "entries": {residency_map.residency_map_key("/mnt/shared/part-0", 0): {
            "stage_path": str(fleet.stage / "part-0"), "bytes": PHASE_BYTES,
            "offset": 0, "sha256": "a" * 64}}})
    _cycle(fleet)

    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == CONSUMER
    assert claimed["residency_verdict"]["state"] == "resident"


def test_the_frozen_plan_refuses_a_mover_row_that_carries_no_pin(fleet) -> None:
    """The bite, on the driver: strip the block and the plan will not freeze.

    A plan is the last place this is cheap to catch -- after it, the row is in
    the queue and the only witness is a ledger reading free while the stage is
    full.
    """

    plan = json.loads(json.dumps(fleet.staged["plan"]))
    del plan["phases"][0]["mover_row"]["residency"]

    with pytest.raises(residency_plan.ResidencyPlanError, match="no residency block"):
        residency_plan.validate_plan(plan)

    plan = json.loads(json.dumps(fleet.staged["plan"]))
    plan["phases"][0]["mover_row"]["residency"]["range_end_bytes"] = PHASE_BYTES // 2
    with pytest.raises(residency_plan.ResidencyPlanError, match="its mover row pins"):
        residency_plan.validate_plan(plan)


# -- the cleanup paths that run *after* a pin was kept ----------------------


def test_a_stale_claim_copy_does_not_hand_back_a_completed_pin(fleet) -> None:
    """``reap_stale`` concludes what ``finish`` already concluded, correctly.

    A stale directory view of ``claimed/`` is this fleet's documented reality,
    and the reaper's terminal-claim branch is written for exactly it: the
    worker filed ``done/``, a leftover copy of the claim is still visible, and
    the copy is cleanup rather than a retry.  What it must not do is return the
    tier tokens that ``finish`` deliberately kept -- the bytes are on the stage,
    and every path that reclaims them (an egress, the orphan sweep) walks the
    tier's *held* keys, so a release here leaves occupancy nothing can ever
    charge to anyone: the ledger reads it free and the next mover is admitted
    against capacity already spent.
    """

    queue, mover = fleet.queue, _lead(fleet)
    claim = _stage_the_lead(fleet)

    # The exact shape the reaper sees: the concluded claim record back in
    # ``claimed/``, its lease beside it, both aged past the lease timeout.
    stale = dict(claim)
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, mover), stale)
    pool._write_json_atomic(queue.lease_path(mover), {
        "owner": stale.get("claimed_by"), "action_key": mover,
        "attempt": stale.get("attempt", 1),
        "heartbeat_unix": pool._now() - 10 * pool.LEASE_TIMEOUT_S})

    reaped = queue.reap_stale(timeout_s=1.0)

    # It did conclude the stale copy...
    assert not queue.item_path(pool.CLAIMED, mover).exists()
    assert mover not in reaped, "a filed ending is cleanup, not work to retry"
    # ...and the pin survived it.
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 2}
    assert queue.tier_ledger(TIER).available().get("stage_gib", 0) == 0
    assert queue.residency_pin_holds(
        pool._read_json(queue.item_path(pool.DONE, mover)), mover) is True
    assert queue._lead_is_pinned(fleet.staged["residency"], mover) is True


def test_a_widowed_lease_does_not_hand_back_a_completed_pin(fleet) -> None:
    """The same question where there is no claim record left to judge.

    ``sweep_widowed_leases`` says "any tokens still held under the key go
    back", which is right for a host ledger and wrong for a tier: a mover's
    tier tokens are *meant* to outlive its claim, from ``finish`` until an
    egress deletes the bytes.
    """

    queue, mover = fleet.queue, _lead(fleet)
    claim = _stage_the_lead(fleet)
    pool._write_json_atomic(queue.lease_path(mover), {
        "owner": claim.get("claimed_by"), "action_key": mover,
        "heartbeat_unix": pool._now() - 10 * pool.LEASE_TIMEOUT_S})

    queue.sweep_widowed_leases(timeout_s=1.0)

    assert not queue.lease_path(mover).exists()
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 2}


def test_an_unpinned_mover_still_gets_its_tokens_back(fleet) -> None:
    """The other direction, so the fix is a judgement and not a blanket keep.

    A mover that moved nothing ends ``executed`` exactly like one that staged
    its range, and its host *and* tier tokens must both come back.
    """

    queue, mover = fleet.queue, _lead(fleet)
    _cycle(fleet)
    claim = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claim is not None and claim["action_key"] == mover
    queue.record_move(mover, {
        "tier_id": TIER, "consumer_action_key": CONSUMER, "complete": False,
        "bytes_staged": 0, "entries_staged": 0,
        "range_start_bytes": 0, "range_end_bytes": PHASE_BYTES,
        "stage_root": str(fleet.stage),
        "disk_pacing": {"mean_self_read_mb_s": 0.0}})
    queue.finish(mover, status="executed", claim_snapshot=claim)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {}

    pool._write_json_atomic(queue.item_path(pool.CLAIMED, mover), dict(claim))
    pool._write_json_atomic(queue.lease_path(mover), {
        "owner": claim.get("claimed_by"), "action_key": mover,
        "attempt": claim.get("attempt", 1),
        "heartbeat_unix": pool._now() - 10 * pool.LEASE_TIMEOUT_S})
    queue.reap_stale(timeout_s=1.0)

    assert queue._filed_pin_holds(mover) is False
    assert queue.tier_ledger(TIER).available().get("stage_gib", 0) == 2


def test_a_lead_that_is_claimed_right_now_is_not_pinned(fleet) -> None:
    """Claim-time tokens are a promise; the pin is the receipt (#625).

    The head mover ran once and landed short: its ``done`` record says
    ``executed``, its receipt says ``complete: false``, its tokens went back
    at ``finish``.  The next cycle republishes it, a worker claims it, and for
    as long as that copy runs the ledger holds tokens under the same key.
    The consumer's gate must still say no.
    """

    queue, mover = fleet.queue, _lead(fleet)
    _cycle(fleet)
    claim = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claim is not None and claim["action_key"] == mover
    queue.record_move(mover, {
        "tier_id": TIER, "consumer_action_key": CONSUMER, "complete": False,
        "bytes_staged": PHASE_BYTES // 2, "entries_staged": 0,
        "range_start_bytes": 0, "range_end_bytes": PHASE_BYTES,
        "stage_root": str(fleet.stage),
        "disk_pacing": {"mean_self_read_mb_s": 0.0}})
    queue.finish(mover, status="executed", claim_snapshot=claim)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {}
    assert queue._lead_is_pinned(fleet.staged["residency"], mover) is False

    _cycle(fleet)                                   # republished: unpinned, terminal
    again = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert again is not None and again["action_key"] == mover
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 2}

    assert queue._lead_is_pinned(fleet.staged["residency"], mover) is False
    verdict = queue.residency_verdict({"residency": fleet.staged["residency"]})
    assert verdict["state"] != "resident", verdict
