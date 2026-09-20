"""Funded tier claims: the minimal committed primitive (window liveness lane).

Drives REAL tier ledgers and REAL ``PoolQueue.claim`` calls with small
logical GiB quotas -- never fleet defaults, never forged worker progress.
This is the acceptance Root asked for first: a readable explicit funding
contract (state table in :mod:`prismabuild.window_credit`) plus focused
real-ledger claim/retry/fault tests, before broad window wiring.

What is proved here:

* reserve -> transfer -> claim -> finish with exact-fit protection from an
  unrelated stealer, no double-hold, and ``consumed`` fusing;
* a denied claim (second tier occupied) preserves the entitlement: the
  first tier's record stays ``transferring`` under the SAME generation with
  the fence still held, and the retry consumes that same generation without
  taking the fence twice;
* missing/malformed/torn funding never authorizes a subtraction, and old
  physical tokens (landed bytes, leftovers) never count as advance credit;
* the binding is exact -- tier, mover, kind, ``published_unix``,
  sealed residency range, live consumer/plan digest, named token identity --
  and ``write_funding`` is compare-and-swap on generation;
* grant/transaction prefixes never collide with claims or action keys;
* every funding generation/state effect serializes on the mover's
  transition lock (cross-process barrier test), so a coordinator
  reserve/rotate racing a claim defers instead of pulling the claim's pin;
* consumed physical bytes are never credit: after claim -> copy -> DONE,
  the mover's pinned tokens survive every credit helper untouched until
  the owner path (egress) deletes the bytes.

Lane: this file plus ``pool.py`` tier-claim/credit hunks and the funding
helpers are the liveness lane's.  Output732 owns the produced-output scope
path (pbrun template capture, ``publish``/``validate_residency`` output
handling, cycle output tick) and REUSES this contract via root -- no second
funding protocol is defined here.
"""
from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
from prismabuild import window_credit  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
RAM_KIND = f"ram_gib@{RAM_TIER}"
MANIFEST = "8" * 64
GIB = storage_tiers.GIB

CONSUMER = "c" * 64


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, mover: str, *,
          tag: str, gib: int = 2) -> dict[str, object]:
    start, end = 0, gib * GIB
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=end, phases=[{
            "name": f"phase-{tag}",
            "start_bytes": start, "end_bytes": end, "stage_gib": gib,
            "mover_row": {
                **_row(mover, {STAGE_KIND: gib}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(_hexkey(f"egress-{tag}"), {"mem_gb": 1}, queue),
        }])


def _queue(tmp_path: Path, *, stage_gib: int, ram_gib: int = 0) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": stage_gib})
    if ram_gib:
        queue.mint_tier_capacity(RAM_TIER, {"ram_gib": ram_gib})
    return queue


def _publish_mover(queue: pool.PoolQueue, plan: dict[str, object], mover: str,
                   *, resources: dict[str, int] | None = None) -> dict[str, object]:
    phases = plan["phases"]
    assert isinstance(phases, list)
    mover_row = dict(phases[0]["mover_row"])  # type: ignore[index]
    assert str(mover_row["action_key"]) == mover
    if resources is not None:
        mover_row["resources"] = dict(resources)
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=mover, cas_root=mover_row["cas_root"],
        checkout_root=mover_row["checkout_root"],
        worker_script=mover_row["worker_script"],
        tags=["dl380g10"], resources=mover_row["resources"],
        residency=mover_row["residency"])
    row = pool.read_queue_record(queue.item_path(pool.READY, mover))
    assert isinstance(row, dict)
    return row


def _stealer_row(queue: pool.PoolQueue, key: str, gib: int) -> None:
    span = gib * GIB
    queue.publish(
        action_key=key, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        tags=["dl380g10"], resources={STAGE_KIND: gib},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": span,
                   "range_start_bytes": 0, "range_end_bytes": span})


def _fields(queue: pool.PoolQueue, plan: dict[str, object], mover: str,
            row: dict[str, object], *, kind: str) -> dict[str, object]:
    phases = plan["phases"]
    assert isinstance(phases, list)
    leg = phases[0]
    return {"consumer_action_key": str(plan["consumer_action_key"]),
            "plan_sha256": residency_plan.plan_sha256(plan),
            "mover_action_key": mover,
            "range_start_bytes": int(leg["start_bytes"]),  # type: ignore[index]
            "range_end_bytes": int(leg["end_bytes"]),  # type: ignore[index]
            "kind": kind,
            "published_unix": float(row["published_unix"])}  # type: ignore[arg-type]


def _reserve_and_transfer(queue: pool.PoolQueue, plan: dict[str, object],
                          mover: str, row: dict[str, object], *,
                          kind: str, gib: int, grant: str) -> str:
    assert queue.reserve_fence(TIER, grant, _fields(
        queue, plan, mover, row, kind=kind), gib) is True
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "reserved"
    generation = str(record["generation"])
    assert queue.transfer_fence(TIER, grant, mover) == gib
    assert queue.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="transferring",
        generation=generation) is True
    return generation


def test_grant_and_transaction_prefixes() -> None:
    """Fence holders and funding tokens never read as claims or actions."""
    grant = window_credit.grant_key("c" * 64, TIER, "mover_row", "phase-0")
    assert grant.startswith(window_credit.GRANT_PREFIX)
    assert not grant.startswith("claiming.")
    assert len(grant) != 64 or any(
        c not in "0123456789abcdef" for c in grant)
    assert window_credit.grant_key("c" * 64, TIER, "mover_row", "phase-0") == grant
    assert (window_credit.grant_key("c" * 64, TIER, "mover_row", "phase-1")
            != grant)


def test_funding_state_machine_and_cas(tmp_path: Path) -> None:
    """reserved -> transferring -> consumed | released; CAS on generation."""
    queue = _queue(tmp_path, stage_gib=6)
    mover, consumer = _hexkey("fsm-mover"), _hexkey("fsm-consumer")
    plan = _plan(queue, consumer, mover, tag="fsm")
    row = _publish_mover(queue, plan, mover)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-fsm")
    fields = _fields(queue, plan, mover, row, kind="stage_gib")

    assert queue.reserve_fence(TIER, grant, fields, 2) is True
    first = queue.read_funding(mover, TIER)
    assert first is not None and first["state"] == "reserved"
    gen = str(first["generation"])

    # Illegal jumps refuse; wrong generations refuse.
    assert queue.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="consumed",
        generation=gen) is False
    assert queue.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="transferring",
        generation="0" * 32) is False
    # Unconditional overwrite refuses even with a valid record body.
    tampered = dict(first, state="transferring")
    with pytest.raises(pool.PoolContractError):
        queue.write_funding(tampered)
    with pytest.raises(pool.PoolContractError):
        queue.write_funding(tampered, expect_generation="1" * 32)

    assert queue.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="transferring",
        generation=gen) is True
    assert queue.advance_funding_state(
        mover, TIER, expect="transferring", advance_to="consumed",
        generation=gen) is True
    # Terminal: never advances, never authorizes again.
    assert queue.advance_funding_state(
        mover, TIER, expect="transferring", advance_to="released",
        generation=gen) is False
    assert queue.advance_funding_state(
        mover, TIER, expect="consumed", advance_to="released",
        generation=gen) is False
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (0, None)


def test_funded_cover_exact_binding(tmp_path: Path) -> None:
    """Every binding field is load-bearing; one mismatch is no credit."""
    queue = _queue(tmp_path, stage_gib=6)
    mover, consumer = _hexkey("bind-mover"), _hexkey("bind-consumer")
    plan = _plan(queue, consumer, mover, tag="bind")
    row = _publish_mover(queue, plan, mover)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-bind")
    generation = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=2, grant=grant)

    covered, live = queue.funded_cover(TIER, row, "stage_gib", 2)
    assert (covered, live) == (2, generation)

    # Republished content-hash key never inherits older credit.
    republished = dict(row, published_unix=float(row["published_unix"]) + 1.0)
    assert queue.funded_cover(TIER, republished, "stage_gib", 2) == (0, None)
    # Wrong kind / tier / need.
    assert queue.funded_cover(TIER, row, "ram_gib", 2) == (0, None)
    assert queue.funded_cover("other-tier", row, "stage_gib", 2) == (0, None)
    # Range mismatch (row edited out from under the binding).
    shifted = dict(row, residency=dict(
        row["residency"], range_end_bytes=int(  # type: ignore[union-attr]
            row["residency"]["range_end_bytes"]) + 1))  # type: ignore[index]
    assert queue.funded_cover(TIER, shifted, "stage_gib", 2) == (0, None)
    # Replaced plan under a new digest never inherits credit.  (Written
    # directly: ``freeze`` is first-writer by contract, so a test simulates
    # the replacement the retirement machinery would file.)
    plan_path = queue.residency_plan_path(consumer)
    original = plan_path.read_bytes()
    plan2 = _plan(queue, consumer, mover, tag="bind-v2")
    assert residency_plan.plan_sha256(plan2) != residency_plan.plan_sha256(plan)
    plan_path.chmod(0o644)
    plan_path.write_text(json.dumps(plan2), encoding="utf-8")
    try:
        assert queue.funded_cover(TIER, row, "stage_gib", 2) == (0, None)
    finally:
        plan_path.write_bytes(original)
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (2, generation)
    # Torn record authorizes nothing and wedges nothing.
    queue.funding_path(mover, TIER).write_text("{torn", encoding="utf-8")
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (0, None)
    # A coordinator re-fence repairs the torn binding with a new generation.
    assert queue.reserve_fence(TIER, grant, _fields(
        queue, plan, mover, row, kind="stage_gib"), 2) is True
    repaired = queue.read_funding(mover, TIER)
    assert repaired is not None and repaired["state"] == "reserved"
    assert str(repaired["generation"]) != generation


def test_reserve_transfer_claim_finish_no_double_hold(tmp_path: Path) -> None:
    """Exact-fit funded claim: stealer blocked, free untouched, fuse shut."""
    queue = _queue(tmp_path, stage_gib=5)
    ledger = queue.tier_ledger(TIER)
    mover, consumer = _hexkey("exact-mover"), _hexkey("exact-consumer")
    plan = _plan(queue, consumer, mover, tag="exact")

    # A bare fence already holds room: a 4 GiB stealer fits the empty tier
    # but not beside 2 GiB of fence, while the scan still admits a 3 GiB
    # one.  Dummy binding (no mover row): this part proves occupancy only.
    dummy = {"consumer_action_key": consumer,
             "plan_sha256": residency_plan.plan_sha256(plan),
             "mover_action_key": _hexkey("exact-dummy"),
             "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
             "kind": "stage_gib", "published_unix": 1.0}
    grant_dum = window_credit.grant_key(consumer, TIER, "mover_row", "dummy")
    assert queue.reserve_fence(TIER, grant_dum, dummy, 2) is True
    assert ledger.available().get("stage_gib") == 3
    big = _hexkey("exact-stealer")
    _stealer_row(queue, big, 4)
    assert queue.claim(tags=["dl380g10"], owner="w-steal") is None
    assert queue.item_path(pool.READY, big).exists()
    small = _hexkey("exact-small")
    _stealer_row(queue, small, 3)
    got = queue.claim(tags=["dl380g10"], owner="w-small")
    assert got is not None and got["action_key"] == small
    queue.finish(small, status="executed")
    assert int(ledger.release(grant_dum)) == 2

    # The funded mover then claims its fence with no new money: the same
    # scan denies the 4 GiB stealer and admits the mover behind it.
    row = _publish_mover(queue, plan, mover)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-exact")
    generation = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=2, grant=grant)
    assert ledger.available().get("stage_gib") == 3
    free_before = ledger.available().get("stage_gib")
    got = queue.claim(tags=["dl380g10"], owner="w-mover")
    assert got is not None and got["action_key"] == mover
    assert queue.item_path(pool.READY, big).exists()
    # No new money: the fence covered the whole demand.
    assert ledger.available().get("stage_gib") == free_before
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "consumed"
    assert str(record["generation"]) == generation

    queue.finish(mover, status="executed")
    assert ledger.available().get("stage_gib") == 5
    # Consumed credit is never reused.
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (0, None)
    got = queue.claim(tags=["dl380g10"], owner="w-steal-again")
    assert got is not None and got["action_key"] == big
    queue.finish(big, status="executed")


def test_denied_claim_preserves_entitlement_for_retry(tmp_path: Path) -> None:
    """Tier1 fence survives a tier2 shortage; retry spends it exactly once."""
    queue = _queue(tmp_path, stage_gib=4, ram_gib=4)
    mover, consumer = _hexkey("retry-mover"), _hexkey("retry-consumer")
    plan = _plan(queue, consumer, mover, tag="retry")
    resources = {STAGE_KIND: 2, RAM_KIND: 2}
    row = _publish_mover(queue, plan, mover, resources=resources)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-retry")
    generation = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=2, grant=grant)

    # Occupy the whole second tier with a squatter holding real tokens.
    ram_ledger = queue.tier_ledger(RAM_TIER)
    assert ram_ledger.acquire("squatter", {"ram_gib": 4}) is True

    # The claim is denied on the second tier -- before any persistence, so
    # before any consumed-marking: the first tier's entitlement must survive
    # intact for the retry, never freed into a stealer window.
    assert queue.claim(tags=["dl380g10"], owner="w-retry-1") is None
    assert queue.item_path(pool.READY, mover).exists()
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "transferring"
    assert str(record["generation"]) == generation
    stage_ledger = queue.tier_ledger(TIER)
    assert int(stage_ledger.holder_tokens(mover).get("stage_gib", 0)) == 2

    assert ram_ledger.release("squatter") == 4
    got = queue.claim(tags=["dl380g10"], owner="w-retry-2")
    assert got is not None and got["action_key"] == mover
    # Same generation, spent exactly once: still exactly 2, not 4.
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "consumed"
    assert str(record["generation"]) == generation
    assert int(stage_ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    queue.finish(mover, status="executed")
    assert stage_ledger.available().get("stage_gib") == 4


def test_physical_tokens_never_count_as_credit(tmp_path: Path) -> None:
    """A landed copy's held tokens are occupancy: the next copy pays full."""
    queue = _queue(tmp_path, stage_gib=6)
    mover, consumer = _hexkey("phys-mover"), _hexkey("phys-consumer")
    plan = _plan(queue, consumer, mover, tag="phys")
    row = _publish_mover(queue, plan, mover)
    ledger = queue.tier_ledger(TIER)
    # A previous attempt's leftovers, held with no funding record at all.
    assert ledger.acquire(mover, {"stage_gib": 2}) is True
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (0, None)
    got = queue.claim(tags=["dl380g10"], owner="w-phys")
    assert got is not None and got["action_key"] == mover
    # Full charge on top of occupancy: 2 old + 2 new, never 2 total.
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 4
    assert ledger.available().get("stage_gib") == 2
    queue.finish(mover, status="executed")


def test_malformed_funding_is_inert_and_replaceable(tmp_path: Path) -> None:
    """Unknown fields / wrong shapes authorize nothing; fencing recovers."""
    queue = _queue(tmp_path, stage_gib=6)
    mover, consumer = _hexkey("mal-mover"), _hexkey("mal-consumer")
    plan = _plan(queue, consumer, mover, tag="mal")
    row = _publish_mover(queue, plan, mover)
    path = queue.funding_path(mover, TIER)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"schema": pool.TIER_FUNDING_SCHEMA_V1,
                                "tier_id": TIER, "extra": True}),
                    encoding="utf-8")
    assert queue.read_funding(mover, TIER) is None
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (0, None)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-mal")
    assert queue.reserve_fence(TIER, grant, _fields(
        queue, plan, mover, row, kind="stage_gib"), 2) is True
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "reserved"


def test_consumed_funding_refences_after_finish(tmp_path: Path) -> None:
    """A spent generation never blocks the next window's fresh fence."""
    queue = _queue(tmp_path, stage_gib=4)
    mover, consumer = _hexkey("re-mover"), _hexkey("re-consumer")
    plan = _plan(queue, consumer, mover, tag="re")
    row = _publish_mover(queue, plan, mover)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-re")
    first_gen = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=2, grant=grant)
    got = queue.claim(tags=["dl380g10"], owner="w-re")
    assert got is not None and got["action_key"] == mover
    queue.finish(mover, status="executed")
    # Same mover republished (new publication): old credit is inert ...
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (0, None)
    # ... and a fresh fence mints a fresh generation instead of wedging.
    queue.publish(action_key=mover, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py", tags=["dl380g10"],
                  resources={STAGE_KIND: 2}, residency=row["residency"])
    row2 = pool.read_queue_record(queue.item_path(pool.READY, mover))
    assert isinstance(row2, dict)
    assert queue.reserve_fence(TIER, grant, _fields(
        queue, plan, mover, row2, kind="stage_gib"), 2) is True
    second = queue.read_funding(mover, TIER)
    assert second is not None and second["state"] == "reserved"
    assert str(second["generation"]) != first_gen


def _hold_mover_lock(queue_root: str, mover: str, ready, release) -> None:
    """Child side of the cross-process barrier: hold the mover lock."""
    from prismabuild import pool as _pool_mod
    queue = _pool_mod.PoolQueue(queue_root)
    with queue.mover_transition_lock(mover, blocking=True) as acquired:
        assert acquired
        ready.set()
        release.wait(30)


def test_funding_writers_serialize_on_mover_lock(tmp_path: Path) -> None:
    """A coordinator racing a claim-holder defers; it never rotates."""
    ctx = multiprocessing.get_context("fork")
    queue = _queue(tmp_path, stage_gib=4)
    mover, consumer = _hexkey("lock-mover"), _hexkey("lock-consumer")
    plan = _plan(queue, consumer, mover, tag="lock")
    row = _publish_mover(queue, plan, mover)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-lock")
    fields = _fields(queue, plan, mover, row, kind="stage_gib")
    assert queue.reserve_fence(TIER, grant, fields, 2) is True
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "reserved"
    generation = str(record["generation"])

    ready, release = ctx.Event(), ctx.Event()
    child = ctx.Process(target=_hold_mover_lock,
                        args=(str(queue.root), mover, ready, release))
    child.start()
    try:
        assert ready.wait(30), "lock-holder child never acquired"
        # Racing the holder: both generation effects refuse loudly.
        assert queue.reserve_fence(TIER, grant, fields, 2) is False
        assert queue.advance_funding_state(
            mover, TIER, expect="reserved", advance_to="transferring",
            generation=generation) is False
        # And the binding is untouched -- no rotation under the claim.
        current = queue.read_funding(mover, TIER)
        assert current is not None and current["state"] == "reserved"
        assert str(current["generation"]) == generation
    finally:
        release.set()
        child.join(30)
    assert not child.is_alive()
    # Holder gone: the same effects proceed.
    assert queue.reserve_fence(TIER, grant, fields, 2) is True
    assert queue.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="transferring",
        generation=generation) is True


def test_consumed_physical_bytes_are_not_credit(tmp_path: Path) -> None:
    """Claim -> copy -> DONE pins bytes; no credit helper frees them."""
    queue = _queue(tmp_path, stage_gib=4)
    ledger = queue.tier_ledger(TIER)
    mover, consumer = _hexkey("pin-mover"), _hexkey("pin-consumer")
    plan = _plan(queue, consumer, mover, tag="pin")
    row = _publish_mover(queue, plan, mover)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-pin")
    generation = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=2, grant=grant)

    got = queue.claim(tags=["dl380g10"], owner="w-pin")
    assert got is not None and got["action_key"] == mover
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": "/stage/pin", "complete": True,
        "bytes_staged": 2 * GIB})
    queue.finish(mover, status="executed")
    # Landed: the mover still holds its 2 GiB under DONE, record consumed.
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    assert ledger.available().get("stage_gib") == 2
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "consumed"
    assert str(record["generation"]) == generation

    # Retired credit metadata frees nothing: every state effect refuses,
    # the idempotent reserve moves nothing.  The cover still answers the
    # full fence while it is held in full under the same publication --
    # that is the retry-reuse of an unexecuted attempt, unreachable for a
    # DONE row (no DONE row ever claims again), and it dies the moment the
    # set is partial or the key republishes (see the next test).
    assert queue.advance_funding_state(
        mover, TIER, expect="consumed", advance_to="released",
        generation=generation) is False
    assert queue.advance_funding_state(
        mover, TIER, expect="transferring", advance_to="consumed",
        generation=generation) is False
    free_before = ledger.available().get("stage_gib")
    assert queue.reserve_fence(TIER, grant, _fields(
        queue, plan, mover, row, kind="stage_gib"), 2) is True
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    assert ledger.available().get("stage_gib") == free_before
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (2, generation)
    # Only the owner path may return landed bytes; until it does the
    # ledger still charges them to the mover that staged them.
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 2


def test_claim_record_write_failure_unwinds_exact(tmp_path: Path, monkeypatch) -> None:
    """Injected claim-record failure: no execution, exact fence recovery."""
    queue = _queue(tmp_path, stage_gib=4)
    ledger = queue.tier_ledger(TIER)
    mover, consumer = _hexkey("inj-mover"), _hexkey("inj-consumer")
    plan = _plan(queue, consumer, mover, tag="inj")
    row = _publish_mover(queue, plan, mover)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-inj")
    generation = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=2, grant=grant)

    real_write = pool._write_json_atomic

    def fail_claim_record(path, *args, **kwargs):
        if (str(path).endswith(f"{mover}.json")
                and f"{pool.CLAIMED}{os.sep}" in str(path)):
            raise OSError("injected claim-record failure")
        return real_write(path, *args, **kwargs)

    monkeypatch.setattr(pool, "_write_json_atomic", fail_claim_record)
    assert queue.claim(tags=["dl380g10"], owner="w-inj") is None
    # The row is back in ready with its publication intact; the fence never
    # touched free, so the free count is exact and no stealer window opened.
    row2 = pool.read_queue_record(queue.item_path(pool.READY, mover))
    assert isinstance(row2, dict)
    assert row2["published_unix"] == row["published_unix"]
    assert row2["resources"] == row["resources"]
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "transferring"
    assert str(record["generation"]) == generation
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    assert ledger.available().get("stage_gib") == 2

    # Retry spends the same fence exactly once: holdings do not grow.
    monkeypatch.setattr(pool, "_write_json_atomic", real_write)
    got = queue.claim(tags=["dl380g10"], owner="w-inj-retry")
    assert got is not None and got["action_key"] == mover
    assert got["tier_funding"][TIER]["generation"] == generation
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    assert ledger.available().get("stage_gib") == 2
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "consumed"
    queue.finish(mover, status="executed")
    assert ledger.available().get("stage_gib") == 4


def test_lease_write_failure_returns_remainder_keeps_fence(
        tmp_path: Path, monkeypatch) -> None:
    """Injected lease failure on a funded+remainder claim: exact unwind."""
    queue = _queue(tmp_path, stage_gib=4, ram_gib=4)
    stage_ledger = queue.tier_ledger(TIER)
    ram_ledger = queue.tier_ledger(RAM_TIER)
    mover, consumer = _hexkey("lease-mover"), _hexkey("lease-consumer")
    plan = _plan(queue, consumer, mover, tag="lease")
    resources = {STAGE_KIND: 2, RAM_KIND: 2}
    row = _publish_mover(queue, plan, mover, resources=resources)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-lease")
    generation = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=2, grant=grant)

    real_lease = pool.PoolQueue.write_lease

    def fail_lease(*args, **kwargs):
        raise pool.PoolContractError("injected lease failure")

    monkeypatch.setattr(pool.PoolQueue, "write_lease", fail_lease)
    assert queue.claim(tags=["dl380g10"], owner="w-lease") is None
    # Funded fence kept in full; the ram remainder this attempt committed
    # went home exactly (ram free is whole again); nothing executed.
    row2 = pool.read_queue_record(queue.item_path(pool.READY, mover))
    assert isinstance(row2, dict)
    assert row2["published_unix"] == row["published_unix"]
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "transferring"
    assert str(record["generation"]) == generation
    assert int(stage_ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    assert stage_ledger.available().get("stage_gib") == 2
    assert int(ram_ledger.holder_tokens(mover).get("ram_gib", 0)) == 0
    assert ram_ledger.available().get("ram_gib") == 4

    # Retry takes the remainder exactly once beside the same fence.
    monkeypatch.setattr(pool.PoolQueue, "write_lease", real_lease)
    got = queue.claim(tags=["dl380g10"], owner="w-lease-retry")
    assert got is not None and got["action_key"] == mover
    assert int(stage_ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    assert int(ram_ledger.holder_tokens(mover).get("ram_gib", 0)) == 2
    assert stage_ledger.available().get("stage_gib") == 2
    assert ram_ledger.available().get("ram_gib") == 2
    queue.finish(mover, status="executed")
    assert stage_ledger.available().get("stage_gib") == 4
    assert ram_ledger.available().get("ram_gib") == 4


def test_mark_failure_unwinds_without_execution(
        tmp_path: Path, monkeypatch) -> None:
    """Injected consumed-marker failure: no execution, exact fence recovery.

    One claim carries at most one funded tier -- a row has a single
    residency block, and ``funded_cover`` requires the row's residency tier
    to equal the record's tier -- so the marking loop's partial-failure
    shape (tier1 fused, tier2 failed) is structurally unreachable; the
    generic loop plus the consumed-reuse rule still recovers it exactly if
    it ever arises.  What is reachable and tested here: the single mark
    fails, the claim unwinds instead of executing, repeated failures neither
    grow holdings nor open a stealer window, and the retry fuses the same
    generation shut spending the fence exactly once.
    """
    queue = _queue(tmp_path, stage_gib=4, ram_gib=4)
    stage_ledger = queue.tier_ledger(TIER)
    ram_ledger = queue.tier_ledger(RAM_TIER)
    mover, consumer = _hexkey("mark-mover"), _hexkey("mark-consumer")
    plan = _plan(queue, consumer, mover, tag="mark")
    resources = {STAGE_KIND: 2, RAM_KIND: 2}
    row = _publish_mover(queue, plan, mover, resources=resources)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-mark")
    generation = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=2, grant=grant)

    real_mark = pool.PoolQueue._advance_funding_state_locked
    failures = {"count": 0}

    def fail_mark_once(self, mover_key, tier_id, **kwargs):
        if failures["count"] < 2:
            failures["count"] += 1
            return False
        return real_mark(self, mover_key, tier_id, **kwargs)

    monkeypatch.setattr(pool.PoolQueue, "_advance_funding_state_locked",
                        fail_mark_once)
    for attempt in ("w-mark-1", "w-mark-2"):
        assert queue.claim(tags=["dl380g10"], owner=attempt) is None
        # No execution either time; the fence stayed held in full under its
        # generation, the ram remainder went home -- never freed, never
        # regrown.
        assert queue.item_path(pool.READY, mover).exists()
        record = queue.read_funding(mover, TIER)
        assert record is not None and record["state"] == "transferring"
        assert str(record["generation"]) == generation
        assert int(stage_ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
        assert int(ram_ledger.holder_tokens(mover).get("ram_gib", 0)) == 0
        assert stage_ledger.available().get("stage_gib") == 2
        assert ram_ledger.available().get("ram_gib") == 4
    assert failures["count"] == 2

    # Injection exhausted: the retry fuses the same generation shut and runs
    # with holdings byte-exact beside a once-taken remainder.
    monkeypatch.setattr(pool.PoolQueue, "_advance_funding_state_locked",
                        real_mark)
    got = queue.claim(tags=["dl380g10"], owner="w-mark-retry")
    assert got is not None and got["action_key"] == mover
    assert got["tier_funding"][TIER]["generation"] == generation
    assert int(stage_ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    assert int(ram_ledger.holder_tokens(mover).get("ram_gib", 0)) == 2
    assert stage_ledger.available().get("stage_gib") == 2
    assert ram_ledger.available().get("ram_gib") == 2
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "consumed"
    assert str(record["generation"]) == generation
    queue.finish(mover, status="executed")
    assert stage_ledger.available().get("stage_gib") == 4
    assert ram_ledger.available().get("ram_gib") == 4


def test_consumed_cover_needs_full_held_set(tmp_path: Path) -> None:
    """Retry-reuse dies the moment the bound set is partial."""
    queue = _queue(tmp_path, stage_gib=4)
    ledger = queue.tier_ledger(TIER)
    mover, consumer = _hexkey("part-mover"), _hexkey("part-consumer")
    plan = _plan(queue, consumer, mover, tag="part")
    row = _publish_mover(queue, plan, mover)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-part")
    generation = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=2, grant=grant)

    got = queue.claim(tags=["dl380g10"], owner="w-part")
    assert got is not None and got["action_key"] == mover
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "consumed"
    bound = [str(name) for name in record["tokens"]]
    assert len(bound) == 2
    # Fully held under the same publication: the retry re-covers.
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (2, generation)
    # One bound token gone: nothing is covered, never a half credit.
    assert ledger.release_except(mover, bound[1:]) == 1
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (0, None)
    # All gone: still nothing.
    assert ledger.release(mover) == 1
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (0, None)
    queue.finish(mover, status="executed")


def test_real_copy_lifecycle_stays_charged_until_egress(tmp_path: Path) -> None:
    """One small ACTUAL mover path: copy bytes, pin, delete, release."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]
                           / "tools" / "fleet"))
    import stage_release
    from prismabuild import residency_map
    queue = _queue(tmp_path, stage_gib=3)
    ledger = queue.tier_ledger(TIER)
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=str(stage)) == "registered"

    span = 1 << 20
    mover, consumer = _hexkey("real-mover"), _hexkey("real-consumer")
    plan = residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=span, phases=[{
            "name": "phase-real",
            "start_bytes": 0, "end_bytes": span, "stage_gib": 1,
            "mover_row": {
                **_row(mover, {STAGE_KIND: 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": span,
                    "range_start_bytes": 0, "range_end_bytes": span},
            },
            "egress_row": _row(_hexkey("real-egress"), {"mem_gb": 1}, queue),
        }])
    row = _publish_mover(queue, plan, mover)
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-real")
    generation = _reserve_and_transfer(
        queue, plan, mover, row, kind="stage_gib", gib=1, grant=grant)

    got = queue.claim(tags=["dl380g10"], owner="w-real")
    assert got is not None and got["action_key"] == mover
    # The actual copy: real bytes on the stage, fragment filed, receipt
    # measured -- the pin below is earned by bytes, not by a comment.
    staged = stage / "real.bin"
    staged.write_bytes(b"s" * span)
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "entries": {residency_map.residency_map_key("/pool/real.bin", 0): {
            "stage_path": str(staged), "bytes": span,
            "offset": 0, "sha256": "b" * 64}}})
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "complete": True, "bytes_staged": span})
    queue.finish(mover, status="executed")
    # Landed and pinned: charged to the mover, record consumed.
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    assert ledger.available().get("stage_gib") == 2
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "consumed"
    assert str(record["generation"]) == generation
    assert queue.advance_funding_state(
        mover, TIER, expect="consumed", advance_to="released",
        generation=generation) is False
    # Only the owner path returns landed bytes: the real egress deletes the
    # real file, releases the charge, and drops the fragment.
    receipt = stage_release.evict(
        queue, mover, consumer_action_key=consumer, stage_root=str(stage))
    assert not staged.exists()
    assert int(receipt.get("tokens_released", 0)) == 1
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 0
    assert ledger.available().get("stage_gib") == 3
