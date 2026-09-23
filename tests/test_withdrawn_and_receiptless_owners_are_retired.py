"""Housekeeping retires a withdrawn consumer's dead owners and a receipt-less holder.

Two gaps, both seen on the live fleet on 2026-09-22.

**A withdrawn consumer's dead owner was never retired.**  The dead-owner
sweep (#839) took a consumer's death only from ``failed/``.  A consumer an
operator withdrew left its withdrawn movers' fragments vouching for staged
paths with no tokens and no live row, exactly the #839 shape, and nothing
routed them through ``evict``.  WS-P ran three egresses by hand that day
(receipts 8c84c2eadb17, c55763dd54b5, c948370a27cf) to clear them.  A
withdrawal is as final as a failure when its immutable decision says so, and
it is checked the way a withdrawn mover's already is.

**A held mover whose receipt is gone could never be swept.**  The orphan
sweep reads the consumer off the mover's receipt and skipped every holder
without one.  The canary leg-3 mover ``aa34e2a6e22f`` finished on 2026-09-19,
its consumer is done, it holds 1 stage GiB, and ``movers/`` has no receipt
for it -- but its fragment is still filed under its consumer, and the
fragment is the document that names whose bytes these are.  One direct
fragment is an exact owner; none, or more than one, is not.  The consumer it
names must also have ended, proven by exactly one outcome record: a consumer
that is still queued, or that the queue has no record of at all, may yet read
the bytes.  A produced-output mover is never resolved this way: its tokens
belong to its batch's lifecycle, which owns the funding
(``safe_release_instance``).

A holder the sweep cannot resolve is reported only when keeping it costs
something: the sweep was given pressure for the tier, and after every orphan
it could evict the tier still lacks the room.  Otherwise the pass stays
quiet, as it did before, so an unresolvable holder does not add a line to
every tier cycle.

Everything runs on a synthetic stage under ``tmp_path`` registered to a
queue under ``tmp_path``; nothing reads or writes a real stage.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool, produced_output, residency_map, storage_tiers  # noqa: E402
import stage_release  # noqa: E402

import test_dead_owner_fragment_blocks_then_retires as dead  # noqa: E402

TIER = dead.TIER
NAMES = dead.NAMES
fleet = dead.fleet
STAGE_KIND = f"stage_gib@{TIER}"


def _withdraw_consumer(queue: pool.PoolQueue) -> str:
    """A consumer that ran, was withdrawn, and concluded -- nothing live."""

    key = dead._key()
    holder = dead._publish(queue, key, max_attempts=1)
    queue.withdraw(key, reason="operator withdrew the consumer", by="test")
    queue.finish(key, status="withdrawn", detail={"returncode": -15},
                 claim_snapshot=holder)
    assert queue.item_path(pool.WITHDRAWN, key).exists()
    assert not queue.item_path(pool.CLAIMED, key).exists()
    return key


def _withdrawn_owner(fleet) -> tuple[str, str]:
    """Withdrawn consumer + withdrawn mover, fragment but nothing else."""

    queue, stage, _cas = fleet
    consumer = _withdraw_consumer(queue)
    mover = dead._withdraw_mover(queue)
    for name in NAMES:
        dead._stage_marked(stage, name)
    dead._write_fragment(queue, stage, consumer, mover, NAMES)
    assert queue.move_record(mover) is None
    assert mover not in queue.tier_ledger(TIER).held_keys()
    return consumer, mover


def _retired(receipts, mover) -> list[dict]:
    return [entry for entry in receipts
            if entry.get("action_key") == mover and entry.get("complete") is True]


# ------------------------------------------------ a withdrawn consumer's owner


def test_a_withdrawn_consumers_dead_owner_is_retired(fleet) -> None:
    queue, stage, _cas = fleet
    consumer, mover = _withdrawn_owner(fleet)
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    assert _retired(receipts, mover), receipts
    assert [entry["reason"] for entry in _retired(receipts, mover)] == ["dead-owner-sweep"]
    assert not any((stage / name).exists() for name in NAMES)
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


def test_a_withdrawn_consumer_without_its_decision_retains(fleet) -> None:
    """The withdrawal is proven by its immutable decision, as a mover's is."""

    queue, stage, _cas = fleet
    consumer, mover = _withdrawn_owner(fleet)
    marker = json.loads(queue.item_path(pool.WITHDRAWN, consumer).read_bytes())
    queue.withdrawal_decision_path(marker).unlink()
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    assert not _retired(receipts, mover)
    dead._assert_retained(queue, stage, consumer, mover)


def test_a_withdrawn_consumer_that_is_live_again_retains(fleet) -> None:
    queue, stage, _cas = fleet
    consumer, mover = _withdrawn_owner(fleet)
    queue.publish(action_key=consumer, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 1}, max_attempts=1,
                  recompute=True)
    assert queue.item_path(pool.READY, consumer).exists()
    stage_release.sweep(queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    dead._assert_retained(queue, stage, consumer, mover)


def test_a_consumer_both_withdrawn_and_failed_retains(fleet) -> None:
    """Two terminal records for one consumer is not a death, it is a question."""

    queue, stage, _cas = fleet
    consumer, mover = _withdrawn_owner(fleet)
    failed_consumer, _generation = dead._fail_consumer(queue)
    record = json.loads(queue.item_path(pool.FAILED, failed_consumer).read_bytes())
    record["action_key"] = consumer
    queue.item_path(pool.FAILED, consumer).write_text(json.dumps(record))
    stage_release.sweep(queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    dead._assert_retained(queue, stage, consumer, mover)


# ------------------------------------------------ a holder whose receipt is gone


MANIFEST = "9" * 64


def _held_mover(queue: pool.PoolQueue, stage: Path, consumer: str) -> str:
    """A finished mover that staged its range and holds its tokens.

    The receipt is filed so ``finish`` keeps the tokens, exactly as a real
    mover's does, and then removed: the live canary mover holds 1 GiB with no
    receipt in ``movers/``.
    """

    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    mover = dead._key()
    queue.publish(action_key=mover, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py",
                  resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 1},
                  residency={
                      "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                      "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
                      "range_start_bytes": 0,
                      "range_end_bytes": storage_tiers.GIB},
                  max_attempts=1, retry_safe=False)
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == mover
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": storage_tiers.GIB,
        "bytes_staged": storage_tiers.GIB, "complete": True})
    queue.finish(mover, status="executed")
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 1}
    queue.move_path(mover).unlink()
    assert queue.move_record(mover) is None
    return mover


def _done_consumer(queue: pool.PoolQueue) -> str:
    key = dead._key()
    dead._publish(queue, key, max_attempts=1)
    queue.finish(key, status="executed", detail={"returncode": 0})
    assert queue.item_path(pool.DONE, key).exists()
    return key


def _receiptless(fleet) -> tuple[str, str]:
    queue, stage, _cas = fleet
    consumer = _done_consumer(queue)
    mover = _held_mover(queue, stage, consumer)
    for name in NAMES:
        dead._stage_marked(stage, name)
    dead._write_fragment(queue, stage, consumer, mover, NAMES)
    return consumer, mover


#: More room than the synthetic tier has: the window can never fit, so every
#: retention costs something and must say why.
UNMET = {TIER: 64}


def _refusals(receipts, mover) -> list[dict]:
    refused = [entry for entry in receipts if entry.get("action_key") == mover]
    assert all(entry.get("complete") is not True for entry in refused), refused
    assert all(entry.get("event") == stage_release.RECEIPTLESS_HOLDER_EVENT
               for entry in refused), refused
    return refused


def test_a_receiptless_holder_is_swept_through_its_fragments_consumer(fleet) -> None:
    queue, stage, _cas = fleet
    consumer, mover = _receiptless(fleet)
    receipts = stage_release.sweep(queue, stage_roots={TIER: str(stage)})
    swept = [entry for entry in receipts if entry.get("action_key") == mover]
    assert [entry.get("reason") for entry in swept] == ["orphan-sweep"], receipts
    assert swept[0]["consumer_action_key"] == consumer
    assert swept[0]["complete"] is True
    assert not any((stage / name).exists() for name in NAMES)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {}
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


def test_a_receiptless_holder_waits_for_pressure_like_any_orphan(fleet) -> None:
    """#598: an orphan's eviction is a decision the window's need makes."""

    queue, stage, _cas = fleet
    consumer, mover = _receiptless(fleet)
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    assert not [entry for entry in receipts if entry.get("action_key") == mover]
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 1}
    assert all((stage / name).exists() for name in NAMES)


def test_a_receiptless_holder_named_by_two_consumers_retains(fleet) -> None:
    queue, stage, _cas = fleet
    consumer, mover = _receiptless(fleet)
    other = _done_consumer(queue)
    dead._write_fragment(queue, stage, other, mover, NAMES[:1])
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure=UNMET)
    refused = _refusals(receipts, mover)
    assert any("2 fragments" in " ".join(entry.get("errors", [])) for entry in refused)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 1}
    assert all((stage / name).exists() for name in NAMES)


def test_a_receiptless_holder_with_no_fragment_retains(fleet) -> None:
    """No document names an owner, so nothing here may decide one."""

    queue, stage, _cas = fleet
    consumer = _done_consumer(queue)
    mover = _held_mover(queue, stage, consumer)
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure=UNMET)
    refused = _refusals(receipts, mover)
    assert any("no fragment names" in " ".join(entry.get("errors", []))
               for entry in refused)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 1}


def test_a_receiptless_holder_with_a_tainted_census_retains(fleet) -> None:
    queue, stage, _cas = fleet
    consumer, mover = _receiptless(fleet)
    residue = queue.root / pool.RESIDENCY / consumer / "residue.json"
    residue.write_bytes(b"{not json")
    receipts = stage_release.sweep(queue, stage_roots={TIER: str(stage)})
    assert not _retired(receipts, mover)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 1}
    assert all((stage / name).exists() for name in NAMES)


def test_a_produced_output_holder_is_left_to_its_batch_lifecycle(fleet) -> None:
    """Only a direct fragment resolves an owner; a produced batch's does not."""

    queue, stage, _cas = fleet
    consumer = _done_consumer(queue)
    mover = _held_mover(queue, stage, consumer)
    namespace = dead._key()
    root = produced_output.output_fragment_root(queue.root / pool.RESIDENCY)
    for name in NAMES:
        dead._stage_marked(stage, name)
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": namespace, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(str(stage / name), 0): {
                "stage_path": str(stage / name), "bytes": dead.SIZE,
                "sha256": "b" * 64, "offset": 0} for name in NAMES}})
    receipts = stage_release.sweep(queue, stage_roots={TIER: str(stage)})
    assert not _retired(receipts, mover)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 1}
    assert all((stage / name).exists() for name in NAMES)


# ------------------------------------------ an owner that has not provably ended


def _fragment_named(fleet, consumer: str) -> str:
    queue, stage, _cas = fleet
    mover = _held_mover(queue, stage, consumer)
    for name in NAMES:
        dead._stage_marked(stage, name)
    dead._write_fragment(queue, stage, consumer, mover, NAMES)
    return mover


def _kept(queue, stage, mover) -> None:
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 1}
    assert all((stage / name).exists() for name in NAMES)


def test_a_fragment_naming_a_consumer_the_queue_never_saw_retains(fleet) -> None:
    """No outcome record is not an ending: the bytes may still be read."""

    queue, stage, _cas = fleet
    never = dead._key()
    mover = _fragment_named(fleet, never)
    receipts = stage_release.sweep(queue, stage_roots={TIER: str(stage)})
    # Reported once as unresolved (#929), and nothing else happens to it.
    assert [entry.get("event") for entry in receipts
            if entry.get("action_key") == mover] == [
                stage_release.HOLDER_UNRESOLVED_EVENT], receipts
    _kept(queue, stage, mover)
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure=UNMET)
    refused = _refusals(receipts, mover)
    assert any("no outcome record" in " ".join(entry.get("errors", []))
               for entry in refused), refused
    _kept(queue, stage, mover)


def test_a_fragment_naming_a_still_queued_consumer_retains(fleet) -> None:
    """A claimed consumer whose plan does not name the mover may still read it."""

    queue, stage, _cas = fleet
    consumer = dead._key()
    dead._publish(queue, consumer, max_attempts=1)
    assert queue.item_path(pool.CLAIMED, consumer).exists()
    mover = _fragment_named(fleet, consumer)
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure=UNMET)
    refused = _refusals(receipts, mover)
    assert any("still queued" in " ".join(entry.get("errors", []))
               for entry in refused), refused
    _kept(queue, stage, mover)


def test_an_unresolvable_holder_is_quiet_when_it_costs_nothing(fleet) -> None:
    """Without unmet pressure a retained holder adds no line to every cycle.

    It is reported once, as unresolved, the first time a pass sees it (#929):
    an operator hears about a holder nothing can classify, and hears it once.
    """

    queue, stage, _cas = fleet
    consumer = _done_consumer(queue)
    mover = _held_mover(queue, stage, consumer)
    events = []
    for pressure in (None, {TIER: 0}, {TIER: 1}):
        receipts = stage_release.sweep(
            queue, stage_roots={TIER: str(stage)}, pressure=pressure)
        events.extend(entry.get("event") for entry in receipts
                      if entry.get("action_key") == mover)
    assert events == [stage_release.HOLDER_UNRESOLVED_EVENT], events
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 1}
