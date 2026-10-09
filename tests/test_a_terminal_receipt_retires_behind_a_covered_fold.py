"""Movement receipts retire behind a compact, recoverable exact checkpoint (#992).

Issue #992's remaining requirement: ``pb_gc`` must retire the movement
receipts of terminal consumers out of ``pb-queue/movers/``, because the set
otherwise grows for the life of the queue.  PR #1025 deferred it because
every pricing fold and both direct evidence lookups read those receipts:

* ``storage_tiers.fill_supply_from_records`` (ceiling, best, the median
  share and the pool-identity gate), ``mover_cap_from_records`` (the
  concurrency curve), ``mover_demand_from_receipts`` (cpu/mem maxima),
  ``mover_fill_price`` (the latest window per manifest) and
  ``movement_actions.egress_price``;
* ``PoolQueue.move_records``, the pricing history pbrun and
  produced_output seal new movers from;
* ``PoolQueue.move_record``, the direct evidence adoption, orphan-proof and
  retry paths read.

The corrected architecture keeps every output exact: at retirement the full
receipt is archived under its content digest and a compact projection -- the
pricing fields extended with every field the folds read -- is appended to a
versioned checkpoint.  All reads then apply the original pure fold functions
to ``retired projections + active receipts``, with an active record of the
same key overriding the retired one.  The active file leaves ``movers/``;
its history does not leave the queue.

These tests state behaviour through production entry points only:
``tier_loop.ReceiptCache``/``cycle`` for the announced fills, the
``storage_tiers`` folds, ``PoolQueue.move_records``/``move_record``,
``movement_actions.egress_price``, and ``pb_gc``'s survey/sweep.  They
deliberately do not assert that a retired receipt leaves ``move_records``:
it must not.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import random
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import movement_actions, pool, storage_tiers  # noqa: E402
import pb_gc  # noqa: E402
import stage_move  # noqa: E402
import tier_loop  # noqa: E402

#: The queue-root kind this requirement adds, spelled as the other kinds are.
MOVEMENT_RECEIPT = "movement receipt"

TIER = "prismabuild-stage:testbox"
HOST = "testbox"
GIB = storage_tiers.GIB
#: The identity the tier announces, one it can move to, and a dead pool's.
IDENTITY_A = {"stage": {"guid": "3333333333333333", "state": "ONLINE",
                        "members": ["sda", "sdb"]},
              "source": None}
IDENTITY_B = {"stage": {"guid": "3333333333333333", "state": "ONLINE",
                        "members": ["sda", "sdb", "sdc"]},
              "source": None}
OLD_IDENTITY = {"stage": {"guid": "1111111111111111", "state": "ONLINE",
                          "members": ["sdc"]},
                "source": None}
MANIFEST = "a" * 64
T0 = 1_700_000_000.0
#: 2 GiB over 20 s is 107 MB/s of staged bytes: below a 500 MB/s seal (a
#: shortfall) and above a 100 MB/s one (not).
STAGED = 2 * GIB
SECONDS = 20.0


def _key(tag: str) -> str:
    return hashlib.sha256(f"test-992-item2:{tag}".encode()).hexdigest()


def _terminal(queue: pool.PoolQueue, state: str, key: str,
              age_s: float = pool.LEASE_TIMEOUT_S + 60) -> None:
    path = queue.item_path(state, key)
    path.write_text(json.dumps(
        {"schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": key,
         "status": "executed" if state == pool.DONE else "failed",
         "published_unix": T0}) + "\n")
    stamp = time.time() - age_s
    os.utime(path, (stamp, stamp))


def _ready(queue: pool.PoolQueue, key: str) -> None:
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  tags=[HOST], resources={"cpu": 1, "mem_gb": 1})


def _move(queue: pool.PoolQueue, label: str, *, consumer: str, unix: float,
          identity: dict[str, object] = IDENTITY_A, delivered: float = 300.0,
          sealed: int = 500, file_side: float = 200.0, sharers: int = 1,
          tier_id: str = TIER) -> str:
    """One filed movement receipt, priced exactly as the folds read it."""

    key = _key(label)
    queue.record_move(key, {
        "schema": pool.POOL_MOVE_SCHEMA_V1,
        "consumer_action_key": consumer,
        "tier_id": tier_id,
        "unix": unix,
        "seconds": SECONDS,
        "complete": True,
        "bytes_staged": STAGED,
        "mb_per_s_file_side": file_side,
        "disk_pacing": {"mean_pool_read_mb_s": delivered,
                        "pool_read_bytes": STAGED},
        "fill_demand_mb_s_pool_side": sealed,
        "movers_claimed_on_tier": sharers,
        "pool_identity": identity,
        "manifest_sha256": MANIFEST,
        "cpu_seconds": 30.0, "peak_rss_bytes": 1 << 28,
    })
    return key


def _egress(queue: pool.PoolQueue, label: str, *, consumer: str, unix: float,
            stage_root: str) -> str:
    key = _key(label)
    queue.record_move(key, {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "consumer_action_key": consumer,
        "tier_id": TIER, "unix": unix, "seconds": 3.0, "complete": True,
        "stage_root": stage_root, "entries_judged": 10,
        "census_s": 1.0, "census_validate_s": 0.5, "lock_held_s": 2.5,
        "unlink_s": 0.5, "prune_s": 0.1,
    })
    return key


class _Loop:
    """A tmp_path queue and stage, and the loop's own kept reads."""

    def __init__(self, tmp_path: Path) -> None:
        self.queue = pool.PoolQueue(tmp_path / "pb-queue")
        self.queue.ensure_layout()
        self.stage = tmp_path / "stage"
        self.stage.mkdir()
        self.queue.mint_tier_capacity(TIER, {"stage_gib": 10})
        self.receipts = tier_loop.ReceiptCache()

    def cycle(self, identity: dict[str, object] = IDENTITY_A,
              receipts: tier_loop.ReceiptCache | None = None,
              ) -> dict[str, object]:
        announced = tier_loop.cycle(
            self.queue, host=HOST, source_pool="storage_pool",
            receipts=receipts or self.receipts,
            discover=lambda **_kw: {TIER: {
                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                "tier_id": TIER, "host": HOST, "tier": "stage",
                "mountpoint": str(self.stage),
                "capacity_bytes": 10 * GIB,
                "pool_identity": identity}})
        found = [record for record in announced
                 if record.get("tier_id") == TIER]
        assert found, announced
        return found[0]


def _history(loop: _Loop) -> dict[str, str]:
    """Terminal shortfalls and an egress for two consumers, a live one.

    ``r1``/``r3``/``r3b``/``r4``/``r5`` belong to terminal consumers (a done
    one and a failed one) and are retirement candidates.  ``r2`` belongs to a
    consumer still queued and must be kept.
    """

    queue = loop.queue
    terminal = _key("consumer-terminal")
    other = _key("consumer-other")
    live = _key("consumer-live")
    _terminal(queue, pool.DONE, terminal)
    _terminal(queue, pool.FAILED, other)
    _ready(queue, live)
    return {
        "terminal": terminal, "other": other, "live": live,
        "r1": _move(queue, "shortfall", consumer=terminal, unix=T0,
                    delivered=300.0, sealed=500, file_side=200.0),
        "r2": _move(queue, "live-receipt", consumer=live, unix=T0 + 10.0,
                    delivered=280.0, sealed=100, file_side=260.0),
        "r3": _move(queue, "cap-2a", consumer=other, unix=T0 + 20.0,
                    delivered=150.0, sealed=500, file_side=120.0, sharers=2),
        "r3b": _move(queue, "cap-2b", consumer=other, unix=T0 + 30.0,
                     delivered=145.0, sealed=100, file_side=140.0, sharers=2),
        "r4": _egress(queue, "egress", consumer=other, unix=T0 + 40.0,
                      stage_root=str(loop.stage)),
        "r5": _move(queue, "old-identity", consumer=terminal, unix=T0 + 50.0,
                    delivered=240.0, sealed=500, file_side=220.0,
                    identity=OLD_IDENTITY),
    }


def _records(queue: pool.PoolQueue) -> list[dict[str, object]]:
    return queue.move_records(schemas=pool.POOL_MOVEMENT_RECEIPT_SCHEMAS)


def _outputs(records: list[dict[str, object]],
             stage: Path) -> dict[str, object]:
    """Every pricing and fold output the retired receipts feed."""

    return {
        "fill": storage_tiers.fill_supply_from_records(
            records, pool_identity=IDENTITY_A),
        "fill_ungated": storage_tiers.fill_supply_from_records(records),
        "cap": storage_tiers.mover_cap_from_records(
            records, tier_id=TIER, pool_identity=IDENTITY_A),
        "demand": storage_tiers.mover_demand_from_receipts(
            records, tier_id=TIER, readers=2, fallback_mem_gb=2,
            pool_identity=IDENTITY_A),
        "price": storage_tiers.mover_fill_price(
            records, tier_id=TIER, pool_identity=IDENTITY_A,
            manifest_sha256=MANIFEST),
        "egress": movement_actions.egress_price(records,
                                                stage_root=str(stage)),
    }


def _survey(queue: pool.PoolQueue) -> dict[str, object]:
    plan = pb_gc.survey_queue(Path(queue.root))
    section = plan["sections"].get(MOVEMENT_RECEIPT)
    assert section is not None, (
        "pb_gc surveys no movement-receipt kind; movement receipts are never "
        "retired")
    return plan


def _candidates(plan: dict[str, object]) -> set[str]:
    return {str(row["key"])
            for row in plan["sections"][MOVEMENT_RECEIPT]["remove"]}  # type: ignore[index]


def _sweep(plan: dict[str, object]) -> dict[str, object]:
    return pb_gc.sweep_queue(plan, lock_takers_verify=True,
                             retire_movement_receipts=True)


def _fresh(loop: _Loop, identity: dict[str, object] = IDENTITY_A,
           ) -> dict[str, object]:
    """One cycle through a brand-new cache -- the restart path."""

    return loop.cycle(identity=identity, receipts=tier_loop.ReceiptCache())


def test_a_terminal_receipt_retires_and_every_pricing_output_is_unchanged(
        tmp_path: Path) -> None:
    """The retirement, with supply, cap, demand, fill, egress and pricing held.

    A fresh cache after the sweep -- the restart path -- must announce the
    same supply and reader plan, because the compact checkpoint, not the
    active directory, is now the carrier of the retired history.
    """

    loop = _Loop(tmp_path)
    keys = _history(loop)
    queue = loop.queue
    before_records = _records(queue)
    before_outputs = _outputs(before_records, loop.stage)
    announced = loop.cycle()
    before_supply = announced["fill_supply"]
    before_cap = announced[tier_loop.READER_PLAN_FIELD]["cap"]

    plan = _survey(queue)
    candidates = _candidates(plan)
    assert candidates == {keys[name] for name in
                          ("r1", "r3", "r3b", "r4", "r5")}, candidates
    assert keys["r2"] not in candidates
    archived = {key: queue.move_record(key) for key in candidates}

    outcome = _sweep(plan)
    assert {str(row["key"]) for row in outcome["removed"]} == candidates
    for key in candidates:
        assert not queue.move_path(key).is_file(), key

    after_records = _records(queue)
    assert [str(record["action_key"]) for record in after_records] == [
        str(record["action_key"]) for record in before_records]
    assert _outputs(after_records, loop.stage) == before_outputs
    for key, body in archived.items():
        assert queue.move_record(key) == body, key

    fresh = _fresh(loop)
    assert fresh["fill_supply"] == before_supply
    assert fresh[tier_loop.READER_PLAN_FIELD]["cap"] == before_cap


def test_an_identity_move_and_return_preserves_the_fold(tmp_path: Path) -> None:
    """A receipt gated out under identity B is not lost: B->A restores the fold.

    Identity includes the pool's scan state, so a tier can return to an
    earlier identity.  Retirement must therefore keep a projection a later
    identity may read again, never treat "gated out now" as authority to
    discard.
    """

    loop = _Loop(tmp_path)
    keys = _history(loop)
    before = _fresh(loop, IDENTITY_A)["fill_supply"]

    plan = _survey(loop.queue)
    _sweep(plan)
    assert not loop.queue.move_path(keys["r1"]).is_file()

    moved = _fresh(loop, IDENTITY_B)["fill_supply"]
    assert moved != before
    back = _fresh(loop, IDENTITY_A)["fill_supply"]
    assert back == before


def test_equal_and_late_unix_order_is_reproduced_exactly(tmp_path: Path) -> None:
    """Equal timestamps and a late-arriving earlier unix keep their fold order.

    ``a`` and ``b`` share an unix; ``c`` arrived after both and carries an
    earlier one.  The fold is order-sensitive (a shortfall resets the
    ceiling), so retiring ``a`` must not move ``b`` or ``c`` in the merged
    order: the valid sequence is ``c, a, b``, not ``c, b, a``.
    """

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    live = _key("live")
    _terminal(queue, pool.DONE, terminal)
    _ready(queue, live)
    a = _move(queue, "a", consumer=terminal, unix=T0,
              delivered=300.0, sealed=500, file_side=200.0)
    b = _move(queue, "b", consumer=live, unix=T0,
              delivered=400.0, sealed=100, file_side=250.0)
    c = _move(queue, "late", consumer=terminal, unix=T0 - 100.0,
              delivered=350.0, sealed=500, file_side=210.0)

    before_records = _records(queue)
    before = _outputs(before_records, loop.stage)

    # The fold is order-sensitive, which is why the merge has to reproduce
    # the original name order for equal timestamps: swapping ``a`` and ``b``
    # at their shared unix changes the answer.
    by_key = {str(record["action_key"]): record for record in before_records}
    assert _outputs([by_key[c], by_key[b], by_key[a]],
                    loop.stage)["fill"] != before["fill"]

    plan = _survey(queue)
    assert _candidates(plan) == {a, c}
    _sweep(plan)

    after_records = _records(queue)
    assert [str(record["action_key"]) for record in after_records] == [
        str(record["action_key"]) for record in before_records]
    assert _outputs(after_records, loop.stage) == before


def test_an_active_replacement_after_retirement_overrides_the_projection(
        tmp_path: Path) -> None:
    """A re-filed key is the active record; the retired projection stands aside."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "replaced", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    plan = _survey(queue)
    _sweep(plan)
    assert not queue.move_path(key).is_file()

    replacement = {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "consumer_action_key": terminal,
        "tier_id": TIER, "unix": T0 + 1.0, "seconds": SECONDS,
        "complete": True, "bytes_staged": STAGED,
        "mb_per_s_file_side": 275.0,
        "disk_pacing": {"mean_pool_read_mb_s": 275.0,
                        "pool_read_bytes": STAGED},
        "fill_demand_mb_s_pool_side": 100,
        "movers_claimed_on_tier": 1,
        "pool_identity": IDENTITY_A, "manifest_sha256": MANIFEST,
    }
    queue.record_move(key, replacement)
    records = _records(queue)
    assert [r for r in records if r.get("action_key") == key], records
    assert queue.move_record(key)["mb_per_s_file_side"] == 275.0
    assert _outputs(records, loop.stage) == _outputs(
        [{**replacement, "action_key": key}], loop.stage)


def test_a_receipt_replaced_after_the_survey_is_not_deleted(tmp_path: Path) -> None:
    """The sweep re-reads the receipt and leaves a new filing alone."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "raced", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    plan = _survey(queue)
    assert _candidates(plan) == {key}

    replacement = {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "consumer_action_key": terminal,
        "tier_id": TIER, "unix": T0 + 5.0, "seconds": SECONDS,
        "complete": True, "bytes_staged": STAGED,
        "mb_per_s_file_side": 225.0,
        "disk_pacing": {"mean_pool_read_mb_s": 225.0,
                        "pool_read_bytes": STAGED},
        "fill_demand_mb_s_pool_side": 500,
        "movers_claimed_on_tier": 1,
        "pool_identity": IDENTITY_A, "manifest_sha256": MANIFEST,
    }
    queue.record_move(key, replacement)

    outcome = _sweep(plan)
    assert outcome["removed"] == []
    assert any("replaced" in str(why) for _row, why in outcome["skipped"]), (
        outcome["skipped"])
    assert queue.move_record(key)["mb_per_s_file_side"] == 225.0
    assert _outputs(_records(queue), loop.stage) == _outputs(
        [{**replacement, "action_key": key}], loop.stage)

    # The new filing is then ordinary work for the next survey.
    second = _survey(queue)
    assert _candidates(second) == {key}
    _sweep(second)
    assert not queue.move_path(key).is_file()
    assert _outputs(_records(queue), loop.stage) == _outputs(
        [{**replacement, "action_key": key}], loop.stage)


def test_a_live_mover_or_held_token_keeps_the_receipt(tmp_path: Path) -> None:
    """Active interest is a veto: a queued retry and a held token both keep it."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    plain = _move(queue, "plain", consumer=terminal, unix=T0,
                  delivered=300.0, sealed=500, file_side=200.0)
    queued = _move(queue, "queued-retry", consumer=terminal, unix=T0 + 1.0,
                   delivered=260.0, sealed=500, file_side=210.0)
    charged = _move(queue, "charged", consumer=terminal, unix=T0 + 2.0,
                    delivered=250.0, sealed=500, file_side=205.0)
    _ready(queue, queued)
    assert queue.tier_ledger(TIER).acquire(charged, {"stage_gib": 1})

    assert _candidates(_survey(queue)) == {plain}


def test_a_missing_or_truncated_checkpoint_fails_closed(tmp_path: Path) -> None:
    """Retired history is never read as a shorter history (#992 item 2).

    A retirement leaves a checkpoint and a state marker, and its archived
    body.  A truncated, emptied, missing or tampered-away checkpoint must
    refuse in this process and in a fresh one; only a store with no marker
    and no archived body at all may read as "nothing was retired".
    """

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "retired", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    _sweep(_survey(queue))
    assert not queue.move_path(key).is_file()
    before = _records(queue)

    checkpoint = queue.mover_retirement_checkpoint_path()
    state = queue.mover_retirement_state_path()
    assert checkpoint.is_file() and state.is_file()
    good_checkpoint = checkpoint.read_bytes()
    good_state = state.read_bytes()

    checkpoint.write_bytes(good_checkpoint[: len(good_checkpoint) // 2])
    with pytest.raises(pool.PoolContractError):
        _records(queue)
    with pytest.raises(pool.PoolContractError):
        _fresh(loop)
    assert _survey(queue)["sections"][MOVEMENT_RECEIPT]["remove"] == []  # type: ignore[index]
    with pytest.raises(pool.PoolContractError):
        pool.PoolQueue(queue.root).read_mover_retirements()

    checkpoint.write_bytes(b"")
    with pytest.raises(pool.PoolContractError):
        _records(queue)

    checkpoint.write_bytes(good_checkpoint)
    assert _records(queue) == before
    checkpoint.unlink()
    with pytest.raises(pool.PoolContractError):
        _records(queue)

    # State gone too, but the archived bodies remain: still refused.
    state.unlink()
    with pytest.raises(pool.PoolContractError):
        _records(queue)
    with pytest.raises(pool.PoolContractError):
        _fresh(loop)

    # With the whole store gone there is nothing left to attribute the
    # archive to and no evidence a retirement happened: the reader cannot
    # invent history it cannot see.  (An operator deleting every trace is
    # outside what any checkpoint can detect; the contract keeps each piece
    # until all three are gone together, which no code path does.)
    for path in queue.mover_archive_dir().glob("*.json"):
        path.unlink()
    assert _records(queue) == []


def test_a_tampered_checkpoint_projection_or_revision_fails_closed(
        tmp_path: Path) -> None:
    """The checkpoint binds its entries: a changed value is not a new history."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "tampered", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    _sweep(_survey(queue))
    checkpoint = queue.mover_retirement_checkpoint_path()
    state = queue.mover_retirement_state_path()
    good_checkpoint = checkpoint.read_bytes()
    good_state = state.read_bytes()
    before = _records(queue)

    body = json.loads(checkpoint.read_text())
    body["entries"][key]["projection"]["disk_pacing"][
        "mean_pool_read_mb_s"] += 1.0
    checkpoint.write_text(json.dumps(body) + "\n")
    with pytest.raises(pool.PoolContractError):
        _records(queue)

    # A recomputed self-digest still fails the entry's projection binding.
    body["checkpoint_sha256"] = pool._retirement_checkpoint_digest(body)
    checkpoint.write_text(json.dumps(body) + "\n")
    with pytest.raises(pool.PoolContractError):
        _records(queue)

    # A checkpoint older than its state marker is a rollback, not history.
    checkpoint.write_bytes(good_checkpoint)
    marker = json.loads(state.read_text())
    marker["revision"] += 1
    state.write_text(json.dumps(marker) + "\n")
    with pytest.raises(pool.PoolContractError):
        _records(queue)

    state.write_bytes(good_state)
    assert _records(queue) == before


def test_a_corrupt_active_receipt_never_falls_back_to_the_archive(
        tmp_path: Path) -> None:
    """Archived evidence is not the current execution's, and not a shadow."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "shadowed", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    _sweep(_survey(queue))
    archived = queue.move_record(key)
    assert archived is not None

    active = queue.move_path(key)
    active.write_text("")                 # present but empty: unknown state
    assert queue.move_record(key) is None
    active.write_text("{not json\n")      # malformed active: raise, as before
    with pytest.raises(pool.PoolContractError):
        queue.move_record(key)


def test_a_named_tier_lock_failure_retains_the_receipt(tmp_path: Path) -> None:
    """An underivable tier mint lock is not permission to retire unlocked."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _key("bad-tier")
    queue.record_move(key, {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "consumer_action_key": terminal,
        "tier_id": "bad@tier", "unix": T0, "seconds": SECONDS,
        "complete": True, "bytes_staged": STAGED,
        "mb_per_s_file_side": 200.0,
        "disk_pacing": {"mean_pool_read_mb_s": 300.0,
                        "pool_read_bytes": STAGED},
        "fill_demand_mb_s_pool_side": 500, "movers_claimed_on_tier": 1,
        "pool_identity": IDENTITY_A, "manifest_sha256": MANIFEST,
    })
    assert _candidates(_survey(queue)) == set()
    info = queue.move_path(key).stat()
    version = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
               int(info.st_ctime_ns))
    reason = queue.retire_move_receipt(key, expected_version=version)
    assert reason, "a named tier's lock failure must retain the receipt"
    assert queue.move_path(key).is_file()
    assert queue.move_record(key)["tier_id"] == "bad@tier"


def test_a_corrupt_archive_fails_direct_lookup_closed(tmp_path: Path) -> None:
    """Direct evidence recovery refuses a body that is not the archived one."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "archived", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    _sweep(_survey(queue))
    for path in queue.mover_archive_dir().glob("*.json"):
        path.write_text("{corrupt\n")
    with pytest.raises(pool.PoolContractError):
        queue.move_record(key)


def test_a_restored_receipt_is_retired_again_without_duplication(
        tmp_path: Path) -> None:
    """The crash prefix -- checkpoint durable, active unlink not -- is idempotent."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "crash-prefix", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    before_records = _records(queue)
    before = _outputs(before_records, loop.stage)
    _sweep(_survey(queue))
    body = queue.move_record(key)
    assert body is not None

    # A crash after the checkpoint but before the unlink left the active file;
    # a restart re-files the same receipt.
    queue.record_move(key, body)
    assert _candidates(_survey(queue)) == {key}
    _sweep(_survey(queue))
    assert not queue.move_path(key).is_file()
    records = _records(queue)
    assert len([r for r in records
                if r.get("action_key") == key]) == 1
    assert _outputs(records, loop.stage) == before


@pytest.mark.parametrize("seed", range(6))
def test_retirement_preserves_every_output_over_generated_histories(
        tmp_path: Path, seed: int) -> None:
    """Generated histories: retire the eligible set, every output is unchanged."""

    rng = random.Random(seed)
    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    live = _key("live")
    _ready(queue, live)
    for index in range(10):
        _move(queue, f"generated-move-{index}",
              consumer=terminal if index % 3 else live,
              unix=T0 + rng.choice([0.0, 0.0, 1.0, 2.0, 3.0, 10.0]) + index,
              delivered=float(rng.randrange(80, 420)),
              sealed=rng.choice([80, 100, 250, 500, 900]),
              file_side=float(rng.randrange(60, 300)),
              sharers=rng.randrange(1, 4),
              identity=rng.choice([IDENTITY_A, IDENTITY_A, OLD_IDENTITY]))
    for index in range(2):
        _egress(queue, f"generated-egress-{index}", consumer=terminal,
                unix=T0 + 30.0 + index, stage_root=str(loop.stage))

    before_records = _records(queue)
    before = _outputs(before_records, loop.stage)
    plan = _survey(queue)
    candidates = _candidates(plan)
    assert candidates, "the generated history has no eligible receipt"
    _sweep(plan)

    after_records = _records(queue)
    assert [str(record["action_key"]) for record in after_records] == [
        str(record["action_key"]) for record in before_records]
    assert _outputs(after_records, loop.stage) == before


def _version(path: Path) -> tuple[int, int, int, int, int]:
    info = path.stat()
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            int(info.st_ctime_ns))


def _filesystem_type(path: Path) -> str | None:
    """The type the complete trust resolver names for ``path`` (#1506)."""


    info = os.stat(path)
    return stage_move._object_filesystem_type(
        info, path=path, follow_symlinks=False)


#: Filesystems whose directory times come from this kernel's clock, which the
#: trusted-stamp guard requires before it may reuse a listing.
LOCAL_CLOCK = {"zfs", "ext4", "xfs", "btrfs", "tmpfs"}


def test_a_holder_after_the_survey_stops_the_prepare(tmp_path: Path) -> None:
    """A holder that appears after the survey is re-checked under the mint lock."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "holder-late", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    assert _candidates(_survey(queue)) == {key}

    assert queue.tier_ledger(TIER).acquire(key, {"stage_gib": 1})
    version = _version(queue.move_path(key))
    outcome = queue.prepare_move_retirement(key, expected_version=version)
    assert isinstance(outcome, str), (
        "a holder acquired after the survey was archived anyway")
    assert queue.move_path(key).is_file()
    assert not queue.mover_archive_dir().exists() or not list(
        queue.mover_archive_dir().glob("*.json")), "the body was archived"


def test_a_holder_after_the_commit_stops_the_unlink(tmp_path: Path) -> None:
    """A holder restored between the checkpoint and the unlink keeps the receipt."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "holder-restored", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    baseline = queue.begin_mover_retirement_batch()
    version = _version(queue.move_path(key))
    entry = queue.prepare_move_retirement(key, expected_version=version)
    assert isinstance(entry, dict)
    _keys, identity, reason = queue.commit_move_retirements(
        [entry], expected_baseline=baseline)
    assert reason == "" and identity is not None

    assert queue.tier_ledger(TIER).acquire(key, {"stage_gib": 1})
    why = queue.unlink_retired_move(key, expected_version=version, entry=entry,
                                    commit_identity=identity)
    assert why, "a holder restored after the commit was unlinked anyway"
    assert queue.move_path(key).is_file()
    # The active record still overrides the retired projection exactly, with
    # no duplicate fold input.
    assert queue.read_mover_retirements()[key] == entry["projection"]
    assert len([record for record in _records(queue)
                if record.get("action_key") == key]) == 1


def test_an_unlistable_archive_directory_refuses_to_reset_history(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An archive directory that cannot be listed is unknown, not empty."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "unlistable", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    _sweep(_survey(queue))
    queue.mover_retirement_checkpoint_path().unlink()
    queue.mover_retirement_state_path().unlink()

    real_scandir = os.scandir

    def denied(path):  # type: ignore[no-untyped-def]
        # An integer is a directory descriptor: ``shutil.rmtree`` walks by
        # descriptor on current Pythons, and pytest's tmp_path cleanup does it
        # while this wrapper is still installed (#1506, as #1588).
        if isinstance(path, (str, os.PathLike)) and Path(path) == queue.mover_archive_dir():
            raise PermissionError("denied")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", denied)
    with pytest.raises(pool.PoolContractError):
        queue.begin_mover_retirement_batch()
    assert not queue.mover_retirement_checkpoint_path().exists()


def test_an_unexpected_archive_entry_refuses_to_reset_history(
        tmp_path: Path) -> None:
    """Anything unexpected in the archive store is conservatively history."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "unexpected", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    version = _version(queue.move_path(key))
    entry = queue.prepare_move_retirement(key, expected_version=version)
    assert isinstance(entry, dict)       # archived, active file still filed
    (queue.mover_archive_dir() / "notes.txt").write_text("operator notes\n")
    with pytest.raises(pool.PoolContractError):
        queue.begin_mover_retirement_batch()
    assert not queue.mover_retirement_checkpoint_path().exists()
    assert queue.move_path(key).is_file()


def test_retirement_is_an_explicit_opt_in(tmp_path: Path) -> None:
    """A rolling publish must not start retiring receipts by itself.

    ``--apply`` without ``--retire-movement-receipts`` reports the candidate
    and touches nothing, so an old reader generation never loses history.
    """

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "opt-in", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    plan = _survey(queue)
    assert _candidates(plan) == {key}

    outcome = pb_gc.sweep_queue(plan, lock_takers_verify=True)
    assert outcome["removed"] == []
    assert any(pb_gc.MISSING_RETIREMENT_OPT_IN in str(why)
               for _row, why in outcome["skipped"]), outcome["skipped"]
    assert queue.move_path(key).is_file()
    assert _candidates(_survey(queue)) == {key}

    assert {str(row["key"]) for row in _sweep(_survey(queue))["removed"]} == {key}


def test_a_crash_between_checkpoint_and_state_is_repaired_on_retry(
        tmp_path: Path) -> None:
    """The retry completes the transaction instead of skipping its marker."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    first = _move(queue, "first", consumer=terminal, unix=T0,
                  delivered=300.0, sealed=500, file_side=200.0)
    _sweep(_survey(queue))

    second = _move(queue, "second", consumer=terminal, unix=T0 + 1.0,
                   delivered=250.0, sealed=500, file_side=210.0)
    version = _version(queue.move_path(second))
    baseline = queue.begin_mover_retirement_batch()
    entry = queue.prepare_move_retirement(second, expected_version=version)
    assert isinstance(entry, dict)
    committed, _identity, reason = queue.commit_move_retirements(
        [entry], expected_baseline=baseline)
    assert reason == "" and committed == [second]
    checkpoint_path = queue.mover_retirement_checkpoint_path()
    state_path = queue.mover_retirement_state_path()
    revision = pool._read_json(checkpoint_path)["revision"]

    # The crash the review names: the checkpoint has the entry, the state
    # marker was never written, and the active receipt is still filed.
    state_path.unlink()
    assert set(queue.read_mover_retirements()) == {first, second}

    assert queue.retire_move_receipt(second, expected_version=version) == ""
    assert not queue.move_path(second).is_file()
    state = pool._read_json(state_path)
    assert state["revision"] == revision
    assert state["checkpoint_sha256"] == pool._read_json(
        checkpoint_path)["checkpoint_sha256"]
    assert pool._read_json(checkpoint_path)["revision"] == revision
    assert set(queue.read_mover_retirements()) == {first, second}


def test_first_write_archive_orphans_must_be_exact_active_copies(
        tmp_path: Path) -> None:
    """A checkpoint may restart only over archives every active receipt copies.

    A crash after the first archive and before its commit leaves archives
    with no checkpoint; the active receipt still reproduces them, so the
    commit is safe.  But archives no active receipt reproduces may be
    already-retired history: a lost checkpoint must never be reset to an
    empty one.
    """

    clean = _Loop(tmp_path / "clean")
    terminal = _key("terminal")
    _terminal(clean.queue, pool.DONE, terminal)
    key = _move(clean.queue, "first-write", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    version = _version(clean.queue.move_path(key))
    baseline = clean.queue.begin_mover_retirement_batch()
    entry = clean.queue.prepare_move_retirement(key, expected_version=version)
    assert isinstance(entry, dict)
    committed, identity, reason = clean.queue.commit_move_retirements(
        [entry], expected_baseline=baseline)
    assert reason == "" and committed == [key] and identity is not None
    assert clean.queue.unlink_retired_move(
        key, expected_version=version, entry=entry,
        commit_identity=identity) == ""
    assert not clean.queue.move_path(key).is_file()
    assert clean.queue.move_record(key) is not None

    lost = _Loop(tmp_path / "lost")
    terminal = _key("terminal")
    _terminal(lost.queue, pool.DONE, terminal)
    retired = _move(lost.queue, "retired", consumer=terminal, unix=T0,
                    delivered=300.0, sealed=500, file_side=200.0)
    _sweep(_survey(lost.queue))
    lost.queue.mover_retirement_checkpoint_path().unlink()
    lost.queue.mover_retirement_state_path().unlink()
    with pytest.raises(pool.PoolContractError):
        lost.queue.read_mover_retirements()
    assert _candidates(_survey(lost.queue)) == set()
    assert [row for row in _sweep(_survey(lost.queue))["removed"]
            if row["kind"] == MOVEMENT_RECEIPT] == []

    later = _move(lost.queue, "later", consumer=terminal, unix=T0 + 1.0,
                  delivered=250.0, sealed=500, file_side=210.0)
    version = _version(lost.queue.move_path(later))
    entry = lost.queue.prepare_move_retirement(later, expected_version=version)
    assert isinstance(entry, dict)
    with pytest.raises(pool.PoolContractError):
        lost.queue.begin_mover_retirement_batch()
    assert lost.queue.move_path(later).is_file()
    assert not lost.queue.mover_retirement_checkpoint_path().exists()


def test_a_read_during_preparation_sees_an_exact_empty_checkpoint(
        tmp_path: Path) -> None:
    """A first batch initializes the checkpoint before its first archive.

    Preparation writes archives while the checkpoint is still empty; if the
    checkpoint did not exist yet, every reader would refuse for the whole
    window.  The batch baseline is captured first and materialized as an
    exact empty checkpoint plus marker, so a concurrent read folds the active
    receipts without refusing.
    """

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    keys = [_move(queue, f"window-{index}", consumer=terminal,
                  unix=T0 + index, delivered=300.0, sealed=500,
                  file_side=200.0) for index in range(3)]
    baseline = queue.begin_mover_retirement_batch()
    assert queue.read_mover_retirements() == {}

    prepared = [queue.prepare_move_retirement(
        key, expected_version=_version(queue.move_path(key))) for key in keys]
    assert all(isinstance(entry, dict) for entry in prepared)
    assert queue.read_mover_retirements() == {}
    _fresh(loop)                       # a reader during the prepare window
    assert queue.move_path(keys[0]).is_file()

    committed, identity, reason = queue.commit_move_retirements(
        prepared, expected_baseline=baseline)
    assert reason == "" and set(committed) == set(keys)
    assert identity is not None
    for key, entry in zip(keys, prepared):
        assert queue.unlink_retired_move(
            key, expected_version=_version(queue.move_path(key)), entry=entry,
            commit_identity=identity) == ""
    assert set(queue.read_mover_retirements()) == set(keys)


def test_a_steady_reading_cycle_reuses_the_validated_checkpoint(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Nothing changed, so no checkpoint or projection is hashed again.

    The reuse rests on the trusted directory stamp, so this needs a
    filesystem whose directory times come from this kernel's clock (a
    recognizable local mount, e.g. a private tmpfs via ``TMPDIR``).
    """

    info = os.stat(tmp_path)
    assert stage_move._object_filesystem_type(
        info, path=tmp_path, follow_symlinks=False) in LOCAL_CLOCK, (
        f"{stage_move._object_filesystem_type(info, path=tmp_path)} keeps no "
        f"trusted stamp; run this on a local-clock filesystem (TMPDIR)")

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "cached", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    _sweep(_survey(queue))
    assert not queue.move_path(key).is_file()

    calls = {"projection": 0, "checkpoint": 0}
    real_projection = pool._projection_sha256
    real_digest = pool._retirement_checkpoint_digest

    def counted_projection(projection):  # type: ignore[no-untyped-def]
        calls["projection"] += 1
        return real_projection(projection)

    def counted_digest(body):  # type: ignore[no-untyped-def]
        calls["checkpoint"] += 1
        return real_digest(body)

    monkeypatch.setattr(pool, "_projection_sha256", counted_projection)
    monkeypatch.setattr(pool, "_retirement_checkpoint_digest", counted_digest)
    loop.cycle()                       # this cache validates once
    loop.cycle()
    calls["projection"] = calls["checkpoint"] = 0
    loop.cycle()
    loop.cycle()
    assert calls == {"projection": 0, "checkpoint": 0}, calls


def test_a_batch_retirement_replaces_the_checkpoint_once(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The #992 shape: 6,000 receipts retire with one checkpoint write and
    every pricing output unchanged."""

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    keys = [_move(queue, f"batched-{index}", consumer=terminal,
                  unix=T0 + index, delivered=float(100 + index % 200),
                  sealed=500, file_side=float(90 + index % 150))
            for index in range(6000)]
    before_records = _records(queue)
    before = _outputs(before_records, loop.stage)
    plan = _survey(queue)
    assert _candidates(plan) == set(keys)

    written: list[str] = []
    real_write = pool._write_bytes_atomic

    def counted(path, data):  # type: ignore[no-untyped-def]
        written.append(path.name)
        return real_write(path, data)

    monkeypatch.setattr(pool, "_write_bytes_atomic", counted)
    outcome = _sweep(plan)
    assert {str(row["key"]) for row in outcome["removed"]} == set(keys)
    # One initialization before the first archive plus one commit for the
    # whole batch: two checkpoint writes and two markers, not 6,000.
    assert written.count(pool.MOVERS_RETIREMENT_CHECKPOINT) == 2, written
    assert written.count(pool.MOVERS_RETIREMENT_STATE) == 2, written
    checkpoint = pool._read_json(queue.mover_retirement_checkpoint_path())
    assert checkpoint["revision"] == 1
    assert set(checkpoint["entries"]) == set(keys)
    assert set(queue.read_mover_retirements()) == set(keys)
    assert _outputs(_records(queue), loop.stage) == before


def test_a_tampered_projection_after_a_warm_cache_is_never_reused(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A changed record refuses even when its checkpoint header is unchanged.

    On a mount whose stamps cannot be trusted, a warm cache must not reuse
    validated entries by header fields alone: the review's mutation keeps the
    revision and the self-digest and changes one projection, which the
    projection binding must catch.
    """


    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "warm-tamper", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    _sweep(_survey(queue))
    loop.cycle()                       # warm the cache

    monkeypatch.setattr(
        stage_move, "_object_filesystem_type",
        lambda info, *, path=None, descriptor=None,
        follow_symlinks=True: None)
    checkpoint = queue.mover_retirement_checkpoint_path()
    body = json.loads(checkpoint.read_text())
    body["entries"][key]["projection"]["disk_pacing"][
        "mean_pool_read_mb_s"] += 1.0
    checkpoint.write_text(json.dumps(body) + "\n")   # header fields kept
    with pytest.raises(pool.PoolContractError):
        loop.cycle()
    with pytest.raises(pool.PoolContractError):
        _records(queue)


def test_a_stale_batch_never_overwrites_a_newer_retirement(
        tmp_path: Path) -> None:
    """The review's interleaving: a stale prepared entry must not commit.

    GC-old prepares A's version; an action re-files the key; GC-new prepares,
    commits and unlinks the newer B; GC-old then commits A.  Without a
    checkpoint baseline the stale batch replaces B's entry and readers price
    a receipt that no longer exists.
    """

    loop = _Loop(tmp_path)
    queue = loop.queue
    terminal = _key("terminal")
    _terminal(queue, pool.DONE, terminal)
    key = _move(queue, "raced", consumer=terminal, unix=T0,
                delivered=300.0, sealed=500, file_side=200.0)
    baseline = queue.begin_mover_retirement_batch()
    version_a = _version(queue.move_path(key))
    entry_a = queue.prepare_move_retirement(key, expected_version=version_a)
    assert isinstance(entry_a, dict)

    replacement = {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "consumer_action_key": terminal,
        "tier_id": TIER, "unix": T0 + 1.0, "seconds": SECONDS,
        "complete": True, "bytes_staged": STAGED,
        "mb_per_s_file_side": 275.0,
        "disk_pacing": {"mean_pool_read_mb_s": 275.0,
                        "pool_read_bytes": STAGED},
        "fill_demand_mb_s_pool_side": 100,
        "movers_claimed_on_tier": 1,
        "pool_identity": IDENTITY_A, "manifest_sha256": MANIFEST,
    }
    queue.record_move(key, replacement)
    version_b = _version(queue.move_path(key))
    entry_b = queue.prepare_move_retirement(key, expected_version=version_b)
    assert isinstance(entry_b, dict)
    keys, identity, reason = queue.commit_move_retirements(
        [entry_b], expected_baseline=baseline)
    assert reason == "" and keys == [key] and identity is not None
    assert queue.unlink_retired_move(
        key, expected_version=version_b, entry=entry_b,
        commit_identity=identity) == ""
    assert not queue.move_path(key).is_file()

    keys, identity, reason = queue.commit_move_retirements(
        [entry_a], expected_baseline=baseline)
    assert keys == [] and identity is None and reason, (
        "a stale batch overwrote a newer retirement")
    assert queue.read_mover_retirements()[key]["mb_per_s_file_side"] == 275.0
    assert not queue.move_path(key).is_file()


def test_a_retirement_cache_never_merges_another_queue(tmp_path: Path) -> None:
    """One cache reading two queues must not fold one queue's retired history
    into the other's active set."""

    first = _Loop(tmp_path / "first")
    second = _Loop(tmp_path / "second")
    first_key = _key("first")
    second_key = _key("second")
    for loop, label, key in ((first, "first", first_key),
                             (second, "second", second_key)):
        terminal = _key(f"terminal-{label}")
        _terminal(loop.queue, pool.DONE, terminal)
        _move(loop.queue, label, consumer=terminal, unix=T0,
              delivered=300.0, sealed=500, file_side=200.0)
    _sweep(_survey(first.queue))

    cache = tier_loop.ReceiptCache()
    # Neither read names a queue: the cache derives the root from the
    # directories each time, so the second one must not keep the first.
    rows_first = cache.read([first.queue.root / pool.PREWARM,
                             first.queue.root / pool.MOVERS])
    rows_second = cache.read([second.queue.root / pool.PREWARM,
                              second.queue.root / pool.MOVERS])
    assert any(record.get("action_key") == first_key for record in rows_first)
    assert not any(record.get("action_key") == first_key
                   for record in rows_second), rows_second
    assert any(record.get("action_key") == second_key
               for record in rows_second)


