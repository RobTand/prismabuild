"""Group reconcile, census, funding split and release (#1594).

Tests first: they pin the R1'' rule table, the funded-claim shape
the unchanged claim path accepts, and the R2 terminal cleanup.
The integrator runs them; nothing here runs anything itself.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
from prismabuild import prelaunch_group as pg  # noqa: E402

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "8" * 64
GIB = storage_tiers.GIB

PHASES = ["phase-a", "phase-b"]


def _hexkey(seed: str) -> str:
    """A 64-hex action key derived from a short seed."""
    return (seed.encode().hex() * 64)[:64]


def _queue(tmp_path: Path, *, stage_gib: int) -> pool.PoolQueue:
    """An admitted queue with a real minted stage tier."""
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "pb-queue"), capacity={"cpu": 4, "mem_gb": 8},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": stage_gib})
    return queue


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict:
    """A minimal runnable row for one mover."""
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, first: str, second: str,
          *, gib: int = 2) -> dict:
    """A two-phase plan with one chunk mover per phase."""
    span = gib * GIB
    phases = []
    for tag, mover, start in (("a", first, 0), ("b", second, span)):
        phases.append({
            "name": f"phase-{tag}",
            "start_bytes": start, "end_bytes": start + span, "stage_gib": gib,
            "mover_row": {
                **_row(mover, {"cpu": 1, "mem_gb": 1, STAGE_KIND: gib}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 2 * span,
                    "range_start_bytes": start, "range_end_bytes": start + span},
            },
            "egress_row": _row(_hexkey(f"egress-{tag}"), {"mem_gb": 1}, queue),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=2 * span, phases=phases)


def _publish(queue: pool.PoolQueue, plan: dict, mover: str) -> dict:
    """Freeze once, then publish one mover row and return it."""
    residency_plan.freeze(queue, plan)
    phases = plan["phases"]
    assert isinstance(phases, list)
    mover_row = None
    for phase in phases:
        row = phase["mover_row"]
        if str(row["action_key"]) == mover:
            mover_row = dict(row)
    assert mover_row is not None
    queue.publish(
        action_key=mover, cas_root=mover_row["cas_root"],
        checkout_root=mover_row["checkout_root"],
        worker_script=mover_row["worker_script"],
        tags=["dl380g10"], resources=mover_row["resources"],
        residency=mover_row["residency"])
    row = pool.read_queue_record(queue.item_path(pool.READY, mover))
    assert isinstance(row, dict)
    return row


def _leg(plan: dict, mover: str) -> dict:
    """The sealed leg for one mover, carrying its key for publication."""
    found = residency_plan.find_mover_leg(plan, mover)
    assert found is not None
    return {**found, "mover_action_key": mover}


def _chunks(plan: dict, movers: list[str]) -> list[dict]:
    """Intent chunk entries quoting the sealed legs."""
    digest = residency_plan.plan_sha256(plan)
    consumer = str(plan["consumer_action_key"])
    out = []
    for mover in movers:
        leg = _leg(plan, mover)
        out.append({"mover_action_key": mover,
                    "start_bytes": int(leg["start_bytes"]),
                    "end_bytes": int(leg["end_bytes"]),
                    "stage_gib": int(leg["stage_gib"]),
                    "plan_sha256": digest, "consumer_action_key": consumer})
    return out


def _group(queue: pool.PoolQueue, plan: dict, movers: list[str],
           *, demand: int) -> tuple[str, str]:
    """File the intent for one unit and return (unit, holder)."""
    unit = str(plan["consumer_action_key"])
    holder = pg.holder_name(unit, TIER, PHASES)
    assert pg.file_intent(queue, unit, holder, TIER, demand,
                          _chunks(plan, movers)) is True
    return unit, holder


def _drive_to_committed(queue, tier, unit, holder, demand, movers):
    """Run the reconcile to a committed group; return the outcome."""
    first = pg.reconcile(queue, tier, unit, holder, demand, movers,
                         writer_is_me=True)
    assert first.state == "acquiring"
    assert "prelaunch-group-begun" in first.events
    second = pg.reconcile(queue, tier, unit, holder, demand, movers,
                          writer_is_me=True)
    assert second.state == "acquiring"
    assert "prelaunch-acquisition-committed" in second.events
    third = pg.reconcile(queue, tier, unit, holder, demand, movers,
                         writer_is_me=True)
    assert third.state == "reserved"
    assert third.authority is True
    return third


def _move_tokens(held: Path, names: list[str], destination: Path) -> None:
    """Rename named tokens; the test form of an interrupted commit."""
    destination.mkdir(parents=True, exist_ok=True)
    for name in names:
        os.rename(held / name, destination / name)


# ---------------------------------------------------------------- names


def test_holder_name_has_no_dots_and_hex_digests(tmp_path) -> None:
    """The contract shape: unit16 plus two 12-hex digests."""
    unit = _hexkey("unit")
    name = pg.holder_name(unit, TIER, PHASES)
    assert name == (f"prelaunch-{unit[:16]}-"
                    f"{hashlib.sha256(TIER.encode()).hexdigest()[:12]}-"
                    f"{hashlib.sha256(chr(31).join(PHASES).encode()).hexdigest()[:12]}")
    assert "." not in name
    assert len(name.split("-")) == 4


def test_receipts_live_under_unit_dot_tier_digest(tmp_path) -> None:
    """Receipts use the queue no-clobber helpers under their own directory."""
    queue = _queue(tmp_path, stage_gib=8)
    consumer, mover = _hexkey("dir-consumer"), _hexkey("dir-mover")
    plan = _plan(queue, consumer, mover, _hexkey("dir-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    directory = pg.group_dir(queue, unit, TIER)
    assert directory.parent.name == "prelaunch-groups"
    assert directory.name == f"{unit}.{pg.tier_digest(TIER)}"
    assert (directory / "intent.json").exists()
    assert pg.file_intent(queue, unit, holder, TIER, 2,
                          _chunks(plan, [mover])) is True
    with pytest.raises(pool.PoolContractError):
        pg.file_intent(queue, unit, holder, TIER, 999, _chunks(plan, [mover]))
    assert json.loads((directory / "intent.json").read_text())["demand_gib"] == 2


# ------------------------------------------------- crash point 1: begin


def test_crash_after_begin_rolls_forward_by_this_writer(tmp_path) -> None:
    """Begin leaves h=0 p=T; the next passes commit and conclude."""
    queue = _queue(tmp_path, stage_gib=8)
    consumer, mover = _hexkey("begin-consumer"), _hexkey("begin-mover")
    plan = _plan(queue, consumer, mover, _hexkey("begin-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    first = pg.reconcile(queue, TIER, unit, holder, 2, [mover], writer_is_me=True)
    assert first.state == "acquiring"
    assert first.census.h == 0 and first.census.p == 2
    second = pg.reconcile(queue, TIER, unit, holder, 2, [mover], writer_is_me=True)
    assert second.state == "acquiring"
    assert "prelaunch-acquisition-committed" in second.events
    third = pg.reconcile(queue, TIER, unit, holder, 2, [mover], writer_is_me=True)
    assert third.state == "reserved"
    assert third.authority is True
    assert third.census.h == 2
    assert (pg.group_dir(queue, unit, TIER) / "committed.json").exists()


def test_begin_decline_leaves_nothing_held(tmp_path) -> None:
    """A full tier declines the begin; the group stays unreserved."""
    queue = _queue(tmp_path, stage_gib=2)
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire("squatter", {"stage_gib": 2}) is True
    consumer, mover = _hexkey("decl-consumer"), _hexkey("decl-mover")
    plan = _plan(queue, consumer, mover, _hexkey("decl-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    outcome = pg.reconcile(queue, TIER, unit, holder, 2, [mover], writer_is_me=True)
    assert outcome.state == "unreserved"
    assert "prelaunch-begin-declined" in outcome.events
    assert outcome.census.h == 0 and outcome.census.p == 0


# --------------------------------------- crash point 2: 80 of 166 renames


def _split_80_of_166(queue, holder: str) -> str:
    """Begin 166, then move 80 tokens by hand; return the handle name."""
    ledger = queue.tier_ledger(TIER)
    handle = ledger.begin_acquire(holder, {"stage_gib": 166})
    assert handle is not None
    names = sorted(p.name for p in (ledger.held_dir / handle).iterdir())
    assert len(names) == 166
    _move_tokens(ledger.held_dir / handle, names[:80], ledger.held_dir / holder)
    return handle


def test_crash_at_80_of_166_rolls_the_rest_forward(tmp_path) -> None:
    """h=80 p=86 owns all 166; this writer commits the remainder."""
    queue = _queue(tmp_path, stage_gib=200)
    unit = _hexkey("split-consumer")
    holder = pg.holder_name(unit, TIER, PHASES)
    assert pg.file_intent(queue, unit, holder, TIER, 166, []) is True
    _split_80_of_166(queue, holder)
    found = pg.census(queue, TIER, unit, holder, 166, [])
    assert (found.h, found.p) == (80, 86)
    assert pg.incremental_need_gib(found, 166) == 0
    outcome = pg.reconcile(queue, TIER, unit, holder, 166, [], writer_is_me=True)
    assert outcome.state == "acquiring"
    assert "prelaunch-acquisition-committed" in outcome.events
    done = pg.reconcile(queue, TIER, unit, holder, 166, [], writer_is_me=True)
    assert done.census.h == 166
    assert done.state == "reserved" and done.authority is True


def test_crash_at_80_of_166_with_a_dotted_host_is_left_alone(tmp_path) -> None:
    """A dotted hostname parses whole; a foreign claimant keeps its tokens."""
    queue = _queue(tmp_path, stage_gib=200)
    ledger = queue.tier_ledger(TIER)
    unit = _hexkey("dotted-consumer")
    holder = pg.holder_name(unit, TIER, PHASES)
    assert pg.file_intent(queue, unit, holder, TIER, 166, []) is True
    handle = _split_80_of_166(queue, holder)
    parts = handle.split(".")
    dotted = ".".join([parts[0], parts[1], parts[2], "node.example.org",
                       parts[-2], parts[-1]])
    os.rename(ledger.held_dir / handle, ledger.held_dir / dotted)
    found = pg.census(queue, TIER, unit, holder, 166, [])
    assert (found.h, found.p) == (80, 86)
    assert found.handles[0][2] == "node.example.org"
    assert found.unparsed == []
    outcome = pg.reconcile(queue, TIER, unit, holder, 166, [], writer_is_me=True)
    assert outcome.state == "acquiring"
    assert "prelaunch-acquisition-in-flight" in outcome.events
    assert outcome.census.h == 80 and outcome.census.p == 86


def test_incremental_need_is_whole_before_begin(tmp_path) -> None:
    """Before begin the gate need is the whole demand."""
    queue = _queue(tmp_path, stage_gib=200)
    unit = _hexkey("need-consumer")
    holder = pg.holder_name(unit, TIER, PHASES)
    assert pg.file_intent(queue, unit, holder, TIER, 166, []) is True
    found = pg.census(queue, TIER, unit, holder, 166, [])
    assert pg.incremental_need_gib(found, 166) == 166


# --------------------------------- crash point 3: after the stale sweep


def test_stale_sweep_then_rollback_retries_from_intent(tmp_path) -> None:
    """A swept handle reads p=0 h=80; the pass rolls back, the next begins."""
    queue = _queue(tmp_path, stage_gib=200)
    ledger = queue.tier_ledger(TIER)
    unit = _hexkey("sweep-consumer")
    holder = pg.holder_name(unit, TIER, PHASES)
    assert pg.file_intent(queue, unit, holder, TIER, 166, []) is True
    handle = _split_80_of_166(queue, holder)
    parts = handle.split(".")
    old_usec = int(time.time() * 1_000_000) - 1_000_000_000
    dead = ".".join([parts[0], str(old_usec), parts[2],
                     socket.gethostname(), str(2 ** 30), parts[-1]])
    os.rename(ledger.held_dir / handle, ledger.held_dir / dead)
    assert dead in ledger.sweep_stale_acquisitions()
    outcome = pg.reconcile(queue, TIER, unit, holder, 166, [], writer_is_me=True)
    assert outcome.state == "unreserved"
    assert "prelaunch-group-rolled-back" in outcome.events
    assert ledger.available().get("stage_gib") == 200
    assert (pg.group_dir(queue, unit, TIER) / "rolled-back-0.json").exists()
    again = pg.reconcile(queue, TIER, unit, holder, 166, [], writer_is_me=True)
    assert again.state == "acquiring"
    assert "prelaunch-group-begun" in again.events


def test_live_foreign_handle_is_left_alone(tmp_path) -> None:
    """A recent foreign handle reports in-flight and moves nothing."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer, mover = _hexkey("frgn-consumer"), _hexkey("frgn-mover")
    plan = _plan(queue, consumer, mover, _hexkey("frgn-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    handle = ledger.begin_acquire(holder, {"stage_gib": 2})
    assert handle is not None
    parts = handle.split(".")
    foreign = ".".join([parts[0], parts[1], parts[2], "peer.example.org",
                        "42424", parts[-1]])
    os.rename(ledger.held_dir / handle, ledger.held_dir / foreign)
    outcome = pg.reconcile(queue, TIER, unit, holder, 2, [mover], writer_is_me=True)
    assert "prelaunch-acquisition-in-flight" in outcome.events
    assert outcome.census.h == 0 and outcome.census.p == 2
    assert outcome.authority is False


def test_reader_without_the_writer_role_changes_nothing(tmp_path) -> None:
    """writer_is_me=False observes; a live own handle still waits."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer, mover = _hexkey("ro-consumer"), _hexkey("ro-mover")
    plan = _plan(queue, consumer, mover, _hexkey("ro-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    handle = ledger.begin_acquire(holder, {"stage_gib": 2})
    assert handle is not None
    outcome = pg.reconcile(queue, TIER, unit, holder, 2, [mover], writer_is_me=False)
    assert "prelaunch-not-writer" in outcome.events
    assert outcome.census.h == 0 and outcome.census.p == 2
    assert ledger.commit_acquire(holder, handle) == 2


# ------------------------------------------------------- census exactness


def test_malformed_handle_is_unparsed_never_counted(tmp_path) -> None:
    """A bad nonce names nothing; the event says so and the row continues."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer, mover = _hexkey("unparsed-consumer"), _hexkey("unparsed-mover")
    plan = _plan(queue, consumer, mover, _hexkey("unparsed-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    handle = ledger.begin_acquire(holder, {"stage_gib": 2})
    assert handle is not None
    parts = handle.split(".")
    bad = ".".join([parts[0], parts[1], parts[2], "node01", parts[-2], "xyz"])
    os.rename(ledger.held_dir / handle, ledger.held_dir / bad)
    found = pg.census(queue, TIER, unit, holder, 2, [mover])
    assert (found.h, found.p) == (0, 0)
    assert found.unparsed == [bad]
    outcome = pg.reconcile(queue, TIER, unit, holder, 2, [mover], writer_is_me=True)
    assert "prelaunch-handle-unparsed" in outcome.events


def test_another_holders_handle_belongs_to_neither_count(tmp_path) -> None:
    """Exact holder equality: a sibling group reads h=0 p=0."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer, mover = _hexkey("sib-consumer"), _hexkey("sib-mover")
    plan = _plan(queue, consumer, mover, _hexkey("sib-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    other = pg.holder_name(_hexkey("sibling"), TIER, PHASES)
    handle = ledger.begin_acquire(other, {"stage_gib": 2})
    assert handle is not None
    try:
        found = pg.census(queue, TIER, unit, holder, 2, [mover])
        assert (found.h, found.p) == (0, 0)
        assert found.unparsed == []
    finally:
        assert ledger.abandon_acquire(handle) == 2


# ------------------------------------------------------------- fail closed


def test_committed_with_a_missing_token_fails_closed(tmp_path) -> None:
    """h+m+r below demand releases the remainder and grants no authority."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer, mover = _hexkey("short-consumer"), _hexkey("short-mover")
    plan = _plan(queue, consumer, mover, _hexkey("short-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    _drive_to_committed(queue, TIER, unit, holder, 2, [mover])
    assert ledger.transfer_count(holder, "squatter", 1) == 1
    outcome = pg.reconcile(queue, TIER, unit, holder, 2, [mover], writer_is_me=True)
    assert outcome.state == "short"
    assert "prelaunch-group-short" in outcome.events
    assert outcome.authority is False
    assert ledger.holder_tokens(holder).get("stage_gib", 0) == 0
    assert (pg.group_dir(queue, unit, TIER) / "committed.json").exists()


def test_committed_with_an_extra_token_fails_closed(tmp_path) -> None:
    """h+m+r above demand is overfull, not authority."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer, mover = _hexkey("over-consumer"), _hexkey("over-mover")
    plan = _plan(queue, consumer, mover, _hexkey("over-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    _drive_to_committed(queue, TIER, unit, holder, 2, [mover])
    forged = ledger.held_dir / holder / "stage_gib-999999"
    assert not forged.exists()
    forged.write_text("forged", encoding="utf-8")
    outcome = pg.reconcile(queue, TIER, unit, holder, 2, [mover], writer_is_me=True)
    assert outcome.state == "overfull"
    assert "prelaunch-group-overfull" in outcome.events
    assert outcome.authority is False


# -------------------------------------------- split to movers and funding


def test_split_to_movers_keeps_the_sum(tmp_path) -> None:
    """Per-token renames never touch free; h+m+r stays the demand."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer = _hexkey("split-consumer")
    first, second = _hexkey("split-m1"), _hexkey("split-m2")
    plan = _plan(queue, consumer, first, second)
    unit, holder = _group(queue, plan, [first, second], demand=4)
    _drive_to_committed(queue, TIER, unit, holder, 4, [first, second])
    row_a = _publish(queue, plan, first)
    out_a = pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, first),
                             float(row_a["published_unix"]))
    assert out_a.status == "published" and out_a.moved == 2
    free_mid = ledger.available().get("stage_gib")
    row_b = _publish(queue, plan, second)
    out_b = pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, second),
                             float(row_b["published_unix"]))
    assert out_b.status == "published" and out_b.moved == 2
    assert ledger.available().get("stage_gib") == free_mid == 4
    found = pg.census(queue, TIER, unit, holder, 4, [first, second])
    assert (found.h, found.m, found.r) == (0, 4, 0)
    outcome = pg.reconcile(queue, TIER, unit, holder, 4, [first, second],
                           writer_is_me=True)
    assert outcome.state == "done" and outcome.authority is True


def test_publish_repeats_idempotently(tmp_path) -> None:
    """A second publish of one chunk moves nothing and keeps its generation."""
    queue = _queue(tmp_path, stage_gib=8)
    consumer, mover = _hexkey("idem-consumer"), _hexkey("idem-mover")
    plan = _plan(queue, consumer, mover, _hexkey("idem-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    _drive_to_committed(queue, TIER, unit, holder, 2, [mover])
    row = _publish(queue, plan, mover)
    first = pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, mover),
                             float(row["published_unix"]))
    assert first.status == "published"
    second = pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, mover),
                              float(row["published_unix"]))
    assert second.status == "already"
    assert second.generation == first.generation
    assert second.moved == 0


def test_publish_refuses_without_a_mover_row(tmp_path) -> None:
    """No queue row means no transfer; the fence guard says the same."""
    queue = _queue(tmp_path, stage_gib=8)
    consumer, mover = _hexkey("norow-consumer"), _hexkey("norow-mover")
    plan = _plan(queue, consumer, mover, _hexkey("norow-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    _drive_to_committed(queue, TIER, unit, holder, 2, [mover])
    residency_plan.freeze(queue, plan)
    row = {"published_unix": 1.0}
    outcome = pg.publish_chunk(queue, TIER, unit, holder, plan,
                               _leg(plan, mover), float(row["published_unix"]))
    assert outcome.status == "refused"
    assert "prelaunch-mover-unpublished" in outcome.events


def test_republished_mover_rotates_to_a_fresh_generation(tmp_path) -> None:
    """A new publication never inherits an older generation's credit."""
    queue = _queue(tmp_path, stage_gib=8)
    consumer, mover = _hexkey("repub-consumer"), _hexkey("repub-mover")
    plan = _plan(queue, consumer, mover, _hexkey("repub-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    _drive_to_committed(queue, TIER, unit, holder, 2, [mover])
    row = _publish(queue, plan, mover)
    first = pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, mover),
                             float(row["published_unix"]))
    assert first.status == "published"
    republished = dict(row, published_unix=float(row["published_unix"]) + 1.0)
    queue.item_path(pool.READY, mover).write_text(
        json.dumps(republished), encoding="utf-8")
    second = pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, mover),
                              float(republished["published_unix"]))
    assert second.status == "published"
    assert second.generation != first.generation
    assert queue.funded_cover(TIER, republished, "stage_gib", 2)[1] == second.generation


def test_mover_claim_uses_funding_without_a_second_charge(tmp_path) -> None:
    """The unchanged claim path spends the group fence and takes no free."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer = _hexkey("claim-consumer")
    first, second = _hexkey("claim-m1"), _hexkey("claim-m2")
    plan = _plan(queue, consumer, first, second)
    unit, holder = _group(queue, plan, [first, second], demand=4)
    _drive_to_committed(queue, TIER, unit, holder, 4, [first, second])
    row = _publish(queue, plan, first)
    published = pg.publish_chunk(queue, TIER, unit, holder, plan,
                                 _leg(plan, first), float(row["published_unix"]))
    assert published.status == "published"
    assert queue.funded_cover(TIER, row, "stage_gib", 2) == (2, published.generation)
    free_before = ledger.available().get("stage_gib")
    got = queue.claim(tags=["dl380g10"], owner="w-mover")
    assert got is not None and got["action_key"] == first
    assert ledger.available().get("stage_gib") == free_before
    assert int(ledger.holder_tokens(first).get("stage_gib", 0)) == 2
    record = queue.read_funding(first, TIER)
    assert record is not None and record["state"] == "consumed"
    assert str(record["generation"]) == published.generation


# ---------------------------------------------------------------- release


def test_release_at_terminal_with_an_unconsumed_chunk(tmp_path) -> None:
    """Remainder and unfunded mover tokens return; the receipt counts them."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer, mover = _hexkey("rel-consumer"), _hexkey("rel-mover")
    plan = _plan(queue, consumer, mover, _hexkey("rel-other"))
    unit, holder = _group(queue, plan, [mover], demand=4)
    _drive_to_committed(queue, TIER, unit, holder, 4, [mover])
    row = _publish(queue, plan, mover)
    published = pg.publish_chunk(queue, TIER, unit, holder, plan,
                                 _leg(plan, mover), float(row["published_unix"]))
    assert published.status == "published"
    assert pg.release_unit(queue, TIER, unit, holder, [mover],
                           terminal=True) == ["prelaunch-group-released"]
    assert ledger.available().get("stage_gib") == 8
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "released"
    released = json.loads((pg.group_dir(queue, unit, TIER) / "released.json").read_text())
    assert released["holder_released"] == 2
    assert released["chunks"] == {mover: 2}
    found = pg.census(queue, TIER, unit, holder, 4, [mover])
    assert (found.h, found.m, found.r) == (0, 0, 4)


def test_release_at_terminal_keeps_a_copying_chunk(tmp_path) -> None:
    """A claimed mover keeps its charge until its own stop or terminal."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer = _hexkey("copy-consumer")
    first, second = _hexkey("copy-m1"), _hexkey("copy-m2")
    plan = _plan(queue, consumer, first, second)
    unit, holder = _group(queue, plan, [first, second], demand=4)
    _drive_to_committed(queue, TIER, unit, holder, 4, [first, second])
    row_a = _publish(queue, plan, first)
    assert pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, first),
                            float(row_a["published_unix"])).status == "published"
    got = queue.claim(tags=["dl380g10"], owner="w-copy")
    assert got is not None and got["action_key"] == first
    row_b = _publish(queue, plan, second)
    assert pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, second),
                            float(row_b["published_unix"])).status == "published"
    assert pg.release_unit(queue, TIER, unit, holder, [first, second],
                           terminal=True) == ["prelaunch-group-released"]
    assert int(ledger.holder_tokens(first).get("stage_gib", 0)) == 2
    record = queue.read_funding(first, TIER)
    assert record is not None and record["state"] == "consumed"
    assert int(ledger.holder_tokens(second).get("stage_gib", 0)) == 0
    assert ledger.available().get("stage_gib") == 6


def test_release_leaves_a_shared_mover_alone(tmp_path) -> None:
    """A co-owned mover keeps its fence past this consumer's end."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer, mover = _hexkey("shr-consumer"), _hexkey("shr-mover")
    plan = _plan(queue, consumer, mover, _hexkey("shr-other"))
    unit, holder = _group(queue, plan, [mover], demand=4)
    _drive_to_committed(queue, TIER, unit, holder, 4, [mover])
    row = _publish(queue, plan, mover)
    assert pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, mover),
                            float(row["published_unix"])).status == "published"
    assert pg.release_unit(queue, TIER, unit, holder, [mover], terminal=True,
                           shared_owned={mover}) == ["prelaunch-group-released"]
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "transferring"
    assert ledger.available().get("stage_gib") == 6


def test_release_without_terminal_changes_nothing(tmp_path) -> None:
    """A live unit keeps its group; cleanup acts only at the end."""
    queue = _queue(tmp_path, stage_gib=8)
    ledger = queue.tier_ledger(TIER)
    consumer, mover = _hexkey("live-consumer"), _hexkey("live-mover")
    plan = _plan(queue, consumer, mover, _hexkey("live-other"))
    unit, holder = _group(queue, plan, [mover], demand=2)
    _drive_to_committed(queue, TIER, unit, holder, 2, [mover])
    assert pg.release_unit(queue, TIER, unit, holder, [mover],
                           terminal=False) == []
    assert int(ledger.holder_tokens(holder).get("stage_gib", 0)) == 2
