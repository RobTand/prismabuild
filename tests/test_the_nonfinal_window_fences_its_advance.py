"""#832: a nonfinal advance_needs result must carry the fence it computed.

``advance_needs`` computes ``fence_target``/``fence_prior`` from the frontier
(the earliest unstaged ahead leg) for every plan, but returned them only on
its no-waiting branch.  A window with any unpublished ahead leg -- the
ordinary rolling case -- answered without the fields, and
``tier_loop._protect_tier_advances`` reads a missing ``fence_target`` as
"final: explicitly needs no advance" and permits publication.  The current
was then exposed without the advance reservation the current-plus-next window
is supposed to guarantee; the initial fit check alone cannot keep that room
against a competing claim between publish and bind.

These tests drive the real production paths on tiny 1 MiB fixtures.  They
assert ledger and funding-record facts -- the advance's room is blind-held
under its grant and then transferred/bound to its mover's row -- not merely
that a dictionary key exists:

* a nonfinal stage window binds the frontier advance before its cycle ends and
  never double-holds it over further cycles;
* an adopted-ahead range at the head moves the frontier and the fence follows
  it to the next unpublished advance, while the adopted pin is untouched;
* a final window still needs no advance fence (the negative half: the fix
  must not invent one);
* the ram publication path holds and binds its own advance the same way;
* a blind grant for an unpublished target permits the window and binds when
  the row appears, without ever gating the publication the bind waits for;
* a *partial* blind grant permits nothing: insufficient capacity stays held
  and fails closed rather than exposing a current on a half reservation.

The seam values themselves (frontier leg, prior legs, chunked targets) are
pinned by ``test_advance_needs_names_its_fence_target.py``.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
from prismabuild import window_credit  # noqa: E402
import tier_loop  # noqa: E402
import residency_publication  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
RAM_KIND = f"ram_gib@{RAM_TIER}"
GIB = storage_tiers.GIB
SPAN = 1 << 20

CONSUMER = "c" * 64


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int],
         queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _queue(tmp_path: Path, *, stage_gib: int, ram_gib: int = 0) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.ledger().ensure_capacity({"cpu": 4, "mem_gb": 8})
    queue.mint_tier_capacity(TIER, {"stage_gib": stage_gib})
    if ram_gib:
        queue.mint_tier_capacity(RAM_TIER, {"ram_gib": ram_gib})
    return queue


def _tiers(tmp_path: Path) -> dict[str, dict[str, object]]:
    return {TIER: {"tier_id": TIER, "tier": "stage",
                   "mountpoint": str(tmp_path / "stage")}}


def _ram_tiers(tmp_path: Path) -> dict[str, dict[str, object]]:
    record = storage_tiers.read_ram_epoch(tmp_path / "ram")
    return {**_tiers(tmp_path), RAM_TIER: {
        "tier_id": RAM_TIER, "tier": "ram",
        "mountpoint": str(tmp_path / "ram"),
        "epoch": str(record["epoch"]) if record else ""}}


def _plan(queue: pool.PoolQueue, phases: int, *, ram: bool = False,
          digest: str = "8" * 64, consumer: str = CONSUMER,
          per_phase: int = 1, span: int = SPAN) -> dict[str, object]:
    built = []
    for ordinal in range(phases):
        start, end = ordinal * span, (ordinal + 1) * span
        entry: dict[str, object] = {
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": per_phase,
            "mover_row": {
                **_row(_hexkey(f"nf-m{ordinal}"),
                       {STAGE_KIND: per_phase, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": digest, "manifest_bytes": phases * span,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(_hexkey(f"nf-e{ordinal}"), {"mem_gb": 1},
                               queue),
        }
        if ram:
            entry["ram_mover_row"] = {
                **_row(_hexkey(f"nf-r{ordinal}"),
                       {RAM_KIND: per_phase, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                    "manifest_sha256": digest, "manifest_bytes": phases * span,
                    "range_start_bytes": start, "range_end_bytes": end},
            }
            entry["ram_egress_row"] = _row(_hexkey(f"nf-re{ordinal}"),
                                           {"mem_gb": 1}, queue)
        built.append(entry)
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER,
        stage_root="/stage/prewarm", manifest_sha256=digest,
        manifest_bytes=phases * span, phases=built,
        **({"ram_tier_id": RAM_TIER} if ram else {}))


def _publish_consumer(queue: pool.PoolQueue, plan: dict[str, object],
                      consumer: str) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": str(plan["manifest_sha256"]),
                   "manifest_bytes": int(plan["manifest_bytes"]),
                   "leads": residency_plan.leads_for(plan)})


def _movers(plan: dict[str, object], role: str = "mover_row") -> list[str]:
    phases = plan["phases"]
    assert isinstance(phases, list)
    return [str(phase[role]["action_key"]) for phase in phases]  # type: ignore[index]


def _published(events: list[dict[str, object]], event: str) -> set[str]:
    return {str(entry["action_key"]) for entry in events
            if entry.get("event") == event}


def _assert_one_live_advance(queue: pool.PoolQueue, tier_id: str, kind: str,
                             consumer: str, plan: dict[str, object],
                             mover: str, phase: str, start: int,
                             end: int) -> None:
    """Exactly one fence, bound to the mover's own publication.

    One record, naming this consumer, plan, publication, range and generation;
    its demand is held (under the mover once handed off) and no grant holder
    keeps a second copy of the same room.
    """

    record = queue.read_funding(mover, tier_id)
    assert isinstance(record, dict), f"no advance fence bound to {mover[:12]}"
    assert record.get("state") in ("reserved", "transferring"), record
    assert str(record.get("consumer_action_key")) == consumer, record
    assert str(record.get("plan_sha256")) == \
        residency_plan.plan_sha256(plan), record
    assert str(record.get("mover_action_key")) == mover, record
    assert int(record.get("range_start_bytes")) == start, record
    assert int(record.get("range_end_bytes")) == end, record
    assert isinstance(record.get("generation"), str) and record["generation"]
    tokens = record.get("tokens")
    held = {path.name for path in
            (queue.tier_ledger(tier_id).held_dir / mover).glob("*-*")}
    assert isinstance(tokens, list) and tokens, record
    assert all(str(name) in held for name in tokens), (tokens, sorted(held))
    assert window_credit.held_grants(queue.tier_ledger(tier_id)) == [], \
        "the fence moved under the mover; no grant copy may remain"


def test_a_nonfinal_stage_window_fences_its_advance_before_its_cycle_ends(
        tmp_path: Path) -> None:
    """Three phases, run-ahead bound one: the published advance is protected."""

    queue = _queue(tmp_path, stage_gib=3)
    plan = _plan(queue, 3)
    _publish_consumer(queue, plan, CONSUMER)
    m0, m1, m2 = _movers(plan)
    ledger = queue.tier_ledger(TIER)

    events = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    published = _published(events, "mover-published")
    # The window is genuinely nonfinal: the current and its advance publish,
    # the run-ahead bound holds the third leg back.
    assert m0 in published and m1 in published, events
    assert m2 not in published, events
    assert any(entry.get("event") == "window-stalled" for entry in events), events

    # The advance's room is real *before* the current is exposed: one record,
    # bound to the advance's own queued row.
    _assert_one_live_advance(queue, TIER, "stage_gib", CONSUMER, plan, m1,
                             "phase-1", SPAN, 2 * SPAN)
    assert int(ledger.holder_tokens(m1).get("stage_gib", 0)) == 1
    assert ledger.available().get("stage_gib") == 2

    # One fence per consumer/tier/leg: further cycles re-prove the same
    # reservation instead of taking a second one beside it.
    tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    assert int(ledger.holder_tokens(m1).get("stage_gib", 0)) == 1
    assert ledger.available().get("stage_gib") == 2
    assert window_credit.held_grants(ledger) == []

    # It is a real funded claim, not merely a queued row: claiming the
    # advance consumes its own fence, so no new room leaves free.
    before = ledger.available().get("stage_gib")
    ready = [item for item in queue.ready_items()
             if str(item.get("action_key")) == m1]
    got = queue.claim(tags=["dl380g10"], owner="w-832", ready=ready)
    assert got is not None and got["action_key"] == m1
    assert (queue.read_funding(m1, TIER) or {}).get("state") == "consumed"
    assert int(ledger.holder_tokens(m1).get("stage_gib", 0)) == 1
    assert ledger.available().get("stage_gib") == before


def _claim_with_progress(queue: pool.PoolQueue, consumer: str, *,
                         phase: str) -> None:
    """Give a consumer an accepted phase, claiming it first when needed.

    The lease is what the storage role trusts (``prewarm_loop.progress_phase``
    checks the claim stamp against it), written the way
    ``tests/prewarm_fixture.py`` writes it.  A later phase on an already
    claimed consumer refreshes the same lease, as the consumer reading the
    next phase would.
    """

    ready = queue.item_path(pool.READY, consumer)
    claimed = queue.item_path(pool.CLAIMED, consumer)
    if ready.exists():
        item = json.loads(ready.read_text())
        ready.unlink()
        item.update({"action_key": consumer, "claimed_unix": time.time(),
                     "claimed_by": "nonfinal-fence-fixture",
                     "claimed_host": "dl380g10"})
        claimed.write_text(json.dumps(item))
    else:
        item = json.loads(claimed.read_text())
    queue.write_lease(
        consumer, owner="nonfinal-fence-fixture", claim_snapshot=item,
        progress_observation={
            "source": "action-progress",
            "last_accepted": {"phase": phase, "units_completed": 1,
                              "reported_unix": time.time()}})


def test_an_adopted_head_moves_the_fence_to_the_next_unpublished_advance(
        tmp_path: Path) -> None:
    """A resident head is the frontier's predecessor: the fence follows it.

    The adopted-ahead range keeps its own pin (adoption transfers the donor's
    reservation), so the frontier is the first unpublished leg and the fence
    must protect the leg after that, not the adopted one and not the leg the
    same cycle publishes.  The consumer's accepted progress makes the run-ahead
    bound cover the advance's row, which is the state in which the bind can
    complete.
    """

    queue = _queue(tmp_path, stage_gib=3)
    plan = _plan(queue, 4)
    _publish_consumer(queue, plan, CONSUMER)
    ledger = queue.tier_ledger(TIER)
    m0, m1, m2, m3 = _movers(plan)

    # The adopted-ahead range: the head's reservation is already under its own
    # key (adoption transfers the donor's pin) with no queue row.
    assert ledger.acquire(m0, {"stage_gib": 1}) is True
    _claim_with_progress(queue, CONSUMER, phase="phase-0")
    published_state, staged_state = tier_loop._mover_state(queue, plan, TIER)
    assert m0 in published_state and m0 in staged_state

    events = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    published = _published(events, "mover-published")
    assert m1 in published and m2 in published, events
    assert m3 not in published, events   # run-ahead bound holds the fourth
    assert [entry for entry in events
            if entry.get("event") == "window-gated"] == [], events

    # The fence protects the first unpublished advance (phase 2), not the
    # adopted range and not the leg already published.
    _assert_one_live_advance(queue, TIER, "stage_gib", CONSUMER, plan, m2,
                             "phase-2", 2 * SPAN, 3 * SPAN)
    assert int(ledger.holder_tokens(m0).get("stage_gib", 0)) == 1
    assert int(ledger.holder_tokens(m2).get("stage_gib", 0)) == 1
    assert ledger.available().get("stage_gib") == 1


def _drive_adopted_head_until_the_advance_publishes(
        queue: pool.PoolQueue, plan: dict[str, object], tmp_path: Path,
        head: str, successor: str, advance: str) -> dict[str, object]:
    """Run the adopted-head window to the cycle its advance reaches ready.

    Cycle one publishes the successor while the run-ahead bound withholds the
    advance; the successor then lands and the consumer accepts progress, so
    the bound covers the advance.  Returns the observed publication set, the
    number of cycles that gated the consumer, and the events themselves.
    """

    ledger = queue.tier_ledger(TIER)
    tiers = _tiers(tmp_path)
    assert ledger.acquire(head, {"stage_gib": 1}) is True   # adopted head

    first = tier_loop.residency_window(queue, tiers=tiers)
    assert successor in _published(first, "mover-published"), first
    assert advance not in _published(first, "mover-published"), first

    assert ledger.acquire(successor, {"stage_gib": 1}) is True
    queue.item_path(pool.READY, successor).unlink()
    _claim_with_progress(queue, CONSUMER, phase="phase-0")

    gated_cycles = 0
    published: set[str] = set()
    cycles: list[list[dict[str, object]]] = []
    for _ in range(4):
        events = tier_loop.residency_window(queue, tiers=tiers)
        cycles.append(events)
        if [entry for entry in events
                if entry.get("event") == "window-gated"]:
            gated_cycles += 1
        published |= _published(events, "mover-published")
        if advance in published:
            break
    return {"published": published, "gated_cycles": gated_cycles,
            "cycles": cycles}


def test_a_blind_advance_does_not_gate_the_row_that_would_bind_it(
        tmp_path: Path) -> None:
    """The caller must not demand a row before publishing it.

    An adopted head leaves the frontier at the first unpublished leg and the
    fence on the leg after it; the run-ahead bound cannot publish that target
    in the same cycle, so its grant is blind-held with no queued row.  The
    window must keep publishing the frontier as room and accepted progress
    allow -- a bind that waits for the row must never be allowed to withhold
    the row forever.  This is the shape the return-only patch wedged: the
    held grant, an unpublished target, the always-failing bind, and the
    consumer gate that followed it.
    """

    queue = _queue(tmp_path, stage_gib=3)
    plan = _plan(queue, 4)
    _publish_consumer(queue, plan, CONSUMER)
    ledger = queue.tier_ledger(TIER)
    m0, m1, m2, m3 = _movers(plan)

    outcome = _drive_adopted_head_until_the_advance_publishes(
        queue, plan, tmp_path, m0, m1, m2)
    assert m2 in outcome["published"], {
        "published": sorted(outcome["published"]),
        "gated_cycles": outcome["gated_cycles"]}
    # The residual is bounded and loud: at most one cycle can be gated while
    # a superseded target's grant is released after the want pass.  The cycle
    # that publishes the advance is never the gated one -- no wedge.
    assert outcome["gated_cycles"] <= 1, outcome["gated_cycles"]
    assert not [entry for entry in outcome["cycles"][-1]
                if entry.get("event") == "window-gated"], outcome

    # The consumer reads on into phase-1, so the run-ahead bound reaches the
    # last leg.  The window publishes it too -- deferred binds never strand
    # the grant or block the window -- and leaves no grant behind.
    _claim_with_progress(queue, CONSUMER, phase="phase-1")
    later: set[str] = set()
    for _ in range(4):
        events = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
        later |= _published(events, "mover-published")
        if m3 in later:
            break
    assert m3 in later, {"published": sorted(later)}
    assert window_credit.held_grants(ledger) == []


def test_a_deferred_blind_grant_binds_when_its_row_publishes(
        tmp_path: Path) -> None:
    """The deferred bind completes exactly when the advance's row appears.

    The same adopted-head shape: the grant is blind-held while its target is
    unpublished, then the target publishes and the pass that sees its row
    binds the fence to it -- one record, this consumer, this plan, this
    phase's range, and no second copy under the grant.  The bind is deferred,
    never skipped and never doubled.
    """

    queue = _queue(tmp_path, stage_gib=3)
    plan = _plan(queue, 4)
    _publish_consumer(queue, plan, CONSUMER)
    m0, m1, m2, m3 = _movers(plan)

    outcome = _drive_adopted_head_until_the_advance_publishes(
        queue, plan, tmp_path, m0, m1, m2)
    assert m2 in outcome["published"], outcome
    _claim_with_progress(queue, CONSUMER, phase="phase-1")
    later: set[str] = set()
    for _ in range(4):
        events = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
        later |= _published(events, "mover-published")
        if m3 in later:
            break
    assert m3 in later, {"published": sorted(later)}
    _assert_one_live_advance(queue, TIER, "stage_gib", CONSUMER, plan, m3,
                             "phase-3", 3 * SPAN, 4 * SPAN)


def test_a_partial_blind_grant_permits_nothing(tmp_path: Path) -> None:
    """Insufficient capacity is not the advance's reservation.

    A blind grant holding less than the target's demand -- a surviving
    partial take, a split transfer's remainder -- must not stand in for the
    full reservation.  The window stays gated, the partial tokens stay held,
    and no mover publishes; fail-closed, never a current beside a half
    reservation.  This is the root-QA regression: permitting on any held
    grant let a 1 GiB grant fund a 2 GiB advance.
    """

    queue = _queue(tmp_path, stage_gib=5)
    plan = _plan(queue, 4, per_phase=2, span=2 * GIB)
    _publish_consumer(queue, plan, CONSUMER)
    ledger = queue.tier_ledger(TIER)
    m0, m1, m2, m3 = _movers(plan)
    grant = window_credit.grant_key(CONSUMER, TIER, "mover_row", "phase-1")
    assert ledger.acquire(grant, {"stage_gib": 1}) is True   # 1 of 2 GiB

    events = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))

    assert not _published(events, "mover-published"), events
    gated = [entry for entry in events if entry.get("event") == "window-gated"]
    assert gated and gated[0]["reason"] == "joint-fit-stall", events
    assert int(ledger.holder_tokens(grant).get("stage_gib", 0)) == 1
    assert int(ledger.holder_tokens(m1).get("stage_gib", 0)) == 0
    assert queue.read_funding(m1, TIER) is None
    assert [key for key in (m0, m1, m2, m3)
            if queue.item_path(pool.READY, key).exists()] == []


def test_a_final_window_still_needs_no_advance_fence(tmp_path: Path) -> None:
    """The negative half: a last leg publishes with no invented reservation."""

    queue = _queue(tmp_path, stage_gib=3)
    plan = _plan(queue, 2)
    _publish_consumer(queue, plan, CONSUMER)
    ledger = queue.tier_ledger(TIER)
    m0, m1 = _movers(plan)

    assert ledger.acquire(m0, {"stage_gib": 1}) is True

    events = tier_loop.residency_window(queue, tiers=_tiers(tmp_path))
    assert m1 in _published(events, "mover-published"), events
    assert [entry for entry in events
            if entry.get("event") == "window-gated"] == [], events

    assert queue.read_funding(m1, TIER) is None
    assert int(ledger.holder_tokens(m1).get("stage_gib", 0)) == 0
    assert window_credit.held_grants(ledger) == []
    assert ledger.available().get("stage_gib") == 2


def test_the_ram_window_fences_its_advance_before_the_promotion_runs(
        tmp_path: Path) -> None:
    """The ram publication path holds and binds its own advance the same way."""

    (tmp_path / "ram").mkdir()
    queue = _queue(tmp_path, stage_gib=3, ram_gib=3)
    epoch = storage_tiers.ensure_ram_epoch(tmp_path / "ram", host="dl380g10")
    assert epoch is not None
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "ram",
        "tier_id": RAM_TIER, "host": "dl380g10",
        "mountpoint": str(tmp_path / "ram"), "epoch": str(epoch["epoch"]),
        "capacity_bytes": 3 * GIB})
    plan = _plan(queue, 3, ram=True)
    _publish_consumer(queue, plan, CONSUMER)
    r0, r1, r2 = _movers(plan, "ram_mover_row")
    stage_ledger = queue.tier_ledger(TIER)

    # Two stage sources have landed (the fixture files what a finished copy
    # leaves: fragment plus complete receipt, with the booking still held).
    for ordinal, mover in ((0, _movers(plan)[0]), (1, _movers(plan)[1])):
        assert stage_ledger.acquire(mover, {"stage_gib": 1}) is True
        residency_publication.vouch_landed(
            queue, consumer_action_key=CONSUMER, mover_action_key=mover,
            tier_id=TIER, stage_root="/stage/prewarm",
            manifest_sha256=str(plan["manifest_sha256"]),
            range_start_bytes=ordinal * SPAN,
            range_end_bytes=(ordinal + 1) * SPAN)

    events = tier_loop.ram_residency_window(queue, tiers=_ram_tiers(tmp_path))
    published = _published(events, "ram-mover-published")
    assert r0 in published and r1 in published, events
    assert r2 not in published, events
    assert [entry for entry in events
            if entry.get("event") == "ram-window-gated"] == [], events

    # The ram advance's room is held under the promotion's own row.
    _assert_one_live_advance(queue, RAM_TIER, "ram_gib", CONSUMER, plan, r1,
                             "phase-1", SPAN, 2 * SPAN)
    ram_ledger = queue.tier_ledger(RAM_TIER)
    assert int(ram_ledger.holder_tokens(r1).get("ram_gib", 0)) == 1
    assert ram_ledger.available().get("ram_gib") == 2
    # The stage pins the promotions read from are untouched by the fence.
    assert int(stage_ledger.holder_tokens(_movers(plan)[0]).get(
        "stage_gib", 0)) == 1

    tier_loop.ram_residency_window(queue, tiers=_ram_tiers(tmp_path))
    assert int(ram_ledger.holder_tokens(r1).get("ram_gib", 0)) == 1
    assert ram_ledger.available().get("ram_gib") == 2
    assert window_credit.held_grants(ram_ledger) == []
