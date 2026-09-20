"""A withdrawn mover must not be republished at the price it was withdrawn for.

Observed 2026-09-19 during the stage A fill-capacity wedge: seven stale-priced
movers were withdrawn with ``pbrun --withdraw``, and their residency plans were
left standing.  The next ``tier_loop`` cycle read each frozen plan, found the
mover was neither queued nor pinned, and republished the *same* key with its
composition-time resources: fill ``259`` against a tier whose measured capacity
was ``65.7`` -- the exact never-fits state the withdrawal was meant to break
(#708).  Thirty-four dead plans had accumulated under
``pb-queue/residency-plans/`` and needed hand reaping.  (The ``released 0
token(s)`` the operator saw is not "nothing was held": a ready row has no
reservation yet, and a stopped claim returns its own once its worker's cleanup
proves safe.)

The rules this file pins:

* a withdrawal marks the plan that minted the withdrawn action superseded --
  by the plan's own identity, so a stale marker never covers a replacement --
  and stops all publication from it, at any price;
* the body stays filed while anything still names it: a withdrawn consumer's
  queued children are still cancellable, and a running consumer's other
  resident ranges stay named for the orphan sweep and adoption;
* the body is archived only once :func:`residency_plan.handoff_safe` says no
  consumer and no queued or claimed child still refers to it, so the residue
  is evidence with a name rather than litter;
* no token is released by retirement, and successful adopted work is left for
  adoption;
* a mover's sealed row is never rewritten (an action key is the hash of its
  sealed resources and argv, #710); a deliberate resubmission is what seals
  fresh work, and the planner-level proof lives in
  ``test_pbrun_residency_stage_submission.py``.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
FILL_KIND = f"fill_mb_s_pool_side@{TIER}"
MANIFEST = "9" * 64
GIB = storage_tiers.GIB

FIRST = "1" * 64
SECOND = "2" * 64


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(queue: pool.PoolQueue, key: str,
         resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, *, label: str,
          gib: tuple[int, ...] = (2, 2), fill: int = 259) -> dict[str, object]:
    built = []
    start = 0
    for ordinal, size in enumerate(gib):
        end = start + size * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end,
            "stage_gib": size,
            "mover_row": {
                **_row(queue, _hexkey(f"{label}mover{ordinal}"),
                       {STAGE_KIND: size, "cpu": 2, "mem_gb": 1,
                        FILL_KIND: fill}),
                # As ``pbrun --residency stage`` seals it: the queue row, not
                # only the action body, because the pin is read off the record.
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(queue, _hexkey(f"{label}egress{ordinal}"),
                               {"cpu": 1, "mem_gb": 1}),
        })
        start = end
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


def _publish_consumer(queue: pool.PoolQueue, consumer: str,
                      plan: dict[str, object]) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                   "leads": residency_plan.leads_for(plan)})


def _claim(queue: pool.PoolQueue, key: str) -> None:
    source = queue.item_path(pool.READY, key)
    item = json.loads(source.read_text())
    source.unlink()
    item.update({"action_key": key, "claimed_unix": 1000.0,
                 "claimed_by": "worker", "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, key).write_text(json.dumps(item))


def _fail_consumer(queue: pool.PoolQueue, consumer: str) -> None:
    source = queue.item_path(pool.READY, consumer)
    record = json.loads(source.read_text())
    record["status"] = "failed"
    queue.item_path(pool.FAILED, consumer).write_text(json.dumps(record))
    source.unlink()


def _tier(tmp_path: Path) -> dict[str, object]:
    return {"tier_id": TIER, "tier": "stage",
            "mountpoint": str(tmp_path / "stage")}


def _artifacts(queue: pool.PoolQueue, consumer: str) -> list[Path]:
    """Everything filed under ``superseded/`` for one consumer."""

    directory = queue.residency_plan_path(consumer).parent / residency_plan.SUPERSEDED
    if not directory.is_dir():
        return []
    return sorted(path for path in directory.iterdir()
                  if path.name.startswith(f"{consumer}."))


def _markers(queue: pool.PoolQueue, consumer: str) -> list[Path]:
    """The *active* retirement markers: one plan filing each."""

    return [path for path in _artifacts(queue, consumer)
            if path.name.endswith(".superseded.json")]


def _marker_evidence(queue: pool.PoolQueue, consumer: str) -> list[Path]:
    """Retired markers kept beside the archived body they named."""

    return [path for path in _artifacts(queue, consumer)
            if path.name.endswith(".marker.json")]


def _archives(queue: pool.PoolQueue, consumer: str) -> list[Path]:
    return [path for path in _artifacts(queue, consumer)
            if not path.name.endswith(".superseded.json")
            and not path.name.endswith(".marker.json")]


def _published(events: list[dict[str, object]]) -> list[str]:
    return [str(event.get("action_key")) for event in events
            if event.get("event") == "mover-published"]


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": 8})
    return q


# -- a withdrawn mover is not republished ------------------------------------


def test_a_withdrawn_ready_mover_is_not_republished_and_its_plan_is_superseded(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """RED before #708: the window republished the same key at the same price."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    lead = _hexkey("firstmover0")
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    queue.withdraw(lead, reason="stale price", by="operator")

    events = tier_loop.residency_window(queue, tiers={TIER: _tier(tmp_path)})

    assert not queue.item_path(pool.READY, lead).exists()
    assert lead not in _published(events)
    assert any(event.get("event") == "residency-plan-superseded"
               and event.get("consumer") == FIRST for event in events)
    # The body stays filed: a running consumer's other ranges are still named.
    assert residency_plan.read(queue, FIRST) is not None
    assert residency_plan.superseded(queue, plan) is not None
    assert _markers(queue, FIRST)
    # ...and it is not archived while the consumer still runs.
    assert _archives(queue, FIRST) == []


def test_a_withdrawn_claimed_mover_supersedes_its_plan_and_stays_unpublished(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """The claim record stays the worker's; the window stops publishing now."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    lead = _hexkey("firstmover0")
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    _claim(queue, lead)

    result = queue.withdraw(lead, reason="stale price", by="operator")
    assert result["status"] == "withdrawn"
    assert queue.item_path(pool.CLAIMED, lead).exists()

    tier_loop.residency_window(queue, tiers={TIER: _tier(tmp_path)})
    assert residency_plan.superseded(queue, plan) is not None

    # What the claiming worker concludes is the worker's to conclude; once the
    # record is gone the window must still not ask for the cancelled key.
    queue.item_path(pool.CLAIMED, lead).unlink()
    events = tier_loop.residency_window(queue, tiers={TIER: _tier(tmp_path)})

    assert not queue.item_path(pool.READY, lead).exists()
    assert lead not in _published(events)


def test_the_window_never_publishes_a_withdrawn_key(queue: pool.PoolQueue) -> None:
    """The decision is a member of the window's own inputs, not a caller habit."""

    plan = _plan(queue, FIRST, label="first")

    decision = residency_plan.window(
        plan, accepted_phase=None, free_gib=8,
        withdrawn=[_hexkey("firstmover0")])

    assert [entry["phase"] for entry in decision["publish"]] == ["phase-1"]


def test_window_pressure_does_not_ask_the_tier_for_a_withdrawn_mover(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """Pressure is what the tier must offer; cancelled work offers nothing."""

    plan = _plan(queue, FIRST, label="first", gib=(4, 1))
    _publish_consumer(queue, FIRST, plan)
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    queue.withdraw(_hexkey("firstmover0"), reason="stale price", by="operator")

    need = tier_loop.window_pressure(
        queue, tiers={TIER: _tier(tmp_path)},
        consumers=[(FIRST, {"accepted_phase": None}, plan, TIER)])

    assert need.get(TIER) == 1


def test_an_admission_preemption_does_not_supersede_the_plan(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """Admission requeues its holder immediately; the window is not the owner."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    lead = _hexkey("firstmover0")
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    queue.withdraw(lead, reason="preempted for foreground", by="admission",
                   preempted_by="f" * 64)

    events = tier_loop.residency_window(queue, tiers={TIER: _tier(tmp_path)})

    assert residency_plan.read(queue, FIRST) is not None
    assert residency_plan.superseded(queue, plan) is None
    assert _markers(queue, FIRST) == []
    assert lead not in _published(events)
    assert not queue.item_path(pool.READY, lead).exists()


# -- the withdrawal path reaps the residue it minted -------------------------


def test_a_dead_consumers_movers_are_withdrawn_and_its_plan_reaped(
        queue: pool.PoolQueue) -> None:
    """The #620 pass is a withdrawal path too, and reaping waits for the stop."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    queued = _hexkey("firstmover0")
    running = _hexkey("firstmover1")
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    queue.publish(**plan["phases"][1]["mover_row"], recompute=True)
    _claim(queue, running)
    _fail_consumer(queue, FIRST)

    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert {event["mover"] for event in events if event.get("withdrawn")} == {
        queued, running}
    # The body is retained while the claimed stop is still outstanding: the
    # next cycle needs the plan to find it.
    assert residency_plan.read(queue, FIRST) is not None
    assert _archives(queue, FIRST) == []

    # The worker concludes; now nothing names the plan and it is archived.
    queue.item_path(pool.CLAIMED, running).unlink()
    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert any(event.get("event") == "residency-plan-reaped" for event in events)
    assert residency_plan.read(queue, FIRST) is None
    assert _archives(queue, FIRST)
    assert not queue.item_path(pool.READY, queued).exists()


def test_a_withdrawn_consumer_still_cancels_its_children_before_reaping(
        queue: pool.PoolQueue) -> None:
    """Marking must not hide the plan from the pass that stops its movers."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    lead = _hexkey("firstmover0")
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)

    result = queue.withdraw(FIRST, reason="operator asked", by="test")
    assert result["status"] == "withdrawn"
    assert result.get("residency_plan_superseded") is True
    assert residency_plan.superseded(queue, plan) is not None

    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert [event["mover"] for event in events if event.get("withdrawn")] == [lead]
    assert any(event.get("event") == "residency-plan-reaped" for event in events)
    assert residency_plan.read(queue, FIRST) is None
    assert _archives(queue, FIRST)


def test_a_superseded_plan_is_not_replaced_until_its_work_has_ended(
        queue: pool.PoolQueue) -> None:
    """The planner's precondition, at the plan level: mark, then reap, then seal."""

    plan = _plan(queue, FIRST, label="stale", fill=259)
    _publish_consumer(queue, FIRST, plan)
    fresh = _plan(queue, FIRST, label="fresh", fill=65)
    assert residency_plan.mark_superseded(
        queue, FIRST, plan=plan, reason="mover-withdrawn") is not None

    with pytest.raises(residency_plan.ResidencyPlanError, match="already filed"):
        residency_plan.freeze(queue, fresh)
    # A live consumer keeps the body: a handoff is not made from underneath it.
    assert residency_plan.reap(queue, FIRST, reason="test") is None
    assert residency_plan.read(queue, FIRST) is not None

    queue.withdraw(FIRST, reason="operator asked", by="test")
    assert residency_plan.reap(queue, FIRST, reason="test") is not None
    assert residency_plan.read(queue, FIRST) is None

    residency_plan.freeze(queue, fresh)
    assert residency_plan.read(queue, FIRST) == residency_plan.validate_plan(fresh)
    # The stale cancellation covers the plan it named, never the replacement.
    assert residency_plan.superseded(queue, fresh) is None
    assert residency_plan.leads_for(fresh) != residency_plan.leads_for(plan)


# -- what retirement must not disturb ----------------------------------------


def test_retirement_leaves_a_resident_range_held_for_adoption(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """Successful work is adoption's, not withdrawal's (#620, #598)."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    finished = _hexkey("firstmover0")
    stage = tmp_path / "stage"
    stage.mkdir()
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(finished, {"stage_gib": 2})
    queue.record_move(finished, {
        "consumer_action_key": FIRST, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
        "bytes_staged": 2 * GIB, "complete": True, "unix": 1000.0})
    _fail_consumer(queue, FIRST)

    tier_loop.withdraw_dead_consumer_movers(queue)

    assert residency_plan.read(queue, FIRST) is None
    assert finished in ledger.held_keys(), (
        "retirement never releases a resident range's tokens")


def test_a_running_consumers_other_resident_ranges_stay_named(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """Marking one mover must not expose the rest of a live window (#708)."""

    plan = _plan(queue, FIRST, label="first", gib=(2, 2, 2))
    _publish_consumer(queue, FIRST, plan)
    lead = _hexkey("firstmover0")
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    later = _hexkey("firstmover2")
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(later, {"stage_gib": 2})
    stage = tmp_path / "stage"
    stage.mkdir()
    queue.record_move(later, {
        "consumer_action_key": FIRST, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": 4 * GIB, "range_end_bytes": 6 * GIB,
        "bytes_staged": 2 * GIB, "complete": True, "unix": 1000.0})

    queue.withdraw(lead, reason="stale price", by="operator")
    tier_loop.residency_window(queue, tiers={TIER: _tier(tmp_path)})

    assert residency_plan.superseded(queue, plan) is not None
    wanted, _owners = stage_release.live_claims(queue)
    assert later in wanted, "the live plan still names its staged later phase"

    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    swept = stage_release.sweep(queue, stage_roots={TIER: str(stage)},
                                pressure={TIER: 8})

    assert later in ledger.held_keys(), (
        "a resident range of the running consumer was treated as an orphan")


def test_an_unrelated_consumers_plan_survives_and_still_publishes(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """Retirement is scoped to the consumer whose work was withdrawn."""

    first = _plan(queue, FIRST, label="first")
    second = _plan(queue, SECOND, label="second")
    _publish_consumer(queue, FIRST, first)
    _publish_consumer(queue, SECOND, second)
    queue.publish(**first["phases"][0]["mover_row"], recompute=True)
    queue.publish(**second["phases"][0]["mover_row"], recompute=True)
    queue.withdraw(_hexkey("firstmover0"), reason="stale price", by="operator")

    events = tier_loop.residency_window(queue, tiers={TIER: _tier(tmp_path)})

    assert residency_plan.superseded(queue, first) is not None
    assert residency_plan.superseded(queue, second) is None
    assert residency_plan.read(queue, SECOND) is not None
    assert _markers(queue, SECOND) == []
    assert _hexkey("secondmover1") in _published(events)
    assert queue.item_path(pool.READY, _hexkey("secondmover1")).exists()


# -- the cancellation outranks the snapshot, and identity is the filing ------


def test_a_cancellation_after_the_cycle_snapshot_is_not_superseded_by_publish(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """A cycle snapshot cannot close the race; publish's own lock does.

    The window is handed an *empty* snapshot while the queue already carries
    the operator's cancellation -- exactly the ordering a once-per-cycle
    snapshot cannot see.  The automatic publication must refuse under the
    key's transition lock, so the marker is never retired by the very write
    it was filed to stop.
    """

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    lead = _hexkey("firstmover0")
    queue.publish(**plan["phases"][0]["mover_row"], recompute=True)
    queue.withdraw(lead, reason="stale price", by="operator")

    events = tier_loop.residency_window(
        queue, tiers={TIER: _tier(tmp_path)}, withdrawn=frozenset())

    assert lead not in _published(events)
    assert not queue.item_path(pool.READY, lead).exists()
    refusal = [event for event in events
               if event.get("event") == "mover-publish-refused-withdrawn"]
    assert len(refusal) == 1, events
    assert refusal[0]["action_key"] == lead
    assert refusal[0]["plan_superseded"] is True
    assert residency_plan.superseded(queue, plan) is not None


@pytest.mark.parametrize("damage", ["corrupt", "unreadable"])
def test_an_unreadable_supersession_marker_refuses_publication(
        queue: pool.PoolQueue, tmp_path: Path, damage: str) -> None:
    """Unknown retirement is not "not retired": a damaged marker defers."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    marker = residency_plan.superseded_path(queue, plan)
    marker.parent.mkdir(parents=True, exist_ok=True)
    if damage == "corrupt":
        marker.write_text("{ this is not a marker")
    else:
        marker.mkdir()        # present, and unreadable as a file

    record = residency_plan.superseded(queue, plan)

    assert record is not None and record.get("unreadable") is True, record
    events = tier_loop.residency_window(queue, tiers={TIER: _tier(tmp_path)})
    assert not any(event.get("event") == "mover-published" for event in events)
    assert not queue.item_path(pool.READY, _hexkey("firstmover0")).exists()
    assert residency_plan.read(queue, FIRST) is not None


def test_a_deliberate_same_body_reseal_is_not_cancelled_by_the_old_marker(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """Markers bind a *filing*, not a body: the same bytes can be requested again."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    queue.withdraw(FIRST, reason="operator asked", by="test")
    assert residency_plan.reap(queue, FIRST, reason="test") is not None
    assert _marker_evidence(queue, FIRST)

    # The deliberate resubmission seals the identical body: a new filing, and
    # the marker of the one it replaced does not cover it.
    residency_plan.freeze(queue, plan)
    _publish_consumer(queue, FIRST, plan)

    assert residency_plan.superseded(queue, plan) is None
    events = tier_loop.residency_window(queue, tiers={TIER: _tier(tmp_path)})
    assert _hexkey("firstmover0") in _published(events)


def test_a_stale_reaper_does_not_archive_a_new_plan(queue: pool.PoolQueue) -> None:
    """The reaper rechecks the exact filing under the consumer's lock."""

    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    queue.withdraw(FIRST, reason="operator asked", by="test")
    captured, filing = residency_plan.read_filed(queue, FIRST)
    assert captured is not None and filing is not None

    # A deliberate resubmission replaces the filing before the stale reaper
    # runs: identical bytes, different incarnation.
    queue.residency_plan_path(FIRST).unlink()
    residency_plan.freeze(queue, plan)

    assert residency_plan.reap(queue, FIRST, reason="stale",
                               plan=captured, filing=filing) is None
    assert residency_plan.read(queue, FIRST) is not None
