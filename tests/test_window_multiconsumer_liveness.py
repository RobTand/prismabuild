"""Multi-consumer window fit/liveness: per-consumer decisions oversubscribe shared free.

PRG-04 audit fixture (PB main e91a55d, no production edits).  These tests use
only real production paths -- ``residency_plan.build_plan``/``window``,
``PoolQueue`` tier ledgers, ``tier_loop.residency_window``/``window_pressure``
-- with small synthetic GiB sizes as test parameters (never fleet defaults).

What they prove (all green on current main):

* each consumer's ``window()`` fits alone against the same free snapshot, but
  the two decisions jointly exceed that free snapshot;
* the real coordinator (``residency_window``) publishes both consumers against
  one free snapshot, because a publish to ``ready/`` reserves no tokens --
  tokens move only at mover claim time (``pool._begin_tier_acquire``:
  ``never_fits_tier_capacity`` vs ``tier_reservation_unavailable``);
* ``window_pressure`` reports the MAX next phase across consumers, not the
  SUM, so the sweep frees for one advance, never for the joint need;
* the per-consumer runahead budget (``capacity - step``) sums past capacity
  for N>=2: no joint inequality exists anywhere on main.

This is the PRG-04 gap: active staged + in-flight + minimum feasible next
advancement + reserve <= capacity is never checked jointly, and no
deterministic PB policy guarantees one admissible next step across concurrent
consumers.  Admission serializes via retry (one mover wins
``tier_reservation_unavailable`` while the other waits), but ordering is
queue-scan order with no fairness, no starvation bound, and no
permanent-oversize-vs-transient-wait typing at the window layer (that typing
exists only per-mover at claim time).

Update (#745, window progress policy on the accepted funded-claim
primitive): the coordinator now gates newcomers on current-plus-protected-
next minimum admission.  The publish-both test below asserts the corrected
policy -- exactly one window admitted, the other gated ``joint-fit-stall``
transient -- while the remaining tests stay as the audit record of the
hazard it closes.

Update (#832): the admitted window's protected next is a real reservation,
not a gate-only promise.  ``advance_needs`` now carries the frontier fence on
its ordinary nonfinal answer, so the window blind-takes the advance's room
under its grant before the cycle's publication and binds it to the advance's
own queued row; the free assertion below measures that reservation.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
GIB = storage_tiers.GIB


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, *,
          gib_per_phase: int = 2, phases: int = 4,
          seed: str = "mover") -> dict[str, object]:
    built = []
    for ordinal in range(phases):
        start = ordinal * gib_per_phase * GIB
        end = start + gib_per_phase * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end,
            "stage_gib": gib_per_phase,
            "mover_row": {
                **_row(_hexkey(f"{seed}{consumer[:4]}{ordinal}"),
                       {STAGE_KIND: gib_per_phase, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(_hexkey(f"egress{consumer[:4]}{ordinal}"),
                               {"mem_gb": 1}, queue),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


def _queue(tmp_path: Path, *, capacity_gib: int = 5) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": capacity_gib})
    return q


def _claim(queue: pool.PoolQueue, plan: dict[str, object], consumer: str) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                   "leads": residency_plan.leads_for(plan)})


CONSUMER_A = "a" * 64
CONSUMER_B = "b" * 64


def test_two_windows_each_fit_alone_but_jointly_exceed_free(tmp_path) -> None:
    """Pure-function half: same free snapshot, two fitting decisions, joint overrun."""
    queue = _queue(tmp_path, capacity_gib=5)
    plan_a = _plan(queue, CONSUMER_A, seed="moverA")
    plan_b = _plan(queue, CONSUMER_B, seed="moverB")

    dec_a = residency_plan.window(plan_a, accepted_phase=None, free_gib=5)
    dec_b = residency_plan.window(plan_b, accepted_phase=None, free_gib=5)

    assert [e["phase"] for e in dec_a["publish"]] == ["phase-0", "phase-1"]
    assert [e["phase"] for e in dec_b["publish"]] == ["phase-0", "phase-1"]
    joint = (sum(int(e["stage_gib"]) for e in dec_a["publish"])
             + sum(int(e["stage_gib"]) for e in dec_b["publish"]))
    assert joint == 8
    assert joint > 5  # each fits the 5 GiB snapshot alone; together they do not


def test_residency_window_admits_one_window_and_gates_the_other(tmp_path) -> None:
    """Corrected policy: the gate admits one newcomer; the other waits typed.

    Same setup that used to publish both consumers' movers against one free
    snapshot (the PRG-04 hazard: a publish reserves nothing).  The window
    gate now covers each newcomer's current-plus-protected-next minimum
    against the joint footprint, so exactly one consumer's movers publish
    and the other is gated ``joint-fit-stall`` (transient, never permanent):
    a second current may not land in the room the first advance was
    promised.  The winner is queue-scan order, so the test reads it off the
    events rather than hard-coding it.  The lead's own publication reserves
    nothing, but its protected next does: the room the gate admitted the
    winner for is held under the advance's fence before the cycle ends.
    """
    queue = _queue(tmp_path, capacity_gib=5)
    plan_a = _plan(queue, CONSUMER_A, seed="moverA")
    plan_b = _plan(queue, CONSUMER_B, seed="moverB")
    _claim(queue, plan_a, CONSUMER_A)
    _claim(queue, plan_b, CONSUMER_B)

    events = tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})

    published = [(e["consumer"], e["phase"]) for e in events
                 if e.get("event") == "mover-published"]
    consumers = {c for c, _p in published}
    assert len(consumers) == 1
    gated = [e for e in events if e.get("event") == "window-gated"]
    assert len(gated) == 1
    (winner,) = consumers
    assert gated[0]["consumer"] != winner
    assert gated[0]["reason"] == "joint-fit-stall"
    assert gated[0]["permanent"] is False
    # The lead's publish reserves nothing, but the protected next is a real
    # reservation: the admitted window's advance holds a bound fence for its
    # 2 GiB, so free is capacity minus that advance (#832).
    plans = {CONSUMER_A: plan_a, CONSUMER_B: plan_b}
    advance = str(plans[winner]["phases"][1]["mover_row"]["action_key"])  # type: ignore[index]
    record = queue.read_funding(advance, TIER)
    assert record is not None, [e for e in events
                                if e.get("event") == "mover-published"]
    assert record.get("state") in ("reserved", "transferring"), record
    assert str(record.get("consumer_action_key")) == winner, record
    assert str(record.get("plan_sha256")) == \
        residency_plan.plan_sha256(plans[winner]), record
    assert int(record.get("range_start_bytes")) == 2 * GIB, record
    assert int(record.get("range_end_bytes")) == 4 * GIB, record
    ledger = queue.tier_ledger(TIER)
    kinds = ledger.available()
    assert kinds.get("stage_gib", 0) == 3
    held = sum(int(tokens.get("stage_gib", 0))
               for tokens in (ledger.holder_tokens(holder)
                              for holder in ledger.held_keys()))
    assert held + kinds.get("stage_gib", 0) == 5


def test_window_pressure_reports_max_not_sum(tmp_path) -> None:
    """After both first movers are queued-but-unstaged, pressure is one phase."""
    queue = _queue(tmp_path, capacity_gib=5)
    plan_a = _plan(queue, CONSUMER_A, seed="moverA")
    plan_b = _plan(queue, CONSUMER_B, seed="moverB")
    _claim(queue, plan_a, CONSUMER_A)
    _claim(queue, plan_b, CONSUMER_B)
    tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})

    pressure = tier_loop.window_pressure(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})

    assert pressure.get(TIER) == 2  # max(2, 2), not the joint 4


def test_per_consumer_runahead_budgets_sum_past_capacity(tmp_path) -> None:
    """Two rolling consumers each hold capacity-step; the sum is not bounded."""
    queue = _queue(tmp_path, capacity_gib=6)
    plan_a = _plan(queue, CONSUMER_A, seed="moverA")
    plan_b = _plan(queue, CONSUMER_B, seed="moverB")

    budget_a = residency_plan.runahead_budget_gib(
        plan_a, "phase-0", capacity_gib=6)
    budget_b = residency_plan.runahead_budget_gib(
        plan_b, "phase-0", capacity_gib=6)

    assert budget_a == 4 and budget_b == 4  # capacity - step, each
    assert budget_a + budget_b == 8 > 6  # no joint bound exists on main
