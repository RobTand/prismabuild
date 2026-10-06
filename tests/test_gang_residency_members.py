"""A gang member that declares a manifest or residency (#583, #1247, #1517).

``pbgang`` forwards the data-manifest and residency options (the allowlist
change), and this file answers, by test, what a member carrying them does to
its gang's admission -- the interaction nothing in ``pool.py``, ``_gang.py``
or ``manifest_promotion.py`` spells out:

Q1. While a member's leads are not resident, the claim pass refuses that
    member *before* the gang code: it does not elect, does not ready and does
    not fence its host. Its siblings DO elect and ready, so their hosts fence
    strictly lower-priority work for as long as the movers take -- the
    ready-wait is unbounded (``skew_s`` bounds only the post-claim start
    barrier). The gang cannot commit until the leads land.
Q2. Once every lead is executed and pinned and the map is composed, the gang
    starts normally; each member's claim record carries the verdict and the
    launcher passes ``PRISMABUILD_RESIDENCY_MAP`` to the member that declared
    residency, and to nobody else.
Q3. A teardown (member failure or member withdrawal) withdraws the members
    through the ordinary path, which marks the consumer's frozen plan
    superseded; the leads are separate actions the gang never withdraws.
    Their stage pins are released by the tier loop's orphan sweep -- once no
    live item names them, which for a claimed member means after its covered
    row has ended at the worker's withdrawal checkpoint -- not by the gang
    itself.
Q4. A member that declares a manifest with no ``--residency`` flag (the
    #1247 planner's row) is gated by its filed plan exactly like an explicit
    one: same verdict, same wait, same map at launch through the declared-
    manifest branch of ``residency_map_environment``.

Same two-host fixture as ``test_gang_reservation_1517``: real queue, ledgers,
census and controllers; only the clock and sampler are controlled.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest  # noqa: F401
from test_gang_reservation_1517 import HOSTS, gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401

from prismabuild import _gang, core as pb, pool, residency_map, residency_plan

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import manifest_promotion  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TIER = "prismabuild-stage:sparklina"
STAGE_KIND = f"stage_gib@{TIER}"
GIB = 1 << 30
MANIFEST = "9" * 64


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _consumer_block(leads: list[str]) -> dict[str, object]:
    return {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
            "manifest_sha256": MANIFEST, "manifest_bytes": 6 * GIB,
            "leads": list(leads)}


def _publish_lead(queue, clock, lead: str, stage: Path, *, gib: int = 2) -> None:
    """Publish one movement node the way a frozen plan's mover row is."""
    clock[0] += 0.001
    queue.publish(action_key=lead, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: gib},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                             "manifest_sha256": MANIFEST, "manifest_bytes": 6 * GIB,
                             "range_start_bytes": 0, "range_end_bytes": gib * GIB},
                  max_attempts=1, retry_safe=False, tags=["sparklina"])


def _run_one_mover(queue, monkeypatch, lead: str, consumer: str, stage: Path,
                   *, start: int, end: int) -> None:
    """Claim, receipt and finish one movement node: executed and pinned."""
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    claimed = queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=["sparklina"])
    assert claimed is not None and claimed["action_key"] == lead, claimed
    queue.record_move(lead, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": start, "range_end_bytes": end,
        "bytes_staged": end - start, "complete": True})
    queue.finish(lead, status="executed")


def _stage_lead(queue, clock, monkeypatch, lead: str, consumer: str, stage: Path,
                *, gib: int = 2) -> None:
    _publish_lead(queue, clock, lead, stage, gib=gib)
    _run_one_mover(queue, monkeypatch, lead, consumer, stage,
                   start=0, end=gib * GIB)


def _compose_map(monkeypatch, queue, key: str, leads: list[str]) -> None:
    """The tier loop's composed map, as ``read_map`` answers it."""
    path = queue.residency_map_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}\n")
    monkeypatch.setattr(residency_map, "read_map",
                        lambda p: {"leads": list(leads)})


def _stage_tier(tmp_path: Path) -> dict[str, object]:
    return {"schema": "prismabuild.storage_tier.v1", "tier": "stage",
            "tier_id": TIER, "host": "sparklina",
            "mountpoint": str(tmp_path / "stage"),
            "mover_python": sys.executable,
            "mover_tools_root": str(ROOT / "tools" / "fleet")}


# --------------------------------------------------------------- Q1: the wait


def test_pending_leads_refuse_the_member_before_its_gang_code(gang_fleet,
                                                              monkeypatch, tmp_path):
    """Q1, step by step: a member whose movers have not run refuses before the
    gang election, so it neither elects nor fences its own host, while its
    sibling elects, readies and fences sparky for the whole wait."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    lead = _hexkey("pending-lead")
    group, (first, second) = members(
        "pending", residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})

    # The member is refused, not committed: the denial names the pending lead.
    assert gclaim("sparklina") is None
    waiting = denial(first, "sparklina")
    assert waiting["reason"] == "residency_lead_not_resident", waiting
    assert waiting["evidence"]["residency"]["pending"] == [
        {"lead": lead, "status": "absent"}]
    # No election for this member, so sparklina is not fenced by its gang.
    assert 0 not in _gang.elections(queue, group, 2)
    refill = publish("refill-pending-host", priority=-10, timeout_s=None,
                     cpu=1, gpu=0, mem_gb=1, tags=["sparklina"])
    assert gclaim("sparklina") == refill, denial(refill, "sparklina")
    finish(refill, "sparklina")

    # The sibling elects and readies sparky; the gang holds it there, and the
    # fence drains strictly lower-priority work for as long as the wait lasts.
    assert gclaim("sparky") is None
    held = denial(second, "sparky")
    assert held["reason"] == "gang_waiting_for_peers", held
    assert held["evidence"]["waiting"] == [
        {"index": 0, "action_key": first[:12], "state": "waiting", "host": None}]
    assert _gang.elections(queue, group, 2)[1]["host"] == "sparky"
    drained = publish("drained-on-sparky", priority=-10, timeout_s=None,
                      cpu=1, gpu=0, mem_gb=1, tags=["sparky"])
    assert gclaim("sparky") is None
    assert denial(drained, "sparky")["reason"] in (
        "deferred_for_gang_reservation", "deferred_behind_withheld_row")
    assert queue.item_path(pool.READY, drained).exists()
    # Nobody holds tokens while the gang waits, and nobody has committed.
    assert queue.ledger("sparky").held_keys() == []
    assert not queue.item_path(pool.CLAIMED, first).exists()
    assert not queue.item_path(pool.CLAIMED, second).exists()

    # The movers land; the member passes the same gate and the gang commits
    # whole, in rank order, both hosts.
    _stage_lead(queue, clock, monkeypatch, lead, first, stage)
    _compose_map(monkeypatch, queue, first, [lead])
    assert gclaim("sparklina") == first, denial(first, "sparklina")
    assert gclaim("sparky") == second, denial(second, "sparky")
    assert queue.ledger("sparklina").held_keys() == [first]
    assert queue.ledger("sparky").held_keys() == [second]


# ------------------------------------------------------- Q2: start and launch


def test_a_resident_member_claims_with_its_verdict_and_map(gang_fleet,
                                                           monkeypatch, tmp_path):
    """Q2: the claim record carries the resident verdict and the launcher
    names the composed map -- for the residency member only."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    lead = _hexkey("launched-lead")
    group, (first, second) = members(
        "launched", residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    _stage_lead(queue, clock, monkeypatch, lead, first, stage)
    _compose_map(monkeypatch, queue, first, [lead])

    assert gclaim("sparklina") is None   # ready, waiting for its sibling
    assert gclaim("sparky") == second, denial(second, "sparky")
    assert gclaim("sparklina") == first, denial(first, "sparklina")

    claimed = pool._read_json(queue.item_path(pool.CLAIMED, first))
    assert claimed["residency_verdict"] == {
        "state": "resident", "leads": [lead],
        "map_path": str(queue.residency_map_path(first))}
    environment = queue.launch_environment(claimed)
    assert environment[pb.RESIDENCY_MAP_ENV] == str(queue.residency_map_path(first))
    assert environment[pb.QUEUE_ROOT_ENV] == str(queue.root)
    plain = pool._read_json(queue.item_path(pool.CLAIMED, second))
    assert pb.RESIDENCY_MAP_ENV not in queue.launch_environment(plain)
    # The start barrier releases a whole claimed gang: nobody waits in it.
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    assert queue._gang_start_barrier(claimed, owner="test", heartbeat_s=60.0) is None
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparky")
    assert queue._gang_start_barrier(plain, owner="test", heartbeat_s=60.0) is None


# ----------------------------------------------------- Q3: teardown and pins


def _frozen_plan(queue, consumer: str, lead: str) -> dict[str, object]:
    """The plan a ``--residency stage`` submitter freezes before publishing."""
    phases = [{
        "name": "phase-0", "start_bytes": 0, "end_bytes": 2 * GIB, "stage_gib": 2,
        "mover_row": {"action_key": lead, "cas_root": str(queue.root / "cas"),
                      "checkout_root": str(queue.root / "co"),
                      "worker_script": str(queue.root / "worker.py"),
                      "tags": ["sparklina"],
                      "resources": {"cpu": 1, "mem_gb": 1, STAGE_KIND: 2},
                      "residency": {"schema": pool.RESIDENCY_SCHEMA_V1,
                                    "tier_id": TIER, "manifest_sha256": MANIFEST,
                                    "manifest_bytes": 6 * GIB,
                                    "range_start_bytes": 0,
                                    "range_end_bytes": 2 * GIB}},
        "egress_row": {"action_key": _hexkey(f"{consumer[:8]}-egress"),
                       "cas_root": str(queue.root / "cas"),
                       "checkout_root": str(queue.root / "co"),
                       "worker_script": str(queue.root / "worker.py"),
                       "tags": ["sparklina"], "resources": {"mem_gb": 1}},
    }]
    plan = residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/tests",
        manifest_sha256=MANIFEST, manifest_bytes=6 * GIB, phases=phases)
    residency_plan.freeze(queue, plan)
    return plan


def test_teardown_supersedes_the_members_plan_and_leaves_the_leads_to_the_tier_loop(
        gang_fleet, monkeypatch, tmp_path):
    """Q3: a member failure tears the gang down; the withdrawal marks the
    member's frozen plan superseded, but the gang never touches the leads:
    the mover stays published and keeps its stage pins until the tier loop's
    orphan sweep takes them back."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    lead = _hexkey("torn-lead")
    group, (first, second) = members(
        "torn", residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    plan = _frozen_plan(queue, first, lead)
    _stage_lead(queue, clock, monkeypatch, lead, first, stage)
    _compose_map(monkeypatch, queue, first, [lead])
    assert gclaim("sparklina") is None
    assert gclaim("sparky") == second
    assert gclaim("sparklina") == first
    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(lead) == {"stage_gib": 2}

    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparky")
    queue.finish(second, status="failed", detail={"returncode": 1})
    torn = _gang.teardown(queue, group)
    assert torn is not None and second[:12] in torn["reason"], torn
    # The residency member went through the ordinary withdrawal, which marks
    # its plan superseded -- the window can never be republished at its price.
    assert queue.item_path(pool.WITHDRAWN, first).exists()
    assert residency_plan.superseded(queue, plan) is not None
    # The lead is nobody's gang member: the teardown neither withdraws it nor
    # releases its pins. The tokens stand for bytes still on the stage.
    assert not queue.item_path(pool.WITHDRAWN, lead).exists()
    assert ledger.holder_tokens(lead) == {"stage_gib": 2}
    # The withdrawn member's row is covered, and until its worker reaches the
    # withdrawal checkpoint a covered claimed row is still a live consumer:
    # the sweep retains the lead, because live_claims still names it. This
    # fixture has no worker, so take the checkpoint the worker would take.
    claimed_row = pool._read_json(queue.item_path(pool.CLAIMED, first))
    assert claimed_row is not None, "the withdrawn member's row already left claimed/"
    assert queue.withdrawal_covers(claimed_row) is not None
    queue.finish(first, status="withdrawn")
    # The tier loop's dead-consumer pass archives the plan a live row can no
    # longer name, and then the sweep is what releases the tokens: no live
    # item names the mover any more, so it is an orphan and its tokens go back.
    tier_loop.withdraw_dead_consumer_movers(queue)
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    swept = stage_release.sweep(queue, stage_roots={TIER: str(stage)})
    assert ledger.holder_tokens(lead) == {}, {
        "swept": swept,
        "wanted_owners": stage_release.live_claims(queue),
        "plan_still_filed": residency_plan.read(queue, first) is not None,
        "superseded": residency_plan.superseded(queue, plan) is not None}
    # The gang's fences are gone with it.
    refill = publish("after-torn-teardown", priority=-10, timeout_s=None,
                     cpu=1, gpu=0, mem_gb=1)
    assert gclaim("sparky") == refill, denial(refill, "sparky")


def test_a_withdrawn_member_tears_down_the_same_way(gang_fleet, monkeypatch, tmp_path):
    """Q3, the operator path: ``pb withdraw`` of one member ends the gang;
    its plan is marked and its lead's pins stand until the tier loop."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    lead = _hexkey("withdrawn-lead")
    group, (first, second) = members(
        "withdrawn-member", residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    plan = _frozen_plan(queue, first, lead)
    _stage_lead(queue, clock, monkeypatch, lead, first, stage)
    _compose_map(monkeypatch, queue, first, [lead])
    assert gclaim("sparklina") is None
    assert gclaim("sparky") == second
    assert gclaim("sparklina") == first
    verdict = queue.withdraw(first, reason="operator cancelled the window")
    assert verdict["residency_plan_superseded"] is True, verdict
    assert _gang.teardown(queue, group) is not None
    assert queue.item_path(pool.WITHDRAWN, second).exists()
    assert not queue.item_path(pool.WITHDRAWN, lead).exists()
    assert queue.tier_ledger(TIER).holder_tokens(lead) == {"stage_gib": 2}


def test_two_members_sharing_one_lead_go_resident_together(gang_fleet, monkeypatch,
                                                           tmp_path):
    """--residency-share auto against one registered shared lead (#1026).

    Two members declare the same manifest and the same ``--residency stage``
    plan against ONE movement node: both rows name the identical lead, one
    mover is published, executed and pinned once, and both verdicts sit at
    the same gate -- lead resident, map not yet composed -- until each
    member's own map is composed, the only per-member difference; then both
    claim. The fixture publishes the shared plan the way a shared sealing
    writes it (identical leads); what it does not exercise is pbrun's
    share-namespace dedup itself, i.e. that one sealing consumer's mover key
    is the same object the other binds -- only the queue-side consequence is
    asserted here.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    lead = _hexkey("shared-lead")
    group, (first, second) = members("shared", residency=_consumer_block([lead]),
                                     residency_all=True)
    rows = {key: json.loads(queue.item_path(pool.READY, key).read_text())
            for key in (first, second)}
    assert rows[first]["residency"]["leads"] == [lead]
    assert rows[second]["residency"]["leads"] == [lead]

    # One registered shared mover: published once, executed and pinned once.
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    _stage_lead(queue, clock, monkeypatch, lead, first, stage)
    assert queue.tier_ledger(TIER).holder_tokens(lead) == {"stage_gib": 2}
    for key in (first, second):
        item = json.loads(queue.item_path(pool.READY, key).read_text())
        assert queue.residency_verdict(item)["state"] == "map_not_composed", (key,)

    for key in (first, second):
        _compose_map(monkeypatch, queue, key, [lead])
    assert gclaim("sparklina") is None, denial(first, "sparklina")
    assert gclaim("sparky") == second, denial(second, "sparky")
    assert gclaim("sparklina") == first, denial(first, "sparklina")
    for key in (first, second):
        claimed = pool._read_json(queue.item_path(pool.CLAIMED, key))
        assert claimed["residency_verdict"]["state"] == "resident"
        environment = queue.launch_environment(claimed)
        assert environment[pb.RESIDENCY_MAP_ENV] == str(queue.residency_map_path(key))
    assert queue.residency_map_path(first) != queue.residency_map_path(second)


# ------------------------------------------------ Q4: the planner's gang row


def test_a_planner_row_member_is_gated_like_an_explicit_one(gang_fleet,
                                                            monkeypatch, tmp_path):
    """Q4: a member that declares a manifest with no ``--residency`` flag gets
    its plan from the #1247 planner and is then gated, held and launched
    exactly like a member that passed the flag."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    group, (first, second) = members("planned", declares_manifest=True)
    queue.mint_tier_capacity(TIER, {"stage_gib": 16})
    outcomes = manifest_promotion.promote_ready_manifest_rows(
        queue, tmp_path / "cas", _stage_tier(tmp_path),
        ready=queue.ready_items(), limit=2)
    by_key = {outcome["action_key"]: outcome for outcome in outcomes}
    assert by_key[first]["outcome"] == "planned", by_key
    assert by_key[second]["outcome"] == "no_manifest", by_key
    plan = residency_plan.read(queue, first)
    leads = residency_plan.leads_for(plan)
    assert leads

    # The bare row is gated by the filed plan, verdict for verdict.
    item = json.loads(queue.item_path(pool.READY, first).read_text())
    assert "residency" not in item
    explicit = {**item,
                "residency": residency_plan.consumer_residency(queue, item)[0]}
    assert queue.residency_verdict(item) == queue.residency_verdict(explicit)
    assert queue.residency_verdict(item)["state"] == "lead_not_resident"
    assert queue.residency_verdict(item)["leads"] == leads

    # The gang waits on it exactly as on an explicit residency member.
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_not_resident"
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] == "gang_waiting_for_peers"

    # The tier loop publishes the plan's first mover; the rest publish from
    # the frozen plan the ordinary way, and every lead lands and pins.
    events = tier_loop.residency_window(queue, tiers={TIER: _stage_tier(tmp_path)})
    published = [event.get("action_key") for event in events
                 if event.get("event") == "mover-published"]
    assert leads[0] in published, events
    for phase in plan["phases"][1:]:
        queue.publish(**phase["mover_row"], recompute=True)
    ranges = {str(phase["mover_row"]["action_key"]):
              (int(phase["mover_row"]["residency"]["range_start_bytes"]),
               int(phase["mover_row"]["residency"]["range_end_bytes"]))
              for phase in plan["phases"]}
    for _ in ranges:
        claimed = queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=["sparklina"])
        assert claimed is not None
        mover = str(claimed["action_key"])
        assert mover in ranges, mover
        monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
        start, end = ranges[mover]
        queue.record_move(mover, {
            "consumer_action_key": first, "tier_id": TIER,
            "stage_root": str(stage), "manifest_sha256": MANIFEST,
            "range_start_bytes": start, "range_end_bytes": end,
            "bytes_staged": end - start, "complete": True})
        queue.finish(mover, status="executed")
    _compose_map(monkeypatch, queue, first, leads)

    assert gclaim("sparklina") == first, denial(first, "sparklina")
    assert gclaim("sparky") == second, denial(second, "sparky")
    # The map reaches the planner's member through the declared-manifest
    # branch: the row carries no residency block at all.
    claimed = pool._read_json(queue.item_path(pool.CLAIMED, first))
    assert claimed["residency_verdict"]["state"] == "resident"
    environment = queue.launch_environment(claimed)
    assert environment[pb.RESIDENCY_MAP_ENV] == str(queue.residency_map_path(first))


def test_a_drained_host_still_admits_the_lead_its_gang_waits_on(gang_fleet, monkeypatch, tmp_path):
    """The one cycle a pure drain has: a member waits for a lead that the drain would forbid.

    Real claims.  The gang has waited past ``GANG_DRAIN_AFTER_S``, so sparky
    admits no new equal-priority work -- but the lead named in the waiting
    member's residency block is a GPU row without a residency range, published
    after the gang, and must still be admitted; an unrelated older GPU single
    is drained.
    """
    from prismabuild import _measurement_reservation as reservation
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    lead = _hexkey("gpu-lead")
    group, (first, second) = members("lead-exempt", priority=-10,
                                     residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    assert gclaim("sparklina") is None
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] == "gang_waiting_for_peers"
    clock[0] += reservation.GANG_DRAIN_AFTER_S + 1
    unrelated = publish("unrelated-gpu", priority=-10, timeout_s=None,
                        cpu=1, gpu=1, mem_gb=8, tags=["sparky"])
    clock[0] += 0.001
    queue.publish(action_key=lead, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  resources={"cpu": 1, "gpu": 1, "mem_gb": 8}, needs_gpu=True,
                  priority=-10, max_attempts=1, retry_safe=False, tags=["sparky"])
    assert gclaim("sparky") == lead, denial(lead, "sparky")
    assert denial(unrelated, "sparky")["reason"] in (
        "deferred_for_gang_reservation", "deferred_behind_withheld_row"), denial(unrelated, "sparky")


def test_a_drained_host_still_admits_the_egress_a_running_action_waits_on(
        gang_fleet, monkeypatch, tmp_path):
    """Principle 1, real claims: the drain never holds work that returns capacity.

    The egress named by a live consumer's frozen plan is a CPU-only row at the
    gang's priority published after the drain starts.  It returns the tier
    capacity the gang waits on, so sparky admits it; an unrelated GPU single
    published beside it is drained.
    """
    from prismabuild import _measurement_reservation as reservation
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    lead = _hexkey("egress-lead")
    group, (first, second) = members("egress-exempt", priority=-10,
                                     residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    _frozen_plan(queue, first, lead)
    egress = _hexkey(f"{first[:8]}-egress")
    assert gclaim("sparklina") is None
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] == "gang_waiting_for_peers"
    clock[0] += reservation.GANG_DRAIN_AFTER_S + 1
    unrelated = publish("unrelated-gpu-2", priority=-10, timeout_s=None,
                        cpu=1, gpu=1, mem_gb=8, tags=["sparky"])
    clock[0] += 0.001
    queue.publish(action_key=egress, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  resources={"cpu": 1, "mem_gb": 1}, priority=-10, max_attempts=1,
                  retry_safe=True, tags=["sparky"])
    assert gclaim("sparky") == egress, denial(egress, "sparky")
    assert denial(unrelated, "sparky")["reason"] in (
        "deferred_for_gang_reservation", "deferred_behind_withheld_row"), denial(unrelated, "sparky")


def test_a_drained_host_still_admits_a_leads_own_prerequisite(gang_fleet, monkeypatch, tmp_path):
    """Principle 2, real claims: the exemption is the transitive closure.

    The member waits on ``lead``, and ``lead`` itself waits on ``deep`` (its
    residency block names it).  Both are GPU rows at the gang's priority
    published after the drain starts; ``deep`` is admitted too.
    """
    from prismabuild import _measurement_reservation as reservation
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    lead = _hexkey("closure-lead")
    deep = _hexkey("closure-deep")
    group, (first, second) = members("closure-exempt", priority=-10,
                                     residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    assert gclaim("sparklina") is None
    assert gclaim("sparky") is None
    clock[0] += reservation.GANG_DRAIN_AFTER_S + 1
    unrelated = publish("unrelated-gpu-3", priority=-10, timeout_s=None,
                        cpu=1, gpu=1, mem_gb=8, tags=["sparky"])
    for key, block in ((lead, _consumer_block([deep])), (deep, None)):
        clock[0] += 0.001
        extra = {} if block is None else {"residency": block}
        queue.publish(action_key=key, cas_root=str(queue.root / "cas"),
                      checkout_root=str(queue.root / "co"),
                      worker_script=str(queue.root / "worker.py"),
                      resources={"cpu": 1, "gpu": 1, "mem_gb": 8}, needs_gpu=True,
                      priority=-10, max_attempts=1, retry_safe=False, tags=["sparky"], **extra)
    assert gclaim("sparky") == deep, (denial(deep, "sparky"), denial(lead, "sparky"))
    assert denial(unrelated, "sparky")["reason"] in (
        "deferred_for_gang_reservation", "deferred_behind_withheld_row"), denial(unrelated, "sparky")
