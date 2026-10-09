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


def test_a_member_whose_leads_all_ended_terminally_tears_the_gang_down(
        gang_fleet, monkeypatch, tmp_path):
    """#1543: a gang that can never start must not fence its hosts forever.

    Member 0's only lead fails, so its verdict is ``residency_lead_terminal``
    and nothing will repair it.  Gang elections never expire and the sweep
    tears a gang down only on an UNSUCCESSFUL member, which a READY member
    never is, so the sibling kept fencing sparky until someone withdrew the
    gang.  The claim pass that finds the terminal verdict now tears the gang
    down: the siblings are withdrawn, the fence is released.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    lead = _hexkey("dead-lead")
    group, (first, second) = members("dead-gang", residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    _publish_lead(queue, clock, lead, stage)
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    claimed = queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=["sparklina"])
    assert claimed is not None and claimed["action_key"] == lead, claimed
    queue.finish(lead, status="failed")

    # The sibling elects and fences sparky while the gang waits.
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] == "gang_waiting_for_peers"
    drained = publish("drained-by-the-dead-gang", priority=-10, timeout_s=None,
                      cpu=1, gpu=0, mem_gb=1, tags=["sparky"])
    assert gclaim("sparky") is None, "the live gang should fence sparky first"
    assert denial(drained, "sparky")["reason"] in (
        "deferred_for_gang_reservation", "deferred_behind_withheld_row")

    # The member's own pass finds every lead ended, but one reading is only a
    # mark: the gang is torn down once it has stood for the window and a fresh
    # read still agrees (#1583 review).
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_terminal"
    assert _gang.teardown(queue, group) is None, "one reading must not tear the gang down"
    assert _gang.terminal_mark_path(queue, group, first).exists()
    clock[0] += _gang.TERMINAL_CONFIRM_S - 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None, "the window has not passed"
    clock[0] += 2
    assert gclaim("sparklina") is None
    torn = _gang.teardown(queue, group)
    assert torn is not None and "residency_lead_terminal" in str(torn.get("reason")), torn

    # The sweep withdraws the READY members and the fence is gone.
    queue.sweep_gangs()
    for key in (first, second):
        assert queue.item_path(pool.WITHDRAWN, key).exists(), key
        assert not queue.item_path(pool.READY, key).exists(), key
    assert gclaim("sparky") == drained, denial(drained, "sparky")


def test_a_member_whose_leads_are_merely_pending_does_not_tear_the_gang_down(
        gang_fleet, monkeypatch, tmp_path):
    """The control: a lead that has not run yet is a wait, not a terminal state."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    lead = _hexkey("slow-lead")
    group, (first, second) = members("slow-gang", residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_not_resident"
    assert _gang.teardown(queue, group) is None
    queue.sweep_gangs()
    assert queue.item_path(pool.READY, first).exists()
    assert queue.item_path(pool.READY, second).exists()


def _fail_lead_and_mark(gang_fleet, monkeypatch, tmp_path, name):
    """A gang whose member 0's only lead failed, read terminal once."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    lead = _hexkey(name + "-lead")
    group, (first, second) = members(name, residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    _publish_lead(queue, clock, lead, stage)
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    claimed = queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=["sparklina"])
    assert claimed is not None and claimed["action_key"] == lead, claimed
    queue.finish(lead, status="failed")
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_terminal"
    assert _gang.terminal_mark_path(queue, group, first).exists()
    return queue, clock, gclaim, denial, group, first, second, lead


def test_a_lead_that_reads_live_again_clears_the_mark_and_the_gang_survives(
        gang_fleet, monkeypatch, tmp_path):
    """The reviewed race: a requeue that a pass saw as ended for a moment.

    The lead is requeued after the first terminal reading.  The next pass
    reads it live, which clears the mark; a terminal reading after the window
    starts the wait again instead of confirming the old one.
    """
    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "requeue-gang")
    ended = pool._read_json(queue.item_path(pool.FAILED, lead))
    # The lead is live again: a CLAIMED record under a later generation (a READY
    # one would be claimed by the very pass under test).
    live = dict(pool._read_json(queue.item_path(pool.DONE, lead))
                or pool._read_json(queue.item_path(pool.FAILED, lead)))
    live["published_unix"] = float(ended["published_unix"]) + 5
    live.pop("status", None)
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, lead), live)
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_not_resident"
    assert not _gang.terminal_mark_path(queue, group, first).exists()

    # It ends again long after: a terminal reading that is new, not confirmed.
    queue.item_path(pool.CLAIMED, lead).unlink()
    clock[0] += 10 * _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_terminal"
    assert _gang.teardown(queue, group) is None
    assert _gang.terminal_mark_path(queue, group, first).exists()


def test_a_changed_ending_restarts_the_wait(gang_fleet, monkeypatch, tmp_path):
    """A lead requeued and ended again between two passes is a new reading."""
    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "regen-gang")
    path = queue.item_path(pool.FAILED, lead)
    record = dict(pool._read_json(path))
    record["published_unix"] = float(record["published_unix"]) + 7
    pool._write_json_atomic(path, record)
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None, "the old mark must not confirm a new ending"
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is not None


def test_a_fresh_read_that_disagrees_blocks_the_teardown(gang_fleet, monkeypatch, tmp_path):
    """The confirming read is its own read: it must still be terminal."""
    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "fresh-gang")
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    real = queue.residency_verdict
    calls = []

    def verdict(item, **kwargs):
        calls.append(item["action_key"])
        result = real(item, **kwargs)
        if len(calls) % 2 == 0:  # the confirming re-read sees the lead running
            result = dict(result, pending=[{"lead": lead, "status": "claimed"}])
        return result

    monkeypatch.setattr(queue, "residency_verdict", verdict)
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None
    assert not _gang.terminal_mark_path(queue, group, first).exists()


def test_a_requeue_that_wins_before_the_proof_blocks_the_teardown(
        gang_fleet, monkeypatch, tmp_path):
    """P1: an aged mark must not tear down a gang the lead rejoined in time.

    The mark ages past the window, then the lead is republished under a
    later generation before the confirming pass runs.  The proof under the
    lead's lock reads it live, so no teardown marker is filed and the
    gang still waits for its movers instead of ending.
    """
    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "boundary-gang")
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    clock[0] += 0.001
    queue.publish(action_key=lead, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  resources={"cpu": 1, "mem_gb": 1},
                  max_attempts=1, retry_safe=False, tags=["elsewhere"])
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_not_resident"
    assert _gang.teardown(queue, group) is None
    assert queue.item_path(pool.READY, first).exists()
    assert queue.item_path(pool.READY, second).exists()


def test_a_busy_lead_lock_blocks_the_teardown_and_restarts_the_wait(
        gang_fleet, monkeypatch, tmp_path):
    """P1: the proof takes the leads' locks without waiting.

    A lead whose transition lock another thread owns cannot be proved
    terminal on this pass.  The pass files no teardown marker and resets
    the mark, so the next terminal reading starts a new window.
    """
    import threading

    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "busy-gang")
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    entered = threading.Event()
    release = threading.Event()
    outcome = {}

    def hold_lead():
        with queue._transition_locked(lead, blocking=True) as acquired:
            outcome["acquired"] = acquired
            entered.set()
            release.wait(timeout=30)

    holder = threading.Thread(target=hold_lead, daemon=True)
    holder.start()
    assert entered.wait(timeout=30)
    try:
        assert outcome.get("acquired") is True
        assert gclaim("sparklina") is None
    finally:
        release.set()
        holder.join(timeout=30)
    assert _gang.teardown(queue, group) is None
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None, "the busy pass must restart the wait"
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is not None


def test_a_failed_confirming_read_restarts_the_window(
        gang_fleet, monkeypatch, tmp_path):
    """P2: an uncertain confirming read is not a confirmation.

    The mark ages past the window, then the confirming verdict read
    fails.  The pass files no teardown marker, and the first recovered
    terminal reading starts a new window instead of confirming the old
    mark at once.
    """
    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "read-error-gang")
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    real = queue.residency_verdict
    calls = []

    def verdict(item, **kwargs):
        calls.append(item["action_key"])
        result = real(item, **kwargs)
        if len(calls) % 2 == 0:  # the confirming re-read fails
            raise OSError("stale handle")
        return result

    monkeypatch.setattr(queue, "residency_verdict", verdict)
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None
    monkeypatch.setattr(queue, "residency_verdict", real)
    clock[0] += 1
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_terminal"
    assert _gang.teardown(queue, group) is None, "recovery must start a new window"
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is not None


def test_a_failed_first_verdict_read_restarts_the_window(
        gang_fleet, monkeypatch, tmp_path):
    """P2: a failed first verdict read invalidates the old mark too.

    The claim pass that cannot read the verdict at all files its own
    denial and resets the mark, so a later terminal reading starts a
    new window rather than confirming across the gap.
    """
    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "first-read-error-gang")
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    real = queue.residency_verdict

    def verdict(item, **kwargs):
        if item.get("action_key") == first:
            raise OSError("stale handle")
        return real(item, **kwargs)

    monkeypatch.setattr(queue, "residency_verdict", verdict)
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_record_unreadable"
    assert _gang.teardown(queue, group) is None
    monkeypatch.setattr(queue, "residency_verdict", real)
    clock[0] += 1
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_terminal"
    assert _gang.teardown(queue, group) is None, "recovery must start a new window"
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is not None


def test_a_live_reading_whose_mark_survives_restarts_the_window(
        gang_fleet, monkeypatch, tmp_path):
    """P2: a live reading that cannot unlink its mark fails closed.

    The member reads live, but the mark removal fails.  The pass leaves
    a reset mark, so the next terminal reading restarts the window
    instead of confirming the old mark at once.
    """
    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "stuck-mark-gang")
    ended = pool._read_json(queue.item_path(pool.FAILED, lead))
    live = dict(pool._read_json(queue.item_path(pool.FAILED, lead)))
    live["published_unix"] = float(ended["published_unix"]) + 5
    live.pop("status", None)
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, lead), live)
    real_clear = _gang.clear_terminal
    monkeypatch.setattr(_gang, "clear_terminal",
                        lambda queue, group, key: False)
    assert gclaim("sparklina") is None
    monkeypatch.setattr(_gang, "clear_terminal", real_clear)
    assert denial(first, "sparklina")["reason"] == "residency_lead_not_resident"
    queue.item_path(pool.CLAIMED, lead).unlink()
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_terminal"
    assert _gang.teardown(queue, group) is None, "the stuck mark must restart the wait"
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is not None


@pytest.mark.parametrize("state", [pool.READY, pool.CLAIMED])
@pytest.mark.parametrize("refresh_fails", [False, True])
def test_cached_live_misses_do_not_confirm_an_aged_terminal_mark(
        gang_fleet, monkeypatch, tmp_path, state, refresh_fails):
    """A later generation survives cached misses and failed directory refreshes."""
    import os

    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "cached-live-gang")
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    queue.publish(action_key=lead, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 2},
                  residency={**_consumer_block([]),
                             "range_start_bytes": 0, "range_end_bytes": 2 * GIB},
                  max_attempts=1, retry_safe=False, tags=["elsewhere"])
    if state == pool.CLAIMED:
        claimed = queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=["elsewhere"])
        assert claimed is not None and claimed["action_key"] == lead
    live_path = queue.item_path(state, lead)
    real_read = pool._read_json
    real_open = os.open
    real_listdir = os.listdir
    real_confirm = queue._gang_terminal_confirmed
    proving = False

    def confirm(*args, **kwargs):
        nonlocal proving
        proving = True
        try:
            return real_confirm(*args, **kwargs)
        finally:
            proving = False

    def cached_read(path, **kwargs):
        if path == live_path:
            return None
        return real_read(path, **kwargs)

    def open_directory(path, flags, *args, **kwargs):
        if proving and refresh_fails and Path(path) == live_path.parent and flags & os.O_DIRECTORY:
            raise OSError("directory refresh failed")
        return real_open(path, flags, *args, **kwargs)

    def list_directory(path):
        if proving and refresh_fails and Path(path) == live_path.parent:
            raise OSError("directory refresh failed")
        return real_listdir(path)

    with monkeypatch.context() as fault:
        fault.setattr(queue, "_gang_terminal_confirmed", confirm)
        fault.setattr(pool, "_read_json", cached_read)
        fault.setattr(os, "open", open_directory)
        fault.setattr(os, "listdir", list_directory)
        assert gclaim("sparklina") is None
        assert _gang.teardown(queue, group) is None, "uncertain absence must not end the gang"
        assert queue.item_path(pool.READY, first).exists()
        assert queue.item_path(pool.READY, second).exists()

    if state == pool.READY:
        claimed = queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=["elsewhere"])
        assert claimed is not None and claimed["action_key"] == lead
    queue.record_move(lead, {
        "consumer_action_key": first, "tier_id": TIER,
        "stage_root": str(tmp_path / "stage"), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
        "bytes_staged": 2 * GIB, "complete": True})
    queue.finish(lead, status="executed")
    _compose_map(monkeypatch, queue, first, [lead])
    assert gclaim("sparky") is None
    assert gclaim("sparklina") == first, denial(first, "sparklina")
    assert gclaim("sparky") == second, denial(second, "sparky")


@pytest.mark.parametrize("reading", ["initial", "confirming", "live"])
@pytest.mark.parametrize("restart", [False, True])
def test_failed_unlink_and_replacement_require_a_new_confirmation_window(
        gang_fleet, monkeypatch, tmp_path, reading, restart):
    """Recovery cannot use an aged mark after both reset operations fail."""
    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "reset-failure-gang")
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    mark_path = _gang.terminal_mark_path(queue, group, first)
    old_mark = mark_path.read_bytes()
    real_verdict = queue.residency_verdict
    real_write = pool._write_json_atomic
    calls = 0

    def verdict(item, **kwargs):
        nonlocal calls
        calls += 1
        result = real_verdict(item, **kwargs)
        if reading == "initial" or (reading == "confirming" and calls == 2):
            raise OSError("verdict read failed")
        if reading == "live":
            return dict(result, pending=[{"lead": lead, "status": "claimed"}])
        return result

    def write(path, record):
        if path == mark_path:
            raise OSError("mark replacement failed")
        return real_write(path, record)

    with monkeypatch.context() as fault:
        fault.setattr(queue, "residency_verdict", verdict)
        fault.setattr(_gang, "clear_terminal", lambda *args: False)
        fault.setattr(pool, "_write_json_atomic", write)
        assert gclaim("sparklina") is None
        assert _gang.teardown(queue, group) is None
        assert mark_path.read_bytes() == old_mark, "the reset failure must retain the old bytes"

    if restart:
        recovered = pool.PoolQueue(queue.root)
        monkeypatch.setattr(queue, "_gang_terminal_confirmed",
                            recovered._gang_terminal_confirmed)
    clock[0] += 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None, "recovery must start a new window"
    clock[0] += _gang.TERMINAL_CONFIRM_S - 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None
    clock[0] += 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is not None
    queue.sweep_gangs()
    assert queue.item_path(pool.WITHDRAWN, first).exists()
    assert queue.item_path(pool.WITHDRAWN, second).exists()


def test_a_future_terminal_timestamp_restarts_confirmation(
        gang_fleet, monkeypatch, tmp_path):
    """A future timestamp cannot supply elapsed confirmation time."""
    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "future-mark-gang")
    path = _gang.terminal_mark_path(queue, group, first)
    mark = json.loads(path.read_text())
    mark["first_seen_unix"] = clock[0] + 10 * _gang.TERMINAL_CONFIRM_S
    pool._write_json_atomic(path, mark)
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None
    clock[0] += _gang.TERMINAL_CONFIRM_S - 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None
    clock[0] += 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is not None


@pytest.mark.parametrize("directory", [pool.READY, pool.DONE, pool.MOVERS])
def test_a_failed_refresh_under_the_lead_lock_restarts_confirmation(
        gang_fleet, monkeypatch, tmp_path, directory):
    """The final proof cannot convert a failed refresh into terminal absence."""
    from contextlib import contextmanager
    import os

    queue, clock, gclaim, denial, group, first, second, lead = _fail_lead_and_mark(
        gang_fleet, monkeypatch, tmp_path, "locked-refresh-gang")
    clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
    real_lock = queue._transition_locked
    real_listdir = os.listdir
    real_open = os.open
    locked = False
    target = queue.root / directory

    @contextmanager
    def transition(key, **kwargs):
        nonlocal locked
        with real_lock(key, **kwargs) as acquired:
            if key == lead and acquired:
                locked = True
            try:
                yield acquired
            finally:
                if key == lead:
                    locked = False

    def list_directory(path):
        if locked and Path(path) == target:
            raise OSError("proof directory refresh failed")
        return real_listdir(path)

    def open_directory(path, flags, *args, **kwargs):
        if locked and Path(path) == target and flags & os.O_DIRECTORY:
            raise OSError("proof directory refresh failed")
        return real_open(path, flags, *args, **kwargs)

    with monkeypatch.context() as fault:
        fault.setattr(queue, "_transition_locked", transition)
        fault.setattr(os, "listdir", list_directory)
        fault.setattr(os, "open", open_directory)
        assert gclaim("sparklina") is None
        assert _gang.teardown(queue, group) is None
        assert not _gang.terminal_mark_path(queue, group, first).exists()
    clock[0] += 1
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is None
    clock[0] += _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    assert _gang.teardown(queue, group) is not None


@pytest.mark.parametrize("fault_directory", [None, "holder", "receipt"])
def test_fresh_pin_proof_preserves_a_resident_gang(
        gang_fleet, monkeypatch, tmp_path, fault_directory):
    """A stale unpinned verdict cannot destroy current resident bytes."""
    import os

    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    lead = _hexkey("pinned-proof-lead")
    group, (first, second) = members("pinned-proof", residency=_consumer_block([lead]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    _stage_lead(queue, clock, monkeypatch, lead, first, stage)
    _compose_map(monkeypatch, queue, first, [lead])
    real_pin = queue._lead_is_pinned

    def cached_pin(residency, key, **kwargs):
        if not kwargs.get("fresh"):
            return False
        return real_pin(residency, key, **kwargs)

    with monkeypatch.context() as stale:
        stale.setattr(queue, "_lead_is_pinned", cached_pin)
        assert gclaim("sparklina") is None
        assert denial(first, "sparklina")["reason"] == "residency_lead_terminal"
        clock[0] += 2 * _gang.TERMINAL_CONFIRM_S
        real_listdir = os.listdir
        target = (queue.tier_ledger(TIER).held_dir if fault_directory == "holder"
                  else queue.move_path(lead).parent if fault_directory == "receipt"
                  else None)

        def list_directory(path):
            if target is not None and Path(path) == target:
                raise OSError("pin directory refresh failed")
            return real_listdir(path)

        stale.setattr(os, "listdir", list_directory)
        assert gclaim("sparklina") is None
        assert _gang.teardown(queue, group) is None
        assert queue.item_path(pool.READY, first).exists()
        assert queue.item_path(pool.READY, second).exists()
    assert gclaim("sparky") is None
    assert gclaim("sparklina") == first, denial(first, "sparklina")
    assert gclaim("sparky") == second, denial(second, "sparky")
