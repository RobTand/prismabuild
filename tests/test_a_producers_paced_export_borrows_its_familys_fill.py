"""A producer's paced export borrows fill its own family holds (#999).

A paced export (#747) demands ``fill_mb_s_pool_side`` on the tier its batch
stages through, and it is priced at the tier's whole offer.  The producer's
own refill movers hold that tier's fill while they refill its window, and its
progress is counted when the export lands (#982), so the producer's own
movement held its own progress out: the export was refused
``tier_reservation_unavailable`` on every pass while the movers ran.  The #998
allowance covers the host kinds only (WS-DA finding 13a).

An export at the fill ceiling may now borrow what the producer's family holds
on that tier -- the producer's claim, the movers and egresses its plan
publishes, and its other exports -- and never what a stranger holds, so a
foreign paced write at the same ceiling is still refused.  The claim files
the borrow (``tier_fill_borrowed``); the terminal record says which lenders
released before the export ended and the overcommit that left.

Fixture concessions: the producer and its exports are sealed by the real
produced-output fixtures and ``ProducedSpool.submit_group``, claimed through
the real adaptive path on sparklina's CPU map.  The producer's frozen plan is
stated as the movers and egresses it names (``residency_plan.read``), and its
movers' fill is taken on the real tier ledger under their keys.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import test_a_producers_exports_run_on_its_allowance as al
import test_prepaid_writer_integration as fx
import test_produced_spool_paced_export as paced
from prismabuild import adaptive_cpu, core, pool, produced_output as po
from prismabuild import produced_spool as ps, residency_plan, storage_tiers

FILL = storage_tiers.FILL_KIND
FILL_DEMAND = paced.FILL_DEMAND
#: The tier's fill offer, all of which the producer's movers hold.
OFFER = 7
MOVER = "d" * 64
EGRESS = "e" * 64
STRANGER = "f" * 64


def _paced_producer(tmp_path: Path, monkeypatch):
    """#998's spool producer, opted into paced exports, with a stated plan."""

    cas_root = tmp_path / "cas"
    template = fx._template(str(tmp_path / "canonical"))
    initial = fx._producer_request(tmp_path, cas_root, template)
    cas, request = po._read_producer_request(cas_root, initial)
    request.pop("action_key")
    request["environment"]["variables"].update({
        ps.ROOT_ENV: str(tmp_path / "local"), ps.MAX_ENV: "256",
        ps.PACED_EXPORT_ENV: "1"})
    action = core.seal_action(request)
    cas.publish_action_request(action)
    owner = str(action["action_key"])
    queue = fx._queue(tmp_path)
    queue.publish(action_key=owner, cas_root=str(cas_root),
                  worker_script=str(fx.REPO / "tools" / "prismabuild_worker.py"),
                  checkout_root=str(tmp_path / "mover-checkout"),
                  resources={**al.PRODUCER_DEMAND, **po.owner_demand_terms(template)},
                  produced_output_template=template)
    al._sample(monkeypatch, psi=0.)
    claimed = queue.claim(owner="spool-producer", capacity=al.CAPACITY,
                          cpu_tiers=al.CPU_TIERS, adaptive_cpu=True)
    assert claimed is not None and claimed["action_key"] == owner
    control = fx._broker_control(queue, owner)
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=owner,
        claim_snapshot=claimed, env={"PRISMABUILD_ACTION_KEY": owner,
            "PRISMABUILD_ACTION_NONCE": control["nonce"],
            "PRISMABUILD_ACTION_SCOPE": control["scope_id"]})
    po.declare_instance(queue.root, instance)
    assert po.admit_instance(queue, instance, template)["ok"]
    fx._announce_tier(queue, tmp_path / "stage")
    paced.offer_fill(queue, OFFER)
    spool = ps.ProducedSpool(queue, instance, template, cas_root=cas_root,
                             root=tmp_path / "local", max_bytes=256)
    assert spool.paced_export

    plan = {"phases": [{"mover_row": {"action_key": MOVER},
                        "egress_row": {"action_key": EGRESS}}]}
    real_read = residency_plan.read
    monkeypatch.setattr(residency_plan, "read", lambda q, consumer, **kw: (
        plan if consumer == owner else real_read(q, consumer, **kw)))
    return spool, cas


def _hold_fill(queue: pool.PoolQueue, key: str, count: int = OFFER) -> None:
    assert queue.tier_ledger(fx.TIER).acquire(key, {FILL: count})
    assert queue.tier_ledger(fx.TIER).available().get(FILL, 0) == 0


def _paced_export(spool) -> str:
    key = al._export(spool, "g0")
    action = paced.sealed(spool, "g0")
    assert action["params"]["demand"][FILL_DEMAND] == OFFER
    return key


def test_the_export_borrows_its_own_movers_fill(tmp_path, monkeypatch) -> None:
    """The live wedge: the producer's own mover holds the tier's whole fill
    offer.  A foreign paced write ahead of the export is refused; the export
    is claimed in the same pass on fill its family lent."""

    spool, cas = _paced_producer(tmp_path, monkeypatch)
    queue, owner = spool.queue, spool.owner
    _hold_fill(queue, MOVER)
    foreign = al._foreign(spool, cas, "paced-write", {"cpu": 1, "mem_gb": 1, FILL_DEMAND: OFFER})
    export_key = _paced_export(spool)
    al._running(spool, monkeypatch, psi=0.)

    claim = al._claim(queue, spool.host)
    if claim is None or claim["action_key"] != export_key:
        denial = al._denial(queue, export_key)
        raise AssertionError(
            "a producer's paced export must not be held out by its own movers' "
            f"fill; it was refused {denial['reason']}: "
            f"{json.dumps(denial['evidence'].get('tier_shortage'), default=str)[:400]}")
    borrow = claim["tier_fill_borrowed"][fx.TIER]
    assert borrow["borrowed"] == OFFER and borrow["taken_free"] == 0
    assert borrow["funded_by"] == [MOVER] and borrow["lent"] == {MOVER: OFFER}
    assert borrow["owner"] == owner and borrow["plan_children"] == 2
    # The borrow takes no token: the mover still holds every one.
    ledger = queue.tier_ledger(fx.TIER)
    assert ledger.holder_tokens(MOVER).get(FILL) == OFFER
    assert not ledger.holder_tokens(export_key)
    # And the export still runs on its producer's host allowance (#998).
    al._assert_on_allowance(spool, export_key)

    assert queue.item_path(pool.READY, foreign).exists()
    denial = al._denial(queue, foreign)
    assert denial["reason"] == "tier_reservation_unavailable", denial
    assert "family_fill" not in denial["evidence"]["tier_shortage"]

    # The mover lands and releases before the export ends: the terminal
    # record names it and the overcommit it left.
    ledger.release(MOVER)
    queue.finish(export_key, status="executed", claim_snapshot=claim)
    done = pool._read_json(queue.item_path(pool.DONE, export_key))
    ended = done["tier_fill_borrowed"][fx.TIER]
    assert ended["lenders_released_before_end"] == [MOVER]
    assert ended["overcommit_mb_s"] == OFFER


def test_a_strangers_fill_is_never_borrowed(tmp_path, monkeypatch) -> None:
    """The tier's fill is held by a mover no plan of the producer names.  The
    export is refused as before, and the refusal says the family could not
    cover it."""

    spool, _cas = _paced_producer(tmp_path, monkeypatch)
    queue = spool.queue
    _hold_fill(queue, STRANGER)
    export_key = _paced_export(spool)
    al._running(spool, monkeypatch, psi=0.)

    assert al._claim(queue, spool.host) is None
    denial = al._denial(queue, export_key)
    assert denial["reason"] == "tier_reservation_unavailable", denial
    shortage = denial["evidence"]["tier_shortage"]
    assert shortage["family_fill"] == "not_enough_family_fill"
    assert shortage["dependent_of"] == spool.owner
    assert queue.tier_ledger(fx.TIER).holder_tokens(STRANGER).get(FILL) == OFFER


def test_the_export_takes_what_is_free_and_borrows_the_rest(
        tmp_path, monkeypatch) -> None:
    """Part of the offer is free and the rest is the family's.  The export
    takes the free part as tokens and borrows only the remainder."""

    spool, _cas = _paced_producer(tmp_path, monkeypatch)
    queue = spool.queue
    ledger = queue.tier_ledger(fx.TIER)
    assert ledger.acquire(EGRESS, {FILL: 5})
    export_key = _paced_export(spool)
    al._running(spool, monkeypatch, psi=0.)

    claim = al._claim(queue, spool.host)
    assert claim is not None and claim["action_key"] == export_key
    borrow = claim["tier_fill_borrowed"][fx.TIER]
    assert borrow["taken_free"] == 2 and borrow["borrowed"] == OFFER - 2
    assert borrow["lent"] == {EGRESS: 5}
    assert ledger.holder_tokens(export_key).get(FILL) == 2


def test_the_slot_count_is_derived_from_measured_rates(tmp_path) -> None:
    """``ceil(max landing / min spacing)`` over the host's own exports of the
    template; unmeasured, one slot labelled ``unmeasured``; declared, the
    sealed value labelled ``declared``."""

    template = "a" * 64
    assert adaptive_cpu.export_slots({}, template) == (
        1, {"basis": "unmeasured", "template_sha256": template, "landings": 0, "spacings": 0})
    rates = {template: {"landing_s": [6., 21.], "spacing_s": [41., 44.]}}
    slots, basis = adaptive_cpu.export_slots(rates, template)
    assert slots == 1 and basis["basis"] == "measured"
    assert basis["landing_s"] == 21. and basis["spacing_s"] == 41.
    rates = {template: {"landing_s": [30., 95.], "spacing_s": [41., 44.]}}
    assert adaptive_cpu.export_slots(rates, template)[0] == math.ceil(95. / 41.)

    cas = core.PrismaBuildCAS(tmp_path / "cas")
    key = al._plain_request(cas, "tests/slots", {adaptive_cpu.SPOOL_ROOT_ENV: "/spool"})
    item = {"action_key": key, "cas_root": str(cas.root),
            "produced_output": {"template_sha256": template}}
    allowance = adaptive_cpu.producer_allowance(item, rates)
    assert allowance["slots"] == 3 and allowance["cpu"] == 3 and allowance["mem_gb"] == 3
    assert allowance["basis"]["basis"] == "measured"
    declared = al._plain_request(cas, "tests/slots-declared", {
        adaptive_cpu.SPOOL_ROOT_ENV: "/spool", adaptive_cpu.EXPORT_SLOTS_ENV: "2"})
    allowance = adaptive_cpu.producer_allowance(
        {"action_key": declared, "cas_root": str(cas.root),
         "produced_output": {"template_sha256": template}}, rates)
    assert allowance["slots"] == 2 and allowance["basis"]["basis"] == "declared"


def test_a_host_learns_its_templates_export_rates(tmp_path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    ledger = queue.ledger()
    adaptive_cpu.write_json(ledger.base / "cpu-map.json", al.CPU_TIERS)
    owner, template = "b" * 64, "c" * 64
    assert adaptive_cpu.learn_export(ledger, template, owner, 1000., 12.)
    assert adaptive_cpu.learn_export(ledger, template, owner, 1041., 20.)
    assert not adaptive_cpu.learn_export(ledger, template, owner, 1041., 20.)
    base = adaptive_cpu.local_state_base(ledger.base)
    rates = adaptive_cpu.read_json(base / adaptive_cpu.EXPORT_RATES)
    assert rates[template]["landing_s"] == [12., 20.]
    assert rates[template]["spacing_s"] == [41.]
    assert adaptive_cpu.export_slots(rates, template)[0] == 1
