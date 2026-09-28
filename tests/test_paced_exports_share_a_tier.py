"""Paced exports price their fill from their own measured rate (#1014 item 3).

A paced export used to seal the tier's whole fill offer (#747's conservative
first cut: no export receipt priced a pool write), so a second paced export
on the same tier waited for the first to end and exports serialized across
producers.  These tests pin the measured fix end to end: an export files
its achieved file-side rate queue-side when it completes, one immutable
file per export (``exports/<export_key>.json``, #1319), a producer's next
export seals at its newest receipt's rate capped by the tier's offer --
however slow, with no floor and no writer-bound gate -- the tier ledger
keeps the sum of concurrent exports' declared rates at the offer, and a
producer with no measurement still takes the whole offer -- a missing
signal never admits a second writer.
"""
from __future__ import annotations

import json
from pathlib import Path

import test_a_producers_exports_run_on_its_allowance as al
import test_prepaid_writer_integration as fx
import test_produced_spool as base
from prismabuild import core, pool, produced_output as po, produced_spool as ps, storage_tiers

FILL = storage_tiers.FILL_KIND
FILL_DEMAND = f"{FILL}{storage_tiers.TIER_DEMAND_SEPARATOR}{fx.TIER}"
#: The schema of the queue-side export receipt the fix files and reads.
EXPORT_RECEIPT_SCHEMA = "prismaquant.prismabuild.pool_export.v1"

#: The literal record the fix reads, written by hand for the seal tests:
#: on a tree without the reader it is inert, which is exactly the defect.
PRIOR_EXPORT_KEY = "a" * 64


def world_roomy(tmp_path, *, payload_max: int):
    """A producer whose template allows two 1 MiB payloads outstanding."""

    cas_root = tmp_path / "cas"
    template = fx._template(str(tmp_path / "canonical"))
    template["durable_maxima"]["payload_max_bytes"] = payload_max
    initial = fx._producer_request(tmp_path, cas_root, template)
    cas, request = po._read_producer_request(cas_root, initial)
    request.pop("action_key")
    request["environment"]["variables"].update(
        {ps.ROOT_ENV: str(tmp_path / "local"), ps.MAX_ENV: str(2 << 20),
         ps.PACED_EXPORT_ENV: "1"})
    action = core.seal_action(request)
    cas.publish_action_request(action)
    owner = action["action_key"]
    q = fx._queue(tmp_path)
    inst = fx._bind(q, template, owner, cas_root)
    fx._announce_tier(q, tmp_path / "stage")
    return ps.ProducedSpool(q, inst, template, cas_root=cas_root,
                            root=tmp_path / "local", max_bytes=2 << 20)


def offer_fill(queue: pool.PoolQueue, mb_s: int) -> None:
    """Mint ``mb_s`` fill tokens and announce them, as the tier loop does."""

    queue.mint_tier_capacity(fx.TIER, {fx.KIND: 4, FILL: mb_s})
    path = Path(queue.root) / "tiers" / f"{fx.TIER}.json"
    record = json.loads(path.read_text())
    record["tokens"] = {fx.KIND: 4, FILL: mb_s}
    pool._write_json_atomic(path, record)


def sealed(spool, batch) -> dict:
    return ps._read(spool._group(batch) / "export.json")["action"]


def file_prior_receipt(queue, owner, *, rate=60.0, unix=2000.0,
                       key=PRIOR_EXPORT_KEY, tier=fx.TIER, held=0.0,
                       seal=60) -> None:
    """File one completed export's queue-side receipt, as the export will.

    One small immutable file per export (``exports/<export_key>.json``).
    ``held`` is the pacer's own held/slept accounting (``ExportPacer.wrote``
    records a hold only when it actually sleeps), and ``seal`` the rate that
    run was sealed at.  Written by hand for the seal tests: on a tree
    without the per-export reader it is inert, which is exactly the defect.
    """

    record = {"schema": EXPORT_RECEIPT_SCHEMA, "action_key": key, "unix": unix,
              "tier_id": tier, "owner": owner, "rate_mb_s": seal,
              "bytes": 600_000_000, "seconds": 10.0, "held_seconds": held,
              "flushes": 3, "mb_per_s_file_side": rate}
    exports = Path(queue.root) / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    pool._write_json_atomic(exports / f"{key}.json", record)


def claim(spool):
    return spool.queue.claim(owner="share-a-tier-test", tags=[spool.host])


def second_paced_write(spool, mb_s: int) -> str:
    """Another producer's paced export: a plain row tagged to the host and
    sealing ``mb_s`` of the tier's fill, priced at its own measured rate."""

    cas = core.PrismaBuildCAS(spool.root.parent / "cas")
    return al._foreign(spool, cas, "paced-write",
                       {"cpu": 1, "mem_gb": 1, FILL_DEMAND: mb_s})


def test_two_paced_exports_from_two_producers_share_the_tier(tmp_path):
    """Each measured well under half the offer: both claim, and the offer bounds them.

    The producer's prior export wrote at 60 MB/s on a tier offering 164,
    so its next export seals at 60 instead of the whole offer.  A second
    producer's paced write at 104 claims beside it and fills the offer
    exactly -- the two exports run together, which the whole-offer seal
    refused.  Without a measured rate the seal is the whole offer, free
    is zero, and the second producer waits for the first to end -- the
    serialization this file pins away.
    """

    spool = base.world(tmp_path, env={ps.PACED_EXPORT_ENV: "1"})
    offer_fill(spool.queue, 164)
    file_prior_receipt(spool.queue, spool.owner, rate=60.0)
    first = spool.submit_group("b1", base.prepare(spool, "b1")[2])
    assert sealed(spool, "b1")["params"]["demand"][FILL_DEMAND] == 60

    claimed_first = claim(spool)
    assert claimed_first is not None and claimed_first["action_key"] == first["export_key"]

    foreign = second_paced_write(spool, 104)
    claimed_second = claim(spool)
    assert claimed_second is not None and claimed_second["action_key"] == foreign

    ledger = spool.queue.tier_ledger(fx.TIER)
    assert ledger.holder_tokens(first["export_key"]).get(FILL) == 60
    assert ledger.holder_tokens(foreign).get(FILL) == 104
    assert ledger.available().get(FILL, 0) == 0      # the offer bounds the sum


def test_two_measured_exports_share_the_tier_while_a_third_borrows(tmp_path):
    """Two measured seals fit under the offer; the third borrows (#1319, #999).

    The producer's prior export wrote at 60 MB/s on a tier offering 164,
    filed at the per-export home.  Three paced exports each seal at 60:
    the first two claim beside each other (120 under the 164 offer) and
    the third finds 44 free -- short of 60 -- so it takes the free 44 as
    tokens and borrows the remaining 16 from its siblings' held fill:
    other exports are family lenders by design (#999), and a producer's
    own reservations holding its own export out would be the wedge #999
    was built to fix.  The borrow is explicit and bounded -- lenders
    named, overcommit tracked -- while the ledger still bounds the
    unborrowed sum at the offer; the tier no longer serializes behind
    one export.  On a tree without the per-export reader the filed
    receipt is inert, every export seals the whole offer, and the second
    export waits: the serialization this pins away (this test still
    fails there, at the second claim).
    """

    spool = base.world(tmp_path, env={ps.PACED_EXPORT_ENV: "1"})
    offer_fill(spool.queue, 164)
    file_prior_receipt(spool.queue, spool.owner, rate=60.0)
    first = spool.submit_group("b1", base.prepare(spool, "b1")[2])
    assert sealed(spool, "b1")["params"]["demand"][FILL_DEMAND] == 60
    claimed_first = claim(spool)
    assert claimed_first is not None \
        and claimed_first["action_key"] == first["export_key"]

    second = spool.submit_group("b2", base.prepare(spool, "b2")[2])
    assert sealed(spool, "b2")["params"]["demand"][FILL_DEMAND] == 60
    claimed_second = claim(spool)
    assert claimed_second is not None \
        and claimed_second["action_key"] == second["export_key"]

    ledger = spool.queue.tier_ledger(fx.TIER)
    assert ledger.holder_tokens(first["export_key"]).get(FILL) == 60
    assert ledger.holder_tokens(second["export_key"]).get(FILL) == 60
    assert ledger.available().get(FILL, 0) == 44   # the offer bounds the sum

    third = spool.submit_group("b3", base.prepare(spool, "b3")[2])
    assert sealed(spool, "b3")["params"]["demand"][FILL_DEMAND] == 60
    claimed_third = claim(spool)
    assert claimed_third is not None \
        and claimed_third["action_key"] == third["export_key"]
    borrow = claimed_third["tier_fill_borrowed"][fx.TIER]
    assert borrow["taken_free"] == 44 and borrow["borrowed"] == 16
    assert sum(borrow["lent"].values()) == 16
    assert set(borrow["lent"]) <= {first["export_key"], second["export_key"]}
    assert borrow["owner"] == spool.owner
    # The borrow takes no token: the siblings still hold their 60 each,
    # and the ledger still bounds the unborrowed sum at the offer.
    assert ledger.holder_tokens(first["export_key"]).get(FILL) == 60
    assert ledger.holder_tokens(second["export_key"]).get(FILL) == 60
    assert ledger.holder_tokens(third["export_key"]).get(FILL) == 44


def test_a_pacer_bound_receipt_prices_its_achieved_rate(tmp_path):
    """A held run's achieved rate is its seal, and it prices (#1319).

    The prior export was sealed at the whole 164 MB/s offer and its pacer
    held it for 30 s at an achieved 60: that is a stable fixed point, not
    a ratchet -- the next run re-proves the seal -- so the next export
    seals at 60 instead of the whole offer, and a second producer's
    writer-bound 104 MB/s write claims beside it and fills the offer
    exactly.  The writer-bound gate is gone with the rate floor
    (no-heuristics rule: a measured positive finite rate is a
    measurement).
    """

    spool = base.world(tmp_path, env={ps.PACED_EXPORT_ENV: "1"})
    offer_fill(spool.queue, 164)
    file_prior_receipt(spool.queue, spool.owner, rate=60.0, held=30.0,
                       seal=164)
    first = spool.submit_group("b1", base.prepare(spool, "b1")[2])
    assert sealed(spool, "b1")["params"]["demand"][FILL_DEMAND] == 60

    claimed_first = claim(spool)
    assert claimed_first is not None \
        and claimed_first["action_key"] == first["export_key"]

    foreign = second_paced_write(spool, 104)
    claimed_second = claim(spool)
    assert claimed_second is not None and claimed_second["action_key"] == foreign
    ledger = spool.queue.tier_ledger(fx.TIER)
    assert ledger.available().get(FILL, 0) == 0      # the offer bounds the sum


def test_an_export_with_no_measured_rate_takes_the_whole_offer(tmp_path):
    """No receipt for this producer: the seal is the offer, and it is exclusive.

    The conservative default is unchanged (#747): a missing signal fails
    closed, so the export reserves the whole offer and a measured second
    producer -- sealing 60 of the 164 it could justify -- still waits, a
    stranger's row that no family fill can cover (#999).
    """

    spool = base.world(tmp_path, env={ps.PACED_EXPORT_ENV: "1"})
    offer_fill(spool.queue, 164)
    first = spool.submit_group("b1", base.prepare(spool, "b1")[2])
    assert sealed(spool, "b1")["params"]["demand"][FILL_DEMAND] == 164
    assert sealed(spool, "b1")["params"]["command"][-4:] == \
        ["--pace-mb-s", "164", "--pace-tier", fx.TIER]

    claimed = claim(spool)
    assert claimed is not None and claimed["action_key"] == first["export_key"]
    ledger = spool.queue.tier_ledger(fx.TIER)
    assert ledger.available().get(FILL, 0) == 0     # the whole offer is held

    foreign = second_paced_write(spool, 60)
    assert claim(spool) is None         # no measurement, no second writer
    assert spool.queue.item_path(pool.READY, foreign).exists()
    denial = al._denial(spool.queue, foreign)
    assert denial["reason"] == "tier_reservation_unavailable", denial


def test_a_finished_export_files_its_rate_and_prices_the_next_seal(tmp_path):
    """The export files its pacing queue-side; the next seal prices from it.

    One real export runs at a 2 MB/s seal over a 1 MB payload, files its
    receipt under ``exports/``, and the producer's next export seals at the
    rate that receipt measured -- the achieved file-side rate, capped by
    the offer.
    """

    spool = world_roomy(tmp_path, payload_max=2 << 20)
    offer_fill(spool.queue, 2)
    payload = b"x" * 1_000_000      # under the template's 1 MiB payload maximum
    _source, _destination, entries = base.prepare(spool, payload=payload, ceiling=1 << 20)
    first = spool.submit_group("b1", entries)
    assert sealed(spool, "b1")["params"]["demand"][FILL_DEMAND] == 2

    claimed = claim(spool)
    assert claimed is not None and claimed["action_key"] == first["export_key"]
    outcome = spool.queue.execute(claimed, timeout_s=120)
    assert outcome.get("returncode") == 0, outcome
    spool.queue.finish(first["export_key"], status="executed")

    pacing = ps._read(spool._group("b1") / "receipt.json")["pacing"]
    filed_path = (Path(spool.queue.root) / "exports"
                  / f"{first['export_key']}.json")
    filed = json.loads(filed_path.read_text())
    assert filed["schema"] == EXPORT_RECEIPT_SCHEMA
    assert filed["action_key"] == first["export_key"]
    assert filed["tier_id"] == fx.TIER and filed["owner"] == spool.owner
    assert filed["bytes"] == len(payload) and filed["seconds"] == pacing["seconds"]
    assert filed["mb_per_s_file_side"] == pacing["mb_per_s_file_side"]
    assert 1.0 <= filed["mb_per_s_file_side"] <= 2.1   # held to its seal
    # The pacer held this run (the copy outran its 2 MB/s schedule), and
    # the held receipt still prices under #1319: the next seal is the
    # achieved rate capped by the offer, not the whole offer.
    assert filed["held_seconds"] > 0.0
    expected = min(int(filed["mb_per_s_file_side"]), 2)
    spool.submit_group("b2", base.prepare(spool, "b2", ceiling=1 << 20)[2])
    assert sealed(spool, "b2")["params"]["demand"][FILL_DEMAND] == expected


def test_export_measured_mb_s_prices_any_positive_finite_rate():
    """Unit pin of the pricing rule the seal reads (#1319).

    The reader is pure over ONE record -- the newest of the producer's
    per-export receipts: owner/tier scoping, then the amended measurable
    test (positive bytes over positive seconds at a positive finite
    rate, however slow -- no floor, no writer-bound gate).  A zero,
    negative, missing or non-finite rate, or a record from another owner
    or tier, prices nothing.
    """

    def receipt(rate, *, held=0.0, owner="owner-a", tier=fx.TIER,
                key="k"):
        return {"action_key": key, "unix": 2000.0,
                "tier_id": tier, "owner": owner, "rate_mb_s": 60,
                "bytes": 600_000_000, "seconds": 10.0,
                "held_seconds": held, "flushes": 3,
                "mb_per_s_file_side": rate}

    read = storage_tiers.export_measured_mb_s
    assert read(None, tier_id=fx.TIER, owner="owner-a") is None
    assert read(receipt(60.0), tier_id=fx.TIER, owner="owner-a") == 60
    assert read(receipt(60.5), tier_id=fx.TIER, owner="owner-a") == 60
    # Not this owner, not this tier: not the producer's history.
    assert read(receipt(60.0, owner="owner-b"),
                tier_id=fx.TIER, owner="owner-a") is None
    assert read(receipt(60.0, tier="elsewhere"),
                tier_id=fx.TIER, owner="owner-a") is None
    # A slow measurement is still a measurement: no floor.
    assert read(receipt(0.2), tier_id=fx.TIER, owner="owner-a") == 0
    # A zero, negative, missing or non-finite rate prices nothing.
    assert read(receipt(0.0), tier_id=fx.TIER, owner="owner-a") is None
    assert read(receipt(-5.0), tier_id=fx.TIER, owner="owner-a") is None
    assert read(receipt(float("inf")), tier_id=fx.TIER, owner="owner-a") is None
    assert read({k: v for k, v in receipt(60.0).items()
                 if k != "mb_per_s_file_side"},
                tier_id=fx.TIER, owner="owner-a") is None
    # Internally inconsistent parts are corrupt, not a measurement.
    assert read({**receipt(60.0), "bytes": 0},
                tier_id=fx.TIER, owner="owner-a") is None
    assert read({**receipt(60.0), "seconds": 0.0},
                tier_id=fx.TIER, owner="owner-a") is None
    # A run the pacer held prices its achieved rate like any other run.
    assert read(receipt(60.0, held=30.0),
                tier_id=fx.TIER, owner="owner-a") == 60
    # A receipt without any bound accounting prices too: the bound
    # state is no longer part of the test.
    assert read({"unix": 2000.0, "tier_id": fx.TIER, "owner": "owner-a",
                 "bytes": 1, "seconds": 1.0, "mb_per_s_file_side": 60.0},
                tier_id=fx.TIER, owner="owner-a") == 60


def test_a_degenerate_newest_receipt_fails_closed_while_history_survives(tmp_path):
    """Per-export files have nothing to erase (#1319).

    Every schema-valid record is filed under its own export key, and the
    seal prices the newest one only.  A degenerate newest (bytes 0 --
    measured nothing) prices ``None`` and the next seal falls back to
    the whole offer: fail closed, never a second writer on a missing
    signal.  The older priced record stays listed for diagnosis, and
    another owner's receipts are invisible to this producer's seal.  A
    record without a 64-hex export key is corrupt and refused.
    """

    spool = base.world(tmp_path, env={ps.PACED_EXPORT_ENV: "1"})
    offer_fill(spool.queue, 164)
    q = spool.queue

    def receipt(unix, *, rate=60.0, key="1" * 64, owner=None):
        return {"schema": EXPORT_RECEIPT_SCHEMA, "action_key": key,
                "unix": unix, "tier_id": fx.TIER,
                "owner": spool.owner if owner is None else owner,
                "rate_mb_s": 60, "bytes": 600_000_000, "seconds": 10.0,
                "held_seconds": 0.0, "flushes": 3,
                "mb_per_s_file_side": rate}

    filed = q.record_export(receipt(2000.0))
    assert filed is not None and filed.name == "1" * 64 + ".json"
    listed = q.export_records(spool.owner, fx.TIER)
    assert [record["action_key"] for record in listed] == ["1" * 64]

    # A degenerate newer export files beside the priced record ...
    again = q.record_export({**receipt(3000.0, key="2" * 64),
                             "bytes": 0, "mb_per_s_file_side": 0.0})
    assert again is not None and again.name == "2" * 64 + ".json"
    listed = q.export_records(spool.owner, fx.TIER)
    assert [record["action_key"] for record in listed] == ["1" * 64, "2" * 64]
    # ... and prices nothing, so the next seal takes the whole offer.
    assert storage_tiers.export_measured_mb_s(
        listed[-1], tier_id=fx.TIER, owner=spool.owner) is None
    spool.submit_group("b1", base.prepare(spool, "b1")[2])
    assert sealed(spool, "b1")["params"]["demand"][FILL_DEMAND] == 164

    # Another owner's receipts never enter this producer's listing.
    q.record_export(receipt(4000.0, key="3" * 64, owner="someone-else"))
    assert len(q.export_records(spool.owner, fx.TIER)) == 2
    assert len(q.export_records("someone-else", fx.TIER)) == 1

    # No 64-hex export key: corrupt, refused loudly.
    try:
        q.record_export(receipt(5000.0, key="not-a-key"))
    except pool.PoolContractError:
        pass
    else:                                                  # pragma: no cover
        raise AssertionError("record_export filed a keyless receipt")
