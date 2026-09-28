"""Paced exports price their fill from their own measured rate (#1014 item 3).

A paced export used to seal the tier's whole fill offer (#747's conservative
first cut: no export receipt priced a pool write), so a second paced export
on the same tier waited for the first to end and exports serialized across
producers.  These tests pin the measured fix end to end: an export files
its achieved file-side rate queue-side when it completes, a producer's next
export seals at that rate capped by the tier's offer, the tier ledger keeps
the sum of concurrent exports' declared rates at the offer, and a producer
with no measurement still takes the whole offer -- a missing signal never
admits a second writer.
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
                       key=PRIOR_EXPORT_KEY, tier=fx.TIER) -> None:
    """File one completed export's queue-side receipt, as the export will."""

    record = {"schema": EXPORT_RECEIPT_SCHEMA, "action_key": key, "unix": unix,
              "tier_id": tier, "owner": owner, "rate_mb_s": 60,
              "bytes": 600_000_000, "seconds": 10.0, "held_seconds": 0.0,
              "flushes": 3, "mb_per_s_file_side": rate}
    exports = Path(queue.root) / "exports"
    exports.mkdir(exist_ok=True)
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
    producer's paced write, likewise measured at 60, claims beside it --
    the two exports run together, which the whole-offer seal refused --
    and the tier ledger keeps their summed rate at the offer: 44 tokens
    stay free.  Without the measured rate the seal is the whole offer,
    free is zero, and the second producer waits for the first to end --
    the serialization this file pins away.
    """

    spool = base.world(tmp_path, env={ps.PACED_EXPORT_ENV: "1"})
    offer_fill(spool.queue, 164)
    file_prior_receipt(spool.queue, spool.owner, rate=60.0)
    first = spool.submit_group("b1", base.prepare(spool, "b1")[2])
    assert sealed(spool, "b1")["params"]["demand"][FILL_DEMAND] == 60

    claimed_first = claim(spool)
    assert claimed_first is not None and claimed_first["action_key"] == first["export_key"]

    foreign = second_paced_write(spool, 60)
    claimed_second = claim(spool)
    assert claimed_second is not None and claimed_second["action_key"] == foreign

    ledger = spool.queue.tier_ledger(fx.TIER)
    assert ledger.holder_tokens(first["export_key"]).get(FILL) == 60
    assert ledger.holder_tokens(foreign).get(FILL) == 60
    assert ledger.available().get(FILL, 0) == 44     # the offer bounds the sum


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
    filed_path = Path(spool.queue.root) / "exports" / f"{first['export_key']}.json"
    filed = json.loads(filed_path.read_text())
    assert filed["schema"] == EXPORT_RECEIPT_SCHEMA
    assert filed["action_key"] == first["export_key"]
    assert filed["tier_id"] == fx.TIER and filed["owner"] == spool.owner
    assert filed["bytes"] == len(payload) and filed["seconds"] == pacing["seconds"]
    assert filed["mb_per_s_file_side"] == pacing["mb_per_s_file_side"]
    assert 1.0 <= filed["mb_per_s_file_side"] <= 2.1   # held to its seal

    # The next seal is the filed rate capped by the offer.
    measured = int(filed["mb_per_s_file_side"])
    expected = min(measured, 2) if measured >= 1 else 2
    spool.submit_group("b2", base.prepare(spool, "b2", ceiling=1 << 20)[2])
    assert sealed(spool, "b2")["params"]["demand"][FILL_DEMAND] == expected
