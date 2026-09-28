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

import test_prepaid_writer_integration as fx
import test_produced_spool as base
from prismabuild import pool, produced_spool as ps, storage_tiers

FILL = storage_tiers.FILL_KIND
FILL_DEMAND = f"{FILL}{storage_tiers.TIER_DEMAND_SEPARATOR}{fx.TIER}"
#: The schema of the queue-side export receipt the fix files and reads.
EXPORT_RECEIPT_SCHEMA = "prismaquant.prismabuild.pool_export.v1"

#: The literal record the fix reads, written by hand for the seal tests:
#: on a tree without the reader it is inert, which is exactly the defect.
PRIOR_EXPORT_KEY = "a" * 64


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


def test_two_paced_exports_from_one_producer_share_the_tier(tmp_path):
    """Each measured well under half the offer: both claim, a third waits.

    The producer's prior export wrote at 60 MB/s on a tier offering 164,
    so both of its next exports seal at 60 and run together, and the tier
    ledger keeps their sum at the offer: a third 60 MB/s export finds
    164 - 120 = 44 free and waits.  Without the measured rate the seal is
    the whole offer and the second export waits for the first to end --
    the serialization this file pins away.
    """

    spool = base.world(tmp_path, env={ps.PACED_EXPORT_ENV: "1"})
    offer_fill(spool.queue, 164)
    file_prior_receipt(spool.queue, spool.owner, rate=60.0)
    first = spool.submit_group("b1", base.prepare(spool, "b1")[2])
    assert sealed(spool, "b1")["params"]["demand"][FILL_DEMAND] == 60

    claimed_first = claim(spool)
    assert claimed_first is not None and claimed_first["action_key"] == first["export_key"]

    second = spool.submit_group("b2", base.prepare(spool, "b2")[2])
    assert sealed(spool, "b2")["params"]["demand"][FILL_DEMAND] == 60
    claimed_second = claim(spool)
    assert claimed_second is not None and claimed_second["action_key"] == second["export_key"]

    ledger = spool.queue.tier_ledger(fx.TIER)
    assert ledger.holder_tokens(first["export_key"]).get(FILL) == 60
    assert ledger.holder_tokens(second["export_key"]).get(FILL) == 60
    assert ledger.available().get(FILL, 0) == 44     # the offer bounds the sum

    spool.submit_group("b3", base.prepare(spool, "b3")[2])
    assert sealed(spool, "b3")["params"]["demand"][FILL_DEMAND] == 60
    assert claim(spool) is None         # a third waits its turn


def test_an_export_with_no_measured_rate_takes_the_whole_offer(tmp_path):
    """No receipt for this producer: the seal is the offer, and it is exclusive.

    The conservative default is unchanged (#747): a missing signal fails
    closed, so the export reserves the whole offer and a second paced
    export -- same producer, still no measurement -- waits for it.
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

    second = spool.submit_group("b2", base.prepare(spool, "b2")[2])
    assert sealed(spool, "b2")["params"]["demand"][FILL_DEMAND] == 164
    assert claim(spool) is None         # no measurement, no second writer


def test_a_finished_export_files_its_rate_and_prices_the_next_seal(tmp_path):
    """The export files its pacing queue-side; the next seal prices from it.

    One real export runs at a 2 MB/s seal over a 1 MB payload, files its
    receipt under ``exports/``, and the producer's next export seals at the
    rate that receipt measured -- the achieved file-side rate, capped by
    the offer.
    """

    spool = base.world(tmp_path, maximum=2 << 20, env={ps.PACED_EXPORT_ENV: "1"})
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
