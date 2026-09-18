"""A tier's fill supply comes off the members' sector counters (#607).

The live tier announced ``{"fill_source": "measured", "fill_mb_s_pool_side":
0}``.  Both halves were wrong at once, and in opposite directions.

``fill_rate_from_records`` read ``disk_pacing.mean_self_read_mb_s``, which is
nfsd ``export_stats`` bytes served to the *consumer's* client addresses.  A
stage mover copies pool -> stage on the file server and is not an NFS client of
it, so its own reads are attributed to nobody: all five live receipts in
``pb-queue/movers/`` report 0.0 (one 0.2) with ``mean_util_pct`` 77-86 and
file-side rates of 229-1478 MB/s.  ``/proc/self/io`` is no better -- four of
those five report ``read_bytes: 0``, because ZFS issues its device reads from
``zio`` taskq threads.  What does see every read is the sum over the pool's
members of ``/sys/block/<dev>/stat`` field 2, sectors read, which the pacer
sampled the file of and threw the field away.

Then ``tier_tokens`` turned the one 0.2 into ``int(0.2) == 0`` and minted a
``fill_mb_s_pool_side`` *key* worth nothing, so the tier reported a measured
capacity that admits no mover at all.

This file asserts both, driving the real pacer against a stat source it can
state, and the real ``tier_loop.cycle`` against the receipt shape that shipped.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB

#: One live receipt's ``disk_pacing`` block, copied from
#: ``pb-queue/movers/9efd3e4f6259….json``: the only one of the five that
#: reported a non-zero self rate, and the one that minted the zero token.
LIVE_PACING = {
    "active": True,
    "devices": ["sdb", "sdc", "sdd", "sde"],
    "mean_self_read_mb_s": 0.2,
    "mean_util_pct": 77.7,
    "mean_backlog_ms": 10013.9,
    "self_read_mb_s": 0.0,
    "other_read_mb_s": 0.0,
    "served_attribution": "attributed",
}


def _stat_row(sectors_read: int, ticks: int) -> list[int]:
    """One ``/sys/block/<dev>/stat`` row with the two fields under test."""

    row = [0] * 11
    row[prewarm_loop.STAT_READS_COMPLETED] = max(1, sectors_read // 256)
    row[prewarm_loop.STAT_READ_SECTORS] = sectors_read
    row[prewarm_loop.STAT_READ_MS] = 1
    row[prewarm_loop.STAT_IO_TICKS] = ticks
    row[prewarm_loop.STAT_WEIGHTED_IO_MS] = 1
    return row


def _pacer(rows: dict[str, list[list[int]]], clock) -> prewarm_loop.DiskPacer:
    """A pacer over named devices whose stat rows advance on each read."""

    cursors = {device: 0 for device in rows}

    def stat_source(device: str):
        index = cursors[device]
        cursors[device] = min(index + 1, len(rows[device]) - 1)
        return rows[device][index]

    pacer = prewarm_loop.DiskPacer(
        sorted(rows), max_util_pct=100.0, max_read_await_ms=1e9,
        max_backlog_ms=1e9, stat_source=stat_source, clock=clock)
    return pacer


def test_the_pacer_reports_what_the_members_delivered_not_what_nfs_served():
    """Four members, 128 MiB of sectors each per second, over two intervals."""

    ticks = [0, 1000, 2000]
    per_interval = 128 * 1024 * 1024 // 512      # sectors
    rows = {
        device: [_stat_row(per_interval * step, ticks[step]) for step in range(3)]
        for device in ("sdb", "sdc", "sdd", "sde")
    }
    now = [1000.0]
    pacer = _pacer(rows, lambda: now[0])
    pacer.begin_row(served_host="", served_addresses=(), served_reason="no self")
    for _ in range(3):
        pacer._measure(now[0])
        now[0] += 1.0
    report = pacer.report()
    # Two intervals of 4 x 128 MiB in 1 s each: 4 x 134.2 MB/s.
    assert report["mean_pool_read_mb_s"] == pytest.approx(536.9, abs=0.5)
    assert report["pool_read_bytes"] == 2 * 4 * 128 * 1024 * 1024
    assert report["pool_read_seconds"] == pytest.approx(2.0)
    # And the field that could not see it is still reported, still zero.
    assert report["mean_self_read_mb_s"] is None


def test_a_row_that_read_nothing_reports_no_rate_rather_than_zero():
    rows = {"sdb": [_stat_row(0, 0), _stat_row(0, 0)]}
    now = [10.0]
    pacer = _pacer(rows, lambda: now[0])
    pacer.begin_row(served_host="", served_addresses=(), served_reason="no self")
    pacer._measure(now[0])
    now[0] += 1.0
    pacer._measure(now[0])
    report = pacer.report()
    assert report["pool_read_bytes"] == 0
    assert report["mean_pool_read_mb_s"] == 0.0


def test_the_live_receipt_shape_mints_no_fill_and_says_so(tmp_path):
    """The receipts that shipped, through the real cycle: no token, no label."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    for index in range(3):
        queue.record_move(str(index) * 64, {
            "tier_id": TIER, "complete": True, "seconds": 44.5,
            "bytes_staged": 10 * GIB, "mb_per_s_file_side": 244.7,
            "disk_pacing": dict(LIVE_PACING), "unix": 100.0 + index})

    def discover(**_kwargs):
        record = {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                  "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                  "mountpoint": str(tmp_path / "stage"),
                  "capacity_bytes": 600 * GIB}
        record[storage_tiers.FILL_RECORD_FIELD] = storage_tiers.fill_rate_from_records(
            _kwargs.get("fill_records") or ())
        return {TIER: record}

    announced = tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                                receipts=tier_loop.ReceiptCache(), discover=discover)
    assert len(announced) == 1
    record = announced[0]
    assert record["fill_records"] == 3
    assert storage_tiers.FILL_KIND not in record["tokens"], record["tokens"]
    assert record["fill_source"] == "none"


def test_a_receipt_carrying_the_pool_side_field_mints_the_measured_token(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.record_move("a" * 64, {
        "tier_id": TIER, "complete": True, "seconds": 44.5,
        "disk_pacing": {**LIVE_PACING, "mean_pool_read_mb_s": 166.0},
        "unix": 100.0})
    # A second reader overlapping the first: because the field counts every
    # read of the pool, the higher delivery is in the record without anybody
    # reconstructing who was concurrent with whom.
    queue.record_move("b" * 64, {
        "tier_id": TIER, "complete": True, "seconds": 44.5,
        "disk_pacing": {**LIVE_PACING, "mean_pool_read_mb_s": 498.0},
        "unix": 120.0})

    def discover(**_kwargs):
        record = {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                  "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                  "mountpoint": str(tmp_path / "stage"),
                  "capacity_bytes": 600 * GIB}
        record[storage_tiers.FILL_RECORD_FIELD] = storage_tiers.fill_rate_from_records(
            _kwargs.get("fill_records") or ())
        return {TIER: record}

    record = tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                             receipts=tier_loop.ReceiptCache(),
                             discover=discover)[0]
    assert record["tokens"][storage_tiers.FILL_KIND] == 498
    assert record["fill_source"] == "measured"


@pytest.mark.parametrize("rate,minted", [(0.2, False), (0.9, False),
                                         (1.0, True), (166.4, True)])
def test_a_rate_below_one_whole_mb_s_mints_no_kind_at_all(rate, minted):
    """``int(0.2)`` tokens is a supply that admits nothing, labelled measured."""

    record = {"tier": "stage", "capacity_bytes": 4 * GIB,
              storage_tiers.FILL_RECORD_FIELD: rate}
    tokens = storage_tiers.tier_tokens(record)
    assert (storage_tiers.FILL_KIND in tokens) is minted
    if minted:
        assert tokens[storage_tiers.FILL_KIND] == int(rate)


def test_the_retired_field_no_longer_mints_anything():
    """A record carrying only the nfsd attribution is not a pool measurement."""

    assert storage_tiers.fill_rate_from_records(
        [{"disk_pacing": dict(LIVE_PACING)}]) is None
