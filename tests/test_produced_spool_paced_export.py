"""An opted-in produced-output export enters the pool under a fill reservation and a pace (#747).

Measured on dl380g10 on 2026-09-22: member reads fell from 175 to 92 MB/s
while the pool took 150-300 MB/s of member writes, and to 19 MB/s above
that. The pool's memory was never short. An export write is paid for in
stage-mover reads, so an export prices itself on the tier ledger the movers
reserve from, by the rule they are priced by, and holds its write rate to
what it reserved. These tests pin that contract end to end through a real
claim, execute and finish.

The price is not yet measured, so the behaviour is opt-in: a producer whose
sealed environment does not set ``PRISMABUILD_PRODUCED_SPOOL_PACED_EXPORT=1``
exports exactly as before, and a runtime publication changes no live export.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import test_prepaid_writer_integration as fx
import test_produced_spool as base
from prismabuild import pool, produced_spool as ps, storage_tiers

FILL = storage_tiers.FILL_KIND
FILL_DEMAND = f"{FILL}{storage_tiers.TIER_DEMAND_SEPARATOR}{fx.TIER}"


def offer_fill(queue: pool.PoolQueue, mb_s: int) -> None:
    """Mint ``mb_s`` fill tokens and announce them, as the tier loop does."""

    queue.mint_tier_capacity(fx.TIER, {fx.KIND: 4, FILL: mb_s})
    path = Path(queue.root) / "tiers" / f"{fx.TIER}.json"
    record = json.loads(path.read_text())
    record["tokens"] = {fx.KIND: 4, FILL: mb_s}
    pool._write_json_atomic(path, record)


def sealed(spool, batch="b1") -> dict:
    return ps._read(spool._group(batch) / "export.json")["action"]


#: The producer environment that opts every export into the pace.
PACED = {ps.PACED_EXPORT_ENV: "1"}


def unpaced(action) -> bool:
    return (action["params"]["demand"] == {"cpu": 1, "mem_gb": 1}
            and "--pace-mb-s" not in action["params"]["command"])


@pytest.mark.parametrize("env", [None, {ps.PACED_EXPORT_ENV: ""}, {ps.PACED_EXPORT_ENV: "0"}])
def test_an_export_is_unpaced_unless_its_producer_opts_in(tmp_path, env):
    """The tier offers fill, and still nothing is reserved: off is the default."""

    spool = base.world(tmp_path, env=env)
    assert spool.paced_export is False
    offer_fill(spool.queue, 7)
    _source, _destination, entries = base.prepare(spool)
    handle = spool.submit_group("b1", entries)
    assert unpaced(sealed(spool))
    row = json.loads(spool.queue.item_path(pool.READY, handle["export_key"]).read_text())
    assert row["resources"] == {"cpu": 1, "mem_gb": 1}


def test_an_opt_in_that_is_not_zero_or_one_is_refused(tmp_path):
    with pytest.raises(ps.SpoolError, match="must be 0 or 1"):
        base.world(tmp_path, env={ps.PACED_EXPORT_ENV: "yes"})


@pytest.mark.parametrize("env, paced, expect_paced", [
    (None, True, True),
    (PACED, False, False),
    (PACED, None, True),
])
def test_one_group_may_override_its_producers_setting(tmp_path, env, paced, expect_paced):
    """How an A/B interleaves paced and unpaced exports from one producer."""

    spool = base.world(tmp_path, env=env)
    offer_fill(spool.queue, 7)
    _source, _destination, entries = base.prepare(spool)
    spool.submit_group("b1", entries, paced=paced)
    action = sealed(spool)
    assert unpaced(action) is not expect_paced
    if expect_paced:
        assert action["params"]["demand"][FILL_DEMAND] == 7


def test_a_group_override_must_be_a_bool(tmp_path):
    spool = base.world(tmp_path)
    _source, _destination, entries = base.prepare(spool)
    with pytest.raises(ps.SpoolError, match="paced must be"):
        spool.submit_group("b1", entries, paced="1")


def test_a_tier_offering_no_fill_leaves_the_export_unreserved(tmp_path):
    spool = base.world(tmp_path, env=PACED)
    _source, _destination, entries = base.prepare(spool)
    spool.submit_group("b1", entries)
    action = sealed(spool)
    assert action["params"]["demand"] == {"cpu": 1, "mem_gb": 1}
    assert "--pace-mb-s" not in action["params"]["command"]


def test_a_paced_export_reserves_the_fill_and_holds_its_rate(tmp_path):
    spool = base.world(tmp_path, maximum=2 << 20, env=PACED)
    offer_fill(spool.queue, 1)
    payload = b"x" * 1_000_000      # under the template's 1 MiB payload maximum
    _source, destination, entries = base.prepare(spool, payload=payload, ceiling=1 << 20)
    handle = spool.submit_group("b1", entries)
    action = sealed(spool)
    demand = {"cpu": 1, "mem_gb": 1, FILL_DEMAND: 1}
    assert action["params"]["demand"] == demand
    assert action["params"]["command"][-4:] == ["--pace-mb-s", "1", "--pace-tier", fx.TIER]

    claimed = spool.queue.claim(owner="spool-export-test", tags=[spool.host])
    assert claimed is not None and claimed["action_key"] == handle["export_key"]
    assert claimed["resources"] == demand
    ledger = spool.queue.tier_ledger(fx.TIER)
    assert ledger.holder_tokens(handle["export_key"]).get(FILL) == 1
    assert ledger.available().get(FILL, 0) == 0     # a mover waits its turn

    outcome = spool.queue.execute(claimed, timeout_s=120)
    assert outcome.get("returncode") == 0, outcome
    spool.queue.finish(handle["export_key"], status="executed")
    assert not ledger.holder_tokens(handle["export_key"])
    assert spool.poll_group("b1")["complete"]
    assert destination.read_bytes() == payload

    pacing = ps._read(spool._group("b1") / "receipt.json")["pacing"]
    assert pacing["schema"] == ps.PACING_SCHEMA and pacing["tier_id"] == fx.TIER
    assert pacing["rate_mb_s"] == 1 and pacing["bytes"] == len(payload)
    print("PACING", pacing)
    # 1 MB at 1 MB/s: the pace binds, measured on the file side.
    assert pacing["seconds"] >= 0.95 and pacing["held_seconds"] > 0.5
    assert pacing["flushes"] >= 1 and pacing["mb_per_s_file_side"] <= 1.1


def test_a_replay_keeps_the_price_it_was_sealed_at(tmp_path):
    spool = base.world(tmp_path, env=PACED)
    offer_fill(spool.queue, 1)
    _source, _destination, entries = base.prepare(spool)
    first = spool.submit_group("b1", entries)
    offer_fill(spool.queue, 5)
    again = spool.submit_group("b1", entries)
    assert again["export_key"] == first["export_key"]
    # Nor does a replay that asks for no pace unseal the one it sealed.
    assert spool.submit_group("b1", entries, paced=False)["export_key"] == first["export_key"]
    assert sealed(spool)["params"]["demand"][FILL_DEMAND] == 1
    row = json.loads(spool.queue.item_path(pool.READY, first["export_key"]).read_text())
    assert row["resources"] == sealed(spool)["params"]["demand"]


class PacerClock:
    """``produced_spool``'s ``time`` for a pacer test: only a wait moves it.

    The pacer subtracts the time the copy has taken from each wait.  On the
    real clock a write or ``fdatasync`` that a loaded box slows shortens the
    waits the pacer computes, so a test that asserts the waits measures the
    box, not the pacer (#1047: under 16 shards the third wait came out 0.2 s,
    not 0.3 s).  Here a write and a flush take no time and a wait takes
    exactly what it asks for, so the waits are the pacer's schedule.
    """

    def __init__(self, events: list[tuple[str, float]]) -> None:
        self.now = 1000.0
        self.events = events

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.events.append(("sleep", seconds))
        self.now += seconds


def test_the_pacer_flushes_before_it_waits(tmp_path, monkeypatch):
    events: list[tuple[str, float]] = []
    real_sync = os.fdatasync
    # The real flush, kept for the order only: on this clock it takes no time.
    monkeypatch.setattr(ps.os, "fdatasync",
                        lambda fd: (events.append(("sync", 0.0)), real_sync(fd))[1])
    monkeypatch.setattr(ps, "time", PacerClock(events))
    pacer = ps.ExportPacer(1)
    block = 125_000     # 0.125 s at 1 MB/s: every sum below is exact in binary
    with (tmp_path / "out").open("wb") as handle:
        for _ in range(3):
            handle.write(b"y" * block)
            pacer.wrote(block, handle)
    kinds = [kind for kind, _ in events]
    assert kinds == ["sync", "sleep"] * 3
    # Each block runs 0.125 s ahead of the rate the moment it is written, and
    # the pacer waits out exactly that, block after block.
    assert [s for kind, s in events if kind == "sleep"] == [0.125] * 3
    record = pacer.record(fx.TIER)
    assert record["flushes"] == 3 and record["bytes"] == 3 * block
    assert record["seconds"] == record["held_seconds"] == 0.375
    assert record["mb_per_s_file_side"] == 1.0
    ps._check_pacing(record)


def test_a_pacer_behind_its_schedule_neither_flushes_nor_waits(tmp_path, monkeypatch):
    monkeypatch.setattr(ps.os, "fdatasync", lambda fd: pytest.fail("flushed"))
    monkeypatch.setattr(ps.time, "sleep", lambda s: pytest.fail("waited"))
    pacer = ps.ExportPacer(1)
    pacer.started -= 10.0
    with (tmp_path / "out").open("wb") as handle:
        handle.write(b"z" * 100_000)
        pacer.wrote(100_000, handle)
    assert pacer.record(fx.TIER)["flushes"] == 0


@pytest.mark.parametrize("damage", ["schema", "missing", "negative", "rate", "tier"])
def test_a_pacing_record_that_is_not_the_pacers_is_refused(damage):
    record = ps.ExportPacer(1).record(fx.TIER)
    if damage == "schema":
        record["schema"] = "other"
    elif damage == "missing":
        del record["flushes"]
    elif damage == "negative":
        record["bytes"] = -1
    elif damage == "rate":
        record["rate_mb_s"] = 0
    else:
        record["tier_id"] = ""
    with pytest.raises(ps.SpoolError):
        ps._check_pacing(record)


def test_a_paced_export_names_both_its_rate_and_its_tier(tmp_path):
    spool = base.world(tmp_path)
    _source, _destination, entries = base.prepare(spool)
    handle = spool.submit_group("b1", entries)
    group = spool._group("b1")
    record = ps._read(group / "export.json")
    with pytest.raises(ps.SpoolError, match="names both"):
        ps.export_group(spool.queue, group / "manifest.json", record["manifest_sha256"],
                        handle["export_key"], pace_mb_s=1)


@pytest.mark.parametrize("tokens, measured, expected", [
    (None, None, (None, None, "unmeasured")),
    (None, 40, (40, None, "receipts")),
    ({FILL: 167}, None, (167, 167, "tier-offer")),
    ({FILL: 167}, 90, (90, 167, "receipts-under-offer")),
    ({FILL: 167}, 400, (167, 167, "tier-offer-cap")),
])
def test_movers_and_exports_share_one_fill_rule(tokens, measured, expected):
    tier = {} if tokens is None else {"tokens": tokens}
    assert storage_tiers.current_fill_offer(tier, measured) == expected


@pytest.mark.parametrize("kinds, accepted", [
    ({FILL: 1}, True),
    ({fx.KIND: 1}, False),
    ({FILL: 1, fx.KIND: 1}, False),
])
def test_only_a_rate_is_attributed_by_its_sealed_command(tmp_path, kinds, accepted):
    """A rate names no bytes; occupancy still needs a range or a window (#595)."""

    queue = fx._queue(tmp_path)
    resources = {"cpu": 1, **{f"{kind}@{fx.TIER}": need for kind, need in kinds.items()}}

    def publish():
        queue.publish(action_key="7" * 64, cas_root="/cas", worker_script="/w.py",
                      checkout_root="/co", resources=resources)
    if accepted:
        publish()
        assert queue.item_path(pool.READY, "7" * 64).exists()
    else:
        with pytest.raises(pool.PoolContractError, match="residency block"):
            publish()
