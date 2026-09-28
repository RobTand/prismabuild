"""Family fill lends each token once, and says which lenders were mid-copy (#1292).

#999's borrow funds a paced export from fill its own family holds, and the
lenders keep their tokens and keep copying: while the borrow runs the tier's
pool traffic exceeds its offer by the lent amount.  Two things were missing
(``#1014`` item 2):

* Nothing at borrow time recorded whether a lender was mid-copy, so the
  transient overcommit could not be attributed per lender until the end
  (``note_fill_borrow_end`` reconstructs only which lenders released early).
* The per-borrow cap (``borrowed > sum(lenders.values())``) counted each
  lender's full holdings with no deduction for loans already outstanding, so
  two concurrent family exports could each draw the same lender's tokens --
  and each other's taken-free tokens -- stacking overcommit without bound.
  A sealed mover's ``fill_mb_s`` cannot be rewritten mid-flight (pacing the
  family under one budget stays #905's home), so the pool-side bound is that
  a token is lent once at a time.

These tests pin both: the borrow record names the mid-copy lenders (from the
#1090 landing reports ``mover_landing`` reads), a report older than its
lender's claim is not mid-copy, and a second concurrent export is refused the
tokens a live borrow already drew -- until the first ends, when its row
leaves ``claimed/`` and the tokens are lendable again.
"""
from __future__ import annotations

import json
from pathlib import Path

import test_a_producers_exports_run_on_its_allowance as al
import test_a_producers_paced_export_borrows_its_familys_fill as fam
import test_prepaid_writer_integration as fx
import test_produced_spool as sp
import test_produced_spool_paced_export as paced
from prismabuild import pool


def _landing_report(queue, mover: str, *, started: float) -> None:
    """Write the mover's own live landing report (#1090) as stage_move does."""

    path = queue.consumer_events_dir(mover) / pool.MOVER_LANDING_REPORT
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema": pool.MOVER_LANDING_SCHEMA_V1, "copied_bytes": 5,
        "landed_bytes": 3, "started_unix": started,
        "reported_unix": started + 10.}))


def test_a_borrow_records_which_lenders_were_mid_copy(tmp_path, monkeypatch) -> None:
    """The family lender with a live landing report is named at borrow time;
    the egress holding tokens with no report is not."""

    spool, _cas = fam._paced_producer(tmp_path, monkeypatch)
    queue = spool.queue
    ledger = queue.tier_ledger(fx.TIER)
    assert ledger.acquire(fam.MOVER, {fam.FILL: 4})
    assert ledger.acquire(fam.EGRESS, {fam.FILL: 3})
    _landing_report(queue, fam.MOVER, started=2000.)
    export_key = fam._paced_export(spool)
    al._running(spool, monkeypatch, psi=0.)

    claim = al._claim(queue, spool.host)
    assert claim is not None and claim["action_key"] == export_key
    borrow = claim["tier_fill_borrowed"][fx.TIER]
    assert borrow["borrowed"] == fam.OFFER and borrow["taken_free"] == 0
    assert borrow["lent"] == {fam.MOVER: 4, fam.EGRESS: 3}
    assert borrow["lenders_mid_copy"] == [fam.MOVER]


def test_a_report_from_before_the_lenders_claim_is_not_mid_copy(
        tmp_path, monkeypatch) -> None:
    """A landing report left by a lender's previous attempt does not name it
    mid-copy for the current claim: the report's start must not predate the
    lender's claim."""

    spool, _cas = fam._paced_producer(tmp_path, monkeypatch)
    queue = spool.queue
    fam._hold_fill(queue, fam.MOVER)
    _landing_report(queue, fam.MOVER, started=2000.)
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, fam.MOVER),
                            {"action_key": fam.MOVER, "claimed_unix": 3000.})
    export_key = fam._paced_export(spool)
    al._running(spool, monkeypatch, psi=0.)

    claim = al._claim(queue, spool.host)
    assert claim is not None and claim["action_key"] == export_key
    borrow = claim["tier_fill_borrowed"][fx.TIER]
    assert borrow["lenders_mid_copy"] == []


def _announce_fill(queue, mb_s: int) -> None:
    """Re-announce the tier's fill offer without minting, as the tier loop
    does between staged exports: the next export seals this demand."""

    path = Path(queue.root) / "tiers" / f"{fx.TIER}.json"
    record = json.loads(path.read_text())
    record["tokens"] = {fx.KIND: 4, fam.FILL: mb_s}
    pool._write_json_atomic(path, record)


def test_a_borrow_draws_only_what_it_took(tmp_path, monkeypatch) -> None:
    """A borrow records the draw, not the lender's balance (#1293 round 2).

    The lender holds the whole 100-token offer.  Exports needing 30 then 30
    both borrow -- each draws 30, leaving the other 70 lendable -- and an
    export needing 50 is refused while 60 is drawn and only 40 fits.  When a
    borrowing export ends, its draw returns and the 50 is admitted.  A
    borrow that recorded the lender's full balance would lock the whole
    family out after the first 30."""

    spool, _cas = fam._paced_producer(tmp_path, monkeypatch)
    queue = spool.queue
    paced.offer_fill(queue, 100)
    ledger = queue.tier_ledger(fx.TIER)
    held = int(ledger.available().get(fam.FILL, 0))
    assert held >= 100
    assert ledger.acquire(fam.MOVER, {fam.FILL: held})
    assert ledger.available().get(fam.FILL, 0) == 0

    _announce_fill(queue, 30)
    first = al._export(spool, "g0")
    assert paced.sealed(spool, "g0")["params"]["demand"][paced.FILL_DEMAND] == 30
    al._running(spool, monkeypatch, psi=0.)
    claim = al._claim(queue, spool.host)
    assert claim is not None and claim["action_key"] == first
    borrow = claim["tier_fill_borrowed"][fx.TIER]
    assert borrow["borrowed"] == 30 and borrow["taken_free"] == 0
    assert borrow["lent"] == {fam.MOVER: 30}
    assert borrow["funded_by"] == [fam.MOVER]

    second = al._export(spool, "g1")
    assert paced.sealed(spool, "g1")["params"]["demand"][paced.FILL_DEMAND] == 30
    again = al._claim(queue, spool.host)
    assert again is not None and again["action_key"] == second
    assert again["tier_fill_borrowed"][fx.TIER]["lent"] == {fam.MOVER: 30}

    _announce_fill(queue, 50)
    third = al._export(spool, "g2")
    assert paced.sealed(spool, "g2")["params"]["demand"][paced.FILL_DEMAND] == 50
    assert al._claim(queue, spool.host) is None
    denial = al._denial(queue, third)
    assert denial["reason"] == "tier_reservation_unavailable", denial
    shortage = denial["evidence"]["tier_shortage"]
    assert shortage["family_fill"] == "not_enough_family_fill", shortage

    queue.finish(first, status="executed", claim_snapshot=claim)
    fourth = al._claim(queue, spool.host)
    assert fourth is not None and fourth["action_key"] == third
    assert fourth["tier_fill_borrowed"][fx.TIER]["lent"] == {fam.MOVER: 50}


def test_a_lender_cannot_fund_two_borrows_at_once(tmp_path, monkeypatch) -> None:
    """A lender's tokens are lendable once at a time.  The second family
    export waits on the tier rather than stacking the same tokens again;
    when the first borrow's row leaves ``claimed/`` the tokens return."""

    spool, _cas = fam._paced_producer(tmp_path, monkeypatch)
    queue = spool.queue
    fam._hold_fill(queue, fam.MOVER)
    first = fam._paced_export(spool)
    al._running(spool, monkeypatch, psi=0.)
    claim = al._claim(queue, spool.host)
    assert claim is not None and claim["action_key"] == first

    second = al._export(spool, "g1")
    again = al._claim(queue, spool.host)
    if again is not None:
        raise AssertionError(
            "a second concurrent export must not borrow fill a live borrow "
            f"already drew; it claimed {again['action_key']}")
    denial = al._denial(queue, second)
    assert denial["reason"] == "tier_reservation_unavailable", denial
    shortage = denial["evidence"]["tier_shortage"]
    assert shortage["family_fill"] == "not_enough_family_fill", shortage

    queue.finish(first, status="executed", claim_snapshot=claim)
    third = al._claim(queue, spool.host)
    assert third is not None and third["action_key"] == second
    ended = pool._read_json(queue.item_path(pool.DONE, first))
    assert ended["tier_fill_borrowed"][fx.TIER]["borrowed"] == fam.OFFER
