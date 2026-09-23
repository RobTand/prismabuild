"""A producer's unheld output window is an obligation the tier gate can count (#747).

A produced-output owner reserves its window on the tier at claim (its
sealed resources carry ``stage_gib@<tier>: window_gib``).  Its batches then
spend that window by exact transfer, and retirement returns the spent
credits to *free*, not to the owner: the owner re-acquires them later with
``produced_output.refill_window``.  Between the retirement and the refill
the window is owed but unheld, and the tier gate counted it as zero
(``output_gib=0``, ``output-scope-unenforced``), so a consumer's window
could take the room the producer's next refill needs.

``unheld_window_gib`` measures exactly that gap: for every live owner with
an admitted window on the tier, ``window - held - outstanding``, where
outstanding is what its batches still hold outside the owner.  Held tokens
and ready demand are already in the gate's other terms, so nothing is
counted twice.  The tier loop counts it only when
``PRISMABUILD_TIER_OUTPUT_WINDOWS=1`` is set in its environment; unset,
every decision is the one it made before.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, produced_output as po, window_credit  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

import test_a_resident_range_is_adopted_rather_than_recopied as adopted  # noqa: E402
import test_produced_output_lifecycle_r2 as produced  # noqa: E402
import test_window_progress_protection as protection  # noqa: E402

TIER = produced.STAGE_TIER
KIND = produced.STAGE_BARE


def _tear(path: Path) -> None:
    """Replace a filed (read-only) record with bytes nothing can parse."""

    path.unlink()
    path.write_text("{torn")


@pytest.fixture()
def owner(tmp_path: Path):
    """A live producer that reserved its 2 GiB window at claim."""

    origin = tmp_path / "outputs"
    origin.mkdir()
    queue = produced._queue(tmp_path)
    template = produced._template(str(origin))
    bound = produced._bind_live(queue, tmp_path, template)
    assert po.admit_instance(queue, bound["instance"], template)["ok"] is True
    key = bound["owner"]
    assert queue.tier_ledger(TIER).holder_tokens(key) == {KIND: 2}
    return queue, key, bound, template


def test_a_held_window_owes_nothing(owner) -> None:
    queue, key, _bound, _template = owner
    owed = po.unheld_window_gib(queue, TIER)
    assert owed == {"gib": 0, "owners": {key: 0}, "unknown": [],
                    "bounded": []}


def test_a_window_retirement_returned_to_free_is_owed(owner) -> None:
    """The gap between a retirement and the owner's refill."""

    queue, key, _bound, _template = owner
    queue.tier_ledger(TIER).release(key)
    assert queue.tier_ledger(TIER).holder_tokens(key) == {}
    owed = po.unheld_window_gib(queue, TIER)
    assert owed == {"gib": 2, "owners": {key: 2}, "unknown": [],
                    "bounded": []}


def test_a_finished_owner_is_owed_nothing(owner) -> None:
    queue, key, _bound, _template = owner
    queue.tier_ledger(TIER).release(key)
    queue.finish(key, status="executed")
    assert po.unheld_window_gib(queue, TIER) == {
        "gib": 0, "owners": {}, "unknown": [], "bounded": []}


def test_a_dead_owners_torn_records_are_never_read(owner) -> None:
    """Liveness is checked before any record, so a dead scope costs nothing."""

    queue, key, bound, template = owner
    queue.tier_ledger(TIER).release(key)
    queue.finish(key, status="executed")
    _tear(po.template_path(queue.root, template))
    _tear(po.instance_dir(queue.root, bound["instance"]) / "instance.json")
    assert po.unheld_window_gib(queue, TIER)["gib"] == 0


def test_another_tier_is_owed_nothing(owner) -> None:
    queue, key, _bound, _template = owner
    queue.tier_ledger(TIER).release(key)
    assert po.unheld_window_gib(queue, "prismabuild-stage:elsewhere")["gib"] == 0


def test_a_batch_census_it_cannot_read_charges_the_upper_bound(
        owner, monkeypatch) -> None:
    """Outstanding batch tokens only lower the debt, so unknown counts none."""

    queue, key, _bound, _template = owner
    queue.tier_ledger(TIER).release(key)
    monkeypatch.setattr(type(queue), "_output_outstanding_window_tokens",
                        lambda self, owner_key, tier_id, kind: (1, True))
    owed = po.unheld_window_gib(queue, TIER)
    assert owed == {"gib": 2, "owners": {key: 2}, "unknown": [],
                    "bounded": [key]}


def test_a_live_owners_unreadable_template_is_unknown_not_zero(owner) -> None:
    queue, key, _bound, template = owner
    queue.tier_ledger(TIER).release(key)
    _tear(po.template_path(queue.root, template))
    owed = po.unheld_window_gib(queue, TIER)
    assert owed["gib"] is None, owed
    assert [entry["owner"] for entry in owed["unknown"]] == [key]


# ----------------------------------------------------------- the tier gate


def test_the_gate_counts_output_only_when_enforced() -> None:
    kwargs = dict(held_gib=4, ready_gib=0, capacity_gib=8, cur_min_gib=2,
                  next_min_gib=None, existing_min_next_gib=0)
    off = window_credit.gate_newcomer(output_gib=0, **kwargs)
    assert off["admit"] is True
    assert off["output_note"] == window_credit.OUTPUT_UNENFORCED_NOTE
    on = window_credit.gate_newcomer(output_gib=3, output_enforced=True,
                                     **kwargs)
    assert on["admit"] is False and on["reason"] == window_credit.REASON_STALL
    assert on["output_note"] == ""


def test_the_tier_loop_obligation_is_off_by_default(owner, monkeypatch) -> None:
    queue, key, _bound, _template = owner
    queue.tier_ledger(TIER).release(key)
    monkeypatch.delenv(tier_loop.OUTPUT_WINDOWS_ENV, raising=False)
    assert tier_loop.output_obligation(queue, TIER) == (
        0, False, window_credit.OUTPUT_UNENFORCED_NOTE, "")
    for value in ("", "0"):
        monkeypatch.setenv(tier_loop.OUTPUT_WINDOWS_ENV, value)
        assert tier_loop.output_obligation(queue, TIER)[:2] == (0, False)


def test_the_tier_loop_obligation_counts_the_owed_window(owner, monkeypatch) -> None:
    queue, key, _bound, _template = owner
    queue.tier_ledger(TIER).release(key)
    monkeypatch.setenv(tier_loop.OUTPUT_WINDOWS_ENV, "1")
    assert tier_loop.output_obligation(queue, TIER) == (2, True, "", "")


def test_an_unknown_obligation_is_an_error_not_zero(owner, monkeypatch) -> None:
    queue, _key, _bound, template = owner
    _tear(po.template_path(queue.root, template))
    monkeypatch.setenv(tier_loop.OUTPUT_WINDOWS_ENV, "1")
    gib, enforced, _note, error = tier_loop.output_obligation(queue, TIER)
    assert enforced is True and error, (gib, error)


def test_any_other_setting_is_refused(owner, monkeypatch) -> None:
    queue, _key, _bound, _template = owner
    monkeypatch.setenv(tier_loop.OUTPUT_WINDOWS_ENV, "yes")
    with pytest.raises(ValueError):
        tier_loop.output_obligation(queue, TIER)


def test_the_flag_is_off_unless_named() -> None:
    assert tier_loop._parser().parse_args([]).output_windows is False
    assert tier_loop._parser().parse_args(
        ["--output-windows"]).output_windows is True


def test_the_loop_refuses_to_start_on_any_other_setting(monkeypatch) -> None:
    monkeypatch.setenv(tier_loop.OUTPUT_WINDOWS_ENV, "true")
    with pytest.raises(SystemExit):
        tier_loop.main(["--interval-s", "5"])


# ------------------------------------------------ through the real tier loop


def _owes(gib):
    def census(_queue, tier_id):
        owed = gib if tier_id == TIER else 0
        return {"gib": owed, "owners": {}, "unknown": [], "bounded": []}
    return census


def _unknown(_queue, _tier_id):
    return {"gib": None, "owners": {},
            "unknown": [{"owner": "f" * 64, "error": "template unreadable"}],
            "bounded": []}


@pytest.mark.parametrize("enforced", [False, True])
def test_an_owed_window_gates_the_newcomer_that_fits_only_without_it(
        tmp_path: Path, monkeypatch, enforced: bool) -> None:
    """Six GiB fit two 1+1 windows, but not beside a 3 GiB owed window.

    Whether or not ``--output-windows`` is set, since #907: the commitment
    charges the owed window in the same decision as the two windows' read
    footprints (#905), 3 + 2 + 2 = 7 GiB against 6.  Unset, the joint-fit
    gate counts it as zero and would admit the second window on 2 + 2; set,
    it refuses on 3 + 2 + 2 as well, and the commitment names the refusal
    because no eviction can make that room.  The opt-in now decides only
    whether the joint-fit gate and the fence count the owed window.
    """

    ctx = protection._setup_two_consumers(tmp_path, stage_gib=6)
    queue = ctx["queue"]
    monkeypatch.setattr(po, "unheld_window_gib", _owes(3))
    if enforced:
        monkeypatch.setenv(tier_loop.OUTPUT_WINDOWS_ENV, "1")
    else:
        monkeypatch.delenv(tier_loop.OUTPUT_WINDOWS_ENV, raising=False)
    events = tier_loop.residency_window(queue, tiers=protection._tiers(tmp_path))
    gated = protection._gated(events)
    assert set(gated) == {protection.CONSUMER_B}, gated
    gate = gated[protection.CONSUMER_B]
    assert gate["reason"] == window_credit.REASON_COMMITMENT
    assert gate["commitment"]["unheld_output_gib"] == 3
    assert gate["commitment"]["committed_gib"] == 3 + 2
    assert gate["output_note"] == (
        "" if enforced else window_credit.OUTPUT_UNENFORCED_NOTE)
    assert protection._published(events) == set(ctx["aa"]["movers"])


def test_an_unknown_obligation_defers_the_tier(tmp_path: Path,
                                               monkeypatch) -> None:
    ctx = protection._setup_two_consumers(tmp_path, stage_gib=6)
    monkeypatch.setattr(po, "unheld_window_gib", _unknown)
    monkeypatch.setenv(tier_loop.OUTPUT_WINDOWS_ENV, "1")
    events = tier_loop.residency_window(ctx["queue"],
                                        tiers=protection._tiers(tmp_path))
    assert protection._published(events) == set()
    assert [e for e in events
            if e.get("event") == "advance-deferred-unknown-evidence"
            and "template unreadable" in str(e.get("error"))], events


@pytest.mark.parametrize("enforced,expected", [(False, 4), (True, 5)])
def test_the_relief_covers_the_owed_window(tmp_path: Path, monkeypatch,
                                           enforced: bool,
                                           expected: int) -> None:
    """The sweep must free what the gate will count, or the newcomer waits.

    Capacity 6: two finished movers' orphans (2 + 2), 2 free, and a
    newcomer's 2 + 2 window.  Without the owed GiB the admission shortfall
    is 2, so the relief asks free to reach 4; with it the gate needs one
    more, and a relief that ignored it would evict too little.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": 6})
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    for ordinal in (0, 1):
        adopted._stage_range(queue, mover=adopted._hexkey(f"stalemover{ordinal}"),
                             consumer="8" * 64, stage=stage, ordinal=ordinal,
                             manifest="e" * 64)
    adopted._publish_consumer(queue, adopted.SECOND,
                              adopted._plan(queue, adopted.SECOND,
                                            label="second"))
    assert queue.tier_ledger(TIER).available()["stage_gib"] == 2
    monkeypatch.setattr(po, "unheld_window_gib", _owes(1))
    if enforced:
        monkeypatch.setenv(tier_loop.OUTPUT_WINDOWS_ENV, "1")
    else:
        monkeypatch.delenv(tier_loop.OUTPUT_WINDOWS_ENV, raising=False)
    pressure = tier_loop.window_pressure(
        queue, tiers={TIER: adopted._tier_record(stage, gib=6)})
    assert pressure.get(TIER) == expected, pressure


@pytest.mark.parametrize("enforced,expected", [(False, 1), (True, 4)])
def test_the_next_phase_relief_leaves_the_owed_window_free(
        tmp_path: Path, monkeypatch, enforced: bool, expected: int) -> None:
    """An admitted window's fence counts the owed GiB, so its relief must too.

    Consumer A's movers are published and hold nothing; the next-phase term
    asks the tier for one GiB.  Enforced with 3 GiB owed, the fence check
    needs those 3 free as well, and a relief that asked for 1 would leave the
    advance waiting beside reclaimable orphans.
    """

    ctx = protection._setup_two_consumers(tmp_path, stage_gib=6)
    queue = ctx["queue"]
    tiers = protection._tiers(tmp_path)
    monkeypatch.setattr(po, "unheld_window_gib", _owes(3))
    if enforced:
        monkeypatch.setenv(tier_loop.OUTPUT_WINDOWS_ENV, "1")
    else:
        monkeypatch.delenv(tier_loop.OUTPUT_WINDOWS_ENV, raising=False)
    tier_loop.residency_window(queue, tiers=tiers)
    assert queue.item_path(pool.READY, ctx["aa"]["movers"][0]).exists()
    assert tier_loop.window_pressure(queue, tiers=tiers).get(TIER) == expected


def test_an_unknown_obligation_asks_no_relief(tmp_path: Path,
                                              monkeypatch) -> None:
    ctx = protection._setup_two_consumers(tmp_path, stage_gib=6)
    queue = ctx["queue"]
    tiers = protection._tiers(tmp_path)
    tier_loop.residency_window(queue, tiers=tiers)
    assert tier_loop.window_pressure(queue, tiers=tiers).get(TIER) == 1
    monkeypatch.setattr(po, "unheld_window_gib", _unknown)
    monkeypatch.setenv(tier_loop.OUTPUT_WINDOWS_ENV, "1")
    assert TIER not in tier_loop.window_pressure(queue, tiers=tiers)
