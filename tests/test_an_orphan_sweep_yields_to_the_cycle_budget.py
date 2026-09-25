"""An orphan sweep's evictions are cycle-budget units (#1136).

Live shape (dl380g10, 2026-09-25): one tier cycle took 287.2 s, 227.5 s of
it in ``sweep_orphans``.  Forty-seven orphans were evicted one after
another, each behind a 1.1-13.1 s owner census, with the storage pool
saturated by movers.  The loop hands the sweep its `tier_loop.Liveness`
(#1072), but only the dead-owner census asked it for permission; the
eviction loop never called ``budget.start`` or ``budget.done``.  So nothing
re-announced the tier record between evictions, and nothing stopped the
sweep at the cycle's budget.  The stage record aged past the 120 s bound,
and a stage-fed GPU row died with ``StagedRangeNotLanded``.

Every eviction here costs a fixed time on a fake clock, the one the
`Liveness` stamps with (``pool._now``), and nothing else takes any time.
The bound is scaled down so that two evictions fit the budget and a third
does not.  What must hold:

* the sweep stops at the budget and leaves the rest resident, charged and
  uncharged orphans alike;
* the tier record is re-announced between evictions, so a reader never
  sees it older than the liveness horizon;
* the next cycle starts from the first orphan the budget refused;
* without a budget, the sweep still evicts everything the pressure asks for.

Every fixture is a temp stage root registered to a temp queue (never a real
/stage or /ram), and the staged files are sparse.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import test_uncharged_dead_owners_give_room_under_pressure as unc  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
from prismabuild import pool, storage_tiers  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = unc.TIER
#: The unit kind the sweep asks the budget for.  Read softly, so that a tree
#: without it fails on the behaviour below rather than on an import.
UNIT = getattr(stage_release, "ORPHAN_EVICT_UNIT", "orphan-evict")
#: The liveness terms, scaled down: ``H = L - P`` = 50 s, and the budget
#: ``b = H - W - I - T`` = 45 s, since no write and nothing outside the
#: units takes time on the fake clock.
BOUND_S = 60.0
POLL_S = 10.0
INTERVAL_S = 5.0
HORIZON_S = BOUND_S - POLL_S
#: What one eviction costs.  Two fit the budget (the second starts at
#: ``20 + 20 <= 45``); the third does not (``40 + 20 > 45``).
EVICT_S = 20.0
PER_CYCLE = 2
#: More room than every orphan together can give: pressure never stops the
#: sweep, so only the budget can.
NEEDED_GIB = 100
REASONS = ("orphan-sweep", "uncharged-owner-sweep")


class FakeClock:
    """``pool._now`` for the test: moves only when an eviction runs."""

    def __init__(self) -> None:
        self.now = time.time()

    def __call__(self) -> float:
        return self.now


class _Sweeps:
    """The fleet, the clock, the budget, and what each eviction saw."""

    def __init__(self, fleet, monkeypatch: pytest.MonkeyPatch) -> None:
        self.fleet = fleet
        self.queue, self.stage, _ = fleet
        self.clock = FakeClock()
        monkeypatch.setattr(pool, "_now", self.clock)
        self.liveness = tier_loop.Liveness(
            interval_s=INTERVAL_S, bound_s=BOUND_S, poll_s=POLL_S)
        #: Every orphan eviction, in order: ``(mover, record age at start)``.
        self.evicted: list[tuple[str, float | None]] = []
        self._held = 0
        real_evict = stage_release.evict

        def evict(queue, mover, *args, **kwargs):
            if kwargs.get("reason") in REASONS:
                self.evicted.append((str(mover), self.age()))
                self.clock.now += EVICT_S
            return real_evict(queue, mover, *args, **kwargs)

        monkeypatch.setattr(stage_release, "evict", evict)

    def owner(self, name: str, unix: float, *, charged: bool = False,
              ) -> tuple[str, str]:
        """One dead owner of 1 GiB; a charged one holds a stage token."""

        if not charged:
            return unc.dead_owner(self.fleet, [name], unix=unix)
        # ``dead_owner`` mints the tier to the new owner's size, which would
        # retire the token an earlier charged owner holds: mint it to all.
        held = self._held
        real_mint = self.queue.mint_tier_capacity
        self.queue.mint_tier_capacity = lambda tier, tokens: real_mint(
            tier, {kind: count + held for kind, count in tokens.items()})
        try:
            made = unc.dead_owner(self.fleet, [name], unix=unix, charged=True)
        finally:
            del self.queue.mint_tier_capacity
        self._held += 1
        return made

    def record(self) -> dict[str, object]:
        return json.loads(self.queue.tier_record_path(TIER).read_text())

    def age(self) -> float | None:
        """The tier record's age as a reader computes it; none unannounced."""

        path = self.queue.tier_record_path(TIER)
        if not path.exists():
            return None
        return self.clock() - float(self.record()["announced_unix"])

    def cycle(self, *, budget: bool = True, needed: int = NEEDED_GIB,
              ) -> tuple[list[str], dict[str, object]]:
        """One tier cycle's sweep; the movers it evicted, and the cycle line."""

        before = len(self.evicted)
        liveness = self.liveness if budget else None
        if liveness is not None:
            liveness.begin_cycle()
            liveness.announce(self.queue, {
                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                "tier_id": TIER, "host": "fixture", "tier": "stage",
                "mountpoint": str(self.stage), "capacity_bytes": 700 * unc.GIB})
        stage_release.sweep(
            self.queue, stage_roots={TIER: str(self.stage)},
            pressure={TIER: needed},
            **({} if liveness is None else {"budget": liveness}))
        line = ({} if liveness is None
                else liveness.end_cycle(completed=True))
        return [mover for mover, _age in self.evicted[before:]], line


@pytest.fixture(autouse=True)
def _fresh_process_state():
    stage_release.reset_skip_checkpoints()
    stage_release.reset_holder_reports()
    yield
    stage_release.reset_skip_checkpoints()
    stage_release.reset_holder_reports()


def _five(sweeps: _Sweeps) -> list[tuple[str, str]]:
    """Five orphans, oldest first, charged and uncharged interleaved."""

    return [sweeps.owner(f"orphan-{index:05d}", 100.0 * (index + 1),
                         charged=bool(index % 2))
            for index in range(5)]


def test_the_sweep_stops_at_the_budget_and_leaves_the_rest(fleet, monkeypatch):
    sweeps = _Sweeps(fleet, monkeypatch)
    owners = _five(sweeps)

    evicted, line = sweeps.cycle()

    assert evicted == [owners[0][1], owners[1][1]], (
        f"the sweep evicted {len(evicted)} orphans at {EVICT_S:g} s each "
        f"against a {HORIZON_S - INTERVAL_S:g} s budget; "
        f"{PER_CYCLE} fit: {line}")
    assert [unc._owned(sweeps.queue, *owner) for owner in owners] == [
        False, False, True, True, True]
    assert line["units"].get(UNIT) == PER_CYCLE, line
    assert line["deferred"].get(UNIT, 0) >= 1, line
    assert line["overran"] is False, line


def test_the_record_is_reannounced_between_evictions(fleet, monkeypatch):
    sweeps = _Sweeps(fleet, monkeypatch)
    _five(sweeps)
    first: tuple[dict[str, object], dict[str, object]] | None = None

    for _ in range(3):
        before = len(sweeps.evicted)
        _evicted, line = sweeps.cycle()
        if first is None:
            first = line, sweeps.record()
        ages = [float(age) for _mover, age in sweeps.evicted[before:]
                if age is not None]
        assert ages and max(ages) < HORIZON_S, (
            f"an eviction started with the tier record {max(ages):.1f} s old "
            f"against the {HORIZON_S:g} s horizon: {line}")
        assert line["oldest_age_s"] < HORIZON_S, line
    # The first cycle's second eviction ends with the record 40 s old and a
    # 20 s stretch ahead of it: it is re-announced there, after that unit.
    assert first is not None
    line, record = first
    assert line["refreshes"] >= 1, line
    assert (record.get("liveness_refresh") or {}).get("after") == UNIT, record


def test_the_next_cycle_resumes_from_the_first_refused_orphan(
        fleet, monkeypatch):
    sweeps = _Sweeps(fleet, monkeypatch)
    owners = _five(sweeps)
    first, _line = sweeps.cycle()
    assert first == [owners[0][1], owners[1][1]], first
    refused = owners[2]
    # An orphan older than every other appears between the cycles.  Oldest
    # first alone would take it next; the budget's order starts from the
    # orphan it refused, so that one goes first.
    older = sweeps.owner("orphan-older", 50.0)

    second, _line = sweeps.cycle()

    assert second[:1] == [refused[1]], (
        f"the next cycle started from {second[:1]}, not from the refused "
        f"orphan {refused[1][:12]}: {second}")
    assert second == [refused[1], owners[3][1]], second
    assert unc._owned(sweeps.queue, *older)


@pytest.mark.parametrize("needed,left", [(3, 2), (NEEDED_GIB, 0)],
                         ids=["room-for-three", "room-for-all"])
def test_without_a_budget_the_sweep_evicts_what_the_pressure_asks(
        fleet, monkeypatch, needed, left):
    sweeps = _Sweeps(fleet, monkeypatch)
    owners = [sweeps.owner(f"free-{index:05d}", 100.0 * (index + 1))
              for index in range(5)]

    evicted, _line = sweeps.cycle(budget=False, needed=needed)

    assert evicted == [owner[1] for owner in owners[:len(owners) - left]], (
        evicted)
    assert [unc._owned(sweeps.queue, *owner) for owner in owners] == (
        [False] * (len(owners) - left) + [True] * left)
