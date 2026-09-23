"""Admission charges consumers' refill horizons jointly (#907, #905).

#903 bounded each window by its refill horizon: the phase a consumer reads,
the bytes it can hold ahead of that phase, and enough further ranges to cover
a copy's landing time.  Admission did not follow.  The joint-fit gate admits
a newcomer when its *minimum* fits -- held + queued + its current + its next
-- and the window then grows into whatever room is free, up to the horizon.
Nothing bounded the *sum* of the horizons, so two admitted windows could
between them want more of the stage than it has.  Ranges inside a horizon
are never evicted, so one reader then waits on the other's reading: a range
miss, a 300 s range wait, a stall.

The fix charges every admitted window's **read footprint** -- the most stage
GiB its window will ever publish at once over the rest of its plan -- when a
newcomer is admitted, together with the tier's unheld produced-output windows
(#905, whether or not ``--output-windows`` is set).  The newcomer is admitted
when everything already committed plus its own footprint fits the tier.

The numbers every case here uses, so each assertion can be checked by hand:

* phases of 2 GiB; a consumer reserves ``mem_gb`` 8, so it can hold 8 GiB
  ahead of the phase it reads;
* a claimed reader was claimed 1000 s ago and entered ``phase-0`` 10 s ago:
  2 GiB in 990 s, 2.2 MB/s; its landed copies took 200 s each.  A copy lands
  within 30 s (heartbeat) + 60 s (cycle) + 200 s = 290 s, in which it reads
  0.6 GB, so its refill is the one-leg minimum.  Its footprint is the phase it
  reads, 8 GiB of read-ahead and one refill leg: **12 GiB**;
* a newcomer has no measured rate, so the tier's announced fill supply,
  10 MB/s, prices both its consumption and its landing: a 2 GiB copy lands in
  215 s, 305 s with the latency, in which it reads 3.0 GB -- two legs.  Its
  footprint is 2 + 8 + 4 = **14 GiB**.

``tests/fixtures`` holds no live numbers for this: the 2026-09-23 survey is in
the PR, and each case below is the smallest state that shows one term.

Everything runs on a ``tmp_path`` queue and stage root; nothing touches a live
queue or a real stage mountpoint (#628).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, produced_output as po, residency_plan  # noqa: E402
from prismabuild import storage_tiers, window_credit  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    GIB, STAGE_KIND, TIER, _hexkey, _row, assert_ledger_matches_the_stage)
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _claim, _cycle, _fixture_queue, _land)

PHASE_GIB = 2
MEM_GB = 8
#: Each landed copy took this long: 2 GiB / 200 s = 10.7 MB/s.
MOVER_SECONDS = 200.0
CLAIMED_AGO_S = 1000.0
REPORTED_AGO_S = 10.0
#: The tier's announced fill supply, MB/s: a newcomer's consumption and
#: landing before either is measured.
SUPPLY_MB_S = 10
#: Footprints by the module docstring's arithmetic.
READER_FOOTPRINT = 12
NEWCOMER_FOOTPRINT = 14


class Consumer:
    """One consumer of its own manifest: ``phases`` phases of ``PHASE_GIB``.

    ``output_gib`` is a claim-time stage demand -- a produced-output window --
    when positive.
    """

    def __init__(self, queue: pool.PoolQueue, stage: Path, label: str, *,
                 phases: int = 10, mem_gb: int = MEM_GB,
                 output_gib: int = 0) -> None:
        self.queue, self.stage, self.label = queue, stage, label
        self.key = _hexkey(f"{label}consumer")
        self.manifest = _hexkey(f"{label}manifest")
        self.phases = phases
        total = phases * PHASE_GIB * GIB
        built = []
        for ordinal in range(phases):
            start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
            built.append({
                "name": f"phase-{ordinal}",
                "start_bytes": start, "end_bytes": end, "stage_gib": PHASE_GIB,
                "mover_row": {
                    **_row(queue, self.mover(ordinal),
                           {STAGE_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
                    "residency": {
                        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": self.manifest,
                        "manifest_bytes": total,
                        "range_start_bytes": start, "range_end_bytes": end},
                },
                "egress_row": _row(queue, _hexkey(f"{label}egress{ordinal}"),
                                   {"mem_gb": 1}),
            })
        self.plan = residency_plan.build_plan(
            consumer_action_key=self.key, tier_id=TIER,
            stage_root="/stage/prewarm", manifest_sha256=self.manifest,
            manifest_bytes=total, phases=built)
        residency_plan.freeze(queue, self.plan)
        resources: dict[str, int] = {"cpu": 1, "mem_gb": mem_gb}
        if output_gib:
            resources[STAGE_KIND] = output_gib
        queue.publish(
            action_key=self.key, cas_root=queue.root / "cas",
            checkout_root=queue.root / "co",
            worker_script=queue.root / "worker.py", resources=resources,
            residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                       "manifest_sha256": self.manifest,
                       "manifest_bytes": total,
                       "leads": residency_plan.leads_for(self.plan)})

    def mover(self, ordinal: int) -> str:
        return _hexkey(f"{self.label}mover{ordinal}")

    def land(self, *ordinals: int, consumer: str | None = None) -> None:
        """Stage ``ordinals`` as a finished mover leaves them.

        ``consumer`` files the copy under another consumer's name -- a
        withdrawn donor whose range this consumer can adopt.
        """

        for ordinal in ordinals:
            mover = (self.mover(ordinal) if consumer is None
                     else _hexkey(f"donor{self.label}{ordinal}"))
            _land(self.queue, self.stage, consumer=consumer or self.key,
                  manifest=self.manifest, mover=mover,
                  name=f"phase-{ordinal}", start=ordinal * PHASE_GIB * GIB,
                  end=(ordinal + 1) * PHASE_GIB * GIB, seconds=MOVER_SECONDS)

    def claim(self, phase: str = "phase-0", *,
              claimed_ago: float = CLAIMED_AGO_S) -> None:
        now = time.time()
        _claim(self.queue, self.key, phase=phase,
               claimed_unix=now - claimed_ago,
               reported_unix=now - REPORTED_AGO_S)

    def held(self, ordinal: int) -> bool:
        return bool(self.queue.tier_ledger(TIER).holder_tokens(self.mover(ordinal)))

    def lead_published(self) -> bool:
        return (self.queue.item_path(pool.READY, self.mover(0)).exists()
                or self.held(0))


def _tiers(stage: Path, *, supply: int | None = SUPPLY_MB_S
           ) -> dict[str, dict[str, object]]:
    """The stage tier as this box announces it, with its fill supply."""

    tokens: dict[str, int] = {}
    if supply is not None:
        tokens[storage_tiers.FILL_KIND] = supply
    return {TIER: {"tier_id": TIER, "tier": "stage", "mountpoint": str(stage),
                   "tokens": tokens}}


def _gate(events: list[dict[str, object]], key: str) -> dict[str, object] | None:
    gates = [event for event in events if event.get("event") == "window-gated"
             and event.get("consumer") == key]
    assert len(gates) <= 1, gates
    return gates[0] if gates else None


def _refused(events: list[dict[str, object]], key: str) -> dict[str, object]:
    """The commitment refusal filed for ``key``, with its terms."""

    gate = _gate(events, key)
    assert gate is not None, f"{key[:8]} was admitted: {events}"
    assert gate["reason"] == window_credit.REASON_COMMITMENT, gate
    assert gate["permanent"] is False
    terms = gate["commitment"]
    assert isinstance(terms, dict), gate
    return terms


def _reader(queue: pool.PoolQueue, stage: Path, *,
            landed: tuple[int, ...] = tuple(range(6)),
            phases: int = 10) -> Consumer:
    """A claimed reader inside ``phase-0`` with ``landed`` phases staged.

    By default exactly its footprint: phases 0 to 5 (12 GiB), so it will ask
    the tier for nothing more than it gives back as it reads.
    """

    reader = Consumer(queue, stage, "a", phases=phases)
    reader.land(*landed)
    reader.claim()
    return reader


# ------------------------------------------------------------ two horizons


@pytest.mark.parametrize("capacity,admitted", [(24, False), (26, True)])
def test_a_newcomer_is_admitted_only_when_both_footprints_fit(
        tmp_path: Path, capacity: int, admitted: bool) -> None:
    """The #907 case: a running reader's window and a newcomer's, jointly.

    The reader holds its whole footprint, 12 GiB, and the newcomer's is
    14 GiB: 26 GiB between them.  Before the fix a 24 GiB stage admitted the
    newcomer on its minimum -- 12 held + 2 current + 2 next = 16 -- and the
    two windows then wanted 26 GiB of 24.  After it, the newcomer waits until
    26 GiB is there, and the refusal names every term.
    """

    queue, stage = _fixture_queue(tmp_path, capacity)
    _reader(queue, stage)
    newcomer = Consumer(queue, stage, "b")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert newcomer.lead_published() is admitted
    if admitted:
        assert _gate(events, newcomer.key) is None
        return
    terms = _refused(events, newcomer.key)
    assert terms["capacity_gib"] == capacity
    assert terms["footprint_gib"] == NEWCOMER_FOOTPRINT
    assert terms["holding_gib"] == 0
    assert terms["committed_gib"] == READER_FOOTPRINT
    assert terms["committed_gib"] + terms["growth_gib"] > capacity


def test_the_first_admitted_newcomer_is_charged_before_the_second(
        tmp_path: Path) -> None:
    """Two newcomers in one pass: the second sees the first's footprint.

    Each footprint is 14 GiB; a 28 GiB stage fits both, a 27 GiB one only
    the first.  Before the fix both were admitted on 2 + 2 each.
    """

    queue, stage = _fixture_queue(tmp_path, 27)
    first = Consumer(queue, stage, "b")
    second = Consumer(queue, stage, "c")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert first.lead_published()
    assert not second.lead_published()
    terms = _refused(events, second.key)
    assert terms["committed_gib"] == NEWCOMER_FOOTPRINT


# --------------------------------------------------------- declared outputs


@pytest.mark.parametrize("owed,admitted", [(0, True), (5, False)])
def test_an_owed_output_window_is_charged_without_the_opt_in(
        tmp_path: Path, monkeypatch, owed: int, admitted: bool) -> None:
    """#905: a producer's unheld output window is charged in the same decision.

    12 GiB of reader footprint, 14 of newcomer and 5 owed is 31 GiB against
    30.  ``--output-windows`` is off, so the joint-fit gate counts the owed
    window as zero and admits the newcomer on 16 GiB; the commitment counts
    it regardless, because the producer will take that room back from free.
    """

    monkeypatch.delenv(tier_loop.OUTPUT_WINDOWS_ENV, raising=False)
    monkeypatch.setattr(po, "unheld_window_gib", _owes(owed))
    queue, stage = _fixture_queue(tmp_path, 30)
    _reader(queue, stage)
    newcomer = Consumer(queue, stage, "b")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert newcomer.lead_published() is admitted
    if not admitted:
        terms = _refused(events, newcomer.key)
        assert terms["unheld_output_gib"] == owed
        assert terms["committed_gib"] == READER_FOOTPRINT + owed


def test_an_unreadable_output_census_defers_only_the_newcomer(
        tmp_path: Path, monkeypatch) -> None:
    """An owed window the census cannot read is not zero (the silent zero).

    Without the opt-in the newcomer waits, named as unknown evidence; the
    admitted reader's own window is not deferred with it.
    """

    monkeypatch.delenv(tier_loop.OUTPUT_WINDOWS_ENV, raising=False)
    monkeypatch.setattr(po, "unheld_window_gib", _unknown)
    queue, stage = _fixture_queue(tmp_path, 40)
    reader = _reader(queue, stage, landed=tuple(range(5)))
    newcomer = Consumer(queue, stage, "b")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert not newcomer.lead_published()
    gate = _gate(events, newcomer.key)
    assert gate is not None
    assert gate["reason"] == window_credit.REASON_DEFER_UNKNOWN
    assert "template unreadable" in str(gate["commitment"]["error"])
    # The reader's last in-horizon phase still publishes.
    assert queue.item_path(pool.READY, reader.mover(5)).exists()


def _owes(gib: int):
    def census(_queue, tier_id):
        return {"gib": gib if tier_id == TIER else 0, "owners": {},
                "unknown": [], "bounded": []}
    return census


def _unknown(_queue, _tier_id):
    return {"gib": None, "owners": {},
            "unknown": [{"owner": "f" * 64, "error": "template unreadable"}],
            "bounded": []}


# ------------------------------------------------------ adoption is admission


@pytest.mark.parametrize("capacity,adopted", [(24, False), (26, True)])
def test_adopting_a_newcomers_lead_passes_the_same_commitment(
        tmp_path: Path, capacity: int, adopted: bool) -> None:
    """A lead taken over from a withdrawn donor is the newcomer's admission.

    Adoption moves tokens instead of acquiring them, and a consumer whose
    lead holds tokens counts as admitted, so before the fix a successor --
    R13 over R12's ranges -- was admitted by adoption and never met the
    gate.  The donor's 2 GiB are an orphan the sweep could take, so they are
    not committed; the reader's 12 and the newcomer's 14 are.
    """

    queue, stage = _fixture_queue(tmp_path, capacity)
    _reader(queue, stage)
    newcomer = Consumer(queue, stage, "b")
    donor = "dead"
    newcomer.land(0, consumer=_hexkey(f"{donor}consumer"))

    events = tier_loop.adopt_resident_ranges(queue, tiers=_tiers(stage))

    assert newcomer.held(0) is adopted
    if adopted:
        return
    deferred = [event for event in events
                if event.get("event") == "adoption-deferred"
                and event.get("consumer") == newcomer.key]
    assert len(deferred) == 1, events
    assert deferred[0]["reason"] == window_credit.REASON_COMMITMENT
    terms = deferred[0]["commitment"]
    assert terms["evictable_gib"] == PHASE_GIB
    assert terms["committed_gib"] == READER_FOOTPRINT
    assert terms["footprint_gib"] == NEWCOMER_FOOTPRINT
    assert_ledger_matches_the_stage(queue)


# ------------------------------------------------ what counts as committed


def test_an_admitted_window_that_has_not_grown_is_charged_its_growth(
        tmp_path: Path) -> None:
    """A ready consumer whose lead has landed will claim and grow to 14 GiB.

    It holds 2 GiB now.  Before the fix the gate charged it one protected
    next (2 GiB) and admitted a newcomer on 2 + 2 + 2 + 2 = 8 of 24; after
    it, the admitted window's 12 GiB of growth is committed: 2 + 12 + 14 =
    28 > 24.
    """

    queue, stage = _fixture_queue(tmp_path, 24)
    admitted = Consumer(queue, stage, "b")
    admitted.land(0)
    newcomer = Consumer(queue, stage, "n")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert not newcomer.lead_published()
    terms = _refused(events, newcomer.key)
    assert terms["admitted_growth_gib"] == NEWCOMER_FOOTPRINT - PHASE_GIB
    assert terms["committed_gib"] == NEWCOMER_FOOTPRINT


def test_a_holder_nothing_can_evict_is_committed(tmp_path: Path) -> None:
    """Tokens with no receipt and no live owner stay: they are committed.

    The receipt-less holder is the live ``6fbc96301c6c`` shape: not an
    orphan the sweep can take, and no window's.  10 + 12 + 14 = 36 > 30,
    where the joint-fit gate admitted on 10 + 12 + 2 + 2 = 26.
    """

    queue, stage = _fixture_queue(tmp_path, 30)
    assert queue.tier_ledger(TIER).acquire(_hexkey("static"), {"stage_gib": 10})
    _reader(queue, stage)
    newcomer = Consumer(queue, stage, "n")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert not newcomer.lead_published()
    terms = _refused(events, newcomer.key)
    assert terms["committed_gib"] == 10 + READER_FOOTPRINT


@pytest.mark.parametrize("owed,admitted", [(0, True), (1, False)])
def test_a_lone_newcomer_beside_holders_that_never_grow_is_admitted(
        tmp_path: Path, monkeypatch, owed: int, admitted: bool) -> None:
    """Policy, not soundness: a static holder cannot block a lone newcomer forever.

    10 GiB held by nobody that will ever ask for room, and a newcomer whose
    footprint is 14 GiB, on a 20 GiB stage.  Nothing else on the tier grows,
    so the newcomer contends only with itself: its window runs short of its
    footprint, as it would have before the fix, and it is admitted under the
    joint-fit gate alone.  Refusing it would be a refusal no later cycle
    could lift.  An owed output window is growth, so beside one the newcomer
    waits for it like any other.
    """

    monkeypatch.delenv(tier_loop.OUTPUT_WINDOWS_ENV, raising=False)
    monkeypatch.setattr(po, "unheld_window_gib", _owes(owed))
    queue, stage = _fixture_queue(tmp_path, 20)
    assert queue.tier_ledger(TIER).acquire(_hexkey("static"), {"stage_gib": 10})
    newcomer = Consumer(queue, stage, "n")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert newcomer.lead_published() is admitted
    if not admitted:
        assert _refused(events, newcomer.key)["unheld_output_gib"] == owed


# ------------------------------------------------------------- the ratchet


def test_a_reader_that_slows_keeps_the_footprint_it_was_charged(
        tmp_path: Path) -> None:
    """A slower rate shortens the horizon but not the commitment.

    One claim, 5000 s ago, of a 16-phase reader.  It reported ``phase-4``
    990 s in -- 10 GiB in 990 s, a two-leg refill and a 14 GiB footprint --
    and ``phase-5`` 4990 s in: 12 GiB in 4990 s, a one-leg refill and a
    12 GiB footprint.  Holding phases 5 to 9 with ``phase-10`` queued, it has
    grown to 12, so priced at the later rate it asks for nothing more, and a
    26 GiB stage would fit the newcomer's 14 beside it.  But a reader that
    slowed can speed up again, and the room a newcomer took meanwhile is the
    room its window grows back into.  The commitment keeps the fastest rate
    this claim has shown.
    """

    queue, stage = _fixture_queue(tmp_path, 26)
    reader = Consumer(queue, stage, "a", phases=16)
    reader.land(*range(4, 10))
    now = time.time()
    claimed = now - 5000.0
    _claim(queue, reader.key, phase="phase-4", claimed_unix=claimed,
           reported_unix=claimed + 990.0)
    newcomer = Consumer(queue, stage, "n")
    tiers = _tiers(stage)

    first = tier_loop.residency_window(queue, tiers=tiers)
    assert not newcomer.lead_published()
    assert _refused(first, newcomer.key)["admitted_growth_gib"] == 2

    # The first pass published ``phase-10``, inside the 14 GiB horizon; it
    # is queued, so it counts as the reader's own and as queued demand once.
    assert queue.item_path(pool.READY, reader.mover(10)).exists()
    _report(queue, reader.key, phase="phase-5", reported_unix=now - 10.0)

    second = tier_loop.residency_window(queue, tiers=tiers)
    assert not newcomer.lead_published()
    terms = _refused(second, newcomer.key)
    # phase-4 is passed: evictable, not committed.
    assert terms["evictable_gib"] == PHASE_GIB
    assert terms["admitted_growth_gib"] == 2


def _report(queue: pool.PoolQueue, key: str, *, phase: str,
            reported_unix: float) -> None:
    """File a later accepted phase under the same claim."""

    item = json.loads(queue.item_path(pool.CLAIMED, key).read_text())
    queue.write_lease(
        key, owner="horizon-fixture", claim_snapshot=item,
        progress_observation={
            "source": "action-progress",
            "last_accepted": {"phase": phase, "units_completed": 2,
                              "reported_unix": reported_unix}})


# ----------------------------------------------------- the relief it asks


def test_a_newcomer_the_commitment_refuses_evicts_nothing(
        tmp_path: Path, capsys) -> None:
    """No eviction for a newcomer that eviction cannot admit (#632).

    The reader holds all ten phases, 20 GiB of a 21 GiB stage; ``phase-7``
    to ``phase-9`` are past its horizon.  Before the fix the newcomer's
    joint-fit shortfall (20 + 2 + 2 - 21 = 3) evicted ``phase-9`` and
    ``phase-8`` and its lead published into 4 GiB beside a reader whose
    window still wants 14.  After it, the commitment refuses the newcomer
    -- no fill supply is announced here, so its footprint is its whole
    20 GiB plan -- and nothing is evicted for it.
    """

    queue, stage = _fixture_queue(tmp_path, 21)
    reader = _reader(queue, stage, landed=tuple(range(10)))
    newcomer = Consumer(queue, stage, "n")

    capsys.readouterr()
    _cycle(queue, stage, gib=21)
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()
              if line.startswith("{")]

    assert not newcomer.lead_published()
    for ordinal in range(10):
        assert reader.held(ordinal), ordinal
    assert_ledger_matches_the_stage(queue)
    terms = _refused(events, newcomer.key)
    assert terms["evictable_gib"] == 3 * PHASE_GIB
    assert terms["footprint_gib"] == 10 * PHASE_GIB
