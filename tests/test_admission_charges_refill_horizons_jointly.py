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

Each case up to the last section is the smallest state that shows one term.
The last section replays ``tests/fixtures/r12_stage_20260922.json`` -- R12,
the native capture and Stage-B-shaped quanta on the live stage's numbers.

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
import test_r12_and_the_capture_replay_under_the_refill_horizon as replay  # noqa: E402

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


@pytest.mark.parametrize("landed", [(0, 1), tuple(range(6))])
def test_a_reader_whose_plan_does_not_read_defers_the_newcomer(
        tmp_path: Path, landed: tuple[int, ...]) -> None:
    """A live reader missing from the census is not free room (#907 review).

    A plan read fails on a torn write or the mount's quarter-hourly ESTALE
    (#575), and the reader drops out of that pass's census.  Before the fix
    its ranges then counted as orphans -- their receipts name a consumer the
    census could not see -- and its growth as nothing, so the newcomer was
    admitted alone on 0 of 24.  The next pass reads the plan, and the
    reader's window wants its 12 GiB footprint back beside the newcomer's
    14.  Now the newcomer waits for a census that reads, both when the
    reader holds part of its footprint (4 GiB) and all of it (12).
    """

    queue, stage = _fixture_queue(tmp_path, 24)
    reader = _reader(queue, stage, landed=landed)
    newcomer = Consumer(queue, stage, "b")
    plan_path = Path(queue.residency_plan_path(reader.key))
    plan_path.unlink()
    plan_path.write_text("{")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert not newcomer.lead_published()
    gate = _gate(events, newcomer.key)
    assert gate is not None
    assert gate["reason"] == window_credit.REASON_DEFER_UNKNOWN
    assert "not censused" in str(gate["commitment"]["error"])
    assert tier_loop.window_pressure(queue, tiers=_tiers(stage)).get(TIER) is None


def test_a_ready_window_whose_plan_does_not_read_defers_the_newcomer(
        tmp_path: Path) -> None:
    """The same for a window admitted but not yet claimed.

    Its lead has landed, so it is an admitted window (#908): it will claim
    and grow to its 14 GiB footprint.  With its plan unreadable, only its
    queue item says so -- the tier its residency names, and a lead that
    holds tokens -- and that is enough to keep the newcomer out of the room
    it will grow into.  A ready consumer whose leads are all unpublished is
    the other case: admitted nowhere, it blinds nothing
    (``test_unknown_plan_defers_only_its_consumer``).
    """

    queue, stage = _fixture_queue(tmp_path, 24)
    admitted = Consumer(queue, stage, "a")
    admitted.land(0)
    newcomer = Consumer(queue, stage, "b")
    plan_path = Path(queue.residency_plan_path(admitted.key))
    plan_path.unlink()
    plan_path.write_text("{")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert not newcomer.lead_published()
    gate = _gate(events, newcomer.key)
    assert gate is not None
    assert gate["reason"] == window_credit.REASON_DEFER_UNKNOWN
    assert "not censused" in str(gate["commitment"]["error"])
    assert tier_loop.window_pressure(queue, tiers=_tiers(stage)).get(TIER) is None


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

    It holds its 2 GiB lead, and the pass fences 2 GiB more for its advance
    before any newcomer is asked.  Before the fix the gate charged it that
    protected next and admitted a newcomer on 2 + 2 + 2 + 2 = 8 of 24; after
    it, the admitted window is committed at its whole footprint -- the 4 GiB
    it holds and 10 of growth -- and 14 + 14 = 28 > 24.
    """

    queue, stage = _fixture_queue(tmp_path, 24)
    admitted = Consumer(queue, stage, "b")
    admitted.land(0)
    newcomer = Consumer(queue, stage, "n")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert not newcomer.lead_published()
    terms = _refused(events, newcomer.key)
    # What the admitted window holds (its lead and the fence) plus what it
    # has still to grow is its footprint, however the pass split the two.
    assert terms["held_gib"] + terms["admitted_growth_gib"] == NEWCOMER_FOOTPRINT
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
    this claim has attained: at the ``phase-4`` report it had read at least
    the 8 GiB before ``phase-4``, 8.7 MB/s, still a two-leg refill.
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


def test_a_first_report_just_after_the_claim_does_not_freeze_the_footprint(
        tmp_path: Path) -> None:
    """The horizon's in-phase over-estimate is priced while it lasts, not kept.

    The reader reports ``phase-0`` 5 s after its claim.  The horizon counts
    the whole accepted phase as read: 2 GiB in 5 s, 429 MB/s, a refill that
    covers the plan, so its window would publish all ten phases now, and its
    footprint is 20 GiB.  At its next report, ``phase-1`` 990 s in, that rate
    is 4 GiB in 990 s (4.3 MB/s): a one-leg refill and a 12 GiB footprint,
    and a 26 GiB stage fits the newcomer's 14 beside it.  Before the fix the
    ratchet kept the 429 MB/s for the claim's lifetime, priced the reader at
    the 18 GiB left of its plan, and refused every newcomer beside it.  The
    ratchet now keeps only what the reader has certainly read -- the phases
    before the one it reports -- and ``phase-0`` has none before it.
    """

    queue, stage = _fixture_queue(tmp_path, 26)
    reader = Consumer(queue, stage, "a")
    reader.land(*range(6))
    now = time.time()
    claimed = now - 1000.0
    _claim(queue, reader.key, phase="phase-0", claimed_unix=claimed,
           reported_unix=claimed + 5.0)
    newcomer = Consumer(queue, stage, "n")
    tiers = _tiers(stage)

    early = tier_loop._commitment_census(
        queue, tiers, consumers=tier_loop._planned_consumers(queue, tiers))
    assert early[TIER]["windows"][reader.key]["footprint_gib"] == 10 * PHASE_GIB

    _report(queue, reader.key, phase="phase-1", reported_unix=now - 10.0)
    events = tier_loop.residency_window(queue, tiers=tiers)

    assert newcomer.lead_published(), _gate(events, newcomer.key)


def _report(queue: pool.PoolQueue, key: str, *, phase: str,
            reported_unix: float) -> None:
    """File a later accepted phase under the same claim."""

    item = json.loads(queue.item_path(pool.CLAIMED, key).read_text())
    # The fixture's claim names another host than the one writing the lease,
    # which a lease refresh refuses; the first lease is simply replaced.
    queue.lease_path(key).unlink()
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


# ---------------------------------------------------------- the live numbers
#
# R12 at 22:30:26Z, as ``test_r12_and_the_capture_replay_under_the_refill_
# horizon`` builds it, on the 530 GiB the stage had for windows.  What R12
# commits, by ``_commitment_census``: the 264 GiB it holds inside its horizon
# and its advance (``chain-043`` to ``chain-032``), and its two queued rows past
# the horizon (``chain-021``, ``chain-018``, 44 GiB) -- 308 GiB.  The twelve
# landed ranges past the horizon (264 GiB) are evictable.  Its footprint at
# its measured 20.7 MB/s is the eleven ranges inside the horizon, 242 GiB, and
# it holds more than that already, so it has no growth.  A newcomer is priced
# at the stage's announced 413 MB/s fill supply, with a 90 s report latency
# (30 s heartbeat and the 60 s default cycle).

#: What R12 commits at 22:30Z, by the arithmetic above.
R12_COMMITTED = 264 + 44
#: An R13 shaped like R12, not yet claimed: 100 GiB of host read-ahead (its
#: GPU budget is unknown before a claim), landing at R12's sealed 144 MB/s.
R13_FOOTPRINT = 242
CAPTURE_FOOTPRINT = 20
#: A Stage-B quantum: a 3 GiB head and three 22 GiB layer ranges, 28 GiB of
#: host read-ahead.  Its whole plan fits inside its horizon.
QUANTUM_FOOTPRINT = 3 + 3 * 22


def _live_tiers(stage: Path) -> dict[str, dict[str, object]]:
    """The stage as announced on 2026-09-22, with its 413 MB/s fill supply."""

    return {TIER: {**replay._tier_record(stage, gib=replay.CAPACITY),
                   "tokens": {storage_tiers.FILL_KIND:
                              replay.DATA["tier"]["fill_supply_mb_s"]}}}


def _live_cycle(queue: pool.PoolQueue, stage: Path) -> None:
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: _live_tiers(stage))


def _live_r12(tmp_path: Path) -> tuple[pool.PoolQueue, Path]:
    queue, stage = _fixture_queue(tmp_path, replay.CAPACITY)
    replay._r12(queue, stage, time.time() - replay.SAMPLE_UNIX)
    return queue, stage


def _lead_published(queue: pool.PoolQueue, label: str, lead: str) -> bool:
    mover = replay._mover(label, lead)
    return (queue.item_path(pool.READY, mover).exists()
            or bool(queue.tier_ledger(TIER).holder_tokens(mover)))


def test_the_capture_beside_r12_passes_the_commitment(tmp_path: Path) -> None:
    """22:30Z, the capture not yet admitted: #907 does not stand in its way.

    The capture's whole plan is 20 GiB and fits its horizon, so that is its
    footprint: 308 + 20 = 328 of 530.  The joint-fit gate still refuses it
    for now -- 528 held and 44 queued leave 2 GiB beside its 3 + 1 GiB
    minimum -- and the commitment says eviction can admit it, so the relief
    is asked as it was before the fix.  The ranges it gives back are past
    R12's horizon; nothing inside it moves, and the capture's lead publishes.
    """

    queue, stage = _live_r12(tmp_path)
    replay._capture(queue)

    events = tier_loop.residency_window(queue, tiers=_live_tiers(stage))

    gate = _gate(events, replay.CAPTURE)
    assert gate is not None and gate["reason"] == window_credit.REASON_STALL, gate
    terms = gate["commitment"]
    assert terms["committed_gib"] == R12_COMMITTED
    assert terms["footprint_gib"] == CAPTURE_FOOTPRINT
    assert terms["consumption_basis"] == "fill-supply"
    assert (tier_loop.window_pressure(queue, tiers=_live_tiers(stage))
            .get(TIER) or 0) > 0

    _live_cycle(queue, stage)
    _live_cycle(queue, stage)

    assert _lead_published(queue, "capture", "head")
    held = replay._r12_held(queue)
    assert all(held[name] for name in replay.INSIDE + [replay.ADVANCE]), held
    assert sum(held.values()) < 528
    assert_ledger_matches_the_stage(queue)


def test_an_r13_shaped_newcomer_waits_for_r12s_footprint(
        tmp_path: Path) -> None:
    """22:30Z with R13 queued: refused on the commitment, and nothing evicted.

    308 + 242 = 550 > 530.  Before the fix the joint-fit gate's shortfall
    (528 + 44 + 22 + 22 - 530 = 86) asked the relief for R12's farthest
    ranges, and R13's lead published into them: the two windows then wanted
    550 GiB of 530 between them, and with every range past R12's horizon
    gone, each range inside one horizon waits on the other consumer's
    reading.  After it, R13 waits, and no pressure is asked for it, so the
    orphan sweep and the horizon eviction take nothing.

    The refusal is priced at the 413 MB/s the stage announced.  A newcomer's
    consumption is the fill supply until it reports, so the footprint moves
    with it: at 144 MB/s R13's is 176 GiB, and 308 + 176 = 484 fits.
    """

    queue, stage = _live_r12(tmp_path)
    r13, manifest = _hexkey("r13consumer"), _hexkey("r13manifest")
    r12 = replay.DATA["r12"]
    plan = replay._plan(queue, r13, label="r13", manifest=manifest,
                        phases=r12["phases"], fill=r12["sealed_fill_mb_s"])
    replay._consumer(queue, r13, plan, manifest=manifest,
                     mem_gb=r12["resources"]["mem_gb"])

    events = tier_loop.residency_window(queue, tiers=_live_tiers(stage))

    assert not _lead_published(queue, "r13", str(plan["phases"][0]["name"]))  # type: ignore[index]
    terms = _refused(events, r13)
    assert terms["capacity_gib"] == replay.CAPACITY
    assert terms["committed_gib"] == R12_COMMITTED
    assert terms["footprint_gib"] == R13_FOOTPRINT
    assert terms["evictable_gib"] == 264
    assert tier_loop.window_pressure(queue, tiers=_live_tiers(stage)).get(TIER) is None
    assert sum(replay._r12_held(queue).values()) == 528


def test_three_stage_b_quanta_fit_beside_r12_and_a_fourth_waits(
        tmp_path: Path, monkeypatch) -> None:
    """R12 after the relief gave back its twelve ranges past the horizon.

    It holds 264 GiB, has 44 queued, and commits 308.  Four Stage-B quanta
    arrive, each with a 69 GiB footprint.  The joint-fit gate admits all
    four on their minimums (264 + 44 + 4 x (3 + 22) = 408 of 530), and
    before the fix all four leads published: 308 + 4 x 69 = 584 GiB of
    promises on 530.  After it, three are admitted (308 + 3 x 69 = 515) and
    the fourth waits for one of them.
    """

    monkeypatch.setitem(replay.DATA["r12"], "landed", [
        entry for entry in replay.DATA["r12"]["landed"]
        if entry["phase"] not in replay.PAST])
    queue, stage = _live_r12(tmp_path)
    assert sum(queue.tier_ledger(TIER).holder_tokens(key).get("stage_gib", 0)
               for key in queue.tier_ledger(TIER).held_keys()) == 264
    quanta = []
    for n in range(4):
        label = f"quantum{n}"
        key, manifest = _hexkey(f"{label}consumer"), _hexkey(f"{label}manifest")
        phases = [{"name": "head", "start_bytes": 0, "end_bytes": 3 * GIB,
                   "stage_gib": 3}] + [
            {"name": f"layer-{i}", "start_bytes": (3 + 22 * i) * GIB,
             "end_bytes": (3 + 22 * (i + 1)) * GIB, "stage_gib": 22}
            for i in range(3)]
        plan = replay._plan(queue, key, label=label, manifest=manifest,
                            phases=phases, fill=144)
        replay._consumer(queue, key, plan, manifest=manifest, mem_gb=28)
        quanta.append((label, key))

    events = tier_loop.residency_window(queue, tiers=_live_tiers(stage))

    published = [key for label, key in quanta if _lead_published(queue, label, "head")]
    assert len(published) == 3, events
    (waiting,) = [key for _label, key in quanta if key not in published]
    terms = _refused(events, waiting)
    assert terms["footprint_gib"] == QUANTUM_FOOTPRINT
    assert terms["committed_gib"] == R12_COMMITTED + 3 * QUANTUM_FOOTPRINT
    assert terms["committed_gib"] + terms["growth_gib"] > replay.CAPACITY

