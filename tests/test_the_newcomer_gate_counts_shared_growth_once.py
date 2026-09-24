"""The newcomer gate counts a shared range's growth once (#1093).

Since #1026, consumers of one manifest share one mover per staged range, and
the joint commitment (#907, ``tier_loop._commitment_census``) counts a shared
range's held tokens once.  Each window's growth, though, was its own read
footprint less its own holding, and ``committed`` summed that over every
admitted window: every sharer counted a shared range's future bytes again.
A shared advance's fence grant is held under the one window that fences it,
so every other sharer forecast that range as growth too.

The numbers every case here uses:

* phases of 2 GiB, each window's plan declaring a read-ahead
  (``reader.prefetch_depth_bytes``) that covers the whole plan unless the
  case says otherwise, so a window's footprint is its remaining plan;
* claimed readers inside ``phase-0``, which is staged once, under the
  range's shared mover.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, residency_plan, window_credit  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _claim, _fixture_queue, _land, _publish_consumer)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    GIB, PHASE_GIB, STAGE_KIND, TIER, _hexkey, _row, _tier_record)

MANIFEST = _hexkey("1093shared")
#: The tier announcement every shared row here is sealed against (#1026).
TOOLS = {"mover_python": "/gen/venv/bin/python",
         "mover_tools_root": "/gen/tools/fleet"}


def _plan(queue: pool.PoolQueue, consumer: str, *, label: str, phases: int,
          shared: frozenset[int] | None = None,
          prefetch_gib: int | None = None) -> dict[str, object]:
    """One consumer's frozen plan over ``MANIFEST``, published ready.

    The phases in ``shared`` (every phase by default) are registered for
    sharing, the way ``pbrun.residency_stage_rows`` seals them, so every
    consumer names one mover for each.  Every other phase keeps a mover of
    this consumer's own, as ``--residency-share off`` seals it.  The plan
    declares ``prefetch_gib`` of read-ahead, by default the whole plan.
    """

    total = phases * PHASE_GIB * GIB
    shared = frozenset(range(phases)) if shared is None else shared
    built = []
    for ordinal in range(phases):
        start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
        row = {
            **_row(queue, _hexkey(f"{label}mover{ordinal}"),
                   {STAGE_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": total,
                "range_start_bytes": start, "range_end_bytes": end},
        }
        if ordinal in shared:
            record, _sealed = residency_plan.register_shared_range(
                queue, manifest_sha256=MANIFEST, tier_id=TIER, start=start,
                end=end, seal=lambda row=row: (row, {}),
                registered_by=consumer, sealed_against=TOOLS)
            row = dict(record["mover_row"])
        built.append({
            "name": f"phase-{ordinal}", "start_bytes": start, "end_bytes": end,
            "stage_gib": PHASE_GIB, "mover_row": row,
            "egress_row": _row(queue, _hexkey(f"{label}egress{ordinal}"),
                               {"mem_gb": 1}),
        })
    depth = total if prefetch_gib is None else prefetch_gib * GIB
    plan = residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=total, phases=built,
        reader={"prefetch_depth_bytes": depth})
    _publish_consumer(queue, consumer, plan, manifest=MANIFEST)
    return plan


def _mover(plan: dict[str, object], ordinal: int) -> str:
    return str(plan["phases"][ordinal]["mover_row"]["action_key"])  # type: ignore[index]


def _land_phase(queue: pool.PoolQueue, stage: Path, plan: dict[str, object],
                ordinal: int) -> None:
    """Stage ``phase-<ordinal>`` once, as its mover files it: under the
    range's share namespace when the mover is shared."""

    phase = plan["phases"][ordinal]                            # type: ignore[index]
    start, end = int(phase["start_bytes"]), int(phase["end_bytes"])
    mover = _mover(plan, ordinal)
    owner = (residency_plan.share_namespace_of(queue, mover)
             or str(plan["consumer_action_key"]))
    _land(queue, stage, consumer=owner, manifest=MANIFEST, mover=mover,
          name=f"phase-{ordinal}", start=start, end=end)


def _claim_at(queue: pool.PoolQueue, key: str, phase: str = "phase-0") -> None:
    now = time.time()
    _claim(queue, key, phase=phase, claimed_unix=now - 1000.0,
           reported_unix=now - 10.0)


def _census(queue: pool.PoolQueue, stage: Path, gib: int) -> dict[str, object]:
    tiers = {TIER: _tier_record(stage, gib=gib)}
    consumers = tier_loop._planned_consumers(queue, tiers)
    census = tier_loop._commitment_census(
        queue, tiers, consumers=consumers, remember=False)[TIER]
    assert "error" not in census, census
    return census


# ------------------------------------------------------ the red, on main


def test_n_sharers_of_one_plan_commit_each_range_once(tmp_path: Path) -> None:
    """Three claimed readers inside ``phase-0`` of one 8 GiB plan, every
    range shared and ``phase-0`` staged once.  Each window's footprint is
    the whole plan, 8 GiB, so the three between them will never want more
    than the plan: 8 GiB committed.  Summing each window's growth commits
    2 + 3 x 6 = 20."""

    queue, stage = _fixture_queue(tmp_path, 40)
    keys = [_hexkey(f"1093reader{n}") for n in range(3)]
    plans = {key: _plan(queue, key, label=f"reader{n}", phases=4)
             for n, key in enumerate(keys)}
    _land_phase(queue, stage, plans[keys[0]], 0)
    for key in keys:
        _claim_at(queue, key)

    census = _census(queue, stage, 40)

    windows = census["windows"]
    for key in keys:
        assert windows[key]["footprint_gib"] == 4 * PHASE_GIB, windows[key]
        assert windows[key]["newcomer"] is False
    assert census["held_gib"] == PHASE_GIB
    assert census["committed_gib"] == 4 * PHASE_GIB, census


def test_a_sharers_fence_grant_is_holding_toward_every_sharer(
        tmp_path: Path) -> None:
    """Two claimed readers inside ``phase-0`` of one 8 GiB plan; the first
    holds the fence grant for the shared ``phase-1`` advance.  The grant is
    room for that range, which both windows read: it is holding toward the
    second window's footprint too, and the tier commits the plan once, not
    the second window's forecast of ``phase-1`` again."""

    queue, stage = _fixture_queue(tmp_path, 40)
    first, second = _hexkey("1093fencer"), _hexkey("1093sharer")
    plans = {first: _plan(queue, first, label="fencer", phases=4),
             second: _plan(queue, second, label="sharer", phases=4)}
    _land_phase(queue, stage, plans[first], 0)
    grant = window_credit.grant_key(first, TIER, "mover_row", "phase-1")
    assert queue.tier_ledger(TIER).acquire(grant, {"stage_gib": PHASE_GIB})
    for key in (first, second):
        _claim_at(queue, key)

    census = _census(queue, stage, 40)

    windows = census["windows"]
    assert windows[first]["holding_gib"] == 2 * PHASE_GIB, windows[first]
    assert windows[second]["holding_gib"] == 2 * PHASE_GIB, windows[second]
    assert census["held_gib"] == 2 * PHASE_GIB
    assert census["committed_gib"] == 4 * PHASE_GIB, census


# ------------------------------------------------------- the gate's guarantee


def _newcomer_beside_a_sharer(tmp_path: Path, capacity: int,
                              ) -> tuple[dict[str, object], str, str]:
    """A claimed reader and a ready newcomer over one 8 GiB plan.

    Each plan's ``phase-0`` is its own range, as ``--residency-share off``
    seals it, so the newcomer's lead is unpublished and it is a newcomer;
    ``phase-1`` to ``phase-3`` are shared.  The reader holds its own
    ``phase-0``.  Returns the census, the reader and the newcomer.
    """

    queue, stage = _fixture_queue(tmp_path, capacity)
    reader, newcomer = _hexkey("1093admitted"), _hexkey("1093newcomer")
    tail = frozenset({1, 2, 3})
    plan = _plan(queue, reader, label="admitted", phases=4, shared=tail)
    _plan(queue, newcomer, label="newcomer", phases=4, shared=tail)
    _land_phase(queue, stage, plan, 0)
    _claim_at(queue, reader)
    return _census(queue, stage, capacity), reader, newcomer


@pytest.mark.parametrize("capacity, admit", [(9, False), (10, True)])
def test_a_newcomer_is_refused_one_gib_short_of_the_unique_growth(
        tmp_path: Path, capacity: int, admit: bool) -> None:
    """The ranges the two windows will want between them are the reader's
    held ``phase-0``, the newcomer's own ``phase-0`` and the three shared
    ranges once: 2 + 2 + 6 = 10 GiB.  The reader alone commits its plan,
    8 GiB, so the newcomer grows the commitment by 2 GiB, not by its whole
    8 GiB footprint.  At 10 GiB of capacity it is admitted, and one GiB
    short of that it still is not: the gate never admits past what the two
    can want at once."""

    census, reader, newcomer = _newcomer_beside_a_sharer(tmp_path, capacity)
    windows = census["windows"]
    assert windows[reader]["newcomer"] is False
    assert windows[newcomer]["newcomer"] is True
    assert windows[reader]["footprint_gib"] == 4 * PHASE_GIB
    assert windows[newcomer]["footprint_gib"] == 4 * PHASE_GIB
    assert census["committed_gib"] == 4 * PHASE_GIB

    decision = tier_loop._commitment_decision(census, newcomer, admitted=set())

    terms = decision["commitment"]
    assert terms["committed_gib"] == 4 * PHASE_GIB
    assert terms["growth_gib"] == PHASE_GIB
    assert decision["admit"] is admit, decision
    if not admit:
        assert decision["reason"] == window_credit.REASON_COMMITMENT
        assert terms["shortfall_gib"] == 5 * PHASE_GIB - capacity


def test_private_ranges_count_per_window_and_shared_ones_once(
        tmp_path: Path) -> None:
    """Two claimed readers of one 8 GiB plan whose ``phase-0`` and
    ``phase-1`` are shared and whose ``phase-2`` and ``phase-3`` are each
    reader's own.  The shared 4 GiB is committed once and each reader's
    private 4 GiB once per reader: 12 GiB, where summing each window's
    growth committed 2 + 6 + 6 = 14."""

    queue, stage = _fixture_queue(tmp_path, 40)
    keys = [_hexkey(f"1093mixed{n}") for n in range(2)]
    head = frozenset({0, 1})
    plans = {key: _plan(queue, key, label=f"mixed{n}", phases=4, shared=head)
             for n, key in enumerate(keys)}
    assert _mover(plans[keys[0]], 0) == _mover(plans[keys[1]], 0)
    assert _mover(plans[keys[0]], 2) != _mover(plans[keys[1]], 2)
    _land_phase(queue, stage, plans[keys[0]], 0)
    for key in keys:
        _claim_at(queue, key)

    census = _census(queue, stage, 40)

    for key in keys:
        window = census["windows"][key]
        assert window["footprint_gib"] == 4 * PHASE_GIB
        assert window["holding_gib"] == PHASE_GIB
        assert window["growth_gib"] == 3 * PHASE_GIB
    assert census["held_gib"] == PHASE_GIB
    assert census["committed_gib"] == 6 * PHASE_GIB, census
    assert census["shared_ranges_gib"] == -PHASE_GIB


def test_sharers_that_can_drift_apart_commit_both_footprints(
        tmp_path: Path) -> None:
    """Two claimed readers inside ``phase-0`` of one 24 GiB plan whose
    windows are far shorter than the plan.  Reading together they want one
    window's ranges; but nothing keeps them together, and once one reads on
    they want two windows' ranges and the ``phase-0`` both hold now is
    holding toward only one.  So the pair commits both footprints: the gate
    never admits a newcomer into room a drifting sharer will take.  This is
    more than the sum of each window's growth, which counts the shared
    ``phase-0`` as holding toward both."""

    queue, stage = _fixture_queue(tmp_path, 40)
    keys = [_hexkey(f"1093drift{n}") for n in range(2)]
    plans = {key: _plan(queue, key, label=f"drift{n}", phases=12,
                        prefetch_gib=2 * PHASE_GIB)
             for n, key in enumerate(keys)}
    _land_phase(queue, stage, plans[keys[0]], 0)
    for key in keys:
        _claim_at(queue, key)

    census = _census(queue, stage, 40)

    footprint = census["windows"][keys[0]]["footprint_gib"]
    assert census["windows"][keys[1]]["footprint_gib"] == footprint
    assert 2 * footprint <= 12 * PHASE_GIB, "fixture: two windows fit the plan"
    assert census["committed_gib"] == 2 * footprint, census
    assert census["shared_ranges_gib"] == PHASE_GIB


# ------------------------------------------------------------ the bound alone


def test_the_joint_need_counts_each_range_once() -> None:
    """``window_credit.joint_need_gib`` on its own: disjoint windows sum,
    windows over the same ranges share them, and a window at a later phase
    reaches only the ranges it has still to read."""

    caps = {f"r{n}": 2 for n in range(6)}
    private = {"a": ["r0", "r1"], "b": ["r2", "r3"]}
    assert window_credit.joint_need_gib({"a": 4, "b": 4}, private, caps) == 8
    assert window_credit.joint_need_gib({"a": 3, "b": 4}, private, caps) == 7
    shared = {"a": ["r0", "r1", "r2"], "b": ["r0", "r1", "r2"]}
    assert window_credit.joint_need_gib({"a": 6, "b": 6}, shared, caps) == 6
    # b is two phases on: a's first two ranges are behind it.
    nested = {"a": ["r0", "r1", "r2", "r3"], "b": ["r2", "r3", "r4", "r5"]}
    assert window_credit.joint_need_gib({"a": 4, "b": 4}, nested, caps) == 8
    assert window_credit.joint_need_gib({"a": 8, "b": 8}, nested, caps) == 12
    assert window_credit.joint_need_gib({"a": 0, "b": 4}, nested, caps) == 4
    assert window_credit.joint_need_gib({}, nested, caps) == 0
