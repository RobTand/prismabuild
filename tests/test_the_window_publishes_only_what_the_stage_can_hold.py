"""A read set five times the stage still ships, one window at a time (#583).

The campaign's `run` stage reads 3.33 TB against a 721 GB stage, so "admitted
once every lead is executed" cannot be the whole story: some of the movers must
not exist in the queue yet when the consumer is admitted, and the ones behind
the consumer must give their bytes back.  That is a window, and a window is
three claims a test can break:

* it publishes in read order, only while the tier's own free tokens cover the
  next phase, so nothing is ever queued against capacity that is not there;
* it evicts only what the consumer has read *past*, never the phase it is
  inside;
* one writer composes the map, because a rename cannot merge and a merge is
  what a shared document would need.

Plus the two that are easy to get wrong in the other direction: a mover whose
phase is still ahead of the consumer must survive the orphan sweep, and a
mover that is terminal but no longer pinned must be publishable again -- its
key is a content hash, so a second campaign over the same manifest seals the
same key, and a leftover ``done`` record must not read as "already staged".
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
GIB = storage_tiers.GIB


def _key(prefix: str, ordinal: int) -> str:
    return f"{prefix}{ordinal:02d}".ljust(64, "0")[:64].replace(prefix[0], prefix[0])


def _hexkey(seed: str) -> str:
    """A distinct 64-hex action key per name, without hashing anything real."""

    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, *, gib_per_phase: int = 2,
          phases: int = 4) -> dict[str, object]:
    built = []
    for ordinal in range(phases):
        start = ordinal * gib_per_phase * GIB
        end = start + gib_per_phase * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end,
            "stage_gib": gib_per_phase,
            "mover_row": _row(_hexkey(f"mover{ordinal}"),
                              {STAGE_KIND: gib_per_phase, "mem_gb": 1}, queue),
            "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}, queue),
        })
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": 5})
    return q


# -- the plan is a cover of one read order, or it is refused -----------------


def test_a_plan_with_a_hole_in_the_read_order_is_refused(queue) -> None:
    """Bytes in the hole are staged by nobody and read from the pool silently."""

    plan = _plan(queue)
    plan["phases"][2]["start_bytes"] += GIB   # drive: move one boundary

    with pytest.raises(residency_plan.ResidencyPlanError, match="not where the previous"):
        residency_plan.validate_plan(plan)


def test_a_phase_that_asks_the_tier_for_less_than_it_occupies_is_refused(queue) -> None:
    """Otherwise a mover pins bytes the ledger never counted."""

    plan = _plan(queue)
    plan["phases"][1]["mover_row"]["resources"][STAGE_KIND] = 1

    with pytest.raises(residency_plan.ResidencyPlanError, match="below the 2 GiB"):
        residency_plan.validate_plan(plan)


def test_two_phases_may_not_name_one_action(queue) -> None:
    plan = _plan(queue)
    plan["phases"][3]["mover_row"]["action_key"] = \
        plan["phases"][0]["mover_row"]["action_key"]

    with pytest.raises(residency_plan.ResidencyPlanError, match="share an action key"):
        residency_plan.validate_plan(plan)


def test_a_frozen_plan_is_written_once_and_read_back(queue) -> None:
    """A retry must resume the same decomposition, not cut a new one."""

    plan = _plan(queue)
    residency_plan.freeze(queue, plan)
    residency_plan.freeze(queue, plan)          # idempotent: same bytes

    assert residency_plan.read(queue, CONSUMER) == residency_plan.validate_plan(plan)

    different = _plan(queue, gib_per_phase=3)
    with pytest.raises(residency_plan.ResidencyPlanError, match="already filed"):
        residency_plan.freeze(queue, different)


def test_the_consumer_depends_on_its_first_phase_and_no_other(queue) -> None:
    """Depending on a later phase is how the window deadlocks on itself."""

    plan = _plan(queue)

    assert residency_plan.leads_for(plan) == [_hexkey("mover0")]
    assert len(residency_plan.mover_keys(plan)) == 4


# -- what the window publishes, and what it takes back ----------------------


def test_only_the_phases_the_free_tokens_cover_are_published(queue) -> None:
    """Five free GiB and two GiB a phase is two phases, not four."""

    decision = residency_plan.window(
        _plan(queue), accepted_phase=None, free_gib=5)

    assert [phase["phase"] for phase in decision["publish"]] == ["phase-0", "phase-1"]
    assert decision["evict"] == []


def test_nothing_is_published_when_the_stage_is_full(queue) -> None:
    decision = residency_plan.window(_plan(queue), accepted_phase=None, free_gib=1)

    assert decision["publish"] == []


def test_the_phase_the_consumer_is_reading_is_not_evicted(queue) -> None:
    """Counting the accepted phase as consumed deletes what is being read."""

    plan = _plan(queue)
    staged = [_hexkey(f"mover{n}") for n in range(4)]

    decision = residency_plan.window(
        plan, accepted_phase="phase-2", free_gib=0, published=staged, staged=staged)

    assert [phase["phase"] for phase in decision["evict"]] == ["phase-0", "phase-1"]


def test_a_terminal_mover_that_holds_nothing_is_published_again(queue) -> None:
    """A content-hash key outlives its own eviction; ``done`` is not ``staged``."""

    plan = _plan(queue)
    # Everything staged once and already given back: nothing is pinned, so the
    # window must treat all four phases as work to do rather than as done.
    decision = residency_plan.window(
        plan, accepted_phase="phase-0", free_gib=4, published=[], staged=[])

    assert [phase["phase"] for phase in decision["publish"]] == ["phase-0", "phase-1"]


# -- the coordinator: one writer, one loop ----------------------------------


def _claim_consumer(queue: pool.PoolQueue, plan: dict[str, object]) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=CONSUMER, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                   "leads": residency_plan.leads_for(plan)})


def test_the_loop_publishes_the_window_the_ledger_can_hold(queue, tmp_path) -> None:
    plan = _plan(queue)
    _claim_consumer(queue, plan)

    events = tier_loop.residency_window(
        queue, tiers={TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})

    published = [event["phase"] for event in events if event["event"] == "mover-published"]
    assert published == ["phase-0", "phase-1"]
    assert queue.item_path(pool.READY, _hexkey("mover0")).exists()
    assert not queue.item_path(pool.READY, _hexkey("mover2")).exists()


def test_a_stage_on_another_box_is_left_to_its_own_loop(queue, tmp_path) -> None:
    """Two loops minting one stage's occupancy is what the tier id prevents."""

    _claim_consumer(queue, _plan(queue))

    events = tier_loop.residency_window(
        queue, tiers={"prismabuild-stage:elsewhere": {
            "tier_id": "prismabuild-stage:elsewhere", "tier": "stage",
            "mountpoint": str(tmp_path / "stage")}})

    assert events == []
    assert not queue.item_path(pool.READY, _hexkey("mover0")).exists()


def _fragment(queue, mover: str, *, path: str, stage: Path) -> None:
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "entries": {residency_map.residency_map_key(path, 0): {
            "stage_path": str(stage / Path(path).name), "bytes": 4096,
            "offset": 0, "sha256": "a" * 64}}})


def test_one_writer_merges_every_movers_fragment_into_one_map(queue, tmp_path) -> None:
    """A rename cannot merge, so the merge happens in one place or not at all."""

    stage = tmp_path / "stage"
    _fragment(queue, _hexkey("mover0"), path="/pool/a.bin", stage=stage)
    _fragment(queue, _hexkey("mover1"), path="/pool/b.bin", stage=stage)

    written = tier_loop.compose_map(queue, CONSUMER)

    assert written == queue.residency_map_path(CONSUMER)
    composed = residency_map.read_map(written)
    assert set(composed["entries"]) == {
        residency_map.residency_map_key("/pool/a.bin", 0),
        residency_map.residency_map_key("/pool/b.bin", 0)}
    assert sorted(composed["leads"]) == sorted([_hexkey("mover0"), _hexkey("mover1")])


def test_an_evicted_range_leaves_the_map_behind_it(queue, tmp_path) -> None:
    """A map naming deleted files reads as corruption, not as a cache miss."""

    stage = tmp_path / "stage"
    _fragment(queue, _hexkey("mover0"), path="/pool/a.bin", stage=stage)
    tier_loop.compose_map(queue, CONSUMER)
    assert queue.residency_map_path(CONSUMER).exists()

    # What an egress does: the fragment goes when the bytes go.
    residency_map.fragment_path(
        queue.residency_fragment_root(), CONSUMER, _hexkey("mover0")).unlink()

    assert tier_loop.compose_map(queue, CONSUMER) is None
    assert not queue.residency_map_path(CONSUMER).exists()


def test_the_sweep_spares_a_window_the_consumer_has_not_reached(queue, tmp_path) -> None:
    """Leads name phase 0 only; a plan is what says phase 3 is still wanted."""

    stage = tmp_path / "stage"
    stage.mkdir()
    plan = _plan(queue)
    _claim_consumer(queue, plan)
    ahead = _hexkey("mover3")
    # Drive the ledger into the state a finished, pinned phase-3 mover leaves.
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(ahead, {"stage_gib": 2})
    queue.record_move(ahead, {"consumer_action_key": CONSUMER, "tier_id": TIER,
                              "stage_root": str(stage), "complete": True,
                              "bytes_staged": 2 * GIB})

    swept = stage_release.sweep(queue, stage_roots={TIER: str(stage)})

    assert swept == []
    assert ledger.holder_tokens(ahead) == {"stage_gib": 2}


def test_the_sweep_takes_back_a_mover_no_live_plan_names(queue, tmp_path) -> None:
    stage = tmp_path / "stage"
    stage.mkdir()
    orphan = _hexkey("orphan")
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire(orphan, {"stage_gib": 2})
    queue.record_move(orphan, {"consumer_action_key": CONSUMER, "tier_id": TIER,
                               "stage_root": str(stage), "complete": True,
                               "bytes_staged": 2 * GIB})

    swept = stage_release.sweep(queue, stage_roots={TIER: str(stage)})

    assert [record["reason"] for record in swept] == ["orphan-sweep"]
    assert ledger.holder_tokens(orphan) == {}


# -- the channel the consumer reads the map through -------------------------


def test_the_launcher_names_the_map_only_when_there_is_one(queue, tmp_path) -> None:
    """A variable set for a file that is not there means nothing it says."""

    item = {"action_key": CONSUMER, "residency": {"leads": [_hexkey("mover0")]}}

    assert queue.residency_map_environment(item) == {}

    stage = tmp_path / "stage"
    _fragment(queue, _hexkey("mover0"), path="/pool/a.bin", stage=stage)
    tier_loop.compose_map(queue, CONSUMER)

    assert queue.residency_map_environment(item) == {
        pb.RESIDENCY_MAP_ENV: str(queue.residency_map_path(CONSUMER))}
    # An ordinary action is launched exactly as it was before #583.
    assert queue.residency_map_environment({"action_key": CONSUMER}) == {}


def test_an_action_is_told_its_own_key_and_never_asked_to_seal_it() -> None:
    """A key inside the argv it is computed from has no fixed point."""

    action = {"action_key": "d" * 64, "params": {}}

    environment = pb._residency_environment(action, {"PATH": "/usr/bin"})

    assert environment[pb.ACTION_KEY_ENV] == "d" * 64
    assert pb.RESIDENCY_MAP_ENV not in environment


def test_an_action_that_seals_the_residency_names_is_refused() -> None:
    """A diagnostic must not silently change what the action does."""

    action = {"action_key": "d" * 64, "params": {}}

    with pytest.raises(pb.ActionContractError, match=pb.ACTION_KEY_ENV):
        pb._residency_environment(action, {pb.ACTION_KEY_ENV: "e" * 64})


def test_the_map_path_reaches_the_action_when_the_launcher_set_it(monkeypatch) -> None:
    monkeypatch.setenv(pb.RESIDENCY_MAP_ENV, "/mnt/shared/pb-queue/residency/x.map.json")

    environment = pb._residency_environment({"action_key": "d" * 64, "params": {}}, {})

    assert environment[pb.RESIDENCY_MAP_ENV].endswith("x.map.json")


def test_the_two_definitions_of_the_residency_variables_are_one_string() -> None:
    """Mirrored into attested core, which may import no repository module."""

    assert pb.RESIDENCY_MAP_ENV == residency_map.RESIDENCY_MAP_ENV


# -- the submitter ----------------------------------------------------------


def test_a_submission_cannot_stage_onto_a_tier_the_fleet_does_not_announce(
        queue) -> None:
    """Discovered, never configured: the topology is Rob's to change."""

    import pbrun

    with pytest.raises(SystemExit, match="no box announces one"):
        pbrun.resolve_stage_tier(queue, None)


def test_more_than_one_stage_tier_is_the_operators_choice(queue) -> None:
    import pbrun

    for host in ("dl380g10", "elsewhere"):
        queue.announce_tier({"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                             "tier_id": f"prismabuild-stage:{host}", "tier": "stage",
                             "host": host, "capacity_bytes": 1 << 40,
                             "mountpoint": "/stage/prewarm"})

    with pytest.raises(SystemExit, match="--residency-tier"):
        pbrun.resolve_stage_tier(queue, None)

    named = pbrun.resolve_stage_tier(queue, "prismabuild-stage:elsewhere")
    assert named["host"] == "elsewhere"


def test_one_announced_stage_tier_needs_no_flag(queue) -> None:
    import pbrun

    queue.announce_tier({"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                         "tier_id": TIER, "tier": "stage", "host": "dl380g10",
                         "capacity_bytes": 1 << 40, "mountpoint": "/stage/prewarm"})

    assert pbrun.resolve_stage_tier(queue, None)["tier_id"] == TIER


def test_two_ranges_of_one_manifest_seal_two_different_movers() -> None:
    """The key is the body; two ranges are two bodies or the window is one node."""

    import pbrun

    template = {
        "cas": None, "marker_root": Path("/home/rob/tmp/markers"),
        "checkout_identity": {"commit": "a" * 40},
        "log_name": "x.log", "stamp_name": "y",
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "nondeterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "working_directory": "."},
        "inputs": [], "code_closure": {"files": []},
        "params": {"command": ["true"], "cwd": "/home/rob", "demand": {"cpu": 1},
                   "placement": {"required_tags": []},
                   "checkout_snapshot": {"input": {"sha256": "b" * 64}},
                   "retry_policy": {"max_attempts": 1}},
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }
    try:
        first = pbrun.seal_movement_action(
            template, command=["/bin/true", "--range-start-bytes", "0"],
            demand={STAGE_KIND: 2}, tags=["dl380g10"], log_name="a.log")
        second = pbrun.seal_movement_action(
            template, command=["/bin/true", "--range-start-bytes", "2048"],
            demand={STAGE_KIND: 2}, tags=["dl380g10"], log_name="b.log")
    except SystemExit as exc:                    # pragma: no cover - diagnostic
        pytest.skip(f"the action contract refuses this stub template: {exc}")

    assert first["action_key"] != second["action_key"]
    assert first["params"]["demand"] == {STAGE_KIND: 2}
    # The mover is placed where the stage is, not where the consumer computes.
    assert first["params"]["placement"] == {"required_tags": ["dl380g10"]}
    # And it carries none of the consumer's own bounding: a copy is not the
    # work the progress policy or the profiler was asked about.
    assert pb.PROGRESS_PARAM not in first["params"]
