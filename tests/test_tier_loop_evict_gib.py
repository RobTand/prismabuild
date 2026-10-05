"""``--evict-gib``: the operator's one-shot orphan reclaim (#1535).

The stage tier can only evict under pressure a live window creates, and once
632 GiB of orphaned ranges sat on dl380g10 with nothing asking for room, there
was no way to give the bytes back through PrismaBuild's own eviction -- the
ledger stays consistent, but the tokens stay held.  ``--evict-gib N`` merges N
GiB of operator pressure into the per-tier pressure the cycle already
computes, so the ordinary orphan sweep (``stage_release.sweep``) takes the
oldest orphans first until the tier has N GiB free.  The rules the tests pin:

* with no flag, a cycle is byte-for-byte what it was -- the same phases, the
  same effects, no summary line, orphans kept while nothing needs the room;
* with the flag, the oldest orphans go first, just enough of them, and the
  tokens released equal the bytes removed;
* nothing a live or claimed consumer's frozen plan names is a candidate, and
  the beyond-horizon pass -- whose candidates belong to live consumers -- does
  not run at all in this mode;
* the argument forms that make no sense are refused at the parser;
* ``--evict-dry-run`` reads only: the stage tree, the queue and the announced
  tiers hash identically either side of it, and it says plainly when the
  candidates cannot reach the request.

Everything runs on a ``tmp_path`` queue and stage root; nothing touches a live
queue or a real stage mountpoint (#628).
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, storage_tiers  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    MANIFEST, PHASE_GIB, STAGE_KIND, TIER, _claim_with_progress, _hexkey,
    _plan, _publish_consumer, _row, _stage_range, _tier_record,
    assert_ledger_matches_the_stage)

GIB = storage_tiers.GIB

#: Twelve GiB of stage: three orphans (6) and a live consumer's two landed
#: phases (4) leave 2 free, which is room enough for a window of its own.
CAPACITY_GIB = 12

WITHDRAWN = "5" * 64    # staged its ranges, then went away: the orphans' owner
LIVE = "6" * 64         # a consumer with a frozen plan the sweep must protect

#: The phase laps one full cycle takes, pinned so a silent reorder of the
#: stage graph cannot hide behind this file (test (f): without the flag the
#: cycle is what it was).
EXPECTED_PHASES = frozenset({
    "reclaim_idle_rates", "receipts", "sync_ram_host_mirror", "discover",
    "mint_announce",
    "manifest_promotion", "drop_prior_ram_epochs",
    "release_incomplete_ram_promotions", "planned_consumers", "withdrawn_keys",
    "withdraw_dead_consumer_movers", "adopt_resident_ranges",
    "window_pressure", "reclaim_failed_mover_partials", "sweep_orphans",
    "evict_beyond_horizon", "fan_out_shared_ranges", "ram_residency_window",
    "residency_window", "retire_terminal_output_funding",
    "origin_retirement_tick", "deferred_release",
    "census_cost_and_retired_tiers"})


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(TIER, {"stage_gib": CAPACITY_GIB})
    return q


@pytest.fixture()
def stage(tmp_path: Path, queue: pool.PoolQueue) -> Path:
    path = tmp_path / "stage"
    path.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=path)
    return path


def _orphan_mover(ordinal: int) -> str:
    return _hexkey(f"orphanmover{ordinal}")


def _live_mover(ordinal: int) -> str:
    return _hexkey(f"livemover{ordinal}")


def _orphans(queue: pool.PoolQueue, stage: Path, count: int) -> dict[int, list[Path]]:
    """``count`` landed ranges of a withdrawn consumer: held, and nobody's."""

    queue.publish(**_row(queue, WITHDRAWN, {"cpu": 1, "mem_gb": 1}))
    queue.withdraw(WITHDRAWN, reason="#1535 fixture", by="test")
    return {ordinal: _stage_range(
        queue, mover=_orphan_mover(ordinal), consumer=WITHDRAWN, stage=stage,
        ordinal=ordinal, manifest="e" * 64)
        for ordinal in range(count)}


def _live_consumer(queue: pool.PoolQueue, stage: Path, *, claimed: bool
                   ) -> dict[str, object]:
    """A live consumer whose two landed phases its frozen plan still names.

    ``claimed`` moves it into ``claimed`` with both phases accepted, so every
    landed leg is past its readers' refill horizon -- exactly the population
    the beyond-horizon pass exists to take, and exactly what ``--evict-gib``
    must leave alone.
    """

    plan = _plan(queue, LIVE, phases=2, label="live")
    for ordinal in range(2):
        _stage_range(queue, mover=_live_mover(ordinal), consumer=LIVE,
                     stage=stage, ordinal=ordinal)
    _publish_consumer(queue, LIVE, plan)
    if claimed:
        _claim_with_progress(queue, LIVE, phase="phase-1")
    return plan


def _tiers(stage: Path) -> dict[str, dict[str, object]]:
    return {TIER: _tier_record(stage, gib=CAPACITY_GIB)}


def _cycle(queue: pool.PoolQueue, stage: Path, *, gib: int | None = None):
    """One whole tier cycle, with or without the operator's evict pressure."""

    kwargs = {} if gib is None else {"evict_gib": gib}
    return tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                           receipts=tier_loop.ReceiptCache(),
                           discover=lambda **_kwargs: _tiers(stage), **kwargs)


def _events(captured: str) -> list[dict]:
    return [json.loads(line) for line in captured.splitlines()
            if line.startswith("{")]


def _tree_state(root: Path) -> list[tuple]:
    """Everything a read-only run must not disturb: names, sizes, mtimes."""

    state: list[tuple] = []
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        if path.is_symlink() or not path.is_dir():
            stat = path.lstat()
            state.append((relative, stat.st_size, stat.st_mtime_ns))
        else:
            state.append((relative + "/", None, None))
    return state


def _held(queue: pool.PoolQueue, mover: str) -> bool:
    return bool(queue.tier_ledger(TIER).holder_tokens(mover))


def _tokens_released(run: dict[str, object] | None) -> int:
    return sum(int(one.get("tokens_released") or 0)
               for one in (run or {}).get("sweep_receipts", [])  # type: ignore[union-attr]
               if isinstance(one, dict)
               and one.get("reason") == "orphan-sweep" and one.get("complete"))


def _bytes_deleted(run: dict[str, object] | None) -> int:
    return sum(int(one.get("bytes_deleted") or 0)
               for one in (run or {}).get("sweep_receipts", [])  # type: ignore[union-attr]
               if isinstance(one, dict)
               and one.get("reason") == "orphan-sweep" and one.get("complete"))


# ------------------------------------------------------------------ (a) base


def test_a_cycle_without_pressure_keeps_its_orphans(queue, stage) -> None:
    """The unchanged base: no window asks, so the orphan sweep takes nothing."""

    orphans = _orphans(queue, stage, 3)
    _cycle(queue, stage)
    for ordinal, paths in orphans.items():
        assert _held(queue, _orphan_mover(ordinal))
        assert all(path.exists() for path in paths)
    assert_ledger_matches_the_stage(queue)


def test_a_cycle_without_the_flag_is_what_it_was(queue, stage, capsys) -> None:
    """No flag, no trace of the operator mode: no summary, no skip, no capture.

    The phase laps are pinned too: a pass inserted into the stage graph by
    this feature, or one silently dropped, is a change to every cycle, not
    just the flagged ones.
    """

    _orphans(queue, stage, 2)
    _cycle(queue, stage)
    assert tier_loop.EVICT_RUN is None
    assert tier_loop.LAST_CYCLE["completed"] is True
    assert set(tier_loop.LAST_CYCLE["phases"]) == EXPECTED_PHASES
    events = _events(capsys.readouterr().out)
    assert not [one for one in events
                if one.get("event") in ("evict-gib", "stage-orphan-evicted",
                                        "beyond-horizon-evicted")]
    assert all(_held(queue, _orphan_mover(ordinal)) for ordinal in range(2))
    assert_ledger_matches_the_stage(queue)


# ------------------------------------------------------- (b) the flag evicts


def test_evict_gib_takes_the_oldest_orphans_just_enough(queue, stage) -> None:
    """Free 6, ask 8: the oldest orphan goes, the other two stay.

    The tokens the sweep releases are the bytes it deleted -- the ledger
    invariant the whole feature exists to keep.
    """

    orphans = _orphans(queue, stage, 3)          # 6 held, 6 free
    _cycle(queue, stage, gib=8)
    receipts = tier_loop.EVICT_RUN["sweep_receipts"]
    evicted = [one["action_key"] for one in receipts
               if one.get("reason") == "orphan-sweep" and one.get("complete")]
    assert evicted == [_orphan_mover(0)], receipts
    assert not _held(queue, _orphan_mover(0))
    assert not any(path.exists() for path in orphans[0])
    for ordinal in (1, 2):
        assert _held(queue, _orphan_mover(ordinal))
        assert all(path.exists() for path in orphans[ordinal])
    assert _bytes_deleted(tier_loop.EVICT_RUN) == PHASE_GIB * GIB
    assert _tokens_released(tier_loop.EVICT_RUN) == PHASE_GIB
    assert tier_loop.EVICT_RUN["tier_id"] == TIER
    assert_ledger_matches_the_stage(queue)


def test_evict_gib_larger_than_the_candidates_takes_every_orphan(queue, stage
                                                                 ) -> None:
    """Free 6, ask 30: all three go, oldest first, and nothing is invented."""

    _orphans(queue, stage, 3)
    _cycle(queue, stage, gib=30)
    evicted = [one["action_key"] for one
               in tier_loop.EVICT_RUN["sweep_receipts"]
               if one.get("reason") == "orphan-sweep" and one.get("complete")]
    assert evicted == [_orphan_mover(ordinal) for ordinal in range(3)]
    assert queue.tier_ledger(TIER).held_keys() == []
    assert _tokens_released(tier_loop.EVICT_RUN) == PHASE_GIB * 3
    assert_ledger_matches_the_stage(queue)


# --------------------------------------------- (c) live consumers are immune


@pytest.mark.parametrize("claimed", [False, True])
def test_evict_gib_never_takes_what_a_live_plan_names(queue, stage,
                                                      claimed: bool) -> None:
    """A request nothing but live bytes can satisfy evicts none of them.

    Both phases are landed and -- when claimed -- long past their readers'
    horizons, which is the beyond-horizon pass's population; an unclaimed
    ready consumer's plan protects its whole window the same way.  The
    orphans go; the reader's bytes do not.
    """

    _orphans(queue, stage, 2)
    _live_consumer(queue, stage, claimed=claimed)
    _cycle(queue, stage, gib=CAPACITY_GIB)       # only live bytes could reach it
    for ordinal in range(2):
        assert _held(queue, _live_mover(ordinal))
    assert tier_loop.EVICT_RUN["beyond_horizon"].startswith("skipped")
    receipts = tier_loop.EVICT_RUN["sweep_receipts"]
    assert not [one for one in receipts
                if one.get("action_key") in (_live_mover(0), _live_mover(1))]
    assert_ledger_matches_the_stage(queue)


def test_evict_gib_on_a_full_tier_of_live_bytes_takes_nothing(queue, stage
                                                              ) -> None:
    """The two live phases alone, ask everything: a refusal-shaped no-op.

    Without the skip, the merged pressure would be the beyond-horizon pass's
    invitation to take a live reader's range; with it, the run returns the
    tier exactly as it found it.
    """

    _live_consumer(queue, stage, claimed=True)
    _cycle(queue, stage, gib=CAPACITY_GIB)
    assert [one for one in tier_loop.EVICT_RUN["sweep_receipts"]
            if one.get("complete")] == []
    assert all(_held(queue, _live_mover(ordinal)) for ordinal in range(2))
    assert tier_loop.EVICT_RUN["beyond_horizon"].startswith("skipped")
    assert_ledger_matches_the_stage(queue)


# ------------------------------------------------------------ (d) refusals


def test_the_flag_refuses_its_nonsense_forms(tmp_path, capsys) -> None:
    """Every malformed combination dies at the parser, before any lock."""

    root = str(tmp_path / "pb-queue")
    for argv, fragment in [
        (["--pool-root", root, "--evict-gib", "4"], "--once"),
        (["--pool-root", root, "--once", "--evict-gib", "0"], "positive"),
        (["--pool-root", root, "--once", "--evict-gib", "-3"], "positive"),
        (["--pool-root", root, "--once", "--evict-dry-run"], "--evict-gib"),
        (["--pool-root", root, "--evict-gib", "4", "--evict-dry-run"],
         "--once"),
    ]:
        with pytest.raises(SystemExit) as exit_code:
            tier_loop.main(argv)
        assert exit_code.value.code == 2, argv
        assert fragment in capsys.readouterr().err, argv


def test_evict_gib_without_a_stage_tier_is_refused_by_name(queue, stage,
                                                           capsys,
                                                           monkeypatch
                                                           ) -> None:
    """No announced stage tier, or several with no --evict-tier: no guess."""

    real_cycle = tier_loop.cycle

    def no_stage(queue_, **kwargs):
        kwargs["discover"] = lambda **_kwargs: {}
        return real_cycle(queue_, **kwargs)

    monkeypatch.setattr(tier_loop, "cycle", no_stage)
    assert tier_loop.main(["--pool-root", str(queue.root), "--once",
                           "--evict-gib", "2"]) == 2
    assert "--evict-gib found no stage tier" in capsys.readouterr().err

    def two_stages(queue_, **kwargs):
        records = _tiers(stage)
        second = dict(records[TIER])
        second["tier_id"] = "prismabuild-stage2:dl380g10"
        records["prismabuild-stage2:dl380g10"] = second
        kwargs["discover"] = lambda **_kwargs: records
        return real_cycle(queue_, **kwargs)

    monkeypatch.setattr(tier_loop, "cycle", two_stages)
    assert tier_loop.main(["--pool-root", str(queue.root), "--once",
                           "--evict-gib", "2"]) == 2
    assert "--evict-tier" in capsys.readouterr().err


# ------------------------------------------------------------- (e) dry run


def _announce(queue: pool.PoolQueue, stage: Path) -> None:
    """The announced record the live role leaves behind, as the dry run reads it."""

    queue.announce_tier(_tier_record(stage, gib=CAPACITY_GIB))


def test_the_dry_run_lists_oldest_first_and_skips_the_live(queue, stage,
                                                           capsys) -> None:
    """The order the real sweep would evict, the live range named as skipped."""

    _orphans(queue, stage, 2)
    _live_consumer(queue, stage, claimed=False)
    _announce(queue, stage)
    code = tier_loop.main(["--pool-root", str(queue.root), "--once",
                           "--evict-gib", "10", "--evict-dry-run"])
    assert code == 0
    events = _events(capsys.readouterr().out)
    candidates = [one for one in events
                  if one["event"] == "evict-dry-run-candidate"]
    skipped = [one for one in events if one["event"] == "evict-dry-run-skipped"]
    summary = [one for one in events
               if one["event"] == "evict-dry-run-summary"][-1]
    # Oldest receipt first, with the running total the operator reads down
    # the list.
    assert [one["mover"] for one in candidates] == [
        _orphan_mover(0), _orphan_mover(1)]
    assert [one["running_total_gib"] for one in candidates] == [
        float(PHASE_GIB), float(PHASE_GIB * 2)]
    assert all("no live item" in one["why"] for one in candidates)
    # The protection is shown, not silent.
    assert [one["mover"] for one in skipped] == [_live_mover(0), _live_mover(1)]
    assert all("SKIPPED, owned by a live consumer" in one["why"]
               for one in skipped)
    assert summary["requested_gib"] == 10
    assert summary["would_free_gib"] == float(PHASE_GIB * 2)
    assert summary["reaches_requested"] is False
    assert "cannot reach" in summary["note"]


def test_the_dry_run_changes_nothing(queue, stage, capsys) -> None:
    """Stage tree, queue directory and announced tiers hash identically."""

    _orphans(queue, stage, 2)
    _live_consumer(queue, stage, claimed=True)
    _announce(queue, stage)
    before = (_tree_state(stage), _tree_state(queue.root),
              sorted(json.dumps(one, sort_keys=True) for one in queue.tiers()),
              queue.tier_ledger(TIER).capacity(),
              queue.tier_ledger(TIER).available())
    started = time.monotonic()
    code = tier_loop.main(["--pool-root", str(queue.root), "--once",
                           "--evict-gib", "1", "--evict-dry-run"])
    assert code == 0
    after = (_tree_state(stage), _tree_state(queue.root),
             sorted(json.dumps(one, sort_keys=True) for one in queue.tiers()),
             queue.tier_ledger(TIER).capacity(),
             queue.tier_ledger(TIER).available())
    assert before == after
    # And it was quick because it never ran a cycle.
    assert time.monotonic() - started < 60
    events = _events(capsys.readouterr().out)
    assert [one["event"] for one in events if one["event"].startswith(
        "evict-dry-run")] != []


def test_the_dry_run_says_so_when_it_reaches_the_request(queue, stage,
                                                         capsys) -> None:
    """A request the orphans can cover is answered without a shortfall."""

    _orphans(queue, stage, 3)
    _announce(queue, stage)
    code = tier_loop.main(["--pool-root", str(queue.root), "--once",
                           "--evict-gib", "4", "--evict-dry-run"])
    assert code == 0
    events = _events(capsys.readouterr().out)
    summary = [one for one in events
               if one["event"] == "evict-dry-run-summary"][-1]
    assert summary["reaches_requested"] is True
    assert summary["would_free_gib"] == float(PHASE_GIB * 3)
    assert "would free 6.0 GiB of the 4 requested" in summary["note"]


# ------------------------------------------- the one-shot's own summary line


def test_the_one_shot_prints_one_summary_line(queue, stage, capsys,
                                              monkeypatch) -> None:
    """The real `--once --evict-gib` surface: request, bytes, tokens, receipts.

    The dataset's ``available`` is faked at the same seam discovery reads it,
    so the before/after pair is exercised without a ZFS pool.
    """

    _orphans(queue, stage, 3)
    real_cycle = tier_loop.cycle

    def cycle_here(queue_, **kwargs):
        kwargs.setdefault("discover", lambda **_kwargs: _tiers(stage))
        return real_cycle(queue_, **kwargs)

    monkeypatch.setattr(tier_loop, "cycle", cycle_here)

    def fake_dataset(pool_name, *, runner=None):
        return {"dataset": f"{pool_name}/prewarm", "available_bytes": 5 * GIB,
                "mountpoint": str(stage), "primarycache": "all"}

    monkeypatch.setattr(storage_tiers, "stage_dataset", fake_dataset)
    code = tier_loop.main(["--pool-root", str(queue.root), "--once",
                           "--evict-gib", "8"])
    assert code == 0
    events = _events(capsys.readouterr().out)
    summaries = [one for one in events if one.get("event") == "evict-gib"]
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary["requested_gib"] == 8
    assert summary["tier_id"] == TIER
    assert summary["stage_root"] == str(stage)
    assert summary["bytes_evicted"] == PHASE_GIB * GIB
    assert summary["tokens_released"] == PHASE_GIB
    assert summary["available_before_bytes"] == 5 * GIB
    assert summary["available_after_bytes"] == 5 * GIB
    assert summary["available_dataset"] == "storage_pool/prewarm"
    assert summary["beyond_horizon"].startswith("skipped")
    assert summary["runtime_gate"]["skipped"] is True
    assert "pbstatus.py --starvation" in summary["ledger_report"]
    assert summary["ledger_gib"]["held_gib"] == PHASE_GIB * 2
    assert len([one for one in summary["sweep_receipts"]
                if one.get("complete")]) == 1
    # The held-versus-staged cross-check reads the ledger it just settled.
    assert summary["held_vs_staged"]["held_tokens_gib"] == PHASE_GIB * 2
    assert summary["held_vs_staged"]["receipt_range_gib"] == float(PHASE_GIB * 2)
    assert_ledger_matches_the_stage(queue)
