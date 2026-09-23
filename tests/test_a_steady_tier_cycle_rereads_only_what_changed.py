"""A steady tier cycle re-reads only what changed since the last one (#992).

#944 recorded the tier loop on dl380g10 at about 99% of one core, and the
audit (WS-DA finding 6) named why: every 5 s cycle listed ``done/``,
``failed/`` and ``withdrawn/`` (38,000 names on 2026-09-23), parsed every
residency fragment, and read every movement receipt and every ``ready/`` and
``claimed/`` record, whether or not anything had changed.

The queue here has the issue's shape -- 30,000 done records, 6,000 movement
receipts and 400 residency namespaces -- plus live consumers with frozen
plans, landed ranges and a copy in flight, built by the same code as
``tools/fleet/bench_tier_cycle.py``.  A cycle after one in which nothing
changed must:

* list none of the terminal directories (each candidate is looked up by
  key);
* parse no fragment and no queue record, and list no directory the tier
  loop keeps (its stamps all hold);
* record ``cycle_seconds`` and the seconds of every step in
  ``tier_loop.LAST_CYCLE``, which the ``tier-cycle`` line carries;
* finish under ``STEADY_CPU_BOUND_S`` of CPU (the derivation is at the
  constant).

And a change must still be seen: a receipt filed and a fragment written
between two cycles are read by the next one.

What the loop keeps is handed to every later cycle, so it must stay what is
on disk: after several cycles every kept fragment and receipt equals a fresh
parse of its file.  And a receipt directory that cannot be read is skipped
and named on the cycle's record, as the plain read skipped it; it does not
fail every cycle, which would stop the windows and landing records the
loop publishes.

Nothing here touches the live queue, a real stage root or a real device.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool, residency_map  # noqa: E402
import bench_tier_cycle as bench  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

#: CPU seconds one steady cycle may take at this shape.  Derived from the
#: after-fix measurement at exactly this shape on sparky (GB10), through
#: pbrun, ``bench_tier_cycle.py --done 30000 --receipts 6000 --empty-dirs 370
#: --small-dirs 30`` (the rest of ``_shape`` is the bench's default): a steady
#: median of MEASURED_STEADY_CPU_S over ten cycles on two Cortex-X925 cores
#: (action db8b95ac885b), against BASE_STEADY_CPU_S on the tree before the
#: fix, also on X925 cores (1549e2a2aca1).  The same tree placed on two
#: Cortex-A725 efficiency cores measured 0.351 s (1fcd0b355def).  The bound
#: is four times the X925 median, so an efficiency core or a loaded box does
#: not fail it, and about a third of the base: bringing back the census parse
#: or the terminal listings the fix removed crosses it.  CPU time rather than
#: wall, because the box the test runs on is shared.
MEASURED_STEADY_CPU_S = 0.159
BASE_STEADY_CPU_S = 1.813
STEADY_CPU_BOUND_S = 4 * MEASURED_STEADY_CPU_S

#: The steps of a cycle, as ``LAST_CYCLE["phases"]`` names them.
PHASES = {
    "reclaim_idle_rates", "receipts", "discover", "mint_announce",
    "drop_prior_ram_epochs", "release_incomplete_ram_promotions",
    "planned_consumers", "withdrawn_keys", "withdraw_dead_consumer_movers",
    "adopt_resident_ranges", "window_pressure",
    "reclaim_failed_mover_partials", "sweep_orphans", "evict_beyond_horizon",
    "ram_residency_window", "residency_window",
    "retire_terminal_output_funding", "origin_retirement_tick",
    "deferred_release", "census_cost_and_retired_tiers",
}


def _shape() -> argparse.Namespace:
    """The issue's widths; everything else is the live queue's (2026-09-23).

    400 flat namespaces (370 empty, 30 with small fragments) beside four
    large fragments and 1,957 produced-output directories: 240,705 fragment
    entries in all, which is what a census parses.
    """

    return argparse.Namespace(
        done=30000, failed=7142, withdrawn=1877, receipts=6000, passes=1324,
        empty_dirs=370, small_dirs=30, big_fragments="90000,60000,50000,40000",
        produced_fragment_dirs=1957, live_consumers=6, phases=3,
        files_per_range=64, ready_noise=30, claimed_noise=28)


def _small_shape() -> argparse.Namespace:
    """The same parts at a handful each, for what is not about width."""

    return argparse.Namespace(
        done=5, failed=2, withdrawn=2, receipts=10, passes=3,
        empty_dirs=2, small_dirs=1, big_fragments="20",
        produced_fragment_dirs=2, live_consumers=2, phases=3,
        files_per_range=4, ready_noise=2, claimed_noise=2)


class _Loop:
    """A tmp_path queue and stage, and the loop's own kept reads."""

    def __init__(self, tmp_path: Path,
                 shape: argparse.Namespace | None = None) -> None:
        self.queue = pool.PoolQueue(tmp_path / "pb-queue")
        self.queue.ensure_layout()
        self.stage = tmp_path / "stage"
        self.stage.mkdir()
        self.counts = bench.build_queue(self.queue, self.stage,
                                        shape or _shape())
        self.receipts = tier_loop.ReceiptCache()

    def cycle(self) -> list[dict[str, object]]:
        return tier_loop.cycle(
            self.queue, host=bench.HOST, source_pool="storage_pool",
            receipts=self.receipts,
            discover=lambda **_kw: {bench.TIER: bench._tier_record(self.stage)})


def _listings(monkeypatch: pytest.MonkeyPatch,
              directories: list[Path]) -> list[str]:
    """Every ``os.listdir``/``os.scandir`` of ``directories`` from now on."""

    wanted = {str(path) for path in directories}
    calls: list[str] = []
    real_listdir, real_scandir = os.listdir, os.scandir

    def listdir(path=".", *args, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(path, (str, os.PathLike)) and str(path) in wanted:
            calls.append(str(path))
        return real_listdir(path, *args, **kwargs)

    def scandir(path=".", *args, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(path, (str, os.PathLike)) and str(path) in wanted:
            calls.append(str(path))
        return real_scandir(path, *args, **kwargs)

    monkeypatch.setattr(os, "listdir", listdir)
    monkeypatch.setattr(os, "scandir", scandir)
    return calls


def _filesystem_type(path: Path) -> str | None:
    """The type ``/proc/self/mountinfo`` names for ``path``'s device."""

    device = os.stat(path).st_dev
    wanted = f"{os.major(device)}:{os.minor(device)}"
    with open("/proc/self/mountinfo") as stream:
        for line in stream:
            fields = line.split()
            if len(fields) > 2 and fields[2] == wanted and " - " in line:
                return line.split(" - ", 1)[1].split()[0]
    return None


#: Filesystems whose directory times come from this kernel's clock: the
#: only ones on which the index may skip a listing (``stage_move``).
LOCAL_CLOCK = {"zfs", "ext4", "xfs", "btrfs", "tmpfs"}


@pytest.fixture()
def loop(tmp_path: Path) -> _Loop:
    built = _Loop(tmp_path)
    assert built.counts[pool.DONE] == 30000
    assert built.counts["receipts"] == 6000
    namespaces = [entry for entry in os.scandir(
        built.queue.residency_fragment_root()) if entry.is_dir()
        and len(entry.name) == 64]
    assert len(namespaces) >= 400, len(namespaces)
    # The stamps the index keeps are trusted only on a filesystem whose
    # directory times come from this kernel's clock; the fixture must be on
    # one, or the test below measures the fallback instead of the index.
    assert _filesystem_type(tmp_path) in LOCAL_CLOCK, (
        _filesystem_type(tmp_path))
    return built


def test_a_steady_cycle_reads_only_what_changed(
        loop: _Loop, monkeypatch: pytest.MonkeyPatch) -> None:
    loop.cycle()          # cold: every cache is empty
    loop.cycle()          # what the cold cycle itself filed is read once
    queue = loop.queue
    terminal = _listings(monkeypatch, [
        queue.dir(pool.DONE), queue.dir(pool.FAILED),
        queue.dir(pool.WITHDRAWN)])
    started = time.process_time()
    loop.cycle()
    cpu = time.process_time() - started
    assert terminal == [], terminal
    assert cpu < STEADY_CPU_BOUND_S, (
        f"steady cycle took {cpu:.3f} s of CPU, bound {STEADY_CPU_BOUND_S} s")
    recorded = tier_loop.LAST_CYCLE
    assert recorded["completed"] is True
    assert set(recorded["phases"]) == PHASES, sorted(
        set(recorded["phases"]) ^ PHASES)
    assert isinstance(recorded["cycle_seconds"], float)
    assert recorded["cycle_seconds"] >= max(recorded["phases"].values())
    reads = recorded["reads"]
    # Nothing changed, so nothing is listed or parsed again: every kept
    # directory's stamp held.
    assert reads["records_parsed"] == 0, reads
    assert reads["records_listed"] == 0, reads
    assert reads["census_parses"] == 0, reads
    assert reads["census_listed"] == 0, reads
    assert reads["records_kept"] > 0 and reads["census_kept"] >= 400, reads


def test_a_change_between_cycles_is_read_by_the_next(loop: _Loop) -> None:
    """A filed receipt and a written fragment reach the very next cycle."""

    loop.cycle()
    loop.cycle()
    before = loop.cycle()[0]["fill_records"]
    queue = loop.queue
    mover = bench._key("late-receipt")
    late = bench._key("late-fragment")
    queue.record_move(mover, {"tier_id": bench.TIER, "unix": time.time(),
                              "complete": True})
    root = queue.residency_fragment_root()
    namespace = sorted(entry.name for entry in os.scandir(root)
                       if entry.is_dir() and len(entry.name) == 64)[0]
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": namespace,
        "mover_action_key": late,
        "tier_id": bench.TIER, "stage_root": str(loop.stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(str(loop.stage / "late"), 0): {
                "stage_path": str(loop.stage / "late"), "bytes": 1,
                "sha256": "b" * 64, "offset": 0}}})
    after = loop.cycle()[0]["fill_records"]
    reads = tier_loop.LAST_CYCLE["reads"]
    assert after == before + 1
    assert reads["records_parsed"] >= 1, reads
    # The namespace the fragment landed in was listed again and the new
    # fragment parsed, by every census the cycle took; nothing else was.
    assert reads["census_listed"] >= 1 and reads["census_parses"] >= 1, reads
    kept = loop.receipts.census.namespaces.get(str(root / namespace))
    assert kept is not None
    assert [name for name, _document in kept[1]] == [late]


def _write_late_fragment(loop: _Loop, name: str) -> None:
    root = loop.queue.residency_fragment_root()
    namespace = sorted(entry.name for entry in os.scandir(root)
                       if entry.is_dir() and len(entry.name) == 64)[0]
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": namespace,
        "mover_action_key": bench._key(name),
        "tier_id": bench.TIER, "stage_root": str(loop.stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(str(loop.stage / name), 0): {
                "stage_path": str(loop.stage / name), "bytes": 1,
                "sha256": "b" * 64, "offset": 0}}})


def test_what_the_loop_keeps_stays_what_is_on_disk(loop: _Loop) -> None:
    """Every kept document equals a fresh parse of its file, cycles later.

    The kept census documents and receipts are the objects every later
    cycle is handed; a step that changed one in place would change what
    every following cycle reads without touching the file.
    """

    loop.cycle()
    loop.cycle()
    loop.queue.record_move(bench._key("kept-receipt"), {
        "tier_id": bench.TIER, "unix": time.time(), "complete": True})
    _write_late_fragment(loop, "kept-fragment")
    loop.cycle()
    loop.cycle()
    census = loop.receipts.census
    assert len(census.fragments) >= 100, len(census.fragments)
    for key, (_version, document) in census.fragments.items():
        with open(key) as stream:
            fresh = residency_map.validate_fragment(json.load(stream))
        assert document == fresh, key
        paths = census.paths_of(document)
        if paths is not None:
            assert paths == stage_release._fragment_stage_paths(fresh), key
    remembered = {id(document) for _version, document
                  in census.fragments.values()}
    assert census.namespaces
    for directory, (_stamp, found, _files) in census.namespaces.items():
        for mover, document in found:
            assert id(document) in remembered, (directory, mover)
    receipts = loop.queue.root / tier_loop.MOVER_RECEIPTS
    kept = loop.receipts.records._directories[str(receipts)][1]
    assert len(kept) >= 6000, len(kept)
    for name, (_version, record) in kept.items():
        assert record == pool._read_json(receipts / name), name


def test_an_unreadable_receipt_directory_is_skipped_and_named(
        tmp_path: Path) -> None:
    if os.geteuid() == 0:
        pytest.skip("root reads a directory whatever its mode")
    small = _Loop(tmp_path, _small_shape())
    prewarm = small.queue.root / pool.PREWARM
    prewarm.mkdir(parents=True, exist_ok=True)
    small.cycle()
    assert tier_loop.LAST_CYCLE["completed"] is True
    assert not tier_loop.LAST_CYCLE.get("receipts_unreadable")
    before = small.cycle()[0]["fill_records"]
    prewarm.chmod(0)
    try:
        small.cycle()
        recorded = tier_loop.LAST_CYCLE
        assert recorded["completed"] is True
        assert list(recorded["receipts_unreadable"]) == [str(prewarm)], (
            recorded.get("receipts_unreadable"))
    finally:
        prewarm.chmod(0o755)
    after = small.cycle()[0]["fill_records"]
    assert not tier_loop.LAST_CYCLE.get("receipts_unreadable")
    assert after == before
