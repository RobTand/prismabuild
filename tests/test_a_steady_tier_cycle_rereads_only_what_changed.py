"""A steady tier cycle re-reads only what changed since the last one (#992).

#944 recorded the tier loop on dl380g10 at about 99% of one core, and the
audit (WS-DA finding 6) named why: every 5 s cycle listed ``done/``,
``failed/`` and ``withdrawn/`` (38,000 names on 2026-09-23), parsed every
residency fragment, and read every movement receipt and every ``ready/`` and
``claimed/`` record, whether or not anything had changed.

The queue retains terminal records, movement receipts and residency
namespaces, plus live consumers with frozen plans, landed ranges and a copy
in flight. The builder is shared with ``tools/fleet/bench_tier_cycle.py``;
the ordinary suite uses a small shape instead of rebuilding the campaign
forest per test. A cycle after one in which nothing changed must:

* list none of the terminal directories (each candidate is looked up by
  key);
* parse no fragment and no queue record, and list no directory the tier
  loop keeps (its stamps all hold);
* record ``cycle_seconds`` and the seconds of every step in
  ``tier_loop.LAST_CYCLE``, which the ``tier-cycle`` line carries;
* measure process CPU seconds for diagnostics, not a machine-specific
  correctness budget. Quantitative performance qualification belongs in
  ``tools/fleet/bench_tier_cycle.py``.

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
import shutil
import sys
import time
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import kept_reads, pool, residency_map  # noqa: E402
import bench_tier_cycle as bench  # noqa: E402
from prismabuild import stage_release  # noqa: E402
from prismabuild import tier_cycle as tier_loop  # noqa: E402
from prismabuild import staged_path

#: The steps of a cycle, as ``LAST_CYCLE["phases"]`` names them.
PHASES = {
    "reclaim_idle_rates", "receipts", "sync_ram_host_mirror", "discover",
    "mint_announce", "manifest_promotion",
    "drop_prior_ram_epochs", "release_incomplete_ram_promotions",
    "planned_consumers", "withdrawn_keys", "withdraw_dead_consumer_movers",
    "adopt_resident_ranges", "window_pressure",
    "reclaim_failed_mover_partials", "sweep_orphans", "evict_beyond_horizon",
    "fan_out_shared_ranges", "ram_residency_window", "residency_window",
    "output_scope_tick", "retire_terminal_output_funding", "origin_retirement_tick",
    "deferred_release", "census_cost_and_retired_tiers",
}


def _small_shape() -> argparse.Namespace:
    """All queue and residency cases at unit-test scale."""

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
        self.queue.records.ensure_layout()
        self.stage = tmp_path / "stage"
        self.stage.mkdir()
        self.counts = bench.build_queue(self.queue, self.stage,
                                        shape or _small_shape())
        self.receipts = tier_loop.ReceiptCache()
        self._directory_clock = time.time_ns()

    def changed_directory(self, path: Path) -> None:
        self._directory_clock = max(self._directory_clock, time.time_ns()) + 1000000000
        os.utime(path, ns=(self._directory_clock, self._directory_clock))

    def record_move(self, key: str, document: dict) -> None:
        self.queue.record_move(key, document)
        self.changed_directory(self.queue.root / tier_loop.MOVER_RECEIPTS)

    def cycle(self) -> list[dict[str, object]]:
        # Keep the production trust checks, but put their fence after both
        # explicitly advanced mtimes and kernel-assigned ctimes. Real time
        # need not catch up with the fixture's directory clock. Patch only
        # kept_reads' clock binding, not the process-wide time module.
        clock = SimpleNamespace(clock_gettime_ns=lambda _clock: max(
            self._directory_clock, time.time_ns()) + 1000000000)
        with pytest.MonkeyPatch.context() as trust_clock:
            trust_clock.setattr(kept_reads, "time", clock)
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


def _clock_root(tmp_path: Path) -> Path:
    """Use the admitted local-disk temporary directory."""
    root = tmp_path / "local-clock"
    root.mkdir()
    return root


@pytest.fixture()
def loop(tmp_path: Path) -> _Loop:
    root = _clock_root(tmp_path)
    try:
        built = _Loop(root)
        assert built.counts[pool.record_store.DONE] == 5
        assert built.counts["receipts"] == 10
        namespaces = [entry for entry in os.scandir(
            built.queue.records.residency_fragment_root()) if entry.is_dir()
            and len(entry.name) == 64]
        assert len(namespaces) >= 6, len(namespaces)
        assert _filesystem_type(root) in LOCAL_CLOCK, (
            _filesystem_type(root))
        yield built
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_a_steady_cycle_reads_only_what_changed(
        loop: _Loop, monkeypatch: pytest.MonkeyPatch) -> None:
    loop.cycle()          # cold: every cache is empty
    loop.cycle()          # what the cold cycle itself filed is read once
    queue = loop.queue
    terminal = _listings(monkeypatch, [
        queue.records.dir(pool.record_store.DONE), queue.records.dir(pool.record_store.FAILED),
        queue.records.dir(pool.record_store.WITHDRAWN)])
    started = time.process_time()
    loop.cycle()
    cpu = time.process_time() - started
    assert terminal == [], terminal
    # CPU cost depends on processor and interpreter; the read guards below
    # are the portable verdict. Keep timing visible in captured diagnostics.
    print(f"steady cycle took {cpu:.3f} s of CPU")
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
    assert reads["records_kept"] > 0 and reads["census_kept"] >= 6, reads


def test_a_steady_cycle_does_not_list_ledger_directories(
        loop: _Loop, monkeypatch: pytest.MonkeyPatch) -> None:
    """The census also reuses unchanged token names (#1027)."""

    loop.cycle()
    loop.cycle()
    ledger = loop.queue.tier(bench.TIER)
    directories = [ledger.paths.free, ledger.paths.held, ledger.paths.minted, ledger.paths.minted / 'dead', *pool.immutable_files.scan_optional(ledger.paths.held)]
    listings = _listings(monkeypatch, directories)
    loop.cycle()
    reads = tier_loop.LAST_CYCLE["reads"]
    assert listings == [], listings
    assert reads.get("ledger_listed") == 0, reads
    assert reads.get("ledger_kept", 0) > 0, reads


def test_a_change_between_cycles_is_read_by_the_next(loop: _Loop) -> None:
    """A filed receipt and a written fragment reach the very next cycle."""

    loop.cycle()
    loop.cycle()
    before = loop.cycle()[0]["fill_records"]
    queue = loop.queue
    mover = bench._key("late-receipt")
    late = bench._key("late-fragment")
    loop.record_move(mover, {"tier_id": bench.TIER, "unix": time.time(),
                             "complete": True})
    root = queue.records.residency_fragment_root()
    namespace = bench.bench_stage_adopt._key("empty0")
    assert not any((root / namespace).iterdir())
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
    loop.changed_directory(root / namespace)
    after = loop.cycle()[0]["fill_records"]
    reads = tier_loop.LAST_CYCLE["reads"]
    assert after == before + 1
    assert reads["records_parsed"] >= 1, reads
    # The namespace the fragment landed in was listed again and the new
    # fragment parsed, by every census the cycle took; nothing else was.
    assert reads["census_listed"] >= 1 and reads["census_parses"] >= 1, reads
    kept = loop.receipts.census.namespaces._slots.get(str(root / namespace))
    assert kept is not None
    assert [name for name, _document in kept.value[0]] == [late]


def _write_late_fragment(loop: _Loop, name: str) -> None:
    root = loop.queue.records.residency_fragment_root()
    namespace = bench.bench_stage_adopt._key("empty0")
    assert not any((root / namespace).iterdir())
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
    loop.changed_directory(root / namespace)


def test_what_the_loop_keeps_stays_what_is_on_disk(loop: _Loop) -> None:
    """Every kept document equals a fresh parse of its file, cycles later.

    The kept census documents and receipts are the objects every later
    cycle is handed; a step that changed one in place would change what
    every following cycle reads without touching the file.
    """

    loop.cycle()
    loop.cycle()
    loop.record_move(bench._key("kept-receipt"), {
        "tier_id": bench.TIER, "unix": time.time(), "complete": True})
    _write_late_fragment(loop, "kept-fragment")
    loop.cycle()
    loop.cycle()
    census = loop.receipts.census
    assert len(census.fragments) >= 5, len(census.fragments)
    for key, (_version, document) in census.fragments.items():
        with open(key) as stream:
            fresh = residency_map.validate_fragment(json.load(stream))
        assert document == fresh, key
        paths = census.paths_of(document)
        if paths is not None:
            assert paths == staged_path._fragment_stage_paths(fresh), key
    remembered = {id(document) for _version, document
                  in census.fragments.values()}
    assert census.namespaces._slots
    for directory, slot in census.namespaces._slots.items():
        found, _files = slot.value
        for mover, document in found:
            assert id(document) in remembered, (directory, mover)
    receipts = loop.queue.root / tier_loop.MOVER_RECEIPTS
    parsed = loop.receipts.records.parsed
    store = pool.record_store.RecordStore.kept(
        loop.queue.root, records=loop.receipts.records)
    kept = {path.name: (None, record)
            for path, record in store.receipt_records_at(receipts)}
    assert loop.receipts.records.parsed == parsed, "the retained receipt query reparsed"
    assert len(kept) >= 10, len(kept)
    for name, (_version, record) in kept.items():
        assert record == pool.record_store.read_document(receipts / name), name


def test_an_unreadable_receipt_directory_is_skipped_and_named(
        tmp_path: Path, monkeypatch) -> None:
    small = _Loop(tmp_path)
    prewarm = small.queue.root / pool.record_store.PREWARM
    prewarm.mkdir(parents=True, exist_ok=True)
    small.cycle()
    assert tier_loop.LAST_CYCLE["completed"] is True
    assert not tier_loop.LAST_CYCLE.get("receipts_unreadable")
    before = small.cycle()[0]["fill_records"]
    native_listdir, native_scandir = os.listdir, os.scandir

    def is_target(path):
        if isinstance(path, int):
            opened, wanted = os.fstat(path), prewarm.stat()
            return (opened.st_dev, opened.st_ino) == (wanted.st_dev, wanted.st_ino)
        path = Path(path)
        if str(path).startswith("/proc/self/fd/"):
            path = path.resolve()
        return path == prewarm

    def listdir(path=".", *args, **kwargs):
        if is_target(path):
            raise PermissionError(str(prewarm))
        return native_listdir(path, *args, **kwargs)

    def scandir(path=".", *args, **kwargs):
        if is_target(path):
            raise PermissionError(str(prewarm))
        return native_scandir(path, *args, **kwargs)

    small.changed_directory(prewarm)
    with monkeypatch.context() as denied:
        denied.setattr(os, "listdir", listdir)
        denied.setattr(os, "scandir", scandir)
        small.cycle()
        recorded = tier_loop.LAST_CYCLE
        assert recorded["completed"] is True
        assert list(recorded["receipts_unreadable"]) == [str(prewarm)], (
            recorded.get("receipts_unreadable"))
    small.changed_directory(prewarm)
    after = small.cycle()[0]["fill_records"]
    assert not tier_loop.LAST_CYCLE.get("receipts_unreadable")
    assert after == before
