"""pbmetrics: a scrape of an unchanged queue re-reads nothing (#1020).

The exporter answered every scrape by listing every state directory, stat-ing
every terminal record and every movement receipt, and re-reading every
residency plan.  On the live queue (31k done records, 7.5k receipts, 330
plans) that was about 1,000 NFS lookups a second from each box that ran it,
around the clock, and its cost grew with the fleet's history rather than with
its live work.

These tests size a ``tmp_path`` queue like the live one and count what a
scrape does to the filesystem: directory listings, ``stat`` calls and file
opens.  The exporter's server path (``MetricsCache``) is what is measured, so
what the exporter keeps from one scrape to the next is what is tested.

The same fixture and the same probe are what the before/after profile of
#1020 was measured with (``python tests/test_pbmetrics_kept_reads.py
--profile``), so the numbers in the pull request and the assertions here come
from one instrument.
"""
from __future__ import annotations

import argparse
import builtins
from collections import Counter
from contextlib import contextmanager
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import time
from unittest import mock

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import pbmetrics  # noqa: E402
import stage_move  # noqa: E402

HOSTS = ("sparky", "gx10-6b77", "dl380g10")
STAGE_TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
WINDOW_S = 3600.0
LIMIT = 500

#: The live queue's shape on 2026-09-23 (dl380g10, ``ls -f | wc -l``), scaled
#: down where noted.  Plans are 40 rather than 330: each one is a frozen plan
#: plus a ledger holding, and 40 already puts the plan census in every
#: number the probe reports.
LIVE_SHAPE = {
    "done": 30_000, "failed": 7_000, "withdrawn": 1_900, "decisions": 1_800,
    "superseded": 1_500, "movers": 6_000, "plans": 40, "workers": 200,
    "ready": 4, "claimed": 11,
}

#: A small queue with every family in it, for the output comparisons.
SMALL_SHAPE = {
    "done": 40, "failed": 12, "withdrawn": 6, "decisions": 5,
    "superseded": 9, "movers": 30, "plans": 4, "workers": 8,
    "ready": 4, "claimed": 5,
}


# --------------------------------------------------------------------------
# The probe
# --------------------------------------------------------------------------

class _CountedEntry:
    """An ``os.DirEntry`` whose ``stat()`` is counted the first time it runs.

    ``DirEntry.stat`` is a C method and cannot be patched, and it caches its
    answer per entry, so only its first call per ``follow_symlinks`` mode
    reaches the filesystem.  ``is_file``/``is_dir`` answer from the listing's
    ``d_type`` on Linux and are not counted.
    """

    __slots__ = ("_entry", "_probe", "_statted")

    def __init__(self, entry: os.DirEntry, probe: "FsProbe") -> None:
        self._entry = entry
        self._probe = probe
        self._statted: set[bool] = set()

    def stat(self, *, follow_symlinks: bool = True):
        if follow_symlinks not in self._statted:
            self._statted.add(follow_symlinks)
            self._probe.stats.append(self._entry.path)
        return self._entry.stat(follow_symlinks=follow_symlinks)

    def __getattr__(self, name: str):
        return getattr(self._entry, name)

    def __fspath__(self) -> str:
        return self._entry.path

    def __repr__(self) -> str:
        return repr(self._entry)


class _CountedScan:
    def __init__(self, scan, probe: "FsProbe") -> None:
        self._scan = scan
        self._probe = probe

    def __iter__(self):
        for entry in self._scan:
            yield _CountedEntry(entry, self._probe)

    def __next__(self):
        return _CountedEntry(next(self._scan), self._probe)

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self._scan.close()

    def close(self) -> None:
        self._scan.close()


class FsProbe:
    """Counts the filesystem operations one block performs.

    ``listings`` are ``scandir``/``listdir`` calls, ``stats`` every ``stat``
    and ``lstat`` (including a directory entry's first ``stat``), and
    ``opens`` every ``open``, ``io.open`` and ``os.open``.  On the NFS
    mount each is at least one LOOKUP or GETATTR unless the client answers
    it from its attribute cache, so their sum is the lookup count #1020
    measures.
    """

    def __init__(self) -> None:
        self.listings: list[str] = []
        self.stats: list[str] = []
        self.opens: list[str] = []

    @property
    def lookups(self) -> int:
        return len(self.listings) + len(self.stats) + len(self.opens)

    def summary(self) -> dict[str, int]:
        return {"listings": len(self.listings), "stats": len(self.stats),
                "opens": len(self.opens), "lookups": self.lookups}

    @contextmanager
    def active(self):
        real = {"scandir": os.scandir, "listdir": os.listdir, "stat": os.stat,
                "lstat": os.lstat, "os_open": os.open, "open": builtins.open}

        def scandir(path="."):
            self.listings.append(os.fspath(path))
            return _CountedScan(real["scandir"](path), self)

        def listdir(path="."):
            self.listings.append(os.fspath(path))
            return real["listdir"](path)

        def stat(path, *args, **kwargs):
            self.stats.append(os.fspath(path) if not isinstance(path, int) else "<fd>")
            return real["stat"](path, *args, **kwargs)

        def lstat(path, *args, **kwargs):
            self.stats.append(os.fspath(path))
            return real["lstat"](path, *args, **kwargs)

        def os_open(path, *args, **kwargs):
            self.opens.append(os.fspath(path))
            return real["os_open"](path, *args, **kwargs)

        def open_(file, *args, **kwargs):
            if not isinstance(file, int):
                self.opens.append(os.fspath(file))
            return real["open"](file, *args, **kwargs)

        with (mock.patch.object(os, "scandir", scandir),
              mock.patch.object(os, "listdir", listdir),
              mock.patch.object(os, "stat", stat),
              mock.patch.object(os, "lstat", lstat),
              mock.patch.object(os, "open", os_open),
              mock.patch.object(builtins, "open", open_),
              mock.patch.object(io, "open", open_)):
            yield self


# --------------------------------------------------------------------------
# The fixture queue
# --------------------------------------------------------------------------

def _key(prefix: str, index: int) -> str:
    return hashlib.sha256(f"{prefix}:{index}".encode()).hexdigest()


def _put(path: Path, value: object, mtime: float | None = None) -> None:
    data = json.dumps(value, separators=(",", ":")).encode()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(descriptor, data)
    finally:
        os.close(descriptor)
    if mtime is not None:
        os.utime(path, (mtime, mtime))


def _replace(path: Path, value: object, mtime: float | None = None) -> None:
    """Rewrite one record the way every queue writer does: by rename."""

    temporary = path.with_name(f".{path.name}.tmp")
    _put(temporary, value, mtime)
    os.replace(temporary, path)


def _age(index: int, recent: int) -> float:
    """Seconds before now: ``recent`` records inside the hour, the rest older.

    Distinct for every index, so no two records share an mtime and the
    newest-first selection has no ties to break.
    """

    if index < recent:
        return 30.0 + index * (WINDOW_S - 60.0) / max(recent, 1)
    return WINDOW_S + 60.0 + (index - recent) * 37.0


def _ending(key: str, status: str, host: str, finished: float,
            *, profiled: bool) -> dict:
    detail: dict[str, object] = {"elapsed_s": 60.0, "returncode": 0}
    if profiled:
        detail["resource_profile"] = {
            "scope": {"memory_peak_bytes": 2_000_000_000},
            "process_io": {"read_bytes": 5_000_000, "write_bytes": 7_000_000},
            "box_window": {"gpu": {
                "power_w_peak": 60.0, "power_reference_w": 140.0,
                "power_reference_scope": "measured_peak",
                "power_peak_fraction_of_reference": 60.0 / 140.0}}}
    return {"schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": key,
            "status": status, "claimed_host": host, "finished_host": host,
            "published_unix": finished - 300.0, "claimed_unix": finished - 120.0,
            "finished_unix": finished, "detail": detail}


def _offer(host: str, announced: float, *, gpu: bool) -> dict:
    return {"schema": pool.POOL_OFFER_SCHEMA_V1, "host": host,
            "announced_unix": announced, "tags": [host], "has_gpu": gpu,
            "capacity": {"cpu": 16, "gpu": int(gpu), "mem_gb": 96},
            "observed_capacity": {"cpu": 14, "gpu": int(gpu), "mem_gb": 90},
            "foreign": {}, "loops": 3,
            "observed_detail": {"mem_available_gb": 80,
                                "gpu_memory_domains": ["shared_system"] if gpu else []}}


def _item(key: str, published: float, *, claimed_by: str | None = None,
          claimed: float | None = None, residency: dict | None = None,
          gpu: bool = False) -> dict:
    record: dict[str, object] = {
        "schema": pool.POOL_ITEM_SCHEMA_V1, "action_key": key,
        "published_unix": published, "published_by": "sparky", "tags": [],
        "needs_gpu": gpu,
        "resources": {"cpu": 2, "mem_gb": 4, **({"gpu": 1} if gpu else {})}}
    if claimed_by is not None:
        record.update({"claimed_unix": claimed, "claimed_host": claimed_by,
                       "claimed_by": f"{claimed_by}:worker-1",
                       "resource_scope": {"nonce": key[:32]}})
    if residency is not None:
        record["residency"] = residency
    return record


def _row(queue: pool.PoolQueue, key: str, resources: dict,
         residency: dict | None = None) -> dict:
    record = {"action_key": key, "cas_root": str(queue.root / "cas"),
              "checkout_root": str(queue.root / "co"),
              "worker_script": str(queue.root / "worker.py"),
              "tags": ["dl380g10"], "resources": resources}
    if residency is not None:
        record["residency"] = residency
    return record


def _mover_residency(start: int, end: int) -> dict:
    return {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
            "manifest_sha256": "9" * 64, "manifest_bytes": end,
            "range_start_bytes": start, "range_end_bytes": end}


def build_queue(root: Path, now: float, shape: dict[str, int]) -> pool.PoolQueue:
    """A queue with every directory the exporter reads, at ``shape``'s sizes.

    Every timestamp is relative to ``now``, and every record's mtime is its
    own time, so the windows select the same records at any ``now``.
    """

    queue = pool.PoolQueue(root)
    with mock.patch.object(pool.socket, "gethostname", lambda: "dl380g10"), \
            mock.patch.object(pool, "_now", lambda: now):
        queue.ensure_layout()
        for name in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED,
                     pool.WITHDRAWN, pool.WORKERS, pool.MOVERS,
                     pool.RESIDENCY_PLANS, pool.RESERVATIONS, pool.PASSES):
            (root / name).mkdir(parents=True, exist_ok=True)
        (root / pool.WITHDRAWN / "superseded").mkdir(exist_ok=True)
        (root / pool.WITHDRAWN / "decisions").mkdir(exist_ok=True)

        # Terminal history.  About one record in 250 is inside the window.
        for state, count, status in ((pool.DONE, shape["done"], "executed"),
                                     (pool.FAILED, shape["failed"], "failed"),
                                     (pool.WITHDRAWN, shape["withdrawn"], "withdrawn")):
            recent = max(2, count // 250)
            for index in range(count):
                key = _key(state, index)
                finished = now - _age(index, recent)
                _put(root / state / f"{key}.json",
                     _ending(key, status, HOSTS[index % len(HOSTS)], finished,
                             profiled=index % 3 == 0),
                     finished)
        # Durable withdrawal decisions, each older than its summary record, so
        # none of them displaces the summary (``pbstatus._ending_paths``).
        for index in range(shape["decisions"]):
            key = _key(pool.WITHDRAWN, index)
            directory = root / pool.WITHDRAWN / "decisions" / key
            directory.mkdir()
            finished = now - _age(index, max(2, shape["withdrawn"] // 250))
            _put(directory / "1.000000.json",
                 {"status": "withdrawn", "action_key": key}, finished - 5.0)
        # Unstarted-claim releases: the time is in the name.
        for index in range(shape["superseded"]):
            key = _key("superseded", index)
            when = now - _age(index, max(2, shape["superseded"] // 30))
            _put(root / pool.WITHDRAWN / "superseded"
                 / f"{key}.{when:.6f}.unstarted-claim.json",
                 {"action_key": key, "claimed_host": HOSTS[index % len(HOSTS)],
                  "claimed_by": f"{HOSTS[index % len(HOSTS)]}:worker-1"}, when)

        # Movement receipts: about one in 60 inside the window.
        for index in range(shape["movers"]):
            key = _key("mover-receipt", index)
            stamped = now - _age(index, max(2, shape["movers"] // 60))
            _put(root / pool.MOVERS / f"{key}.json", {
                "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": key,
                "tier_id": STAGE_TIER, "host": "dl380g10", "unix": stamped,
                "complete": True, "bytes_staged": 2 * storage_tiers.GIB,
                "disk_pacing": {"mean_pool_read_mb_s": 100.0 + index % 200,
                                "pool_read_bytes": (index % 3) * storage_tiers.GIB}},
                 stamped)

        # Worker offers: most of them long stale, one live per host.
        for index in range(shape["workers"]):
            host = HOSTS[index] if index < len(HOSTS) else f"retired-{index:04d}"
            announced = now - (2.0 if index < len(HOSTS) else 86_400.0 + index)
            _put(root / pool.WORKERS / f"{host}.json",
                 _offer(host, announced, gpu=host != "dl380g10"), announced)
        for host in HOSTS:
            adaptive = root / pool.RESERVATIONS / host / "adaptive"
            adaptive.mkdir(parents=True, exist_ok=True)
            _put(adaptive / "cpu-sample.json",
                 {"sampled_unix": now - 1.0, "observation": {"busy_cpus": 1.0}})
            if host != "dl380g10":
                _put(adaptive / "gpu-state.json",
                     {"sampled_unix": now - 2.0,
                      "power_feedback": {"status": "plateau"}})
            (root / pool.RESERVATIONS / host / "telemetry").mkdir(exist_ok=True)

        # The tier, its ledger, and the plans that stage onto it.
        queue.announce_tier({
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
            "tier_id": STAGE_TIER, "tier": "stage", "host": "dl380g10",
            "capacity_bytes": 600 * storage_tiers.GIB, "sampled_unix": now - 5,
            "fill_source": "measured-probing",
            "fill_supply": {"best_mb_s": 300.0, "ceiling_mb_s": 250.0,
                            "may_grow": False, "probing": True,
                            "probe_offer_mb_s": 324}})
        queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 64})
        ledger = queue.tier_ledger(STAGE_TIER)
        plan_consumers = [_key("consumer", index) for index in range(shape["plans"])]
        for index, consumer in enumerate(plan_consumers):
            phases = []
            start = 0
            for ordinal in range(2):
                mover_key = _key(f"plan-mover-{index}", ordinal)
                end = start + storage_tiers.GIB
                phases.append({
                    "name": f"phase-{ordinal:04d}", "start_bytes": start,
                    "end_bytes": end, "stage_gib": 1,
                    "mover_row": {**_row(queue, mover_key, {STAGE_KIND: 1, "mem_gb": 1}),
                                  "residency": _mover_residency(start, end)},
                    "egress_row": _row(queue, _key(f"plan-egress-{index}", ordinal),
                                       {"mem_gb": 1})})
                start = end
            residency_plan.freeze(queue, residency_plan.build_plan(
                consumer_action_key=consumer, tier_id=STAGE_TIER,
                stage_root=str(root / "stage"), manifest_sha256="9" * 64,
                manifest_bytes=start, phases=phases))
            if index % 4 == 0:
                # A booked first phase: the ledger says it, no fragment does.
                assert ledger.acquire(_key(f"plan-mover-{index}", 0), {"stage_gib": 1})
            if index % 5 == 0:
                # A fragment directory whose file names no mover of the plan.
                fragments = queue.residency_fragment_root() / consumer
                fragments.mkdir(parents=True, exist_ok=True)
                _put(fragments / f"{_key('stranger', index)}.json", {"note": "not ours"})

        # Live work: ready rows, then claims with leases and telemetry.  The
        # first claimed row is the first plan's consumer, quiet on phase 0.
        for index in range(shape["ready"]):
            key = _key("ready", index)
            residency = (_mover_residency(0, storage_tiers.GIB)
                         if index == 0 else None)
            _put(root / pool.READY / f"{key}.json",
                 _item(key, now - 40.0 - index, residency=residency))
            _put(queue.passes_path(key),
                 {"passes": index + 1, "first_unix": now - 30.0})
        for index in range(shape["claimed"]):
            key = plan_consumers[0] if index == 0 and plan_consumers else _key("claimed", index)
            host = HOSTS[index % len(HOSTS)]
            residency = ({"schema": pool.RESIDENCY_SCHEMA_V1,
                          "manifest_sha256": "9" * 64,
                          "manifest_bytes": 2 * storage_tiers.GIB,
                          "leads": [_key("plan-mover-0", 0)]}
                         if index == 0 else
                         _mover_residency(0, storage_tiers.GIB) if index == 1 else None)
            record = _item(key, now - 300.0, claimed_by=host, claimed=now - 200.0,
                           residency=residency, gpu=host != "dl380g10")
            _put(root / pool.CLAIMED / f"{key}.json", record)
            lease = {"schema": pool.POOL_LEASE_SCHEMA_V1, "action_key": key,
                     "owner": record["claimed_by"], "host": host,
                     "claimed_unix": record["claimed_unix"],
                     "published_unix": record["published_unix"],
                     "heartbeat_unix": now - 1.0}
            if index == 0:
                lease["progress_observation"] = {
                    "phase": "encode", "quiet_s": 800.0, "grace_s": 900.0,
                    "last_accepted": {"phase": "phase-0000",
                                      "reported_unix": now - 800.0,
                                      "units_completed": 3}}
            _put(queue.lease_path(key), lease)
            _put(root / pool.RESERVATIONS / host / "telemetry" / f"{key}.json", {
                "action_key": key, "nonce": key[:32], "sampled_unix": now - 1.0,
                "complete": True, "cpu_seconds": 20.0 + index, "wall_seconds": 10.0,
                "memory_current_bytes": (index + 1) * 1024 ** 3,
                "memory_peak_bytes": (index + 2) * 1024 ** 3,
                "process_io": {"read_bytes": 1000 * index, "write_bytes": 10 * index}})
        denied_key = _key("ready", 0)
        _put(root / pool.RESERVATIONS / "dl380g10" / "adaptive" / pool.CLAIM_DENIALS, {
            "schema": pool.CLAIM_DENIALS_SCHEMA_V1, "records": {
                "recent": {"action_key": denied_key, "published_unix": now - 40.0,
                           "host": "dl380g10", "reason": "tier_reservation_unavailable",
                           "evidence": {}, "denied_unix": now - 10.0}}})
    return queue


def settle() -> None:
    """Let the coarse clock pass every directory's last change.

    A directory changed inside the current clock tick is never trusted
    (``stage_move._trusted_directory_stamp``), and a fixture written a
    microsecond before the first scrape is exactly that.  A live queue's
    history directories are almost always many ticks old.
    """

    time.sleep(0.05)


def directories_under(root: Path) -> int:
    return sum(1 for _ in os.walk(root))


def _scrape(cache) -> str:
    return cache.get()


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def live_shaped(tmp_path_factory):
    root = tmp_path_factory.mktemp("kept") / "pb-queue"
    now = time.time()
    build_queue(root, now, LIVE_SHAPE)
    settle()
    return root


def _require_trusted(root: Path) -> None:
    # A skip would certify nothing: a queue on a filesystem whose directory
    # stamps are never trusted (NFS) lists every directory every scrape by
    # design, and the test must say so rather than pass vacuously.
    stamp = stage_move._trusted_directory_stamp(root / pool.DONE)
    assert stamp is not None, (
        f"{root} is on {stage_move._filesystem_type(os.lstat(root).st_dev)!r}, "
        "whose directory stamps are not trusted; this test needs a local "
        "filesystem (zfs, ext4, xfs, btrfs or tmpfs)")


def test_idle_scrape_reads_no_record_and_lists_no_directory(live_shaped):
    """The second scrape of an unchanged queue costs one lstat per directory."""

    _require_trusted(live_shaped)
    cache = pbmetrics.MetricsCache(live_shaped, 0.0, WINDOW_S, LIMIT)
    first = _scrape(cache)
    assert "prismabuild_collection_success 1" in first
    probe = FsProbe()
    with probe.active():
        second = _scrape(cache)
    assert "prismabuild_collection_success 1" in second
    assert probe.listings == [], (
        f"{len(probe.listings)} listings on an unchanged queue, e.g. "
        f"{Counter(Path(p).name for p in probe.listings).most_common(5)}")
    assert probe.opens == [], (
        f"{len(probe.opens)} opens on an unchanged queue, e.g. "
        f"{Counter(Path(p).parent.name for p in probe.opens).most_common(5)}")
    not_directories = [path for path in probe.stats if not os.path.isdir(path)]
    assert not_directories == [], (
        f"{len(not_directories)} stats of records on an unchanged queue, e.g. "
        f"{Counter(Path(p).parent.name for p in not_directories).most_common(5)}")
    assert len(probe.stats) <= directories_under(live_shaped)


def test_one_new_ending_is_the_only_record_read(live_shaped):
    """A changed directory is listed again; only the new record is opened."""

    _require_trusted(live_shaped)
    cache = pbmetrics.MetricsCache(live_shaped, 0.0, WINDOW_S, LIMIT)
    _scrape(cache)
    key = _key("late-ending", 0)
    path = live_shaped / pool.DONE / f"{key}.json"
    _replace(path, _ending(key, "executed", "sparky", time.time() - 1.0,
                           profiled=True))
    try:
        settle()
        probe = FsProbe()
        with probe.active():
            text = _scrape(cache)
        assert 'prismabuild_terminal_outcomes{host="sparky",outcome="executed"}' in text
        opened = [p for p in probe.opens if os.path.isfile(p)]
        assert opened == [str(path)], (
            f"{len(opened)} records opened after one new ending, e.g. "
            f"{Counter(Path(p).parent.name for p in opened).most_common(5)}")
        assert probe.listings == [str(live_shaped / pool.DONE)]
    finally:
        path.unlink()
        settle()


GOLDEN = Path(__file__).resolve().parent / "data" / "pbmetrics_kept_reads_main.prom"
#: The fixed clock the golden output was taken at.  Every record is placed
#: relative to it, so the output is a pure function of the fixture.
GOLDEN_NOW = 1_790_000_000.0


@contextmanager
def frozen_clock(now: float):
    """``time.time`` and the queue's clock pinned, as ``test_pbmetrics`` pins them.

    The directory stamps read the coarse realtime clock through
    ``clock_gettime_ns``, which this leaves alone.
    """

    with (mock.patch.object(pbmetrics.time, "time", return_value=now),
          mock.patch.object(pool, "_now", return_value=now)):
        yield


def golden_text(root: Path) -> str:
    """What the exporter reports for the small fixture at ``GOLDEN_NOW``."""

    with frozen_clock(GOLDEN_NOW):
        build_queue(root, GOLDEN_NOW, SMALL_SHAPE)
        return pbmetrics.collect_metrics(
            root, now=GOLDEN_NOW, terminal_window_seconds=WINDOW_S,
            terminal_limit=LIMIT)


def test_output_is_mains(tmp_path):
    """Every family, label and value main reported for this fixture, unchanged.

    ``tests/data/pbmetrics_kept_reads_main.prom`` was written by main's
    exporter (``72b98a871bbb``) from this fixture, with
    ``python tests/test_pbmetrics_kept_reads.py --golden``.
    """

    assert golden_text(tmp_path / "pb-queue") == GOLDEN.read_text()


# --------------------------------------------------------------------------
# Profile harness: the before/after numbers in #1020
# --------------------------------------------------------------------------

def _profile(argv: list[str]) -> int:
    import cProfile
    import pstats

    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--label", required=True)
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    build_queue(args.root, started, LIVE_SHAPE)
    settle()
    report: dict[str, object] = {"label": args.label, "shape": LIVE_SHAPE,
                                 "directories": directories_under(args.root),
                                 "filesystem": stage_move._filesystem_type(
                                     os.lstat(args.root).st_dev)}
    cache = pbmetrics.MetricsCache(args.root, 0.0, WINDOW_S, LIMIT)
    for scrape in ("first", "second", "one-ending"):
        if scrape == "one-ending":
            # One action finishes between scrapes: the busy queue's case.
            key = _key("late-ending", 0)
            _replace(args.root / pool.DONE / f"{key}.json",
                     _ending(key, "executed", "sparky", time.time() - 1.0,
                             profiled=True))
            settle()
        probe = FsProbe()
        profile = cProfile.Profile()
        clock = time.perf_counter()
        with probe.active():
            profile.enable()
            text = cache.get()
            profile.disable()
        elapsed = time.perf_counter() - clock
        profile.dump_stats(str(args.out / f"{args.label}-{scrape}.prof"))
        with open(args.out / f"{args.label}-{scrape}.txt", "w") as stream:
            pstats.Stats(profile, stream=stream).sort_stats(
                "cumulative").print_stats(40)
        report[scrape] = {**probe.summary(), "wall_s": round(elapsed, 4),
                          "success": "prismabuild_collection_success 1" in text,
                          "listed": Counter(Path(p).parent.name if Path(p).parent.name
                                            in ("decisions", "residency") else Path(p).name
                                            for p in probe.listings).most_common(8),
                          "opened": Counter(Path(p).parent.name
                                            for p in probe.opens).most_common(8)}
    # The same three cases with no probe and no profiler: what the scrape
    # costs the box that runs it.
    plain: dict[str, float] = {}
    clock = time.perf_counter()
    pbmetrics.MetricsCache(args.root, 0.0, WINDOW_S, LIMIT).get()
    plain["first"] = time.perf_counter() - clock
    settle()
    clock = time.perf_counter()
    cache.get()
    plain["second"] = time.perf_counter() - clock
    key = _key("late-ending", 1)
    _replace(args.root / pool.DONE / f"{key}.json",
             _ending(key, "executed", "sparky", time.time() - 1.0, profiled=True))
    settle()
    clock = time.perf_counter()
    cache.get()
    plain["one-ending"] = time.perf_counter() - clock
    report["uninstrumented_wall_s"] = {name: round(value, 4)
                                       for name, value in plain.items()}
    import resource
    report["max_rss_mib"] = round(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    (args.out / f"{args.label}.json").write_text(json.dumps(report, indent=1))
    print(json.dumps(report, indent=1))
    return 0


def _golden(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    args.out.write_text(golden_text(args.root))
    return 0


if __name__ == "__main__" and "--golden" in sys.argv:
    sys.argv.remove("--golden")
    raise SystemExit(_golden(sys.argv[1:]))

if __name__ == "__main__" and "--profile" in sys.argv:
    sys.argv.remove("--profile")
    raise SystemExit(_profile(sys.argv[1:]))
