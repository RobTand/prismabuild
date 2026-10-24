"""#2172: retaining parsed receipts must also retain their materialization.

Use the real cycle and the existing #992 small replay, not a stand-in reader.
The filesystem/clock model is explicit; no sleeps, host paths, historical git
objects or live-queue writes are needed. Existing #1045 tests remain controls.
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import kept_reads, pool, record_store  # noqa: E402
from prismabuild import tier_cycle  # noqa: E402
from test_a_steady_tier_cycle_rereads_only_what_changed import (  # noqa: E402
    _Loop, _small_shape,
)
import bench_tier_cycle as bench  # noqa: E402
from kept_read_clock import after_paths  # noqa: E402


@pytest.fixture
def local_clock(monkeypatch):
    """Model a trusted local filesystem with writes behind the coarse tick."""
    monkeypatch.setattr(kept_reads, "_filesystem_type", lambda _path: "zfs")
    real = time.clock_gettime_ns
    monkeypatch.setattr(
        time, "clock_gettime_ns",
        lambda clock: time.time_ns() + 10_000_000_000 if clock == 5 else real(clock))


def _root_key(value):
    if isinstance(value, record_store._JsonObject):
        return dict(value.members).get("action_key")
    if isinstance(value, dict):
        return value.get("action_key")
    return None


def _thaws(monkeypatch, keys):
    counts = Counter()
    original = record_store._thaw_json

    def counted(value):
        key = _root_key(value)
        if key in keys:
            counts[key] += 1
        return original(value)

    monkeypatch.setattr(record_store, "_thaw_json", counted)
    return counts


def _receipt_keys(queue):
    return {document["action_key"]
            for family in (record_store.MOVERS, record_store.PREWARM)
            for _path, document in queue.records.receipt_records_at(queue.root / family)}


def test_unchanged_real_cycles_do_not_materialize_receipt_history(
        tmp_path, monkeypatch, local_clock, record_property):
    replay = _Loop(tmp_path, _small_shape())
    for _ in range(3):
        replay.cycle()
    keys = _receipt_keys(replay.queue)
    assert len(keys) >= 10, "the replay must contain historical receipts"
    counts = _thaws(monkeypatch, keys)
    cpu = []
    for _ in range(3):
        start = time.process_time()
        replay.cycle()
        cpu.append(time.process_time() - start)
    record_property("steady_cycle_cpu_s", cpu)
    assert not counts, (
        "unchanged cycle rematerialized historical receipt trees", counts)


def test_one_new_receipt_does_not_rematerialize_older_receipts(
        tmp_path, monkeypatch, local_clock):
    replay = _Loop(tmp_path, _small_shape())
    for _ in range(3):
        replay.cycle()
    old = _receipt_keys(replay.queue)
    counts = _thaws(monkeypatch, old)
    key = "f" * 64
    assert key not in old
    replay.queue.record_move(key, {
        "tier_id": bench.TIER, "complete": True, "unix": time.time(),
        "seconds": 2.0, "bytes_staged": 400_000_000,
        "pool_read_mb_s": 200, "fill_reserved_mb_s": 1000})
    replay.cycle()
    assert key in {item["action_key"] for item in replay.receipts._records}
    assert not counts, ("a delta recopied unchanged receipt history", counts)


def _view(cache, queue):
    documents = cache.read(
        [queue.root / record_store.MOVERS, queue.root / record_store.PREWARM],
        queue=queue)
    return (documents, cache.fill_supply(None), cache.mover_cap(bench.TIER, None))


@pytest.mark.parametrize("change", ["replace", "rewrite", "delete", "corrupt"])
def test_changed_receipts_match_fresh_reads_without_rethawing_the_rest(
        tmp_path, monkeypatch, local_clock, change):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    first, second = "a" * 64, "b" * 64
    for key, rate in ((first, 100), (second, 200)):
        queue.record_move(key, {
            "tier_id": bench.TIER, "complete": True, "seconds": 2,
            "bytes_staged": rate * 2_000_000, "pool_read_mb_s": rate,
            "fill_reserved_mb_s": 500, "nested": {"items": [1, 2]}})
    cache = tier_cycle.ReceiptCache()
    original = _view(cache, queue)
    # A caller may edit its returned trees, but must not poison retained state.
    original[0][0]["nested"]["items"].append("caller mutation")
    path = queue.root / record_store.MOVERS / (first + ".json")
    if change == "replace":
        queue.record_move(first, {
            "tier_id": bench.TIER, "complete": True, "seconds": 2,
            "bytes_staged": 600_000_000, "pool_read_mb_s": 300,
            "fill_reserved_mb_s": 500, "nested": {"items": [3, 4]}})
    elif change == "rewrite":
        # Equal byte length and same inode: parent-directory identity alone
        # cannot reveal this edit. ctime/mtime must participate in validation.
        text = path.read_text()
        changed = text.replace('100', '300')
        assert changed != text, text
        path.write_text(changed)
        # DirectoryRecords' trusted path assumes atomic publishers. Force its
        # existing invalidation boundary open to test the *file* version here.
        notification = path.parent / "invalidation-2172"
        notification.touch()
        notification.unlink()
    elif change == "delete":
        path.unlink()
    else:
        # Atomic publication of malformed input must refuse, not serve the
        # formerly cached valid receipt. Repair then resumes incremental use.
        temporary = path.with_suffix(".tmp")
        temporary.write_text("{invalid receipt")
        temporary.replace(path)
        with pytest.raises(pool.PoolContractError, match="not valid JSON"):
            _view(tier_cycle.ReceiptCache(), queue)
        with pytest.raises(pool.PoolContractError, match="not valid JSON"):
            _view(cache, queue)
        queue.record_move(first, {
            "tier_id": bench.TIER, "complete": True, "seconds": 2,
            "bytes_staged": 600_000_000, "pool_read_mb_s": 300,
            "fill_reserved_mb_s": 500, "nested": {"items": [3, 4]}})
    expected = _view(tier_cycle.ReceiptCache(), queue)
    # Count frozen->mutable conversions, not the defensive copies owed to a
    # public caller. This leaves the private cycle snapshot API unconstrained.
    materialized = Counter()
    real = record_store._thawed_document

    def thaw(path, document):
        materialized[Path(path).name] += 1
        return real(path, document)

    monkeypatch.setattr(record_store, "_thawed_document", thaw)
    actual = _view(cache, queue)
    assert actual == expected
    assert materialized[second + ".json"] == 0, (
        "invalidating one file materialized an unchanged sibling", materialized)


def test_same_tick_versions_are_not_promoted_to_the_thawed_cache(
        tmp_path, monkeypatch):
    """A same-tick rewrite stays visible; only a later stable tick is reusable."""
    monkeypatch.setattr(kept_reads, "_filesystem_type", lambda _path: "zfs")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    key = "c" * 64
    queue.record_move(key, {
        "tier_id": bench.TIER, "complete": True, "seconds": 1,
        "bytes_staged": 100_000_000, "pool_read_mb_s": 100})
    path = queue.root / record_store.MOVERS / (key + ".json")
    state = after_paths(monkeypatch, path, path.parent)
    real_stat = kept_reads.os.stat
    real_fstat = kept_reads.os.fstat
    real_info = real_stat(path)
    parent_info = real_stat(path.parent)
    tick = max(real_info.st_mtime_ns, real_info.st_ctime_ns,
               parent_info.st_mtime_ns, parent_info.st_ctime_ns)
    state["now"] = tick

    class SameTick:
        def __init__(self, info):
            self.info = info
            self.st_mtime_ns = tick
            self.st_ctime_ns = tick

        def __getattr__(self, name):
            return getattr(self.info, name)

    def stat(pathname, *args, **kwargs):
        info = real_stat(pathname, *args, **kwargs)
        return SameTick(info) if str(pathname) == str(path) else info

    def fstat(descriptor):
        info = real_fstat(descriptor)
        return SameTick(info) if info.st_ino == real_info.st_ino else info

    monkeypatch.setattr(kept_reads.os, "stat", stat)
    monkeypatch.setattr(kept_reads.os, "fstat", fstat)
    cache = tier_cycle.ReceiptCache()
    assert _view(cache, queue)[0][0]["pool_read_mb_s"] == 100
    text = path.read_text()
    path.write_text(text.replace('100', '300'))
    assert _view(cache, queue)[0][0]["pool_read_mb_s"] == 300
    # Now the exact same metadata is behind the fence. It can be kept, but
    # the untrusted earlier version must not be reused during promotion.
    state["now"] = tick + 1
    assert _view(cache, queue)[0][0]["pool_read_mb_s"] == 300
    expected = _view(tier_cycle.ReceiptCache(), queue)
    counts = _thaws(monkeypatch, {key})
    # Public results still need isolation. Only frozen-document thaws are
    # prohibited here (full-cycle defensive-copy churn is tested above).
    materialized = []
    real = record_store._thawed_document

    def thaw(path, document):
        materialized.append(Path(path).name)
        return real(path, document)

    monkeypatch.setattr(record_store, "_thawed_document", thaw)
    assert _view(cache, queue) == expected
    assert not materialized, ("stable later-tick receipt was rethawed", materialized, counts)
