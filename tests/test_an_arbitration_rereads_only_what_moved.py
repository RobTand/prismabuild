"""A divergent name's re-decision under the stage lock lists nothing (#1004).

#966 settles a staged name whose recorded owner holds different bytes by the
owners' states: the owners are judged under their transition locks, and then,
under the stage ownership lock, the name is decided again (``_arbitration`` ->
``redo`` -> ``_proof_search``) against a census of the residency forest that
began after the judgment.  That census listed the whole forest, once per
divergent name, under the one lock every mover and egress on the stage root
waits for: 38-66 ms for 429 namespaces on dl380g10, so a range in which every
name diverges from one dead owner -- 2,048 entries -- held the lock for about
100 s, which is #981's serialization again.

The census now keeps each directory's fragment names under the stamp taken
before its listing (``_StagedPublisher._forest_census``, the #992 index), and
a later census compares the stamps rather than listing: one ``lstat`` per
directory and one ``stat`` per fragment, the fingerprint of the judgment's
census.  Here 2,000 names diverge from one
dead owner (a FAILED consumer, its DONE mover) in a forest of 400 other
namespaces, and one successor publisher replaces them all:

* every name is replaced, and every owner judgement names the dead owner;
* the forest is listed once for the whole range, not once per name, and
  nothing of it is listed while the stage ownership lock is held;
* the ``ownership_lock_held`` the publisher records stays under
  ``LOCK_HELD_BOUND_S`` (the derivation is at the constant).

A fragment filed into the forest mid-range is still seen by the very next
decision.  Nothing here touches the live queue or a real stage root.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
from pathlib import Path
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
import test_stale_material_done_owner_retires as stale  # noqa: E402
from prismabuild import pool, residency_map  # noqa: E402
import bench_stage_adopt  # noqa: E402
import stage_move  # noqa: E402

NAMES = 2000
SIZE = 64
OLD = b"o" * SIZE
NEW = b"n" * SIZE
OLD_DIGEST = hashlib.sha256(OLD).hexdigest()
NEW_DIGEST = hashlib.sha256(NEW).hexdigest()

#: Total seconds the publisher may hold the stage ownership lock across all
#: 2,000 replacements.  Derived from the after-fix hold measured at exactly
#: this shape on sparky (GB10) through pbrun: MEASURED_LOCK_HELD_S (action
#: 1e0a9b624335), against BASE_LOCK_HELD_S with this test on the tree before
#: the fix (pbtest shard 7bcd96891b36), where every re-decision listed the
#: 406-directory forest under the lock, 810,000 listings in all.  Both ran on
#: a Cortex-X925 core.  What remains under the
#: lock per name is one ``lstat`` per forest directory and one ``stat`` per
#: fragment (#761), the pin, claim and in-flight censuses a replacement
#: passes (#966), and the rename.  The bound is three times the measured
#: hold, so a loaded box does not fail it, and below the base hold.  What
#: tells the two trees apart is the listing count under the lock below; the
#: seconds bound is there so a listing per name, or anything else that grows
#: with the forest under the lock, cannot come back unnoticed.
MEASURED_LOCK_HELD_S = 3.31
BASE_LOCK_HELD_S = 13.23
LOCK_HELD_BOUND_S = 3 * MEASURED_LOCK_HELD_S


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


def _identity(path: Path) -> dict[str, int]:
    info = os.stat(path)
    return {"ino": int(info.st_ino), "size": int(info.st_size),
            "mtime_ns": int(info.st_mtime_ns),
            "ctime_ns": int(info.st_ctime_ns)}


def _dead_owner(fleet, destinations: list[Path]) -> tuple[str, str]:
    """A FAILED consumer whose DONE mover holds OLD bytes at every name.

    The #966 incident's owner: a complete receipt, a fragment, a sidecar
    that dates the current inode of every name, and its tier charge.
    """

    queue, stage, _cas = fleet
    consumer, _generation = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    entries = {}
    named = {}
    for path in destinations:
        path.write_bytes(OLD)
        key = residency_map.residency_map_key(str(path), 0)
        named[key] = {"stage_path": str(path), "bytes": SIZE,
                      "sha256": OLD_DIGEST, "offset": 0}
        entries[key] = {"stage_path": str(path), "bytes": SIZE,
                        "sha256": OLD_DIGEST, "file_id": _identity(path)}
    stale._write_sidecar(queue, stage, consumer, mover, entries)
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": base.TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64, "entries": named})
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": base.TIER,
        "stage_root": str(stage), "manifest_sha256": "a" * 64,
        "complete": True, "entries_declared": len(destinations),
        "entries_staged": len(destinations),
        "bytes_staged": len(destinations) * SIZE,
        "range_bytes": len(destinations) * SIZE, "range_start_bytes": 0,
        "range_end_bytes": len(destinations) * SIZE, "errors": []})
    queue.finish(mover, status="executed", detail={"returncode": 0})
    stale._charge(queue, mover)
    return consumer, mover


def _forest_listings(monkeypatch: pytest.MonkeyPatch, publisher,
                     root: Path) -> list[tuple[str, bool]]:
    """Every ``os.scandir`` of the residency root or a namespace in it.

    Each call is recorded with whether the calling thread held the stage
    ownership lock at the time.  Not ``leases/`` or ``material/``: the pin
    census a replacement passes reads those per name by design (#966), and
    they are not the forest.
    """

    calls: list[tuple[str, bool]] = []
    holding = threading.local()
    real_scandir = os.scandir
    real_lock = publisher.queue.stage_ownership_lock
    prefix = str(root)
    skipped = {os.path.join(prefix, name) for name in ("leases", "material")}

    @contextlib.contextmanager
    def lock(*args, **kwargs):  # type: ignore[no-untyped-def]
        with real_lock(*args, **kwargs):
            holding.depth = getattr(holding, "depth", 0) + 1
            try:
                yield
            finally:
                holding.depth -= 1

    def scandir(path=".", *args, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(path, (str, os.PathLike)):
            name = str(path)
            if name == prefix or (os.path.dirname(name) == prefix
                                  and name not in skipped):
                calls.append((name, getattr(holding, "depth", 0) > 0))
        return real_scandir(path, *args, **kwargs)

    monkeypatch.setattr(publisher.queue, "stage_ownership_lock", lock)
    monkeypatch.setattr(os, "scandir", scandir)
    return calls


@pytest.fixture()
def world(fleet, monkeypatch: pytest.MonkeyPatch):  # noqa: F811
    queue, stage, _cas = fleet
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", stale.GRACE)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)
    counts = bench_stage_adopt.build_forest(
        queue, stage, empty_dirs=370, small_dirs=30, big_fragments=[],
        produced_fragment_dirs=0)
    assert counts["empty_dirs"] + 30 >= 400
    names = stage / "range"
    names.mkdir()
    destinations = [names / f"part-{index:05d}.bin" for index in range(NAMES)]
    consumer, mover = _dead_owner(fleet, destinations)
    successor, copier = base._key(), base._key()
    publisher = base._publisher(fleet, copier, successor)
    # The fixture must be where the index is allowed to skip a listing, or
    # the test below measures the fallback instead of the index.
    assert _filesystem_type(queue.root) in LOCAL_CLOCK, (
        _filesystem_type(queue.root))
    # A listing is kept only under a stamp older than the clock the proof
    # reads (CLOCK_REALTIME_COARSE), so a directory the fixture wrote in the
    # current tick would be listed once more before it is kept.  Two ticks
    # of that clock make every fixture write provably older than the first
    # census, and the counts below exact.
    time.sleep(2 * time.clock_getres(5))
    return publisher, destinations, consumer, mover, copier


def _replace(publisher, destination: Path, copier: str):
    temp = destination.parent / f".{destination.name}.{copier[:16]}.partial"
    temp.write_bytes(NEW)
    return publisher.publish({"bytes": SIZE, "sha256": NEW_DIGEST},
                             destination, temp, NEW_DIGEST)


def test_2000_names_of_one_dead_owner_list_the_forest_once(
        world, monkeypatch: pytest.MonkeyPatch) -> None:
    publisher, destinations, consumer, mover, copier = world
    root = publisher.residency_root
    listings = _forest_listings(monkeypatch, publisher, root)
    for destination in destinations:
        written, digest, _identity_ = _replace(publisher, destination, copier)
        assert (written, digest) == (SIZE, NEW_DIGEST)
    assert all(path.read_bytes() == NEW for path in destinations)
    assert len(publisher.invalidated) == NAMES
    assert {(row["consumer_action_key"], row["mover_action_key"], row["state"])
            for entry in publisher.invalidated
            for row in entry["owners"]} == {(consumer, mover, "ended")}
    report = publisher.clock.report()
    assert report["outcomes"] == {"replaced_ended_owner": NAMES}, report
    held = report["thread_seconds"]["ownership_lock_held"]
    print(f"ownership_lock_held {held}")
    # The forest did not change during the range, so the first census listed
    # it and every later one, locked or not, compared stamps: nothing of the
    # forest was listed under the lock.
    locked = [name for name, under_lock in listings if under_lock]
    roots = [name for name, _under_lock in listings if name == str(root)]
    namespaces = [entry.name for entry in os.scandir(root) if entry.is_dir()]
    print(f"forest listings {len(listings)} (root {len(roots)}, under the "
          f"lock {len(locked)}), namespaces {len(namespaces)}")
    assert locked == [], (len(locked), locked[:3])
    assert len(roots) == 1, len(roots)
    assert len(listings) <= 1 + len(namespaces), (
        len(listings), len(namespaces))
    assert held["calls"] >= NAMES, held
    assert held["seconds"] < LOCK_HELD_BOUND_S, held


def test_a_fragment_filed_mid_range_is_seen_by_the_next_decision(
        world) -> None:
    """The stamp comparison is a fingerprint, not a snapshot.

    A live owner files a fragment naming the next divergent name after the
    publisher has replaced the ones before it: the very next decision reads
    it, finds a live owner, and refuses terminally -- nothing replaced.
    """

    publisher, destinations, _consumer, _mover, copier = world
    queue = publisher.queue
    for destination in destinations[:10]:
        _replace(publisher, destination, copier)
    live_consumer = base._key()
    base._publish(queue, live_consumer, max_attempts=1)   # claimed: live
    late_mover = base._key()
    target = destinations[10]
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": live_consumer, "mover_action_key": late_mover,
        "tier_id": base.TIER, "stage_root": str(publisher.stage_root),
        "manifest_sha256": "a" * 64,
        "entries": {residency_map.residency_map_key(str(target), 0): {
            "stage_path": str(target), "bytes": SIZE, "sha256": OLD_DIGEST,
            "offset": 0}}})
    stale._write_sidecar(queue, publisher.stage_root, live_consumer,
                         late_mover, {residency_map.residency_map_key(
                             str(target), 0): {
                             "stage_path": str(target), "bytes": SIZE,
                             "sha256": OLD_DIGEST,
                             "file_id": _identity(target)}})
    with pytest.raises(stage_move._PublicationRefused) as refused:
        _replace(publisher, target, copier)
    assert live_consumer[:12] in str(refused.value), str(refused.value)
    assert target.read_bytes() == OLD
