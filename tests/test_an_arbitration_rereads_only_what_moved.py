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
* while the lock is held, nothing of the forest is read either, and what is
  touched of it per hold is at most its fingerprint: one ``lstat`` of the
  root and of each directory and one ``stat`` of each fragment.  Those are
  counted, not timed (the reason is at :data:`FINGERPRINT_OPS`).

A fragment filed into the forest mid-range is still seen by the very next
decision.  Nothing here touches the live queue or a real stage root.
"""

from __future__ import annotations

import builtins
import collections
import contextlib
import hashlib
import io
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

#: What the publisher may do to the forest while it holds the stage
#: ownership lock, per hold: ``lstat`` the root and each directory, ``stat``
#: each fragment -- the census's fingerprint (#761) -- and nothing else.  No
#: listing and no read.  Around it the lock also covers the pin, claim and
#: in-flight censuses a replacement passes (#966) and the rename; those read
#: ``leases/``, ``claimed/`` and the destination's directory, not the forest.
#:
#: This used to be a wall-clock bound on ``ownership_lock_held``: three times
#: the 3.31 s measured at this shape on sparky (action 1e0a9b624335), against
#: 13.23 s before the fix (pbtest shard 7bcd96891b36), which listed the
#: 406-directory forest 810,000 times under the lock.  The seconds measured
#: the box as much as the lock: under full-suite load the same tree held it
#: between 10.97 and 22.2 s with nothing listed under it, and passed alone
#: (#1079, #1094, #1108).  The count is what the seconds stood in for: a
#: listing per name (by ``scandir`` or ``listdir``), a read per name, or a
#: second pass over the forest per name breaks it on any box, and load
#: changes none of it.  What it does not see is work under the lock that
#: touches no file; a bound at three times an idle sample could not tell
#: that from load either.  The seconds are still printed, as the receipt's
#: ``ownership_lock_held``.
FINGERPRINT_OPS = ("lstat", "stat")

#: Directories of the residency root that are not the forest: the pin census
#: a replacement passes reads them per name by design (#966).
_NOT_THE_FOREST = frozenset({"leases", "material"})


def _filesystem_type(path: Path) -> str | None:
    """The type the complete trust resolver names for ``path`` (#1506)."""

    info = os.stat(path)
    return stage_move._object_filesystem_type(
        info, path=path, follow_symlinks=False)


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


class _ForestTouches:
    """What was done to the residency forest, and what of it under the lock.

    ``listings`` is every ``os.scandir`` or ``os.listdir`` of the residency
    root or a namespace in it, with whether the calling thread held the stage
    ownership lock at the time.  ``locked`` counts, by call, what was done to
    the forest -- the root, a namespace, or a file in one -- while the lock
    was held, and ``holds`` counts lock acquisitions. ``events`` records
    listings and trust refusals in call order. Only the stamp and underlying
    version check supply refusals; the current-version wrapper does not
    supply another. Each listing consumes its directory's pending refusal,
    if any. The pin directories, ``leases/`` and ``material/``, are excluded.
    """

    def __init__(self, root: Path) -> None:
        self.prefix = str(root)
        self.inside = self.prefix + os.sep
        self.listings: list[tuple[str, bool]] = []
        self.locked: collections.Counter[str] = collections.Counter()
        self.locked_directories: collections.Counter[str] = collections.Counter()
        self.events: list[tuple[str, str, bool]] = []
        self.holds = 0
        self.holding = threading.local()
        self.refusals = 0

    def under_lock(self) -> bool:
        return getattr(self.holding, "depth", 0) > 0

    def part(self, path: object) -> str | None:
        """``"root"``, ``"namespace"`` or ``"file"`` of the forest, or ``None``.

        String work only: under the lock this runs once per ``stat`` and
        ``lstat``, 1.16 million times in the test below.
        """

        if type(path) is not str:
            if not isinstance(path, os.PathLike):
                return None     # a descriptor: nothing here passes one
            path = os.fspath(path)
            if not isinstance(path, str):
                return None
        if path == self.prefix:
            return "root"
        if not path.startswith(self.inside):
            return None
        head, _sep, rest = path[len(self.inside):].partition(os.sep)
        if not head or head in _NOT_THE_FOREST:
            return None
        if not rest:
            return "namespace"
        return None if os.sep in rest else "file"


def _forest_touches(monkeypatch: pytest.MonkeyPatch, publisher,
                    root: Path) -> _ForestTouches:
    """Record every listing of the forest, and what the lock covers of it."""

    touches = _ForestTouches(root)
    real_lock = publisher.queue.stage_ownership_lock

    @contextlib.contextmanager
    def lock(*args, **kwargs):  # type: ignore[no-untyped-def]
        with real_lock(*args, **kwargs):
            touches.holding.depth = getattr(touches.holding, "depth", 0) + 1
            touches.holds += 1
            try:
                yield
            finally:
                touches.holding.depth -= 1

    def listing(real):  # type: ignore[no-untyped-def]
        def call(path=".", *args, **kwargs):  # type: ignore[no-untyped-def]
            if touches.part(path) in ("root", "namespace"):
                under = touches.under_lock()
                name = os.fspath(path)
                touches.listings.append((name, under))
                touches.events.append(("list", name, under))
                if under:
                    touches.locked["list"] += 1
                    touches.locked_directories[name] += 1
            return real(path, *args, **kwargs)
        return call

    def counted(kind, real):  # type: ignore[no-untyped-def]
        holding, part, locked = touches.holding, touches.part, touches.locked

        def call(path, *args, **kwargs):  # type: ignore[no-untyped-def]
            if (kind == "read" and args
                    and isinstance(args[0], int)
                    and (args[0] & os.O_PATH)):
                return real(path, *args, **kwargs)
            if getattr(holding, "depth", 0):
                where = part(path)
                if where is not None:
                    locked[kind] += 1
                    if where in ("root", "namespace"):
                        touches.locked_directories[os.fspath(path)] += 1
            return real(path, *args, **kwargs)
        return call

    monkeypatch.setattr(publisher.queue, "stage_ownership_lock", lock)
    monkeypatch.setattr(os, "scandir", listing(os.scandir))
    monkeypatch.setattr(os, "listdir", listing(os.listdir))
    monkeypatch.setattr(os, "lstat", counted("lstat", os.lstat))
    monkeypatch.setattr(os, "stat", counted("stat", os.stat))
    monkeypatch.setattr(os, "open", counted("read", os.open))
    monkeypatch.setattr(io, "open", counted("read", io.open))
    monkeypatch.setattr(builtins, "open", counted("read", builtins.open))

    def refused(real):  # type: ignore[no-untyped-def]
        def call(path):  # type: ignore[no-untyped-def]
            answer = real(path)
            if answer is None and touches.part(path) in ("root", "namespace"):
                touches.refusals += 1
                touches.events.append(
                    ("refusal", os.fspath(path), touches.under_lock()))
            return answer
        return call

    monkeypatch.setattr(stage_move, "_trusted_directory_stamp",
                        refused(stage_move._trusted_directory_stamp))
    # _current_directory_version calls this function. Do not wrap both.
    monkeypatch.setattr(stage_move, "_directory_version_at",
                        refused(stage_move._directory_version_at))
    return touches


def _assert_forest_listings(
        events: list[tuple[str, str, bool]], root: str,
        directories: list[str]) -> collections.Counter[str]:
    """Permit one re-list per earlier refusal, for the same directory."""

    expected = {root, *directories}
    seen: set[str] = set()
    pending: dict[str, collections.deque[int]] = collections.defaultdict(
        collections.deque)
    locked_relists: collections.Counter[str] = collections.Counter()
    for index, (kind, name, under_lock) in enumerate(events):
        assert name in expected, (kind, name)
        if kind == "refusal":
            pending[name].append(index)
            continue
        if name in seen or under_lock:
            assert pending[name], (
                f"unjustified forest listing: {name} at event {index}")
            pending[name].popleft()
        if under_lock:
            locked_relists[name] += 1
        seen.add(name)
    assert seen == expected, (seen, expected)
    return locked_relists


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
    touches = _forest_touches(monkeypatch, publisher, root)
    for destination in destinations:
        written, digest, _identity_ = _replace(publisher, destination, copier)
        assert (written, digest) == (SIZE, NEW_DIGEST)
    listings = list(touches.listings)
    locked = collections.Counter(touches.locked)
    events = list(touches.events)
    locked_directories = collections.Counter(touches.locked_directories)
    holds = touches.holds
    refusals = touches.refusals
    assert all(path.read_bytes() == NEW for path in destinations)
    assert len(publisher.invalidated) == NAMES
    assert {(row["consumer_action_key"], row["mover_action_key"], row["state"])
            for entry in publisher.invalidated
            for row in entry["owners"]} == {(consumer, mover, "ended")}
    report = publisher.clock.report()
    assert report["outcomes"] == {"replaced_ended_owner": NAMES}, report
    held = report["thread_seconds"]["ownership_lock_held"]
    print(f"ownership_lock_held {held}")
    directories = [entry.path for entry in os.scandir(root)
                   if entry.is_dir() and entry.name not in _NOT_THE_FOREST]
    fragments = sum(1 for directory in directories
                    for entry in os.scandir(directory) if entry.is_file())
    # The census's per-hold cost on this filesystem: the fingerprint where
    # the mount table names the device directly, plus one verifying
    # ``lstat`` per directory where each trust check re-proves an anonymous
    # device through its descriptor (#1358). No listing, no read, no second
    # pass on either.
    probed = stage_move._filesystem_type(os.stat(root).st_dev) is None
    directory_cost = 2 if probed else 1
    per_hold = (1 + len(directories)) * directory_cost + fragments
    # Consume one earlier refusal for this directory at each re-list.
    # A refused initial stamp prevents retention of the first listing, so
    # that refusal permits its next listing, not another directory's list.
    listed_locked = [name for name, under_lock in listings if under_lock]
    roots = [name for name, _under_lock in listings if name == str(root)]
    print(f"forest listings {len(listings)} (root {len(roots)}, under the "
          f"lock {len(listed_locked)}), directories {len(directories)}, "
          f"fragments {fragments}; under the lock, {holds} holds touched "
          f"the forest {dict(sorted(locked.items()))}, at most "
          f"{per_hold} per hold; trust refusals {refusals}")
    locked_relists = _assert_forest_listings(events, str(root), directories)
    assert held["calls"] >= NAMES, held
    assert holds == held["calls"], (holds, held)
    # A matched re-list adds at most two directory trust checks and one
    # listing. It cannot pay for another directory or another forest pass.
    assert set(locked) - {"list"} <= set(FINGERPRINT_OPS), locked
    assert locked.get("list", 0) == sum(locked_relists.values()), (
        locked, locked_relists)
    for name, count in locked_directories.items():
        bound = (holds * directory_cost
                 + locked_relists[name] * (2 * directory_cost + 1))
        assert count <= bound, (name, count, bound)
    assert sum(locked.values()) <= (
        holds * per_hold
        + sum(locked_relists.values()) * (2 * directory_cost + 1)
    ), (locked, holds, per_hold, locked_relists)


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


@pytest.mark.parametrize("extra", ["unrelated", "reused", "late"])
def test_a_refusal_cannot_pay_for_an_unjustified_listing(
        fleet, monkeypatch: pytest.MonkeyPatch, extra: str) -> None:
    """The regression gate must reject a re-list without its own refusal."""

    queue, _stage, _cas = fleet
    root = queue.root / pool.RESIDENCY
    first, second = root / "first", root / "second"
    first.mkdir()
    second.mkdir()
    publisher = base._publisher(fleet, base._key(), base._key())
    time.sleep(2 * time.clock_getres(5))
    # Keep trust stable except for one root comparison. Exercise the real
    # census, including current -> version, rather than synthetic counters.
    refuse_root = False

    def filesystem(_info, *, path=None, **_kwargs):
        nonlocal refuse_root
        if refuse_root and os.fspath(path) == str(root):
            refuse_root = False
            return None
        return "tmpfs"

    monkeypatch.setattr(stage_move, "_object_filesystem_type", filesystem)
    touches = _forest_touches(monkeypatch, publisher, root)
    publisher._forest_census()
    assert publisher._listed_root is not None
    assert set(publisher._listed_children) == {"first", "second"}
    with publisher.queue.stage_ownership_lock(str(publisher.stage_root)):
        if extra == "late":
            with os.scandir(second) as entries:
                tuple(entries)
        refuse_root = True
        publisher._forest_census()
        if extra != "late":
            target = second if extra == "unrelated" else root
            with os.scandir(target) as entries:
                tuple(entries)
    with pytest.raises(AssertionError, match="unjustified forest listing"):
        _assert_forest_listings(
            touches.events, str(root), [str(first), str(second)])
    assert touches.refusals == 1


@pytest.mark.parametrize("changed", [
    "stable", "root", "namespace", "initial", "both"])
def test_only_the_directory_that_refuses_trust_is_relisted(
        fleet, monkeypatch: pytest.MonkeyPatch, changed: str) -> None:
    """One refusal re-lists one directory; restored trust retains its names."""

    queue, _stage, _cas = fleet
    root = queue.root / pool.RESIDENCY
    first, second = root / "first", root / "second"
    first.mkdir()
    second.mkdir()
    publisher = base._publisher(fleet, base._key(), base._key())
    time.sleep(2 * time.clock_getres(5))
    target = {"stable": None, "root": str(root), "namespace": str(first),
              "initial": str(root), "both": str(root)}[changed]
    remaining = int(changed == "initial")

    def filesystem(_info, *, path=None, **_kwargs):
        nonlocal remaining
        if remaining and os.fspath(path) == target:
            remaining -= 1
            return None
        return "tmpfs"

    monkeypatch.setattr(stage_move, "_object_filesystem_type", filesystem)
    touches = _forest_touches(monkeypatch, publisher, root)
    publisher._forest_census()
    remaining = (2 if changed == "both"
                 else int(changed in {"root", "namespace"}))
    for _ in range(2):
        with publisher.queue.stage_ownership_lock(str(publisher.stage_root)):
            publisher._forest_census()
    expected = collections.Counter({str(root): 1, str(first): 1, str(second): 1})
    if target is not None:
        expected[target] += 2 if changed == "both" else 1
    assert collections.Counter(name for name, _ in touches.listings) == expected
    relists = _assert_forest_listings(
        touches.events, str(root), [str(first), str(second)])
    extra_lists = 2 if changed == "both" else int(target is not None)
    assert relists == (
        collections.Counter() if target is None
        else collections.Counter({target: extra_lists}))
    assert touches.refusals == extra_lists
    extra_checks = {"stable": 0, "root": 2, "namespace": 2,
                    "initial": 1, "both": 2}[changed]
    assert touches.locked == collections.Counter(
        lstat=6 + extra_checks, list=extra_lists)
    assert touches.locked_directories == collections.Counter({
        name: 2 + extra_checks + extra_lists if name == target else 2
        for name in expected})
    assert publisher._listed_root is not None
    assert set(publisher._listed_children) == {"first", "second"}
