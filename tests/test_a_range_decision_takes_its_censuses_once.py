"""A range decision takes its pin, claim and in-flight censuses once (#1028).

A divergent name's re-decision under the stage ownership lock passes three
censuses before it may replace: the live pin census (``_live_pins``), the
live mover claim census (``_live_claim_cover``) and the in-flight-partial
census (``_inflight_partials``), which lists the destination's range
directory.  On main each is taken once per name, so a 2,048-name range
decision lists the range directory 2,048 times -- one listing where one
answers every name of the range -- and each re-decision holds the lock the
movers and egresses of the stage root wait for (~1.7 ms a name at the
campaign's range size, action 1e0a9b624335).

The censuses are now hints the range decision shares: the claim listing
and each range directory's partial names are read once per stamp of their
source directory, and every name revalidates with one directory version
(``_current_directory_version``), re-reading only what moved -- so a
claim or a partial that appears mid-range moves its directory and is seen
by the next name of the same range, exactly as the per-name censuses saw
it.  What is pinned here: an unchanged directory is listed once for a
whole 2,048-name range decision (the adoption pass over an already-
correct campaign range, which writes nothing); a partial and a claim that
appear mid-range are seen by the next name; a mixed range decides each
name exactly as the per-name censuses answered it; and the
``ownership_lock_held`` accounting the receipt publishes is unchanged.

Nothing here measures seconds: a lock-hold claim needs mover receipts and
a py-spy under the hold.  What is counted is the listings.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
import test_stale_material_done_owner_retires as stale  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

#: The campaign's range size, the shape the issue prices (#1028).
NAMES = 2048
SIZE = 64
OLD = b"o" * SIZE
NEW = b"n" * SIZE
OLD_DIGEST = hashlib.sha256(OLD).hexdigest()
NEW_DIGEST = hashlib.sha256(NEW).hexdigest()
MOUNT_PREFIX = "/mnt/shared"


def _identity(path: Path) -> dict[str, int]:
    info = os.stat(path)
    return {"ino": int(info.st_ino), "size": int(info.st_size),
            "mtime_ns": int(info.st_mtime_ns),
            "ctime_ns": int(info.st_ctime_ns)}


def _campaign_range(stage: Path, count: int) -> list[Path]:
    """One campaign range: every ``<offset>-<size>`` name of one shard."""

    directory = stage / "shard.bin.pbrange"
    return [directory / f"{index * SIZE}-{SIZE}" for index in range(count)]


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
        path.parent.mkdir(parents=True, exist_ok=True)
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


def _terminal_pair(queue: pool.PoolQueue, stage: Path, destination: Path,
                   ) -> tuple[str, str]:
    """A FAILED consumer whose DONE mover vouches OLD bytes at one name.

    The pin's cover: a fragment and a dated sidecar a reader lease can pin,
    whose owner is provably ended, so the name's arbitration reaches the
    pin census instead of stopping at an unproven owner.
    """

    consumer, _generation = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    queue.finish(mover, status="executed", detail={"returncode": 0})
    key = residency_map.residency_map_key(str(destination), 0)
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": base.TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {key: {"stage_path": str(destination), "bytes": SIZE,
                          "sha256": OLD_DIGEST, "offset": 0}}})
    stale._write_sidecar(queue, stage, consumer, mover, {
        key: {"stage_path": str(destination), "bytes": SIZE,
              "sha256": OLD_DIGEST, "file_id": _identity(destination)}})
    return consumer, mover


def _claim_range(queue: pool.PoolQueue, cas: Path, key: str,
                 entries: list[dict[str, object]], start: int, end: int,
                 ) -> None:
    """Seal one claimed range mover: request + manifest blob + claim row.

    Shaped the way ``_claim_range`` in
    ``test_the_claim_cover_check_reuses_a_claims_derived_paths.py`` seals
    one: the request names the range through ``--range-start-bytes`` and
    ``--range-end-bytes`` and the manifest through
    ``PBCAMPAIGN_DATA_MANIFEST_INPUT_ID``, both read fresh by
    ``_claimed_paths``.
    """

    manifest = {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {},
        "mount_prefix": MOUNT_PREFIX,
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(int(entry["bytes"]) for entry in entries),
    }
    blob = json.dumps(manifest).encode("utf-8")
    digest = hashlib.sha256(blob).hexdigest()
    shard = cas / "blobs" / digest[:2]
    shard.mkdir(parents=True, exist_ok=True)
    (shard / digest).write_bytes(blob)
    request = {
        "action_key": key,
        "params": {"command": ["python3", "stage_move.py",
                               "--range-start-bytes", str(start),
                               "--range-end-bytes", str(end)]},
        "inputs": [{"id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                    "sha256": digest, "bytes": len(blob)}],
    }
    shard = cas / "requests" / key[:2]
    shard.mkdir(parents=True, exist_ok=True)
    (shard / f"{key}.json").write_text(json.dumps(request))
    claimed_dir = queue.dir(pool.CLAIMED)
    claimed_dir.mkdir(parents=True, exist_ok=True)
    record = {
        "action_key": key,
        "resources": {"cpu": 2, "mem_gb": 1, f"stage_gib@{base.TIER}": 1},
        "cas_root": str(cas),
    }
    (claimed_dir / f"{key}.json").write_text(json.dumps(record))


class _RangeListings:
    """How often the range directory was listed, and how often the lock."""

    def __init__(self, directory: Path) -> None:
        self.target = os.fspath(directory)
        self.listings = 0


def _count_range_listings(monkeypatch: pytest.MonkeyPatch,
                          directory: Path) -> _RangeListings:
    """Count every ``scandir``/``listdir`` of the range directory."""

    counted = _RangeListings(directory)

    def listing(real):  # type: ignore[no-untyped-def]
        def call(path=".", *args, **kwargs):  # type: ignore[no-untyped-def]
            try:
                spelled = os.fspath(path)
            except TypeError:
                spelled = None
            if spelled == counted.target:
                counted.listings += 1
            return real(path, *args, **kwargs)
        return call

    monkeypatch.setattr(os, "scandir", listing(os.scandir))
    monkeypatch.setattr(os, "listdir", listing(os.listdir))
    return counted


def _count_lock_holds(monkeypatch: pytest.MonkeyPatch, publisher,
                      ) -> list[int]:
    """One entry per stage ownership lock hold the publisher takes."""

    holds: list[int] = []
    real_lock = publisher.queue.stage_ownership_lock

    @contextlib.contextmanager
    def lock(*args, **kwargs):  # type: ignore[no-untyped-def]
        with real_lock(*args, **kwargs):
            holds.append(1)
            yield

    monkeypatch.setattr(publisher.queue, "stage_ownership_lock", lock)
    return holds


def _replace(publisher, destination: Path, copier: str):
    temp = destination.parent / f".{destination.name}.{copier[:16]}.partial"
    temp.write_bytes(NEW)
    return publisher.publish({"bytes": SIZE, "sha256": NEW_DIGEST},
                             destination, temp, NEW_DIGEST)


def _adoption_world(fleet, count: int):
    """A campaign range of already-correct names nothing vouches for.

    The #1081 shape -- a promotion killed before it filed its records
    leaves correct copies that nothing names -- so the range decision is
    one adoption pass: ``try_adopt`` per name, which takes the gate's
    three censuses and its lock per name and writes nothing, so the range
    directory holds still across the whole decision unless the test moves
    it.
    """

    queue, stage, cas = fleet
    destinations = _campaign_range(stage, count)
    destinations[0].parent.mkdir(parents=True, exist_ok=True)
    for path in destinations:
        path.write_bytes(NEW)
    # A quiet pool's claimed/: present and empty, so the claim census
    # reads an empty listing rather than failing one closed.
    queue.dir(pool.CLAIMED).mkdir(parents=True, exist_ok=True)
    successor, copier = base._key(), base._key()
    publisher = base._publisher(fleet, copier, successor)
    return publisher, destinations


def test_a_2048_name_range_decision_lists_the_range_directory_once(
        fleet, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    publisher, destinations = _adoption_world(fleet, NAMES)
    counted = _count_range_listings(monkeypatch, destinations[0].parent)
    holds = _count_lock_holds(monkeypatch, publisher)
    entry = {"bytes": SIZE, "sha256": NEW_DIGEST}
    for path in destinations:
        adopted = publisher.try_adopt(entry, path)
        assert adopted is not None and adopted[1] == NEW_DIGEST, path
    assert all(path.read_bytes() == NEW for path in destinations)
    report = publisher.clock.report()
    assert report["outcomes"] == {"adopted_by_content": NAMES}, report
    held = report["thread_seconds"]["ownership_lock_held"]
    print(f"ownership_lock_held {held}")
    print(f"range directory listed {counted.listings} times "
          f"across {len(holds)} holds")
    # The ask (#1028): one listing of the range directory answers every
    # name's in-flight census while the directory holds still.  Main lists
    # it once per name, 2,048 times (measured on the divergent shape,
    # action 06b849a8de6d: every name's gate reaches the same listing).
    assert counted.listings == 1, counted.listings
    # The lock accounting is what it always was: one hold per name's act,
    # every one of them counted by the receipt's clock.
    assert held["calls"] >= NAMES, held
    assert len(holds) == held["calls"], (len(holds), held)


def test_a_partial_that_appears_mid_range_is_seen_by_the_next_name(
        fleet, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    """The hint is revalidated: the partial moves the directory, and the
    very next name of the same range re-lists and defers to it."""

    publisher, destinations = _adoption_world(fleet, 3)
    entry = {"bytes": SIZE, "sha256": NEW_DIGEST}
    assert publisher.try_adopt(entry, destinations[0]) is not None

    sibling = base._key()
    in_flight = destinations[1]
    partial = in_flight.parent / f".{in_flight.name}.{sibling[:16]}.partial"
    partial.write_bytes(b"half a copy")

    assert publisher.try_adopt(entry, in_flight) is None, (
        "a copy in flight that appeared after the census must still be "
        "seen by the next name of the same range")
    # The partial names another entry's destination only.
    assert publisher.try_adopt(entry, destinations[2]) is not None
    assert in_flight.read_bytes() == NEW, (
        "the blocked name's bytes are never replaced")


def test_a_claim_that_appears_mid_range_is_seen_by_the_next_name(
        fleet, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    """A claim sealed after the census moves ``claimed/``; the next name
    re-reads it and defers, and the name after that is free again."""

    queue, stage, cas = fleet
    publisher, destinations = _adoption_world(fleet, 3)
    entry = {"bytes": SIZE, "sha256": NEW_DIGEST}
    assert publisher.try_adopt(entry, destinations[0]) is not None

    claim_key = base._key()
    _claim_range(queue, cas, claim_key,
                 [{"path": f"{MOUNT_PREFIX}/shard.bin",
                   "offset": index * SIZE, "bytes": SIZE,
                   "sha256": NEW_DIGEST} for index in range(3)],
                 start=SIZE, end=2 * SIZE)

    assert publisher.try_adopt(entry, destinations[1]) is None, (
        "a live claim that appeared after the census must still be seen "
        "by the next name of the same range")
    assert publisher.try_adopt(entry, destinations[2]) is not None
    assert destinations[1].read_bytes() == NEW, (
        "the covered name's bytes are never replaced")


def test_a_mixed_range_decides_each_name_exactly_as_per_name_censuses_did(
        fleet, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    """Pinned, claimed, in-flight and free names keep their own answers.

    The hint is built at the first decision; the pin, the claim and the
    sibling partial all predate it, so every name's decision must be the
    one the per-name censuses answered: the free names replace, and the
    other three refuse naming what blocks them.
    """

    queue, stage, cas = fleet
    destinations = _campaign_range(stage, 5)
    consumer, mover = _dead_owner(fleet, destinations)
    successor, copier = base._key(), base._key()
    publisher = base._publisher(fleet, copier, successor)

    # names[1]: live-pinned -- a reader lease refs the name's vouch.
    pinned = destinations[1]
    donor_consumer, donor_mover = _terminal_pair(queue, stage, pinned)
    acquired = reader_lease.acquire(
        queue, consumer_action_key=donor_consumer,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=base.TIER,
        epoch="", span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": "fixture", "pid": os.getpid()}, acquire_token="p1",
        covers=[{"mover_action_key": donor_mover,
                 "manifest_sha256": "a" * 64}])
    assert acquired.get("ok"), acquired

    # names[2]: covered by a live mover claim's sealed range.
    claim_key = base._key()
    _claim_range(queue, cas, claim_key,
                 [{"path": f"{MOUNT_PREFIX}/shard.bin",
                   "offset": index * SIZE, "bytes": SIZE,
                   "sha256": OLD_DIGEST} for index in range(5)],
                 start=2 * SIZE, end=3 * SIZE)

    # names[3]: a sibling copy in flight, its partial in the range
    # directory under the owner-keyed convention.
    sibling = base._key()
    in_flight = destinations[3]
    partial = in_flight.parent / f".{in_flight.name}.{sibling[:16]}.partial"
    partial.write_bytes(b"half a copy")

    free = destinations[0]
    written, digest, _identity_ = _replace(publisher, free, copier)
    assert (written, digest) == (SIZE, NEW_DIGEST)

    with pytest.raises(stage_move._PublicationRefused) as refused:
        _replace(publisher, pinned, copier)
    assert "every owner has ended, but it is live-pinned by" in \
        str(refused.value), str(refused.value)

    with pytest.raises(stage_move._PublicationRefused) as refused:
        _replace(publisher, destinations[2], copier)
    assert "every owner has ended, but another live mover claim covers it" \
        in str(refused.value), str(refused.value)

    with pytest.raises(stage_move._PublicationRefused) as refused:
        _replace(publisher, in_flight, copier)
    assert "every owner has ended, but a copy is in flight" in \
        str(refused.value), str(refused.value)

    tail = destinations[4]
    written, digest, _identity_ = _replace(publisher, tail, copier)
    assert (written, digest) == (SIZE, NEW_DIGEST)

    assert free.read_bytes() == NEW and tail.read_bytes() == NEW
    assert all(path.read_bytes() == OLD
               for path in (pinned, destinations[2], in_flight))
    assert len(publisher.invalidated) == 2
    live, tainted = reader_lease.live_for(
        queue, {os.path.normpath(str(pinned))},
        residency_root=queue.residency_fragment_root())
    assert live and not tainted, (live, tainted)
