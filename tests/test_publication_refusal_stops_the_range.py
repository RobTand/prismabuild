"""A refused staged publication stops consuming the range's entries (#853).

The live shape (full512 Stage A R6, 2026-09-22): one shared staged name was
vouched for by a FAILED consumer's DONE mover whose material dated a
superseded incarnation, so the publication gate refused it -- correctly, it
will not adopt an old-incarnation proof and will not overwrite a live or
unknown publication.  That refusal says nothing about the other names: they
may be perfectly publishable.  It does make the range incomplete, though, and
the copier kept consuming the queue after it: each later entry paid a full
payload copy and then one whole 30 s grace before its own refusal, so sixteen
sleeping workers spent the head's window and buried the first obstruction
under capped entry errors.

The policy these tests hold is narrow: a gate refusal ends dispatch for the
run -- no entry beyond the already-dispatched group is handed out -- bounding
the wasted copies and grace waits and keeping the first obstruction visible,
while everything already committed keeps its fragment, sidecar and bytes for
the retry.  The remaining entries are not judged unpublishable; they are
simply left to the next run.

These tests drive the real ``stage_move.move`` with real tiny files and a real
synthetic queue/stage, and assert the bound with deterministic work counters
and barriers, never a wall-clock threshold:

* one worker consumes exactly the refusing entry and stops;
* a four-worker barrier puts four entries really in flight; only that prefix
  is dispatched, no duplicates, and the one valid in-flight entry keeps its
  committed bytes and proof while its peers refuse;
* everything already committed keeps its fragment, sidecar and bytes;
* a live pin and unknown ownership keep their exact refusal and nothing is
  replaced;
* an ordinary source/copy failure still records one entry error and continues,
  because one unreadable source is not evidence about the rest of the range;
* the caller's ``stop`` event is never set by a refusal.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402
import prismabuild.core as pb  # noqa: E402
from prismabuild import reader_lease, residency_map  # noqa: E402
import stage_move  # noqa: E402

SIZE = 4096
TIER = base.TIER
NAMES = [f"entry-{index:02d}.bin" for index in range(12)]


def _payload(seed: int) -> bytes:
    return bytes((seed * 37 + index) % 251 for index in range(SIZE))


def _identity(path: Path) -> dict[str, int]:
    info = os.stat(path)
    return {"ino": int(info.st_ino), "size": int(info.st_size),
            "mtime_ns": int(info.st_mtime_ns),
            "ctime_ns": int(info.st_ctime_ns)}


def _manifest(tmp_path: Path, names, payloads, *, absent=()):
    """One real manifest with real sources; ``absent`` names are not written."""

    mount = tmp_path / "sources"
    mount.mkdir(exist_ok=True)
    entries = []
    for name in names:
        payload = payloads[name]
        source = mount / name
        if name not in absent:
            source.write_bytes(payload)
        entries.append({"path": str(source), "offset": 0, "bytes": SIZE,
                        "sha256": hashlib.sha256(payload).hexdigest()})
    body = {"schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {},
            "annotations": {}, "mount_prefix": str(mount),
            "entries": entries, "entry_count": len(entries),
            "total_bytes": len(entries) * SIZE}
    raw = pb._canonical_file_bytes(pb.validate_data_manifest(body))
    path = tmp_path / "manifest.json"
    path.write_bytes(raw)
    return mount, path, hashlib.sha256(raw).hexdigest(), entries


def _destination(stage: Path, mount: Path, entry) -> Path:
    relative = stage_move.stage_relative(
        str(entry["path"]), 0, SIZE, mount_prefix=str(mount), whole_file=True)
    return stage / relative


def _args(queue, stage, cas, mover, consumer, manifest, digest, entries,
          workers: int):
    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root), "--cas-root", str(cas),
        "--action-key", mover, "--consumer-action-key", consumer,
        "--tier-id", TIER, "--stage-root", str(stage),
        "--manifest", str(manifest), "--manifest-sha256", digest,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(len(entries) * SIZE),
        "--residency-root", str(queue.residency_fragment_root()),
        "--readers", str(workers), "--max-readers", str(workers),
        "--unpaced"])


def _write_owner(queue, stage, consumer, mover, path, digest, *, key=None):
    """One coherent fragment+sidecar pair dating the file that is there.

    ``key`` lets the caller name an entry by the manifest's own key (the
    successor's fragment keys by the origin path), while the mention's
    ``stage_path`` stays the destination the proof must stat.
    """

    key = key or residency_map.residency_map_key(str(path), 0)
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {key: {"stage_path": str(path), "bytes": SIZE,
                          "sha256": digest, "offset": 0}}})
    reader_lease.write_material(
        queue.residency_fragment_root(), consumer_action_key=consumer,
        mover_action_key=mover, tier_id=TIER, stage_root=str(stage),
        manifest_sha256="a" * 64, generation="a" * 32,
        entries={key: {"stage_path": str(path), "bytes": SIZE,
                       "sha256": digest, "file_id": _identity(path)}})
    return key


def _spy_consumed(monkeypatch):
    """The entries the copier actually consumed, through the real call."""

    real = stage_move._Copier._copy_one
    consumed: list[str] = []
    lock = threading.Lock()

    def spying(self, entry, destination, admission, stop,
               source=None, source_offset=None):
        with lock:
            consumed.append(str(entry["path"]))
        return real(self, entry, destination, admission, stop,
                    source=source, source_offset=source_offset)

    monkeypatch.setattr(stage_move._Copier, "_copy_one", spying)
    return consumed


def _short_grace(monkeypatch, grace: float = 0.2):
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", grace)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)


def test_one_refusal_stops_the_single_worker_range(fleet, tmp_path, monkeypatch):
    """The incident refusal: one consumed entry, one error, no withdrawal."""

    queue, stage, cas = fleet
    names = NAMES[:3]
    payloads = {name: _payload(index) for index, name in enumerate(names)}
    mount, manifest, digest, entries = _manifest(tmp_path, names, payloads)
    blocked = _destination(stage, mount, entries[0])
    blocked.parent.mkdir(parents=True, exist_ok=True)
    old = b"o" * SIZE
    blocked.write_bytes(old)
    consumer, mover = base._key(), base._key()
    _write_owner(queue, stage, consumer, mover, blocked,
                 hashlib.sha256(old).hexdigest())
    replacement = blocked.with_name(blocked.name + ".later")
    replacement.write_bytes(payloads[names[0]])
    os.replace(replacement, blocked)
    _short_grace(monkeypatch)
    consumed = _spy_consumed(monkeypatch)
    stop = threading.Event()
    mover_key, successor = base._key(), base._key()
    args = _args(queue, stage, cas, mover_key, successor, manifest, digest,
                 entries, workers=1)

    result = stage_move.move(args, stop=stop)

    assert consumed == [entries[0]["path"]], (
        f"one refused publication must consume exactly its own entry: "
        f"{consumed}")
    assert result["complete"] is False and result["entries_staged"] == 0
    assert len(result["errors"]) == 1, result["errors"]
    assert "shared staged name is published elsewhere" in result["errors"][0]
    assert "withdrawn" not in str(result.get("refusal", ""))
    assert stop.is_set() is False, "a refusal is not the caller's cancellation"
    assert not any(_destination(stage, mount, entry).exists()
                   for entry in entries[1:]), (
        "entries after the refusal must not have been copied")


def test_a_wider_group_dispatches_only_its_in_flight_prefix(
        fleet, tmp_path, monkeypatch):
    """A barrier puts four entries really in flight; only those are dispatched.

    Three of the four refuse (unknown ownership: a non-regular destination)
    and one is valid, so the committed entry's bytes and proof are asserted
    beside its peers' refusals.  The valid entry is held until a peer's
    refusal is recorded, so a fast success cannot race into a fifth entry and
    the assertion is about the dispatched prefix, never queue timing.
    """

    queue, stage, cas = fleet
    names = NAMES[:12]
    payloads = {name: _payload(index) for index, name in enumerate(names)}
    mount, manifest, digest, entries = _manifest(tmp_path, names, payloads)
    valid = entries[2]
    for entry in (entries[0], entries[1], entries[3]):
        _destination(stage, mount, entry).mkdir(parents=True, exist_ok=True)
    first_four = {entry["path"] for entry in entries[:4]}
    barrier = threading.Barrier(4)
    real = stage_move._Copier._copy_one
    consumed: list[str] = []
    lock = threading.Lock()

    def spying(self, entry, destination, admission, stop,
               source=None, source_offset=None):
        path = str(entry["path"])
        with lock:
            consumed.append(path)
        if path in first_four:
            try:
                barrier.wait(timeout=15)
            except threading.BrokenBarrierError as exc:
                raise OSError(f"fixture: four entries never got in flight: "
                              f"{exc}") from exc
        if path == valid["path"]:
            if not self.publication_refused.wait(15):
                raise OSError(
                    "fixture: no peer refusal was recorded while four "
                    "entries were in flight")
        return real(self, entry, destination, admission, stop,
                    source=source, source_offset=source_offset)

    monkeypatch.setattr(stage_move._Copier, "_copy_one", spying)
    stop = threading.Event()
    mover_key, successor = base._key(), base._key()
    args = _args(queue, stage, cas, mover_key, successor, manifest, digest,
                 entries, workers=4)

    result = stage_move.move(args, stop=stop)

    assert len(consumed) == 4 and set(consumed) == first_four, (
        f"the range dispatched beyond its in-flight prefix: {consumed}")
    assert len(set(consumed)) == len(consumed), "an entry was dispatched twice"
    assert result["entries_staged"] == 1 and result["bytes_staged"] == SIZE
    assert len(result["errors"]) == 3, result["errors"]
    staged = _destination(stage, mount, valid)
    assert staged.exists() and staged.read_bytes() == payloads[names[2]], (
        "the valid in-flight entry must keep its committed bytes")
    composed = residency_map.compose(residency_map.read_fragments(
        queue.residency_fragment_root(), successor))["entries"]
    assert residency_map.residency_map_key(valid["path"], 0) in composed, (
        "the valid in-flight entry must keep its proof")
    assert not any(_destination(stage, mount, entry).exists()
                   for entry in entries[4:]), (
        "no entry beyond the dispatched prefix may be consumed")
    assert stop.is_set() is False


def test_a_committed_entry_survives_and_later_entries_are_not_consumed(
        fleet, tmp_path, monkeypatch):
    """Adoption still commits; the refusal stops the very next entry."""

    queue, stage, cas = fleet
    names = NAMES[:3]
    payloads = {name: _payload(index) for index, name in enumerate(names)}
    mount, manifest, digest, entries = _manifest(tmp_path, names, payloads)
    adopted = _destination(stage, mount, entries[0])
    adopted.parent.mkdir(parents=True, exist_ok=True)
    adopted.write_bytes(payloads[names[0]])
    donor_consumer, donor_mover = base._key(), base._key()
    adopted_key = residency_map.residency_map_key(entries[0]["path"], 0)
    _write_owner(
        queue, stage, donor_consumer, donor_mover, adopted,
        hashlib.sha256(payloads[names[0]]).hexdigest(), key=adopted_key)
    adopted_identity = _identity(adopted)
    blocked = _destination(stage, mount, entries[1])
    blocked.mkdir(parents=True, exist_ok=True)
    consumed = _spy_consumed(monkeypatch)
    stop = threading.Event()
    mover_key, successor = base._key(), base._key()
    args = _args(queue, stage, cas, mover_key, successor, manifest, digest,
                 entries, workers=1)

    result = stage_move.move(args, stop=stop)

    assert consumed == [entries[0]["path"], entries[1]["path"]], (
        f"the adopted entry and the refusing entry are the whole consumption: "
        f"{consumed}")
    assert result["entries_staged"] == 1 and result["bytes_staged"] == SIZE
    assert result["complete"] is False and len(result["errors"]) == 1
    assert _identity(adopted) == adopted_identity, "adoption replaced bytes"
    # The committed entry keeps its fragment and its dated sidecar.
    fragments = residency_map.read_fragments(
        queue.residency_fragment_root(), successor)
    staged = residency_map.compose(fragments)["entries"]
    assert adopted_key in staged, staged
    sidecar = reader_lease.read_material(
        queue.residency_fragment_root(), successor, mover_key)
    assert isinstance(sidecar, dict) and adopted_key in sidecar["entries"]
    assert not _destination(stage, mount, entries[2]).exists(), (
        "the third entry must never be consumed after the refusal")
    assert stop.is_set() is False


def test_an_ordinary_copy_failure_does_not_stop_the_range(
        fleet, tmp_path, monkeypatch):
    """One unreadable source is a per-entry error, not a range refusal."""

    queue, stage, cas = fleet
    names = NAMES[:3]
    payloads = {name: _payload(index) for index, name in enumerate(names)}
    mount, manifest, digest, entries = _manifest(
        tmp_path, names, payloads, absent={names[0]})
    consumed = _spy_consumed(monkeypatch)
    stop = threading.Event()
    mover_key, successor = base._key(), base._key()
    args = _args(queue, stage, cas, mover_key, successor, manifest, digest,
                 entries, workers=1)

    result = stage_move.move(args, stop=stop)

    assert consumed == [entry["path"] for entry in entries], consumed
    assert result["entries_staged"] == 2, result
    assert result["complete"] is False
    assert len(result["errors"]) == 1 and names[0] in result["errors"][0], (
        result["errors"])
    assert all(_destination(stage, mount, entry).exists()
               for entry in entries[1:])
    assert stop.is_set() is False


def test_a_live_pin_refusal_is_unchanged_and_stops_the_range(
        fleet, tmp_path, monkeypatch):
    """The live owner is preserved exactly, and its refusal stops the range."""

    queue, stage, cas = fleet
    names = NAMES[:2]
    payloads = {name: _payload(index) for index, name in enumerate(names)}
    mount, manifest, digest, entries = _manifest(tmp_path, names, payloads)
    pinned = _destination(stage, mount, entries[0])
    pinned.parent.mkdir(parents=True, exist_ok=True)
    old = b"p" * SIZE
    pinned.write_bytes(old)
    donor_consumer, donor_mover = base._key(), base._key()
    _write_owner(queue, stage, donor_consumer, donor_mover, pinned,
                 hashlib.sha256(old).hexdigest())
    acquired = reader_lease.acquire(
        queue, consumer_action_key=donor_consumer,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": "fixture", "pid": os.getpid()}, acquire_token="p1",
        covers=[{"mover_action_key": donor_mover,
                 "manifest_sha256": "a" * 64}])
    assert acquired.get("ok"), acquired
    replacement = pinned.with_name(pinned.name + ".later")
    replacement.write_bytes(payloads[names[0]])
    os.replace(replacement, pinned)
    live_bytes = pinned.read_bytes()
    consumed = _spy_consumed(monkeypatch)
    stop = threading.Event()
    mover_key, successor = base._key(), base._key()
    args = _args(queue, stage, cas, mover_key, successor, manifest, digest,
                 entries, workers=1)

    result = stage_move.move(args, stop=stop)

    assert consumed == [entries[0]["path"]], consumed
    assert len(result["errors"]) == 1
    assert "live-pinned by" in result["errors"][0], result["errors"]
    assert pinned.read_bytes() == live_bytes, (
        "the pinned publication must never be replaced by a refusal")
    live, tainted = reader_lease.live_for(
        queue, {os.path.normpath(str(pinned))},
        residency_root=queue.residency_fragment_root())
    assert live and not tainted
    assert not _destination(stage, mount, entries[1]).exists()
    assert stop.is_set() is False


def test_a_pre_set_stop_is_not_mistaken_for_a_refusal(
        fleet, tmp_path, monkeypatch):
    """Cancellation is the caller's event; only the gate sets the range flag."""

    queue, stage, cas = fleet
    names = NAMES[:2]
    payloads = {name: _payload(index) for index, name in enumerate(names)}
    mount, manifest, digest, entries = _manifest(tmp_path, names, payloads)
    consumed = _spy_consumed(monkeypatch)
    stop = threading.Event()
    stop.set()
    mover_key, successor = base._key(), base._key()
    args = _args(queue, stage, cas, mover_key, successor, manifest, digest,
                 entries, workers=1)

    result = stage_move.move(args, stop=stop)

    assert consumed == [], "a set stop consumes nothing"
    assert result["entries_staged"] == 0
    assert stop.is_set() is True, "the caller's event is the caller's"
