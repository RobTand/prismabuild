"""Partial prune of positively stale mentions under the old owner's charge (#853).

The live shape: a FAILED consumer's executed DONE mover leaves a material
sidecar dating a superseded incarnation on one path and a coherent
incarnation on another.  The shared publisher refuses the stale path, so the
owner's mention has to go; the coherent entry beside it is valid reusable
cache whose proof, bytes, generation and charge a successor adopts for free,
so whole-owner `evict` is not allowed to take it.

The approved repair (#853 phase 2) prunes only the positively stale paths
inside the dead-owner transaction: select, classify and act under one stage
ownership hold; inode difference is the stale witness; everything unknown
retains the whole owner.  A partial prune keeps the ENTIRE old holder as a
conservative reservation -- no charge moves until the final old fragment goes
through the ordinary whole-owner egress, which settles it exactly once.  A
fully stale owner goes through that ordinary egress directly.

These tests use the accepted red fixture's real synthetic queue/stage and
drive the real `stage_move.move` and `reader_lease` seam; the skip-cost test
counts real `os.lstat` calls on the fixture's destinations.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
import test_stale_material_done_owner_retires as red  # noqa: E402
from test_stale_material_done_owner_retires import fleet  # noqa: E402
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

NAMES = red.NAMES
SIZE = red.SIZE
TIER = red.TIER


def _key_for(stage: Path, name: str) -> str:
    return residency_map.residency_map_key(str(stage / name), 0)


def _fragment_entries(material_entries: dict) -> dict:
    """The fragment view of material entries: no file_id, offset zero."""

    return {key: {"stage_path": entry["stage_path"], "bytes": entry["bytes"],
                  "sha256": entry["sha256"], "offset": 0}
            for key, entry in material_entries.items()}


def _fragment_doc(queue, consumer: str, mover: str) -> dict:
    path = residency_map.fragment_path(
        queue.residency_fragment_root(), consumer, mover)
    return residency_map.validate_fragment(json.loads(path.read_bytes()))


def _material_doc(queue, consumer: str, mover: str) -> dict:
    got = reader_lease.read_material(
        queue.residency_fragment_root(), consumer, mover)
    assert isinstance(got, dict), got
    return got


def _receipts_for(receipts, mover: str) -> list[dict]:
    return [entry for entry in receipts
            if entry.get("event") == stage_release.STALE_MENTION_EVENT
            and entry.get("action_key") == mover]


def _tokens(queue, mover: str) -> dict:
    return queue.tier_ledger(TIER).holder_tokens(mover)


def _acquire_pin(queue, consumer: str, mover: str) -> dict:
    return reader_lease.acquire(
        queue, consumer_action_key=consumer,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": "fixture", "pid": os.getpid()}, acquire_token="p1",
        covers=[{"mover_action_key": mover, "manifest_sha256": "a" * 64}])


def _read_both(queue, successor, copier, entries, digests, payloads,
               manifest_digest) -> None:
    """One strict leased read of both recovered paths through the real seam."""

    keys = {name: residency_map.residency_map_key(entry["path"], 0)
            for name, entry in zip(NAMES, entries)}
    pin = reader_lease.acquire(
        queue, consumer_action_key=successor,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": "fixture", "pid": os.getpid()}, acquire_token="both",
        covers=[{"mover_action_key": copier,
                 "manifest_sha256": manifest_digest}],
        expected={keys[name]: {"bytes": SIZE, "sha256": digests[name]}
                  for name in NAMES})
    assert pin["ok"], pin
    for name in NAMES:
        fd, _ = reader_lease.open_pinned(queue, pin["pin"], pin["ref_id"],
                                         keys[name])
        try:
            assert os.read(fd, SIZE) == payloads[name]
        finally:
            os.close(fd)
    reader_lease.release(queue, pin["pin_id"], pin["ref_id"],
                         consumer_action_key=successor)


def _count_opens(monkeypatch, mount: Path) -> list[str]:
    """Which source payloads a run actually opens (adoption opens none)."""

    opened: list[str] = []
    real_open = os.open

    def counting(path, flags, *args, **kwargs):
        if isinstance(path, (str, Path)) and str(path).startswith(str(mount)):
            opened.append(str(path))
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", counting)
    return opened


def _count_destination_lstats(monkeypatch, paths) -> list[int]:
    """How many times the fixture's own destinations are stat'ed."""

    watched = {os.path.normpath(str(path)) for path in paths}
    seen = [0]
    real = os.lstat

    def counting(path, *args, **kwargs):
        try:
            name = os.path.normpath(os.fspath(path))
        except TypeError:
            name = ""
        if name in watched:
            seen[0] += 1
        return real(path, *args, **kwargs)

    monkeypatch.setattr(os, "lstat", counting)
    return seen


# --------------------------------------------------------------------------
# The partial prune itself
# --------------------------------------------------------------------------

def test_the_mixed_prune_is_partial_and_keeps_the_whole_charge(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    coherent = stage / NAMES[0]
    inode = os.stat(coherent).st_ino
    generation = _material_doc(queue, consumer, mover)["generation"]
    held = _tokens(queue, mover)
    assert held, "the fixture must carry the incident's charge"

    receipts = red._sweep(queue, stage)

    partial = [entry for entry in _receipts_for(receipts, mover)
               if entry.get("partial") is True]
    assert partial, receipts
    assert partial[0]["entries_pruned"] == 1, partial[0]
    assert partial[0]["entries_retained"] == 1, partial[0]
    assert partial[0]["charge_retained"] is True
    assert partial[0]["complete"] is False, (
        "a partial prune must not report full-owner recovery")
    assert partial[0]["errors"] == [], partial[0]["errors"]
    # The coherent entry keeps its bytes, identity, proof, generation and the
    # old holder keeps its entire charge.
    assert coherent.exists() and os.stat(coherent).st_ino == inode
    fragment = _fragment_doc(queue, consumer, mover)
    assert set(fragment["entries"]) == {_key_for(stage, NAMES[0])}
    material = _material_doc(queue, consumer, mover)
    assert set(material["entries"]) == {_key_for(stage, NAMES[0])}
    assert material["generation"] == generation
    assert _tokens(queue, mover) == held, (
        "no charge may move during a partial prune")
    # The stale destination is physically recovered, so a successor replaces
    # it immediately instead of paying the orphan grace.
    assert not (stage / NAMES[1]).exists()


def test_the_successor_adopts_the_survivor_with_zero_recopy(
        fleet, tmp_path, monkeypatch):
    queue, stage, cas = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    coherent = stage / NAMES[0]
    inode = os.stat(coherent).st_ino
    receipts = red._sweep(queue, stage)
    assert [entry for entry in _receipts_for(receipts, mover)
            if entry.get("entries_pruned") == 1], receipts

    # The shortened donor is a per-path cache, never a whole-range donor:
    # whole-range adoption refuses it and leaves donor and tokens untouched.
    root = queue.residency_fragment_root()
    holder = queue.tier_ledger(TIER).holder_tokens(mover)
    refused = tier_loop.adopt(
        queue, old_key=mover, new_key=base._key(),
        consumer_action_key=base._key(), tier_id=TIER, phase="head",
        range_start_bytes=0, range_end_bytes=len(NAMES) * SIZE,
        residency_root=root)
    assert not refused["adopted"] and refused["reason"] == "donor_range_shortened", (
        refused)
    assert queue.tier_ledger(TIER).holder_tokens(mover) == holder
    assert os.stat(coherent).st_ino == inode

    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", red.GRACE)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)
    payloads = {NAMES[0]: red.OLD_PAYLOAD, NAMES[1]: b"x" * SIZE}
    path, digest, entries, digests = red._successor_manifest(tmp_path, payloads)
    mount = tmp_path / "sources"
    opened = _count_opens(monkeypatch, mount)
    successor, copier = base._key(), base._key()
    base._publish(queue, successor, max_attempts=1)
    args = red._successor_args(queue, stage, cas, copier, successor, path,
                               digest, entries)
    result = stage_move.move(args)

    assert result["complete"] and result["entries_staged"] == len(entries), (
        result["errors"])
    assert opened == [str(mount / NAMES[1])], (
        "the coherent entry must be adopted, never recopied")
    assert os.stat(coherent).st_ino == inode
    queue.record_move(copier, result)
    _read_both(queue, successor, copier, entries, digests, payloads, digest)


def test_a_fully_stale_owner_settles_its_holder_once(fleet, tmp_path, monkeypatch):
    """The existing whole-owner egress still handles the wholly stale shape."""

    queue, stage, cas = fleet
    consumer, mover = red.stale_owner(fleet)
    held = _tokens(queue, mover)
    assert held

    receipts = red._sweep(queue, stage)

    retired = [entry for entry in receipts
               if entry.get("action_key") == mover
               and entry.get("complete") is True]
    assert retired, receipts
    assert not residency_map.fragment_path(
        queue.residency_fragment_root(), consumer, mover).exists()
    assert not reader_lease.material_path(
        queue.residency_fragment_root(), consumer, mover).exists()
    assert _tokens(queue, mover) == {}, "the whole charge settles exactly once"
    assert not any((stage / name).exists() for name in NAMES)

    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", red.GRACE)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)
    payloads = {name: b"x" * SIZE for name in NAMES}
    path, digest, entries, digests = red._successor_manifest(tmp_path, payloads)
    successor, copier = base._key(), base._key()
    base._publish(queue, successor, max_attempts=1)
    result = stage_move.move(red._successor_args(
        queue, stage, cas, copier, successor, path, digest, entries))
    assert result["complete"], result["errors"]
    queue.record_move(copier, result)
    red._successor_reads(queue, successor, copier, entries, digests, payloads,
                         digest)


# --------------------------------------------------------------------------
# Whole-candidate retention: live state, unknown state, wrong state
# --------------------------------------------------------------------------

def test_a_live_pin_retains_the_whole_candidate(fleet):
    queue, stage, _ = fleet
    consumer, _generation = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    for name in NAMES:
        red._stage(stage, name, red.OLD_PAYLOAD)
    red._write_sidecar(queue, stage, consumer, mover, red._entries(stage))
    red._fragment(queue, stage, consumer, mover)
    pin = _acquire_pin(queue, consumer, mover)
    assert pin["ok"], pin
    replacement = red._stage(stage, NAMES[1] + ".later", red.NEW_PAYLOAD)
    os.replace(replacement, stage / NAMES[1])
    queue.finish(mover, status="executed", detail={"returncode": 0})
    red._charge(queue, mover)
    held = _tokens(queue, mover)

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["retained_reason"] == "live-pin", retained
    assert retained[0]["entries_pruned"] == 0
    assert all((stage / name).exists() for name in NAMES)
    assert set(_fragment_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, name) for name in NAMES}
    assert set(_material_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, name) for name in NAMES}
    assert _tokens(queue, mover) == held


def test_a_live_claim_overlap_retains_the_whole_candidate(fleet, monkeypatch):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    monkeypatch.setattr(
        stage_release, "_claimed_paths_attributed",
        lambda *args, **kwargs: ({NAMES[1]}, [], set()))

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["retained_reason"] == "live-claim", retained
    assert retained[0]["entries_pruned"] == 0
    assert all((stage / name).exists() for name in NAMES)
    assert set(_fragment_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, name) for name in NAMES}


def test_a_same_key_claim_retains_the_whole_candidate(fleet, monkeypatch):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    monkeypatch.setattr(
        stage_release, "_claimed_paths_attributed",
        lambda *args, **kwargs: (set(), [], {NAMES[1]}))

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["retained_reason"] == "same-key-claimed"
    assert retained[0]["entries_pruned"] == 0
    assert all((stage / name).exists() for name in NAMES)


def test_a_promotion_handoff_retains_the_whole_candidate(
        fleet, monkeypatch):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    handoff = os.path.normpath(str((stage / NAMES[1]).resolve()))
    monkeypatch.setattr(
        stage_release, "_claimed_source_paths",
        lambda *args, **kwargs: ({handoff}, []))

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["retained_reason"] == "promotion-handoff"
    assert retained[0]["entries_pruned"] == 0
    assert all((stage / name).exists() for name in NAMES)


def test_a_co_owner_fragment_protects_its_file_and_the_rest_still_prunes(
        fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet)
    coowner, comover = base._key(), base._key()
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": coowner, "mover_action_key": comover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "b" * 64,
        "entries": {_key_for(stage, NAMES[1]): {
            "stage_path": str(stage / NAMES[1]), "bytes": SIZE,
            "sha256": red.OLD_DIGEST, "offset": 0}}})
    protected = stage / NAMES[1]
    assert protected.exists()

    receipts = red._sweep(queue, stage)

    partial = [entry for entry in _receipts_for(receipts, mover)
               if entry.get("partial") is True]
    assert partial, receipts
    assert partial[0]["entries_pruned"] == 1
    assert partial[0]["entries_retained"] == 1
    assert protected.exists(), "a co-owner's physical file is never unlinked"
    assert not (stage / NAMES[0]).exists()
    assert set(_fragment_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, NAMES[1])}


def test_a_co_owner_cannot_make_a_nonregular_path_clean(fleet):
    """A co-owner protects a file, never an unknown or nonregular path."""

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet)
    coowner, comover = base._key(), base._key()
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": coowner, "mover_action_key": comover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "b" * 64,
        "entries": {_key_for(stage, NAMES[1]): {
            "stage_path": str(stage / NAMES[1]), "bytes": SIZE,
            "sha256": red.OLD_DIGEST, "offset": 0}}})
    target = stage / NAMES[1]
    os.unlink(target)
    target.mkdir()

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["entries_pruned"] == 0, retained
    assert retained[0]["retained_reason"] == "ownership-uncertain"
    assert (stage / NAMES[0]).exists(), (
        "another stale path must not delete behind a nonregular co-owned path")
    assert target.is_dir()
    assert set(_fragment_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, name) for name in NAMES}


def test_a_same_inode_mutation_retains_the_whole_candidate(fleet):
    """The dated inode is the one that mutates in place: divergence retains."""

    queue, stage, _ = fleet
    # NAMES[0] is the stale path, NAMES[1] the coherent one whose dated inode
    # is then changed in place (same inode, new mtime/ctime).
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[1]})
    with open(stage / NAMES[1], "r+b") as stream:
        stream.write(b"z")

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["entries_pruned"] == 0, retained
    assert retained[0]["retained_reason"] == "ownership-uncertain"
    assert retained[0]["errors"], "an in-place change is not stale evidence"
    assert all((stage / name).exists() for name in NAMES)


@pytest.mark.parametrize("replacement", ["symlink", "directory"])
def test_a_nonregular_destination_retains_the_whole_candidate(
        fleet, replacement):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    target = stage / NAMES[1]
    os.unlink(target)
    if replacement == "symlink":
        target.symlink_to(stage / NAMES[0])
    else:
        target.mkdir()

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["entries_pruned"] == 0, retained
    assert retained[0]["retained_reason"] == "ownership-uncertain"
    assert target.exists() or target.is_symlink()
    assert (stage / NAMES[0]).exists()


def test_a_symlink_outside_the_stage_retains_the_whole_candidate(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    outside = stage.parent / "outside.bin"
    outside.write_bytes(red.NEW_PAYLOAD)
    target = stage / NAMES[1]
    os.unlink(target)
    target.symlink_to(outside)

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["entries_pruned"] == 0, retained
    assert retained[0]["retained_reason"] == "ownership-uncertain"
    assert target.is_symlink()
    assert outside.exists()


def test_a_missing_key_binding_retains_the_whole_candidate(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]},
                                      material=False)
    entries = red._entries(stage)
    entries.pop(_key_for(stage, NAMES[1]), None)
    red._write_sidecar(queue, stage, consumer, mover, entries)

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["entries_pruned"] == 0, retained
    assert retained[0]["retained_reason"] == "material-key-missing"
    assert all((stage / name).exists() for name in NAMES)


def test_a_missing_path_without_a_bound_mention_retains_the_whole_candidate(
        fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]},
                                      material=False)
    entries = red._entries(stage)
    entries.pop(_key_for(stage, NAMES[1]), None)
    os.unlink(stage / NAMES[1])
    red._write_sidecar(queue, stage, consumer, mover, entries)

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["entries_pruned"] == 0, retained
    assert retained[0]["retained_reason"] == "material-key-missing"
    assert (stage / NAMES[0]).exists()
    assert not (stage / NAMES[1]).exists()


def test_a_foreign_material_retains_the_whole_candidate(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    red._write_sidecar(queue, stage, consumer, mover, red._entries(stage),
                       tier_id="prismabuild-stage:other")

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["entries_pruned"] == 0, retained
    assert retained[0]["retained_reason"] == "material-does-not-bind"
    assert all((stage / name).exists() for name in NAMES)


@pytest.mark.parametrize("owner", ["consumer", "mover"])
def test_republication_after_census_retains(fleet, monkeypatch, owner):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    original = stage_release._fragment_census
    published = False

    def census(root):
        nonlocal published
        result = original(root)
        if not published:
            published = True
            queue.publish(action_key=consumer if owner == "consumer" else mover,
                          cas_root="/cas", checkout_root="/co",
                          worker_script="/w.py", resources={"cpu": 1},
                          max_attempts=1, recompute=True)
        return result

    monkeypatch.setattr(stage_release, "_fragment_census", census)
    red._sweep(queue, stage)

    _assert_owner_intact(queue, stage, consumer, mover)
    assert queue.item_path(
        pool.READY, consumer if owner == "consumer" else mover).exists()


def test_a_damaged_done_terminal_retains(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    path = queue.item_path(pool.DONE, mover)
    record = json.loads(path.read_bytes())
    record["published_unix"] += 1
    path.write_text(json.dumps(record))

    receipts = red._sweep(queue, stage)

    assert not [entry for entry in _receipts_for(receipts, mover)
                if entry.get("entries_pruned")]
    _assert_owner_intact(queue, stage, consumer, mover)


def _assert_owner_intact(queue, stage, consumer, mover) -> None:
    assert all((stage / name).exists() for name in NAMES)
    assert set(_fragment_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, name) for name in NAMES}
    assert set(_material_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, name) for name in NAMES}


# --------------------------------------------------------------------------
# Crash replay, idempotence and the bounded skip checkpoint
# --------------------------------------------------------------------------

def test_a_crash_after_the_fragment_write_replays_conservatively(
        fleet, monkeypatch):
    """The committed fragment prune stands; the material superset is inert."""

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    held = _tokens(queue, mover)
    material_generation = _material_doc(queue, consumer, mover)["generation"]
    real_write_material = reader_lease.write_material

    def failing(*args, **kwargs):
        raise OSError("injected crash between the fragment and the material")

    monkeypatch.setattr(reader_lease, "write_material", failing)
    receipts = red._sweep(queue, stage)
    monkeypatch.undo()

    partial = [entry for entry in _receipts_for(receipts, mover)
               if entry.get("partial") is True]
    assert partial, receipts
    assert partial[0]["entries_pruned"] == 1, (
        "the committed fragment prune is reported even when the pair failed")
    assert partial[0]["document_pair_complete"] is False
    assert partial[0]["entries_unlinked"] == 1
    assert partial[0]["bytes_unlinked"] == SIZE
    assert partial[0]["errors"], "the failed second write is reported"
    assert set(_fragment_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, NAMES[0])}
    # The material is a superset: it may date removed fragment entries, but it
    # never asserts ownership.
    assert set(_material_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, name) for name in NAMES}
    assert not (stage / NAMES[1]).exists()
    assert (stage / NAMES[0]).exists()
    assert _tokens(queue, mover) == held

    # Replay: the pair completes, the material is trimmed to the surviving
    # fragment's exact key set, nothing settles twice and the coherent
    # survivor is readable through the strict leased seam.
    replay = red._sweep(queue, stage)
    assert not [entry for entry in _receipts_for(replay, mover)
                if entry.get("entries_pruned")]
    material_after = _material_doc(queue, consumer, mover)
    assert set(material_after["entries"]) == {_key_for(stage, NAMES[0])}, (
        "a material superset must be trimmed to the surviving fragment keys")
    assert material_after["generation"] == material_generation
    assert (stage / NAMES[0]).exists()
    assert _tokens(queue, mover) == held
    pin = reader_lease.acquire(
        queue, consumer_action_key=consumer,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": "fixture", "pid": os.getpid()},
        acquire_token="survivor",
        covers=[{"mover_action_key": mover, "manifest_sha256": "a" * 64}],
        expected={_key_for(stage, NAMES[0]): {
            "bytes": SIZE, "sha256": red.OLD_DIGEST}})
    assert pin["ok"], pin
    fd, _ = reader_lease.open_pinned(queue, pin["pin"], pin["ref_id"],
                                     _key_for(stage, NAMES[0]))
    try:
        assert os.read(fd, SIZE) == red.OLD_PAYLOAD
    finally:
        os.close(fd)
        reader_lease.release(queue, pin["pin_id"], pin["ref_id"],
                             consumer_action_key=consumer)


def test_a_crash_before_the_fragment_write_replays_conservatively(
        fleet, monkeypatch):
    """An interrupted pair reports actual deletions and committed prunes separately."""

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    held = _tokens(queue, mover)

    def failing(*args, **kwargs):
        raise OSError("injected crash before the fragment write")

    monkeypatch.setattr(residency_map, "write_fragment", failing)
    receipts = red._sweep(queue, stage)
    monkeypatch.undo()

    partial = [entry for entry in _receipts_for(receipts, mover)
               if entry.get("partial") is True]
    assert partial, receipts
    assert partial[0]["entries_pruned"] == 0, (
        "an unwritten fragment commits no metadata prune")
    assert partial[0]["document_pair_complete"] is False
    assert partial[0]["entries_unlinked"] == 1
    assert partial[0]["bytes_unlinked"] == SIZE
    assert set(_fragment_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, name) for name in NAMES}
    assert not (stage / NAMES[1]).exists()
    assert (stage / NAMES[0]).exists()
    assert _tokens(queue, mover) == held

    # The replay completes the pair from the absent leaf, once.
    replay = red._sweep(queue, stage)
    completed = [entry for entry in _receipts_for(replay, mover)
                 if entry.get("document_pair_complete") is True]
    assert completed and completed[0]["entries_pruned"] == 1, replay
    assert set(_fragment_doc(queue, consumer, mover)["entries"]) == {
        _key_for(stage, NAMES[0])}
    assert _tokens(queue, mover) == held


def test_repeated_sweeps_neither_prune_nor_settle_twice(fleet):
    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    held = _tokens(queue, mover)
    first = red._sweep(queue, stage)
    assert [entry for entry in _receipts_for(first, mover)
            if entry.get("entries_pruned") == 1]

    second = red._sweep(queue, stage)

    assert not [entry for entry in _receipts_for(second, mover)
                if entry.get("entries_pruned")]
    assert (stage / NAMES[0]).exists()
    assert not (stage / NAMES[1]).exists()
    assert _tokens(queue, mover) == held


def test_an_unchanged_coherent_owner_skips_and_is_invalidated(
        fleet, monkeypatch):
    """The dead-owner pass alone, so only its own stats are counted."""

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    destinations = [stage / name for name in NAMES]
    seen = _count_destination_lstats(monkeypatch, destinations)

    def run_pass():
        return stage_release.sweep_dead_owner_fragments(
            queue, stage_roots={TIER: str(stage)},
            residency_root=queue.residency_fragment_root())

    run_pass()
    assert seen[0] >= len(destinations), "the first pass must scan every entry"

    seen[0] = 0
    run_pass()
    assert seen[0] == 0, "an unchanged owner must skip its entry scan"

    # A metadata rewrite (same content) invalidates the checkpoint.
    red._fragment(queue, stage, consumer, mover)
    seen[0] = 0
    run_pass()
    assert seen[0] >= len(destinations), "a rewritten fragment must re-scan"

    # A replaced incarnation invalidates it and is then positively stale.
    seen[0] = 0
    replacement = red._stage(stage, NAMES[1] + ".later", red.NEW_PAYLOAD)
    os.replace(replacement, stage / NAMES[1])
    receipts = run_pass()
    assert seen[0] >= 1, "a replaced destination must re-scan"
    assert [entry for entry in _receipts_for(receipts, mover)
            if entry.get("entries_pruned") == 1], receipts


def test_replacing_the_parent_directory_invalidates_the_checkpoint(
        fleet, monkeypatch):
    queue, stage, _ = fleet
    sub = stage / "sub"
    sub.mkdir()
    consumer, _generation = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    entries = {}
    for name in NAMES:
        path = red._stage(sub, name, red.OLD_PAYLOAD)
        entries[_key_for(sub, name)] = {
            "stage_path": str(path), "bytes": SIZE,
            "sha256": red.OLD_DIGEST, "file_id": red._identity(path)}
    reader_lease.write_material(
        queue.residency_fragment_root(), consumer_action_key=consumer,
        mover_action_key=mover, tier_id=TIER, stage_root=str(stage),
        manifest_sha256="a" * 64, generation="a" * 32, entries=entries)
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": _fragment_entries(entries)})
    queue.finish(mover, status="executed", detail={"returncode": 0})
    destinations = [sub / name for name in NAMES]
    seen = _count_destination_lstats(monkeypatch, destinations)

    def run_pass():
        return stage_release.sweep_dead_owner_fragments(
            queue, stage_roots={TIER: str(stage)},
            residency_root=queue.residency_fragment_root())

    run_pass()
    assert seen[0] >= len(destinations), "the first pass must scan"
    seen[0] = 0
    run_pass()
    assert seen[0] == 0, "the unchanged owner must skip"

    moved = stage / "sub-moved"
    os.rename(sub, moved)
    os.rename(moved, sub)
    seen[0] = 0
    run_pass()
    assert seen[0] >= len(destinations), (
        "a directory rename must invalidate the checkpoint")


def test_a_claim_blocks_then_recovery_after_it_ends(
        fleet, tmp_path, monkeypatch):
    """A current claim is a negative; once it ends, the ordinary sweep recovers."""

    queue, stage, cas = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names={NAMES[0]})
    monkeypatch.setattr(
        stage_release, "_claimed_paths_attributed",
        lambda *args, **kwargs: ({NAMES[1]}, [], set()))
    first = red._sweep(queue, stage)
    retained = _receipts_for(first, mover)
    assert retained and retained[0]["retained_reason"] == "live-claim"
    assert retained[0]["entries_pruned"] == 0
    assert (stage / NAMES[1]).exists()
    monkeypatch.undo()

    second = red._sweep(queue, stage)
    pruned = [entry for entry in _receipts_for(second, mover)
              if entry.get("entries_pruned") == 1]
    assert pruned, second
    assert not (stage / NAMES[1]).exists()

    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", red.GRACE)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)
    payloads = {NAMES[0]: red.OLD_PAYLOAD, NAMES[1]: b"x" * SIZE}
    path, digest, entries, digests = red._successor_manifest(tmp_path, payloads)
    successor, copier = base._key(), base._key()
    base._publish(queue, successor, max_attempts=1)
    result = stage_move.move(red._successor_args(
        queue, stage, cas, copier, successor, path, digest, entries))
    assert result["complete"], result["errors"]
    queue.record_move(copier, result)
    red._successor_reads(queue, successor, copier, entries, digests, payloads,
                         digest)


def test_a_rename_after_installation_is_seen_by_the_next_sweep(
        fleet, monkeypatch):
    """The installed stamps are the verified ones, never a fresh sample."""

    queue, stage, _ = fleet
    consumer, mover = red.stale_owner(fleet, coherent_names=set(NAMES),
                                      replace=False)
    destinations = [stage / name for name in NAMES]
    seen = _count_destination_lstats(monkeypatch, destinations)
    real_install = stage_release._install_skip_checkpoint
    raced = []

    def racing_install(*args, **kwargs):
        installed = real_install(*args, **kwargs)
        if installed and not raced:
            raced.append(True)
            replacement = red._stage(stage, NAMES[1] + ".later",
                                     red.NEW_PAYLOAD)
            os.replace(replacement, stage / NAMES[1])
        return installed

    monkeypatch.setattr(stage_release, "_install_skip_checkpoint",
                        racing_install)
    red._sweep(queue, stage)
    assert raced, "the fixture must install a checkpoint"

    seen[0] = 0
    receipts = red._sweep(queue, stage)
    assert seen[0] >= 1, (
        "a rename after installation must not be blessed as clean")
    assert [entry for entry in _receipts_for(receipts, mover)
            if entry.get("entries_pruned") == 1], receipts


def test_a_symlinked_intermediate_directory_retains(fleet):
    queue, stage, _ = fleet
    real = stage / "real"
    real.mkdir()
    consumer, _generation = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    path = red._stage(real, NAMES[0], red.OLD_PAYLOAD)
    (stage / "sub").symlink_to(real)
    entries = {_key_for(real, NAMES[0]): {
        "stage_path": str(stage / "sub" / NAMES[0]), "bytes": SIZE,
        "sha256": red.OLD_DIGEST, "file_id": red._identity(path)}}
    reader_lease.write_material(
        queue.residency_fragment_root(), consumer_action_key=consumer,
        mover_action_key=mover, tier_id=TIER, stage_root=str(stage),
        manifest_sha256="a" * 64, generation="a" * 32, entries=entries)
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": _fragment_entries(entries)})
    queue.finish(mover, status="executed", detail={"returncode": 0})

    receipts = red._sweep(queue, stage)

    retained = _receipts_for(receipts, mover)
    assert retained and retained[0]["entries_pruned"] == 0, retained
    assert retained[0]["retained_reason"] == "ownership-uncertain"
    assert path.exists(), "a symlinked intermediate directory is never acted on"


def test_the_skip_checkpoint_cache_is_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(stage_release, "SKIP_CHECKPOINT_MAX_ENTRIES", 1)
    stage_release.reset_skip_checkpoints()
    fragment = tmp_path / "fragment.json"
    fragment.write_text("{}")
    material = tmp_path / "material.json"
    material.write_text("{}")
    fragment_version = stage_release._path_version(fragment)
    material_version = stage_release._path_version(material)
    stamps = {str(tmp_path): stage_release._directory_version(tmp_path)}
    assert stage_release._install_skip_checkpoint(
        ("tier", "consumer", "mover-1"), fragment_version, material_version,
        stamps)
    assert stage_release._install_skip_checkpoint(
        ("tier", "consumer", "mover-2"), fragment_version, material_version,
        stamps)
    assert len(stage_release._skip_checkpoints) == 1, (
        "the cache must bound its entries")
    assert not stage_release._skip_checkpoint_hit(
        ("tier", "consumer", "mover-1"), fragment, material)
    assert stage_release._skip_checkpoint_hit(
        ("tier", "consumer", "mover-2"), fragment, material)
    # The aggregate retained-path budget refuses a candidate that cannot fit.
    stage_release.reset_skip_checkpoints()
    monkeypatch.setattr(stage_release, "SKIP_CHECKPOINT_MAX_TOTAL_DIRS", 0)
    assert not stage_release._install_skip_checkpoint(
        ("tier", "consumer", "mover-3"), fragment_version, material_version,
        stamps)
    # A stamp of None is never installed.
    monkeypatch.setattr(stage_release, "SKIP_CHECKPOINT_MAX_TOTAL_DIRS", 8192)
    assert not stage_release._install_skip_checkpoint(
        ("tier", "consumer", "mover-4"), fragment_version, material_version,
        {str(tmp_path): None})
    stage_release.reset_skip_checkpoints()
    assert stage_release._skip_checkpoints == {}
    assert stage_release._skip_checkpoint_usage == {"dirs": 0, "bytes": 0}
