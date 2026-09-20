"""Bounded recovery of a retired head's unprovable cache copies.

Routine ``reconcile`` cannot reach these.  It decides ownership from the
``user.pbstage.source`` mark, and ``stage_move`` sets that same mark on every
file it publishes, so a stage copy always reads as prewarm-owned and lands in
``unowned_left`` forever.  A head whose fragment and material are gone leaves
bytes no document proves and no sweep may touch, and every later head then pays
the 30 s publisher grace once per entry for them.

``recover_orphaned_range`` asks by identity instead, and takes no scope from
its caller: the consumer, tier, stage root, manifest and exact range are read
off the head's own filed move receipt, cross-checked against the sealed CAS
request, and corroborated by the egress receipt that retired it.

These pin the asymmetry the repair is built on -- **over-retaining is a pass
and over-removing is a failure** -- so every case that is not provably eligible
survives, and every refusal fails *before* anything is unlinked, with the
qualified prefix and the originals asserted intact.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, residency_map  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
HEAD = "1" * 64
EGRESS = "5" * 64
CONSUMER = "2" * 64
LIVE_MOVER = "3" * 64
LIVE_CONSUMER = "4" * 64
NAMES = ["alpha.bin", "beta.bin", "gamma.bin", "delta.bin"]
SIZE = 1024
SPAN = SIZE * len(NAMES)


def _manifest_blob(cas_root: Path, mount: Path) -> str:
    """A real v1 manifest in the CAS, addressed by its own digest."""

    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {}, "annotations": {},
        "mount_prefix": str(mount),
        "entry_count": len(NAMES), "total_bytes": SPAN,
        "entries": [{"path": f"{mount}/{name}", "offset": 0,
                     "bytes": SIZE, "sha256": None} for name in NAMES],
    }
    raw = json.dumps(manifest, sort_keys=True).encode()
    digest = hashlib.sha256(raw).hexdigest()
    blob = pb.PrismaBuildCAS(cas_root).blob_path(digest)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(raw)
    return digest


def _seal(cas_root: Path, action_key: str, digest: str) -> None:
    """The sealed request whose manifest the scope is bound to."""

    path = cas_root / "requests" / action_key[:2] / f"{action_key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "action_key": action_key,
        "params": {"data_manifest": {
            "input": {"id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                      "sha256": digest, "bytes": 1024},
            "mount_prefix": "/unused", "entry_count": len(NAMES)}},
    }))


def _file_receipts(queue: pool.PoolQueue, stage: Path, digest: str, *,
                   entries: int = len(NAMES), end: int = SPAN) -> None:
    # The real field shapes, read off pb-queue/movers/ for an adopted head
    # and its egress.  The asymmetry is the point: the head carries tier,
    # manifest and range; the egress carries none of them, so nothing here
    # may require them of it.
    queue.record_move(HEAD, {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": HEAD,
        "consumer_action_key": CONSUMER, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": digest,
        "range_start_bytes": 0, "range_end_bytes": end, "range_bytes": end,
        "entries_declared": entries, "entries_staged": entries,
        "bytes_staged": SPAN, "bytes_copied": 0, "phase": "head",
        "adopted_from": "d" * 64, "adopted_from_consumer": "e" * 64,
        "host": "dl380g10", "unix": 1.0, "complete": True,
    })
    queue.record_move(EGRESS, {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": EGRESS,
        "consumer_action_key": CONSUMER, "stage_root": str(stage),
        "reason": "egress", "retiring": False,
        "entries_shared": entries, "entries_deleted": 0,
        "entries_already_gone": 0, "entries_deferred": 0,
        "bytes_shared": SPAN, "bytes_deleted": 0,
        "shared_with": [], "live_pins": [], "auto_reclaimed": [],
        "auto_retained": {}, "tokens_released": 0, "tokens_decharged": 0,
        "errors": [], "host": "dl380g10", "unix": 2.0, "complete": True,
    })


def _stage_copy(stage: Path, name: str, *, marked: bool = True) -> Path:
    path = stage / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * SIZE)
    if marked:
        os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR,
                    f"/originals/{name}@0".encode())
    return path


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    cas = tmp_path / "cas"
    cas.mkdir()
    originals = tmp_path / "originals"
    originals.mkdir()
    for name in NAMES:
        (originals / name).write_bytes(b"\1" * SIZE)
    digest = _manifest_blob(cas, originals)
    _seal(cas, HEAD, digest)
    _seal(cas, EGRESS, digest)
    _file_receipts(queue, stage, digest)
    stage_release._manifest_layout_cache.clear()
    return queue, stage, cas, digest, originals


def _recover(fleet, *, apply: bool = False, **kwargs):
    queue, stage, cas, _digest, _originals = fleet
    params = {"stage_root": str(stage), "head_action_key": HEAD,
              "egress_action_key": EGRESS, "cas_root": str(cas),
              "apply": apply}
    params.update(kwargs)
    got = stage_release.recover_orphaned_range(queue, **params)
    print(json.dumps(got, indent=1, sort_keys=True, default=str))
    return got


def _populate(stage: Path) -> None:
    for name in NAMES:
        _stage_copy(stage, name)


def _intact(fleet) -> bool:
    """Nothing staged was unlinked and no original was touched."""

    _queue, stage, _cas, _digest, originals = fleet
    return (all((stage / name).exists() for name in NAMES)
            and all((originals / name).exists() for name in NAMES))


def _live_fragment(queue: pool.PoolQueue, stage: Path, names: list[str],
                   digest: str) -> None:
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": LIVE_CONSUMER, "mover_action_key": LIVE_MOVER,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": digest,
        "entries": {
            residency_map.residency_map_key(str(stage / name), 0): {
                "stage_path": str(stage / name), "bytes": SIZE,
                "sha256": "b" * 64, "offset": 0,
            } for name in names
        },
    })


# --- the positive case ------------------------------------------------------

def test_a_retired_heads_unprovable_copies_are_retired(fleet) -> None:
    _queue, stage, _cas, _digest, originals = fleet
    _populate(stage)

    dry = _recover(fleet)
    assert dry["complete"] and dry["applied"] is False
    assert dry["entries_eligible"] == len(NAMES)
    assert dry["bytes_eligible"] == SPAN
    assert dry["entries_retired"] == 0 and dry["bytes_retired"] == 0, (
        "a report must not read as bytes already deleted")
    assert _intact(fleet), "a report-only pass unlinks nothing"

    got = _recover(fleet, apply=True)
    assert got["complete"] and got["applied"] is True
    assert got["entries_retired"] == len(NAMES)
    assert got["bytes_retired"] == SPAN
    assert not any((stage / name).exists() for name in NAMES)
    assert got["originals_checked"] == got["originals_present"] == len(NAMES)
    assert all((originals / name).exists() for name in NAMES)


def test_the_recovery_is_idempotent(fleet) -> None:
    _queue, stage, _cas, _digest, _originals = fleet
    _populate(stage)

    first = _recover(fleet, apply=True)
    second = _recover(fleet, apply=True)
    assert first["entries_retired"] == len(NAMES)
    assert second["entries_retired"] == 0
    assert second["entries_already_gone"] == len(NAMES)
    assert second["bytes_retired"] == 0 and second["complete"]


# --- retention --------------------------------------------------------------

def test_a_qualified_prefix_a_fragment_names_is_preserved(fleet) -> None:
    """Every fragment retains, wanted or not."""

    queue, stage, _cas, digest, _originals = fleet
    _populate(stage)
    kept = NAMES[:2]
    _live_fragment(queue, stage, kept, digest)

    got = _recover(fleet, apply=True)
    assert got["entries_retired"] == len(NAMES) - len(kept)
    assert got["retained_reasons"] == {
        "attributed_pinned_claimed_or_handed_off": 2}
    assert all((stage / name).exists() for name in kept)


@pytest.mark.parametrize("census", [
    "reader_lease.live_for", "_claimed_paths", "_claimed_source_paths"])
def test_every_ownership_census_retains_what_it_names(
        fleet, monkeypatch, census: str) -> None:
    """A pin, a claim in flight and a promotion handoff each retain."""

    _queue, stage, _cas, _digest, _originals = fleet
    _populate(stage)
    pinned = os.path.normpath(str(stage / NAMES[0]))
    held: object = ({pinned: [LIVE_CONSUMER]}
                    if census.endswith("live_for")
                    else {NAMES[0]} if census == "_claimed_paths"
                    else {pinned})
    if "." in census:
        module, name = census.split(".")
        monkeypatch.setattr(getattr(stage_release, module), name,
                            lambda *a, **k: (held, []))
    else:
        monkeypatch.setattr(stage_release, census, lambda *a, **k: (held, []))

    got = _recover(fleet, apply=True)
    assert (stage / NAMES[0]).exists()
    assert got["entries_retired"] == len(NAMES) - 1


def test_a_file_the_stage_did_not_write_is_retained(fleet) -> None:
    """The source mark is a necessary condition, never permission."""

    _queue, stage, _cas, _digest, _originals = fleet
    _stage_copy(stage, NAMES[0], marked=False)
    for name in NAMES[1:]:
        _stage_copy(stage, name)

    got = _recover(fleet, apply=True)
    assert (stage / NAMES[0]).exists()
    assert got["retained_reasons"]["not_marked_by_the_stage"] == 1


def test_an_unanswerable_mark_retains_rather_than_deletes(
        fleet, monkeypatch) -> None:
    _queue, stage, _cas, _digest, _originals = fleet
    _populate(stage)

    def unanswerable(*_args, **_kwargs):
        raise OSError(95, "not supported")

    monkeypatch.setattr(os, "getxattr", unanswerable)
    got = _recover(fleet, apply=True)
    assert got["entries_retired"] == 0 and _intact(fleet)


# --- refusals: each must fail BEFORE anything is unlinked -------------------

@pytest.mark.parametrize("census", [
    "reader_lease.live_for", "_claimed_paths", "_claimed_source_paths"])
def test_an_unreadable_census_refuses_before_mutating(
        fleet, monkeypatch, census: str) -> None:
    """Fail closed: an unreadable census is not an absence of ownership."""

    _queue, _stage, _cas, _digest, _originals = fleet
    _populate(fleet[1])
    empty: object = {} if census.endswith("live_for") else set()
    if "." in census:
        module, name = census.split(".")
        monkeypatch.setattr(getattr(stage_release, module), name,
                            lambda *a, **k: (empty, ["unreadable"]))
    else:
        monkeypatch.setattr(stage_release, census,
                            lambda *a, **k: (empty, ["unreadable"]))

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and got["entries_retired"] == 0
    assert _intact(fleet)


def test_a_missing_original_refuses_before_mutating(fleet) -> None:
    """The staged copy must never be the last surviving input."""

    _queue, stage, _cas, _digest, originals = fleet
    _populate(stage)
    (originals / NAMES[0]).unlink()

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "last surviving input" in str(got["skipped"])
    assert got["entries_retired"] == 0
    assert all((stage / name).exists() for name in NAMES)


def test_an_original_that_resolves_into_the_stage_is_refused(
        fleet, tmp_path: Path) -> None:
    """An 'original' aliased into the stage is not a separate input."""

    queue, stage, cas, _digest, _originals = fleet
    _populate(stage)
    aliased = tmp_path / "aliased"
    aliased.mkdir()
    for name in NAMES:
        (aliased / name).symlink_to(stage / name)
    digest = _manifest_blob(cas, aliased)
    _seal(cas, HEAD, digest)
    _seal(cas, EGRESS, digest)
    _file_receipts(queue, stage, digest)
    stage_release._manifest_layout_cache.clear()

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "inside the stage root" in str(got["skipped"])
    assert all((stage / name).exists() for name in NAMES)


def test_nothing_happens_while_a_mover_could_be_writing(fleet) -> None:
    queue, stage, _cas, _digest, _originals = fleet
    _populate(stage)
    queue.publish(action_key=LIVE_MOVER, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root), worker_script="w.py",
                  tags=["dl380g10"],
                  resources={"cpu": 1, "mem_gb": 1, f"stage_gib@{TIER}": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "manifest_sha256": "a" * 64,
                             "manifest_bytes": SIZE, "tier_id": TIER,
                             "range_start_bytes": 0, "range_end_bytes": SIZE})
    assert stage_release.movers_in_flight(queue, tier_id=TIER) == {LIVE_MOVER}

    got = _recover(fleet, apply=True)
    assert got["skipped"] == "movers_in_flight"
    assert got["entries_retired"] == 0 and _intact(fleet)


def test_a_head_that_is_not_finished_is_refused(fleet) -> None:
    queue, stage, _cas, _digest, _originals = fleet
    _populate(stage)
    queue.publish(action_key=HEAD, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root), worker_script="w.py",
                  tags=["dl380g10"], resources={"cpu": 1, "mem_gb": 1})

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and "still" in str(got["skipped"])
    assert got["entries_retired"] == 0 and _intact(fleet)


@pytest.mark.parametrize("key", [HEAD, EGRESS])
def test_an_absent_receipt_is_no_evidence_at_all(fleet, key: str) -> None:
    """Absence is not terminal: it is no answer, and no answer refuses."""

    queue, stage, _cas, _digest, _originals = fleet
    _populate(stage)
    queue.move_path(key).unlink()

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and got["entries_retired"] == 0
    assert _intact(fleet)


def test_an_incomplete_receipt_is_refused(fleet) -> None:
    queue, stage, _cas, digest, _originals = fleet
    _populate(stage)
    queue.record_move(HEAD, {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": HEAD,
        "consumer_action_key": CONSUMER, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": digest,
        "range_start_bytes": 0, "range_end_bytes": SPAN,
        "entries_staged": len(NAMES), "complete": False})

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and _intact(fleet)


def test_a_sealed_request_naming_another_manifest_is_refused(fleet) -> None:
    """The receipt alone is not authority; the seal must agree with it."""

    _queue, stage, cas, _digest, _originals = fleet
    _populate(stage)
    _seal(cas, HEAD, "f" * 64)

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "sealed head request" in str(got["skipped"])
    assert _intact(fleet)


def test_an_unsealed_request_is_refused(fleet) -> None:
    _queue, stage, cas, _digest, _originals = fleet
    _populate(stage)
    (cas / "requests" / EGRESS[:2] / f"{EGRESS}.json").unlink()

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and _intact(fleet)


def test_a_receipt_naming_another_stage_root_is_refused(
        fleet, tmp_path: Path) -> None:
    queue, stage, _cas, digest, _originals = fleet
    _populate(stage)
    _file_receipts(queue, tmp_path / "elsewhere", digest)

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and _intact(fleet)


@pytest.mark.parametrize("bad_end", [0, -1, True])
def test_an_unusable_recorded_range_is_refused(fleet, bad_end) -> None:
    """Bounds must be real, nonnegative, ordered integers -- not bools."""

    queue, stage, _cas, digest, _originals = fleet
    _populate(stage)
    _file_receipts(queue, stage, digest, end=bad_end)

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and _intact(fleet)


def test_a_window_that_is_not_the_recorded_one_is_refused(fleet) -> None:
    """A manifest-wide count is not the count one range covers."""

    queue, stage, _cas, digest, _originals = fleet
    _populate(stage)
    _file_receipts(queue, stage, digest, entries=len(NAMES) + 1)

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "entries" in str(got["skipped"])
    assert _intact(fleet)


def test_an_unreadable_manifest_refuses_rather_than_scoping_to_nothing(
        fleet) -> None:
    queue, stage, cas, _digest, _originals = fleet
    _populate(stage)
    _file_receipts(queue, stage, "e" * 64)
    _seal(cas, HEAD, "e" * 64)
    _seal(cas, EGRESS, "e" * 64)
    stage_release._manifest_layout_cache.clear()

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and _intact(fleet)


def test_a_foreign_stage_root_is_refused_before_anything_else(
        fleet, tmp_path: Path) -> None:
    """Whose stage this is comes before what is on it (#628)."""

    foreign = tmp_path / "foreign"
    foreign.mkdir()
    _populate(fleet[1])
    got = _recover(fleet, apply=True, stage_root=str(foreign))
    assert got["complete"] is False
    assert got["event"] == stage_release.STAGE_ROOT_REFUSED_EVENT
    assert _intact(fleet)
