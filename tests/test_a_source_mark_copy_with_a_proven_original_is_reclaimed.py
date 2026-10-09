"""Source-mark-only stage copies with proven originals are reclaimed (#1636).

A staged range names its own original, so the verb needs no receipt.
The dry run lists each copy with its proof. Apply moves only
digest-equal pairs to quarantine on another device. Referenced
copies never move. Restore round-trips.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shlex
import sys
import subprocess
import tempfile

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb, pool, reader_lease, residency_map, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_reclaim  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from stage_move import stage_relative  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
KIND = "stage_gib"
SIZE = 4096
NAMES = ["alpha.bin", "beta.bin"]


def _staged(stage: Path, mount: Path, name: str) -> Path:
    return stage / stage_relative(f"{mount}/{name}", 0, SIZE,
                                  mount_prefix=str(mount))


def _stage_copy(stage: Path, mount: Path, name: str,
                payload: bytes) -> Path:
    path = _staged(stage, mount, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR, b"0:0:0")
    return path


@pytest.fixture()
def fleet(tmp_path: Path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    mount = tmp_path / "originals"
    mount.mkdir()
    good = hashlib.sha256(b"g" * SIZE).digest() * (SIZE // 32)
    for name in NAMES:
        (mount / name).write_bytes(good)
        _stage_copy(stage, mount, name, good)
    with tempfile.TemporaryDirectory(prefix="pb-1636-", dir="/dev/shm") as root:
        quarantine = Path(root)
        assert stage.stat().st_dev != quarantine.stat().st_dev
        yield queue, stage, mount, quarantine, good


def _reclaim(fleet, **kwargs):
    queue, stage, mount, quarantine, _good = fleet
    params = {"tier_id": TIER, "stage_root": str(stage),
              "mount_prefix": str(mount),
              "quarantine_root": str(quarantine),
              "run_id": "run-1"}
    params.update(kwargs)
    return stage_reclaim.reclaim_source_marks(queue, **params)


def _reference_action(fleet, *, tier=None, claim=False, command=None):
    queue, stage, mount, quarantine, good = fleet
    cas = pb.PrismaBuildCAS(queue.root.parent / "cas")
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {}, "annotations": {},
        "mount_prefix": str(mount), "entry_count": 1, "total_bytes": SIZE,
        "entries": [{"path": str(mount / NAMES[0]), "offset": 0,
                     "bytes": SIZE, "sha256": None}]}
    raw = json.dumps(manifest, sort_keys=True).encode()
    digest = hashlib.sha256(raw).hexdigest()
    blob = cas.blob_path(digest)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(raw)
    checkout = stage.parent / "request-code"
    checkout.mkdir(exist_ok=True)
    (checkout / "task.py").write_text("# fixture task\n")
    manifest_input = {"id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                      "sha256": digest, "bytes": len(raw)}
    demand = {"cpu": 1, "mem_gb": 1}
    residency = None
    if tier is not None:
        kind = storage_tiers.capacity_kind_of(tier)
        demand[f"{kind}@{tier}"] = 1
        queue.mint_tier_capacity(tier, {kind: 2})
        residency = {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": tier,
                     "manifest_sha256": digest, "manifest_bytes": SIZE,
                     "range_start_bytes": 0, "range_end_bytes": SIZE}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/usr/bin/env", "python3", "task.py"], "working_directory": ".",
                 "result_path": "result.log"},
        "inputs": [manifest_input], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"cwd": ".", "command": command or ["task.py"],
                   "demand": demand, "placement": {"required_tags": ["x86"]},
                   "data_manifest": {"input": manifest_input,
                                     "mount_prefix": str(mount),
                                     "entry_count": 1, "total_bytes": SIZE}},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None}})
    cas.publish_action_request(action)
    key = action["action_key"]
    queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                  worker_script="worker.py", tags=["x86"], resources=demand,
                  residency=residency)
    if claim:
        claimed = queue.claim(tags=["x86"], capacity={"cpu": 2, "mem_gb": 2})
        assert claimed is not None and claimed["action_key"] == key
    return key, digest


def test_dry_run_lists_proven_pairs_and_changes_nothing(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    before = {name: _staged(stage, mount, name).read_bytes()
              for name in NAMES}
    before_xattr = {
        name: os.getxattr(_staged(stage, mount, name),
                          prewarm_loop.STAGE_SOURCE_XATTR)
        for name in NAMES}

    got = _reclaim(fleet)

    assert got["complete"] is True and got["applied"] is False
    assert got["entries_paired"] == len(NAMES)
    assert got["bytes_paired"] == SIZE * len(NAMES)
    assert "entries_moved" not in got
    for row in got["entries"]:
        assert row["status"] == "paired"
        assert row["original_path"] == str(fleet[2] / row["stage_rel"][:-len(
            ".pbrange/0-4096")])
        assert row["sha256"] == hashlib.sha256(_good).hexdigest()
        assert row["proof_receipt"]["sha256"] == row["sha256"]
        assert row["proof_receipt_sha256"] == pb.canonical_sha256(row["proof_receipt"])
    for name in NAMES:
        assert _staged(stage, mount, name).read_bytes() == before[name]
        assert os.getxattr(_staged(stage, mount, name),
                           prewarm_loop.STAGE_SOURCE_XATTR) == before_xattr[
            name]


def test_apply_moves_only_digest_equal_pairs_to_quarantine(fleet) -> None:
    queue, stage, mount, quarantine, good = fleet
    other = hashlib.sha256(b"x" * SIZE).digest() * (SIZE // 32)
    (mount / "gamma.bin").write_bytes(other)
    _stage_copy(stage, mount, "gamma.bin", good)

    got = _reclaim(fleet, apply=True, run_id="run-apply")

    assert got["complete"] is True
    assert got["entries_moved"] == len(NAMES)
    assert got["bytes_moved"] == SIZE * len(NAMES)
    assert got["refused_reasons"] == {"digest_differs": 1}
    for name in NAMES:
        assert not _staged(stage, mount, name).exists()
        assert (quarantine / "run-apply"
                / _staged(stage, mount, name).relative_to(stage)).exists()
    assert _staged(stage, mount, "gamma.bin").exists()
    manifest = json.loads(
        (quarantine / "run-apply" / "manifest.json").read_text())
    assert manifest["schema"] == stage_reclaim.MANIFEST_SCHEMA_V1
    assert len(manifest["entries"]) == len(NAMES)


def test_a_crash_between_copy_and_removal_loses_no_data(
        fleet, monkeypatch) -> None:
    queue, stage, mount, quarantine, good = fleet
    target = _staged(stage, mount, NAMES[0])
    copied: list[str] = []
    real_copy = stage_reclaim._copy_to_quarantine
    real_unlink = os.unlink

    def _record(source, dest, size, **kwargs):
        got = real_copy(source, dest, size, **kwargs)
        if got is not None:
            copied.append(str(source))
        return got

    def _crash(path, *args, **kwargs):
        if os.fspath(path) == str(target):
            raise OSError("killed between copy and removal")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(stage_reclaim, "_copy_to_quarantine", _record)
    monkeypatch.setattr(os, "unlink", _crash)

    got = _reclaim(fleet, apply=True, run_id="run-crash")

    # The stage copy is unlinked only after its quarantine copy
    # verifies, so the crash loses nothing: the copy verified, the
    # stage copy stands with its bytes, and the journal names the
    # move the manifest does not yet claim.
    assert str(target) in copied
    assert got["entries_moved"] == len(NAMES) - 1
    assert target.read_bytes() == good
    run_dir = quarantine / "run-crash"
    journaled = stage_reclaim._read_journal(run_dir)
    assert sorted(one["stage_rel"] for one in journaled) == sorted(
        str(_staged(stage, mount, name).relative_to(stage))
        for name in NAMES)
    manifest = stage_reclaim.read_manifest(run_dir)
    assert len(manifest["entries"]) == len(NAMES) - 1
    assert any(str(target.relative_to(stage)) in str(error)
               for error in got["errors"])
    # The manifest names only the completed move, and it restores.
    revived = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-crash")
    assert revived["entries_restored"] == len(NAMES) - 1


def test_a_crash_before_the_manifest_still_restores(fleet) -> None:
    queue, stage, mount, quarantine, _good = fleet
    run_dir = quarantine / "run-journal"
    run_dir.mkdir(parents=True)
    target = _staged(stage, mount, NAMES[0])
    dest = run_dir / target.relative_to(stage)
    sha = stage_reclaim._copy_to_quarantine(target, dest, SIZE)
    assert sha is not None
    captured = stage_reclaim._xattrs_and_times(target)
    assert captured is not None
    xattrs, times = captured
    stage_reclaim._append_journal(run_dir, {
        "stage_path": str(target),
        "stage_rel": str(target.relative_to(stage)),
        "original_path": str(mount / NAMES[0]),
        "offset": 0, "size": SIZE, "sha256": sha,
        "xattrs": xattrs, "times": times,
        "quarantine_path": str(dest)})
    target.unlink()

    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-journal")

    assert restored["complete"] is True
    assert restored["entries_restored"] == 1
    assert target.read_bytes() == dest.read_bytes()


def test_unpaired_copies_never_move(fleet) -> None:
    queue, stage, mount, _quarantine, good = fleet
    (mount / NAMES[0]).unlink()
    short = mount / NAMES[1]
    short.write_bytes(b"s" * (SIZE - 1))

    got = _reclaim(fleet, apply=True, run_id="run-unpaired")

    assert got["entries_moved"] == 0
    assert got["refused_reasons"] == {"original_missing": 1,
                                      "original_short": 1}
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_a_produced_output_copy_never_moves(fleet) -> None:
    queue, stage, mount, _quarantine, good = fleet
    lane = stage / "produced-output" / ("a" * 64)
    staged = lane / (NAMES[0] + ".pbrange") / f"0-{SIZE}"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(good)
    os.setxattr(staged, prewarm_loop.STAGE_SOURCE_XATTR, b"0:0:0")

    got = _reclaim(fleet, apply=True, run_id="run-lane")

    assert staged.exists()
    assert got["refused_reasons"].get("produced_output_lane") == 1


def test_a_fragment_owner_never_moves(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    consumer, mover = "c" * 64, "d" * 64
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "e" * 64,
        "entries": {
            residency_map.residency_map_key(
                str(_staged(stage, mount, NAMES[0])), 0): {
                "stage_path": str(_staged(stage, mount, NAMES[0])),
                "bytes": SIZE, "sha256": "f" * 64, "offset": 0}}})

    got = _reclaim(fleet, apply=True, run_id="run-fragment")

    assert got["entries_moved"] == 1
    assert got["entries_paired"] == 1
    assert got["refused_reasons"] == {"attributed": 1}
    assert _staged(stage, mount, NAMES[0]).exists()
    assert not _staged(stage, mount, NAMES[1]).exists()


def test_a_pin_owner_never_moves(fleet) -> None:
    queue, stage, mount, _quarantine, good = fleet
    consumer, mover = "c" * 64, "d" * 64
    root = queue.root / pool.RESIDENCY
    target = _staged(stage, mount, NAMES[0])
    map_key = residency_map.residency_map_key(str(mount / NAMES[0]), 0)
    entry = {"stage_path": str(target), "bytes": SIZE, "offset": 0,
             "sha256": hashlib.sha256(good).hexdigest()}
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": "e" * 64,
        "entries": {map_key: entry}})
    reader_lease.write_material(
        root, consumer_action_key=consumer, mover_action_key=mover, tier_id=TIER,
        stage_root=str(stage), manifest_sha256="e" * 64,
        generation=reader_lease.mint_generation(),
        entries={map_key: {**entry, "file_id": reader_lease.stat_identity(str(target))}})
    pin = reader_lease.acquire(
        queue, consumer_action_key=consumer, attempt={"nonce": "n1", "scope_id": "s1"},
        tier_id=TIER, epoch="", span={"start_bytes": 0, "end_bytes": SIZE},
        holder={"host": "test-host", "pid": 4242}, acquire_token="test-pin",
        covers=[{"mover_action_key": mover, "manifest_sha256": "e" * 64}])
    assert pin["ok"]
    residency_map.fragment_path(root, consumer, mover).unlink()
    reader_lease.material_path(root, consumer, mover).unlink()

    got = _reclaim(fleet, apply=True, run_id="run-pin")

    assert got["entries_moved"] == 1
    assert _staged(stage, mount, NAMES[0]).exists()


def test_an_in_flight_claim_never_moves(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    _reference_action(fleet, tier=TIER, claim=True)

    got = _reclaim(fleet, apply=True, run_id="run-claim")

    assert got.get("entries_moved", 0) == 0
    assert got["skipped"] == "movers_in_flight"
    assert _staged(stage, mount, NAMES[0]).exists()


def test_a_promotion_handoff_never_moves(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    _reference_action(fleet, tier="ram:test-host", claim=True, command=[
        "ram_promote.py", "--source-stage-root", str(stage),
        "--range-start-bytes", "0", "--range-end-bytes", str(SIZE)])

    got = _reclaim(fleet, apply=True, run_id="run-handoff")

    assert got["entries_moved"] == 1
    assert _staged(stage, mount, NAMES[0]).exists()


def test_a_mover_in_flight_refuses_the_pass(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    mover = "9" * 64
    queue.publish(action_key=mover, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root),
                  worker_script="w.py", tags=["dl380g10"],
                  resources={"cpu": 1, "mem_gb": 1,
                             f"stage_gib@{TIER}": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "manifest_sha256": "a" * 64,
                             "manifest_bytes": SIZE, "tier_id": TIER,
                             "range_start_bytes": 0,
                             "range_end_bytes": SIZE})

    got = _reclaim(fleet, apply=True, run_id="run-flight")

    assert got["skipped"] == "movers_in_flight"
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_an_unreadable_reference_record_refuses_the_pass(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet
    path = residency_map.fragment_path(
        queue.root / pool.RESIDENCY, "c" * 64, "d" * 64)
    path.parent.mkdir(parents=True)
    path.write_text("{")

    got = _reclaim(fleet)

    assert got["complete"] is False
    assert got["skipped"] == "attribution_unreadable"
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_restore_round_trips_bytes_marks_and_times(fleet) -> None:
    queue, stage, mount, quarantine, _good = fleet
    before = {name: (
        _staged(stage, mount, name).read_bytes(),
        os.getxattr(_staged(stage, mount, name),
                    prewarm_loop.STAGE_SOURCE_XATTR),
        os.stat(_staged(stage, mount, name))) for name in NAMES}

    got = _reclaim(fleet, apply=True, run_id="run-restore")
    assert got["entries_moved"] == len(NAMES)
    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-restore")

    assert restored["complete"] is True
    assert restored["entries_restored"] == len(NAMES)
    for name in NAMES:
        path = _staged(stage, mount, name)
        # Stat first: the test's own read moves atime past it.
        live = os.stat(path)
        assert path.read_bytes() == before[name][0]
        assert os.getxattr(path,
                            prewarm_loop.STAGE_SOURCE_XATTR) == before[
            name][1]
        assert live.st_mtime_ns == before[name][2].st_mtime_ns
        assert live.st_atime_ns == before[name][2].st_atime_ns
    again = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-restore")
    assert again["entries_restored"] == 0
    assert again["entries_skipped_identical"] == len(NAMES)


def test_restore_refuses_a_conflicting_destination(fleet) -> None:
    queue, stage, mount, quarantine, _good = fleet
    got = _reclaim(fleet, apply=True, run_id="run-conflict")
    assert got["entries_moved"] == len(NAMES)
    _staged(stage, mount, NAMES[0]).parent.mkdir(parents=True,
                                                 exist_ok=True)
    _staged(stage, mount, NAMES[0]).write_bytes(b"z" * SIZE)

    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage),
        quarantine_root=str(quarantine), run_id="run-conflict")

    assert restored["complete"] is False
    assert restored["entries_restored"] == 1


def test_next_mint_exposes_the_reclaimed_room(fleet, monkeypatch) -> None:
    queue, stage, mount, quarantine, good = fleet
    retained_key, waiting_key = "b" * 64, "c" * 64
    retained = stage / "owned.bin"
    retained.write_bytes(b"held")
    os.setxattr(retained, prewarm_loop.STAGE_SOURCE_XATTR, b"0:0:0")
    os.setxattr(retained, stage_reclaim.STAGE_MOVER_XATTR, retained_key.encode())
    queue.mint_tier_capacity(TIER, {KIND: 3})
    assert queue.tier_ledger(TIER).acquire(retained_key, {KIND: 1})
    queue.record_move(retained_key, {
        "tier_id": TIER, "stage_root": str(stage), "complete": True,
        "entries": [{"stage_path": str(retained), "bytes": 4}],
    })

    def available():
        occupied = sum(path.stat().st_size for path in stage.rglob("*")
                       if path.is_file() and path.name != stage_release.STAGE_ROOT_MARKER)
        return 2 * storage_tiers.GIB + 4 - occupied

    monkeypatch.setattr(storage_tiers, "stage_dataset", lambda pool: {
        "available_bytes": available()})
    reader = tier_loop._supply_reader_for(
        {"tier": "stage", "pool": "fake-stage"}, TIER, fallback_tokens=0)

    def mint():
        return tier_loop.mint_stage_supply(
            queue, tier_id=TIER, kind=KIND, writable_reader=reader)

    before = mint()
    assert before["landed"] == 1 and before["supply"] == 2

    def publish():
        queue.publish(
            action_key=waiting_key, cas_root=str(queue.root.parent / "cas"),
            checkout_root=str(stage.parent), worker_script="worker.py",
            tags=["x86"], resources={"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": 2},
            residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                       "manifest_sha256": "a" * 64, "manifest_bytes": SIZE,
                       "range_start_bytes": 0, "range_end_bytes": SIZE})

    publish()
    assert queue.claim(tags=["x86"], capacity={"cpu": 2, "mem_gb": 2}) is None
    assert queue.tier_ledger(TIER).holder_tokens(waiting_key) == {}
    queue.withdraw(waiting_key, reason="fixture pauses the mover", by="test")
    got = _reclaim(fleet, apply=True, run_id="run-mint")
    assert got["complete"] is True
    assert got["bytes_moved"] == SIZE * len(NAMES)
    assert retained.read_bytes() == b"held"
    after = mint()
    assert after["landed"] == 1 and after["supply"] == 3
    assert after["writable"] == before["writable"] + 1
    assert queue.tier_ledger(TIER).holder_tokens(retained_key) == {KIND: 1}
    publish()
    admitted = queue.claim(tags=["x86"], capacity={"cpu": 2, "mem_gb": 2})
    assert admitted is not None and admitted["action_key"] == waiting_key
    assert queue.tier_ledger(TIER).held() == {KIND: 3}
    assert queue.tier_ledger(TIER).holder_tokens(waiting_key) == {KIND: 2}
    assert 4 <= queue.tier_ledger(TIER).held()[KIND] * storage_tiers.GIB


def test_quarantine_on_the_stage_device_is_refused(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet

    got = _reclaim(fleet, apply=True, quarantine_root=str(stage / "q"))

    assert got["complete"] is False
    assert "quarantine" in str(got["skipped"])
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_a_run_id_outside_its_directory_is_refused(fleet) -> None:
    queue, stage, mount, _quarantine, _good = fleet

    got = _reclaim(fleet, apply=True, run_id="../escape")

    assert got["complete"] is False
    assert "run_id" in str(got["skipped"])
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_a_ranged_name_inside_a_ranged_name_still_pairs(fleet) -> None:
    queue, stage, mount, quarantine, good = fleet
    nested = "nested.pbrange/0-5"
    (mount / "nested.pbrange").mkdir(parents=True, exist_ok=True)
    (mount / nested).write_bytes(good)
    staged = stage / stage_relative(
        f"{mount}/{nested}", 0, SIZE, mount_prefix=str(mount))
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(good)
    os.setxattr(staged, prewarm_loop.STAGE_SOURCE_XATTR, b"0:0:0")

    got = _reclaim(fleet)

    assert got["complete"] is True
    assert got["entries_paired"] == len(NAMES) + 1
    nested_row = [row for row in got["entries"]
                  if row["stage_rel"] == str(
                      staged.relative_to(stage))]
    assert len(nested_row) == 1
    assert nested_row[0]["original_path"] == str(mount / nested)


def test_an_apply_reuses_a_dry_run_memo(fleet, monkeypatch) -> None:
    queue, stage, mount, quarantine, _good = fleet
    memo_path = quarantine / "dry-memo.json"

    dry = _reclaim(fleet, run_id="memo-run", memo_out=str(memo_path))

    assert dry["complete"] is True
    assert dry["entries_paired"] == len(NAMES)
    assert dry["memo_hits"] == 0
    assert dry["memo"] == str(memo_path)
    assert memo_path.exists()
    real_prove = stage_reclaim.prove_pair
    calls: list[str] = []

    def _count(stage_path, original, offset, size):
        calls.append(str(stage_path))
        return real_prove(stage_path, original, offset, size)

    monkeypatch.setattr(stage_reclaim, "prove_pair", _count)

    got = _reclaim(fleet, apply=True, run_id="run-memo",
                   memo_path=memo_path)

    assert got["complete"] is True
    assert got["entries_moved"] == len(NAMES)
    assert got["memo_hits"] == len(NAMES)
    assert calls == []
    assert (quarantine / "run-memo" / "memo.json").exists()


def test_a_memo_entry_for_a_changed_file_rehashes(fleet) -> None:
    queue, stage, mount, quarantine, _good = fleet
    memo_path = quarantine / "stale-memo.json"
    dry = _reclaim(fleet, run_id="memo-stale", memo_out=str(memo_path))
    assert dry["entries_paired"] == len(NAMES)
    changed = mount / NAMES[0]
    changed.write_bytes(b"n" * SIZE)

    got = _reclaim(fleet, apply=True, run_id="memo-stale-2", memo_path=memo_path)

    assert got["complete"] is True
    assert got["memo_hits"] == len(NAMES) - 1
    assert got["refused_reasons"] == {"digest_differs": 1}
    assert got["entries_moved"] == 1
    assert _staged(stage, mount, NAMES[0]).exists()


def test_an_unreadable_ledger_refuses_the_pass(fleet, monkeypatch) -> None:
    queue, stage, mount, _quarantine, _good = fleet

    def _refuse(_tier_id):
        raise pool.PoolContractError("ledger gone")

    monkeypatch.setattr(queue, "tier_ledger", _refuse)

    got = _reclaim(fleet)

    assert got["complete"] is False
    assert "ledger_unreadable" in str(got["skipped"])
    for name in NAMES:
        assert _staged(stage, mount, name).exists()


def test_original_change_after_quarantine_copy_retains_stage(fleet, monkeypatch):
    queue, stage, mount, quarantine, good = fleet
    target = _staged(stage, mount, NAMES[0])
    real_copy = stage_reclaim._copy_to_quarantine

    def change_original(source, dest, size):
        digest = real_copy(source, dest, size)
        if source == target:
            (mount / NAMES[0]).write_bytes(b"changed" * SIZE)
        return digest

    monkeypatch.setattr(stage_reclaim, "_copy_to_quarantine", change_original)
    got = _reclaim(fleet, apply=True, run_id="late-source-change")
    assert target.exists()
    assert target.read_bytes() == good
    assert got["entries_moved"] == 1
    assert got["complete"] is False


def test_dry_run_preserves_old_access_times(fleet):
    queue, stage, mount, quarantine, good = fleet
    target = _staged(stage, mount, NAMES[0])
    original = mount / NAMES[0]
    times = (1_000_000_000, 2_000_000_000)
    for path in (target, original):
        os.utime(path, ns=times)
    got = _reclaim(fleet)
    assert got["entries_paired"] == len(NAMES)
    for path in (target, original):
        info = path.stat()
        assert (info.st_atime_ns, info.st_mtime_ns) == times


def test_a_receipt_without_a_fragment_keeps_its_copy(fleet):
    queue, stage, mount, quarantine, good = fleet
    target = _staged(stage, mount, NAMES[0])
    queue.record_move("8" * 64, {
        "tier_id": TIER, "stage_root": str(stage), "complete": True,
        "entries": [{"stage_path": str(target), "bytes": SIZE}],
    })
    got = _reclaim(fleet, apply=True, run_id="receipt-only")
    assert target.exists()
    assert got["entries_moved"] == 1


def test_an_unreadable_ready_record_refuses_all_moves(fleet):
    queue, stage, mount, quarantine, good = fleet
    queue.item_path(pool.READY, "7" * 64).write_text("{")
    got = _reclaim(fleet, apply=True, run_id="bad-ready")
    assert got["complete"] is False
    assert all(_staged(stage, mount, name).exists() for name in NAMES)


def test_restore_uses_journal_entries_absent_from_final_manifest(fleet):
    queue, stage, mount, quarantine, good = fleet
    got = _reclaim(fleet, apply=True, run_id="partial-manifest")
    assert got["entries_moved"] == len(NAMES)
    run = quarantine / "partial-manifest"
    entries = stage_reclaim.read_manifest(run)["entries"]
    stage_reclaim.write_manifest(run, entries[:1])
    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage), quarantine_root=str(quarantine),
        run_id=run.name)
    assert restored["complete"] is True
    assert restored["entries_restored"] == len(NAMES)
    assert all(_staged(stage, mount, name).read_bytes() == good
               for name in NAMES)


def test_restore_refuses_an_identical_symlink(fleet):
    queue, stage, mount, quarantine, good = fleet
    got = _reclaim(fleet, apply=True, run_id="restore-symlink")
    assert got["entries_moved"] == len(NAMES)
    target = _staged(stage, mount, NAMES[0])
    target.parent.mkdir(parents=True)
    target.symlink_to(mount / NAMES[0])
    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage), quarantine_root=str(quarantine),
        run_id="restore-symlink")
    assert restored["complete"] is False
    assert target.is_symlink()
    assert restored["entries_skipped_identical"] == 0


@pytest.mark.parametrize("claim", [False, True], ids=["ready", "claimed"])
def test_a_consumer_without_a_fragment_keeps_its_input(fleet, claim):
    queue, stage, mount, quarantine, good = fleet
    _reference_action(fleet, claim=claim)
    got = _reclaim(fleet, apply=True, run_id="consumer-only")
    assert got["complete"] is True
    assert got["entries_moved"] == 1
    assert _staged(stage, mount, NAMES[0]).read_bytes() == good
    assert not _staged(stage, mount, NAMES[1]).exists()


def test_a_range_receipt_without_a_fragment_keeps_its_extent(fleet):
    queue, stage, mount, quarantine, good = fleet
    key, digest = _reference_action(fleet)
    queue.item_path(pool.READY, key).unlink()
    queue.record_move(key, {
        "tier_id": TIER, "stage_root": str(stage), "complete": True,
        "manifest_sha256": digest, "range_start_bytes": 0,
        "range_end_bytes": SIZE})
    got = _reclaim(fleet, apply=True, run_id="range-receipt")
    assert got["entries_moved"] == 1
    assert _staged(stage, mount, NAMES[0]).read_bytes() == good


def test_a_late_fragment_blocks_removal(fleet, monkeypatch):
    queue, stage, mount, quarantine, good = fleet
    target = _staged(stage, mount, NAMES[0])
    real_copy = stage_reclaim._copy_to_quarantine

    def add_fragment(source, dest, size, **kwargs):
        sha = real_copy(source, dest, size, **kwargs)
        if source == target:
            residency_map.write_fragment(queue.root / pool.RESIDENCY, {
                "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                "consumer_action_key": "c" * 64, "mover_action_key": "d" * 64,
                "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": "e" * 64,
                "entries": {residency_map.residency_map_key(str(mount / NAMES[0]), 0): {
                    "stage_path": str(target), "bytes": SIZE, "sha256": sha, "offset": 0}}})
        return sha

    monkeypatch.setattr(stage_reclaim, "_copy_to_quarantine", add_fragment)
    got = _reclaim(fleet, apply=True, run_id="late-fragment")
    assert got["complete"] is False
    assert got["entries_moved"] == 1
    assert target.read_bytes() == good


def test_a_forged_memo_cannot_authorize_a_differing_original(fleet):
    queue, stage, mount, quarantine, good = fleet
    (mount / NAMES[0]).write_bytes(b"d" * SIZE)
    memo = quarantine / "forged.json"
    target = _staged(stage, mount, NAMES[0])
    stage_reclaim.write_memo_file(memo, [{
        "stage_rel": str(target.relative_to(stage)),
        "sha256": hashlib.sha256(good).hexdigest(),
        "stage_identity": stage_reclaim._file_identity(target.stat()),
        "original_identity": stage_reclaim._file_identity((mount / NAMES[0]).stat()),
        "original_path": str(mount / NAMES[0]), "offset": 0, "size": SIZE}])
    got = _reclaim(fleet, apply=True, run_id="forged-memo", memo_path=memo)
    assert got["complete"] is False
    assert got["entries_moved"] == 1
    assert target.read_bytes() == good


def test_apply_budget_includes_copy_verification_and_final_source_read(fleet):
    queue, stage, mount, quarantine, good = fleet
    got = _reclaim(fleet, apply=True, run_id="read-budget",
                   max_read_gib=(5 * SIZE) / storage_tiers.GIB)
    assert got["complete"] is True
    assert got["entries_moved"] == 1
    assert got["refused_reasons"] == {"read_budget_exceeded": 1}
    assert got["bytes_hashed"] + got["apply_read_reserved_bytes"] == 5 * SIZE
    assert _staged(stage, mount, NAMES[1]).read_bytes() == good


def test_a_nonzero_original_extent_is_proven(fleet):
    queue, stage, mount, quarantine, good = fleet
    (mount / "offset.bin").write_bytes(b"prefix" + good + b"suffix")
    target = stage / stage_relative(
        str(mount / "offset.bin"), 6, SIZE, mount_prefix=str(mount))
    target.parent.mkdir(parents=True)
    target.write_bytes(good)
    os.setxattr(target, prewarm_loop.STAGE_SOURCE_XATTR, b"0:0:0")
    got = _reclaim(fleet, apply=True, run_id="nonzero")
    row = next(row for row in got["entries"] if row["stage_path"] == str(target))
    assert row["offset"] == 6 and row["sha256"] == hashlib.sha256(good).hexdigest()
    assert not target.exists()
    assert (quarantine / "nonzero" / target.relative_to(stage)).read_bytes() == good


def test_restore_metadata_failure_leaves_no_published_copy(fleet, monkeypatch):
    queue, stage, mount, quarantine, good = fleet
    got = _reclaim(fleet, apply=True, run_id="metadata-failure")
    assert got["entries_moved"] == len(NAMES)
    real_setxattr = os.setxattr

    def refuse_stage(path, *args, **kwargs):
        if stage in Path(path).parents:
            raise OSError("xattrs unavailable")
        return real_setxattr(path, *args, **kwargs)

    monkeypatch.setattr(os, "setxattr", refuse_stage)
    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage), quarantine_root=str(quarantine),
        run_id="metadata-failure")
    assert restored["complete"] is False
    assert restored["entries_restored"] == 0
    assert all(not _staged(stage, mount, name).exists() for name in NAMES)
    assert all((quarantine / "metadata-failure" /
                _staged(stage, mount, name).relative_to(stage)).read_bytes() == good
               for name in NAMES)


@pytest.mark.parametrize("checkpoint", ["journal", "manifest"])
def test_process_exit_preserves_every_copy_and_restore_record(fleet, checkpoint):
    queue, stage, mount, quarantine, good = fleet
    code = """
import os, sys
from pathlib import Path
from prismabuild import pool
import stage_reclaim
queue = pool.PoolQueue(Path(sys.argv[1]))
checkpoint = sys.argv[5]
if checkpoint == "journal":
    append = stage_reclaim._append_journal
    def stop(*args):
        append(*args)
        os._exit(77)
    stage_reclaim._append_journal = stop
else:
    stage_reclaim.write_manifest = lambda *args: os._exit(77)
stage_reclaim.reclaim_source_marks(
    queue, tier_id=sys.argv[6], stage_root=sys.argv[2], mount_prefix=sys.argv[3],
    quarantine_root=sys.argv[4], run_id="process-exit", apply=True)
"""
    repo = Path(__file__).resolve().parents[1]
    env = {**os.environ, "PYTHONPATH": f"{repo / 'src'}:{repo / 'tools/fleet'}"}
    child = subprocess.run(
        [sys.executable, "-c", code, str(queue.root), str(stage), str(mount),
         str(quarantine), checkpoint, TIER], env=env, timeout=30)
    assert child.returncode == 77
    for name in NAMES:
        target = _staged(stage, mount, name)
        held = quarantine / "process-exit" / target.relative_to(stage)
        assert (target.exists() and target.read_bytes() == good) or (
            held.exists() and held.read_bytes() == good)
    restored = stage_reclaim.restore_run(
        queue, stage_root=str(stage), quarantine_root=str(quarantine),
        run_id="process-exit")
    assert restored["complete"] is True
    assert restored["entries_restored"] == (0 if checkpoint == "journal" else len(NAMES))
    assert all(_staged(stage, mount, name).read_bytes() == good for name in NAMES)


def test_a_stage_parent_substitution_cannot_move_a_referenced_copy(fleet, monkeypatch):
    queue, stage, mount, quarantine, good = fleet
    target = _staged(stage, mount, NAMES[0])
    relocated = stage / "relocated"
    real_copy = stage_reclaim._copy_to_quarantine

    def substitute_parent(source, dest, size, **kwargs):
        sha = real_copy(source, dest, size, **kwargs)
        if source == target:
            target.parent.rename(relocated)
            target.parent.symlink_to(relocated, target_is_directory=True)
            residency_map.write_fragment(queue.root / pool.RESIDENCY, {
                "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                "consumer_action_key": "c" * 64, "mover_action_key": "d" * 64,
                "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": "e" * 64,
                "entries": {residency_map.residency_map_key(str(mount / NAMES[0]), 0): {
                    "stage_path": str(relocated / target.name), "bytes": SIZE,
                    "sha256": sha, "offset": 0}}})
        return sha

    monkeypatch.setattr(stage_reclaim, "_copy_to_quarantine", substitute_parent)
    got = _reclaim(fleet, apply=True, run_id="parent-substitution")
    assert got["complete"] is False
    assert got["entries_moved"] == 1
    assert (relocated / target.name).read_bytes() == good


CLI_RECLAIM_KEY = "a" * 64
CLI_RESTORE_KEY = "b" * 64


@pytest.fixture()
def own_key_unset(monkeypatch):
    """A direct run names its key by flag, never by the shard's own."""
    monkeypatch.delenv(pb.ACTION_KEY_ENV, raising=False)


def _cli_args(fleet, *extra: str) -> list[str]:
    queue, stage, mount, _quarantine, _good = fleet
    return ["--pool-root", str(queue.root), "--tier-id", TIER,
            "--stage-root", str(stage), "--mount-prefix", str(mount), *extra]


def test_the_cli_dry_run_prints_the_receipt_in_the_owners_spelling(
        fleet, capsys, tmp_path, own_key_unset) -> None:
    queue, stage, mount, _quarantine, good = fleet
    receipt_file = tmp_path / "receipt.json"

    code = stage_reclaim.main(_cli_args(
        fleet, "--receipt", str(receipt_file), "--action-key", CLI_RECLAIM_KEY))
    printed = capsys.readouterr().out

    got = json.loads(printed)
    assert code == 0 and got["applied"] is False
    assert got["entries_paired"] == len(NAMES)
    # One line in the digest owner's sorted spelling, not a private encoding.
    assert printed == pb.sorted_json(got) + "\n"
    assert json.loads(receipt_file.read_text()) == got
    # A dry run files no movement receipt, even when it knows its own key.
    assert queue.move_record(CLI_RECLAIM_KEY) is None
    for name in NAMES:
        assert _staged(stage, mount, name).read_bytes() == good


def test_the_cli_applies_and_its_printed_restore_command_restores(
        fleet, capsys, own_key_unset) -> None:
    queue, stage, mount, quarantine, good = fleet

    code = stage_reclaim.main(_cli_args(
        fleet, "--apply", "--run-id", "cli-run", "--action-key",
        CLI_RECLAIM_KEY, "--quarantine-root", str(quarantine)))
    applied = json.loads(capsys.readouterr().out)

    assert code == 0 and applied["entries_moved"] == len(NAMES)
    filed = queue.move_record(CLI_RECLAIM_KEY)
    assert filed is not None and filed["event"] == stage_reclaim.RECLAIM_EVENT
    for name in NAMES:
        assert not _staged(stage, mount, name).exists()
    # The advertised restore command is a real invocation of this CLI.
    command = shlex.split(applied["restore_command"])
    assert Path(command[1]).name == "stage_reclaim.py"
    code = stage_reclaim.main(command[2:] + ["--action-key", CLI_RESTORE_KEY])
    restored = json.loads(capsys.readouterr().out)
    assert code == 0 and restored["entries_restored"] == len(NAMES)
    assert queue.move_record(CLI_RESTORE_KEY)["event"] == (
        "stage-source-mark-restored")
    for name in NAMES:
        assert _staged(stage, mount, name).read_bytes() == good


def test_the_cli_refuses_an_apply_without_a_quarantine_root(
        fleet, capsys, own_key_unset) -> None:
    _queue, stage, mount, _quarantine, good = fleet

    code = stage_reclaim.main(_cli_args(fleet, "--apply"))
    got = json.loads(capsys.readouterr().out)

    assert code == 1 and got["complete"] is False
    assert got["skipped"] == "apply needs a quarantine root"
    for name in NAMES:
        assert _staged(stage, mount, name).read_bytes() == good


@pytest.mark.parametrize("flag", ["--receipt", "--memo-out"])
def test_the_cli_refuses_an_output_inside_the_stage(
        fleet, capsys, own_key_unset, flag) -> None:
    _queue, stage, _mount, _quarantine, _good = fleet

    with pytest.raises(SystemExit) as stopped:
        stage_reclaim.main(_cli_args(fleet, flag, str(stage / "out.json")))

    assert stopped.value.code == 2
    assert "outside the stage" in capsys.readouterr().err
    assert not (stage / "out.json").exists()


def test_the_cli_restore_needs_a_quarantine_root(
        fleet, capsys, own_key_unset) -> None:
    with pytest.raises(SystemExit) as stopped:
        stage_reclaim.main(_cli_args(fleet, "--restore", "cli-run"))

    assert stopped.value.code == 2
    assert "--restore needs --quarantine-root" in capsys.readouterr().err


def test_the_cli_help_explains_every_flag_and_renders(capsys) -> None:
    with pytest.raises(SystemExit) as stopped:
        stage_reclaim.main(["--help"])

    shown = capsys.readouterr().out
    assert stopped.value.code == 0
    # argparse wraps the help, so compare with the whitespace collapsed.
    assert "(default 64.0)" in " ".join(shown.split())
    for flag in ("--pool-root", "--action-key", "--tier-id", "--stage-root",
                 "--max-read-gib", "--cas-root", "--residency-root",
                 "--receipt"):
        assert flag in shown
