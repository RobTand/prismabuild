"""What a movement node may publish, and what it must refuse to.

A mover's output is a claim the consumer's gate will trust in preference to
the pool, so the interesting cases are all the ones where the claim would be
wrong: bytes that are not the manifest's bytes, a range that stages past what
its tokens reserved, a copy that stopped half way.  Each of those has to end
with nothing published rather than something plausible published, because the
consumer cannot tell the difference and the stage has no second opinion.

Every test here drives the real tool.  ``--unpaced`` is the only concession:
the pacer needs a ZFS pool's member devices, and refusing to run without one
is the storage role's correct behaviour, not something to fake.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prismabuild.core as pb  # noqa: E402
import prismabuild.pool as pool  # noqa: E402
import prismabuild.residency_map as rm  # noqa: E402
import stage_move  # noqa: E402

CONSUMER = "c" * 64
MOVER = "a" * 64
MANIFEST_SHA = "9" * 64


def _write(path: Path, payload: bytes) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {"path": str(path), "offset": 0, "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest()}


def _manifest(mount: Path, entries: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "test"},
        "mount_prefix": str(mount),
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(int(entry["bytes"]) for entry in entries),
        "annotations": {},
    }


def _args(tmp_path: Path, manifest: dict, *, start: int, end: int, **overrides):
    """The tool's own parsed arguments, then this case's differences.

    Built from ``stage_move.build_parser`` rather than re-listed here, because
    a hand-written copy of the defaults goes stale silently: when the pacer's
    ``--served-host`` was added to the tool, every test that drove ``move``
    began failing on an attribute the real tool always supplies, and the
    fixture was the only thing that did not have it.

    ``--unpaced`` is the one concession: the pacer needs a live ZFS pool's
    member devices, and refusing to run without one is the storage role's
    correct behaviour rather than something to fake.
    """

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    args = stage_move.build_parser().parse_args([
        "--pool-root", str(tmp_path / "queue"),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", MOVER,
        "--consumer-action-key", CONSUMER,
        "--tier-id", "prismabuild-stage:dl380g10",
        "--stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", MANIFEST_SHA,
        "--range-start-bytes", str(start),
        "--range-end-bytes", str(end),
        "--manifest", str(manifest_path),
        "--residency-root", str(tmp_path / "queue" / pool.RESIDENCY),
        "--block", str(1 << 16),
        "--readers", "2",
        "--max-readers", "2",
        "--unpaced",
    ])
    for name, value in overrides.items():
        setattr(args, name, value)
    return args


def _three_files(tmp_path: Path) -> tuple[Path, list[dict[str, object]]]:
    mount = tmp_path / "mnt"
    entries = [
        _write(mount / "shard-1.bin", b"a" * 4096),
        _write(mount / "sub" / "shard-2.bin", b"b" * 8192),
        _write(mount / "shard-3.bin", b"c" * 2048),
    ]
    return mount, entries


def test_the_range_is_copied_verified_and_named_in_a_fragment(tmp_path: Path) -> None:
    mount, entries = _three_files(tmp_path)
    manifest = _manifest(mount, entries)
    args = _args(tmp_path, manifest, start=0, end=4096 + 8192 + 2048)

    receipt = stage_move.move(args)

    assert receipt["complete"] is True
    assert receipt["bytes_staged"] == 4096 + 8192 + 2048
    assert receipt["entries_staged"] == 3
    assert receipt["errors"] == []
    assert "refusal" not in receipt
    stage = Path(args.stage_root)
    assert (stage / "shard-1.bin").read_bytes() == b"a" * 4096
    assert (stage / "sub" / "shard-2.bin").read_bytes() == b"b" * 8192
    fragments = rm.read_fragments(args.residency_root, CONSUMER)
    assert len(fragments) == 1
    assert set(fragments[0]["entries"]) == {
        rm.residency_map_key(str(mount / "shard-1.bin"), 0),
        rm.residency_map_key(str(mount / "sub" / "shard-2.bin"), 0),
        rm.residency_map_key(str(mount / "shard-3.bin"), 0),
    }


def test_only_the_declared_prefix_is_staged(tmp_path: Path) -> None:
    """A mover is one range of the read order, not the whole manifest."""

    mount, entries = _three_files(tmp_path)
    args = _args(tmp_path, _manifest(mount, entries), start=0, end=4096)

    receipt = stage_move.move(args)

    assert receipt["bytes_staged"] == 4096
    assert receipt["complete"] is True
    assert (Path(args.stage_root) / "shard-1.bin").exists()
    assert not (Path(args.stage_root) / "sub" / "shard-2.bin").exists()


def test_two_ranges_of_one_file_get_names_of_their_own(tmp_path: Path) -> None:
    """Two movers cannot both rename-publish into one name, so neither does."""

    mount = tmp_path / "mnt"
    payload = bytes(range(256)) * 16
    _write(mount / "shard.bin", payload)
    head = {"path": str(mount / "shard.bin"), "offset": 0, "bytes": 2048,
            "sha256": hashlib.sha256(payload[:2048]).hexdigest()}
    tail = {"path": str(mount / "shard.bin"), "offset": 2048, "bytes": 2048,
            "sha256": hashlib.sha256(payload[2048:]).hexdigest()}
    manifest = _manifest(mount, [head, tail])

    first = stage_move.move(_args(tmp_path, manifest, start=0, end=2048))
    second = stage_move.move(
        _args(tmp_path, manifest, start=2048, end=4096, action_key="b" * 64))

    assert first["complete"] and second["complete"]
    stage = tmp_path / "stage"
    staged = sorted(str(path.relative_to(stage))
                    for path in stage.rglob("*") if path.is_file())
    assert staged == [
        "shard.bin.pbrange/0-2048", "shard.bin.pbrange/2048-2048"]
    assert (stage / "shard.bin.pbrange" / "0-2048").read_bytes() == payload[:2048]
    assert (stage / "shard.bin.pbrange" / "2048-2048").read_bytes() == payload[2048:]
    # Two movers, two fragments, one composed map with both ranges.
    composed = rm.compose(rm.read_fragments(
        tmp_path / "queue" / pool.RESIDENCY, CONSUMER))
    assert set(composed["entries"]) == {
        rm.residency_map_key(str(mount / "shard.bin"), 0),
        rm.residency_map_key(str(mount / "shard.bin"), 2048),
    }


def test_bytes_that_are_not_the_manifests_bytes_are_not_published(
        tmp_path: Path) -> None:
    """The map would be a lie the consumer trusts in preference to the pool."""

    mount, entries = _three_files(tmp_path)
    # Mutate the driver's input, not the fixture on disk: the manifest now
    # claims a digest the file does not have, which is what a silently
    # corrupted or replaced source looks like from here.
    entries[1] = dict(entries[1], sha256="0" * 64)
    args = _args(tmp_path, _manifest(mount, entries), start=0, end=4096 + 8192)

    receipt = stage_move.move(args)

    assert receipt["complete"] is False
    assert receipt["entries_staged"] == 1
    assert any("digest mismatch" in error for error in receipt["errors"])
    assert not (Path(args.stage_root) / "sub" / "shard-2.bin").exists()
    fragments = rm.read_fragments(args.residency_root, CONSUMER)
    assert list(fragments[0]["entries"]) == [
        rm.residency_map_key(str(mount / "shard-1.bin"), 0)]


def test_a_source_that_cannot_be_read_leaves_nothing_behind(tmp_path: Path) -> None:
    mount, entries = _three_files(tmp_path)
    (mount / "shard-1.bin").unlink()
    args = _args(tmp_path, _manifest(mount, entries), start=0, end=4096)

    receipt = stage_move.move(args)

    assert receipt["complete"] is False
    assert receipt["bytes_staged"] == 0
    assert receipt["refusal"] == "residency_moved_nothing"
    stage = Path(args.stage_root)
    assert not list(stage.rglob("*.partial"))
    assert not (stage / "shard-1.bin").exists()


def test_a_range_whose_entries_exceed_it_is_refused_before_the_copy(
        tmp_path: Path) -> None:
    """The tokens bound what the tier can hold; find the overrun for free.

    ``entries_between`` takes an entry straddling the start whole, so a range
    that ends inside an entry hands this mover more bytes than it reserved.
    That is the overrun, and the refusal must come before 34 GB moves, not
    after.
    """

    mount, entries = _three_files(tmp_path)
    args = _args(tmp_path, _manifest(mount, entries), start=0, end=4096 + 4000)

    with pytest.raises(SystemExit, match="residency_overran_reservation"):
        stage_move.move(args)

    assert not Path(args.stage_root).exists() or not list(
        Path(args.stage_root).rglob("*"))
    assert rm.read_fragments(args.residency_root, CONSUMER) == []


def test_the_receipt_says_which_side_every_rate_was_measured_on(
        tmp_path: Path) -> None:
    """A file-side rate the ARC answered is not a pool measurement."""

    mount, entries = _three_files(tmp_path)
    args = _args(tmp_path, _manifest(mount, entries), start=0, end=4096)

    receipt = stage_move.move(args)

    assert "mb_per_s" not in receipt, "an unqualified rate is the thing to avoid"
    assert "mb_per_s_file_side" in receipt
    assert "mean_self_read_mb_s" in receipt["disk_pacing"]
    assert receipt["range_start_bytes"] == 0 and receipt["range_end_bytes"] == 4096
    assert receipt["range_bytes"] == 4096
    assert isinstance(receipt["proc_io"], dict)
    assert receipt["manifest_sha256"] == MANIFEST_SHA


def test_the_receipt_is_filed_where_the_tier_loop_reads_it(tmp_path: Path) -> None:
    """The fill measurement has to outlive the mover's own conclusion."""

    mount, entries = _three_files(tmp_path)
    args = _args(tmp_path, _manifest(mount, entries), start=0, end=4096)
    queue = pool.PoolQueue(Path(args.pool_root))
    queue.ensure_layout()

    receipt = stage_move.move(args)
    path = queue.record_move(MOVER, receipt)

    assert path == queue.root / pool.MOVERS / f"{MOVER}.json"
    read = queue.move_record(MOVER)
    assert read is not None
    assert read["schema"] == pool.POOL_MOVE_SCHEMA_V1
    assert read["bytes_staged"] == 4096
    # A record of another schema is not this one, however well-formed.
    pool._write_json_atomic(path, {**read, "schema": "something.else.v1"})
    assert queue.move_record(MOVER) is None


def test_a_mover_receipt_prices_the_tier_the_same_way_a_prewarm_record_does(
        tmp_path: Path) -> None:
    """One rule for the fill measurement, whichever loop produced the record."""

    import prismabuild.storage_tiers as storage_tiers

    mover = {"disk_pacing": {"mean_self_read_mb_s": 242.5},
             "mb_per_s_file_side": 1141.0}
    assert storage_tiers.fill_rate_from_records([mover]) == 242.5
    # A receipt with no pool-side attribution says nothing about the pool.
    assert storage_tiers.fill_rate_from_records(
        [{"mb_per_s_file_side": 1141.0}]) is None


def test_a_failed_fragment_publication_is_recorded_and_the_copy_goes_on(
        tmp_path: Path, monkeypatch) -> None:
    """A dead worker that reports ``errors: []`` is worse than a slow one.

    The publish ran inside the copier's lock with no ``try``: one ESTALE on
    the queue directory killed the thread holding it, then every other worker
    in turn on its next landed entry, and ``run`` joined the corpses and
    returned normally.  The receipt said ``complete: False``, ``errors: []``,
    exit 0 -- a degradation with no cause anywhere in the record.
    """

    mount, entries = _three_files(tmp_path)
    args = _args(tmp_path, _manifest(mount, entries),
                 start=0, end=4096 + 8192 + 2048)
    calls = {"n": 0}
    real = rm.write_fragment

    def flaky(root, fragment):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("ESTALE: stale file handle")
        return real(root, fragment)

    monkeypatch.setattr(rm, "write_fragment", flaky)

    receipt = stage_move.move(args)

    assert any("fragment publication" in error for error in receipt["errors"])
    # Every entry still copied: a fragment that will not write is not a reason
    # to stop staging bytes.
    assert receipt["entries_staged"] == 3
    assert receipt["bytes_staged"] == 4096 + 8192 + 2048
    stage = Path(args.stage_root)
    assert (stage / "shard-1.bin").exists() and (stage / "shard-3.bin").exists()
    # And the final publish repaired the fragment, so the map is whole.
    assert len(rm.read_fragments(args.residency_root, CONSUMER)[0]["entries"]) == 3


def test_a_second_movers_bad_copy_never_destroys_the_first_ones_good_one(
        tmp_path: Path) -> None:
    """Verify before the rename, because the rename *is* the publication.

    Two movers cover one entry whenever a consumer is re-dispatched under a
    new action key, or whenever an entry straddles two adjacent windows.  With
    the digest checked after ``os.replace``, mover B's changed bytes landed on
    mover A's good copy and were then unlinked -- leaving A's fragment, a file
    under A's own key, naming a stage path with nothing behind it.  The map
    would have vouched for bytes that are gone.
    """

    mount = tmp_path / "mnt"
    entry = _write(mount / "shard.bin", b"a" * 4096)
    manifest = _manifest(mount, [entry])

    first = stage_move.move(_args(tmp_path, manifest, start=0, end=4096))
    assert first["complete"] is True
    staged = Path(_args(tmp_path, manifest, start=0, end=4096).stage_root) / "shard.bin"
    assert staged.read_bytes() == b"a" * 4096

    # The source changes under us -- a rewritten shard, or bit rot.  That is
    # the case the manifest digest exists for.
    (mount / "shard.bin").write_bytes(b"z" * 4096)
    second = stage_move.move(
        _args(tmp_path, manifest, start=0, end=4096, action_key="b" * 64))

    assert second["complete"] is False
    assert any("digest mismatch" in error for error in second["errors"])
    # A's copy is untouched, and A's fragment still resolves to it.
    assert staged.read_bytes() == b"a" * 4096
    composed = rm.compose([rm.read_fragments(
        tmp_path / "queue" / pool.RESIDENCY, CONSUMER)[0]])
    resolved = rm.lookup(composed, str(mount / "shard.bin"), 0)
    assert resolved is not None and Path(resolved["stage_path"]).exists()


def test_the_fragment_is_written_a_bounded_number_of_times(
        tmp_path: Path, monkeypatch) -> None:
    """Once per entry is quadratic in bytes; measured, it was 75 KB/s."""

    mount = tmp_path / "mnt"
    entries = [_write(mount / f"shard-{n:03d}.bin", bytes([n % 251]) * 1024)
               for n in range(60)]
    args = _args(tmp_path, _manifest(mount, entries), start=0, end=60 * 1024)
    calls = {"n": 0}
    real = rm.write_fragment

    def counted(root, fragment):
        calls["n"] += 1
        return real(root, fragment)

    monkeypatch.setattr(rm, "write_fragment", counted)

    receipt = stage_move.move(args)

    assert receipt["entries_staged"] == 60
    # The cadence is wall-clock, so a fast window publishes once on the way
    # through and once at the end.  What must never hold again is one write
    # per entry.
    assert calls["n"] <= 4, f"{calls['n']} publications for 60 entries"


def test_the_one_off_manifest_door_refuses_what_core_refuses(
        tmp_path: Path) -> None:
    """Everything downstream assumes core already said no to these shapes."""

    mount = tmp_path / "mnt"
    entry = _write(mount / "shard.bin", b"a" * 4096)
    manifest = _manifest(mount, [dict(entry, bytes=0)])
    manifest["total_bytes"] = 0

    with pytest.raises(SystemExit):
        stage_move.move(_args(tmp_path, manifest, start=0, end=4096))


def test_two_entries_may_not_stage_as_one_file(tmp_path: Path) -> None:
    """Derived range names and declared paths share one namespace."""

    mount = tmp_path / "mnt"
    payload = b"a" * 4096
    _write(mount / "shard.bin", payload)
    # Two entries make it a split file, so its first range derives the name
    # ``shard.bin.pbrange/0-2048`` -- which is also a legal declared path, and
    # this manifest declares it.
    head = {"path": str(mount / "shard.bin"), "offset": 0, "bytes": 2048,
            "sha256": hashlib.sha256(payload[:2048]).hexdigest()}
    tail = {"path": str(mount / "shard.bin"), "offset": 2048, "bytes": 2048,
            "sha256": hashlib.sha256(payload[2048:]).hexdigest()}
    collider = _write(mount / "shard.bin.pbrange" / "0-2048", b"b" * 2048)
    manifest = _manifest(mount, [head, tail, collider])

    with pytest.raises(SystemExit, match="residency_destination_collision"):
        stage_move.move(_args(tmp_path, manifest, start=0, end=3 * 2048))

    stage = Path(tmp_path / "stage")
    assert not stage.exists() or not list(stage.rglob("*"))
