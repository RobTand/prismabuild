"""A staged name encodes the exact byte range it holds, so a prefix read and a
whole-file read of one source never contend for one name.

Production shape (GLM Stage A R11, 2026-09-22): a routing capture's data
manifest named each ``model-000NN-of-00120.safetensors`` exactly once, at
offset 0, for its ~45 KB safetensors header.  R11's manifest named the same
shards exactly once, at offset 0, for all 5.37 GB.  ``stage_relative`` gave
both the bare relative name, because "named once from offset zero" was taken
to mean "the whole file".  The capture's mover published 45 KB under the name
R11's movers needed for 5.37 GB; every R11 mover then found a publication
whose sidecar mentioned a different size, read it as "owned", waited out the
grace and refused ("shared staged name is published elsewhere, still unproven
after the grace").  74 shard names across 40 read phases were blocked, and the
capture's own mover was blocked the same way by an earlier withdrawn capture.
No proof can ever settle that contest: the two publications hold different
bytes, and one file cannot hold both.

A manifest entry carries ``path``, ``offset``, ``bytes`` and ``sha256`` and
no file size, so "this entry covers the whole file" is not derivable from the
sealed inputs the name is a pure function of.  The names therefore encode the
range for every entry: ``<rel>.pbrange/<offset>-<bytes>``.  Two entries share
a name exactly when they name the same bytes of the same source, which is
when sharing is correct.

These tests drive the real ``stage_move.move`` with real files on a
``tmp_path`` stage registered to a ``tmp_path`` queue -- never a real stage.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
import prismabuild.core as pb  # noqa: E402
from prismabuild import residency_map  # noqa: E402
import stage_move  # noqa: E402

TIER = base.TIER
WHOLE = 8192
PREFIX = 512
SHARD = "model-00099-of-00120.safetensors"


def _payload() -> bytes:
    return bytes((index * 31 + 7) % 251 for index in range(WHOLE))


def _manifest(tmp_path: Path, label: str, source: Path, size: int):
    """One sealed-shape manifest naming ``source`` once, from offset 0."""

    payload = source.read_bytes()[:size]
    entries = [{"path": str(source), "offset": 0, "bytes": size,
                "sha256": hashlib.sha256(payload).hexdigest()}]
    body = {"schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {},
            "annotations": {}, "mount_prefix": str(source.parent),
            "entries": entries, "entry_count": 1, "total_bytes": size}
    raw = pb._canonical_file_bytes(pb.validate_data_manifest(body))
    path = tmp_path / f"manifest-{label}.json"
    path.write_bytes(raw)
    return path, hashlib.sha256(raw).hexdigest()


def _move(queue, stage, cas, source: Path, tmp_path: Path, label: str,
          size: int) -> tuple[dict, str, str]:
    manifest, digest = _manifest(tmp_path, label, source, size)
    mover, consumer = base._key(), base._key()
    args = stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root), "--cas-root", str(cas),
        "--action-key", mover, "--consumer-action-key", consumer,
        "--tier-id", TIER, "--stage-root", str(stage),
        "--manifest", str(manifest), "--manifest-sha256", digest,
        "--range-start-bytes", "0", "--range-end-bytes", str(size),
        "--residency-root", str(queue.residency_fragment_root()),
        "--readers", "1", "--max-readers", "1", "--unpaced"])
    return stage_move.move(args, stop=threading.Event()), consumer, mover


def _staged_path(queue, consumer: str, mover: str) -> Path:
    fragment = residency_map.validate_fragment(
        __import__("json").loads(residency_map.fragment_path(
            queue.residency_fragment_root(), consumer, mover).read_bytes()))
    (entry,) = fragment["entries"].values()
    return Path(str(entry["stage_path"]))


def _short_grace(monkeypatch) -> None:
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)


def _both_publish(fleet, tmp_path, monkeypatch, order) -> None:
    queue, stage, cas = fleet
    mount = tmp_path / "sources"
    mount.mkdir()
    source = mount / SHARD
    source.write_bytes(_payload())
    _short_grace(monkeypatch)
    staged = {}
    for label in order:
        size = WHOLE if label == "whole" else PREFIX
        result, consumer, mover = _move(queue, stage, cas, source, tmp_path,
                                        label, size)
        assert result["complete"] is True, (label, result["errors"])
        assert result["errors"] == [], (label, result["errors"])
        staged[label] = _staged_path(queue, consumer, mover)
    assert staged["prefix"] != staged["whole"], (
        "a header prefix and the whole file must not share a staged name")
    assert staged["prefix"].read_bytes() == _payload()[:PREFIX]
    assert staged["whole"].read_bytes() == _payload()


def test_a_whole_file_read_publishes_after_a_prefix_read_of_the_same_file(
        fleet, tmp_path, monkeypatch):
    """The R11 order: the capture's header prefix landed first."""

    _both_publish(fleet, tmp_path, monkeypatch, ("prefix", "whole"))


def test_a_prefix_read_publishes_after_a_whole_file_read_of_the_same_file(
        fleet, tmp_path, monkeypatch):
    """The capture's order after R11: the whole shard landed first."""

    _both_publish(fleet, tmp_path, monkeypatch, ("whole", "prefix"))


def test_two_reads_of_the_same_bytes_still_share_one_staged_name(
        fleet, tmp_path, monkeypatch):
    """Sharing is kept exactly where it is correct: the same range."""

    queue, stage, cas = fleet
    mount = tmp_path / "sources"
    mount.mkdir()
    source = mount / SHARD
    source.write_bytes(_payload())
    _short_grace(monkeypatch)
    first, c1, m1 = _move(queue, stage, cas, source, tmp_path, "one", WHOLE)
    second, c2, m2 = _move(queue, stage, cas, source, tmp_path, "two", WHOLE)
    assert first["complete"] is True and second["complete"] is True
    assert _staged_path(queue, c1, m1) == _staged_path(queue, c2, m2)


def test_the_name_encodes_offset_and_size_for_every_entry():
    """Pure derivation: the same (path, offset, bytes) is the same name, and
    anything else is a different name, whatever else the manifest says."""

    name = stage_move.stage_relative
    prefix = name("/m/d/f.safetensors", 0, PREFIX, mount_prefix="/m")
    whole = name("/m/d/f.safetensors", 0, WHOLE, mount_prefix="/m")
    assert prefix == f"d/f.safetensors{stage_move.RANGE_SUFFIX}/0-{PREFIX}"
    assert whole == f"d/f.safetensors{stage_move.RANGE_SUFFIX}/0-{WHOLE}"
    assert prefix != whole
    assert name("/m/d/f.safetensors", 0, WHOLE, mount_prefix="/m") == whole


def test_a_staged_input_ignores_named_once():
    """Input side: being named once never buys a staged input the bare name.

    ``named_once`` is what the former rule read as "whole"; for a staged
    input it changes nothing, so a caller that still passes it cannot bring
    the collision back.
    """

    name = stage_move.stage_relative
    for named_once in (False, True):
        assert name("/m/d/f.safetensors", 0, PREFIX, mount_prefix="/m",
                    named_once=named_once) == (
            f"d/f.safetensors{stage_move.RANGE_SUFFIX}/0-{PREFIX}")


def test_a_produced_output_keeps_the_name_its_producer_declared():
    """Produced-output side: a declared output keeps its declared name.

    The namespace is the producing action's digest, so no other publisher's
    read derives a name inside it; a path its manifest names once from
    offset zero stays ``produced-output/<namespace>/<rel>``, and a split
    output still gets range names.
    """

    namespace = "ab" * 32
    name = stage_move.stage_relative
    assert name("/o/p1.bin", 0, WHOLE, mount_prefix="/o", namespace=namespace,
                named_once=True) == f"produced-output/{namespace}/p1.bin"
    assert name("/o/p1.bin", 0, PREFIX, mount_prefix="/o",
                namespace=namespace, named_once=False) == (
        f"produced-output/{namespace}/p1.bin{stage_move.RANGE_SUFFIX}"
        f"/0-{PREFIX}")
    assert name("/o/p1.bin", PREFIX, PREFIX, mount_prefix="/o",
                namespace=namespace, named_once=True) == (
        f"produced-output/{namespace}/p1.bin{stage_move.RANGE_SUFFIX}"
        f"/{PREFIX}-{PREFIX}")


def test_a_retired_range_whose_directory_was_pruned_is_absent(tmp_path):
    """Retiring a range prunes its then-empty ``<rel>.pbrange`` directory.

    The containment check must read that as the range being absent (so a
    stale mention can be pruned on replay), while a missing structural
    directory above it stays unknown ownership.
    """

    import stage_release

    stage = tmp_path / "stage"
    (stage / "model").mkdir(parents=True)
    relative = stage_move.stage_relative(
        "/m/model/f.safetensors", 0, WHOLE, mount_prefix="/m")
    state = stage_release._containment_state
    assert state(stage, stage / relative) == "absent"
    missing_parent = stage_move.stage_relative(
        "/m/gone/f.safetensors", 0, WHOLE, mount_prefix="/m")
    assert state(stage, stage / missing_parent) == "unknown"
