"""One window read, two destinations: the ARC, and a copy on the stage pool.

The stage is not a second reader.  It reuses the read the loop already makes
-- same ``Reader``, same ``DiskPacer``, same window arithmetic, same ARC
budget -- and writes the bytes as they arrive.  What this file pins is that
the copy is faithful, that it is named so nothing can mistake a byte range for
a whole file, and that a member of the stage pool reaches a record only under
its ``/dev/disk/by-id`` name.

That last one is not fussiness.  Device numbering on this fleet's file server
is the reverse of what the model names suggest: ``nvme0n1`` is the stage
device and ``nvme1n1`` is the root disk whose fourth partition is ``/``.  A
record that named a device number would hand an operator the wrong disk.
"""
from __future__ import annotations

import json
from pathlib import Path
import re
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, StagePool  # noqa: E402

#: Anything that looks like a kernel device name.
DEVICE_NAME = re.compile(r"\b(nvme\d+n\d+|sd[a-z]+)\b")


def test_the_window_lands_on_the_stage_byte_for_byte(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    source = fleet.mount / "row.pt"
    key = fleet.action("row", [fleet.file("row.pt", 8192)])
    source.write_bytes(bytes(range(256)) * 32)
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0))

    assert event["stage"]["state"] == "present"
    assert event["stage"]["staged_bytes"] == 8192
    assert event["stage"]["staged_entries"] == 1
    assert stage.objects() == ["row.pt.pbstage@0+8192"]
    assert (stage.mount / "row.pt.pbstage@0+8192").read_bytes() == source.read_bytes()

    record = fleet.queue.prewarm(key)
    assert record["status"] == "complete"
    assert record["stage"]["staged_bytes"] == 8192
    assert record["stage"]["staged_entries"] == 1
    assert record["stage"]["staged_through_bytes"] == 8192


def test_an_entry_is_named_by_its_byte_range_and_not_by_its_file(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A manifest entry is a range, so a stage object is a range.

    Writing a range under the file's own mirrored name would leave a short
    file that an overlay export would serve as the whole thing.  The range in
    the name is what keeps this tree a residency-map source -- the consumer
    contract #583's design names first -- rather than a lower layer that lies.
    """

    fleet = Fleet(tmp_path)
    fleet.file("big.pt", 8192)
    manifest_entry = {"path": str(fleet.mount / "big.pt"), "offset": 4096,
                      "bytes": 4096}
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
    import prewarm_loop

    key = prewarm_loop.stage_object_key(manifest_entry, str(fleet.mount))
    assert key == "big.pt.pbstage@4096+4096"


def test_a_path_outside_the_manifests_mount_prefix_is_refused() -> None:
    """The manifest is submitter-supplied, so the one rule that matters is
    that nothing it names can be written outside the stage root."""

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
    import prewarm_loop

    for path in ("/etc/shadow", "/mnt/shared/../etc/shadow", "/mnt/shared"):
        assert prewarm_loop.stage_object_key(
            {"path": path, "offset": 0, "bytes": 1}, "/mnt/shared") is None


def test_a_member_reaches_the_record_only_by_its_by_id_name(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("row.pt", 4096)])
    stage = StagePool(tmp_path, device="nvme0n1",
                      by_id_name="nvme-LT0800KEXVA_CVMD54710026800BGN")
    stage.install(monkeypatch)

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0))

    assert event["stage"]["members"] == [
        "nvme-LT0800KEXVA_CVMD54710026800BGN"]
    identity = json.dumps({k: event["stage"][k] for k in
                           ("pool", "dataset", "mountpoint", "members")})
    assert not DEVICE_NAME.search(identity), identity


def test_a_dry_run_reads_the_tier_and_writes_nothing(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Discovery is read-only, so a plan still reports the tier it would have
    used -- and creates nothing on it, not even a directory."""

    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("row.pt", 8192)])
    stage = StagePool(tmp_path)
    stage.install(monkeypatch)

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0,
                                   dry_run=True))

    assert event["stage"]["state"] == "present"
    assert event["stage"]["staged_bytes"] == 0
    assert list(stage.mount.iterdir()) == []
