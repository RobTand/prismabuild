"""A box's explicitly configured alias survives an OS hostname rename."""

import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import supervise


def test_the_checked_in_alias_resolves_the_same_worker_shape(monkeypatch, tmp_path):
    config = Path(__file__).resolve().parents[1] / "tools/fleet/fleet_boxes.json"
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    # Both names measure the box's disk (#911); hold it still between them.
    reading = os.statvfs("/")
    monkeypatch.setattr(supervise.os, "statvfs", lambda path: reading)
    monkeypatch.setattr(supervise, "_SPOOL_VERDICTS", {})
    canonical = supervise.declared_shape("gx10-6b77", 0)
    assert supervise.declared_shape("sparklina", 0) == canonical
    assert canonical[0] == 3


def test_the_wsl_box_resolves_through_its_windows_hostname(monkeypatch, tmp_path):
    """WSL2 answers the Windows machine name, and submissions spell it wsl-gpu.

    ``socket.gethostname()`` on that box returns ``DESKTOP-P5UOGNJ`` because
    WSL2 inherits the Windows machine name, so the supervisor would refuse to
    start on a roster keyed only by the name the fleet uses for it.

    Presence is a separate question (#606): the checked-in shape stays in the
    roster while the box is declared offline, so this resolves that shape as
    an active box would.
    """

    config = _checked_in_roster(tmp_path, presence=None)
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    canonical = supervise.declared_shape("wsl-gpu", 0)
    assert supervise.declared_shape("DESKTOP-P5UOGNJ", 0) == canonical
    loops, args = canonical
    assert loops == 3
    tags = {args[i + 1] for i, arg in enumerate(args) if arg == "--tag"}
    assert {"wsl-gpu", "gfx1201", "rocm", "rdna4"} <= tags
    assert args[args.index("--mem-gb") + 1] == "16"


def test_an_offline_wsl_box_refuses_under_both_names(monkeypatch, tmp_path):
    """An absence declared on the canonical name also stops the alias."""

    config = _checked_in_roster(tmp_path, presence={
        "status": "offline", "status_reason": "out of scope",
        "status_by": "test", "status_unix": 1.0})
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    for name in ("wsl-gpu", "DESKTOP-P5UOGNJ"):
        with pytest.raises(SystemExit, match="offline"):
            supervise.declared_shape(name, 0)


def _checked_in_roster(tmp_path, *, presence):
    """The checked-in roster with wsl-gpu's presence fields replaced."""

    real = Path(__file__).resolve().parents[1] / "tools/fleet/fleet_boxes.json"
    document = json.loads(real.read_text())
    entry = document["boxes"]["wsl-gpu"]
    for field in ("status", "status_reason", "status_by", "status_unix"):
        entry.pop(field, None)
    entry.update(presence or {})
    config = tmp_path / "fleet_boxes.json"
    config.write_text(json.dumps(document))
    return config


def test_every_declared_alias_names_one_box():
    """Two boxes claiming one alias is the shape ``_config`` refuses."""

    config = Path(__file__).resolve().parents[1] / "tools/fleet/fleet_boxes.json"
    boxes = json.loads(config.read_text())["boxes"]
    aliases = [shape["_alias"] for shape in boxes.values() if shape.get("_alias")]
    assert len(aliases) == len(set(aliases))
    assert set(aliases).isdisjoint(boxes)


def test_an_ambiguous_alias_is_refused(monkeypatch, tmp_path):
    config = tmp_path / "fleet_boxes.json"
    config.write_text(json.dumps({"boxes": {
        "one": {"_alias": "renamed", "loops": 2, "args": []},
        "two": {"_alias": "renamed", "loops": 3, "args": []},
    }}))
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    with pytest.raises(SystemExit, match="ambiguous.*renamed"):
        supervise.declared_shape("renamed", 0)


def test_the_renamed_box_still_offers_its_original_placement_tag():
    config = Path(__file__).resolve().parents[1] / "tools/fleet/fleet_boxes.json"
    args = json.loads(config.read_text())["boxes"]["gx10-6b77"]["args"]
    tags = {args[i + 1] for i, arg in enumerate(args) if arg == "--tag"}
    assert {"sparklina", "gx10-6b77"} <= tags
