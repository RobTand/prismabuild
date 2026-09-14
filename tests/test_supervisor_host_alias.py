"""A box's explicitly configured alias survives an OS hostname rename."""

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import supervise


def test_the_checked_in_alias_resolves_the_same_worker_shape(monkeypatch, tmp_path):
    config = Path(__file__).resolve().parents[1] / "tools/fleet/fleet_boxes.json"
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    canonical = supervise.declared_shape("gx10-6b77", 0)
    assert supervise.declared_shape("sparklina", 0) == canonical
    assert canonical[0] == 3


def test_the_wsl_box_resolves_through_its_windows_hostname(monkeypatch, tmp_path):
    """WSL2 answers the Windows machine name, and submissions spell it wsl-gpu.

    ``socket.gethostname()`` on that box returns ``DESKTOP-P5UOGNJ`` because
    WSL2 inherits the Windows machine name, so the supervisor would refuse to
    start on a roster keyed only by the name the fleet uses for it.
    """

    config = Path(__file__).resolve().parents[1] / "tools/fleet/fleet_boxes.json"
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    canonical = supervise.declared_shape("wsl-gpu", 0)
    assert supervise.declared_shape("DESKTOP-P5UOGNJ", 0) == canonical
    loops, args = canonical
    assert loops == 3
    tags = {args[i + 1] for i, arg in enumerate(args) if arg == "--tag"}
    assert {"wsl-gpu", "gfx1201", "rocm", "rdna4"} <= tags
    assert args[args.index("--mem-gb") + 1] == "16"


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
