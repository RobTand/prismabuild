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
