"""The roster enables output windows per tier role, without a code-default flip."""
from __future__ import annotations

import json
from pathlib import Path

from prismabuild import tier_cycle


def test_declared_tier_roles_count_output_windows():
    roster = Path(__file__).resolve().parents[1] / "tools/fleet/fleet_boxes.json"
    boxes = json.loads(roster.read_text())["boxes"]
    roles = {host: box["roles"]["tiers"] for host, box in boxes.items()
             if "tiers" in (box.get("roles") or {})}
    assert roles, "no tier role was checked"

    for host, argv in roles.items():
        assert argv.count("--output-windows") == 1, host
        assert tier_cycle._parser().parse_args(argv).output_windows is True, host


def test_output_windows_remain_a_per_box_rollout(monkeypatch):
    monkeypatch.delenv(tier_cycle.OUTPUT_WINDOWS_ENV, raising=False)
    assert tier_cycle._parser().parse_args([]).output_windows is False
    assert tier_cycle.output_windows_enabled() is False
