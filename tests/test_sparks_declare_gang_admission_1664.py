"""Only the Sparks declare gang admission; no other box does (#1664).

A generation built from main dropped ``--gang-admission`` from both Spark
worker shapes. The supervisor adopts the new generation's roster at
re-exec, so it spawned Spark loops without the flag and a native gang
could not publish until the sealed generation was edited by hand.
Gang admission stays default-off in the worker loop; the roster is the
statement that turns it on, and only for the two Sparks.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ROSTER = ROOT / "tools" / "fleet" / "fleet_boxes.json"

GANG_FLAG = "--gang-admission"
SPARKS = ("sparky", "gx10-6b77")
OTHERS = ("dl380g10", "wsl-gpu")


def _args(name: str) -> list[str]:
    boxes = json.loads(ROSTER.read_text())["boxes"]
    assert isinstance(boxes, dict), "roster has no boxes mapping"
    entry = boxes[name]
    assert isinstance(entry, dict), f"{name} has no roster entry"
    args = entry.get("args")
    assert isinstance(args, list), f"{name} declares no loop args"
    return [str(arg) for arg in args]


def test_the_two_sparks_declare_gang_admission() -> None:
    missing = [name for name in SPARKS if GANG_FLAG not in _args(name)]
    assert not missing, (
        f"{missing} declare no {GANG_FLAG}; their loops would offer no "
        f"gang-v1 capability and no gang member would be claimed (#1664)")


def test_no_other_box_declares_gang_admission() -> None:
    extra = [name for name in OTHERS if GANG_FLAG in _args(name)]
    assert not extra, (
        f"{extra} declare {GANG_FLAG}; gang admission stays on the two "
        f"Sparks only (#1664)")
