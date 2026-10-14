"""The published fleet shape turns the tiers' output windows on (#905, phase 2).

``--output-windows`` makes each tier loop count a tier's unheld produced-output
window in the joint-fit gate and the fence check (#747).  The code default stays
off; this is the per-box rollout, declared in ``fleet_boxes.json`` -- the file
the supervisor reads its roles from -- so it is versioned and published like the
rest of the shape.  These tests read that file, not a copy of it: a box whose
tier loop lost the flag, or a flag the tier loop's own parser no longer knows,
fails here instead of on the live box.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

import tier_loop  # noqa: E402

FLAG = "--output-windows"


def _boxes() -> dict:
    document = json.loads((ROOT / "tools" / "fleet" / "fleet_boxes.json").read_text())
    return document["boxes"]


def _tier_argvs() -> dict[str, list[str]]:
    return {host: entry["roles"]["tiers"] for host, entry in _boxes().items()
            if "tiers" in (entry.get("roles") or {})}


def test_at_least_one_box_runs_a_tier_loop() -> None:
    assert _tier_argvs(), "no box declares a tiers role; the check below is vacuous"


def test_every_declared_tier_loop_counts_output_windows() -> None:
    missing = [host for host, argv in _tier_argvs().items() if FLAG not in argv]

    assert not missing, f"tier loops without {FLAG}: {missing}"


def test_the_flag_is_declared_once_per_argv() -> None:
    for host, argv in _tier_argvs().items():
        assert argv.count(FLAG) == 1, host


def test_the_tier_loop_parser_accepts_each_declared_argv_and_sets_the_flag() -> None:
    for host, argv in _tier_argvs().items():
        parsed = tier_loop._parser().parse_args(argv)

        assert parsed.output_windows is True, host


def test_the_code_default_is_unchanged() -> None:
    assert tier_loop._parser().parse_args([]).output_windows is False
