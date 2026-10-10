"""Tests for the prismabuild#1730 gate probe helpers.

The probe itself runs on a Spark under PrismaBuild; these tests pin its
pure math (D1 floor, subset choice, rate division) on any CPU box.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import gate_1730_probe as probe


def test_reproduces_design_space_claim():
    # Design 2026-10-05: sparky 125 GiB free with 92 GiB floor fits
    # neither rule for the 163.47 GiB set. The probe math must agree.
    gib = 1024 ** 3
    verdict = probe.d1_verdict(125 * gib, int(125 / 0.07 * gib), probe.A8S_BYTES)
    assert verdict["fits"] is False
    assert verdict["reason"] == "1.5N+20GiB rule fails"


def test_sparklina_floor_blocks_the_copy():
    # Sparklina: 147 GiB free, 46 GiB floor. The 1.5N+20 rule needs
    # 265.2 GiB free, so it fails before the floor matters.
    gib = 1024 ** 3
    verdict = probe.d1_verdict(147 * gib, 920 * gib, probe.A8S_BYTES)
    assert verdict["fits"] is False
    assert verdict["tmp_need_gib"] == round(probe.A8S_BYTES / gib * 1.5 + 20.0, 2)


def test_floor_alone_can_refuse():
    # Copy rule passes (175-150-20=5) but the floor fails (175-100-100<0).
    gib = 1024 ** 3

    verdict = probe.d1_verdict(175 * gib, 2000 * gib, 100 * gib)
    assert verdict["fits"] is False
    assert verdict["reason"] == "5% floor fails"


def test_subset_stops_at_budget():
    files = [("/f%d" % i, 3 * 1024 ** 3) for i in range(5)]
    picked = probe.pick_subset(files, 8 * 1024 ** 3)
    assert len(picked) == 3
    assert sum(size for _, size in picked) == 9 * 1024 ** 3


def test_subset_keeps_small_files_whole():
    files = [("/tiny", 100), ("/big", 10 * 1024 ** 3)]
    picked = probe.pick_subset(files, 8 * 1024 ** 3)
    assert [name for name, _ in picked] == ["/tiny", "/big"]


def test_a8s_constant_matches_canonical_bytes():
    assert abs(probe.A8S_BYTES / 1024 ** 3 - 163.47) < 0.01
