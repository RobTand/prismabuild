"""Tests for the prismabuild#1730 gate probe helpers.

The probe itself runs on a Spark under PrismaBuild; these tests pin its
pure logic (D1 floor, fixed arm shape, prefix cap, sparse check) on any
CPU box.
"""
from __future__ import annotations

import os
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


def test_arm_selects_sixteen_largest_files():
    files = [("/f%02d" % i, (i + 1) * 1024 ** 3) for i in range(20)]
    sel = probe.select_arm_files(files)
    assert sel["count"] == 16
    assert sel["full"] is True
    # Largest 16 of 1..20 GiB are 5..20 GiB; smallest pick is 5 GiB.
    assert sel["prefix"] == probe.PREFIX_BYTES
    assert sel["total"] == 16 * probe.PREFIX_BYTES


def test_arm_clips_prefix_to_smallest_pick():
    small = probe.PREFIX_BYTES // 2
    files = [("/big%02d" % i, 2 * probe.PREFIX_BYTES) for i in range(15)]
    files.append(("/small", small))
    sel = probe.select_arm_files(files)
    assert sel["count"] == 16
    assert sel["prefix"] == small
    assert sel["total"] == 16 * small


def test_arm_marks_short_sets_not_full():
    files = [("/f%d" % i, 2 * probe.PREFIX_BYTES) for i in range(9)]
    sel = probe.select_arm_files(files)
    assert sel["count"] == 9
    assert sel["full"] is False


def test_stream_counts_read_equal_bytes_with_no_idle_thread(tmp_path):
    per_file = 1024 * 1024
    paths = []
    for i in range(16):
        path = tmp_path / ("arm-%02d.bin" % i)
        path.write_bytes(b"\x5a" * per_file)
        paths.append(str(path))
    totals = []
    for streams in (1, 4, 16):
        arm = probe.run_arm(paths, streams, False, per_file)
        totals.append(arm["bytes_gib"])
        assert arm["idle_threads"] == 0
        assert len(arm["per_thread_s"]) == streams
    assert totals[0] == totals[1] == totals[2]


def test_prefix_read_caps_at_limit(tmp_path):
    path = tmp_path / "capped.bin"
    path.write_bytes(b"\x5a" * (2 * 1024 * 1024))
    assert probe.read_prefix_bytes(str(path), 1024 * 1024) == 1024 * 1024


def test_sparse_check_flags_a_hole():
    assert probe.is_sparse(4096, 0) is True
    assert probe.is_sparse(4096, 8192) is False
    assert probe.is_sparse(4096, -1) is None


def test_written_file_reports_backing(tmp_path):
    path = str(tmp_path / "backed.bin")
    probe.write_test_file(path, 2 * 1024 * 1024)
    assert os.path.getsize(path) == 2 * 1024 * 1024
    assert probe.is_sparse(2 * 1024 * 1024, probe.allocated_bytes(path)) is False


def test_a8s_constant_matches_canonical_bytes():
    assert abs(probe.A8S_BYTES / 1024 ** 3 - 163.47) < 0.01
