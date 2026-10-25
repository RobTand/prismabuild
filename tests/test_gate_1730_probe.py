"""Tests for the prismabuild#1730 gate probe helpers.

The probe itself runs on a Spark under PrismaBuild; these tests pin its
pure logic (D1 floor, fixed arm shape, prefix cap, sparse check) on any
CPU box.
"""
from __future__ import annotations

import os
import io
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import gate_1730_probe as probe


def test_reproduces_design_space_claim():
    # Design 2026-10-05: sparky 125 GiB free with 92 GiB floor fits
    # neither rule for the 163.47 GiB set. The probe math must agree.
    gib = 1024 ** 3
    verdict = probe.d1_verdict(125 * gib, int(125 / 0.07 * gib), probe.A8S_BYTES)
    assert verdict["fits"] is False
    assert verdict["reason"] == "1.5N+20GiB rule fails"


def test_preflight_never_reads_a8s_or_creates_speed_files(tmp_path, monkeypatch, capsys):
    def forbidden(*_args, **_kwargs):
        pytest.fail("preflight must not read model payloads or write a speed corpus")

    monkeypatch.setattr(probe, "measure_source", forbidden)
    monkeypatch.setattr(probe, "measure_local_nvme", forbidden)
    monkeypatch.setattr(probe, "CANONICAL_DIR", str(tmp_path / "canonical"))
    monkeypatch.setattr(probe, "STAGE_DIR", str(tmp_path / "missing-stage"))
    monkeypatch.setattr(probe, "environment_report", lambda: {})
    monkeypatch.setattr(probe, "os", SimpleNamespace(stat=os.stat, path=os.path,
        statvfs=lambda _path: SimpleNamespace(f_blocks=100, f_bavail=27,
            f_bfree=30, f_bsize=4096, f_frsize=4096)))
    monkeypatch.setattr(probe, "measure_bind", lambda **_kwargs: {"probe": "fixture"})
    out = tmp_path / "preflight.json"
    assert probe.main(["--preflight-only", "--destination", str(tmp_path),
                       "--bind-image", "sha256:" + "a" * 64, "--out", str(out)]) == 0
    report = json.loads(out.read_text())
    assert json.loads(capsys.readouterr().out) == report
    assert report["stage_path"]["present"] is False
    assert report["speed"]["status"] == "not_run"
    assert report["space"]["total_bytes"] == 409600
    assert report["space"]["available_bytes"] == 110592
    assert report["bind"] == {"probe": "fixture"}


def test_path_observation_resolves_mount_after_autofs_activation(tmp_path, monkeypatch):
    root = str(tmp_path)
    autofs = f"1 0 0:1 / {root} rw - autofs systemd-1 rw\n"
    nfs = f"2 1 0:2 / {root} rw - nfs4 server:/shared rw\n"
    rows = [autofs]

    def stat(_path):
        rows.append(nfs)
        return SimpleNamespace(st_dev=2)

    monkeypatch.setattr(probe, "os", SimpleNamespace(stat=stat, path=os.path))
    monkeypatch.setattr(probe, "open", lambda *_a, **_kw: io.StringIO("".join(rows)), raising=False)
    observed = probe.observe_path(root + "/model")
    assert observed["present"] is True
    assert observed["mountinfo"] == nfs.strip()


def test_path_observation_reports_the_symlink_targets_mount(tmp_path, monkeypatch):
    backing = tmp_path / "backing"
    (backing / "model").mkdir(parents=True)
    link = tmp_path / "link"
    link.symlink_to(backing, target_is_directory=True)
    parent_mount = f"1 0 0:1 / {tmp_path} rw - ext4 /dev/local rw\n"
    target_mount = f"2 1 0:2 / {backing} rw - nfs4 server:/stage rw\n"
    monkeypatch.setattr(probe, "open",
        lambda *_a, **_kw: io.StringIO(parent_mount + target_mount), raising=False)

    observed = probe.observe_path(str(link / "model"))

    assert observed["present"] is True
    assert observed["mountinfo"] == target_mount.strip()


def test_bind_cleans_local_fixture_when_shared_temp_creation_fails(tmp_path, monkeypatch):
    make_temp = tempfile.mkdtemp
    created = []

    def make_fixture(suffix=None, prefix=None, dir=None):
        if dir == "/mnt/shared":
            raise PermissionError("shared fixture unavailable")
        path = make_temp(suffix=suffix, prefix=prefix, dir=dir)
        created.append(Path(path))
        return path

    image = "sha256:" + "a" * 64
    monkeypatch.setattr(probe, "host_local_root", lambda: {"path": str(tmp_path)})
    monkeypatch.setattr(probe, "run_docker",
        lambda *_a, **_kw: {"returncode": 0, "stdout": image})
    monkeypatch.setattr(probe.tempfile, "mkdtemp", make_fixture)

    with pytest.raises(PermissionError, match="shared fixture unavailable"):
        probe.measure_bind(image=image)

    assert len(created) == 1
    assert not created[0].exists()


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
    # ZFS compression and delayed block accounting invalidate this fixture's
    # allocated-byte assumption. Private tmpfs pages report their allocation.
    with tempfile.TemporaryDirectory(prefix=f"pb1730-{tmp_path.name}-", dir="/dev/shm") as root:
        path = str(Path(root) / "backed.bin")
        probe.write_test_file(path, 2 * 1024 * 1024)
        assert os.path.getsize(path) == 2 * 1024 * 1024
        assert probe.is_sparse(2 * 1024 * 1024, probe.allocated_bytes(path)) is False


def test_private_backing_fixture_still_detects_a_real_hole(tmp_path):
    with tempfile.TemporaryDirectory(prefix=f"pb1730-{tmp_path.name}-", dir="/dev/shm") as root:
        path = str(Path(root) / "hole.bin")
        with open(path, "wb") as stream:
            stream.truncate(2 * 1024 * 1024)
        assert os.path.getsize(path) == 2 * 1024 * 1024
        assert probe.is_sparse(2 * 1024 * 1024, probe.allocated_bytes(path)) is True


def test_a8s_constant_matches_canonical_bytes():
    assert abs(probe.A8S_BYTES / 1024 ** 3 - 163.47) < 0.01


@pytest.mark.parametrize("rows, expected", [
    ("hits 4 11\nmisses 4 invalid\nsize 4 22\nother 4 99\n", {"hits": 11, "size": 22}),
    ("hits 4 invalid\nother 4 99\n", None),
])
def test_arc_snapshot_keeps_available_valid_counters(tmp_path, monkeypatch, rows, expected):
    stats = tmp_path / "arcstats"
    stats.write_text("header one\nheader two\n" + rows)
    monkeypatch.setattr(probe, "open", lambda *_args, **_kwargs: stats.open(), raising=False)
    assert probe._gate_arc_snapshot() == expected


def test_stage_default_reads_only_private_fixture_files(tmp_path):
    stage = Path(probe.STAGE_DIR)
    assert stage.is_relative_to(tmp_path), "the probe must not read the live stage mount"
    stage.mkdir()
    for index in range(16):
        (stage / f"private-{index:02d}.bin").write_bytes(bytes([index]) * (1024 * 1024))
    observed = probe.measure_source("private-stage", str(stage))
    assert observed["arm_files"] == 16
    assert observed["arm_total_gib"] == round(16 / 1024, 3)
    assert [(arm["streams"], arm["cold"]) for arm in observed["arms"]] == probe.ARM_ORDER
    assert all(arm["bytes_gib"] == round(16 / 1024, 3) and arm["idle_threads"] == 0
               for arm in observed["arms"])
