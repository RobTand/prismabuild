"""Exercise the host helper against private mount tables and fake sysfs only."""
import importlib.util
from pathlib import Path
import shutil
import subprocess

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "fleet/storage/nfs_readahead.py"
spec = importlib.util.spec_from_file_location("nfs_readahead", SCRIPT)
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)

AUTOFS = "52 36 0:40 / /mnt/shared rw - autofs systemd-1 rw\n"


def nfs(bdi="0:64", mount_id="392", source="10.100.98.3:/storage_pool/shared"):
    return f"{mount_id} 52 {bdi} / /mnt/shared rw - nfs4 {source} rw\n"


@pytest.fixture
def host(tmp_path):
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(AUTOFS + nfs())
    bdi_root = tmp_path / "bdi"
    for bdi in ("0:40", "0:64", "0:88", "8:0"):
        directory = bdi_root / bdi
        directory.mkdir(parents=True)
        (directory / "read_ahead_kb").write_text("1024\n")
    return mountinfo, bdi_root


def test_inspection_does_not_write(host):
    mountinfo, bdi_root = host
    result = helper.configure(16384, mountinfo=mountinfo, bdi_root=bdi_root)
    assert result["read_ahead_kib"] == 1024
    assert result["applied"] is False
    assert (bdi_root / "0:64/read_ahead_kb").read_text() == "1024\n"


def test_observe_reads_the_window_without_a_requested_value(host):
    """The readiness check's entry point: report, never request or apply."""

    mountinfo, bdi_root = host
    (bdi_root / "0:64/read_ahead_kb").write_text("4096\n")
    reading = helper.observe(mountinfo=mountinfo, bdi_root=bdi_root)
    # 4096 is neither of ``configure``'s two windows, and must still report.
    assert reading["read_ahead_kib"] == 4096
    assert reading["recommended_kib"] == helper.RECOMMENDED_KIB == 16384
    assert reading["bdi"] == "0:64"
    assert reading["source"] == "10.100.98.3:/storage_pool/shared"
    assert "applied" not in reading
    assert all(p.read_text() == ("4096\n" if "0:64" in str(p) else "1024\n")
               for p in bdi_root.glob("*/read_ahead_kb"))


def test_observe_refuses_a_wrong_mount_the_same_way_configure_does(host):
    mountinfo, bdi_root = host
    mountinfo.write_text(AUTOFS)
    with pytest.raises(ValueError):
        helper.observe(mountinfo=mountinfo, bdi_root=bdi_root)


def test_observe_reports_a_non_numeric_window_as_an_os_error(host):
    """Kept apart from the wrong-mount refusal: different fault, different fix."""

    mountinfo, bdi_root = host
    (bdi_root / "0:64/read_ahead_kb").write_text("not a number\n")
    with pytest.raises(OSError):
        helper.observe(mountinfo=mountinfo, bdi_root=bdi_root)


def test_change_only_current_nfs_bdi_and_rediscover_after_mount(host):
    mountinfo, bdi_root = host
    first = helper.configure(16384, apply=True, mountinfo=mountinfo, bdi_root=bdi_root)
    assert first["before_kib"] == 1024
    assert first["read_ahead_kib"] == 16384
    for bdi in ("0:40", "0:88", "8:0"):
        assert (bdi_root / bdi / "read_ahead_kb").read_text() == "1024\n"
    mountinfo.write_text(AUTOFS + nfs("0:88", "500", "10.100.99.3:/storage_pool/shared"))
    second = helper.configure(16384, apply=True, mountinfo=mountinfo, bdi_root=bdi_root)
    assert second["bdi"] == "0:88"
    assert second["read_ahead_kib"] == 16384
    # Restoring the original window must work on real sysfs as well as a fixture.
    restored = helper.configure(1024, apply=True, mountinfo=mountinfo, bdi_root=bdi_root)
    assert restored["read_ahead_kib"] == 1024


@pytest.mark.parametrize("table", [
    "", AUTOFS, AUTOFS + nfs() + nfs("0:88"),
    nfs().replace("nfs4", "ext4"), nfs(source="server:/other"),
    nfs().replace("0:64", "../../wrong"), nfs().replace(" / /mnt/shared", " /subdir /mnt/shared"),
    "392 52 0:64 / /mnt/shared rw\n",
])
def test_refuse_missing_ambiguous_or_wrong_mount_without_writing(host, table):
    mountinfo, bdi_root = host
    mountinfo.write_text(table)
    with pytest.raises(ValueError):
        helper.configure(16384, apply=True, mountinfo=mountinfo, bdi_root=bdi_root)
    assert all(p.read_text() == "1024\n" for p in bdi_root.glob("*/read_ahead_kb"))


def test_missing_attribute_is_not_created(host):
    mountinfo, bdi_root = host
    target = bdi_root / "0:64/read_ahead_kb"
    target.unlink()
    with pytest.raises(FileNotFoundError):
        helper.configure(16384, apply=True, mountinfo=mountinfo, bdi_root=bdi_root)
    assert not target.exists()


def test_changed_mount_is_refused_before_write(host, monkeypatch):
    mountinfo, bdi_root = host
    original = helper.shared_mount
    calls = 0

    def changing(path):
        nonlocal calls
        calls += 1
        if calls == 2:
            mountinfo.write_text(AUTOFS + nfs("0:88", "500"))
        return original(path)

    monkeypatch.setattr(helper, "shared_mount", changing)
    with pytest.raises(ValueError, match="changed during readahead discovery"):
        helper.configure(16384, apply=True, mountinfo=mountinfo, bdi_root=bdi_root)
    assert all(p.read_text() == "1024\n" for p in bdi_root.glob("*/read_ahead_kb"))


def test_readback_mismatch_is_not_success(host, monkeypatch):
    mountinfo, bdi_root = host
    original = Path.read_text
    target = bdi_root / "0:64/read_ahead_kb"
    reads = 0

    def ignored_write(path, *args, **kwargs):
        nonlocal reads
        if path == target:
            reads += 1
            if reads == 2:
                return "1024\n"
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", ignored_write)
    with pytest.raises(ValueError, match="readback 1024 != requested 16384"):
        helper.configure(16384, apply=True, mountinfo=mountinfo, bdi_root=bdi_root)


def test_systemd_unit_verifies_with_mount_enablement(tmp_path):
    """Ask systemd to load the installed unit and its mount dependency graph."""
    analyzer = shutil.which("systemd-analyze")
    assert analyzer, "systemd-analyze is required for the host-unit validation"
    unit_dir = tmp_path / "etc/systemd/system"
    unit_dir.mkdir(parents=True)
    source = SCRIPT.with_name("prismabuild-nfs-readahead.service")
    shutil.copyfile(source, unit_dir / source.name)
    (unit_dir / "mnt-shared.mount").write_text(
        "[Mount]\nWhat=server:/storage_pool/shared\nWhere=/mnt/shared\nType=nfs4\n")
    (unit_dir / "mnt-shared.mount.wants").mkdir()
    (unit_dir / "mnt-shared.mount.wants" / source.name).symlink_to("../" + source.name)
    for name in ("sysinit", "basic", "shutdown", "network", "network-online", "remote-fs-pre", "remote-fs", "umount"):
        (unit_dir / f"{name}.target").write_text("[Unit]\nDescription=Fixture target\n")
    for executable in ("usr/bin/python3", "bin/mount", "bin/umount"):
        path = tmp_path / executable
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o755)
    result = subprocess.run(
        [analyzer, "verify", "--man=no", f"--root={tmp_path}", source.name, "mnt-shared.mount"],
        capture_output=True, text=True, timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
