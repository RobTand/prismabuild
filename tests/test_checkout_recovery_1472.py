"""PB1472: filesystem stat identities, not VMA superblock guesses, fence recovery."""
from __future__ import annotations

import errno
import os
from pathlib import Path
import shutil

import pytest

from prismabuild import checkout_recovery as recovery
from test_checkout_recovery_1465 import (
    _assert_real_mapping, _native_holder, _planned, _process, _refused,
    _selected_inodes, case, cpu_shared_library, make_case,
)


@pytest.mark.parametrize("phase", ["prepare", "apply"])
def test_authoritative_magic_link_identity_catches_different_vma_identity(case, phase):
    selected = case.checkout / "native" / "libkernel.so"
    alias = case.protected_root / "external-alias.so"
    os.link(selected, alias)
    case.bank()
    plan = _planned(case)
    process = _process(case)
    info = selected.stat()
    # Deliberately not either filesystem-level number. Neither number from maps
    # is an authority for comparing against the original tree's stat identity.
    device = info.st_dev ^ 1
    (process / "maps").write_text(
        f"1000-2000 r--p 00000000 {os.major(device):x}:{os.minor(device):x} "
        f"{info.st_ino + 1} {alias}\n")
    link = process / "map_files" / "1000-2000"
    link.unlink()
    link.symlink_to(alias)
    operation = case.prepare if phase == "prepare" else lambda: case.apply(plan)
    result = _refused(case, operation)
    assert any("maps a selected checkout inode" in error for error in result["errors"]), result


def test_vma_identity_collision_does_not_override_unrelated_filesystem_identity(case):
    process = _process(case)
    row = (process / "maps").read_text().split(maxsplit=5)
    selected = (case.checkout / "native" / "libkernel.so").stat()
    row[3] = f"{os.major(selected.st_dev):x}:{os.minor(selected.st_dev):x}"
    row[4] = str(selected.st_ino)
    (process / "maps").write_text(" ".join(row))
    _planned(case)


def _intercept_mapping_stat(monkeypatch, process, replacement):
    directory = (process / "map_files").stat()
    actual_stat = os.stat

    def mapping_stat(path, *, dir_fd=None, **options):
        if dir_fd is not None and path == "1000-2000":
            held = os.fstat(dir_fd)
            if (held.st_dev, held.st_ino) == (directory.st_dev, directory.st_ino):
                return replacement(lambda: actual_stat(path, dir_fd=dir_fd, **options))
        return actual_stat(path, dir_fd=dir_fd, **options)

    monkeypatch.setattr(os, "stat", mapping_stat)


@pytest.mark.parametrize("error", [errno.EPERM, errno.EACCES, errno.ENOENT, errno.EIO])
def test_missing_magic_link_identity_refuses_even_when_maps_and_names_are_complete(case, monkeypatch, error):
    process = _process(case)
    _planned(case)

    def unavailable(original):
        raise OSError(error, "mapping identity qualification fault")

    _intercept_mapping_stat(monkeypatch, process, unavailable)
    result = _refused(case, case.prepare)
    assert any("map_files identity unavailable" in item for item in result["errors"]), result


def test_unchanged_maps_and_names_do_not_hide_changed_authoritative_identity(case, monkeypatch):
    process = _process(case)
    _planned(case)
    reads = 0

    def changed(original):
        nonlocal reads
        reads += 1
        info = original()
        if reads == 1:
            return info
        fields = list(info)
        fields[1] += 1
        return os.stat_result(fields)

    _intercept_mapping_stat(monkeypatch, process, changed)
    result = _refused(case, case.prepare)
    assert reads == 2
    assert any("maps/map_files changed during census" in item for item in result["errors"]), result


@pytest.mark.parametrize("kind", ["mmap", "dlopen"])
@pytest.mark.parametrize("location", ["external-hardlink", "deleted-external", "unrelated"])
@pytest.mark.parametrize("phase", ["prepare", "apply"])
def test_real_missing_magic_link_authority_preserves_the_exact_orphan(
        case, monkeypatch, cpu_shared_library, kind, location, phase):
    """Real native negative proof, explicitly separate from privileged positives."""
    selected = case.checkout / "native" / "libkernel.so"
    if kind == "dlopen":
        shutil.copyfile(cpu_shared_library, selected)
    alias = case.protected_root / "external-alias.so"
    if location == "unrelated":
        shutil.copyfile(selected, alias)
    else:
        os.link(selected, alias)
    with _native_holder(case, monkeypatch, kind=kind, mapped=alias) as (holder, report):
        if location == "deleted-external":
            alias.unlink()
        case.bank()
        plan = _planned(case)
        rows = _assert_real_mapping(case, holder, report, alias,
                                    deleted=location == "deleted-external")
        identity = (report["dev"], report["ino"])
        assert (identity in _selected_inodes(case)) == (location != "unrelated")
        try:
            (Path("/proc") / str(holder.pid) / "map_files" / rows[0][0].decode()).stat()
        except PermissionError:
            pass  # Actual kernel denial, not an injected capability decision.
        else:
            pytest.skip("missing-authority witness needs a denied magic-link stat; "
                        "positive native tests cover available authority")
        operation = (lambda: case.prepare(proc_root=Path("/proc"))) if phase == "prepare" else (
            lambda: case.apply(plan, proc_root=Path("/proc")))
        result = _refused(case, operation)
        assert any("map_files identity unavailable" in error and str(holder.pid) in error
                   for error in result["errors"]), result
    # The same actual throwaway bank/queue remains usable after the real holder
    # exits. This is fixture cleanup only, not an operational recovery action.
    if phase == "prepare":
        _planned(case, proc_root=Path("/proc"))
    else:
        result = case.apply(plan, proc_root=Path("/proc"))
        assert result["complete"] is True and result["removed"] == [str(case.directory)], result
