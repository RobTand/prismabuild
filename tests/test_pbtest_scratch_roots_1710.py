"""Idle expiry discovers roots without replacing owned_tree's custody."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import errno
import os
from pathlib import Path
import pwd
import stat
import threading
import time
from unittest.mock import Mock

import pytest

from prismabuild import owned_tree, pbtest_scratch_roots as roots
from test_pbtest_attempt_temp_expiry import _checkout, _shard, pbtest
from test_supervisor_prunes_worker_logs import _cycle, logs  # noqa: F401


@pytest.fixture
def registry(tmp_path, monkeypatch):
    path = tmp_path / "registry"
    monkeypatch.setattr(roots, "REGISTRY", path)
    monkeypatch.setattr(pbtest, "ROOTS_PROGRAM", pbtest.ROOTS_PROGRAM +
                       '\n_pb_temp_roots.update(REGISTRY=__import__("pathlib").Path(' +
                       repr(str(path)) + '))\n')
    return path


def _root_attempt(root, *, basetemp=False, retained=True):
    root.mkdir()
    parent = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        roots.register_pbtest_root(parent, basetemp=basetemp)
    finally:
        os.close(parent)
    attempt = root / ("a" * 64) / ("b" * 32) if basetemp else root / "pb-deadbeef"
    attempt.mkdir(parents=True)
    directory = os.open(attempt, os.O_RDONLY | os.O_DIRECTORY)
    try:
        lock = owned_tree.take_owner_lock(directory, owned_tree.LOCK_NAME)
        if retained:
            owned_tree.start_retention(lock)
    finally:
        os.close(directory)
    return attempt, lock


def _future_reap(parent, pattern, **options):
    return owned_tree.reap_dead_trees(parent, pattern,
                                     now=time.time() + 90000, **options)


@pytest.mark.parametrize("basetemp", [False, True])
def test_real_failed_shard_registers_root_and_expires_without_later_shard(
        registry, tmp_path, monkeypatch, basetemp):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    base = tmp_path / "base" if basetemp else None
    if base is not None:
        base.mkdir()
    monkeypatch.setenv("HOME", str(tmp_path / "sealed-home"))
    checkout = _checkout(tmp_path, "    assert False\n")
    result = _shard(checkout, scratch, base)
    assert result.returncode == 1, result.stdout + result.stderr
    root = base if basetemp else scratch
    assert list(root.rglob("marker"))
    entry, = registry.iterdir()
    assert entry.is_symlink() and entry.readlink() == root
    assert entry.name.startswith("basetemp-" if basetemp else "tmpdir-")
    assert roots.sweep_pbtest_roots(owned_tree.reap_dead_trees) == [(entry.name, [], None)]
    reports = roots.sweep_pbtest_roots(_future_reap)
    assert reports[0][1] and reports[0][2] is None
    assert root.exists() and list(root.iterdir()) == []


@pytest.mark.parametrize("basetemp", [False, True])
def test_periodic_sweep_preserves_a_live_owner_at_any_age(registry, tmp_path, basetemp):
    attempt, lock = _root_attempt(tmp_path / "scratch", basetemp=basetemp)
    try:
        then = time.time() - 90000
        os.utime(attempt / owned_tree.LOCK_NAME, (then, then))
        assert roots.sweep_pbtest_roots(_future_reap)[0][1:] == ([], None)
        assert attempt.exists()
    finally:
        os.close(lock)


def test_dead_owner_gets_a_full_window_from_first_supervisor_observation(registry, tmp_path):
    attempt, lock = _root_attempt(tmp_path / "scratch", retained=False)
    os.close(lock)
    assert roots.sweep_pbtest_roots(_future_reap)[0][1:] == ([], None)
    assert attempt.exists()
    assert roots.sweep_pbtest_roots(owned_tree.reap_dead_trees)[0][1:] == ([], None)
    assert roots.sweep_pbtest_roots(_future_reap)[0][1] == [attempt.name]
    assert not attempt.exists()


def test_real_supervisor_reads_the_registry_and_reaps_idle_scratch(registry, logs, tmp_path, monkeypatch):
    attempt, lock = _root_attempt(tmp_path / "scratch")
    os.close(lock)
    then = time.time() - 90000
    os.utime(attempt / owned_tree.LOCK_NAME, (then, then))
    _cycle(monkeypatch, ["--loops", "1"], [])
    assert not attempt.exists()


@pytest.mark.parametrize("replacement", ["directory", "symlink", "parent_symlink", "missing"])
def test_changed_root_identity_retains_every_tree(registry, tmp_path, replacement):
    root = tmp_path / "parent" / "scratch"
    root.parent.mkdir()
    attempt, lock = _root_attempt(root)
    os.close(lock)
    original = tmp_path / "original"
    root.rename(original)
    if replacement == "directory":
        other, held = _root_attempt(root)
        os.close(held)
        # Do not register the replacement as a new root for this test.
        for entry in registry.iterdir():
            if entry.name.endswith(f"-{root.stat().st_ino}"):
                entry.unlink()
    elif replacement == "symlink":
        root.symlink_to(original, target_is_directory=True)
    elif replacement == "parent_symlink":
        moved_parent = tmp_path / "moved-parent"
        root.parent.rename(moved_parent)
        (moved_parent / "scratch").symlink_to(original, target_is_directory=True)
        root.parent.symlink_to(moved_parent, target_is_directory=True)
    reports = roots.sweep_pbtest_roots(_future_reap)
    assert reports and all(not removed and error for _, removed, error in reports)
    assert original.exists() and (original / attempt.name).exists()
    if replacement == "directory":
        assert other.exists()


def test_registry_must_be_private_and_not_a_symlink(registry, tmp_path):
    registry.mkdir(mode=0o755)
    os.chmod(registry, 0o755)
    with pytest.raises(ValueError, match="private"):
        roots.sweep_pbtest_roots(_future_reap)
    registry.rmdir()
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    registry.symlink_to(elsewhere, target_is_directory=True)
    with pytest.raises(OSError):
        roots.sweep_pbtest_roots(_future_reap)
    assert list(elsewhere.iterdir()) == []


def test_registration_refusal_precedes_attempt_creation(registry, tmp_path):
    registry.mkdir(mode=0o755)
    os.chmod(registry, 0o755)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    result = _shard(_checkout(tmp_path, ""), scratch)
    assert result.returncode != 0
    assert "cannot use the attempt base temp" in result.stderr
    assert list(scratch.iterdir()) == []


def test_registration_is_idempotent_and_each_layout_is_separate(registry, tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    descriptor = os.open(scratch, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for basetemp in (False, False, True, True):
            roots.register_pbtest_root(descriptor, basetemp=basetemp)
    finally:
        os.close(descriptor)
    assert len(list(registry.iterdir())) == 2


@pytest.mark.parametrize("shared_root", [True, False], ids=["same-root", "different-roots"])
@pytest.mark.parametrize("basetemp", [False, True], ids=["tmpdir", "basetemp"])
def test_concurrent_registration_publishes_complete_entries_without_lost_roots(
        registry, tmp_path, monkeypatch, shared_root, basetemp):
    worker_count = 4
    worker_roots = [tmp_path / ("shared" if shared_root else f"root-{index}")
                    for index in range(worker_count)]
    for root in set(worker_roots):
        root.mkdir()
    staged = []

    def inspect_staged_publications():
        entries = list(registry.iterdir())
        assert len(entries) == worker_count
        assert all(entry.name.startswith(".") and entry.is_symlink() for entry in entries)
        assert sorted(entry.readlink() for entry in entries) == sorted(worker_roots)
        reaper = Mock(side_effect=AssertionError("unpublished roots reached the reaper"))
        assert roots.sweep_pbtest_roots(reaper) == []
        reaper.assert_not_called()
        staged.append(True)

    start = threading.Barrier(worker_count, timeout=30)
    publish = threading.Barrier(worker_count, action=inspect_staged_publications, timeout=30)
    actual_replace = roots.os.replace

    def synchronized_publication(*args, **kwargs):
        if os.fspath(args[0]).startswith((".tmpdir-", ".basetemp-")):
            publish.wait()
        return actual_replace(*args, **kwargs)

    monkeypatch.setattr(roots.os, "replace", synchronized_publication)

    def register(root):
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            start.wait()
            roots.register_pbtest_root(descriptor, basetemp=basetemp)
        finally:
            os.close(descriptor)

    with ThreadPoolExecutor(max_workers=worker_count) as workers:
        futures = [workers.submit(register, root) for root in worker_roots]
        for future in futures:
            future.result(timeout=60)

    layout = "basetemp" if basetemp else "tmpdir"
    expected = {f"{layout}-{root.stat().st_dev}-{root.stat().st_ino}": root
                for root in worker_roots}
    assert staged == [True], "workers did not overlap at publication"
    assert {entry.name for entry in registry.iterdir()} == set(expected)
    for name, root in expected.items():
        entry = registry / name
        assert entry.is_symlink() and entry.readlink() == root
        assert entry.resolve(strict=True) == root


@pytest.mark.parametrize("refusal", ["malformed-entry", "unreadable-root", "foreign-owner"])
@pytest.mark.parametrize("basetemp", [False, True], ids=["tmpdir", "basetemp"])
def test_registry_refusals_never_call_the_reaper_or_remove_scratch(
        registry, tmp_path, monkeypatch, refusal, basetemp):
    root = tmp_path / "scratch"
    attempt, lock = _root_attempt(root, basetemp=basetemp)
    os.close(lock)
    marker = attempt / "keep.txt"
    marker.write_text("scratch survives refusal")
    then = time.time() - 90000
    os.utime(attempt / owned_tree.LOCK_NAME, (then, then))
    entry, = registry.iterdir()
    observations = []
    if refusal == "malformed-entry":
        entry.unlink()
        entry.write_text(str(root))
    elif refusal == "unreadable-root":
        actual_open = roots.os.open

        def unreadable(path, *args, **kwargs):
            if os.fspath(path) == str(root):
                observations.append("denied")
                raise PermissionError(errno.EACCES, "root unreadable", str(root))
            return actual_open(path, *args, **kwargs)

        monkeypatch.setattr(roots.os, "open", unreadable)
    else:
        identity = root.stat().st_dev, root.stat().st_ino
        actual_fstat = roots.os.fstat

        def foreign_owner(descriptor):
            info = actual_fstat(descriptor)
            if (info.st_dev, info.st_ino) == identity:
                observations.append("foreign")
                fields = list(info)
                fields[stat.ST_UID] = os.geteuid() + 1
                return os.stat_result(fields)
            return info

        monkeypatch.setattr(roots.os, "fstat", foreign_owner)

    reaper = Mock(side_effect=AssertionError("refused root reached owned_tree"))
    monkeypatch.setattr(owned_tree, "reap_dead_trees", reaper)
    reports = roots.sweep_pbtest_roots(owned_tree.reap_dead_trees)
    assert len(reports) == 1 and reports[0][0] == entry.name
    assert reports[0][1] == [] and reports[0][2]
    reaper.assert_not_called()
    if refusal == "unreadable-root":
        assert observations == ["denied"]
        assert "root unreadable" in reports[0][2]
    elif refusal == "foreign-owner":
        assert observations == ["foreign"]
        assert "unknown owner" in reports[0][2]
    assert root.is_dir() and attempt.is_dir()
    assert marker.read_text() == "scratch survives refusal"
    assert (attempt / owned_tree.LOCK_NAME).is_file()


def test_registration_refreshes_a_stale_inode_reference(registry, tmp_path):
    registry.mkdir(mode=0o700)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    info = scratch.stat()
    entry = registry / f"tmpdir-{info.st_dev}-{info.st_ino}"
    entry.symlink_to(tmp_path / "removed-root")
    descriptor = os.open(scratch, os.O_RDONLY | os.O_DIRECTORY)
    try:
        roots.register_pbtest_root(descriptor)
    finally:
        os.close(descriptor)
    assert entry.readlink() == scratch
    assert list(registry.iterdir()) == [entry]


def test_unknown_and_unmarked_entries_are_preserved_and_root_count_is_bounded(
        registry, tmp_path, monkeypatch):
    for index in range(5):
        attempt, lock = _root_attempt(tmp_path / str(index))
        os.close(lock)
        (attempt.parent / "pb-unknownx").mkdir()
    (registry / "unrecognized").symlink_to(tmp_path)
    monkeypatch.setattr(roots, "ROOTS_PER_SWEEP", 2)
    assert len(roots.sweep_pbtest_roots(_future_reap)) == 2
    assert (registry / "unrecognized").is_symlink()
    assert all((tmp_path / str(index) / "pb-unknownx").exists() for index in range(5))


def test_registry_default_uses_the_executing_uid_not_sealed_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "sealed-home"))
    namespace = {}
    exec(compile(Path(roots.__file__).read_bytes(), roots.__file__, "exec"), namespace)
    assert namespace["pbtest_registry_path"]() == (
        Path(pwd.getpwuid(os.geteuid()).pw_dir) / ".local/state/prismabuild/pbtest-roots-v1")
