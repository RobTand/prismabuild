"""The worker expires ended pbtest scratch, not live or unknown scratch."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import supervise
from prismabuild import pbtest_scratch as scratch_owner

KEY = "a" * 64
NONCE = "b" * 32
SCOPE = "prismabuild-job" + hashlib.sha256((KEY + NONCE).encode()).hexdigest()[:32] + ".slice"


def _root_record(root):
    info = root.stat()
    return {"root": str(root), "device": info.st_dev, "inode": info.st_ino}


def _kept(root, name, age):
    directory = root / name
    directory.mkdir(mode=0o700)
    info = directory.stat()
    record = {"schema": "prismabuild.pbtest_scratch.v1", **_root_record(root),
              "name": name, "attempt_device": info.st_dev,
              "attempt_inode": info.st_ino, "action_key": KEY, "nonce": NONCE,
              "scope": SCOPE, "kept_unix": time.time() - age}
    (directory / ".pbtest-owner.json").write_text(json.dumps(record))
    (directory / ".pbtest-lock").touch(mode=0o600)
    (directory / "evidence").write_bytes(b"failed-test" * 4096)
    return directory


@pytest.fixture
def custody(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(scratch_owner, "registry_root",
                        lambda: tmp_path / ".local/state/prismabuild/pbtest-scratch")
    monkeypatch.setenv("PRISMABUILD_ACTION_SCOPE", SCOPE)
    cgroups = tmp_path / "cgroups"
    cgroups.mkdir()
    monkeypatch.setattr(scratch_owner, "CGROUP_ROOT", cgroups)
    root = tmp_path / "scratch"
    root.mkdir()
    return root


def test_worker_tick_expires_old_kept_scratch(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(scratch_owner, "registry_root",
                        lambda: tmp_path / ".local/state/prismabuild/pbtest-scratch")
    monkeypatch.setattr(supervise, "_last_scratch_sweep", None)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    registry = tmp_path / ".local/state/prismabuild/pbtest-scratch"
    registry.mkdir(mode=0o700, parents=True)
    digest = hashlib.sha256(os.fsencode(str(scratch))).hexdigest()
    (registry / (digest + ".json")).write_text(json.dumps(_root_record(scratch)))
    expired = _kept(scratch, "pb-expired", 90000)
    fresh = _kept(scratch, "pb-fresh", 60)
    unknown = scratch / "pbtest-legacy"
    unknown.mkdir()
    monkeypatch.setattr(supervise, "CLAIM", tmp_path / "supervisor.claim")
    monkeypatch.setattr(supervise, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(supervise, "SYSTEMD_UNIT", tmp_path / "absent.service")
    monkeypatch.setattr(supervise, "declared_shape", lambda *a, **k: (1, []))
    monkeypatch.setattr(supervise, "box_presence", lambda *a: None)
    monkeypatch.setattr(supervise, "_live_loops", lambda: [os.getpid()])
    monkeypatch.setattr(supervise, "loop_args_of", lambda pid: [])
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset({os.getpid()}))
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: False)
    monkeypatch.setattr(supervise, "declared_roles", lambda host: [])
    monkeypatch.setattr(supervise, "_reap_children", lambda: 0)
    monkeypatch.setattr(sys, "argv", ["supervise", "--once", "--loops", "1"])
    assert supervise.main() == 0
    assert not expired.exists(), "the worker kept expired scratch after its maintenance tick"
    assert (fresh / "evidence").read_bytes() == b"failed-test" * 4096
    assert unknown.is_dir()


def test_attempt_lock_preserves_live_controller_after_pytest_returns(custody):
    attempt = scratch_owner.create(custody, KEY, NONCE)
    try:
        attempt.finish(1)
        report = scratch_owner.sweep_root(custody, now=time.time() + 90000)
        assert report["removed"] == []
        assert (attempt.directory / "pytest").is_dir()
        directory = attempt.directory
    finally:
        attempt.close()
    report = scratch_owner.sweep_root(custody, now=time.time() + 90000)
    assert not directory.exists()
    assert report["removed"][0]["name"] == directory.name


@pytest.mark.parametrize("events", ["populated 1\n", "unreadable", "populated 0\n"])
def test_scope_liveness_controls_expiry(custody, events):
    directory = _kept(custody, "pb-scope", 90000)
    scope = scratch_owner.CGROUP_ROOT / SCOPE
    scope.mkdir()
    (scope / "cgroup.events").write_text(events)
    report = scratch_owner.sweep_root(custody)
    if events == "populated 0\n":
        assert not directory.exists()
        assert report["removed"][0]["name"] == directory.name
    else:
        assert (directory / "evidence").read_bytes() == b"failed-test" * 4096
        assert report["removed"] == []


def test_missing_host_cgroup_root_preserves_uncertain_ownership(custody):
    directory = _kept(custody, "pb-uncertain", 90000)
    scratch_owner.CGROUP_ROOT.rmdir()
    assert scratch_owner.sweep_root(custody)["removed"] == []
    assert (directory / "evidence").is_file()


def test_age_starts_at_completion(custody):
    attempt = scratch_owner.create(custody, KEY, NONCE)
    directory = attempt.directory
    try:
        assert scratch_owner.sweep_root(custody, now=time.time() + 90000)["removed"] == []
        attempt.finish(1)
        kept = attempt.record["kept_unix"]
    finally:
        attempt.close()
    assert scratch_owner.sweep_root(custody, now=kept + 86399)["removed"] == []
    report = scratch_owner.sweep_root(custody, now=kept + 86400)
    assert not directory.exists()
    assert report["removed"][0]["name"] == directory.name


def test_supervisor_sweep_interval_bounds_retention(custody, monkeypatch):
    attempt = scratch_owner.create(custody, KEY, NONCE)
    attempt.finish(0)
    monkeypatch.setattr(supervise, "_last_scratch_sweep", None)
    monkeypatch.setattr(supervise.time, "monotonic", lambda: 10000)
    first = _kept(custody, "pb-first", 90000)
    supervise.maintain_pbtest_scratch("test-host")
    assert not first.exists()
    second = _kept(custody, "pb-next", 90000)
    monkeypatch.setattr(supervise.time, "monotonic", lambda: 13599)
    supervise.maintain_pbtest_scratch("test-host")
    assert (second / "evidence").is_file()
    monkeypatch.setattr(supervise.time, "monotonic", lambda: 13600)
    supervise.maintain_pbtest_scratch("test-host")
    assert not second.exists()


@pytest.mark.parametrize("phase", ["startup", "completion"])
def test_concurrent_sweep_cannot_enter_partial_lifecycle(custody, monkeypatch, phase):
    entered, proceed = threading.Event(), threading.Event()
    errors, attempts = [], []
    original_write = scratch_owner._write

    def pause_write(fd, name, record):
        if name == scratch_owner.OWNER:
            entered.set()
            assert proceed.wait(10)
        original_write(fd, name, record)

    attempt = None
    if phase == "completion":
        attempt = scratch_owner.create(custody, KEY, NONCE)
        attempts.append(attempt)
    monkeypatch.setattr(scratch_owner, "_write", pause_write)

    def transition():
        try:
            if phase == "startup":
                attempts.append(scratch_owner.create(custody, KEY, NONCE))
            else:
                attempt.finish(1)
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=transition)
    worker.start()
    try:
        assert entered.wait(10)
        with pytest.raises(BlockingIOError):
            scratch_owner.sweep_root(custody, now=time.time() + 90000)
        proceed.set()
        worker.join(10)
        assert not worker.is_alive()
        assert errors == []
        assert scratch_owner.sweep_root(custody, now=time.time() + 90000)["removed"] == []
        assert (attempts[0].directory / "pytest").is_dir()
    finally:
        proceed.set()
        worker.join(10)
        for owned in attempts:
            owned.close()


def test_expiry_never_follows_symlinks_outside_root(custody, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    marker = outside / "evidence"
    marker.write_text("outside")
    (custody / "pb-escape").symlink_to(outside, target_is_directory=True)
    old = _kept(custody, "pb-old", 90000)
    (old / "outside").symlink_to(outside, target_is_directory=True)
    forged = custody / "pb-forged"
    forged.mkdir()
    (forged / scratch_owner.OWNER).symlink_to(marker)
    report = scratch_owner.sweep_root(custody)
    assert not old.exists()
    assert marker.read_text() == "outside"
    assert (custody / "pb-escape").is_symlink()
    assert (forged / scratch_owner.OWNER).is_symlink()
    assert [row["name"] for row in report["removed"]] == ["pb-old"]


def test_registered_root_replacement_cannot_authorize_deletion(custody):
    attempt = scratch_owner.create(custody, KEY, NONCE)
    attempt.finish(0)
    custody.rename(custody.with_name("old-root"))
    custody.mkdir()
    new = _kept(custody, "pb-new-root", 90000)
    reports = scratch_owner.sweep_registered()
    assert reports[0]["error"] == "registered scratch root identity changed"
    assert (new / "evidence").is_file()


@pytest.mark.parametrize("field,value", [
    ("attempt_inode", -1), ("scope", "prismabuild-job" + "0" * 32 + ".slice"),
    ("nonce", ""), ("kept_unix", None), ("kept_unix", float("nan")),
])
def test_invalid_or_unfinished_custody_preserves_scratch(custody, field, value):
    directory = _kept(custody, "pb-invalid", 90000)
    owner = directory / scratch_owner.OWNER
    record = json.loads(owner.read_text())
    record[field] = value
    owner.write_text(json.dumps(record))
    assert scratch_owner.sweep_root(custody)["removed"] == []
    assert (directory / "evidence").is_file()


def test_expiry_preserves_same_device_mounts(custody, monkeypatch):
    directory = _kept(custody, "pb-mount", 90000)
    nested = directory / "mounted"
    nested.mkdir()
    (nested / "external").write_text("mounted data")
    read_text = Path.read_text

    def mountinfo(path, *args, **kwargs):
        if path == Path("/proc/self/mountinfo"):
            return f"1 0 0:1 / {nested} rw - tmpfs tmpfs rw\n"
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", mountinfo)
    report = scratch_owner.sweep_root(custody)
    assert report["removed"] == []
    assert (nested / "external").read_text() == "mounted data"
    assert report["skipped"][0]["reason"] == "scratch contains a mount"


def test_expiry_refuses_a_symlink_root(custody, tmp_path):
    directory = _kept(custody, "pb-outside", 90000)
    alias = tmp_path / "alias"
    alias.symlink_to(custody, target_is_directory=True)
    with pytest.raises(ValueError, match="real absolute directory"):
        scratch_owner.sweep_root(alias)
    assert (directory / "evidence").is_file()
