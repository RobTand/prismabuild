"""Independent scratch-queue acceptance probes for PB #2023 review r1."""
from __future__ import annotations

import errno
import inspect
import json
import os
from pathlib import Path
import shutil
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))
import prismabuild
from prismabuild import attempt_archive as archive, attempt_retention as retention
from prismabuild import digest_primitives as digests, immutable_files as native
from attempt_archive_fixture import AttemptArchiveFixture


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    assert shutil.which("zstd"), "missing zstd, cannot qualify archive"
    return AttemptArchiveFixture(tmp_path, monkeypatch)


def test_public_positional_contract_and_exception_identity(fleet):
    resolver = prismabuild.resolve_attempt_log
    parameters = list(inspect.signature(resolver).parameters.values())
    assert [p.name for p in parameters] == ["queue_root", "descriptor"]
    assert all(p.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD for p in parameters)
    for name in ("InvalidLogDescriptor", "AttemptLogMissing", "AttemptLogIntegrityError", "AttemptArchiveUnavailable"):
        assert getattr(prismabuild, name) is getattr(archive, name)
    record = fleet.finish(stdout="")
    fact = fleet.descriptors(record)["stdout"]
    assert resolver(fleet.queue.root, fact) == b""
    fleet.retire()
    assert resolver(fleet.queue.root, fact) == b""


@pytest.mark.parametrize("stage", ["chunks", "manifests", "index"])
@pytest.mark.parametrize("kind", ["file", "directory"])
def test_fsync_failure_before_index_commit_preserves_live_evidence(fleet, monkeypatch, stage, kind):
    record = fleet.finish()
    logs = fleet.descriptors(record)
    original = os.fsync
    fired = False
    def fail_target(fd):
        nonlocal fired
        target = Path(os.readlink(f"/proc/self/fd/{fd}"))
        is_directory = target.is_dir()
        if not fired and target.is_relative_to(fleet.root) and stage in target.parts and is_directory == (kind == "directory"):
            fired = True
            raise OSError(errno.EIO, "review injected fsync failure")
        return original(fd)
    with monkeypatch.context() as fault:
        fault.setattr(os, "fsync", fail_target)
        with pytest.raises(retention.AttemptRetentionRefused):
            fleet.retire()
    assert fired
    for fact in logs.values():
        assert (fleet.queue.root / fact["path"]).exists()
        assert len(archive.resolve_attempt_log(fleet.queue.root, fact)) == fact["bytes"]
    assert len(fleet.retire()["removed"]) == 2


@pytest.mark.parametrize("field", ["bytes", "sha256", "compressed_bytes", "compressed_sha256"])
def test_well_formed_addressed_manifest_checks_every_chunk_fact(fleet, field):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    archive._publish_attempt_log_archive(fleet.queue.root, fact)
    manifest = archive._read_attempt_archive_manifest(fleet.config, fact)
    chunk = manifest["chunks"][0]
    if field.endswith("bytes"):
        chunk[field] += 1
    else:
        chunk[field] = "0" * 64
    raw = digests._canonical_bytes(manifest)
    sha = digests.raw_sha256(raw)
    native.publish_immutable(fleet.root / "manifests" / sha[:2] / f"{sha}.json", raw)
    index = archive._attempt_archive_index_path(fleet.config, fact)
    entry = json.loads(index.read_bytes())
    entry["manifest_sha256"] = sha
    index.unlink()
    native.publish_immutable(index, digests._canonical_bytes(entry))
    (fleet.queue.root / fact["path"]).unlink()
    expected = archive.AttemptArchiveUnavailable if field == "compressed_sha256" else archive.AttemptLogIntegrityError
    with pytest.raises(expected):
        archive.resolve_attempt_log(fleet.queue.root, fact)


def test_live_archive_crossover_after_both_live_lookups(fleet, monkeypatch):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    original = archive._check_attempt_archive_store
    crossed = False
    def cross(queue, **kwargs):
        nonlocal crossed
        if not crossed:
            crossed = True
            archive._publish_attempt_log_archive(queue, fact)
            (queue / fact["path"]).unlink()
        return original(queue, **kwargs)
    # The live miss races the other client's durable publication.
    original_live = archive._stream_live_attempt_log
    misses = 0
    def stale_live(path, metadata, consume):
        nonlocal misses
        if misses < 2:
            misses += 1
            raise FileNotFoundError(errno.ENOENT, "review injected live negative cache")
        return original_live(path, metadata, consume)
    with monkeypatch.context() as fault:
        fault.setattr(archive, "_stream_live_attempt_log", stale_live)
        fault.setattr(archive, "_check_attempt_archive_store", cross)
        assert archive.resolve_attempt_log(fleet.queue.root, fact) == b"stdout evidence\n"
    assert crossed and misses == 2


def test_archive_index_io_error_is_unavailable(fleet, monkeypatch):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    fleet.retire()
    index = archive._attempt_archive_index_path(fleet.config, fact)
    original = archive._attempt_archive_json
    def unavailable(path, **kwargs):
        if path == index:
            raise OSError(errno.EIO, "review injected archive I/O error")
        return original(path, **kwargs)
    monkeypatch.setattr(archive, "_attempt_archive_json", unavailable)
    with pytest.raises(archive.AttemptArchiveUnavailable):
        archive.resolve_attempt_log(fleet.queue.root, fact)


@pytest.mark.parametrize("flags", [[], ["--retire-attempt-logs"], ["--apply", "--all-lock-takers-verify"]])
def test_each_incomplete_cli_flag_set_preserves_logs(fleet, flags):
    import pb_gc
    record = fleet.finish()
    logs = fleet.descriptors(record)
    assert pb_gc.main(["--queue-root", str(fleet.queue.root), "--summary", *flags]) == 0
    assert all((fleet.queue.root / fact["path"]).exists() for fact in logs.values())


def test_lease_appearing_during_source_hash_retains_key(fleet, monkeypatch):
    record = fleet.finish()
    original = native.file_identity_nofollow
    fired = False
    def acquire_lease(path, **kwargs):
        nonlocal fired
        result = original(path, **kwargs)
        if not fired and kwargs["where"] == "retiring attempt log":
            fired = True
            native.publish_immutable(fleet.queue.records.lease_path(record["action_key"]), b"{}")
        return result
    monkeypatch.setattr(native, "file_identity_nofollow", acquire_lease)
    result = fleet.retire()
    assert fired and not result["removed"]
    assert all((fleet.queue.root / fact["path"]).exists() for fact in fleet.descriptors(record).values())


def test_source_replacement_during_last_authority_check_is_retained(fleet, monkeypatch):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    path = fleet.queue.root / fact["path"]
    original = retention._recheck_attempt_retirement_authority
    calls = 0
    def replace_during_authority(queue, plan):
        nonlocal calls
        result = original(queue, plan)
        calls += 1
        if calls == 3:
            path.unlink()
            native.publish_immutable(path, b"unarchived replacement evidence\n")
            os.utime(path, (fleet.old, fleet.old))
        return result
    monkeypatch.setattr(retention, "_recheck_attempt_retirement_authority", replace_during_authority)
    result = fleet.retire()
    assert calls >= 3
    assert path.exists(), "retirement removed a different unverified source inode"
    assert not any(row["path"] == fact["path"] for row in result["removed"])


@pytest.mark.parametrize("replacement", ["same_size_inode", "symlink"])
def test_source_replacement_after_verification_before_unlink_is_retained(fleet, monkeypatch, replacement):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    path = fleet.queue.root / fact["path"]
    original, open_parent = native.unlink_nofollow, native.open_directory_nofollow
    before = path.stat()
    changed = False
    unlinking = False
    held = os.open(path, os.O_RDONLY)
    def replace_after_parent_open(target, **kwargs):
        nonlocal changed
        descriptor = open_parent(target, **kwargs)
        if unlinking and target == path.parent and not changed:
            changed = True
            path.unlink()
            raw = b"X" * fact["bytes"]
            if replacement == "symlink":
                other = path.with_suffix(".replacement")
                native.publish_immutable(other, raw)
                path.symlink_to(other)
            else:
                native.publish_immutable(path, raw)
                os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        return descriptor
    def unlink(target, **kwargs):
        nonlocal unlinking
        unlinking = target == path
        try:
            return original(target, **kwargs)
        finally:
            unlinking = False
    monkeypatch.setattr(native, "open_directory_nofollow", replace_after_parent_open)
    monkeypatch.setattr(native, "unlink_nofollow", unlink)
    try:
        result = fleet.retire()
    finally:
        os.close(held)
    assert changed and path.lstat()
    assert path.read_bytes() == b"X" * fact["bytes"]
    assert not any(row["path"] == fact["path"] for row in result["removed"])
    assert any(row["path"] == fact["path"] and "source" in row["reason"]
               and "chang" in row["reason"] for row in result["skipped"])
