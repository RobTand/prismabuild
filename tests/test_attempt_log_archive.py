"""Verified live/archive delivery and bounded tails, using real queue writers."""
from __future__ import annotations

import errno
import json
from pathlib import Path
import shutil
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))
from prismabuild import (resolve_attempt_log, InvalidLogDescriptor, AttemptLogMissing,
                        AttemptLogIntegrityError, AttemptArchiveUnavailable)
from prismabuild import attempt_archive as archive, immutable_files as native, digest_primitives as digests
from attempt_archive_fixture import AttemptArchiveFixture


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    return AttemptArchiveFixture(tmp_path, monkeypatch)


@pytest.fixture
def zstd_required():
    if shutil.which("zstd") is None:
        pytest.skip("zstd CLI is required for the archive format")


def test_real_writer_live_then_archive_equality_and_pool_reader(fleet, zstd_required):
    record = fleet.finish()
    logs = fleet.descriptors(record)
    before = {name: resolve_attempt_log(fleet.queue.root, fact) for name, fact in logs.items()}
    plan = fleet.plan()
    assert plan["candidates"], plan["retained_keys"]
    result = fleet.retire()
    assert len(result["removed"]) == 2 and not result["skipped"]
    assert all(not (fleet.queue.root / fact["path"]).exists() for fact in logs.values())
    assert {name: resolve_attempt_log(fleet.queue.root, fact) for name, fact in logs.items()} == before
    assert fleet.queue.attempt_outcomes(record)[0]["stdout"] == before["stdout"].decode()
    assert fleet.queue.records.attempt_path(record, 1).exists()
    assert fleet.queue.records.item_path("done", record["action_key"]).exists()
    assert fleet.queue.records.item_path("intent", record["action_key"]).exists()
    assert list((fleet.queue.root / "gc-receipts").glob("attempt-logs-*.json"))


def test_multichunk_delivery_and_tail_retains_only_suffix(fleet, zstd_required):
    text = "first\n" + "x" * (archive.MAX_LOG_CHUNK_BYTES + 17) + "\nlast\n"
    record = fleet.finish(stdout=text)
    fact = fleet.descriptors(record)["stdout"]
    archive._publish_attempt_log_archive(fleet.queue.root, fact)
    (fleet.queue.root / fact["path"]).unlink()
    manifest = archive._read_attempt_archive_manifest(fleet.config, fact)
    assert len(manifest["chunks"]) == 2
    assert resolve_attempt_log(fleet.queue.root, fact) == text.encode()
    assert archive._verified_attempt_log_tail(fleet.queue.root, fact, max_bytes=11) == text.encode()[-11:]
    assert archive._verified_attempt_log_tail(fleet.queue.root, fact, max_bytes=0) == b""


@pytest.mark.parametrize("change", ["size", "sha", "traversal", "absolute", "digits", "bool", "number"])
def test_descriptor_errors_have_public_exception(fleet, change):
    record = fleet.finish()
    fact = dict(fleet.descriptors(record)["stdout"])
    if change == "size":
        fact["bytes"] = -1
    elif change == "sha":
        fact["sha256"] = "F" * 64
    elif change == "traversal":
        fact["path"] = "../" + fact["path"]
    elif change == "absolute":
        fact["path"] = "/" + fact["path"]
    elif change == "digits":
        fact["path"] = fact["path"].replace("00000001.", "9" * 5000 + ".")
    elif change == "bool":
        fact["bytes"] = True
    else:
        fact["path"] = fact["path"].replace("00000001.", "000000001.")
    with pytest.raises(InvalidLogDescriptor):
        resolve_attempt_log(fleet.queue.root, fact)


def test_missing_archive_does_not_mean_empty_history(fleet):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    (fleet.queue.root / fact["path"]).unlink()
    with pytest.raises(AttemptLogMissing):
        resolve_attempt_log(fleet.queue.root, fact)
    (fleet.root / "store.json").unlink()
    with pytest.raises(AttemptArchiveUnavailable):
        resolve_attempt_log(fleet.queue.root, fact)


@pytest.mark.parametrize("damage", ["bytes", "length", "symlink", "writable"])
def test_live_integrity_failure_never_falls_back(fleet, zstd_required, damage):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    archive._publish_attempt_log_archive(fleet.queue.root, fact)
    path = fleet.queue.root / fact["path"]
    if damage == "symlink":
        other = path.with_suffix(".saved")
        path.rename(other)
        path.symlink_to(other)
    elif damage == "writable":
        path.chmod(0o644)
    else:
        path.chmod(0o644)
        path.write_bytes(b"x" * (fact["bytes"] + (damage == "length")))
        path.chmod(0o444)
    with pytest.raises(AttemptLogIntegrityError):
        resolve_attempt_log(fleet.queue.root, fact)


@pytest.mark.parametrize("change", ["unlink", "regular", "symlink"])
def test_live_custody_distinguishes_gc_unlink_from_replacement(fleet, change):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    path = fleet.queue.root / fact["path"]
    chunks = []
    def consume(raw):
        chunks.append(raw)
        if len(chunks) == 1:
            path.unlink()
            if change == "regular":
                native.publish_immutable(path, b"substituted")
            elif change == "symlink":
                path.symlink_to(path.with_suffix(".unrelated"))
    if change == "unlink":
        archive._stream_verified_attempt_log(fleet.queue.root, fact, consume)
        assert b"".join(chunks) == b"stdout evidence\n"
    else:
        with pytest.raises(AttemptLogIntegrityError):
            archive._stream_verified_attempt_log(fleet.queue.root, fact, consume)


@pytest.mark.parametrize("damage", ["compressed", "manifest", "index", "missing_chunk", "misbound_store"])
def test_archived_integrity_and_unavailable_errors(fleet, zstd_required, damage):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    archive._publish_attempt_log_archive(fleet.queue.root, fact)
    (fleet.queue.root / fact["path"]).unlink()
    manifest = archive._read_attempt_archive_manifest(fleet.config, fact)
    entry_path = archive._attempt_archive_index_path(fleet.config, fact)
    entry = json.loads(entry_path.read_bytes())
    if damage in ("compressed", "missing_chunk"):
        sha = manifest["chunks"][0]["compressed_sha256"]
        path = fleet.root / "chunks" / sha[:2] / f"{sha}.zst"
    elif damage == "manifest":
        sha = entry["manifest_sha256"]
        path = fleet.root / "manifests" / sha[:2] / f"{sha}.json"
    elif damage == "misbound_store":
        path = fleet.root / "store.json"
    else:
        path = entry_path
    if damage == "missing_chunk":
        path.unlink()
    else:
        path.chmod(0o644)
        path.write_bytes(b"corrupt" if damage != "index" else digests._canonical_bytes({**entry, "queue_id": "f" * 64}))
        path.chmod(0o444)
    expected = AttemptArchiveUnavailable if damage in ("missing_chunk", "misbound_store") else AttemptLogIntegrityError
    with pytest.raises(expected):
        resolve_attempt_log(fleet.queue.root, fact)


def test_decoder_rejects_bomb_and_bad_frame(fleet, zstd_required):
    encoded = archive._attempt_archive_zstd(b"x" * 2048, decode=False)
    fleet.monkeypatch.setattr(archive, "MAX_LOG_CHUNK_BYTES", 1024)
    with pytest.raises(AttemptLogIntegrityError, match="bound"):
        archive._attempt_archive_zstd(encoded, decode=True)
    with pytest.raises(AttemptLogIntegrityError):
        archive._attempt_archive_zstd(b"bad frame", decode=True)


@pytest.mark.parametrize("kind", ["index", "manifest"])
def test_oversized_archive_metadata_is_bounded(fleet, zstd_required, kind):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    archive._publish_attempt_log_archive(fleet.queue.root, fact)
    (fleet.queue.root / fact["path"]).unlink()
    entry_path = archive._attempt_archive_index_path(fleet.config, fact)
    entry = json.loads(entry_path.read_bytes())
    if kind == "index":
        path, size = entry_path, archive.MAX_LOG_INDEX_BYTES
    else:
        sha = entry["manifest_sha256"]
        path, size = fleet.root / "manifests" / sha[:2] / f"{sha}.json", archive.MAX_LOG_MANIFEST_BYTES
    path.chmod(0o644)
    path.write_bytes(b" " * (size + 1))
    path.chmod(0o444)
    with pytest.raises(AttemptLogIntegrityError):
        resolve_attempt_log(fleet.queue.root, fact)


def test_crossover_refreshes_without_replaying_delivered_bytes(fleet, monkeypatch):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    original = archive._stream_live_attempt_log
    calls = 0
    def stale_once(path, descriptor, consume):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FileNotFoundError(errno.ENOENT, "simulated NFS miss")
        return original(path, descriptor, consume)
    monkeypatch.setattr(archive, "_stream_live_attempt_log", stale_once)
    assert resolve_attempt_log(fleet.queue.root, fact) == b"stdout evidence\n"
    assert calls == 2
    def partial_then_missing(path, descriptor, consume):
        consume(b"prefix")
        raise FileNotFoundError(errno.ENOENT, "simulated late missing address")
    monkeypatch.setattr(archive, "_stream_live_attempt_log", partial_then_missing)
    with pytest.raises(AttemptArchiveUnavailable, match="streaming began"):
        resolve_attempt_log(fleet.queue.root, fact)


def test_mcp_archived_verified_tail_and_corrupt_live_refusal(fleet, zstd_required):
    import pbmcp
    record = fleet.finish(stdout="one\ntwo\nthree\n")
    attempt = fleet.queue.records.attempt_document(fleet.queue.records.attempt_path(record, 1))
    fleet.retire()
    service = pbmcp.Session(queue_root=fleet.queue.root, cas_root=fleet.queue.root.parent / "cas",
                            repo_link=fleet.queue.root.parent / "repo", log_tail_bytes=12)
    tail = service._adopted_log(attempt, tail_lines=1)
    assert tail["present"] and tail["verified"] and tail["lines"] == ["three"]
    assert tail["retained_bytes"] <= 12 and tail["bytes_read"] == len(b"one\ntwo\nthree\n")
    fact = attempt["logs"]["stdout"]
    native.publish_immutable(fleet.queue.root / fact["path"], b"bad")
    refused = service._adopted_log(attempt)
    assert not refused["present"] and not refused["verified"] and "lines" not in refused


def test_bounded_action_result_log_reader_uses_archive(fleet, zstd_required):
    from prismabuild import action_result
    record = fleet.finish()
    attempt = fleet.queue.records.attempt_document(fleet.queue.records.attempt_path(record, 1))
    fleet.retire()
    address, raw = action_result._verified_attempt_log(fleet.queue, record, attempt, 1, "stdout", 1024)
    assert raw == b"stdout evidence\n" and address["bytes"] == len(raw)
    with pytest.raises(action_result.ActionResultError, match="evidence cap"):
        action_result._verified_attempt_log(fleet.queue, record, attempt, 1, "stdout", 1)


def test_archive_rejects_wrong_synchronous_backing(fleet, monkeypatch):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    original = archive.subprocess.run
    def async_dataset(argv, **kwargs):
        result = original(argv, **kwargs)
        if argv[0] == "zfs":
            result.stdout = result.stdout.replace(b"sync\tstandard", b"sync\tdisabled")
        return result
    monkeypatch.setattr(archive.subprocess, "run", async_dataset)
    with pytest.raises(AttemptArchiveUnavailable, match="backing dataset"):
        archive._publish_attempt_log_archive(fleet.queue.root, fact)
    assert (fleet.queue.root / fact["path"]).exists()


def test_apply_rejects_another_dataset_even_with_matching_marker(fleet):
    fleet.config["dataset"] = "nvme/scratch-evidence"
    fleet.write_config()
    marker_path = fleet.root / "store.json"
    marker = json.loads(marker_path.read_bytes())
    marker["dataset"] = fleet.config["dataset"]
    marker_path.unlink()
    native.publish_immutable(marker_path, digests._canonical_bytes(marker))
    with pytest.raises(AttemptArchiveUnavailable, match="approved HDD dataset"):
        archive._check_attempt_archive_store(fleet.queue.root, for_apply=True)


def test_codec_deadline_cancels_a_process_that_never_writes(tmp_path, monkeypatch):
    import os
    child_script = tmp_path / "zstd"
    child_script.write_text(f"#!{sys.executable}\nimport signal\nsignal.pause()\n")
    child_script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setattr(archive, "LOG_CODEC_TIMEOUT_S", 0.2)
    popen, children = archive.subprocess.Popen, []
    def observe(*args, **kwargs):
        child = popen(*args, **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(archive.subprocess, "Popen", observe)
    with pytest.raises(AttemptArchiveUnavailable, match="deadline expired"):
        archive._attempt_archive_zstd(b"input", decode=False)
    assert len(children) == 1 and children[0].poll() is not None
