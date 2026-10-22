"""Real writer/GC flow, rollout refusal, cursor fairness and crash recovery."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import errno
import json
import os
from pathlib import Path
import shutil
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))
from prismabuild import attempt_archive as archive, attempt_retention as retention
from prismabuild import immutable_files as native, digest_primitives as digests, action_edges, pool
from attempt_archive_fixture import AttemptArchiveFixture


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    return AttemptArchiveFixture(tmp_path, monkeypatch)


@pytest.fixture
def zstd_required():
    if shutil.which("zstd") is None:
        pytest.skip("zstd CLI is required for the archive format")


def test_default_short_window_and_hard_minimum(fleet):
    fleet.finish()
    assert fleet.plan()["retention_days"] == 14
    fleet.config["retention_days"] = 1
    fleet.write_config()
    with pytest.raises(retention.AttemptRetentionRefused):
        fleet.plan()  # Old gate binds the prior config, even in survey.
    fleet.gate["config_sha256"] = digests.canonical_sha256(fleet.config)
    fleet.write_gate()
    assert fleet.plan()["retention_days"] == 1
    fleet.config["retention_days"] = 0.49
    fleet.write_config()
    with pytest.raises(retention.AttemptRetentionRefused, match="12 hours"):
        fleet.plan()


@pytest.mark.parametrize("damage", ["expired", "approval", "role", "old_reader", "receipt", "source",
                                    "changed_worker", "draining_changed", "missing_tag", "stale_offer", "mixed_tag"])
def test_gate_refuses_unqualified_or_changed_readers(fleet, damage):
    fleet.finish()
    if damage == "expired":
        fleet.gate["expires_unix"] = fleet.now - 1
    elif damage == "approval":
        del fleet.gate["approvals"]["tl_lead"]
    elif damage == "role":
        fleet.gate["members"].pop()
    elif damage == "old_reader":
        fleet.gate["members"][-1]["generation"] = "unknown-old-reader"
    elif damage in ("receipt", "source"):
        path = fleet.directory / ("RUNTIME_VERSION.json" if damage == "receipt" else "src/prismabuild/attempt_archive.py")
        path.chmod(0o644)
        path.write_bytes(b"changed after qualification")
        path.chmod(0o444)
    elif damage in ("changed_worker", "draining_changed"):
        fleet.offer("unknown-old-generation", state="draining" if damage == "draining_changed" else None)
    elif damage == "stale_offer":
        fleet.offer(announced=fleet.now - 3600)
    else:
        path = fleet.queue.root / "workers/test-host.json"
        offer = json.loads(path.read_bytes())
        offer["tags"] = [] if damage == "missing_tag" else offer["tags"] + ["runtime-generation:other"]
        native.write_bytes_atomic(path, digests._canonical_bytes(offer))
    fleet.write_gate()
    plan = fleet.plan()
    assert not plan["reader_gate"]["verified"]
    with pytest.raises(retention.AttemptRetentionRefused):
        retention.apply_attempt_log_retention(fleet.queue, plan)


def test_draining_exact_loaded_generation_is_included(fleet):
    fleet.offer(state="draining")
    assert fleet.plan()["reader_gate"]["verified"]


@pytest.mark.parametrize("owner", ["ready", "claimed", "lease", "tombstone", "late_finish", "pending_finish", "intent", "held"])
def test_active_ownership_retains_every_generation(fleet, owner):
    old = fleet.finish()
    current = fleet.finish()
    key = current["action_key"]
    if owner in ("ready", "claimed"):
        native.write_bytes_atomic(fleet.queue.records.item_path(owner, key), b"{}")
    elif owner in ("lease", "tombstone", "late_finish"):
        suffix = {"lease": ".lease", "tombstone": ".owner.tombstone", "late_finish": ".owner.late-finish"}[owner]
        native.write_bytes_atomic(fleet.queue.root / "claimed" / f"{key}{suffix}", b"{}")
    elif owner == "pending_finish":
        path = fleet.queue.records.item_path("done", key)
        current["finish_pending"] = {"status": "executed", "detail": {}}
        native.write_bytes_atomic(path, digests._canonical_bytes(current))
        os.utime(path, (fleet.old, fleet.old))
    elif owner == "intent":
        path = fleet.queue.records.item_path("intent", key)
        intent = json.loads(path.read_bytes())
        intent["intent_unix"] = fleet.now
        native.write_bytes_atomic(path, digests._canonical_bytes(intent))
    else:
        path = fleet.queue.root / "reservations/host/held" / key / "cpu-0"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
    assert fleet.plan()["eligible_bytes"] == 0
    assert all((fleet.queue.root / fact["path"]).exists() for record in (old, current)
               for fact in fleet.descriptors(record).values())


def test_old_intent_empty_holder_and_reader_retirement_mark_are_historical(fleet, zstd_required):
    record = fleet.finish()
    key = record["action_key"]
    (fleet.queue.root / "reservations/host/held" / key).mkdir(parents=True, exist_ok=True)
    native.publish_immutable(fleet.queue.root / "residency/leases" / key / "mover.retiring.json", b"{}")
    assert fleet.plan()["eligible_bytes"] > 0
    result = fleet.retire()
    assert len(result["removed"]) == 2 and not result["skipped"]
    assert fleet.queue.records.item_path("intent", key).exists()


def test_container_marker_uses_recorded_owner_and_is_rechecked(fleet, zstd_required, monkeypatch):
    owner = "7" * 64
    record = fleet.finish(container_owner=owner)
    marker = fleet.queue.container_marker(owner)
    native.publish_immutable(marker, b"used")
    assert fleet.plan()["retained_keys"] == {"container ownership present": 1}
    marker.unlink()
    plan = fleet.plan()
    assert plan["candidates"]
    publish = archive._publish_attempt_log_archive
    def publish_then_own(*args):
        result = publish(*args)
        native.publish_immutable(marker, b"used")
        return result
    monkeypatch.setattr(archive, "_publish_attempt_log_archive", publish_then_own)
    result = retention.apply_attempt_log_retention(fleet.queue, plan)
    assert not result["removed"] and result["skipped"]
    assert all((fleet.queue.root / fact["path"]).exists() for fact in fleet.descriptors(record).values())


def test_corruption_stops_after_completed_prefix_and_files_receipt(fleet, zstd_required):
    first = fleet.finish("d" * 64)
    later = fleet.finish("e" * 64)
    bad = fleet.descriptors(first)["stderr"]
    archive._publish_attempt_log_archive(fleet.queue.root, bad)
    manifest = archive._read_attempt_archive_manifest(fleet.config, bad)
    sha = manifest["chunks"][0]["compressed_sha256"]
    chunk = fleet.root / "chunks" / sha[:2] / f"{sha}.zst"
    chunk.chmod(0o644)
    chunk.write_bytes(b"corrupt")
    chunk.chmod(0o444)
    with pytest.raises(retention.AttemptRetentionRefused, match="retirement stopped"):
        retention.apply_attempt_log_retention(fleet.queue, fleet.plan())
    receipt_paths = list((fleet.queue.root / "gc-receipts").glob("attempt-logs-*.json"))
    assert len(receipt_paths) == 1
    receipt = json.loads(receipt_paths[0].read_bytes())
    assert receipt["aborted"] and len(receipt["removed"]) == 1
    assert (fleet.queue.root / bad["path"]).exists()
    assert all((fleet.queue.root / fact["path"]).exists() for fact in fleet.descriptors(later).values())
    assert archive.resolve_attempt_log(fleet.queue.root, fleet.descriptors(first)["stdout"])


@pytest.mark.parametrize("failure", ["missing_chunk", "publication_io", "codec_exit"])
def test_archive_io_failure_stops_remaining_batch_with_prefix_receipt(
    fleet, zstd_required, monkeypatch, tmp_path, failure,
):
    first = fleet.finish("d" * 64)
    later = fleet.finish("e" * 64)
    logs = fleet.descriptors(first)
    first_path = fleet.queue.root / logs["stdout"]["path"]
    target = logs["stderr"]
    publish, stream = archive._publish_attempt_log_archive, archive._stream_archived_attempt_log
    calls = 0
    def readback(config, fact, consume):
        nonlocal calls
        if fact == target:
            calls += 1
            if failure == "missing_chunk" and calls == 3:
                manifest = archive._read_attempt_archive_manifest(config, fact)
                sha = manifest["chunks"][0]["compressed_sha256"]
                (fleet.root / "chunks" / sha[:2] / f"{sha}.zst").unlink()
        return stream(config, fact, consume)
    def prepare(queue_root, fact):
        if fact == target and failure == "publication_io":
            assert not first_path.exists()
            raise OSError(errno.EIO, "simulated archive publication failure")
        return publish(queue_root, fact)
    monkeypatch.setattr(archive, "_publish_attempt_log_archive", prepare)
    monkeypatch.setattr(archive, "_stream_archived_attempt_log", readback)
    if failure == "codec_exit":
        codec = tmp_path / "zstd"
        codec.write_text(f"#!{sys.executable}\nimport os, signal\nos.close(1)\nsignal.pause()\n")
        codec.chmod(0o755)
        monkeypatch.setattr(archive, "LOG_CODEC_TIMEOUT_S", 2)
        unlink = native.unlink_nofollow
        def remove_then_stall(path, **kwargs):
            unlink(path, **kwargs)
            if path == first_path:
                monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
        monkeypatch.setattr(native, "unlink_nofollow", remove_then_stall)
    with pytest.raises(retention.AttemptRetentionRefused, match="retirement stopped"):
        retention.apply_attempt_log_retention(fleet.queue, fleet.plan())
    receipt_path, = (fleet.queue.root / "gc-receipts").glob("attempt-logs-*.json")
    receipt = json.loads(receipt_path.read_bytes())
    assert receipt["aborted"] and len(receipt["removed"]) == 1
    if failure == "codec_exit":
        assert "exit deadline expired" in receipt["aborted"]
    assert not first_path.exists()
    assert (fleet.queue.root / target["path"]).exists()
    assert all((fleet.queue.root / fact["path"]).exists() for fact in fleet.descriptors(later).values())


@pytest.mark.parametrize("field,value", [("schema", "foreign.schema"), ("status", "failed"),
                                         ("published_unix", True), ("finished_unix", True)])
def test_terminal_requires_real_schema_status_and_generation(fleet, field, value):
    record = fleet.finish()
    path = fleet.queue.records.item_path("done", record["action_key"])
    record[field] = value
    native.write_bytes_atomic(path, digests._canonical_bytes(record))
    os.utime(path, (fleet.old, fleet.old))
    assert not fleet.plan()["candidates"]


def test_both_window_timestamps_and_mtime_are_required(fleet):
    record = fleet.finish()
    fact = fleet.descriptors(record)["stdout"]
    path = fleet.queue.root / fact["path"]
    os.utime(path, (fleet.now, fleet.now))
    assert not fleet.plan()["candidates"]
    os.utime(path, (fleet.old, fleet.old))
    terminal_path = fleet.queue.records.item_path("done", record["action_key"])
    os.utime(terminal_path, (fleet.now, fleet.now))
    assert not fleet.plan()["candidates"]


def test_all_old_generations_retire_but_json_and_references_stay(fleet, zstd_required):
    first = fleet.finish()
    last = fleet.finish()
    key = first["action_key"]
    # Foreign references need no new fencing under the lead's logs-only exception.
    reference = action_edges.supersession_path(fleet.queue.root, "f" * 64)
    native.publish_immutable(reference, digests._canonical_bytes(
        dict(schema=action_edges.SUPERSESSION_SCHEMA_V1, old="f" * 64, new=key, new_kind="key")))
    sidecar = fleet.queue.records.attempt_path(first, 1).with_name("00000001.receipt-reconciliation.json")
    native.publish_immutable(sidecar, b"{}")
    before = {fact["path"]: archive.resolve_attempt_log(fleet.queue.root, fact)
              for record in (first, last) for fact in fleet.descriptors(record).values()}
    plan = fleet.plan()
    assert len(plan["candidates"][0]["logs"]) == 4
    result = retention.apply_attempt_log_retention(fleet.queue, plan)
    assert len(result["removed"]) == 4 and not result["skipped"]
    assert reference.exists() and sidecar.exists()
    for record in (first, last):
        assert fleet.queue.records.attempt_path(record, 1).exists()
        for fact in fleet.descriptors(record).values():
            assert archive.resolve_attempt_log(fleet.queue.root, fact) == before[fact["path"]]


def test_multi_pass_cursor_skips_retained_keys_and_wraps_for_earlier_inserts(fleet, zstd_required):
    # More keys than a page; retained terminal JSON remains after every pass.
    records = [fleet.finish(f"{number:064x}") for number in range(2, 8)]
    retained_key = records[0]["action_key"]
    native.write_bytes_atomic(fleet.queue.records.lease_path(retained_key), b"{}")
    visited, retired = set(), 0
    for _ in range(3):
        plan = fleet.plan(max_keys=2)
        visited.update(row["key"] for row in plan["candidates"])
        result = retention.apply_attempt_log_retention(fleet.queue, plan)
        retired += len(result["removed"])
        assert retention.checkpoint_attempt_log_survey(fleet.queue.root, plan)
    assert retired == 10 and len(visited) == 5
    earlier = fleet.finish(f"{1:064x}")
    plan = fleet.plan(max_keys=2)
    assert plan["wrapped"] and plan["candidates"][0]["key"] == earlier["action_key"]
    assert len(retention.apply_attempt_log_retention(fleet.queue, plan)["removed"]) == 2
    assert retention.checkpoint_attempt_log_survey(fleet.queue.root, plan)
    assert (fleet.queue.root / fleet.descriptors(records[0])["stdout"]["path"]).exists()


def test_cursor_compare_and_swap_rejects_stale_concurrent_survey(fleet):
    for number in range(1, 4):
        fleet.finish(f"{number:064x}")
    first, stale = fleet.plan(max_keys=1), fleet.plan(max_keys=2)
    assert retention.checkpoint_attempt_log_survey(fleet.queue.root, first)
    assert not retention.checkpoint_attempt_log_survey(fleet.queue.root, stale)
    assert fleet.plan(max_keys=1)["next_cursor"] == f"{2:064x}"


def test_window_increase_during_chunk_preparation_prevents_unlink(fleet, zstd_required, monkeypatch):
    fleet.finish()
    plan = fleet.plan()
    publish = archive._publish_attempt_log_archive
    def prepare_then_increase(queue_root, fact):
        result = publish(queue_root, fact)
        fleet.config["retention_days"] = 40
        fleet.write_config()
        fleet.gate["config_sha256"] = digests.canonical_sha256(fleet.config)
        fleet.write_gate()
        return result
    monkeypatch.setattr(archive, "_publish_attempt_log_archive", prepare_then_increase)
    result = retention.apply_attempt_log_retention(fleet.queue, plan)
    assert not result["removed"] and result["skipped"]
    assert all((fleet.queue.root / fact["path"]).exists() for fact in plan["candidates"][0]["logs"])


def test_requeue_after_archive_publication_keeps_every_old_log(fleet, zstd_required, monkeypatch):
    record = fleet.finish()
    plan = fleet.plan()
    publish = archive._publish_attempt_log_archive
    changed = False
    def prepare_then_requeue(queue_root, fact):
        nonlocal changed
        result = publish(queue_root, fact)
        if not changed:
            changed = True
            fleet.writer.publish(action_key=record["action_key"], cas_root=str(fleet.cas),
                                 checkout_root=str(fleet.checkout), worker_script=str(fleet.worker_script),
                                 max_attempts=1)
        return result
    monkeypatch.setattr(archive, "_publish_attempt_log_archive", prepare_then_requeue)
    result = retention.apply_attempt_log_retention(fleet.queue, plan)
    assert not result["removed"] and result["skipped"]


class SimulatedRetirementCrash(BaseException):
    pass


@pytest.mark.parametrize("boundary", ["chunks", "manifests", "index", "verified", "before_unlink", "after_unlink", "receipt"])
def test_interruptions_leave_resolvable_prefix_and_retry_adopts_publications(fleet, zstd_required, monkeypatch, boundary):
    record = fleet.finish()
    logs = fleet.descriptors(record)
    plan = fleet.plan()
    fired = False
    original_publish, original_unlink = native.publish_immutable, native.unlink_nofollow
    original_stream = archive._stream_archived_attempt_log
    def crash_once():
        nonlocal fired
        if not fired:
            fired = True
            raise SimulatedRetirementCrash()
    def publish(path, raw, **kwargs):
        result = original_publish(path, raw, **kwargs)
        if (boundary in ("chunks", "manifests", "index") and boundary in path.parts) or (
                boundary == "receipt" and path.parent.name == "gc-receipts"):
            crash_once()
        return result
    def unlink(path, **kwargs):
        if boundary == "before_unlink":
            crash_once()
        result = original_unlink(path, **kwargs)
        if boundary == "after_unlink":
            crash_once()
        return result
    def stream(config, fact, consume):
        result = original_stream(config, fact, consume)
        if boundary == "verified":
            crash_once()
        return result
    with monkeypatch.context() as faults:
        faults.setattr(native, "publish_immutable", publish)
        faults.setattr(native, "unlink_nofollow", unlink)
        faults.setattr(archive, "_stream_archived_attempt_log", stream)
        with pytest.raises(SimulatedRetirementCrash):
            retention.apply_attempt_log_retention(fleet.queue, plan)
    assert fired
    for fact in logs.values():
        assert archive.resolve_attempt_log(fleet.queue.root, fact)
    fleet.retire()
    assert all(not (fleet.queue.root / fact["path"]).exists() for fact in logs.values())
    assert fleet.queue.attempt_outcomes(record)[0]["stdout"] == "stdout evidence\n"


def test_concurrent_collectors_preserve_exact_archive(fleet, zstd_required):
    record = fleet.finish()
    plan = fleet.plan()
    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(lambda _: retention.apply_attempt_log_retention(fleet.queue, plan), range(2)))
    assert sum(len(result["removed"]) for result in results) == 2
    assert fleet.queue.attempt_outcomes(record)[0]["stdout"] == "stdout evidence\n"


def test_logs_only_cli_does_not_remove_legacy_metadata(fleet, zstd_required, monkeypatch):
    import pb_gc
    record = fleet.finish()
    def legacy_forbidden(*args, **kwargs):
        raise AssertionError("logs-only apply must not run unrelated metadata sweep")
    monkeypatch.setattr(pb_gc, "sweep_queue", legacy_forbidden)
    assert pb_gc.main(["--queue-root", str(fleet.queue.root), "--summary", "--apply",
                       "--all-lock-takers-verify", "--retire-attempt-logs"]) == 0
    assert fleet.queue.records.item_path("done", record["action_key"]).exists()
    assert fleet.queue.records.attempt_path(record, 1).exists()


def test_source_inode_replacement_after_archive_is_retained(fleet, zstd_required, monkeypatch):
    record = fleet.finish()
    plan = fleet.plan()
    publish = archive._publish_attempt_log_archive
    changed = False
    def replace_after_archive(queue_root, fact):
        nonlocal changed
        result = publish(queue_root, fact)
        if not changed:
            changed = True
            path = queue_root / fact["path"]
            raw = path.read_bytes()
            path.unlink()
            native.publish_immutable(path, raw)
            os.utime(path, (fleet.old, fleet.old))
        return result
    monkeypatch.setattr(archive, "_publish_attempt_log_archive", replace_after_archive)
    result = retention.apply_attempt_log_retention(fleet.queue, plan)
    fact = fleet.descriptors(record)["stdout"]
    assert (fleet.queue.root / fact["path"]).exists()
    assert result["skipped"]


def test_gate_expiring_after_source_verification_does_not_remove(fleet, zstd_required, monkeypatch):
    fleet.finish()
    plan = fleet.plan()
    original = native.file_identity_nofollow
    def verify_then_expire(path, **kwargs):
        result = original(path, **kwargs)
        if kwargs["where"] == "retiring attempt log":
            fleet.gate["expires_unix"] = fleet.now - 1
            fleet.write_gate()
        return result
    monkeypatch.setattr(native, "file_identity_nofollow", verify_then_expire)
    result = retention.apply_attempt_log_retention(fleet.queue, plan)
    assert not result["removed"] and result["skipped"]


def test_foreign_reference_publication_during_retirement_remains_resolvable(fleet, zstd_required, monkeypatch):
    record = fleet.finish()
    old = fleet.finish("f" * 64, status="failed")
    original = native.file_identity_nofollow
    filed = False
    def verify_with_foreign_supersession(path, **kwargs):
        nonlocal filed
        result = original(path, **kwargs)
        if not filed and kwargs["where"] == "retiring attempt log":
            filed = True
            action_edges.file_supersession(fleet.queue, old["action_key"],
                                          new=record["action_key"], new_kind=action_edges.PRODUCER_KEY)
        return result
    monkeypatch.setattr(native, "file_identity_nofollow", verify_with_foreign_supersession)
    result = fleet.retire()
    assert filed and len(result["removed"]) == 4
    assert action_edges.read_supersession(fleet.queue.root, old["action_key"])["new"] == record["action_key"]
    assert fleet.queue.attempt_outcomes(record)[0]["stdout"] == "stdout evidence\n"


def test_uninventoried_deferred_reader_refuses_gate(fleet):
    native.publish_immutable(fleet.queue.root / "deferred" / f"{'f' * 64}.json", b"{}")
    assert not fleet.plan()["reader_gate"]["verified"]


def test_receipt_bound_source_must_declare_supported_protocol(fleet):
    source_path = fleet.directory / "src/prismabuild/attempt_archive.py"
    source = source_path.read_bytes().replace(b"ATTEMPT_LOG_RESOLVER_PROTOCOL = 1", b"ATTEMPT_LOG_RESOLVER_PROTOCOL = 2")
    source_path.chmod(0o644)
    source_path.write_bytes(source)
    source_path.chmod(0o444)
    receipt_path = fleet.directory / "RUNTIME_VERSION.json"
    receipt = json.loads(receipt_path.read_bytes())
    receipt["files"]["src/prismabuild/attempt_archive.py"] = digests.raw_sha256(source)
    raw = digests._canonical_bytes(receipt)
    receipt_path.chmod(0o644)
    receipt_path.write_bytes(raw)
    receipt_path.chmod(0o444)
    fleet.generation_row.update(receipt_sha256=digests.raw_sha256(raw),
                                content_sha256=digests.canonical_sha256(receipt["files"]))
    fleet.write_gate()
    assert not fleet.plan()["reader_gate"]["verified"]
