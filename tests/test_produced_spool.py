"""Admitted CPU integration: real export action, durable ack, existing stage/lease."""
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket

import pytest

import test_prepaid_writer_integration as fx
from prismabuild import core, pool, produced_output as po, produced_spool as ps
from prismabuild import reader_lease as rlc

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context


def world(tmp_path, *, maximum=256):
    cas_root = tmp_path / "cas"
    template = fx._template(str(tmp_path / "canonical"))
    initial = fx._producer_request(tmp_path, cas_root, template)
    cas, request = po._read_producer_request(cas_root, initial)
    request.pop("action_key")
    request["environment"]["variables"].update({ps.ROOT_ENV: str(tmp_path / "local"),
                                                ps.MAX_ENV: str(maximum)})
    action = core.seal_action(request)
    cas.publish_action_request(action)
    owner = action["action_key"]
    q = fx._queue(tmp_path)
    inst = fx._bind(q, template, owner, cas_root)
    fx._announce_tier(q, tmp_path / "stage")
    spool = ps.ProducedSpool(q, inst, template, cas_root=cas_root,
                             root=tmp_path / "local", max_bytes=maximum)
    return spool


def prepare(spool, batch="b1", payload=b"hello", ceiling=64):
    destination = Path(spool.template["output_prefix"]) / f"{batch}.bin"
    pre = po.require_prewrite(spool.queue, spool.instance, spool.template,
        batch_id=batch, tier=fx.TIER,
        class_bytes={"payload": ceiling, "checkpoint": 0, "temp": 0},
        paths=[str(destination), str(destination) + ".tmp"])
    assert pre["ok"], pre
    directory = spool.reserve_group(batch, ceiling)
    source = directory / f"{batch}.bin"
    # Same inode reserved and written, as the production serializer contract.
    with source.open("x+b") as handle:
        os.posix_fallocate(handle.fileno(), 0, ceiling)
        handle.write(payload)
        handle.truncate(len(payload))
        handle.flush()
        os.fsync(handle.fileno())
    entries = [{"source_path": str(source), "destination_path": str(destination),
                "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}]
    return source, destination, entries


def claim_export(spool, handle):
    claimed = spool.queue.claim(owner="spool-export-test", tags=[spool.host])
    assert claimed is not None and claimed["action_key"] == handle["export_key"]
    assert claimed["resources"] == {"cpu": 1, "mem_gb": 1}
    return claimed


def direct_export(spool, handle):
    group = spool._group("b1")
    record = ps._read(group / "export.json")
    return ps.export_group(spool.queue, group / "manifest.json",
                           record["manifest_sha256"], handle["export_key"])


def test_local_group_exports_as_real_pb_action_then_stages_and_leases(tmp_path):
    spool = world(tmp_path)
    source, destination, entries = prepare(spool)
    handle = spool.submit_group("b1", entries)
    assert handle["ok"] and not destination.exists()
    assert spool.poll_group("b1") == {"ok": True, "complete": False,
                                      "export_key": handle["export_key"]}
    assert not spool.release_group("b1")["ok"]
    assert source.exists()
    claimed = claim_export(spool, handle)
    outcome = spool.queue.execute(claimed, timeout_s=120)
    assert outcome.get("returncode") == 0, outcome
    spool.queue.finish(handle["export_key"], status="executed")
    assert spool.poll_group("b1")["complete"]
    assert destination.read_bytes() == b"hello"
    assert spool.release_group("b1")["ok"] and not source.exists()
    assert spool.release_group("b1")["duplicate"]
    descriptor = po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0", "artifact_class": "payload",
        "path": str(destination), "bytes": 5, "sha256": entries[0]["sha256"],
        "producer_generation": po.mint_generation(),
        "owner_action_key": spool.owner, "owner_attempt": spool.instance["owner_attempt"]},
        spool.template, spool.instance)
    batch = po.publish_prepaid_batch(spool.queue, spool.instance, spool.template,
        [descriptor], batch_id="b1", tier=fx.TIER, cas_root=spool.cas_root,
        producer_action_key=spool.owner, command_extra=["--unpaced"])
    assert batch["ok"], batch
    mover = batch["mover_key"]
    assert fx._claim_mover(spool.queue, "stage-exported")["action_key"] == mover
    receipt = fx._execute_mover(spool.queue, spool.cas_root, mover,
                                tmp_path / "mover-checkout")
    assert receipt["complete"]
    spool.queue.finish(mover, status="executed")
    lease = rlc.acquire(spool.queue,
        consumer_action_key=batch["batch_namespace"],
        attempt={"nonce": secrets.token_hex(16), "scope_id": "spool-test-reader"},
        tier_id=fx.TIER, epoch="", span={"start_bytes": 0, "end_bytes": 5},
        holder={"host": socket.gethostname(), "worker": "reader", "pid": os.getpid()},
        acquire_token=secrets.token_hex(16),
        covers=[{"mover_action_key": mover, "manifest_sha256": batch["manifest_digest"]}],
        expected=None, owner_action_key=fx._hexkey("downstream"),
        residency_root=str(po.output_fragment_root(spool.queue.root / pool.RESIDENCY)))
    assert lease["ok"], lease
    staged = tmp_path / "stage" / "produced-output" / batch["batch_namespace"] / "b1.bin"
    assert staged.read_bytes() == b"hello"


def test_export_ack_retry_adopts_durable_copy_without_opening_payloads(tmp_path, monkeypatch):
    spool = world(tmp_path)
    source, destination, entries = prepare(spool)
    handle = spool.submit_group("b1", entries)
    claim_export(spool, handle)
    original = ps._write
    def interrupted(path, body):
        if Path(path).name == "receipt.json":
            raise RuntimeError("interrupt after durable copies before group acknowledgement")
        return original(path, body)
    monkeypatch.setattr(ps, "_write", interrupted)
    with pytest.raises(RuntimeError, match="interrupt"):
        direct_export(spool, handle)
    assert destination.read_bytes() == b"hello"
    assert not spool.poll_group("b1")["complete"]
    monkeypatch.setattr(ps, "_write", original)
    import builtins
    opener = builtins.open
    def no_payload_read(path, mode="r", *args, **kwargs):
        if Path(path) in {source, destination}:
            raise AssertionError("retry reread a payload")
        return opener(path, mode, *args, **kwargs)
    monkeypatch.setattr(builtins, "open", no_payload_read)
    assert direct_export(spool, handle)["ok"]
    assert spool.poll_group("b1")["complete"]


def test_source_change_and_unacknowledged_cleanup_retain(tmp_path):
    spool = world(tmp_path)
    source, destination, entries = prepare(spool)
    handle = spool.submit_group("b1", entries)
    claim_export(spool, handle)
    source.write_bytes(b"other")
    with pytest.raises(ps.SpoolError, match="source changed"):
        direct_export(spool, handle)
    assert not spool.release_group("b1")["ok"]
    assert source.exists() and not destination.exists()


def test_changed_destination_never_acknowledges_or_releases(tmp_path):
    spool = world(tmp_path)
    source, destination, entries = prepare(spool)
    handle = spool.submit_group("b1", entries)
    claim_export(spool, handle)
    assert direct_export(spool, handle)["ok"]
    destination.write_bytes(b"other")
    assert spool.poll_group("b1")["refusal"] == "export-destination-changed"
    assert not spool.release_group("b1")["ok"] and source.exists()
    with pytest.raises(ps.SpoolError, match="destination changed"):
        direct_export(spool, handle)


def test_spool_bound_and_release_reuses_capacity(tmp_path):
    spool = world(tmp_path, maximum=64)
    source, destination, entries = prepare(spool)
    other = Path(spool.template["output_prefix"]) / "b2.bin"
    assert po.require_prewrite(spool.queue, spool.instance, spool.template,
        batch_id="b2", tier=fx.TIER, class_bytes={"payload":64,"checkpoint":0,"temp":0},
        paths=[str(other),str(other)+".tmp"])["ok"]
    with pytest.raises(ps.SpoolCapacityDeferred):
        spool.reserve_group("b2", 64)
    handle = spool.submit_group("b1", entries)
    claim_export(spool, handle)
    direct_export(spool, handle)
    spool.release_group("b1")
    assert spool.reserve_group("b2", 64).is_dir()


def test_unowned_destination_is_preserved(tmp_path):
    spool = world(tmp_path)
    source, destination, entries = prepare(spool)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"user work")
    handle = spool.submit_group("b1", entries)
    claim_export(spool, handle)
    with pytest.raises(ps.SpoolError, match="unowned"):
        direct_export(spool, handle)
    assert destination.read_bytes() == b"user work"


def test_replay_must_match_original_entries(tmp_path):
    spool = world(tmp_path)
    _, _, entries = prepare(spool)
    first = spool.submit_group("b1", entries)
    assert spool.submit_group("b1", entries)["export_key"] == first["export_key"]
    with pytest.raises(ps.SpoolError, match="replay changed"):
        spool.submit_group("b1", [{**entries[0], "bytes": 4}])


def test_declared_root_and_ceiling_are_sealed(tmp_path):
    spool = world(tmp_path)
    with pytest.raises(ps.SpoolError, match="sealed"):
        ps.ProducedSpool(spool.queue, spool.instance, spool.template,
            cas_root=spool.cas_root, root=tmp_path / "other", max_bytes=256)
    with pytest.raises(ps.SpoolError, match="sealed"):
        ps.ProducedSpool(spool.queue, spool.instance, spool.template,
            cas_root=spool.cas_root, root=tmp_path / "local", max_bytes=257)


def test_interrupted_publication_recopies_local_without_reading_remote(tmp_path, monkeypatch):
    spool = world(tmp_path)
    source, destination, entries = prepare(spool)
    handle = spool.submit_group("b1", entries)
    claim_export(spool, handle)
    original = ps._write
    def stop_after_publication(path, body):
        if Path(path).name == "copy-0.json" and body.get("complete"):
            raise RuntimeError("interrupted after publication")
        return original(path, body)
    monkeypatch.setattr(ps, "_write", stop_after_publication)
    with pytest.raises(RuntimeError, match="interrupted"):
        direct_export(spool, handle)
    assert destination.exists() and not spool.poll_group("b1")["complete"]
    monkeypatch.setattr(ps, "_write", original)
    import builtins
    opener = builtins.open
    def no_remote_read(path, mode="r", *args, **kwargs):
        if Path(path) == destination and "r" in mode:
            raise AssertionError("recovery reread shared payload")
        return opener(path, mode, *args, **kwargs)
    monkeypatch.setattr(builtins, "open", no_remote_read)
    assert direct_export(spool, handle)["ok"]
    assert spool.poll_group("b1")["complete"]


def test_corrupt_reservation_retains_capacity(tmp_path):
    spool = world(tmp_path)
    prepare(spool)
    path = spool._group("b1") / "reservation.json"
    record = ps._read(path)
    record["released"] = "yes"
    ps._write(path, record)
    with pytest.raises(ps.SpoolError, match="malformed"):
        spool.reserve_group("b1", 64)


def test_export_requires_writer_digest_and_no_leftover_files(tmp_path):
    spool = world(tmp_path)
    source, _, entries = prepare(spool)
    with pytest.raises(po.ProducedOutputError):
        spool.submit_group("b1", [{**entries[0], "sha256": None}])
    leftover = source.with_name("unaccounted.tmp")
    leftover.write_bytes(b"x")
    with pytest.raises(ps.SpoolError, match="unaccounted"):
        spool.submit_group("b1", entries)


def test_release_resumes_after_partial_local_unlink(tmp_path):
    spool = world(tmp_path)
    source, _, entries = prepare(spool)
    handle = spool.submit_group("b1", entries)
    claim_export(spool, handle)
    direct_export(spool, handle)
    source.unlink()  # interruption after last unlink, before reservation update
    assert spool.release_group("b1")["ok"]
