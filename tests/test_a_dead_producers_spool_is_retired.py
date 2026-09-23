"""A producer's host spool is retired once its attempt has ended (#1001).

On sparklina a killed producer left 21 GiB of payloads, a withdrawn one
12 GiB, and a finished one 161 group directories of records.  Only the live
producer's own ``release_group`` ever unlinked a payload, so a producer
that died first leaked its whole window.

The retirement tick walks the spool roots the producers' sealed environments
name.  For a namespace whose owner attempt ended, a group with a durable
export acknowledgement is released by ``release_group``'s identity checks,
a group whose export failed, was withdrawn or was never submitted is
discarded, and a group whose export is ready or claimed is kept byte for
byte.  Every retirement is a line in ``produced-spool-retirements``.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import test_prepaid_writer_integration as fx
from test_produced_spool import claim_export, prepare, world
from prismabuild import pool, produced_spool as ps
import worker_loop


def _tree(root: Path) -> dict[str, tuple]:
    """Every entry under ``root`` with its type, size, mode and content digest."""

    found = {}
    for parent, directories, files in os.walk(root):
        for name in directories + files:
            path = Path(parent) / name
            info = path.lstat()
            digest = (hashlib.sha256(path.read_bytes()).hexdigest()
                      if path.is_file() and not path.is_symlink() else None)
            found[str(path.relative_to(root))] = (info.st_mode, info.st_size, digest)
    return found


def _export(spool, batch):
    group = spool._group(batch)
    record = ps._read(group / "export.json")
    return ps.export_group(spool.queue, group / "manifest.json",
                           record["manifest_sha256"], record["export_key"])


def _acknowledged(spool, batch, payload):
    _source, _destination, entries = prepare(spool, batch=batch, payload=payload)
    handle = spool.submit_group(batch, entries)
    claim_export(spool, handle)
    assert _export(spool, batch)["ok"]
    spool.queue.finish(handle["export_key"], status="executed")
    assert spool.poll_group(batch)["complete"]
    return handle


def _failed(spool, batch, payload):
    _source, _destination, entries = prepare(spool, batch=batch, payload=payload)
    handle = spool.submit_group(batch, entries)
    key = handle["export_key"]
    while ps.po._mover_live_state(spool.queue, key) == pool.READY:
        claimed = spool.queue.claim(owner="export-fails", tags=[spool.host])
        assert claimed is not None and claimed["action_key"] == key
        spool.queue.finish(key, status="failed")
    assert ps.po._mover_live_state(spool.queue, key) == pool.FAILED
    return handle


def _withdraw_owner(spool):
    spool.queue.withdraw(spool.owner, reason="test: the producer was withdrawn")
    spool.queue.finish(spool.owner, status="failed")
    assert ps.po._producer_attempt_state(spool.queue, spool.instance) == "dead"


def test_a_withdrawn_owners_acknowledged_and_failed_groups_are_retired(tmp_path):
    spool = world(tmp_path)
    _acknowledged(spool, "b1", b"acknowledged")
    failed = _failed(spool, "b2", b"never-exported!")
    _withdraw_owner(spool)
    namespace = spool.directory
    assert (namespace / "b1" / "payload").is_dir()
    assert (namespace / "b2" / "payload" / "b2.bin").read_bytes() == b"never-exported!"

    tick = ps.retirement_tick(spool.queue, spool.cas_root, host="spool-host")

    # The root came from the producer's sealed environment, not from the test.
    assert tick["roots"] == [str(tmp_path / "local")]
    assert not namespace.exists()
    assert list((tmp_path / "local").iterdir()) == []
    records = ps.retirement_records(spool.queue)
    assert len(records["retirements"]) == 1
    record = records["retirements"][0]
    assert record["schema"] == ps.RETIREMENT_SCHEMA
    assert (record["host"], record["namespace"], record["owner"]) == (
        "spool-host", namespace.name, spool.owner)
    assert record["namespace_removed"] is True
    assert [(g["batch_id"], g["released"], g["bytes"])
            for g in record["groups_released"]] == [("b1", "export-acknowledged", 12)]
    assert [(g["batch_id"], g["discarded"], g["bytes"], g["export_key"])
            for g in record["groups_discarded"]] == [
                ("b2", "export-failed", 15, failed["export_key"])]
    assert record["bytes"] == 27 and record["groups_kept"] == []
    assert record["held_s"] >= 0 and record["seconds"] >= 0
    assert records["ticks"]["spool-host"]["retired"] == 1
    # The canonical output of the acknowledged group is untouched.
    assert (Path(spool.template["output_prefix"]) / "b1.bin").read_bytes() == b"acknowledged"
    # A second tick finds nothing and files nothing new.
    again = ps.retirement_tick(spool.queue, spool.cas_root, host="spool-host")
    assert again["namespaces"] == 0
    assert len(ps.retirement_records(spool.queue)["retirements"]) == 1


def test_a_live_owners_spool_stays_byte_for_byte(tmp_path):
    spool = world(tmp_path)
    _acknowledged(spool, "b1", b"acknowledged")
    _failed(spool, "b2", b"never-exported!")
    before = _tree(tmp_path / "local")

    tick = ps.retirement_tick(spool.queue, spool.cas_root, host="spool-host")

    assert _tree(tmp_path / "local") == before
    assert tick["kept"] == [{"namespace": spool.directory.name,
                             "root": str(tmp_path / "local"),
                             "reason": "owner-attempt-live"}]
    assert ps.retirement_records(spool.queue)["retirements"] == []


def test_a_claimed_export_is_never_touched(tmp_path):
    spool = world(tmp_path)
    _acknowledged(spool, "b1", b"acknowledged")
    _source, _destination, entries = prepare(spool, batch="b2", payload=b"in flight")
    running = spool.submit_group("b2", entries)
    claim_export(spool, running)
    _withdraw_owner(spool)
    assert ps.po._mover_live_state(spool.queue, running["export_key"]) == pool.CLAIMED
    group = spool.directory / "b2"
    before = _tree(group)

    tick = ps.retirement_tick(spool.queue, spool.cas_root, host="spool-host")

    assert _tree(group) == before
    record = ps.retirement_records(spool.queue)["retirements"][0]
    assert record["namespace_removed"] is False
    assert [g["batch_id"] for g in record["groups_released"]] == ["b1"]
    assert record["groups_kept"] == [{"batch_id": "b2", "kept": "export-claimed",
                                      "export_key": running["export_key"]}]
    assert not (spool.directory / "b1").exists()
    assert tick["kept"][0]["reason"] == ["export-claimed"]


def test_a_finished_owners_released_records_are_removed(tmp_path):
    """The 59 MB case: payloads released by the producer, records left behind."""

    spool = world(tmp_path)
    _acknowledged(spool, "b1", b"acknowledged")
    assert spool.release_group("b1")["ok"]
    spool.queue.finish(spool.owner, status="executed")
    assert ps.po._producer_attempt_state(spool.queue, spool.instance) == "succeeded"
    ps.retirement_tick(spool.queue, spool.cas_root, host="spool-host")
    assert not spool.directory.exists()
    record = ps.retirement_records(spool.queue)["retirements"][0]
    assert record["groups_released"] == [
        {"batch_id": "b1", "released": "released-by-producer", "bytes": 0}]


def test_the_worker_loop_ticks_once_per_interval_on_one_loop(tmp_path, monkeypatch):
    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", tmp_path / "roles")
    calls = []
    monkeypatch.setattr(ps, "retirement_tick",
                        lambda queue, cas_root, host: calls.append(host) or {"ok": True})
    queue = fx._queue(tmp_path)
    now = 1_000_000.0
    assert worker_loop.spool_retirement(queue, host="h", cas_root=tmp_path, now=now)
    assert worker_loop.spool_retirement(
        queue, host="h", cas_root=tmp_path, now=now + ps.RETIRE_INTERVAL_S - 1) is None
    assert worker_loop.spool_retirement(
        queue, host="h", cas_root=tmp_path, now=now + ps.RETIRE_INTERVAL_S)
    assert calls == ["h", "h"]


def test_a_tick_that_raises_is_recorded_beside_the_ticks(tmp_path, monkeypatch):
    monkeypatch.setattr(worker_loop, "ROLE_LOCK_ROOT", tmp_path / "roles")

    def broken(queue, cas_root, host):
        raise OSError(5, "spool root unreadable")

    monkeypatch.setattr(ps, "retirement_tick", broken)
    queue = fx._queue(tmp_path)
    try:
        worker_loop.spool_retirement(queue, host="h", cas_root=tmp_path, now=1_000_000.0)
    except OSError:
        pass
    else:
        raise AssertionError("the tick's exception must reach the loop")
    tick = ps.retirement_records(queue)["ticks"]["h"]
    assert tick["schema"] == ps.TICK_SCHEMA
    assert tick["failed"] == "OSError: [Errno 5] spool root unreadable"
