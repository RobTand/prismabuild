"""Publication filters address candidates before canonical namespace reads."""

import json
import os
from pathlib import Path
import socket
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pbmcp
import pbmcp_fixture as fx
from prismabuild import pool


@pytest.mark.parametrize("predicate", ["checkout_root", "published_by", "max_age_s"])
def test_metadata_filters_never_enumerate_canonical_namespaces(tmp_path, monkeypatch, predicate):
    fleet = fx.build(tmp_path)
    native = os.scandir
    forbidden = {fleet.queue_root / state for state in pbmcp.STATES}

    def scandir(path):
        if not isinstance(path, int):
            assert Path(path) not in forbidden, f"enumerated canonical namespace {path}"
        return native(path)

    monkeypatch.setattr(os, "scandir", scandir)
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    value = {"checkout_root": str(fleet.checkout), "published_by": socket.gethostname(),
             "max_age_s": 3600}[predicate]
    response = session.call("pb_actions", {predicate: value, "states": ["ready"]})
    assert response["actions"] is not None, response["unavailable"]
    assert [row["action_key"] for row in response["actions"]] == [fx.READY_KEY]


def test_full_key_reads_each_requested_slot_once(tmp_path, monkeypatch):
    fleet = fx.build(tmp_path)
    native = pool.record_store.RecordStore.inspection_slot
    visited = []

    def read(store, state, key):
        visited.append((state, key))
        return native(store, state, key)

    monkeypatch.setattr(pool.record_store.RecordStore, "inspection_slot", read)
    response = pbmcp._scan_actions(fleet.queue_root, states=["ready", "done"],
                                   keys=[fx.DONE_KEY], limit=50)
    assert [row["action_key"] for row in response["rows"]] == [fx.DONE_KEY]
    assert visited == [("ready", fx.DONE_KEY), ("done", fx.DONE_KEY)]


def test_default_status_stays_small_with_large_admission_samples(tmp_path):
    fleet = fx.build(tmp_path)
    fleet.queue.announce(host="fixture-box", tags=["x86"], has_gpu=False,
                         capacity={"cpu": 8, "mem_gb": 16})
    sample = fleet.queue_root / pool.RESERVATIONS / "fixture-box" / "adaptive" / "cpu-sample.json"
    sample.parent.mkdir(parents=True, exist_ok=True)
    large_sample = {"sampled_unix": time.time(), "control_threads": [
        {"pid": pid, "starttime": pid * 100, "cpu_seconds": 23.5,
         "command": "python worker " + "x" * 128} for pid in range(2048)]}
    sample.write_text(json.dumps(large_sample))
    claimed = fleet.record("claimed", fx.CLAIMED_KEY)
    claimed["gpu_admission"] = {"reason": "admitted", "fresh": True, "sample": large_sample}
    fleet.queue.records.item_path("claimed", fx.CLAIMED_KEY).write_text(json.dumps(claimed))
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    compact = session.call("pb_status")
    detail = session.call("pb_status", {"detail": True})
    assert compact["complete"] and detail["complete"]
    assert len(json.dumps(compact).encode()) < 50_000
    assert len(json.dumps(detail).encode()) > 500_000
    assert {job["action_key"] for job in compact["jobs"]} == {
        fx.READY_KEY, fx.CLAIMED_KEY}
    small = next(job for job in compact["jobs"] if job["action_key"] == fx.CLAIMED_KEY)
    full = next(job for job in detail["jobs"] if job["action_key"] == fx.CLAIMED_KEY)
    assert small["gpu_admission"] == {"reason": "admitted", "fresh": True}
    assert full["gpu_admission"]["sample"] == large_sample
    assert compact["unavailable"] == detail["unavailable"] == []
