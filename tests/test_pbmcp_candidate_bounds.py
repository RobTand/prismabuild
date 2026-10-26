"""Publication filters address candidates before canonical namespace reads."""

import json
import os
from pathlib import Path
import socket
import shutil
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


@pytest.mark.parametrize("field", ["checkout_root", "snapshot_parent", "snapshot_commit",
                                  "published_by"])
def test_sparse_facet_ignores_unrelated_history_before_record_opens(tmp_path, monkeypatch, field):
    fleet = fx.build(tmp_path)
    store = fleet.queue.records
    wanted = fleet.record("ready", fx.READY_KEY)
    wanted["checkout_snapshot"] = {"parent": "1" * 40, "commit": "2" * 40}
    store.item_path("ready", fx.READY_KEY).write_text(json.dumps(wanted))
    assert store.index_publication(wanted)
    for number in range(32):
        other = {**wanted, "action_key": f"{number:064x}", "checkout_root": "/unrelated",
                 "checkout_snapshot": {"parent": "3" * 40, "commit": "4" * 40},
                 "published_by": "other", "published_unix": wanted["published_unix"] + number}
        store.item_path("done", other["action_key"]).write_text(json.dumps(other))
        assert store.index_publication(other)
    native_scan = os.scandir
    native_read = pool.record_store.RecordStore.inspection_slot_result
    visits = []
    reads = []

    def scan(path):
        if not isinstance(path, int):
            assert Path(path) not in {store.dir(state) for state in pbmcp.STATES}
            visits.append(Path(path))
        return native_scan(path)

    def read(records, state, key):
        reads.append((state, key))
        return native_read(records, state, key)

    monkeypatch.setattr(os, "scandir", scan)
    monkeypatch.setattr(pool.record_store.RecordStore, "inspection_slot_result", read)
    value = {"checkout_root": str(fleet.checkout), "snapshot_parent": "1" * 40,
             "snapshot_commit": "2" * 40, "published_by": wanted["published_by"]}[field]
    result = pbmcp._scan_actions(fleet.queue_root, states=["ready"], keys=[], limit=1,
                                 facets={field: value})
    assert [row["action_key"] for row in result["rows"]] == [fx.READY_KEY]
    assert {key for _, key in reads} <= {fx.READY_KEY, fx.DONE_KEY, fx.CLAIMED_KEY}
    assert all(state == "ready" for state, _ in reads)
    assert result["discovery"]["entries_consumed"] <= 4
    assert visits


def test_empty_exhausted_discovery_is_incomplete(tmp_path, monkeypatch):
    fleet = fx.build(tmp_path)
    monkeypatch.setattr(pbmcp, "ACTION_CANDIDATE_ENTRIES", 1)
    body = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link).call(
        "pb_actions", {"checkout_root": str(fleet.checkout), "limit": 0,
                       "priority_min": 100, "states": ["ready"]})
    assert body["actions"] == [] and not body["complete"]
    assert body["truncated"] and body["discovery"]["canonical_reads"] == 1
    assert "entry-budget-exhausted" in body["discovery"]["reasons"]


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


def test_missing_index_reports_incomplete_without_mutating_queue(tmp_path):
    fleet = fx.build(tmp_path)
    shutil.rmtree(fleet.queue_root / "publication-index")
    before = {str(path.relative_to(fleet.queue_root))
              for path in fleet.queue_root.rglob("*")}
    response = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                             repo_link=fleet.repo_link).call(
        "pb_actions", {"checkout_root": str(fleet.checkout)})
    assert response["actions"] == []
    assert not response["complete"] and response["truncated"]
    assert "coverage-missing" in response["discovery"]["reasons"]
    assert before == {str(path.relative_to(fleet.queue_root))
                      for path in fleet.queue_root.rglob("*")}


def test_stale_candidate_cannot_adopt_another_publication(tmp_path):
    fleet = fx.build(tmp_path)
    item = fleet.record("ready", fx.READY_KEY)
    item["published_unix"] += 10
    fleet.queue.records.item_path("ready", fx.READY_KEY).write_text(json.dumps(item))
    body = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link).call(
        "pb_actions", {"checkout_root": str(fleet.checkout), "states": ["ready"]})
    assert body["actions"] == []


def test_canonical_predicates_reject_stale_index_metadata(tmp_path):
    fleet = fx.build(tmp_path)
    item = fleet.record("ready", fx.READY_KEY)
    item["checkout_root"] = "/replacement"
    fleet.queue.records.item_path("ready", fx.READY_KEY).write_text(json.dumps(item))
    body = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link).call(
        "pb_actions", {"checkout_root": str(fleet.checkout), "states": ["ready"]})
    assert body["actions"] == []


def test_canonical_read_budget_counts_missing_slots(tmp_path, monkeypatch):
    fleet = fx.build(tmp_path)
    monkeypatch.setattr(pbmcp, "ACTION_CANONICAL_READS", 2)
    native = pool.record_store.RecordStore.inspection_slot_result
    reads = []

    def read(store, state, key):
        reads.append((state, key))
        return native(store, state, key)

    monkeypatch.setattr(pool.record_store.RecordStore, "inspection_slot_result", read)
    result = pbmcp._scan_actions(
        fleet.queue_root, states=["ready", "done"], keys=[], limit=0,
        facets={"checkout_root": str(fleet.checkout)})
    assert len(reads) == result["discovery"]["canonical_reads"] == 2
    assert "canonical-read-budget-exhausted" in result["discovery"]["reasons"]
    assert result["truncated"]


def test_facet_and_age_keep_future_publications(tmp_path):
    fleet = fx.build(tmp_path)
    item = fleet.record("ready", fx.READY_KEY)
    item["published_unix"] = time.time() + 86400
    fleet.queue.records.item_path("ready", fx.READY_KEY).write_text(json.dumps(item))
    assert fleet.queue.records.index_publication(item)
    body = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link).call(
        "pb_actions", {"checkout_root": str(fleet.checkout), "max_age_s": 3600,
                       "states": ["ready"], "limit": 1})
    assert [row["action_key"] for row in body["actions"]] == [fx.READY_KEY]


def test_publication_time_filter_ignores_terminal_mtime(tmp_path):
    fleet = fx.build(tmp_path)
    os.utime(fleet.queue.records.item_path("done", fx.DONE_KEY), (0, 0))
    body = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link).call(
        "pb_actions", {"max_age_s": 3600, "states": ["done"]})
    assert [row["action_key"] for row in body["actions"]] == [fx.DONE_KEY]


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
