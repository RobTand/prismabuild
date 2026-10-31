"""Indexed discovery must not turn unreadable canonical records into absence."""

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbmcp
from prismabuild import publication_index
from prismabuild.record_store import RecordStore


@pytest.fixture
def reconciled_queue(tmp_path, monkeypatch):
    monkeypatch.setattr(publication_index, "_clock_fence", lambda: 10**30)
    store = RecordStore(tmp_path / "queue")
    key = "a" * 64
    record = {"action_key": key, "published_unix": 100.0,
              "checkout_root": "/fixture"}
    path = store.item_path("ready", key)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(record))
    assert store.index_publication(record)
    with store.begin_publication_reconciliation() as run:
        for _ in range(100):
            progress = run.step(max_entries=2)
            if progress["finished"]:
                break
        assert progress["complete"], progress
    session = pbmcp.Session(queue_root=store.root, cas_root=tmp_path / "cas",
                            repo_link=Path(__file__).resolve().parents[1])
    return store, path, session


@pytest.mark.parametrize("corruption", ['{"broken":', '[]', ''])
def test_in_place_corruption_makes_indexed_population_unavailable(reconciled_queue,
                                                                 corruption):
    store, path, session = reconciled_queue
    query = {"checkout_root": "/fixture", "states": ["ready"]}
    before = session.call("pb_actions", query)
    assert before["complete"] and len(before["actions"]) == 1
    inode = path.stat().st_ino
    stamps = publication_index._namespace_stamps(store.root)
    path.write_text(corruption)
    assert path.stat().st_ino == inode
    assert publication_index._namespace_stamps(store.root) == stamps
    capture = store.publication_candidates(facets={"checkout_root": "/fixture"})
    assert capture.complete and len(capture.candidates) == 1

    response = session.call("pb_actions", query)
    assert response["actions"] == []
    assert not response["complete"] and response["truncated"]
    assert not response["discovery"]["complete"]
    assert response["discovery"]["canonical_reads"] == 1
    assert "canonical-record-unreadable" in response["discovery"]["reasons"]
    assert response["unavailable"]


def test_missing_requested_slot_remains_complete(reconciled_queue):
    _, _, session = reconciled_queue
    response = session.call("pb_actions", {"checkout_root": "/fixture",
                                           "states": ["done"]})
    assert response["actions"] == []
    assert response["complete"] and not response["truncated"]
    assert response["discovery"]["canonical_reads"] == 1
    assert response["unavailable"] == []


@pytest.mark.parametrize("corruption", ["duplicate-field", "nonfinite", "oversized", "symlink"])
def test_indexed_canonical_reader_is_bounded_and_strict(reconciled_queue, tmp_path, corruption):
    from prismabuild import record_store
    store, path, session = reconciled_queue
    raw = path.read_bytes()
    if corruption == "duplicate-field":
        raw = raw.replace(b'"checkout_root": "/fixture"',
                          b'"checkout_root": "/other", "checkout_root": "/fixture"')
    elif corruption == "nonfinite":
        raw = raw[:-1] + b', "untrusted": NaN}'
    elif corruption == "oversized":
        raw = raw[:-1] + b' ' * (8 * 1024 * 1024) + b'}'
    if corruption == "symlink":
        target = tmp_path / "outside.json"
        target.write_bytes(raw)
        path.unlink()
        path.symlink_to(target)
    else:
        path.write_bytes(raw)
    result = store.inspection_slot_result("ready", "a" * 64)
    assert isinstance(result, record_store.Unknown)
    assert result.reason == "record-unreadable" and result.path == path
    response = session.call("pb_actions", {"checkout_root": "/fixture", "states": ["ready"]})
    assert response["actions"] == [] and not response["complete"]
    assert "canonical-record-unreadable" in response["discovery"]["reasons"]
