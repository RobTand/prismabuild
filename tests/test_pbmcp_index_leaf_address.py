"""Detect index address corruption even when directory coverage stays valid."""

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbmcp
from prismabuild import publication_index
from prismabuild.record_store import RecordStore


@pytest.fixture
def indexed_queue(tmp_path, monkeypatch):
    monkeypatch.setattr(publication_index, "_clock_fence", lambda: 10**30)
    store = RecordStore(tmp_path / "queue")
    records = []
    for digit in ("a", "b"):
        record = {"action_key": digit * 64, "published_unix": 100.0,
                  "checkout_root": "/fixture", "published_by": "publisher",
                  "checkout_snapshot": {"parent": "parent", "commit": "commit"}}
        path = store.item_path("ready", record["action_key"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record))
        assert store.index_publication(record)
        records.append(record)
    with store.begin_publication_reconciliation() as run:
        for _ in range(100):
            progress = run.step(max_entries=2)
            if progress["finished"]:
                break
        assert progress["complete"], progress
    session = pbmcp.Session(queue_root=store.root, cas_root=tmp_path / "cas",
                            repo_link=Path(__file__).resolve().parents[1])
    return store, records, session


@pytest.mark.parametrize(("field", "value", "replacement"), [
    ("checkout_root", "/fixture", "/other"),
    ("snapshot_parent", "parent", "other"),
    ("snapshot_commit", "commit", "other"),
    ("published_by", "publisher", "other"),
    ("checkout_root", "/fixture", None),
])
def test_in_place_leaf_address_mismatch_retains_partial_rows(indexed_queue,
                                                            field, value, replacement):
    store, records, session = indexed_queue
    query = {field: value, "states": ["ready"]}
    before = session.call("pb_actions", query)
    assert before["complete"] and len(before["actions"]) == 2
    leaf = next((store.root / "publication-index" / "v1" / "facets" / field).glob(
        "*/entries/" + records[0]["action_key"] + ".*.json"))
    inode = leaf.stat().st_ino
    stamps = publication_index._namespace_stamps(store.root)
    bucket_stamp = publication_index._publication_stamp(leaf.parent.parent)
    entries_stamp = publication_index._publication_stamp(leaf.parent)
    canonical = [store.item_path("ready", r["action_key"]).read_bytes() for r in records]
    body = json.loads(leaf.read_bytes())
    if replacement is None:
        del body["facets"][field]
    else:
        body["facets"][field] = replacement
    leaf.write_text(json.dumps(body))
    assert leaf.stat().st_ino == inode
    assert publication_index._namespace_stamps(store.root) == stamps
    assert publication_index._publication_stamp(leaf.parent.parent) == bucket_stamp
    assert publication_index._publication_stamp(leaf.parent) == entries_stamp
    assert [store.item_path("ready", r["action_key"]).read_bytes() for r in records] == canonical

    response = session.call("pb_actions", query)
    assert [row["action_key"] for row in response["actions"]] == [records[1]["action_key"]]
    assert not response["complete"] and response["truncated"]
    assert not response["discovery"]["complete"] and response["unavailable"]
    assert response["discovery"]["canonical_reads"] == 1
    assert response["discovery"]["entries_consumed"] == 2
    assert "index-leaf-unreadable" in response["discovery"]["reasons"]
    direct = session.call("pb_actions", {**query, "keys": [records[0]["action_key"]]})
    assert direct["complete"] and not direct["truncated"]
    assert [row["action_key"] for row in direct["actions"]] == [records[0]["action_key"]]


@pytest.mark.parametrize(("extra", "reads"), [({"published_by": "other"}, 2), ({"max_age_s": 1}, 0)])
def test_consistent_leaf_can_be_a_genuine_nonmatch(indexed_queue, extra, reads):
    _, _, session = indexed_queue
    response = session.call("pb_actions", {"checkout_root": "/fixture",
                                           "states": ["ready"], **extra})
    assert response["complete"] and not response["truncated"]
    assert response["actions"] == [] and response["unavailable"] == []
    assert response["discovery"]["canonical_reads"] == reads


def test_valid_leaf_in_wrong_day_bucket_is_reported(indexed_queue):
    store, records, _ = indexed_queue
    day = store.root / "publication-index" / "v1" / "days"
    leaf = next((day / "0" / "entries").glob(records[0]["action_key"] + ".*.json"))
    wrong = day / "1" / "entries" / leaf.name
    wrong.parent.mkdir(parents=True)
    leaf.rename(wrong)
    result = store.publication_candidates(facets={}, published_after=86400,
                                          published_before=86401)
    assert not result.complete and result.candidates == ()
    assert "index-leaf-unreadable" in result.reasons
    assert result.entries_consumed == 1 and result.pages_consumed == 1


@pytest.mark.parametrize(("field", "value"), [
    ("published_by", "publisher"), ("snapshot_parent", "parent"),
    ("snapshot_commit", "commit"),
])
def test_secondary_leaf_facets_cannot_hide_canonical_matches(indexed_queue, field, value):
    store, records, session = indexed_queue
    leaf = next((store.root / "publication-index/v1/facets/checkout_root").glob(
        "*/entries/" + records[0]["action_key"] + ".*.json"))
    stamps = publication_index._namespace_stamps(store.root)
    body = json.loads(leaf.read_bytes())
    body["facets"][field] = "corrupted"
    leaf.write_text(json.dumps(body))
    assert publication_index._namespace_stamps(store.root) == stamps

    response = session.call("pb_actions", {"checkout_root": "/fixture", field: value,
                                           "states": ["ready"]})
    assert {row["action_key"] for row in response["actions"]} == {
        record["action_key"] for record in records}
    assert response["discovery"]["canonical_reads"] == 2
    assert response["complete"] and response["unavailable"] == []


def test_secondary_predicates_stay_within_canonical_budget(indexed_queue, monkeypatch):
    _, _, session = indexed_queue
    monkeypatch.setattr(pbmcp, "ACTION_CANONICAL_READS", 1)
    response = session.call("pb_actions", {"checkout_root": "/fixture",
                                           "published_by": "other", "states": ["ready"]})
    assert response["actions"] == []
    assert response["discovery"]["canonical_reads"] == 1
    assert not response["complete"] and response["unavailable"]
    assert "canonical-read-budget-exhausted" in response["discovery"]["reasons"]
