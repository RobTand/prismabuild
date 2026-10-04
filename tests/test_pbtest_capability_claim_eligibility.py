"""Claim-time eligibility for digest-pinned dependencies (#1495).

The row that pins bytes must be unclaimable by a worker that cannot read the
requirement: the capability tag rides the requirement, the matcher counts only
offers that positively answer for the paths, the claim hashes the actual bytes
before spending an attempt, and the submit-side verdict refuses a fleet that
offers no capability at all.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import dependency_digest, pool  # noqa: E402
from prismabuild import core as pb  # noqa: E402

TAG = dependency_digest.DEPENDENCY_DIGEST_TAG
CAPACITY = {"cpu": 4, "mem_gb": 16}


def _queue_at(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    return queue


def _dependency(tmp_path: Path, payload: bytes = b"required bytes") -> dict:
    path = tmp_path / "deps" / "secondary"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {"path": str(path),
            "sha256": hashlib.sha256(payload).hexdigest()}


def _publish(queue, key, **kwargs):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 1, "mem_gb": 1},
                  **kwargs)


def _item_of(queue, key):
    return json.loads(queue.item_path(pool.READY, key).read_text())


def _denials(queue):
    from prismabuild import adaptive_cpu
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    records = adaptive_cpu.read_json(base / pool.CLAIM_DENIALS).get(
        "records", {})
    return list(records.values())


def _intent(path: str) -> dict:
    return {"tags": [TAG], "resources": {"cpu": 1, "mem_gb": 1},
            "requires_files": [{"path": path}]}


# --------------------------------------------------------------------------
# The declaration
# --------------------------------------------------------------------------

def test_publish_seals_the_requirements_and_adds_the_capability_tag(
        tmp_path):
    queue = _queue_at(tmp_path)
    entry = _dependency(tmp_path)
    _publish(queue, "a" * 64, requires_files=[entry])

    record = _item_of(queue, "a" * 64)
    assert record["requires_files"] == [entry]
    assert TAG in record["tags"]


def test_publish_refuses_malformed_entries_and_a_bare_capability(tmp_path):
    queue = _queue_at(tmp_path)
    with pytest.raises(pool.PoolContractError, match="requires_files"):
        _publish(queue, "b" * 64,
                 requires_files=[{"path": "relative/x", "sha256": "a" * 64}])
    with pytest.raises(pool.PoolContractError, match="requires_files"):
        _publish(queue, "c" * 64,
                 requires_files=[{"path": "/x", "sha256": "nope"}])
    with pytest.raises(pool.PoolContractError, match=TAG):
        _publish(queue, "d" * 64, tags=[TAG])
    assert not queue.item_path(pool.READY, "b" * 64).exists()
    assert not queue.item_path(pool.READY, "c" * 64).exists()
    assert not queue.item_path(pool.READY, "d" * 64).exists()


def test_an_item_without_requirements_is_byte_identical_to_before(tmp_path):
    queue = _queue_at(tmp_path)
    _publish(queue, "e" * 64)
    record = _item_of(queue, "e" * 64)
    assert "requires_files" not in record
    assert TAG not in record["tags"]


# --------------------------------------------------------------------------
# The matcher: unknown is not capable
# --------------------------------------------------------------------------

def test_only_a_tag_carrying_offer_that_answers_for_the_path_matches(tmp_path):
    queue = _queue_at(tmp_path)
    entry = _dependency(tmp_path)
    _publish(queue, "f" * 64, requires_files=[entry])

    probe = _intent(entry["path"])
    # An old generation: no tag, no answers -> never capable.
    queue.announce(host="old", tags=[], has_gpu=False, capacity=dict(CAPACITY))
    assert queue.placeable(probe) is False
    # New generation, silent answers: unknown is not capable (#1263's rule).
    queue.announce(host="new", tags=[TAG], has_gpu=False,
                   capacity=dict(CAPACITY), dependency_files=[])
    assert queue.placeable(probe) is False
    # The same box, having answered: capable.
    queue.announce(host="new", tags=[TAG], has_gpu=False,
                   capacity=dict(CAPACITY), dependency_files=[entry["path"]])
    assert queue.placeable(probe) is True


# --------------------------------------------------------------------------
# The claim gate: the bytes are read before the attempt is spent
# --------------------------------------------------------------------------

def test_the_claim_denies_a_dependency_this_box_does_not_have(tmp_path):
    queue = _queue_at(tmp_path)
    entry = _dependency(tmp_path)
    _publish(queue, "1" * 64, requires_files=[entry])

    os_path = Path(entry["path"])
    os_path.unlink()
    assert queue.claim(capacity=dict(CAPACITY), tags=[TAG]) is None
    denial = _denials(queue)[-1]
    assert denial["reason"] == "dependency_not_present"
    assert denial["evidence"]["paths"] == [entry["path"]]
    assert queue.item_path(pool.READY, "1" * 64).exists()


def test_the_claim_denies_drifted_bytes_with_both_digests(tmp_path):
    queue = _queue_at(tmp_path)
    entry = _dependency(tmp_path)
    _publish(queue, "2" * 64, requires_files=[entry])

    Path(entry["path"]).write_bytes(b"drifted")
    assert queue.claim(capacity=dict(CAPACITY), tags=[TAG]) is None
    denial = _denials(queue)[-1]
    assert denial["reason"] == "dependency_digest_mismatch"
    assert denial["evidence"]["expected"] == entry["sha256"]
    assert denial["evidence"]["observed"] == hashlib.sha256(
        b"drifted").hexdigest()
    assert queue.item_path(pool.READY, "2" * 64).exists()


def test_the_claim_takes_bytes_that_match_the_requirement(tmp_path):
    queue = _queue_at(tmp_path)
    entry = _dependency(tmp_path)
    _publish(queue, "3" * 64, requires_files=[entry])

    claimed = queue.claim(capacity=dict(CAPACITY), tags=[TAG])
    assert claimed is not None
    assert not queue.item_path(pool.READY, "3" * 64).exists()


def test_a_tag_without_requirements_on_a_row_fails_closed(tmp_path):
    queue = _queue_at(tmp_path)
    _publish(queue, "4" * 64)
    item = _item_of(queue, "4" * 64)
    item["tags"] = sorted([*item["tags"], TAG])
    queue.item_path(pool.READY, "4" * 64).write_text(json.dumps(item))

    assert queue.claim(capacity=dict(CAPACITY), tags=[TAG]) is None
    denial = _denials(queue)[-1]
    assert denial["reason"] == "dependency_requirement_missing"


def test_a_row_with_malformed_requirements_is_denied_not_run(tmp_path):
    queue = _queue_at(tmp_path)
    _publish(queue, "5" * 64)
    item = _item_of(queue, "5" * 64)
    item["tags"] = sorted([*item["tags"], TAG])
    item["requires_files"] = [{"path": "relative"}]
    queue.item_path(pool.READY, "5" * 64).write_text(json.dumps(item))

    assert queue.claim(capacity=dict(CAPACITY), tags=[TAG]) is None
    denial = _denials(queue)[-1]
    assert denial["reason"] == "malformed_requires_files"


# --------------------------------------------------------------------------
# The submit-side verdict
# --------------------------------------------------------------------------

def test_the_verdict_refuses_a_fleet_with_no_capability(tmp_path):
    queue = _queue_at(tmp_path)
    entry = _dependency(tmp_path)
    queue.announce(host="old", tags=[], has_gpu=False, capacity=dict(CAPACITY))

    assert queue.dependency_placement_verdict(
        _intent(entry["path"])) == "unknown_capability"


def test_the_verdict_answers_present_absent_and_unknown(tmp_path):
    queue = _queue_at(tmp_path)
    entry = _dependency(tmp_path)
    absent_entry = {"path": "/no/such/secondary", "sha256": "a" * 64}

    queue.announce(host="box", tags=[TAG], has_gpu=False,
                   capacity=dict(CAPACITY), dependency_files=[])
    intent = _intent(entry["path"])
    assert queue.dependency_placement_verdict(intent) == "unknown_paths"

    queue.announce(host="box", tags=[TAG], has_gpu=False,
                   capacity=dict(CAPACITY), dependency_files=[],
                   dependency_files_absent=[absent_entry["path"]])
    assert queue.dependency_placement_verdict(
        _intent(absent_entry["path"])) == "absent"

    queue.announce(host="box", tags=[TAG], has_gpu=False,
                   capacity=dict(CAPACITY), dependency_files=[entry["path"]],
                   dependency_files_absent=[absent_entry["path"]])
    assert queue.dependency_placement_verdict(
        intent) == "present"


# --------------------------------------------------------------------------
# The worker's lookup
# --------------------------------------------------------------------------

def test_the_lookup_answers_exactly_the_paths_items_ask_about(tmp_path):
    from worker_loop import dependency_lookup
    entry = _dependency(tmp_path)
    items = [{"requires_files": [entry]},
             {"requires_files": [{"path": "/no/such/secondary",
                                  "sha256": "a" * 64}]},
             {"interpreter": "/ignored"}]
    present, absent = dependency_lookup(items)
    assert present == [entry["path"]]
    assert absent == ["/no/such/secondary"]


# --------------------------------------------------------------------------
# The row pbrun publishes
# --------------------------------------------------------------------------

def test_publication_row_carries_the_sealed_requirements(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "pbrun_subject", REPOSITORY / "tools" / "fleet" / "pbrun.py")
    pbrun = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pbrun)

    entry = _dependency(tmp_path)
    row = pbrun.publication_row(
        {"action_key": "6" * 64,
         "params": {"demand": {}, "placement": {"required_tags": [TAG]},
                    "checkout_snapshot": {},
                    "requires_files": [entry]},
         "environment": {"variables": {pbrun.CONTAINER_OWNER_ENV:
                                      "publication-row-test"}}},
        args=type("Args", (), {"priority": 0, "max_attempts": 1,
                               "retry_safe": True, "priority_reason": None})(),
        queue=pool.PoolQueue(tmp_path / "queue"))
    assert row["requires_files"] == [entry]
    assert row["tags"] == [TAG]
