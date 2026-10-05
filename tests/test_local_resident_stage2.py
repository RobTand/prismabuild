import json
from pathlib import Path

import pytest

from prismabuild import pool, resident_sets
from test_local_resident_mover import world
from test_resident_sets_records import publish


def finished_attempt(tmp_path):
    from admitted_queue_fixture import AdmittedQueueFixture
    queue = AdmittedQueueFixture(pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 1, "mem_gb": 2},
                                 default_demand={"cpu": 1, "mem_gb": 1})
    queue.ensure_layout()
    key = "d" * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / "cas"), checkout_root=str(tmp_path),
                  worker_script="/worker.py", max_attempts=1)
    assert queue.claim() is not None
    terminal = json.loads(queue.finish(key, status="executed").read_text())
    return queue, terminal


def test_phase2_local_serving_validates_in_attempt_outcomes(tmp_path):
    queue, terminal = finished_attempt(tmp_path)
    outcome_path = queue.root / terminal["attempt_history"][0]["outcome"]
    row = json.loads(outcome_path.read_text())
    row["served_from"] = "local"
    outcome_path.chmod(0o644)
    outcome_path.write_text(json.dumps(row))
    outcome_path.chmod(0o444)
    reread = queue.attempt_outcomes(terminal)
    assert reread[0]["served_from"] == "local"


def test_lease_pass_skips_absent_sets_with_no_tree_and_no_tokens(tmp_path, monkeypatch):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    writes = []
    original = store.write_copy

    def counted(set_id, host, body):
        writes.append(body["state"])
        return original(set_id, host, body)

    monkeypatch.setattr(store, "write_copy", counted)
    local_resident.lease_pass(store, "test-host", spec, now=201)
    assert writes == [], "an absent set with no tree and no tokens must not be rewritten every cycle"


def test_one_failing_set_does_not_block_the_others(tmp_path):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    store.release(record["set_id"], by="test")
    other, other_record = publish(tmp_path, lease={"until": 150, "hard_max": 200})
    # Force the second set's copy into a state evict cannot judge: corrupt it.
    local_resident.copy(store, other_record["set_id"], "test-host", spec, now=120)
    store.release(other_record["set_id"], by="test")
    store.copy_path(other_record["set_id"], "test-host").write_text("broken")
    results = local_resident.lease_pass(store, "test-host", spec, now=201)
    by_set = {row.get("set_id"): row for row in results if isinstance(row, dict)}
    good = by_set[record["set_id"]]
    assert good["state"] == "absent", "the healthy expired set must still be evicted"
    bad = by_set[other_record["set_id"]]
    assert bad.get("error"), "the failing set must report its failure, not raise"


def test_missing_docker_is_a_clear_refusal_not_a_traceback(tmp_path, monkeypatch):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    source = tmp_path / "manual"
    source.mkdir()
    (source / "weights").write_bytes(b"weights")
    record_body = json.loads(store.set_path(record["set_id"]).read_text())
    record_body["canonical_root"] = "/mnt/shared/somewhere"
    # Re-publish is immutable; instead point the check through the real path.
    import subprocess as _subprocess

    def no_docker(*args, **kwargs):
        raise FileNotFoundError("docker")

    monkeypatch.setattr(_subprocess, "run", no_docker)
    with pytest.raises(ValueError, match="docker was not found"):
        local_resident.require_adoption_unmounted(
            {"canonical_root": "/mnt/shared/somewhere"}, source)


def test_renew_refuses_a_hard_max_beyond_the_policy_ceiling(tmp_path):
    store, record = publish(tmp_path)
    with pytest.raises(ValueError, match="ceiling"):
        store.renew(record["set_id"], {"until": 150, "hard_max": 150 + 40 * 86400}, by="test", now=100)
    store.renew(record["set_id"], {"until": 150, "hard_max": 100 + 14 * 86400}, by="test", now=100)
    assert store.status(record["set_id"])["lease_log"][-1]["event"] == "renewed"
