import json
from contextlib import contextmanager
from pathlib import Path
import signal

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
    return queue, json.loads(queue.finish(key, status="executed").read_text())


def test_phase2_local_serving_validates_in_attempt_outcomes(tmp_path):
    queue, terminal = finished_attempt(tmp_path)
    outcome_path = queue.root / terminal["attempt_history"][0]["outcome"]
    row = json.loads(outcome_path.read_text())
    row["served_from"] = "local"
    outcome_path.chmod(0o644)
    outcome_path.write_text(json.dumps(row))
    outcome_path.chmod(0o444)
    assert queue.attempt_outcomes(terminal)[0]["served_from"] == "local"


def test_lease_pass_skips_absent_sets_with_no_tree_and_no_tokens(tmp_path, monkeypatch):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    pool.PoolQueue(store.queue_root).tier_ledger("local:test-host").release(record["set_id"])
    writes = []
    original = store.write_copy

    def counted(set_id, host, body):
        writes.append(body["state"])
        return original(set_id, host, body)

    monkeypatch.setattr(store, "write_copy", counted)
    local_resident.lease_pass(store, "test-host", spec, now=201)
    local_resident.lease_pass(store, "test-host", spec, now=202)
    assert writes == [], "an absent set with no tree and no tokens must not be rewritten every cycle"


def _second_set(tmp_path, store):
    import hashlib
    from prismabuild import core as pb_core
    other_root = tmp_path / "canonical2"
    other_root.mkdir()
    (other_root / "weights").write_bytes(b"weights")
    manifest = {"schema": pb_core.DATA_MANIFEST_SCHEMA_V1, "produced_by": {}, "annotations": {},
                "mount_prefix": str(other_root),
                "entries": [{"path": str(other_root / "weights"), "offset": 0, "bytes": 7,
                             "sha256": hashlib.sha256(b"weights").hexdigest()}],
                "entry_count": 1, "total_bytes": 7}
    pool.PoolQueue(store.queue_root).mint_tier_capacity("local:test-host", {"local_gib": 2})
    return store.publish(manifest=manifest, canonical_root=str(other_root), hosts=["test-host"],
                         lease={"until": 150, "hard_max": 200}, created_by="test", now=100)


@pytest.mark.parametrize("fault", ["corrupt_record", "delete_error"])
def test_one_failing_set_does_not_block_the_others(tmp_path, monkeypatch, fault):
    from prismabuild import local_resident, local_tier
    import local_tier_loop
    store, good, spec = world(tmp_path)
    local_resident.copy(store, good["set_id"], "test-host", spec, now=120)
    bad = _second_set(tmp_path, store)
    local_resident.copy(store, bad["set_id"], "test-host", spec, now=120)
    store.release(good["set_id"], by="test")
    store.release(bad["set_id"], by="test")
    saved_bad = store.read_copy(bad["set_id"], "test-host")
    original_delete = local_resident.shutil.rmtree
    if fault == "corrupt_record":
        store.copy_path(bad["set_id"], "test-host").write_text("broken")
    else:
        # An interrupted evicting set is ordered BEFORE the healthy set.
        store.write_copy(bad["set_id"], "test-host", {**saved_bad, "state": "evicting"})

        def fail_bad_delete(path, *args, **kwargs):
            if Path(path).name == bad["set_id"] + ".evicting":
                raise OSError("simulated delete failure")
            return original_delete(path, *args, **kwargs)

        monkeypatch.setattr(local_resident.shutil, "rmtree", fail_bad_delete)
    minted = []
    original_mint = local_tier.mint

    def observe_mint(*args, **kwargs):
        minted.append(True)
        return original_mint(*args, **kwargs)

    monkeypatch.setattr(local_tier, "mint", observe_mint)
    queue = pool.PoolQueue(store.queue_root)
    result = local_tier_loop.cycle(queue, "test-host", {"hosts": {"test-host": spec}})
    rows = {row["set_id"]: row for row in result["lease_pass"]}
    assert rows[good["set_id"]]["state"] == "absent"
    assert rows[bad["set_id"]]["error"]
    assert store.read_copy(good["set_id"], "test-host")["state"] == "absent"
    ledger = queue.tier_ledger("local:test-host")
    assert ledger.holder_tokens(good["set_id"]) == {}
    assert ledger.holder_tokens(bad["set_id"])["local_gib"] == 1
    assert len(minted) == 1, "one failing set must not block capacity minting"
    assert result["capacity"] == ledger.capacity()
    error_path = store.set_path(bad["set_id"]).parent / "lease-errors" / "test-host.json"
    error = json.loads(error_path.read_text())
    assert error["set_id"] == bad["set_id"] and error["error"]
    # A subsequent successful pass resumes cleanup and clears the latest error.
    if fault == "corrupt_record":
        store.write_copy(bad["set_id"], "test-host", saved_bad)
    else:
        monkeypatch.setattr(local_resident.shutil, "rmtree", original_delete)
    local_tier_loop.cycle(queue, "test-host", {"hosts": {"test-host": spec}})
    assert store.read_copy(bad["set_id"], "test-host")["state"] == "absent"
    assert ledger.holder_tokens(bad["set_id"]) == {}
    assert not error_path.exists()


@pytest.mark.parametrize("once", [False, True])
def test_role_reports_a_cycle_error_instead_of_crashing(tmp_path, monkeypatch, capsys, once):
    import local_tier_loop
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"schema": resident_sets.POLICY_SCHEMA, "hosts": {}}))

    @contextmanager
    def singleton(_script):
        yield

    monkeypatch.setattr(local_tier_loop.runtime_gate, "role_singleton", singleton)
    monkeypatch.setattr(local_tier_loop.runtime_gate, "loaded_runtime_commit", lambda: "")
    monkeypatch.setattr(local_tier_loop.runtime_gate, "published_commit", lambda: "")
    monkeypatch.setattr(local_tier_loop.runtime_gate, "read_maintenance_gate", lambda: None)
    calls = []

    def cycle(*args):
        calls.append(True)
        if len(calls) == 1:
            raise OSError("temporary cycle failure")
        signal.raise_signal(signal.SIGTERM)  # Main's handler requests its own cooperative stop.
        return {"capacity": {}, "lease_pass": []}

    monkeypatch.setattr(local_tier_loop, "cycle", cycle)
    options = ["--pool-root", str(tmp_path / "queue"), "--policy", str(policy), "--interval-s", "0.001"]
    if once:
        options += ["--once"]
    assert local_tier_loop.main(options) == (1 if once else 0)
    assert len(calls) == (1 if once else 2)
    output = capsys.readouterr().out
    assert "localtier-cycle-error" in output and "temporary cycle failure" in output


@pytest.mark.parametrize("missing_at", ["ps", "inspect"])
def test_missing_docker_is_a_clear_refusal_not_a_traceback(tmp_path, monkeypatch, missing_at):
    from prismabuild import local_resident
    source = tmp_path / "manual"
    source.mkdir()
    (source / "weights").write_bytes(b"weights")
    import subprocess as _subprocess

    def no_docker(command, *args, **kwargs):
        if missing_at == "inspect" and command[1] == "ps":
            from types import SimpleNamespace
            return SimpleNamespace(returncode=0, stdout="running-container")
        raise FileNotFoundError("docker")

    monkeypatch.setattr(_subprocess, "run", no_docker)
    with pytest.raises(ValueError, match="docker was not found"):
        local_resident.require_adoption_unmounted({"canonical_root": "/mnt/shared/somewhere"}, source)


def test_renew_refuses_a_hard_max_beyond_the_policy_ceiling(tmp_path):
    store, record = publish(tmp_path)
    with pytest.raises(ValueError, match="ceiling"):
        store.renew(record["set_id"], {"until": 150, "hard_max": 150 + 40 * 86400}, by="test", now=100)
    store.renew(record["set_id"], {"until": 150, "hard_max": 100 + 14 * 86400}, by="test", now=100)
    assert store.status(record["set_id"])["lease_log"][-1]["event"] == "renewed"


def test_renewal_uses_the_configured_policy_ceiling(tmp_path):
    store, record = publish(tmp_path)
    policy_path = tmp_path / "local-policy.json"
    policy_path.write_text(json.dumps({"schema": resident_sets.POLICY_SCHEMA,
        "hosts": {}, "renewal_ceiling_s": 3 * 86400}))
    policy = resident_sets.read_policy(policy_path)
    assert policy["renewal_ceiling_s"] == 3 * 86400
    with pytest.raises(ValueError, match="ceiling"):
        store.renew(record["set_id"], {"until": 150, "hard_max": 100 + 4 * 86400},
                    by="test", now=100, renewal_ceiling_s=policy["renewal_ceiling_s"])
    store.renew(record["set_id"], {"until": 150, "hard_max": 100 + 3 * 86400},
                by="test", now=100, renewal_ceiling_s=policy["renewal_ceiling_s"])
    assert store.status(record["set_id"])["lease_log"][-1]["lease"]["hard_max"] == 100 + 3 * 86400
