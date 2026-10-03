"""Existing pool finalization owns scratch cleanup and the capacity barrier."""
import copy
import json
from pathlib import Path

import pytest

from prismabuild import local_scratch as scratch, pool, resource_scope
from test_scratch_declaration_record import runtime, process, live_record  # noqa: F401


FIELD = "scratch_lifetime_record"
SELECTION = {"schema": "prismabuild.scratch_lifetime_selection.v1", "entries": [
    {"root_env": "TEMP_ROOT", "name": "row-temp", "lifetime": "ephemeral"},
    {"root_env": "CACHE_ROOT", "name": "compile", "lifetime": "persistent"}]}


@pytest.fixture
def lifetime(runtime, monkeypatch):
    create, calls = runtime
    claim = pool.PoolQueue.claim
    monkeypatch.setattr(pool.PoolQueue, "claim", lambda self, **kw: claim(
        self, **{ "tags": [scratch.SCRATCH_LIFETIME_TAG], **kw}))
    queue, item, variables = create(json.dumps(SELECTION))
    Path(variables["TEMP_ROOT"]).mkdir()
    cache = Path(variables["CACHE_ROOT"])
    cache.mkdir()
    (cache / "compiled").write_bytes(b"persistent")
    monkeypatch.setattr(resource_scope.ResourceScope, "export_stopped_verdict", lambda scope: {
        "ok": True, "scope_id": scope.unit, "stopped": True, "empty": True,
        "released": True, "retired": False, "settled": True,
        "tickets_pending": False, "stopped_unix": 1.0})
    return queue, item, variables, calls


def leaf(item):
    return scratch.ephemeral_scratch_path(item[FIELD]["entries"][0]["declaration"])


def launch(lifetime, monkeypatch, returncode=0):
    queue, item, variables, calls = lifetime

    def inspect(*args):
        evidence = live_record(queue, item)[FIELD]
        assert evidence["registration_complete"] is True
        assert json.loads(queue.lease_path(item["action_key"]).read_text())[FIELD] == evidence
        assert leaf(item).is_dir()
        (leaf(item) / "temp").write_bytes(b"temporary")

    process(monkeypatch, inspect, returncode=returncode)
    outcome = queue.execute(item, containment=True)
    return queue, item, variables, outcome


@pytest.mark.parametrize("returncode", [0, 1])
def test_finish_deletes_only_ephemeral_and_archives_before_capacity_release(
        lifetime, monkeypatch, returncode):
    queue, item, variables, outcome = launch(lifetime, monkeypatch, returncode)
    old_leaf = leaf(item)
    result = queue.finish(item["action_key"], status=outcome["status"], detail=outcome, claim_snapshot=item)
    assert result != queue.item_path(pool.CLAIMED, item["action_key"]), live_record(queue, item)
    assert not old_leaf.exists()
    assert (Path(variables["CACHE_ROOT"]) / "compiled").read_bytes() == b"persistent"
    assert queue.ledger().held() == {}
    state = pool.DONE if returncode == 0 else pool.READY
    ending = pool._read_json(queue.item_path(state, item["action_key"]))
    archived = pool._read_json(queue.root / ending["attempt_history"][-1]["outcome"])
    assert all(entry["cleaned"] for entry in archived["detail"][FIELD]["entries"])
    if returncode:
        assert FIELD not in ending


@pytest.mark.parametrize("fault", ["partial", "identity", "unproven", "commit"])
def test_failed_cleanup_retains_charge_and_recovers_without_reexecuting(
        lifetime, monkeypatch, fault):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    original = scratch._clean_scratch_directory
    write = pool._write_json_atomic
    if fault == "partial":
        def partial(declaration, identity):
            (leaf(item) / "temp").unlink()
            raise OSError("fixture interrupted deletion")
        monkeypatch.setattr(scratch, "_clean_scratch_directory", partial)
    elif fault == "identity":
        old = leaf(item)
        old.rename(old.with_name("retained"))
        old.mkdir()
        (old / "foreign").write_bytes(b"replacement")
    elif fault == "unproven":
        monkeypatch.setattr(resource_scope.ResourceScope, "export_stopped_verdict", lambda s: {})
    else:
        def fail_commit(path, value):
            record = value.get(FIELD)
            if record and record["entries"][0]["cleaned"]:
                raise OSError("fixture cleanup commit failed")
            return write(path, value)
        monkeypatch.setattr(pool, "_write_json_atomic", fail_commit)
    held = queue.ledger().held()
    path = queue.finish(item["action_key"], status=outcome["status"], detail=outcome,
                        claim_snapshot=item)
    assert path == queue.item_path(pool.CLAIMED, item["action_key"])
    assert queue.ledger().held() == held
    assert live_record(queue, item)["finish_pending"]["status"] == "executed"
    if fault == "identity":
        assert (leaf(item) / "foreign").read_bytes() == b"replacement"
        return  # Explicitly ambiguous ownership needs repair, never adoption.
    monkeypatch.setattr(scratch, "_clean_scratch_directory", original)
    monkeypatch.setattr(pool, "_write_json_atomic", write)
    monkeypatch.setattr(resource_scope.ResourceScope, "export_stopped_verdict", lambda s: {
        "ok": True, "scope_id": s.unit, "stopped": True, "empty": True,
        "released": True, "retired": False, "settled": True,
        "tickets_pending": False, "stopped_unix": 1.0})
    restarted = pool.PoolQueue(queue.root)
    restarted.finish(item["action_key"], status=outcome["status"], detail=outcome, claim_snapshot=item)
    assert restarted.ledger().held() == {} and not leaf(item).exists()
    assert (Path(variables["CACHE_ROOT"]) / "compiled").read_bytes() == b"persistent"


def test_crash_after_cleanup_before_release_replays_durable_owner(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    release = queue._release_reservation
    monkeypatch.setattr(queue, "_release_reservation", lambda *a, **k:
                        (_ for _ in ()).throw(OSError("fixture before capacity release")))
    with pytest.raises(OSError, match="fixture before capacity release"):
        queue.finish(item["action_key"], status="executed", detail=outcome, claim_snapshot=item)
    assert not leaf(item).exists() and queue.ledger().held()
    monkeypatch.setattr(queue, "_release_reservation", release)
    restarted = pool.PoolQueue(queue.root)
    restarted.sweep_finish_tombstones(grace_s=0)
    restarted.finish(item["action_key"], status="executed", detail=outcome, claim_snapshot=item)
    assert restarted.ledger().held() == {}


def test_successor_snapshot_refuses_registration_write(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    stale = copy.deepcopy(item)
    stale["published_unix"] += 1
    before = queue.item_path(pool.CLAIMED, item["action_key"]).read_bytes()
    with pytest.raises((scratch.LocalScratchError, pool.PoolContractError)):
        queue._record_scratch_lifetimes(item["action_key"], claim_snapshot=stale,
                                       env={})
    assert queue.item_path(pool.CLAIMED, item["action_key"]).read_bytes() == before


def test_versioned_request_requires_worker_capability(lifetime):
    queue, item, variables, calls = lifetime
    assert scratch.SCRATCH_LIFETIME_TAG in item["tags"]


def test_killed_launcher_existing_reaper_cleans_and_requeues(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    lease = pool._read_json(queue.lease_path(item["action_key"]))
    lease["heartbeat_unix"] = 1.0
    pool._write_json_atomic(queue.lease_path(item["action_key"]), lease)
    queue.reap_stale(timeout_s=1)
    assert not leaf(item).exists() and queue.ledger().held() == {}
    assert queue.item_path(pool.READY, item["action_key"]).exists()


def test_unrecorded_created_leaf_after_registration_crash_is_not_adopted(lifetime, monkeypatch):
    queue, item, variables, calls = lifetime
    create = scratch._create_scratch_directory

    def crash(declaration, parent_identity):
        create(declaration, parent_identity)
        raise OSError("fixture creation before identity commit")

    monkeypatch.setattr(scratch, "_create_scratch_directory", crash)
    process(monkeypatch)
    with pytest.raises(OSError, match="fixture creation before identity commit"):
        queue.execute(item, containment=True)
    pending = live_record(queue, item)
    assert pending[FIELD]["registration_complete"] is False
    assert pending[FIELD]["entries"][0]["identity"] is None
    queue.finish(item["action_key"], status="failed", detail={}, claim_snapshot=pending)
    assert queue.item_path(pool.CLAIMED, item["action_key"]).exists() and queue.ledger().held()


def test_old_worker_cannot_claim_versioned_retry(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch, returncode=1)
    queue.finish(item["action_key"], status="failed", detail=outcome, claim_snapshot=item)
    assert queue.claim(capacity=item["resources"], tags=[]) is None
    assert queue.item_path(pool.READY, item["action_key"]).exists()
    assert queue.ledger().held() == {}
    assert queue.claim(capacity=item["resources"], tags=[scratch.SCRATCH_LIFETIME_TAG]) is not None


def test_predecessor_late_finish_never_deletes_successor_scratch(lifetime, monkeypatch):
    queue, old, variables, outcome = launch(lifetime, monkeypatch, returncode=1)
    queue.finish(old["action_key"], status="failed", detail=outcome, claim_snapshot=old)
    successor = queue.claim(capacity=old["resources"], tags=[scratch.SCRATCH_LIFETIME_TAG])
    process(monkeypatch)
    queue.execute(successor, containment=True)
    sentinel = leaf(successor) / "successor"
    sentinel.write_bytes(b"successor owns this")
    before = queue.item_path(pool.CLAIMED, old["action_key"]).read_bytes()
    lease = queue.lease_path(old["action_key"]).read_bytes()
    held = queue.ledger().held()
    queue.finish(old["action_key"], status="failed", detail=outcome, claim_snapshot=old)
    assert sentinel.read_bytes() == b"successor owns this"
    assert queue.item_path(pool.CLAIMED, old["action_key"]).read_bytes() == before
    assert queue.lease_path(old["action_key"]).read_bytes() == lease
    assert queue.ledger().held() == held


def test_missing_record_in_snapshot_cannot_bypass_durable_owner(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    snapshot = copy.deepcopy(item)
    snapshot.pop(FIELD)
    result = queue.cleanup_action_containers(snapshot)
    assert result["complete"] and not leaf(item).exists()
    assert live_record(queue, item)[FIELD]["entries"][0]["cleaned"] is True
    assert queue.ledger().held()  # Only finish/reaper releases reservation.


def test_uncommitted_completion_in_snapshot_cannot_bypass_actual_cleanup(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    snapshot = copy.deepcopy(item)
    snapshot[FIELD]["entries"][0]["cleaned"] = True
    assert queue.cleanup_action_containers(snapshot)["complete"]
    assert not leaf(item).exists()


@pytest.mark.parametrize("phase", ["intent", "parent", "leaf", "ready"])
def test_registration_mirror_failure_prevents_launch_and_can_replay(
        lifetime, monkeypatch, phase):
    queue, item, variables, calls = lifetime
    write = pool._write_json_atomic
    launched = []

    def fail(path, value):
        record = value.get(FIELD)
        if path == queue.lease_path(item["action_key"]) and record:
            entry = record["entries"][0]
            observed = ("ready" if record["registration_complete"] else "leaf" if entry["identity"] else
                        "parent" if entry["parent_identity"] else "intent")
            if phase == observed:
                raise OSError("fixture mirror interruption")
        return write(path, value)

    monkeypatch.setattr(pool, "_write_json_atomic", fail)
    process(monkeypatch, lambda *args: launched.append(True))
    with pytest.raises(OSError, match="fixture mirror interruption"):
        queue.execute(item, containment=True)
    assert not launched and queue.ledger().held()
    monkeypatch.setattr(pool, "_write_json_atomic", write)
    scope = item["resource_scope"]
    evidence = queue._record_scratch_lifetimes(item["action_key"], claim_snapshot=item, env={
        "PRISMABUILD_ACTION_KEY": item["action_key"], "PRISMABUILD_ACTION_NONCE": scope["nonce"],
        "PRISMABUILD_ACTION_SCOPE": scope["scope_id"]})
    assert evidence["registration_complete"] is True and leaf(item).is_dir()
    assert json.loads(queue.lease_path(item["action_key"]).read_text())[FIELD] == evidence


def test_crash_before_leaf_creation_can_settle_without_deletion_authority(lifetime, monkeypatch):
    queue, item, variables, calls = lifetime
    monkeypatch.setattr(scratch, "_create_scratch_directory", lambda *args:
                        (_ for _ in ()).throw(OSError("fixture before leaf creation")))
    process(monkeypatch)
    with pytest.raises(OSError, match="fixture before leaf creation"):
        queue.execute(item, containment=True)
    pending = live_record(queue, item)
    assert pending[FIELD]["entries"][0]["parent_identity"] is not None
    queue.finish(item["action_key"], status="failed", detail={}, claim_snapshot=pending)
    assert queue.ledger().held() == {} and queue.item_path(pool.READY, item["action_key"]).exists()


def test_cleanup_mirror_failure_keeps_capacity_until_repaired(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    write = pool._write_json_atomic

    def fail(path, value):
        record = value.get(FIELD)
        if path == queue.lease_path(item["action_key"]) and record and record["entries"][0]["cleaned"]:
            raise OSError("fixture cleanup mirror interruption")
        return write(path, value)

    monkeypatch.setattr(pool, "_write_json_atomic", fail)
    queue.finish(item["action_key"], status="executed", detail=outcome, claim_snapshot=item)
    assert not leaf(item).exists() and queue.ledger().held()
    assert live_record(queue, item)[FIELD]["entries"][0]["cleaned"] is True
    monkeypatch.setattr(pool, "_write_json_atomic", write)
    queue.reap_stale()
    assert queue.ledger().held() == {} and queue.item_path(pool.DONE, item["action_key"]).exists()


def test_replay_rechecks_private_namespace_permissions_before_launch(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    leaf(item).chmod(0o777)
    with pytest.raises(scratch.LocalScratchError, match="permissions"):
        queue._record_scratch_lifetimes(item["action_key"], claim_snapshot=item, env={})
    assert (leaf(item) / "temp").read_bytes() == b"temporary" and queue.ledger().held()


def test_withdrawn_tombstone_releases_only_its_durably_cleaned_owner(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    key = item["action_key"]
    queue.withdraw(key, signal_child=False)
    withdrawn = queue.item_path(pool.WITHDRAWN, key).read_bytes()
    with monkeypatch.context() as fault:
        fault.setattr(queue, "_release_reservation", lambda *a, **k:
                      (_ for _ in ()).throw(OSError("fixture withdrawal before release")))
        with pytest.raises(OSError, match="fixture withdrawal before release"):
            queue.finish(key, status="withdrawn", detail=outcome, claim_snapshot=item)
    tombstones = list(queue.dir(pool.CLAIMED).glob(f"{key}.*{pool.TOMBSTONE_SUFFIX}"))
    assert len(tombstones) == 1 and queue.ledger().held()
    assert not leaf(item).exists() and not queue.item_path(pool.CLAIMED, key).exists()
    monkeypatch.setattr(scratch, "_clean_scratch_directory", lambda *a:
                        pytest.fail("durably consumed scratch must not be revisited"))
    assert queue.sweep_finish_tombstones(grace_s=-1) == [key]
    assert not tombstones[0].exists() and queue.ledger().held() == {}
    assert queue.item_path(pool.WITHDRAWN, key).read_bytes() == withdrawn
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert (Path(variables["CACHE_ROOT"]) / "compiled").read_bytes() == b"persistent"


@pytest.mark.parametrize("returncode,state", [(0, pool.DONE), (1, pool.READY)])
def test_published_ending_tombstone_recovers_without_rewriting_ending(
        lifetime, monkeypatch, returncode, state):
    queue, item, variables, outcome = launch(lifetime, monkeypatch, returncode=returncode)
    key = item["action_key"]
    ending = queue.item_path(state, key)
    write = pool._write_json_atomic

    def interrupt(path, value):
        write(path, value)
        if path == ending:
            raise OSError("fixture after ending publication")

    with monkeypatch.context() as fault:
        fault.setattr(pool, "_write_json_atomic", interrupt)
        with pytest.raises(OSError, match="fixture after ending publication"):
            queue.finish(key, status=outcome["status"], detail=outcome, claim_snapshot=item)
    before = ending.read_bytes()
    assert list(queue.dir(pool.CLAIMED).glob(f"{key}.*{pool.TOMBSTONE_SUFFIX}"))
    assert queue.sweep_finish_tombstones(grace_s=-1) == [key]
    assert ending.read_bytes() == before and queue.ledger().held() == {}
    assert not list(queue.dir(pool.CLAIMED).glob(f"{key}.*{pool.TOMBSTONE_SUFFIX}"))
    assert not queue.item_path(pool.CLAIMED, key).exists()


def test_widowed_scratch_lease_retains_charge_until_exact_cleanup_recovers(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    key = item["action_key"]
    queue.item_path(pool.CLAIMED, key).unlink()
    held = queue.ledger().held()
    with monkeypatch.context() as fault:
        fault.setattr(scratch, "_clean_scratch_directory", lambda *a:
                      (_ for _ in ()).throw(OSError("fixture orphan cleanup interruption")))
        assert queue.sweep_widowed_leases(timeout_s=-1) == []
    assert queue.ledger().held() == held and queue.lease_path(key).exists()
    assert (leaf(item) / "temp").read_bytes() == b"temporary"
    assert queue.sweep_widowed_leases(timeout_s=-1) == [key]
    assert queue.ledger().held() == {} and not queue.lease_path(key).exists()
    assert not leaf(item).exists() and not queue.item_path(pool.CLAIMED, key).exists()
    assert (Path(variables["CACHE_ROOT"]) / "compiled").read_bytes() == b"persistent"


@pytest.mark.parametrize("contradiction", ["owner", "nonce"])
def test_widowed_scratch_lease_refuses_contradictory_owner_before_scope_stop(
        lifetime, monkeypatch, contradiction):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    key = item["action_key"]
    queue.item_path(pool.CLAIMED, key).unlink()
    lease = pool._read_json(queue.lease_path(key))
    if contradiction == "owner":
        lease["owner"] = "foreign-worker"
    else:
        lease["resource_scope"]["nonce"] = "e" * 32
    pool._write_json_atomic(queue.lease_path(key), lease)
    before = queue.lease_path(key).read_bytes()
    held = queue.ledger().held()
    monkeypatch.setattr(resource_scope.ResourceScope, "terminate_owned", lambda *a, **k:
                        pytest.fail("contradictory lease must not stop a scope"))
    assert queue.sweep_widowed_leases(timeout_s=-1) == []
    assert queue.lease_path(key).read_bytes() == before and queue.ledger().held() == held
    assert (leaf(item) / "temp").read_bytes() == b"temporary"


def test_recovery_owner_cannot_override_a_live_claim(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    key = item["action_key"]
    tombstone = queue.dir(pool.CLAIMED) / f"{key}.fixture{pool.TOMBSTONE_SUFFIX}"
    saved = live_record(queue, item)
    pool._write_json_atomic(tombstone, saved)
    before = queue.item_path(pool.CLAIMED, key).read_bytes()
    held = queue.ledger().held()
    monkeypatch.setattr(resource_scope.ResourceScope, "terminate_owned", lambda *a, **k:
                        pytest.fail("recovery path must not control a live claim"))
    cleanup = queue.cleanup_action_containers(saved, scratch_owner_path=tombstone)
    assert cleanup["complete"] is False
    assert queue.item_path(pool.CLAIMED, key).read_bytes() == before
    assert queue.ledger().held() == held and (leaf(item) / "temp").read_bytes() == b"temporary"


@pytest.mark.parametrize("contradiction", ["nonce", "publication", "schema"])
def test_tombstone_refuses_invalid_or_mismatched_owner_without_touching_ready_successor(
        lifetime, monkeypatch, contradiction):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    key = item["action_key"]
    saved = live_record(queue, item)
    tombstone, mine = queue._entomb_claim(key, expect=saved)
    assert mine and tombstone is not None
    successor = copy.deepcopy(item)
    successor["published_unix"] += 1
    ready = queue._shape_as_ready_item(successor, action_key=key)
    pool._write_json_atomic(ready, successor)
    ready_bytes = ready.read_bytes()
    broken = pool._read_json(tombstone)
    if contradiction == "nonce":
        broken["resource_scope"]["nonce"] = "e" * 32
    elif contradiction == "publication":
        broken["published_unix"] += 2
    else:
        broken[FIELD]["schema"] = "unknown"
    pool._write_json_atomic(tombstone, broken)
    before = tombstone.read_bytes()
    lease = queue.lease_path(key).read_bytes()
    held = queue.ledger().held()
    monkeypatch.setattr(resource_scope.ResourceScope, "terminate_owned", lambda *a, **k:
                        pytest.fail("invalid tombstone must not stop a scope"))
    assert queue.sweep_finish_tombstones(grace_s=-1) == []
    assert tombstone.read_bytes() == before and ready.read_bytes() == ready_bytes
    assert queue.lease_path(key).read_bytes() == lease and queue.ledger().held() == held
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert (leaf(item) / "temp").read_bytes() == b"temporary"


def test_old_tombstone_never_mutates_a_claimed_successors_scratch_or_capacity(lifetime, monkeypatch):
    queue, old, variables, outcome = launch(lifetime, monkeypatch, returncode=1)
    key = old["action_key"]
    saved = live_record(queue, old)
    queue.finish(key, status="failed", detail=outcome, claim_snapshot=old)
    successor = queue.claim(capacity=old["resources"], tags=[scratch.SCRATCH_LIFETIME_TAG])
    process(monkeypatch)
    queue.execute(successor, containment=True)
    sentinel = leaf(successor) / "successor"
    sentinel.write_bytes(b"successor owns this")
    tombstone = queue.dir(pool.CLAIMED) / f"{key}.old-fixture{pool.TOMBSTONE_SUFFIX}"
    pool._write_json_atomic(tombstone, saved)
    before = queue.item_path(pool.CLAIMED, key).read_bytes()
    lease = queue.lease_path(key).read_bytes()
    held = queue.ledger().held()
    monkeypatch.setattr(resource_scope.ResourceScope, "terminate_owned", lambda *a, **k:
                        pytest.fail("old tombstone must not stop the successor"))
    assert queue.cleanup_action_containers(saved, scratch_owner_path=tombstone)["complete"] is False
    assert queue.sweep_finish_tombstones(grace_s=-1) == [key]
    assert queue.item_path(pool.CLAIMED, key).read_bytes() == before
    assert queue.lease_path(key).read_bytes() == lease and queue.ledger().held() == held
    assert sentinel.read_bytes() == b"successor owns this" and not tombstone.exists()

