"""Dedicated scratch charges its own backing, leaving ordinary spool intact."""
from __future__ import annotations

import hashlib
import json
import copy
from types import SimpleNamespace

import pytest

from prismabuild import contracts, filesystem_floor, local_scratch, pool, submission
from prismabuild import scratch_capacity
from test_scratch_declaration_record import runtime, process, live_record, context  # noqa: F401


GIB = 1 << 30
ROOT = "/storage_pool/tl2-qualification-vm"
SELECTION = "PRISMABUILD_LOCAL_SCRATCH_CAPACITY"
KIND = "scratch_gb:" + hashlib.sha256(ROOT.encode()).hexdigest()


def variables():
    return {
        "VM_ROOT": ROOT,
        "VM_MAX": str(320 * GIB),
        local_scratch.PAIRS_ENV: "VM_ROOT:VM_MAX",
        SELECTION: json.dumps({
            "schema": "prismabuild.scratch_capacity_selection.v1",
            "root_envs": ["VM_ROOT"],
        }),
    }


def test_o6_reserves_dedicated_capacity_without_charging_ordinary_spool():
    assert submission.local_disk_terms(variables(), transport="pool") == {KIND: 320}


def test_dedicated_scratch_and_ordinary_output_keep_separate_budgets():
    env = {**variables(), "PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES": str(3 * GIB)}
    assert submission.local_disk_terms(env, transport="pool") == {
        KIND: 320, "spool_gb": 3,
    }


def test_unselected_pairs_keep_the_ordinary_charge():
    env = {**variables(), "CACHE_ROOT": "/cache", "CACHE_MAX": str(GIB + 1),
           local_scratch.PAIRS_ENV: "VM_ROOT:VM_MAX,CACHE_ROOT:CACHE_MAX"}
    assert local_scratch.scratch_terms(env) == {KIND: 320, "spool_gb": 2}
    env.pop(SELECTION)
    assert local_scratch.scratch_terms(env) == {"spool_gb": 322}


@pytest.mark.parametrize("selected", [
    "{", "null", "[]", "{}",
    '{"schema":"prismabuild.scratch_capacity_selection.v1","root_envs":[],"extra":1}',
    '{"schema":"wrong","root_envs":["VM_ROOT"]}',
    '{"schema":"prismabuild.scratch_capacity_selection.v1","root_envs":[]}',
    '{"schema":"prismabuild.scratch_capacity_selection.v1","root_envs":[true]}',
    '{"schema":"prismabuild.scratch_capacity_selection.v1","root_envs":["UNKNOWN"]}',
    '{"schema":"prismabuild.scratch_capacity_selection.v1","root_envs":["VM_ROOT","VM_ROOT"]}',
    '{"schema":"prismabuild.scratch_capacity_selection.v1","root_envs":["VM_ROOT"],"root_envs":[]}',
    " " * 16385,
])
def test_invalid_selection_cannot_silently_charge_a_different_backing(selected):
    with pytest.raises(submission.SubmissionRefused):
        submission.local_disk_terms({**variables(), SELECTION: selected}, transport="pool")


def test_selection_without_pairs_and_slurm_are_refused():
    env = variables()
    env.pop(local_scratch.PAIRS_ENV)
    with pytest.raises(submission.SubmissionRefused, match="undeclared"):
        submission.local_disk_terms(env, transport="pool")
    with pytest.raises(submission.SubmissionRefused, match="pull queue"):
        submission.local_disk_terms(variables(), transport="slurm")


def test_prepare_seals_the_capability_and_selection_changes_identity(tmp_path, monkeypatch):
    from test_local_scratch_host_kind import _build, _env

    template = _build(tmp_path, monkeypatch, *_env(*[
        f"{key}={value}" for key, value in variables().items()]))
    assert template["params"]["demand"] == {"cpu": 1, "mem_gb": 4, KIND: 320}
    assert "scratch-capacity-v1" in template["params"]["placement"]["required_tags"]
    original_key = submission.seal(template)["action_key"]
    template["environment"]["variables"].pop(SELECTION)
    assert submission.seal(template)["action_key"] != original_key


def test_freeze_requires_derived_capacity_and_worker_fence(tmp_path, monkeypatch):
    from submission_support import patch_store
    from test_local_scratch_host_kind import _freeze
    import pbrun

    patch_store(monkeypatch, client=pbrun, root=tmp_path / "fleet")
    with pytest.raises(submission.SubmissionRefused, match="scratch-capacity-v1"):
        _freeze(tmp_path, demand={"cpu": 1, "mem_gb": 1, KIND: 320}, variables=variables())


@pytest.mark.parametrize("changed", ["demand", "tag", "selection"])
def test_publication_refuses_forged_dedicated_capacity(changed):
    env = variables()
    demand = {"cpu": 1, "mem_gb": 4, KIND: 320}
    request = {"environment": {"variables": env},
               "params": {"demand": demand, "placement": {
                   "required_tags": [contracts.SCRATCH_CAPACITY_TAG]}}}
    if changed == "demand":
        demand[KIND] = 319
    elif changed == "tag":
        request["params"]["placement"]["required_tags"] = []
    else:
        env.pop(SELECTION)
    with pytest.raises(pool.PoolContractError):
        pool.PoolQueue._sealed_scratch_capacity(
            request, tags=[contracts.SCRATCH_CAPACITY_TAG], resources=demand)


def test_root_tokens_cannot_overcommit_or_spend_ordinary_spool(tmp_path):
    ledger = pool.ResourceLedger(tmp_path / "reservations", host="proof")
    ledger.ensure_capacity({"spool_gb": 8, KIND: 320})
    assert ledger.acquire("a" * 64, {KIND: 320})
    assert ledger.acquire("b" * 64, {"spool_gb": 8})
    assert not ledger.acquire("c" * 64, {KIND: 1})
    assert ledger.held() == {KIND: 320, "spool_gb": 8}
    ledger.release("a" * 64)
    assert ledger.held() == {"spool_gb": 8}
    assert ledger.acquire("c" * 64, {KIND: 320})


IDENTITY = {"device": "22", "filesystem": "33", "root_inode": "44",
            "filesystem_type": "zfs", "birthtime_ns": 100}
BACKING = {"dataset": "storage_pool/tl2-qualification-vm", "guid": "1234",
           "quota_bytes": 384 * GIB, "reservation_bytes": 384 * GIB}
MOUNT = {"source": BACKING["dataset"], "filesystem_type": "zfs", "major_minor": "0:22",
         "mountpoint": ROOT, "mount_root": "/", "mount_id": 12}


@pytest.fixture
def configured_root(tmp_path, monkeypatch):
    config = tmp_path / "roots.json"
    config.write_text(json.dumps({"schema": "prismabuild.scratch_capacity_roots.v1", "roots": [
        {"root": ROOT, "max_gib": 320, "min_free_gib": 32, "identity": IDENTITY, "backing": BACKING},
    ]}))
    found = {"identity": dict(IDENTITY), "mount": dict(MOUNT), "size_bytes": 384 * GIB,
             "free_bytes": 384 * GIB, "size_inodes": 10000, "free_inodes": 10000}
    monkeypatch.setattr(scratch_capacity, "inspect_root", lambda _root: dict(found))
    monkeypatch.setattr(local_scratch, "root_backing", lambda _root: {
        **MOUNT, "major_minor": "0:11", "source": "/dev/ordinary"})
    monkeypatch.setattr(scratch_capacity, "zfs_properties", lambda _dataset: {
        "guid": "1234", "quota": str(384 * GIB), "reservation": str(384 * GIB),
        "available": str(384 * GIB), "mounted": "yes", "mountpoint": ROOT})
    return scratch_capacity.RootInputs(config, ordinary_root="/ordinary"), found


def test_observer_measures_the_root_and_honors_the_configured_ceiling(configured_root):
    inputs, found = configured_root
    ledger = SimpleNamespace(capacity=lambda: {}, held=lambda: {})
    declared, live = inputs.observe(ledger)
    assert declared == live == {KIND: 320}
    assert inputs.require(variables())[0]["floor_bytes"] == 32 * GIB
    found["free_bytes"] = 100 * GIB
    assert inputs.observe(ledger) == ({KIND: 68}, {KIND: 68})
    with pytest.raises(local_scratch.LocalScratchError, match="insufficient"):
        inputs.require(variables())


@pytest.mark.parametrize("change", ["identity", "inode", "ordinary", "census"])
def test_unknown_or_aliasing_backing_never_grants_capacity(configured_root, monkeypatch, change):
    inputs, found = configured_root
    ledger = SimpleNamespace(capacity=lambda: {}, held=lambda: {})
    if change == "identity":
        found["identity"] = {**IDENTITY, "birthtime_ns": 101}
    elif change == "inode":
        found["free_inodes"] = 499
    elif change == "ordinary":
        monkeypatch.setattr(local_scratch, "root_backing", lambda _root: dict(MOUNT))
    else:
        def unreadable():
            raise OSError("fixture holder census unreadable")
        ledger.held = unreadable
    assert inputs.observe(ledger) == ({KIND: 0}, {KIND: 0})


def test_a_holder_never_creates_credit_for_unwritten_bytes(configured_root):
    inputs, found = configured_root
    # A holder wrote only 10 GiB of a 166-GiB reservation. Adding its full
    # reservation to available bytes would incorrectly inflate this total.
    found["free_bytes"] = 374 * GIB
    held = iter([{}, {KIND: 166}])
    ledger = SimpleNamespace(capacity=lambda: {KIND: 200}, held=lambda: next(held))
    assert inputs.observe(ledger) == ({KIND: 320}, {KIND: 166})
    assert inputs.detail["roots"][0]["basis"] == "holders-only"


def test_unknown_root_or_excess_bound_refuses_at_launch(configured_root):
    inputs, _found = configured_root
    with pytest.raises(local_scratch.LocalScratchError, match="not configured"):
        inputs.require({**variables(), "VM_ROOT": "/other"})
    with pytest.raises(local_scratch.LocalScratchError, match="not configured"):
        inputs.require({**variables(), "VM_MAX": str(321 * GIB)})


def test_filesystem_floor_recognizes_dedicated_byte_kinds():
    assert filesystem_floor.byte_unit(KIND) == GIB
    assert filesystem_floor.byte_unit("scratch_gb:malformed") is None
    assert filesystem_floor.byte_unit("cpu") is None


def test_unreserved_zfs_capacity_never_becomes_an_offer(configured_root, monkeypatch):
    inputs, _found = configured_root
    monkeypatch.setattr(scratch_capacity, "zfs_properties", lambda _dataset: {
        "guid": "1234", "quota": str(384 * GIB), "reservation": "0",
        "available": str(384 * GIB), "mounted": "yes", "mountpoint": ROOT})
    ledger = SimpleNamespace(capacity=lambda: {}, held=lambda: {})
    assert inputs.observe(ledger) == ({KIND: 0}, {KIND: 0})


def test_btrfs_subvolumes_do_not_supply_independent_capacity(tmp_path):
    config = tmp_path / "roots.json"
    config.write_text(json.dumps({"schema": "prismabuild.scratch_capacity_roots.v1", "roots": [
        {"root": ROOT, "max_gib": 320, "min_free_gib": 32,
         "identity": {**IDENTITY, "filesystem_type": "btrfs"}, "backing": BACKING},
    ]}))
    with pytest.raises(local_scratch.LocalScratchError, match="ZFS"):
        scratch_capacity.RootInputs(config, ordinary_root="/ordinary")


def test_enforced_floor_does_not_bypass_an_unbound_root(tmp_path, monkeypatch):
    monkeypatch.setattr(filesystem_floor, "mode", lambda *_a, **_kw: "enforce")
    monkeypatch.setattr(filesystem_floor, "member_bindings", lambda *_a: {})
    ledger = pool.ResourceLedger(tmp_path / "reservations", host="proof")
    gate = filesystem_floor.ledger_gate(ledger, {KIND: 320})
    assert gate is not None
    assert gate.unbound == [filesystem_floor.member_id("reservations", "proof", KIND)]


def test_dedicated_claim_envelope_does_not_require_ordinary_spool():
    nonce, key = "1" * 32, "2" * 64
    scope = "prismabuild-job" + hashlib.sha256((key + nonce).encode()).hexdigest()[:32] + ".slice"
    envelope = local_scratch._scratch_claim_envelope({
        "action_key": key, "published_unix": 1.0, "claimed_unix": 2.0,
        "claimed_host": "proof", "claimed_by": "worker", "attempts": 0,
        "cas_root": "/cas", "resources": {KIND: 320},
        "resource_scope": {"action_key": key, "nonce": nonce, "scope_id": scope},
    })
    assert envelope["resources"] == {KIND: 320}
    assert local_scratch.reservation_matches(variables(), {KIND: 320}, envelope["resources"])
    assert not local_scratch.reservation_matches(variables(), {KIND: 320}, {"spool_gb": 320})


def test_an_empty_root_is_not_an_independent_mount(tmp_path):
    with pytest.raises(local_scratch.LocalScratchError):
        scratch_capacity.inspect_root(str(tmp_path))


@pytest.mark.parametrize("property_name,value", [
    ("quota", "0"), ("reservation", "0"), ("guid", "5678"),
    ("mounted", "no"), ("mountpoint", "/other"), ("available", "unknown"),
])
def test_allocation_drift_refuses_both_offer_and_launch(configured_root, monkeypatch, property_name, value):
    inputs, _found = configured_root
    properties = scratch_capacity.zfs_properties(BACKING["dataset"])
    properties[property_name] = value
    monkeypatch.setattr(scratch_capacity, "zfs_properties", lambda _dataset: properties)
    ledger = SimpleNamespace(capacity=lambda: {}, held=lambda: {})
    assert inputs.observe(ledger) == ({KIND: 0}, {KIND: 0})
    with pytest.raises(local_scratch.LocalScratchError, match="ZFS"):
        inputs.require(variables())


@pytest.mark.parametrize("fault", ["fields", "zero", "identity", "reservation", "alias"])
def test_config_refuses_invalid_or_aliased_allocations(tmp_path, fault):
    entry = {"root": ROOT, "max_gib": 320, "min_free_gib": 32,
             "identity": copy.deepcopy(IDENTITY), "backing": copy.deepcopy(BACKING)}
    entries = [entry]
    if fault == "fields":
        entry["extra"] = 1
    elif fault == "zero":
        entry["max_gib"] = 0
    elif fault == "identity":
        entry["identity"]["birthtime_ns"] = True
    elif fault == "reservation":
        entry["backing"]["reservation_bytes"] = 351 * GIB
    else:
        entries.append({**entry, "root": "/alias"})
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"schema": scratch_capacity.CONFIG_SCHEMA, "roots": entries}))
    with pytest.raises(local_scratch.LocalScratchError):
        scratch_capacity.RootInputs(config, ordinary_root="/ordinary")


def test_no_holder_allows_a_fresh_measurement_above_the_old_total(configured_root):
    inputs, _found = configured_root
    ledger = SimpleNamespace(capacity=lambda: {KIND: 200}, held=lambda: {})
    assert inputs.observe(ledger) == ({KIND: 320}, {KIND: 320})
    assert inputs.detail["roots"][0]["basis"] == "measured-free"


def test_worker_uses_live_root_capacity_instead_of_the_cpu_observer_override(monkeypatch):
    import worker_loop

    ledger = SimpleNamespace(held=lambda: {}, base=None)
    inputs = SimpleNamespace(observe=lambda _ledger: ({KIND: 320}, {KIND: 166}))
    queue = SimpleNamespace(ledger=lambda: ledger, scratch_capacity_inputs=inputs)
    observer = SimpleNamespace(offer=lambda *_a, **_kw: {"cpu": 1, "mem_gb": 4, KIND: 320})
    monkeypatch.setattr(worker_loop.gpu_admission, "host_local_power_state", lambda _base: None)
    declared, live, _gpus = worker_loop.observed_claim_capacity(
        queue, args=SimpleNamespace(gpu=False, assume_idle=False), base_declared={"cpu": 1, "mem_gb": 4},
        observer=observer, gpu_sample=None, known_physical_gpus=0)
    assert declared[KIND] == 320
    assert live[KIND] == 166


def test_roster_wiring_detects_an_alias_with_the_actual_ordinary_root(configured_root, tmp_path, monkeypatch):
    import worker_loop

    inputs, _found = configured_root
    entry = inputs.roots[ROOT]
    config = tmp_path / "again.json"
    config.write_text(json.dumps({"schema": scratch_capacity.CONFIG_SCHEMA, "roots": [{
        "root": ROOT, "max_gib": 320, "min_free_gib": 32, "identity": entry.identity,
        "backing": entry.backing}]}))
    monkeypatch.setattr(worker_loop.fleet_roster.Roster, "at", lambda _root: SimpleNamespace(
        canonical_box=lambda _host: SimpleNamespace(entry={"local_disk": "/actual-ordinary"})))
    monkeypatch.setattr(local_scratch, "root_backing", lambda root: (
        dict(MOUNT) if root == "/actual-ordinary" else {**MOUNT, "source": "/other"}))
    wired = worker_loop.configured_scratch_capacity(config, runtime_root=tmp_path, host="proof")
    ledger = SimpleNamespace(capacity=lambda: {}, held=lambda: {})
    assert wired.observe(ledger) == ({KIND: 0}, {KIND: 0})


def test_unconfigured_workers_do_not_offer_dedicated_capacity():
    assert contracts.SCRATCH_CAPACITY_TAG not in contracts.offered_contract_tags(contracts.WorkerFacts())
    assert contracts.SCRATCH_CAPACITY_TAG in contracts.offered_contract_tags(
        contracts.WorkerFacts(scratch_capacity_configured=True))


def _published(tmp_path, monkeypatch, inputs=None):
    from test_local_scratch_host_kind import _build, _env

    template = _build(tmp_path, monkeypatch, *_env(*[
        f"{key}={value}" for key, value in variables().items()]))
    action = submission.seal(template)
    template["cas"].publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "queue", scratch_capacity_inputs=inputs)
    path = queue.publish(action_key=action["action_key"], cas_root=template["cas"].root,
                         checkout_root=tmp_path, worker_script="/worker.py",
                         resources=action["params"]["demand"], tags=action["params"]["placement"]["required_tags"])
    return queue, action, json.loads(path.read_text())


def test_publish_and_claim_cannot_spend_unconfigured_root_capacity(tmp_path, monkeypatch):
    queue, action, row = _published(tmp_path, monkeypatch)
    assert row["resources"] == {"cpu": 1, "mem_gb": 4, KIND: 320}
    reasons = []
    monkeypatch.setattr(queue, "record_denial", lambda _row, reason, _detail=None: reasons.append(reason))
    assert queue.claim(tags=row["tags"], capacity=row["resources"]) is None
    assert "scratch_capacity_intent_invalid" in reasons
    assert queue.ledger().held() == {}
    assert queue.records.item_path(pool.record_store.READY, action["action_key"]).exists()


def test_a_configured_claim_still_requires_current_backing(tmp_path, configured_root, monkeypatch):
    inputs, found = configured_root
    queue, _action, row = _published(tmp_path, monkeypatch, inputs)
    found["identity"] = {**IDENTITY, "birthtime_ns": 101}
    assert queue.claim(tags=row["tags"], capacity=row["resources"]) is None
    assert queue.ledger().held() == {}


def test_floor_status_lists_unbound_dedicated_byte_kinds(tmp_path, monkeypatch):
    monkeypatch.setattr(filesystem_floor, "mode", lambda *_a, **_kw: "enforce")
    queue_root = tmp_path / "queue"
    ledger = pool.ResourceLedger(queue_root / "reservations", host="proof")
    ledger.ensure_capacity({KIND: 1})
    status = filesystem_floor.floor_status(queue_root)
    assert filesystem_floor.member_id("reservations", "proof", KIND) in status["unbound_byte_ledgers"]


@pytest.mark.parametrize("fault", ["none", "extra", "kind", "allocation"])
def test_persisted_observations_accept_only_the_known_consistent_shape(configured_root, fault):
    inputs, _found = configured_root
    record = {"schema": scratch_capacity.OBSERVATION_SCHEMA, "roots": inputs.require(variables())}
    if fault == "none":
        assert scratch_capacity.validate_observation(record)["roots"][0]["backing"]["guid"] == "1234"
        return
    if fault == "extra":
        record["roots"][0]["extra"] = "untrusted"
    elif fault == "kind":
        record["roots"][0]["kind"] = "spool_gb"
    else:
        record["roots"][0]["backing"]["reservation_bytes"] = 0
    with pytest.raises(local_scratch.LocalScratchError):
        scratch_capacity.validate_observation(record)


def _dedicated_attempt(runtime, monkeypatch):
    """Real CAS, ledger and scratch paths; broker/process endpoints are simulated."""
    from prismabuild import core as pb, scratch_lifetime, resource_scope

    create, calls = runtime
    seal = pb.seal_action

    def dedicated(body):
        if body["task"]["definition_id"] == "tests/scratch-record":
            env = body["environment"]["variables"]
            env[local_scratch.PAIRS_ENV] = "TEMP_ROOT:TEMP_MAX"
            env[SELECTION] = json.dumps({"schema": local_scratch.CAPACITY_SCHEMA,
                                        "root_envs": ["TEMP_ROOT"]})
            body["params"]["demand"].pop("spool_gb")
            body["params"]["demand"].update(local_scratch.scratch_terms(env))
            body["params"]["placement"] = {"required_tags": [contracts.SCRATCH_CAPACITY_TAG]}
        return seal(body)

    def observed(env):
        root = env["TEMP_ROOT"]
        return [{"root": root, "kind": "scratch_gb:" + hashlib.sha256(root.encode()).hexdigest(),
                 "identity": dict(IDENTITY), "mount": {**MOUNT, "mountpoint": root},
                 "backing": dict(BACKING), "size_bytes": 384 * GIB, "free_bytes": 384 * GIB,
                 "size_inodes": 10000, "free_inodes": 10000, "max_gib": 1,
                 "floor_bytes": 32 * GIB, "measured_gib": 1}]

    authority = SimpleNamespace(require=observed)
    initialize, claim = pool.PoolQueue.__init__, pool.PoolQueue.claim
    monkeypatch.setattr(pb, "seal_action", dedicated)
    monkeypatch.setattr(pool.PoolQueue, "__init__", lambda self, *a, **kw: initialize(
        self, *a, **{**kw, "scratch_capacity_inputs": authority}))
    monkeypatch.setattr(pool.PoolQueue, "claim", lambda self, **kw: claim(
        self, **{**kw, "tags": [contracts.SCRATCH_CAPACITY_TAG, contracts.SCRATCH_LIFETIME_TAG]}))
    selected = json.dumps({"schema": scratch_lifetime.SCRATCH_LIFETIME_SELECTION_SCHEMA_V1,
                           "entries": [{"root_env": "TEMP_ROOT", "name": "vm", "lifetime": "ephemeral"}]})
    queue, item, env = create(selected)
    from pathlib import Path
    Path(env["TEMP_ROOT"]).mkdir()
    monkeypatch.setattr(resource_scope.ResourceScope, "export_stopped_verdict", lambda scope: {
        "ok": True, "scope_id": scope.unit, "stopped": True, "empty": True, "released": True,
        "retired": False, "settled": True, "tickets_pending": False, "stopped_unix": 1.0})
    return queue, item, env, authority, calls


def test_launch_rechecks_backing_before_payload_and_retains_the_claim(runtime, monkeypatch):
    queue, item, _env, authority, _calls = _dedicated_attempt(runtime, monkeypatch)
    launched = []
    process(monkeypatch, lambda *_a: launched.append(True))
    def changed(_env):
        raise local_scratch.LocalScratchError("fixture backing changed")
    authority.require = changed
    with pytest.raises(local_scratch.LocalScratchError, match="backing changed"):
        queue.execute(item, containment=True)
    assert launched == []
    assert queue.ledger().held() == item["resources"]


@pytest.mark.parametrize("cleanup_error", [False, True])
def test_dedicated_only_lifetime_deletes_or_retains_its_actual_root_tokens(runtime, monkeypatch, cleanup_error):
    from prismabuild import scratch_lifetime

    queue, item, _env, _authority, _calls = _dedicated_attempt(runtime, monkeypatch)
    def write_leaf(*_args):
        entry = live_record(queue, item)["scratch_lifetime_record"]["entries"][0]
        leaf = local_scratch.ephemeral_scratch_path(entry["declaration"])
        (leaf / "sentinel").write_bytes(b"disposable scratch")
    process(monkeypatch, write_leaf)
    outcome = queue.execute(item, containment=True)
    declaration = item["scratch_lifetime_record"]["entries"][0]["declaration"]
    leaf = local_scratch.ephemeral_scratch_path(declaration)
    assert "spool_gb" not in item["resources"]
    assert local_scratch.bind_ephemeral_scratch(
        queue, root_env="TEMP_ROOT", name="vm", claim_snapshot=live_record(queue, item), env=context(item)) == declaration
    if cleanup_error:
        def interrupted(*_a):
            raise OSError("fixture cleanup ambiguous")
        monkeypatch.setattr(scratch_lifetime, "_clean_scratch_directory", interrupted)
    result = queue.finish(item["action_key"], status=outcome["status"], detail=outcome, claim_snapshot=item)
    if cleanup_error:
        assert result == queue.records.item_path(pool.record_store.CLAIMED, item["action_key"])
        assert queue.ledger().held() == item["resources"]
        assert (leaf / "sentinel").read_bytes() == b"disposable scratch"
    else:
        assert not leaf.exists()
        assert queue.ledger().held() == {}
        ending = pool.record_store.value_or_raise(queue.records.item(pool.record_store.DONE, item["action_key"]))
        assert ending["detail"]["scratch_capacity_observation"]["roots"][0]["kind"] in item["resources"]
