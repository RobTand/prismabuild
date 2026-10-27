"""Dedicated scratch charges its own backing, leaving ordinary spool intact."""
from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest

from prismabuild import contracts, filesystem_floor, local_scratch, pool, submission
from prismabuild import scratch_capacity


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


@pytest.fixture
def configured_root(tmp_path, monkeypatch):
    config = tmp_path / "roots.json"
    config.write_text(json.dumps({"schema": "prismabuild.scratch_capacity_roots.v1", "roots": [
        {"root": ROOT, "max_gib": 320, "min_free_gib": 32, "identity": IDENTITY},
    ]}))
    found = {"identity": dict(IDENTITY), "size_bytes": 384 * GIB,
             "free_bytes": 384 * GIB, "size_inodes": 10000, "free_inodes": 10000}
    monkeypatch.setattr(scratch_capacity, "inspect_root", lambda _root: dict(found))
    monkeypatch.setattr(local_scratch, "root_identity", lambda _root: {
        **IDENTITY, "device": "11", "filesystem": "12"})
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
        monkeypatch.setattr(local_scratch, "root_identity", lambda _root: dict(IDENTITY))
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
    ledger = SimpleNamespace(capacity=lambda: {KIND: 320}, held=lambda: next(held))
    assert inputs.observe(ledger) == ({KIND: 320}, {KIND: 320})


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
