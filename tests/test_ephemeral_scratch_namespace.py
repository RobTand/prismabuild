"""Generation/attempt/host-bound scratch naming only (Refs #1360).

These CPU fixtures exercise the public SDK against a sealed request and a
contained fixture queue. No payload, broker, cleanup, or live directory runs.
A namespace is not a finalizer, filesystem quota, or deletion authority.
"""
from __future__ import annotations

import copy
import hashlib
import json

import pytest

from prismabuild import client, pool
from prismabuild import core as pb


@pytest.fixture
def scratch(tmp_path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "code.py").write_text("# fixture closure\n")
    root = tmp_path / "scratch"
    cache = tmp_path / "persistent-cache"
    variables = {
        "PRISMABUILD_LOCAL_SCRATCH_PAIRS": "TEMP_ROOT:TEMP_MAX,CACHE_ROOT:CACHE_MAX",
        "TEMP_ROOT": str(root), "TEMP_MAX": "1024",
        "CACHE_ROOT": str(cache), "CACHE_MAX": "2048",
    }
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/ephemeral-scratch", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true"], "working_directory": ".",
                 "result_path": "result.bin"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["code.py"]),
        "params": {"demand": {"spool_gb": 2}},
        "environment": {"variables": variables, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    key = str(action["action_key"])
    nonce = "1" * 32
    scope = "prismabuild-job" + hashlib.sha256((key + nonce).encode()).hexdigest()[:32] + ".slice"
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    claim = {"action_key": key, "cas_root": str(cas.root),
             "claimed_by": "fixture-host:123:worker", "claimed_host": "fixture-host",
             "claimed_unix": 123.0, "published_unix": 100.0, "attempts": 0,
             "resources": {"spool_gb": 2},
             "resource_scope": {"action_key": key, "nonce": nonce, "scope_id": scope}}
    claim_path = queue.item_path(pool.CLAIMED, key)
    claim_path.write_text(json.dumps(claim))
    env = {"PRISMABUILD_ACTION_KEY": key, "PRISMABUILD_ACTION_NONCE": nonce,
           "PRISMABUILD_ACTION_SCOPE": scope,
           # A writable runtime environment cannot redirect the sealed root.
           "TEMP_ROOT": str(tmp_path / "foreign")}
    return {"queue": queue, "claim": claim, "claim_path": claim_path, "env": env,
            "root": root, "cache": cache, "cas": cas, "action": action}


def _bind(scratch, **kwargs):
    return client.bind_ephemeral_scratch(
        scratch["queue"], root_env="TEMP_ROOT", name="row-temp",
        claim_snapshot=scratch["claim"], env=scratch["env"], **kwargs)


def test_public_binding_is_sealed_and_nondestructive(scratch):
    scratch["cache"].mkdir()
    marker = scratch["cache"] / "cache.bin"
    marker.write_bytes(b"persistent")
    declaration = _bind(scratch)
    assert declaration["schema"] == client.EPHEMERAL_SCRATCH_SCHEMA_V1
    assert declaration["root"] == str(scratch["root"])
    assert declaration["max_bytes"] == 1024
    assert declaration["root_env"] == "TEMP_ROOT"
    assert declaration["max_env"] == "TEMP_MAX"
    assert declaration["lifetime"] == "ephemeral"
    assert declaration["owner_action_key"] == scratch["claim"]["action_key"]
    assert declaration["owner_published_unix"] == 100.0
    assert declaration["owner_host"] == "fixture-host"
    assert declaration["owner_attempt"] == {k: scratch["claim"]["resource_scope"][k]
                                             for k in ("nonce", "scope_id")}
    path = client.ephemeral_scratch_path(declaration)
    assert path.is_relative_to(scratch["root"] / "prismabuild-ephemeral")
    assert path.name == "row-temp"
    assert scratch["claim"]["action_key"] in path.parts
    assert scratch["claim"]["resource_scope"]["nonce"] in path.parts
    assert not scratch["root"].exists(), "declaration must not create or delete directories"
    assert marker.read_bytes() == b"persistent"
    assert _bind(scratch) == declaration
    assert client.ephemeral_scratch_path(json.loads(json.dumps(declaration))) == path


@pytest.mark.parametrize("change", ["generation", "attempt", "host"])
def test_namespace_never_aliases_successors(scratch, change):
    old = _bind(scratch)
    claim = scratch["claim"]
    if change == "generation":
        claim["published_unix"] += 1
    elif change == "host":
        claim["claimed_host"] = "other-host"
        claim["claimed_by"] = "other-host:456:worker"
    else:
        claim["attempts"] += 1
        nonce = "2" * 32
        scope = "prismabuild-job" + hashlib.sha256(
            (claim["action_key"] + nonce).encode()).hexdigest()[:32] + ".slice"
        claim["resource_scope"].update(nonce=nonce, scope_id=scope)
        scratch["env"].update(PRISMABUILD_ACTION_NONCE=nonce, PRISMABUILD_ACTION_SCOPE=scope)
    scratch["claim_path"].write_text(json.dumps(claim))
    new = _bind(scratch)
    assert client.ephemeral_scratch_path(old) != client.ephemeral_scratch_path(new)


@pytest.mark.parametrize("field", ["PRISMABUILD_ACTION_KEY", "PRISMABUILD_ACTION_NONCE",
                                  "PRISMABUILD_ACTION_SCOPE"])
def test_missing_launch_half_is_not_inferred_from_live_claim(scratch, field):
    del scratch["env"][field]
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)


def test_stale_launch_cannot_bind_successor(scratch):
    scratch["env"]["PRISMABUILD_ACTION_NONCE"] = "2" * 32
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)


@pytest.mark.parametrize("change", ["generation", "attempt", "host", "scope", "missing"])
def test_binding_refuses_changed_or_absent_claim(scratch, change):
    live = copy.deepcopy(scratch["claim"])
    if change == "missing":
        scratch["claim_path"].unlink()
    else:
        if change == "generation":
            live["published_unix"] += 1
        elif change == "attempt":
            live["attempts"] += 1
        elif change == "host":
            live["claimed_host"] = "foreign-host"
        else:
            live["resource_scope"]["scope_id"] = "foreign.slice"
        scratch["claim_path"].write_text(json.dumps(live))
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)


@pytest.mark.parametrize("name", ["", ".", "..", "../cache", "/cache", "a/b", "a\\b", "a\x00b"])
def test_child_name_cannot_escape_namespace(scratch, name):
    with pytest.raises(client.LocalScratchError):
        client.bind_ephemeral_scratch(scratch["queue"], root_env="TEMP_ROOT", name=name,
                                      claim_snapshot=scratch["claim"], env=scratch["env"])


def test_undeclared_root_and_missing_request_refuse(scratch):
    with pytest.raises(client.LocalScratchError):
        client.bind_ephemeral_scratch(scratch["queue"], root_env="UNDECLARED", name="temp",
                                      claim_snapshot=scratch["claim"], env=scratch["env"])
    key = scratch["claim"]["action_key"]
    (scratch["cas"].root / "requests" / key[:2] / f"{key}.json").unlink()
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)


@pytest.mark.parametrize("field,value", [
    ("owner_published_unix", True), ("owner_published_unix", float("nan")),
    ("owner_host", ""), ("owner_action_key", "a"),
    ("owner_attempt", {"nonce": "1" * 32, "scope_id": "foreign.slice"}),
    ("root", "/"), ("root", "/a/../b"), ("root", "//a"),
    ("name", "../cache"), ("max_bytes", True), ("max_bytes", 0),
    ("lifetime", "persistent"), ("unknown", "field"),
])
def test_path_reader_refuses_ambiguous_declaration(scratch, field, value):
    declaration = _bind(scratch)
    declaration[field] = value
    with pytest.raises(client.LocalScratchError):
        client.ephemeral_scratch_path(declaration)


def test_complete_successor_envelope_cannot_use_old_snapshot(scratch):
    live = copy.deepcopy(scratch["claim"])
    nonce = "2" * 32
    scope = "prismabuild-job" + hashlib.sha256(
        (live["action_key"] + nonce).encode()).hexdigest()[:32] + ".slice"
    live["resource_scope"].update(nonce=nonce, scope_id=scope)
    scratch["claim_path"].write_text(json.dumps(live))
    scratch["env"].update(PRISMABUILD_ACTION_NONCE=nonce, PRISMABUILD_ACTION_SCOPE=scope)
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)


@pytest.mark.parametrize("field", ["TEMP_ROOT", "TEMP_MAX", "demand"])
def test_changed_request_cannot_redirect_root_or_reservation(scratch, field):
    key = scratch["claim"]["action_key"]
    request_path = scratch["cas"].root / "requests" / key[:2] / f"{key}.json"
    action = json.loads(request_path.read_text())
    if field == "demand":
        action["params"]["demand"]["spool_gb"] = 1
    else:
        action["environment"]["variables"][field] = (
            str(scratch["cache"]) if field == "TEMP_ROOT" else "99999")
    request_path.chmod(0o600)  # fixture-only corruption of an immutable request
    request_path.write_text(json.dumps(action))
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)


@pytest.mark.parametrize("resources", [{}, {"spool_gb": 1}, {"spool_gb": True}])
def test_claim_cannot_supply_missing_or_changed_scratch_demand(scratch, resources):
    scratch["claim"]["resources"] = resources
    scratch["claim_path"].write_text(json.dumps(scratch["claim"]))
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)


@pytest.mark.parametrize("field,value", [
    ("published_unix", None), ("published_unix", True),
    ("published_unix", float("inf")), ("attempts", None), ("attempts", True),
    ("attempts", -1), ("claimed_host", ""), ("claimed_by", ""),
    ("claimed_unix", None), ("claimed_unix", True),
])
def test_incomplete_claim_does_not_default_to_identity(scratch, field, value):
    scratch["claim"][field] = value
    scratch["claim_path"].write_text(json.dumps(scratch["claim"]))
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)


def test_no_full_lifetime_capability_is_advertised():
    assert "scratch-lifetime-v1" not in client.CAPABILITIES


def test_namespace_uses_existing_submission_generation_recipe(scratch):
    declaration = _bind(scratch)
    expected = (scratch["root"] / "prismabuild-ephemeral"
                / pb.canonical_sha256({"host": scratch["claim"]["claimed_host"]})
                / scratch["claim"]["action_key"]
                / scratch["queue"].attempt_generation(scratch["claim"])
                / scratch["claim"]["resource_scope"]["nonce"] / "row-temp")
    assert client.ephemeral_scratch_path(declaration) == expected


def test_binding_reads_default_launch_environment(scratch, monkeypatch):
    for field, value in scratch["env"].items():
        monkeypatch.setenv(field, value)
    declaration = client.bind_ephemeral_scratch(
        scratch["queue"], root_env="TEMP_ROOT", name="row-temp",
        claim_snapshot=scratch["claim"])
    assert declaration == _bind(scratch)


@pytest.mark.parametrize("count", [1, 2])
def test_contradictory_committed_hosts_refuse_without_mutation(scratch, count):
    for index in range(count):
        held = (scratch["queue"].root / pool.RESERVATIONS / f"foreign-host-{index}"
                / "held" / scratch["claim"]["action_key"])
        held.mkdir(parents=True)
    original = scratch["claim_path"].read_bytes()
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)
    assert scratch["claim_path"].read_bytes() == original
    assert not scratch["root"].exists()
    for index in range(count):
        assert (scratch["queue"].root / pool.RESERVATIONS / f"foreign-host-{index}"
                / "held" / scratch["claim"]["action_key"]).is_dir()


def test_successor_observed_during_request_read_refuses(scratch, monkeypatch):
    read = pool._sealed_action_request

    def replace_after_read(cas_root, key):
        action = read(cas_root, key)
        successor = copy.deepcopy(scratch["claim"])
        successor["published_unix"] += 1
        scratch["claim_path"].write_text(json.dumps(successor))
        return action

    monkeypatch.setattr(pool, "_sealed_action_request", replace_after_read)
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)
    assert json.loads(scratch["claim_path"].read_text())["published_unix"] == 101.0
    assert not scratch["root"].exists()


@pytest.mark.parametrize("field", ["schema", "lifetime", "root_env", "max_env", "root",
                                  "max_bytes", "name", "owner_action_key",
                                  "owner_published_unix", "owner_host", "owner_attempt"])
def test_path_reader_requires_every_declared_field(scratch, field):
    declaration = _bind(scratch)
    del declaration[field]
    with pytest.raises(client.LocalScratchError):
        client.ephemeral_scratch_path(declaration)


@pytest.mark.parametrize("host", [".", "..", "/foreign", "host:123", "host name"])
def test_path_reader_refuses_malformed_host_identity(scratch, host):
    declaration = _bind(scratch)
    declaration["owner_host"] = host
    with pytest.raises(client.LocalScratchError):
        client.ephemeral_scratch_path(declaration)


def test_valid_sealed_underfunded_demand_refuses(scratch):
    body = copy.deepcopy(scratch["action"])
    del body["action_key"]
    body["params"]["demand"]["spool_gb"] = 1
    action = pb.seal_action(body)
    scratch["cas"].publish_action_request(action)
    key = str(action["action_key"])
    nonce = scratch["claim"]["resource_scope"]["nonce"]
    scope = "prismabuild-job" + hashlib.sha256((key + nonce).encode()).hexdigest()[:32] + ".slice"
    scratch["claim"].update(action_key=key, resources={"spool_gb": 1})
    scratch["claim"]["resource_scope"].update(action_key=key, scope_id=scope)
    scratch["env"].update(PRISMABUILD_ACTION_KEY=key, PRISMABUILD_ACTION_SCOPE=scope)
    scratch["claim_path"] = scratch["queue"].item_path(pool.CLAIMED, key)
    scratch["claim_path"].write_text(json.dumps(scratch["claim"]))
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)


def test_sealed_request_symlink_is_not_authority(scratch):
    key = scratch["claim"]["action_key"]
    path = scratch["cas"].root / "requests" / key[:2] / f"{key}.json"
    target = path.with_name("fixture-request-target.json")
    original = path.read_bytes()
    path.rename(target)
    path.symlink_to(target)
    with pytest.raises(client.LocalScratchError):
        _bind(scratch)
    assert path.is_symlink()
    assert target.read_bytes() == original
    assert not scratch["root"].exists()


def test_child_names_share_one_pair_ceiling_without_new_reservations(scratch):
    reservations = scratch["queue"].root / pool.RESERVATIONS
    assert not reservations.exists()
    original = scratch["claim_path"].read_bytes()
    first = _bind(scratch)
    second = client.bind_ephemeral_scratch(
        scratch["queue"], root_env="TEMP_ROOT", name="second-temp",
        claim_snapshot=scratch["claim"], env=scratch["env"])
    assert first["max_bytes"] == second["max_bytes"] == 1024
    assert client.ephemeral_scratch_path(first) != client.ephemeral_scratch_path(second)
    assert scratch["claim_path"].read_bytes() == original
    # A pure metadata binding does not even create a reservations directory.
    assert not reservations.exists()
    assert not scratch["root"].exists()


def test_derivation_has_no_symlink_or_filesystem_authority(scratch):
    declaration = _bind(scratch)
    expected = client.ephemeral_scratch_path(declaration)
    scratch["cache"].mkdir()
    marker = scratch["cache"] / "cache.bin"
    marker.write_bytes(b"persistent")
    scratch["root"].symlink_to(scratch["cache"], target_is_directory=True)
    # Derivation deliberately checks no filesystem object. This path must not
    # be used as proof of safe creation/deletion; the persistent cache survives.
    assert client.ephemeral_scratch_path(declaration) == expected
    assert _bind(scratch) == declaration
    assert scratch["root"].is_symlink()
    assert marker.read_bytes() == b"persistent"
    assert list(scratch["cache"].iterdir()) == [marker]
