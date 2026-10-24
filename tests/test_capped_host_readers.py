"""Remote scope evidence belongs to the claiming host's declared slice."""
import json

import pytest

from prismabuild import action_result, fleet_roster, pool, pool_reconcile, produced_output, resource_scope


@pytest.fixture
def remote_cap(monkeypatch):
    roster = fleet_roster.Roster.capture(json.dumps({"boxes": {
        "claiming": {"cap": {"cpus": "4-7,12-15", "mem_gb": 32, "slice": "pbcap.slice"}},
        "reader": {}}}).encode())
    asked = []

    def lookup(host, **kwargs):
        asked.append(host)
        return roster.canonical_box(host)

    monkeypatch.setattr(fleet_roster.Roster, "for_host", lookup)
    monkeypatch.setattr(resource_scope.socket, "gethostname", lambda: "reader")
    return asked


def _remote_scope(parent):
    key, nonce = "a" * 64, "b" * 32
    unit = produced_output._broker_scope_id(key, nonce)
    kernel = parent[:-6] + "-" + unit.removeprefix("prismabuild-")
    return key, {"action_key": key, "nonce": nonce, "scope_id": unit, "token": "c" * 64,
                 "cgroup_path": f"/sys/fs/cgroup/{parent}/{kernel}",
                 "socket_path": str(resource_scope.BROKER_SOCKET)}


@pytest.mark.parametrize("parent", ["pbcap.slice", "escape.slice", "prismabuild.slice"])
def test_control_reader_uses_the_claiming_host(remote_cap, parent):
    key, scope = _remote_scope(parent)
    if parent == "pbcap.slice":
        assert resource_scope._checked_control_identity(key, scope, host="claiming") == (scope["nonce"], scope["scope_id"])
    else:
        with pytest.raises(ValueError):
            resource_scope._checked_control_identity(key, scope, host="claiming")
    assert set(remote_cap) == {"claiming"}


@pytest.mark.parametrize("parent", ["pbcap.slice", "escape.slice", "prismabuild.slice"])
def test_reconciliation_binds_the_claiming_hosts_declared_parent(remote_cap, parent):
    # Adopted from the independent round-two reconciliation probe.
    key, scope = _remote_scope(parent)
    telemetry = {"complete": True, "errors": [], "action_key": key, "nonce": scope["nonce"],
                 "scope_unit": scope["scope_id"], "host": "claiming", "oom_local": 0, "oom_kill": 0}
    cleanup = {"complete": True, "nonce": scope["nonce"], "released": {"ok": True, "scope_id": scope["scope_id"]},
               "telemetry": telemetry, "checked_unix": 2.0}
    record = {"action_key": key, "claimed_host": "claiming", "finished_host": "claiming",
              "claimed_unix": 1.0, "finished_unix": 3.0,
              "resource_scope": scope, "resource_scope_cleanup": cleanup}
    if parent == "pbcap.slice":
        assert pool_reconcile._cleanup(record, {"resource_telemetry": telemetry}) is cleanup
    else:
        with pytest.raises(pool.PoolContractError, match="scope identity"):
            pool_reconcile._cleanup(record, {"resource_telemetry": telemetry})
    assert set(remote_cap) == {"claiming"}


def test_verified_result_passes_the_selected_claims_host(monkeypatch):
    key, scope = _remote_scope("pbcap.slice")
    identity = {"claimed_by": "worker-1", "claimed_host": "claiming"}
    record = {**identity, "resource_scope": scope}
    telemetry = {"complete": True, "nonce": scope["nonce"], "action_key": key,
                 "scope_unit": scope["scope_id"], "host": "claiming"}
    attempt = {**identity, "detail": {"resource_telemetry": telemetry}}
    observed = []

    class ReachedControl(Exception):
        pass

    def checked(key, control, *, host=None):
        observed.append(host)
        raise ReachedControl

    monkeypatch.setattr(resource_scope, "_checked_control_identity", checked)
    with pytest.raises(ReachedControl):
        action_result._native_producer_context(None, record, attempt, {"action_key": key}, {}, evidence_cap=4096)
    assert observed == ["claiming"]
