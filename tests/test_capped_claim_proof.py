"""A final fence proof belongs to one real claim transition."""
import copy
import os
import pickle
from pathlib import Path
import socket
from types import SimpleNamespace

import pytest

from prismabuild import fleet_roster, host_cap, pool
from test_capped_partial_host import capped


@pytest.mark.parametrize("route", ["claim", "serve_once", "_claim", "_claim_pass", "_start_claim_scan"])
@pytest.mark.parametrize("fence", [None, True, lambda: True])
def test_every_route_requires_a_registered_fence_before_io(tmp_path, route, fence):
    queue = pool.PoolQueue(tmp_path / "uncreated")
    capacity = host_cap.enforce_host_cap({"cpu": 2, "mem_gb": 4}, None, {})
    with pytest.raises(TypeError, match="registered claim fence"):
        getattr(queue, route)(capacity=capacity, admission_open=fence)
    assert not queue.root.exists()


@pytest.mark.parametrize("kind", [host_cap.ClaimFence, host_cap.ClaimFenceProof])
def test_fence_capabilities_refuse_forgery_and_duplication(tmp_path, kind):
    queue = pool.PoolQueue(tmp_path / "queue")
    capacity = host_cap.enforce_host_cap({"cpu": 2}, None, {})
    fence = host_cap.prepare_claim_fence(queue, capacity)
    scan = SimpleNamespace(host=socket.gethostname(), owner="worker-1")
    value = fence if kind is host_cap.ClaimFence else host_cap.check_claim_fence(fence, capacity, queue, scan, "a")
    with pytest.raises(TypeError):
        kind()
    with pytest.raises(TypeError):
        type("ForgedFence", (kind,), {})
    for operation in (copy.copy, copy.deepcopy, pickle.dumps):
        with pytest.raises(TypeError):
            operation(value)
    forged = object.__new__(kind)
    with pytest.raises(TypeError):
        if kind is host_cap.ClaimFence:
            host_cap.require_claim_fence(forged, capacity, queue)
        else:
            host_cap.consume_claim_fence(forged, capacity, queue, scan, "a")


@pytest.mark.parametrize("fault", ["queue", "capacity", "host", "worker"])
def test_entry_binding_cannot_move_to_another_queue_or_worker(tmp_path, monkeypatch, fault):
    queue = pool.PoolQueue(tmp_path / "queue")
    capacity = host_cap.enforce_host_cap({"cpu": 2}, None, {})
    fence = host_cap.prepare_claim_fence(queue, capacity)
    if fault == "queue":
        queue = pool.PoolQueue(queue.root)
    elif fault == "capacity":
        capacity = host_cap.enforce_host_cap(dict(capacity), None, {})
    elif fault == "host":
        monkeypatch.setattr(host_cap.socket, "gethostname", lambda: "another-host")
    else:
        pid = host_cap.os.getpid()
        monkeypatch.setattr(host_cap.os, "getpid", lambda: pid + 1)
    with pytest.raises(TypeError, match="another queue, capacity or worker"):
        host_cap.require_claim_fence(fence, capacity, queue)


@pytest.mark.parametrize("fault", ["queue", "capacity", "host", "worker", "pass", "owner", "key", "replay"])
def test_final_proof_is_bound_and_consumed_once(tmp_path, monkeypatch, fault):
    queue = pool.PoolQueue(tmp_path / "queue")
    capacity = host_cap.enforce_host_cap({"cpu": 2}, None, {})
    fence = host_cap.prepare_claim_fence(queue, capacity)
    scan = SimpleNamespace(host=socket.gethostname(), owner="worker-1")
    proof = host_cap.check_claim_fence(fence, capacity, queue, scan, "a")
    original = (capacity, queue, scan, "a")
    key = "a"
    if fault == "queue":
        queue = pool.PoolQueue(queue.root)
    elif fault == "capacity":
        capacity = host_cap.enforce_host_cap(dict(capacity), None, {})
    elif fault == "host":
        monkeypatch.setattr(host_cap.socket, "gethostname", lambda: "another-host")
    elif fault == "worker":
        pid = host_cap.os.getpid()
        monkeypatch.setattr(host_cap.os, "getpid", lambda: pid + 1)
    elif fault == "pass":
        scan = SimpleNamespace(host=scan.host, owner=scan.owner)
    elif fault == "owner":
        scan.owner = "worker-2"
    elif fault == "key":
        key = "b"
    else:
        host_cap.consume_claim_fence(proof, *original)
    with pytest.raises(TypeError):
        host_cap.consume_claim_fence(proof, capacity, queue, scan, key)
    # A failed consume cannot be retried with the original binding either.
    with pytest.raises(TypeError):
        host_cap.consume_claim_fence(proof, *original)


def test_the_rename_primitive_refuses_a_missing_final_proof(tmp_path):
    queue = pool.PoolQueue(tmp_path / "uncreated")
    capacity = host_cap.enforce_host_cap({"cpu": 2}, None, {})
    scan = SimpleNamespace(capacity=capacity, host=socket.gethostname())
    tiers = SimpleNamespace(host=SimpleNamespace(candidate=SimpleNamespace(key="a")))
    with pytest.raises(TypeError, match="registered claim fence"):
        queue._rename_fenced_claim(tiers, scan, None)
    assert not queue.root.exists()


def test_final_check_uses_the_original_roster_observation(capped, tmp_path, monkeypatch):
    worker, _, _, membership = capped
    queue = pool.PoolQueue(tmp_path / "queue")
    parameters = worker.capped_simulation_parameters(queue, {"cpu": 8, "mem_gb": 32})
    # Losing the roster after sampling cannot turn this observation into an
    # uncapped grant. The original Box and held source are retained privately.
    monkeypatch.setattr(fleet_roster.Roster, "for_host", lambda *a, **k: None)
    membership.write_text("0::/user.slice/worker\n")
    scan = SimpleNamespace(host=socket.gethostname(), owner="worker-1")
    assert host_cap.check_claim_fence(parameters["admission_open"], parameters["capacity"], queue, scan, "a") is None


def test_an_installed_cap_with_a_missing_roster_refuses(monkeypatch):
    monkeypatch.setattr(host_cap, "_installed_cap_present", lambda: True)
    detail = {}
    capacity = host_cap.enforce_host_cap({"cpu": 80, "mem_gb": 214}, None, {}, detail=detail)
    assert capacity == {"cpu": 0, "mem_gb": 0}
    assert detail["host_cap_refusal"] == "installed host cap has no readable roster cap"


def test_the_installed_cap_marker_is_an_isolated_real_file():
    marker = host_cap.HOST_CAP_CONFIG
    assert marker != Path("/etc/prismabuild/host-cap.conf")
    assert not marker.exists()
    assert host_cap.enforce_host_cap({"cpu": 8}, None, {}) == {"cpu": 8}
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("[Service]\nSlice=prismabuild.slice\n")
    detail = {}
    assert host_cap.enforce_host_cap({"cpu": 8}, None, {}, detail=detail) == {"cpu": 0}
    assert "no readable roster cap" in detail["host_cap_refusal"]


@pytest.mark.parametrize("fault", ["eacces", "enotdir"])
def test_an_unreadable_marker_path_fails_closed(tmp_path, monkeypatch, fault):
    if fault == "eacces":
        locked = tmp_path / "locked"
        locked.mkdir()
        marker = locked / "host-cap.conf"
        locked.chmod(0)
    else:
        blocker = tmp_path / "blocker"
        blocker.write_text("")
        marker = blocker / "host-cap.conf"
    monkeypatch.setattr(host_cap, "HOST_CAP_CONFIG", marker)
    try:
        if fault == "eacces" and os.geteuid() == 0:
            pytest.skip("root bypasses directory permissions")
        detail = {}
        capacity = host_cap.enforce_host_cap({"cpu": 8, "mem_gb": 32}, None, {}, detail=detail)
    finally:
        if fault == "eacces":
            locked.chmod(0o755)
    assert capacity == {"cpu": 0, "mem_gb": 0}
    assert detail["host_cap_refusal"] == "installed host cap has no readable roster cap"


def test_a_registered_fence_is_not_a_capped_capacity(tmp_path):
    queue = pool.PoolQueue(tmp_path / "uncreated")
    capacity = host_cap.enforce_host_cap({"cpu": 2}, None, {})
    fence = host_cap.prepare_claim_fence(queue, capacity)
    with pytest.raises(TypeError, match="capacity from enforce_host_cap"):
        host_cap.require_capped_capacity(fence)
    assert not queue.root.exists()
