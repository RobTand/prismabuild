"""A partial host cannot turn host memory or a smoothed roof into cap credit."""
import importlib.util
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from prismabuild import box_capacity, fleet_roster, host_cap, pool


@pytest.fixture
def capped(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "capped_worker", Path(__file__).resolve().parents[1] / "tools/fleet/worker_loop.py")
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    entry = {"cap": {"cpus": "4-7,12-15", "mem_gb": 32, "slice": "prismabuild.slice"}}
    roster = fleet_roster.Roster.capture(json.dumps({"boxes": {"proof": entry}}).encode())
    box = roster.canonical_box("proof")
    monkeypatch.setattr(fleet_roster.Roster, "for_host", lambda *a, **k: box)
    root = tmp_path / "cgroups"
    parent = root / "prismabuild.slice"
    child = parent / "prismabuild-supervisor.service"
    child.mkdir(parents=True)
    for path in (parent, child):
        (path / "cpuset.cpus.effective").write_text("4-7,12-15")
    (parent / "memory.max").write_text(str(32 * host_cap.GIB))
    (parent / "memory.current").write_text(str(2 * host_cap.GIB))
    membership = tmp_path / "membership"
    membership.write_text("0::/prismabuild.slice/prismabuild-supervisor.service\n")
    monkeypatch.setattr(host_cap, "CGROUP_ROOT", root)
    monkeypatch.setattr(host_cap, "MEMBERSHIP", membership)
    monkeypatch.setattr(host_cap.os, "sched_getaffinity", lambda _: set(range(4, 8)) | set(range(12, 16)))
    monkeypatch.setattr(box_capacity, "mem_available_gb", lambda: 200)
    monkeypatch.setattr(box_capacity, "run_queue", lambda: 0)
    return worker, box, parent, membership


@pytest.mark.parametrize("idle", [False, True])
def test_every_capacity_producer_clamps_the_live_slice(capped, tmp_path, idle):
    worker, box, parent, _ = capped
    queue = pool.PoolQueue(tmp_path / "queue")
    args = worker.build_parser().parse_args(["--assume-idle"] if idle else [])
    observer = None if idle else box_capacity.CapacityObserver(
        samples=3, mem_roof=lambda: 214)
    _, capacity, _ = worker.observed_claim_capacity(
        queue, args=args, base_declared={"cpu": 80, "mem_gb": 96},
        observer=observer, gpu_sample=None, known_physical_gpus=0)
    assert capacity["cpu"] == 8
    assert capacity["mem_gb"] == 29
    (parent / "memory.current").write_text(str(20 * host_cap.GIB))
    _, capacity, _ = worker.observed_claim_capacity(
        queue, args=args, base_declared={"cpu": 80, "mem_gb": 96},
        observer=observer, gpu_sample=None, known_physical_gpus=0)
    assert capacity["mem_gb"] == 11


@pytest.mark.parametrize("fault", ["membership", "cpuset", "memory", "missing"])
def test_a_mismatched_cap_closes_all_capacity(capped, tmp_path, fault):
    worker, _, parent, membership = capped
    if fault == "membership":
        membership.write_text("0::/user.slice/prismabuild.slice/worker\n")
    elif fault == "cpuset":
        (parent / "cpuset.cpus.effective").write_text("0-15")
    elif fault == "memory":
        (parent / "memory.max").write_text("max")
    else:
        (parent / "memory.current").unlink()
    queue = pool.PoolQueue(tmp_path / "queue")
    args = worker.build_parser().parse_args(["--assume-idle"])
    _, capacity, _ = worker.observed_claim_capacity(
        queue, args=args, base_declared={"cpu": 8, "mem_gb": 32},
        observer=None, gpu_sample=None, known_physical_gpus=0)
    assert not any(capacity.values())


def test_reservations_are_credited_once_and_uncapped_hosts_do_not_read_cgroups(capped):
    _, box, parent, _ = capped
    (parent / "memory.current").write_text(str(20 * host_cap.GIB))
    held = iter(({"mem_gb": 16}, {}))
    assert host_cap.enforce_host_cap({"cpu": 8, "mem_gb": 32}, box,
                                     lambda: next(held))["mem_gb"] == 11
    (parent / "memory.current").unlink()
    assert host_cap.enforce_host_cap({"cpu": 80, "mem_gb": 214}, None, {}) == {
        "cpu": 80, "mem_gb": 214}


def test_slice_credit_includes_reservations_but_keeps_host_headroom(capped, monkeypatch):
    _, box, parent, _ = capped
    (parent / "memory.current").write_text(str(20 * host_cap.GIB))
    assert host_cap.enforce_host_cap({"cpu": 8, "mem_gb": 32}, box,
                                     {"mem_gb": 16})["mem_gb"] == 27
    monkeypatch.setattr(box_capacity, "mem_available_gb", lambda: 10)
    assert host_cap.enforce_host_cap({"cpu": 8, "mem_gb": 32}, box, {})["mem_gb"] == 2


def test_private_executable_admission_uses_the_same_cap(capped, tmp_path, monkeypatch):
    worker, _, _, membership = capped
    monkeypatch.setattr(worker.cpu_topology, "inherited_tiers", lambda: {
        "preferred": [4, 5, 6, 7], "fallback": [12, 13, 14, 15]})
    queue = pool.PoolQueue(tmp_path / "queue")
    parameters = worker.private_claim_parameters(queue)
    assert parameters["capacity"]["cpu"] == 8
    assert parameters["capacity"]["mem_gb"] == 29
    membership.write_text("0::/user.slice/worker\n")
    with pytest.raises(pool.PoolContractError, match="outside"):
        worker.private_claim_parameters(queue)


@pytest.mark.parametrize("field,bad", [("cpus", "7-4"), ("cpus", "4;5"),
                                      ("mem_gb", True), ("slice", "user.slice/escape")])
def test_publication_rejects_invalid_caps(capped, field, bad):
    _, box, _, _ = capped
    box.entry["cap"][field] = bad
    with pytest.raises(ValueError):
        box.roster.validate_publication()


def test_the_worker_publishes_the_refusal_and_does_not_serve(capped, tmp_path, monkeypatch):
    from test_worker_loop_offers_what_is_free import _run, _offer, BASE
    from prismabuild import census
    import socket

    _, _, _, membership = capped
    membership.write_text("0::/user.slice/worker\n")
    served = []
    monkeypatch.setattr(pool.PoolQueue, "serve_once", lambda *a, **k: served.append(True))
    # _run normally expects zero. A refused one-shot returns deferred.
    from prismabuild import role_session
    monkeypatch.setattr(role_session, "ROLE_DEFERRED_EXIT", 0)
    queue = _run(tmp_path, [*BASE, "--once", "--assume-idle"], gpu_sample=None)
    offer = _offer(queue, socket.gethostname())
    assert not any(offer["observed_capacity"].values())
    assert "outside" in offer["observed_detail"]["host_cap_refusal"]
    assert served == []
    nodes = census.offers(queue.root).value["nodes"]
    assert nodes[0]["state"] == "refused"
    assert "outside" in nodes[0]["reason"]


def test_system_unit_renderer_keeps_the_supervisor_and_broker_inside_the_cap(tmp_path, monkeypatch):
    import supervise
    source = Path(__file__).resolve().parents[1] / "tools/fleet/install_supervisor_unit.sh"
    rendered = subprocess.run(["bash", str(source), "--system", "--slice", "pbcap.slice",
                               "--cpus", "4-7,12-15", "--memory-gb", "32", "--print"],
                              capture_output=True, text=True, check=True).stdout
    assert "User=rob" in rendered
    assert rendered.count("Slice=pbcap.slice") == 4
    assert rendered.count("AllowedCPUs=4-7,12-15") == 5
    assert "MemoryMax=32G" in rendered and "MemorySwapMax=0" in rendered
    assert "Environment=PB_SCOPE_PARENT=pbcap.slice" in rendered
    assert "KillMode=process" in rendered and "TimeoutStopSec=infinity" in rendered
    system = tmp_path / "system.service"
    system.write_text(rendered)
    monkeypatch.setattr(supervise, "SYSTEMD_UNIT", tmp_path / "absent")
    monkeypatch.setattr(supervise, "SYSTEMD_SYSTEM_UNIT", system)
    assert supervise._systemd_managed()


def test_configured_broker_parent_preserves_the_attempt_identity(tmp_path, monkeypatch):
    roster = fleet_roster.Roster.capture(json.dumps({"boxes": {"proof": {
        "cap": {"cpus": "4-7,12-15", "mem_gb": 32, "slice": "pbcap.slice"}}}}).encode())
    monkeypatch.setattr(fleet_roster.Roster, "for_host", lambda *a, **k: roster.canonical_box("proof"))
    from test_resource_broker import module
    broker = module()
    from prismabuild import resource_scope

    scope = broker.scope_id("a" * 64, "b" * 32)
    backend = broker.SystemdBackend(tmp_path, scope_parent="pbcap.slice")
    path = backend.path(scope)
    assert path == tmp_path / "pbcap.slice" / scope.replace("prismabuild-", "pbcap-")
    calls = []
    backend.command = lambda *argv: calls.append(argv)
    backend.release(scope)
    assert calls == [("stop", path.name)]
    identity = SimpleNamespace(action_key="a" * 64, nonce="b" * 32,
                               gpu_memory_max_bytes=None, _explicit_gpu_budget=False)
    response = {"scope_id": scope, "token": "c" * 64,
                "cgroup_path": str(Path("/sys/fs/cgroup/pbcap.slice") / path.name)}
    resource_scope.ResourceScope._adopt_created_scope(identity, response)
    assert identity.unit == scope
    response["cgroup_path"] = str(Path("/sys/fs/cgroup/wrong.slice") / path.name)
    with pytest.raises(OSError, match="invalid scope"):
        resource_scope.ResourceScope._adopt_created_scope(identity, response)


def test_container_boundary_uses_the_configured_kernel_slice_and_canonical_label(tmp_path, monkeypatch):
    from test_docker_shim_relays_termination_1599 import _shim_module

    shim = _shim_module()
    canonical = "prismabuild-job" + "a" * 32 + ".slice"
    kernel = canonical.replace("prismabuild-", "pbcap-")
    cgroups = tmp_path / "cgroups"
    group = cgroups / "pbcap.slice" / kernel
    group.mkdir(parents=True)
    (group / "memory.max").write_text("4096")
    membership = tmp_path / "membership"
    membership.write_text("0::/pbcap.slice/" + kernel + "/payload\n")
    monkeypatch.setenv("PRISMABUILD_DOCKER_TESTING", "1")
    monkeypatch.setenv("PRISMABUILD_CGROUP_FILE", str(membership))
    monkeypatch.setenv("PRISMABUILD_CGROUP_ROOT", str(cgroups))
    scope = shim._resource_scope()
    assert scope == (kernel, 4096)
    row = {"Config": {"Labels": {shim.LABEL: "owner", shim.SHIM_LABEL: "nonce",
                                   shim.SCOPE_LABEL: canonical}},
           "HostConfig": {"CgroupParent": kernel}}
    assert shim._owned_by_this_attempt(row, "owner", scope, "nonce")
    row["HostConfig"]["CgroupParent"] = canonical
    assert not shim._owned_by_this_attempt(row, "owner", scope, "nonce")
