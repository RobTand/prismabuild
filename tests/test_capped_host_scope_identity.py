"""Independent #2141 checks. All execution is inside an admitted PB action."""
from types import SimpleNamespace

import pytest
from test_capped_partial_host import capped
from prismabuild import host_cap, resource_scope


@pytest.mark.parametrize("fault", ["larger_memory", "missing_memory", "wider_worker", "wrong_slice"])
def test_additional_live_mismatches_refuse(capped, fault):
    _, box, parent, membership = capped
    if fault == "larger_memory":
        (parent / "memory.max").write_text(str(64 * host_cap.GIB))
    elif fault == "missing_memory":
        (parent / "memory.max").unlink()
    elif fault == "wider_worker":
        (parent / "prismabuild-supervisor.service/cpuset.cpus.effective").write_text("0-15")
    else:
        membership.write_text("0::/wrong.slice/prismabuild-supervisor.service\n")
    detail = {}
    result = host_cap.enforce_host_cap({"cpu": 80, "mem_gb": 214}, box, {}, detail=detail)
    assert result == {"cpu": 0, "mem_gb": 0}
    assert detail["host_cap_refusal"]


@pytest.mark.parametrize("boundary", ["adopt", "create", "recover", "control"])
def test_a_valid_scope_in_the_wrong_slice_is_refused(capped, tmp_path, monkeypatch, boundary):
    from prismabuild.produced_output import _broker_scope_id
    _, box, _, _ = capped
    assert host_cap.enforce_host_cap({"cpu": 8, "mem_gb": 32}, box, {})["mem_gb"] == 29
    action, nonce = "a" * 64, "b" * 32
    unit = _broker_scope_id(action, nonce)
    response = {"scope_id": unit, "token": "c" * 64,
                "cgroup_path": "/sys/fs/cgroup/escape.slice/" + unit.replace("prismabuild-", "escape-")}
    scope = SimpleNamespace(action_key=action, nonce=nonce,
                            gpu_memory_max_bytes=None, _explicit_gpu_budget=False)
    if boundary == "adopt":
        with pytest.raises(OSError):
            resource_scope.ResourceScope._adopt_created_scope(scope, response)
    elif boundary == "control":
        control = {**response, "action_key": action, "nonce": nonce,
                   "socket_path": str(resource_scope.BROKER_SOCKET)}
        with pytest.raises(ValueError):
            resource_scope._checked_control_identity(action, control)
    else:
        actual = resource_scope.ResourceScope(action, nonce, 1024, tmp_path / "telemetry")
        monkeypatch.setattr(actual, "_request", lambda *a, **kw: response)
        with pytest.raises(OSError):
            getattr(actual, "create" if boundary == "create" else "recover_create")()
