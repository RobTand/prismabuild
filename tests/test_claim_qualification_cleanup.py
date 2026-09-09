"""Qualification teardown must retain the production cleanup audit."""
from contextlib import ExitStack
import json
from pathlib import Path
import sys
import subprocess
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import qualify_claim_recovery as qualification
from prismabuild.resource_scope import ResourceScope


@pytest.fixture
def registered_cleanup(tmp_path, monkeypatch):
    scopes = []
    calls = []

    def create(scope):
        scope.cgroup_path = tmp_path / "scope"
        scope.cgroup_path.mkdir()
        scopes.append(scope)

    def request(scope, operation, **extra):
        calls.append((operation, extra))
        if operation == "release" and scope.cgroup_path.exists():
            scope.cgroup_path.rmdir()
        return {"ok": True, "released": not scope.cgroup_path.exists()}

    class SetupInterrupted(Exception):
        pass

    def interrupt_setup(*args, **kwargs):
        raise SetupInterrupted

    monkeypatch.setattr(ResourceScope, "create", create)
    monkeypatch.setattr(ResourceScope, "control_record", lambda scope: {})
    monkeypatch.setattr(ResourceScope, "_request", request)
    queue = SimpleNamespace(
        ledger=lambda: SimpleNamespace(base=tmp_path),
        item_path=lambda *args: tmp_path / "claim.json",
        write_lease=interrupt_setup,
    )
    # Register the real callback, stopping before a payload is launched. Only
    # the broker is simulated; terminate_owned writes its actual audit file.
    stack = ExitStack()
    with pytest.raises(SetupInterrupted):
        qualification.start_scope(queue, {"action_key": "a" * 64,
                                         "claimed_by": "qualification"}, stack)
    yield scopes[0], stack, calls
    if scopes[0].cgroup_path.exists():
        stack.close()


@pytest.mark.parametrize("reason", ["lease_lost", "executed"])
def test_teardown_preserves_completed_production_cleanup(registered_cleanup, reason):
    scope, stack, calls = registered_cleanup
    scope.terminate_owned(reason)
    scope.release()
    audit = scope.telemetry_path.with_suffix(".termination.json")
    before = audit.read_bytes()
    calls.clear()

    stack.close()

    assert audit.read_bytes() == before
    assert not any(operation == "stop" for operation, _ in calls)


def test_teardown_still_stops_and_releases_unrecovered_scope(registered_cleanup):
    scope, stack, calls = registered_cleanup
    stack.close()
    assert [operation for operation, _ in calls] == ["stop", "release"]
    assert not scope.cgroup_path.exists()
    audit = json.loads(scope.telemetry_path.with_suffix(".termination.json").read_text())
    assert audit["reason"] == "disposable claim qualification cleanup"


@pytest.mark.parametrize("wrong", ["scope", "action", "parent", "running"])
def test_docker_teardown_refuses_unowned_or_running_container(monkeypatch, wrong):
    scope = SimpleNamespace(unit="prismabuild-job" + "b" * 32 + ".slice", action_key="a" * 64)
    row = {"Id": "c" * 64, "Config": {"Labels": {
        "prismabuild.scope": scope.unit, "prismabuild.action": scope.action_key}},
        "HostConfig": {"CgroupParent": scope.unit}, "State": {"Running": False}}
    if wrong in ("scope", "action"):
        row["Config"]["Labels"]["prismabuild." + wrong] = "foreign"
    elif wrong == "parent":
        row["HostConfig"]["CgroupParent"] = "foreign"
    else:
        row["State"]["Running"] = True
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        if "ps" in argv:
            return SimpleNamespace(stdout=row["Id"] + "\n")
        assert "inspect" in argv, "must refuse before container removal"
        return SimpleNamespace(stdout=json.dumps([row]))

    monkeypatch.setattr(qualification.subprocess, "run", run)
    with pytest.raises(AssertionError):
        qualification.remove_stopped_scope_containers(scope)
    assert not any("rm" in argv for argv in calls)


def test_docker_daemon_error_does_not_prove_cleanup(monkeypatch):
    scope = SimpleNamespace(unit="prismabuild-job" + "b" * 32 + ".slice", action_key="a" * 64)

    def unavailable(argv, **kwargs):
        raise subprocess.CalledProcessError(1, argv, stderr="daemon unavailable")

    monkeypatch.setattr(qualification.subprocess, "run", unavailable)
    with pytest.raises(subprocess.CalledProcessError):
        qualification.remove_stopped_scope_containers(scope)
