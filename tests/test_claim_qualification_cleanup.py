"""Qualification teardown must retain the production cleanup audit."""
from contextlib import ExitStack
import json
from pathlib import Path
import sys
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
