"""An empty process census cannot certify an unknown physical drain epoch."""
import fcntl
import json
import os
import time
from types import SimpleNamespace

import pytest

import fleet_membership
import resource_broker
import supervise
from prismabuild import pool


class EmptyKernel:
    def healthy(self):
        return True

    def inventory(self):
        return {}


@pytest.mark.parametrize("fault", ["none", "missing-key", "missing-file", "malformed-file"])
def test_resign_requires_known_gate_even_with_a_proved_empty_census(
        tmp_path, monkeypatch, fault):
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"schema": resource_broker.MAINTENANCE_SCHEMA,
                                "draining": False}))
    authority = resource_broker.Authority(
        tmp_path / "broker-state", os.getuid(), EmptyKernel(),
        max_memory_bytes=1024**3,
        maintenance_state=tmp_path / "durable" / "maintenance.json")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    runtime = tmp_path / "private-runtime"
    runtime.mkdir()
    monkeypatch.setattr(supervise, "_proven_roots", lambda: [runtime])
    assert fleet_membership.live_loop_census() == ([], None)

    clock = SimpleNamespace(now=0.0, waits=[])

    def next_round(seconds):
        clock.waits.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(fleet_membership, "time", SimpleNamespace(
        monotonic=lambda: clock.now, sleep=next_round, time=time.time))
    operations = []
    closed = []

    def broker_call(request):
        operations.append(request["op"])
        reply = authority.admin(0, request)
        if request["op"] == "maintenance_begin":
            assert reply["draining"] is True
            assert reply["active_scopes"] == 0
            written = json.loads(gate.read_text())
            assert written["changed_unix"] > 0
            closed.append(dict(written))
            if fault == "missing-key":
                written.pop("changed_unix")
                replacement = gate.with_suffix(".damaged")
                replacement.write_text(json.dumps(written))
                replacement.replace(gate)
            elif fault == "missing-file":
                gate.unlink()
            elif fault == "malformed-file":
                gate.write_text("{not JSON")
        return reply

    observed = []
    census = fleet_membership.live_loop_census

    def observe_census(*args, **kwargs):
        result = census(*args, **kwargs)
        observed.append(result)
        return result

    monkeypatch.setattr(fleet_membership, "live_loop_census", observe_census)
    claim = tmp_path / "supervisor.claim"
    monkeypatch.setattr(supervise, "CLAIM", claim)
    with claim.open("w") as holder:
        fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
        holder.write(f"{os.getpid()}\n")
        holder.flush()
        owner, error = fleet_membership.supervisor_incarnation()
        assert error is None and owner is not None
        result = fleet_membership.resign(
            reason="prove the drain", queue_root=queue.root, gate=gate,
            broker_call=broker_call, wait_s=2.0)

    assert len(closed) == 1
    assert operations[0] == "maintenance_begin"
    assert "maintenance_end" not in operations
    durable = json.loads(authority.maintenance_state_path.read_text())
    assert durable["draining"] is True
    assert durable["changed_unix"] > 0
    assert durable["owner"] == owner
    assert authority.admin(0, {"op": "maintenance_status"})["active_scopes"] == 0
    if fault == "none":
        assert result["status"] == "resigned", result
        assert len(observed) >= 2 and all(row == ([], None) for row in observed)
    else:
        assert result["status"] == "resigning", {
            "result": result, "census": observed, "broker_operations": operations}
        assert result.get("reason"), result
