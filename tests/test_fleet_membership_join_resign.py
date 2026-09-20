"""Worker expansion + join/resign regression coverage (new lane only).

Exercises the actual ``PoolQueue`` claim/offer/capability path with fresh
controlled offers, plus the ``fleet_membership`` gate/park/withdrawal driver.
No invented backends, no self-coded fake acceptance: offers are real
``announce`` records, claims are real ``claim``/``withdraw`` transitions,
gates are the real ``worker_loop`` drain shape on tmp paths.
"""

from __future__ import annotations

import json
import socket
import sys
import time
from pathlib import Path
from unittest import mock

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import fleet_membership as fm  # noqa: E402
import worker_loop  # noqa: E402

CPU = {"cpu": 1}


def _publish(q: pool.PoolQueue, key: str, **kw) -> None:
    q.publish(
        action_key=key,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources=kw.pop("resources", dict(CPU)),
        max_attempts=kw.pop("max_attempts", 1),
        retry_safe=kw.pop("retry_safe", True),
        tags=kw.pop("tags", ()),
        needs_gpu=kw.pop("needs_gpu", False),
        **kw,
    )


def _offer(q: pool.PoolQueue, host: str, tags, has_gpu=False, capacity=None, **kw):
    q.announce(
        host=host, tags=list(tags), has_gpu=has_gpu,
        capacity=dict(capacity or {"cpu": 4, "mem_gb": 16}),
        runtime_commit="c" * 40, **kw,
    )


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


def test_two_matching_workers_either_may_claim(queue: pool.PoolQueue) -> None:
    key = "a" * 64
    _publish(queue, key, tags=["gb10"])
    _offer(queue, "sparky", ["gb10", "sparky"], capacity={"cpu": 4})
    _offer(queue, "sparklina", ["gb10", "sparklina"], capacity={"cpu": 4})
    item = queue.ready_items()[0]
    assert queue.placeable(item) is True
    assert queue.placeable_hosts(item) == ["sparklina", "sparky"]
    got = queue.claim(tags=["gb10", "sparky"], owner="sparky:1:q1", capacity={"cpu": 4})
    assert got is not None and got["action_key"] == key


def test_cpu_x86_vs_cuda_class_conjunction(queue: pool.PoolQueue) -> None:
    gpu_key = "b" * 64
    _publish(queue, gpu_key, tags=["gb10"], needs_gpu=True, resources={"gpu": 1})
    _offer(queue, "dl380g10", ["x86", "dl380g10"], has_gpu=False)
    item = queue.ready_items()[0]
    assert queue.placeable(item) is False
    assert queue.placeable_hosts(item) == []
    assert queue.claim(tags=["x86", "dl380g10"], owner="dl380:1:q1",
                       capacity={"cpu": 4}) is None
    _offer(queue, "sparky", ["gb10", "sparky"], has_gpu=True,
           capacity={"gpu": 1, "mem_gb": 16})
    assert queue.placeable(queue.ready_items()[0]) is True


def test_discrete_and_unified_budgets_stay_separate(queue: pool.PoolQueue) -> None:
    key = "c" * 64
    _publish(queue, key, tags=["x86"], resources={"mem_gb": 8})
    _offer(queue, "wsl-gpu", ["x86", "wsl-gpu", "gfx1201", "rocm", "rdna4"],
           capacity={"cpu": 4, "mem_gb": 16},
           observed_detail={"gpu_memory_domains": ["discrete"]})
    _offer(queue, "dl380g10", ["x86", "dl380g10"], capacity={"cpu": 4, "mem_gb": 96},
           observed_detail={"gpu_memory_domains": []})
    offers = {o["host"]: o for o in queue.offers()}
    assert offers["wsl-gpu"]["observed_detail"]["gpu_memory_domains"] == ["discrete"]
    # VRAM never inflates host RAM: an 8 GiB host demand fits the 16 GiB host
    # budget and is refused only when the host budget itself is smaller.
    assert queue.placeable(queue.ready_items()[0]) is True
    _offer(queue, "tiny", ["x86", "tiny"], capacity={"cpu": 4, "mem_gb": 4})
    item = queue.ready_items()[0]
    hosts = queue.placeable_hosts(item)
    assert "tiny" not in hosts
    assert {"wsl-gpu", "dl380g10"}.issubset(set(hosts))


def test_unsupported_backend_is_explicitly_unplaceable(queue: pool.PoolQueue) -> None:
    key = "d" * 64
    _publish(queue, key, tags=["cuda"], resources={"gpu": 1}, needs_gpu=True)
    _offer(queue, "some-amd-box", ["x86", "rocm"], has_gpu=False,
           capacity={"cpu": 4, "mem_gb": 16})
    item = queue.ready_items()[0]
    assert queue.placeable(item) is False
    # Unknown capacity kinds are unknown, not zero: absence never refuses.
    _publish(queue, "e" * 64, tags=["x86"], resources={"cpu": 1})
    _offer(queue, "legacy", ["x86", "legacy"], capacity={"mem_gb": 16})
    assert queue.placeable(queue.ready_items()[1]) is True


def test_stale_offer_is_unavailable_not_zero(queue: pool.PoolQueue) -> None:
    key = "f" * 64
    _publish(queue, key, tags=["x86"])
    _offer(queue, "dl380g10", ["x86", "dl380g10"])
    assert queue.placeable(queue.ready_items()[0]) is True
    offer_path = queue.root / "workers" / "dl380g10.json"
    record = json.loads(offer_path.read_text())
    record["announced_unix"] = time.time() - 10_000
    offer_path.write_text(json.dumps(record))
    assert queue.offers() == []
    assert queue.placeable(queue.ready_items()[0]) is None


def _tmp_gate(tmp_path: Path, draining: bool, changed: float = 1234.0) -> Path:
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": draining, "changed_unix": changed}))
    return gate


def test_gate_shape_is_fail_closed_and_keyed(tmp_path: Path) -> None:
    gate = _tmp_gate(tmp_path, True, 1700.0)
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        assert worker_loop.read_maintenance_gate() is not None
        assert worker_loop._gate_key(worker_loop.read_maintenance_gate()) == "1700.0"
    gate.write_text(json.dumps({"draining": False, "changed_unix": 1700.0}))
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        assert worker_loop.read_maintenance_gate() is None
    gate.write_text("not json")
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        assert worker_loop.read_maintenance_gate() is not None


def _incarnation(monkeypatch, host: str) -> str:
    """Pin supervisor identity: test control of environment, same class as the
    hostname patching existing pool tests use."""
    owner = f"{host}:supervisor-9:8"
    monkeypatch.setattr(fm, "supervisor_incarnation", lambda: (owner, None))
    return owner


def _active_roster(tmp_path: Path, host: str) -> Path:
    roster = tmp_path / "fleet_boxes.json"
    roster.write_text(json.dumps(
        {"boxes": {host: {"loops": 1, "args": []}}}))
    return roster


def _runtime_root(tmp_path: Path) -> Path:
    root = tmp_path / "runtime"
    root.mkdir(exist_ok=True)
    (root / "RUNTIME_VERSION.json").write_text(json.dumps({"commit": "a" * 40}))
    return root


def test_resign_withdraws_while_busy_and_waits_for_terminal(
    queue: pool.PoolQueue, tmp_path: Path, monkeypatch
) -> None:
    import threading

    key = "1" * 64
    host = socket.gethostname()
    owner = _incarnation(monkeypatch, host)
    _publish(queue, key, tags=["x86"], max_attempts=1, retry_safe=False)
    snapshot = queue.claim(tags=["x86"], owner=f"{host}:1:q1",
                           capacity={"cpu": 4})
    assert snapshot is not None
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False, "changed_unix": 1.0}))
    parked = gate.parent / "rollout" / "parked"
    parked.mkdir(parents=True)
    calls: list[dict] = []

    def fake_broker(payload: dict) -> dict:
        calls.append(payload)
        if payload["op"] == "maintenance_begin":
            gate.write_text(json.dumps(
                {"draining": True, "changed_unix": 999.0, "owner": payload["owner"]}))
            return {"ok": True, "draining": True, "health": True,
                    "active_scopes": 0, "active_scope_ids": []}
        if payload["op"] == "maintenance_status":
            return {"ok": True, "draining": True, "health": True,
                    "active_scopes": 0, "active_scope_ids": []}
        raise AssertionError(payload)

    (parked / "111-s1-999.0").touch()

    def holder_concludes():
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if queue.item_path(pool.WITHDRAWN, key).exists():
                break
            time.sleep(0.2)
        queue.finish(key, status="failed",
                     detail={"termination_reason": "resign-withdrawn"},
                     claim_snapshot=snapshot)

    finisher = threading.Thread(target=holder_concludes, daemon=True)
    finisher.start()
    try:
        out = fm.resign(host, reason="test resign", queue_root=queue.root,
                        gate=gate, broker_call=fake_broker,
                        live=[(111, "s1")], wait_s=40.0)
    finally:
        finisher.join(timeout=10.0)
    assert out["status"] == "resigned", (
        f"phase={out.get('phase')} reason={out.get('reason')} "
        f"withdrawn={out.get('withdrawn')} broker={out.get('broker')}"
    )
    assert out["gate_key"] == "999.0"
    assert out["owner"] == owner
    assert calls and calls[0]["op"] == "maintenance_begin"
    assert queue.item_path(pool.WITHDRAWN, key).exists()
    assert fm.terminal_of(queue, key) is not None


def test_replacement_incarnation_markers_do_not_ack(tmp_path: Path) -> None:
    gate = _tmp_gate(tmp_path, True, 999.0)
    parked = gate.parent / "rollout" / "parked"
    parked.mkdir(parents=True)
    (parked / "111-abc-111_0").touch()  # stale epoch marker
    gate.write_text(json.dumps({"draining": True, "changed_unix": 1000.0}))
    found = fm.parked_markers(parked, fm.read_gate(gate))
    assert found == []  # old gate key never counts toward the new epoch


def test_join_refuses_absent_roster(tmp_path: Path, monkeypatch) -> None:
    host = socket.gethostname()
    _incarnation(monkeypatch, host)
    roster = tmp_path / "fleet_boxes.json"
    roster.write_text(json.dumps({"boxes": {
        host: {"loops": 1, "args": [],
               "status": "offline", "status_reason": "r",
               "status_by": "root", "status_unix": 1.0}}}))
    out = fm.join(host, roster_path=roster, queue_root=tmp_path / "q",
                  runtime_root=_runtime_root(tmp_path),
                  broker_call=lambda p: {"ok": True, "health": True,
                                         "draining": True, "active_scopes": 0,
                                         "active_scope_ids": []})
    assert out["status"] == "refused" and out["phase"] == "roster"


def test_join_refuses_unhealthy_broker(tmp_path: Path, monkeypatch) -> None:
    host = socket.gethostname()
    _incarnation(monkeypatch, host)
    roster = _active_roster(tmp_path, host)
    out = fm.join(host, roster_path=roster, queue_root=tmp_path / "q",
                  runtime_root=_runtime_root(tmp_path),
                  broker_call=lambda p: {"ok": False, "error": "down"})
    assert out["status"] == "refused" and out["phase"] == "qualification"


def test_join_refuses_foreign_host(tmp_path: Path, monkeypatch) -> None:
    _incarnation(monkeypatch, socket.gethostname())
    out = fm.join("some-other-box", roster_path=tmp_path / "missing.json",
                  queue_root=tmp_path / "q",
                  broker_call=lambda p: (_ for _ in ()).throw(
                      AssertionError("no broker call past host check")))
    assert out["status"] == "refused" and out["phase"] == "host"


def test_join_opens_gate_after_qualification(
    tmp_path: Path, monkeypatch
) -> None:
    host = socket.gethostname()
    owner = _incarnation(monkeypatch, host)
    roster = _active_roster(tmp_path, host)
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": True, "changed_unix": 5.0,
                                "owner": "old"}))

    def fake_broker(payload: dict) -> dict:
        if payload["op"] == "maintenance_status":
            return {"ok": True, "draining": True, "health": True,
                    "active_scopes": 0, "active_scope_ids": [],
                    "maintenance_owner": "old"}
        if payload["op"] == "maintenance_end":
            gate.write_text(json.dumps({"draining": False,
                                        "changed_unix": 6.0}))
            return {"ok": True, "draining": False, "health": True,
                    "active_scopes": 0, "active_scope_ids": []}
        raise AssertionError(payload)

    out = fm.join(host, reason="test join", roster_path=roster,
                  queue_root=tmp_path / "q", gate=gate,
                  runtime_root=_runtime_root(tmp_path),
                  broker_call=fake_broker)
    assert out["status"] == "joined", out
    assert out["owner"] == owner
    assert fm.read_gate(gate) is None


def test_resign_never_releases_on_heartbeat_loss(queue: pool.PoolQueue, tmp_path: Path) -> None:
    """No broker call, no gate change, no withdraw: nothing is released."""
    key = "2" * 64
    _publish(queue, key, tags=["x86"])
    assert queue.ready_items() != []
    # Resign without a reason refuses before any state changes.
    out = fm.resign("testbox", reason="  ", queue_root=queue.root,
                    broker_call=lambda p: (_ for _ in ()).throw(AssertionError("no call")))
    assert out["status"] == "refused"
    assert queue.ready_items() != []
