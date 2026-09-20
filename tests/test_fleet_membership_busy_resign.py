"""Busy-worker resign + retry-takeover + scope containment (RED-first lane).

All paths are real: a real ``PoolQueue`` (tmp root), the real broker
``Authority`` with the established stub backend (same pattern as
``tests/test_resource_broker.py``), real gate files through the real
``worker_loop`` gate readers, and real parked markers. The only injected
boundary is ``broker_call`` (the root-broker socket this uid cannot open in
a test), wired to ``Authority.handle`` directly.
"""

from __future__ import annotations

import json
import os
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
import resource_broker as broker_mod  # noqa: E402


class Backend:
    """Stub kernel backend: the sanctioned broker-test double (cf.
    tests/test_resource_broker.py). Groups are just rows; population is
    explicit so containment order is observable."""

    def __init__(self):
        self.groups: dict[str, dict] = {}

    def healthy(self):
        return True

    def inventory(self):
        return {name: {"populated": row["populated"], "frozen": row["frozen"],
                       "identity": row["identity"]} for name, row in self.groups.items()}

    def create(self, scope, budget):
        self.groups[scope] = {"populated": False, "frozen": False,
                              "identity": [7, len(self.groups) + 1]}
        return {"cgroup_identity": self.groups[scope]["identity"]}

    def run(self, scope, uid, command, stdio):
        self.groups[scope]["populated"] = True
        return object()

    def stop(self, scope):
        self.groups[scope]["frozen"] = True
        self.groups[scope]["populated"] = False

    def empty(self, scope):
        row = self.groups.get(scope)
        return row is None or not row["populated"]

    def reclaim(self, scope):
        return {"before": 0, "after": 0, "complete": True, "page_bytes_after": 0}

    def exists(self, scope):
        return scope in self.groups

    def observe(self, scope):
        row = self.groups.get(scope)
        if row is None:
            return None
        return {"populated": row["populated"], "frozen": row["frozen"],
                "identity": row["identity"]}

    def release(self, scope):
        self.groups.pop(scope, None)


@pytest.fixture()
def authority(tmp_path: Path):
    backend = Backend()
    state = tmp_path / "broker-state"
    auth = broker_mod.Authority(state, os.getuid(), backend,
                               max_memory_bytes=1024**3)
    return auth, backend


def _broker_call(authority):
    """Drive the real broker state machine the way the root daemon would.

    Maintenance ops require euid 0 through ``handle`` (``admin`` refuses
    non-root), so maintenance verbs go to ``admin(0, ...)`` — the same
    object, the same mutex, the same durable gate sync — while
    attempt-scoped ops go through ``handle`` as this uid.
    """
    auth, _ = authority
    _MAINT = {"maintenance_begin", "maintenance_status", "maintenance_end",
              "maintenance_force_end"}

    def call(payload: dict) -> dict:
        if payload.get("op") in _MAINT:
            return auth.admin(0, dict(payload))
        return auth.handle(os.getuid(), os.getpid(), dict(payload))

    return call


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


def _incarnation(monkeypatch) -> str:
    host = socket.gethostname()
    owner = f"{host}:supervisor-9:8"
    monkeypatch.setattr(fm, "supervisor_incarnation", lambda: (owner, None))
    return owner


def _publish_claim(queue: pool.PoolQueue, key: str, **kw) -> dict:
    queue.publish(
        action_key=key, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1}, max_attempts=kw.pop("max_attempts", 2),
        retry_safe=kw.pop("retry_safe", True), tags=["x86"],
    )
    host = socket.gethostname()
    got = queue.claim(tags=["x86"], owner=f"{host}:1:q1", capacity={"cpu": 4})
    assert got is not None
    return got


def test_busy_resign_requires_terminal_and_empty_scope(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """A claimed attempt with no terminal and a live scope is not resigned.

    A holder thread concludes the withdrawn attempt through the real
    ``finish`` path; resign must wait for that terminal (and empty scopes)
    before reporting success.
    """
    import threading

    key = "9" * 64
    _incarnation(monkeypatch)
    snapshot = _publish_claim(queue, key, max_attempts=1, retry_safe=False)
    auth, backend = authority
    gate = Path(auth.maintenance_path)
    parked = gate.parent / "rollout" / "parked"
    parked.mkdir(parents=True, exist_ok=True)
    host = socket.gethostname()

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
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate), \
         mock.patch.object(worker_loop, "PARKED_ROOT", parked):
        finisher.start()
        out = fm.resign(host, reason="busy resign red",
                        queue_root=queue.root, gate=gate,
                        broker_call=_broker_call(authority),
                        live=[], wait_s=45.0)
        finisher.join(timeout=10.0)
    terminals = fm.terminal_of(queue, key) is not None
    assert terminals and out["status"] == "resigned", (
        f"status={out.get('status')} reason={out.get('reason')} "
        f"terminal={terminals} withdrawn={out.get('withdrawn')}"
    )


def test_withdrawn_retry_safe_row_reports_pending_requeue(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """Until the approved requeue handoff hunk lands, resign reports retry-safe
    rows as owed (no silent drop, no duplicate, budget preserved)."""
    key = "8" * 64
    _incarnation(monkeypatch)
    snapshot = _publish_claim(queue, key, max_attempts=3)
    host = socket.gethostname()
    auth, _ = authority
    gate = Path(auth.maintenance_path)

    def holder_concludes():
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if queue.item_path(pool.WITHDRAWN, key).exists():
                break
            time.sleep(0.2)
        queue.finish(key, status="failed",
                     detail={"termination_reason": "resign-withdrawn"},
                     claim_snapshot=snapshot)

    import threading
    finisher = threading.Thread(target=holder_concludes, daemon=True)
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        finisher.start()
        try:
            out = fm.resign(host, reason="retry takeover red",
                            queue_root=queue.root, gate=gate,
                            broker_call=_broker_call(authority),
                            live=[], wait_s=40.0)
        finally:
            finisher.join(timeout=10.0)
    assert out["status"] == "resigning", out
    assert "requeue" in out.get("reason", ""), out
    assert queue.item_path(pool.WITHDRAWN, key).exists()
    assert queue.ready_items() == [], "no unilateral duplicate requeue"
    record = json.loads(queue.item_path(pool.WITHDRAWN, key).read_text())
    assert record.get("retry_safe") is True
    assert int(record.get("max_attempts")) == 3


@pytest.mark.xfail(strict=True, reason="needs the approved §6.5 requeue "
                   "handoff hunk: resign cannot mint budget-preserving "
                   "successors through publish(preempted_claim=...) alone")
def test_withdrawn_retry_safe_row_requeues_with_preserved_budget(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """Withdrawn retry-safe work returns as a new attempt, never a duplicate."""
    key = "8" * 64
    _incarnation(monkeypatch)
    _publish_claim(queue, key, max_attempts=3)
    host = socket.gethostname()
    auth, _ = authority
    gate = Path(auth.maintenance_path)
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        out = fm.resign(host, reason="retry takeover red",
                        queue_root=queue.root, gate=gate,
                        broker_call=_broker_call(authority),
                        live=[], wait_s=5.0)
    assert out["status"] in ("resigned", "resigning"), out
    ready = queue.ready_items()
    requeued = [item for item in ready if item.get("action_key") == key]
    assert len(requeued) == 1, (
        f"retry-safe withdraw must requeue exactly one successor, got {len(requeued)}"
    )
    assert int(requeued[0].get("attempts", -1)) == 1
    assert int(requeued[0].get("attempt_history_missing_before", -1)) == 1


def test_broker_refuses_create_while_draining(authority) -> None:
    """The broker mutex is the linearization point: create is refused under drain."""
    auth, backend = authority
    req = {"op": "create", "action_key": "7" * 64, "nonce": "b" * 32,
           "memory_max_bytes": 64 * 1024**2}
    created = auth.handle(os.getuid(), os.getpid(), dict(req))
    assert created["scope_id"] in backend.groups
    auth.admin(0, {"op": "maintenance_begin", "reason": "red", "owner": "t:1:2"})
    refused = auth.handle(os.getuid(), os.getpid(),
                          {"op": "create", "action_key": "6" * 64, "nonce": "c" * 32,
                           "memory_max_bytes": 64 * 1024**2})
    assert refused.get("ok") is False and refused.get("maintenance") is True
    status = auth.admin(0, {"op": "maintenance_status"})
    assert status["draining"] is True
    assert created["scope_id"] in status["active_scope_ids"]


def test_scope_release_order_empties_before_release(authority) -> None:
    """Stop, verify empty, then release: active_scopes returns to zero."""
    auth, backend = authority
    req = {"op": "create", "action_key": "5" * 64, "nonce": "d" * 32,
           "memory_max_bytes": 64 * 1024**2}
    created = auth.handle(os.getuid(), os.getpid(), dict(req))
    scope = created["scope_id"]
    token = created["token"]
    auth.handle(os.getuid(), os.getpid(),
                {"op": "stop", "action_key": "5" * 64, "nonce": "d" * 32,
                 "token": token, "reason": "test"})
    auth.handle(os.getuid(), os.getpid(),
                {"op": "release", "action_key": "5" * 64, "nonce": "d" * 32,
                 "token": token})
    status = auth.admin(0, {"op": "maintenance_status"})
    assert status["active_scopes"] == 0
    assert scope not in backend.groups
