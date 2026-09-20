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
from prismabuild import adaptive_cpu  # noqa: E402
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

    errors: list = []

    def holder_concludes():
        try:
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                if queue.item_path(pool.WITHDRAWN, key).exists():
                    break
                time.sleep(0.2)
            queue.finish(key, status="failed",
                         detail={"termination_reason": "resign-withdrawn"},
                         claim_snapshot=snapshot)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    finisher = threading.Thread(target=holder_concludes, daemon=True)
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate), \
         mock.patch.object(worker_loop, "PARKED_ROOT", parked):
        finisher.start()
        out = fm.resign(host, reason="busy resign red",
                        queue_root=queue.root, gate=gate,
                        broker_call=_broker_call(authority),
                        live=[], wait_s=45.0)
        finisher.join(timeout=10.0)
    assert not errors, errors
    terminals = fm.terminal_of(queue, snapshot) is not None
    assert terminals and out["status"] == "resigned", (
        f"status={out.get('status')} reason={out.get('reason')} "
        f"terminal={terminals} withdrawn={out.get('withdrawn')}"
    )


def test_withdrawn_retry_safe_row_requeues_with_preserved_budget(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    _sealed_shape(monkeypatch)
    """Withdrawn retry-safe work returns as a new attempt, never a duplicate.

    Full safe handoff, automatic with the original attempt budget: the
    successor carries attempts=1, the missing-prefix link, and the resign
    lineage, and resign still waits for the original attempt's exact
    terminal before reporting success.
    """
    key = "8" * 64
    _incarnation(monkeypatch)
    snapshot = _publish_claim(queue, key, max_attempts=3)
    host = socket.gethostname()
    auth, _ = authority
    gate = Path(auth.maintenance_path)

    errors: list = []

    def holder_concludes():
        try:
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                if queue.item_path(pool.WITHDRAWN, key).exists():
                    break
                time.sleep(0.2)
            queue.finish(key, status="failed",
                         detail={"termination_reason": "resign-withdrawn"},
                         claim_snapshot=snapshot)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    import threading
    finisher = threading.Thread(target=holder_concludes, daemon=True)
    errors: list = []
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        finisher.start()
        try:
            out = fm.resign(host, reason="retry handoff",
                            queue_root=queue.root, gate=gate,
                            broker_call=_broker_call(authority),
                            live=[], wait_s=40.0)
        finally:
            finisher.join(timeout=10.0)
    assert not errors, errors
    assert out["status"] == "resigned", out
    # Publishing the successor retires the live withdrawal marker into
    # withdrawn/superseded/; the original attempt's terminal was proven
    # in-round (resigned requires it) before the publish.
    decisions = queue.withdrawal_decisions(key)
    assert decisions, "withdrawal lineage must survive the handoff"
    ready = queue.ready_items()
    requeued = [item for item in ready if item.get("action_key") == key]
    assert len(requeued) == 1, (
        f"retry-safe withdraw must requeue exactly one successor, got {len(requeued)}"
    )
    assert int(requeued[0].get("attempts", -1)) == 1
    assert int(requeued[0].get("attempt_history_missing_before", -1)) == 1
    assert isinstance(requeued[0].get("resigned_by"), str)


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


def _sealed_shape(monkeypatch) -> None:
    """Stub only the CAS-sealed identity lookup (established pattern from
    test_preemption_handoff_admission.py): direct-publish test rows carry no
    sealed CAS action, and shape lookup is not what these tests prove."""
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", False))


def _proc_starttime(pid: int) -> str:
    line = Path(f"/proc/{pid}/stat").read_text()
    _, _, rest = line.rpartition(")")
    fields = rest.split()
    return fields[19] if len(fields) > 19 else "unknown"


def test_fenced_claim_open_fence_claims(queue: pool.PoolQueue) -> None:
    """An open fence claims through the real _claim path."""
    key = "a" * 64
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1}, max_attempts=1, retry_safe=True,
                  tags=["x86"])
    assert queue.claim(tags=["x86"], owner="h:1:q1", capacity={"cpu": 4},
                       admission_open=lambda: True) is not None


def test_fenced_claim_open_fence_claims(queue: pool.PoolQueue) -> None:
    key = "b" * 64
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1}, max_attempts=1, retry_safe=True,
                  tags=["x86"])
    got = queue.claim(tags=["x86"], owner="h:1:q1", capacity={"cpu": 4},
                      admission_open=lambda: True)
    assert got is not None and got["action_key"] == key


def test_fenced_claim_closed_fence_denies(queue: pool.PoolQueue) -> None:
    key = "c" * 64
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1}, max_attempts=1, retry_safe=True,
                  tags=["x86"])
    assert queue.claim(tags=["x86"], owner="h:1:q1", capacity={"cpu": 4},
                       admission_open=lambda: False) is None
    assert queue.item_path(pool.READY, key).exists()
    assert not queue.item_path(pool.CLAIMED, key).exists()


def test_requeue_refuses_foreign_cancellation(queue: pool.PoolQueue, monkeypatch) -> None:
    _sealed_shape(monkeypatch)
    """Only the party named in the withdrawal decision can revive it."""
    key = "d" * 64
    snapshot = _publish_claim(queue, key, max_attempts=3)
    queue.withdraw(key, reason="operator stop", by="operator:1")
    plan = queue.plan_requeue(snapshot)
    with pytest.raises(pool.PoolContractError):
        queue.publish(**plan["arguments"], preempted_claim=plan["snapshot"],
                      handoff_by="resign:impostor")


def test_broker_epoch_cas_refuses_stale(authority) -> None:
    """The mutex enforces the epoch: stale expectations refuse."""
    auth, _ = authority
    auth.admin(0, {"op": "maintenance_begin", "reason": "t",
                   "owner": "t:1:2"})
    epoch = json.loads(Path(auth.maintenance_path).read_text())["changed_unix"]
    with pytest.raises(ValueError, match="epoch changed"):
        auth.admin(0, {"op": "maintenance_end", "owner": "t:1:2",
                       "expected_changed_unix": epoch + 1000})
    out = auth.admin(0, {"op": "maintenance_end", "owner": "t:1:2",
                         "expected_changed_unix": epoch})
    assert out["draining"] is False


def test_broker_restart_takeover_without_force(authority) -> None:
    """A live supervisor takes over a dead owner's drain with the old epoch."""
    auth, _ = authority
    old = f"{socket.gethostname()}:supervisor-1:0"
    auth.admin(0, {"op": "maintenance_begin", "reason": "t", "owner": old})
    epoch = json.loads(Path(auth.maintenance_path).read_text())["changed_unix"]
    me = f"{socket.gethostname()}:supervisor-{os.getpid()}:{_proc_starttime(os.getpid())}"
    assert broker_mod._live_supervisor_owner(me) is True
    assert broker_mod._live_supervisor_owner(old) is False
    out = auth.admin(0, {"op": "maintenance_end", "owner": me,
                         "expected_changed_unix": epoch})
    assert out["draining"] is False
    gate = json.loads(Path(auth.maintenance_path).read_text())
    assert gate.get("takeover_of") == old
    assert gate.get("takeover_by") == me


def test_broker_takeover_refuses_live_old_supervisor(authority) -> None:
    """A live old supervisor is still around: no takeover from it."""
    auth, _ = authority
    host = socket.gethostname()
    old = f"{host}:supervisor-{os.getpid()}:{_proc_starttime(os.getpid())}"
    assert broker_mod._live_supervisor_owner(old) is True
    auth.admin(0, {"op": "maintenance_begin", "reason": "t", "owner": old})
    epoch = json.loads(Path(auth.maintenance_path).read_text())["changed_unix"]
    new = f"{host}:supervisor-1:{_proc_starttime(1)}"
    with pytest.raises(PermissionError):
        auth.admin(0, {"op": "maintenance_end", "owner": new,
                       "expected_changed_unix": epoch})
    assert auth.admin(0, {"op": "maintenance_status"})["draining"] is True


def test_broker_takeover_refuses_foreign_non_supervisor(authority) -> None:
    auth, _ = authority
    auth.admin(0, {"op": "maintenance_begin", "reason": "t",
                   "owner": f"{socket.gethostname()}:supervisor-1:0"})
    epoch = json.loads(Path(auth.maintenance_path).read_text())["changed_unix"]
    with pytest.raises(PermissionError):
        auth.admin(0, {"op": "maintenance_end", "owner": "someone-else",
                       "expected_changed_unix": epoch})
    with pytest.raises(ValueError, match="epoch changed"):
        auth.admin(0, {"op": "maintenance_end",
                       "owner": f"{socket.gethostname()}:supervisor-1:0",
                       "expected_changed_unix": epoch + 5000})


def test_shared_mount_proof_refuses_local_dir(tmp_path: Path, monkeypatch) -> None:
    """A locally writable directory does not prove shared access."""
    host = socket.gethostname()
    _incarnation(monkeypatch)
    root = tmp_path / "local-queue"
    root.mkdir()
    roster = tmp_path / "fleet_boxes.json"
    roster.write_text(json.dumps(
        {"boxes": {host: {"loops": 1, "args": []}}}))
    ok, info = fm.check_shared_mount(host, root, roster)
    assert ok is False, info
    assert "mount" in info


def test_terminal_is_exact_attempt_not_any_done(queue: pool.PoolQueue) -> None:
    """terminal_of pins the attempt: another generation's done is not ours."""
    key = "e" * 64
    first = _publish_claim(queue, key, max_attempts=1, retry_safe=False)
    queue.finish(key, status="executed", detail={},
                 claim_snapshot=dict(first))
    assert fm.terminal_of(queue, {
        "action_key": key, "published_unix": first["published_unix"],
        "attempts": first["attempts"], "claimed_by": first["claimed_by"],
        "claimed_unix": first["claimed_unix"]}) is not None
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1}, max_attempts=1, retry_safe=True,
                  tags=["x86"])
    second = queue.claim(tags=["x86"],
                         owner=f"{socket.gethostname()}:1:q9",
                         capacity={"cpu": 4})
    assert second is not None
    assert fm.terminal_of(queue, {
        "action_key": key, "published_unix": second["published_unix"],
        "attempts": second["attempts"], "claimed_by": second["claimed_by"],
        "claimed_unix": second["claimed_unix"]}) is None


def test_resign_retains_fence_on_unreadable_reader_refs(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """Reader pins that cannot be enumerated retain the fence (fail closed).

    Without the reader-lease module, a present consumer namespace is
    unknown — never drained, never another host's business to clear.
    Attestation writing and reclaim are the lease worker's automatic path;
    this lane only consumes the proof.
    """
    _incarnation(monkeypatch)
    host = socket.gethostname()
    auth, _ = authority
    gate = Path(auth.maintenance_path)
    queue.ensure_layout()
    consumer = (queue.root / "residency" / "leases" / ("c" * 64))
    consumer.mkdir(parents=True)
    (consumer / ("p" * 32 + ".lease.json")).write_text("{}")
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        out = fm.resign(host, reason="refs retained",
                        queue_root=queue.root, gate=gate,
                        broker_call=_broker_call(authority),
                        live=[], wait_s=5.0)
    assert out["status"] == "resigning", out
    assert "reader refs not drained" in out.get("reason", ""), out


def test_resign_drained_when_no_leases_namespace(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """An absent leases namespace is provably drained without the module."""
    drained, state, _ = fm.reader_refs_gate(queue, socket.gethostname(), None)
    assert drained is True and state == "drained-absent"
