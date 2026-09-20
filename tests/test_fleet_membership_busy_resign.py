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
    """A claimed uninterruptible attempt is retained, never cancelled: no
    withdrawal lands, and resign completes only on its natural terminal
    with empty scopes. Also proves handled-but-unclaimed attempts stay in
    the proof census (the terminal below lands after the claim file moves).
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
    finished = threading.Event()

    def holder_concludes():
        try:
            time.sleep(3.0)
            assert queue.item_path(pool.CLAIMED, key).exists()
            assert not queue.item_path(pool.WITHDRAWN, key).exists()
            queue.finish(key, status="executed", detail={},
                         claim_snapshot=snapshot)
            finished.set()
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
    assert finished.is_set()
    terminals = fm.terminal_of(queue, snapshot) is not None
    assert terminals and out["status"] == "resigned", (
        f"status={out.get('status')} reason={out.get('reason')} "
        f"terminal={terminals}"
    )
    assert not queue.item_path(pool.WITHDRAWN, key).exists()


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


def _busy_roster(tmp_path: Path, host: str, args: list) -> Path:
    roster = tmp_path / "fleet_boxes.json"
    roster.write_text(json.dumps({"boxes": {host: {"loops": 1, "args": args}}}))
    return roster


def _busy_queue(monkeypatch, tmp_path: Path) -> Path:
    from prismabuild import pool as _pool

    root = tmp_path / "q"
    _pool.PoolQueue(root).ensure_layout()
    monkeypatch.setattr(fm, "_mount_identity", lambda path: {
        "source": "dl380g10:/storage_pool/shared", "fstype": "nfs4",
        "mountpoint": "/mnt/shared"})
    return root


def _busy_runtime(tmp_path: Path) -> Path:
    root = tmp_path / "runtime"
    root.mkdir(exist_ok=True)
    (root / "RUNTIME_VERSION.json").write_text(json.dumps({"commit": "a" * 40}))
    return root


def _healthy_broker():
    def fake(payload: dict) -> dict:
        if payload["op"] == "maintenance_status":
            return {"ok": True, "draining": True, "health": True,
                    "active_scopes": 0, "active_scope_ids": []}
        raise AssertionError(payload)

    return fake


def test_qualified_busy_worker_joins_and_admission_decides(
    tmp_path: Path, monkeypatch
) -> None:
    """JOIN validates; admission decides. A box under external memory
    pressure still qualifies, with the pressure recorded in its observed
    offer — and that same observed figure is what refuses oversized work
    at placement while admitting fitting work."""
    host = socket.gethostname()
    _incarnation(monkeypatch)
    roster = _busy_roster(tmp_path, host, ["--class", "x86", "--mem-gb", "96"])
    root = _busy_queue(monkeypatch, tmp_path)
    checks = fm.qualify_host(host, queue_root=root, roster_path=roster,
                             broker_call=_healthy_broker(),
                             runtime_root=_busy_runtime(tmp_path),
                             held={}, mem_gb=10)
    assert checks["ok"] is True, checks
    assert checks["checks"]["offer"]["capacity"]["mem_gb"] == 2
    # External pressure shows up as foreign in the offer vocabulary (the
    # announce record's own `foreign` field) — recorded, not a join refusal.
    assert checks["checks"]["offer"]["foreign"] == {"mem_gb": 94}
    from prismabuild import pool as _pool

    q = _pool.PoolQueue(root)
    observed = dict(checks["checks"]["offer"]["capacity"], cpu=4)
    q.announce(host=host, tags=["x86", host], has_gpu=False,
               capacity=observed, runtime_commit="c" * 40)
    big = "b" * 64
    q.publish(action_key=big, cas_root=q.root / "cas",
              checkout_root=q.root / "co", worker_script=q.root / "worker.py",
              resources={"mem_gb": 8}, max_attempts=1, retry_safe=True,
              tags=["x86"])
    small = "c" * 64
    q.publish(action_key=small, cas_root=q.root / "cas",
              checkout_root=q.root / "co", worker_script=q.root / "worker.py",
              resources={"mem_gb": 2}, max_attempts=1, retry_safe=True,
              tags=["x86"])
    items = {i["action_key"]: i for i in q.ready_items()}
    assert q.placeable(items[big]) is False
    assert q.placeable(items[small]) is True


def test_gpu_foreign_load_defers_not_silently_admits(
    tmp_path: Path, monkeypatch
) -> None:
    """GPU declared with a trusted snapshot naming a foreign holder: the box
    still qualifies, its observed GPU capacity is zero with an explicit
    deferral note, and a GPU demand is not placeable on that offer."""
    host = socket.gethostname()
    _incarnation(monkeypatch)
    roster = _busy_roster(tmp_path, host,
                          ["--class", "x86", "--gpu", "--mem-gb", "16"])
    root = _busy_queue(monkeypatch, tmp_path)
    now = time.time()
    sample = {
        "schema": "prismabuild.gpu_capacity.v1",
        "complete": True, "attributed": True, "sampled_unix": now,
        "devices": [{"uuid": "GPU-testholds", "memory_domain": "shared_system"}],
        "foreign_processes": [{"pid": 4242}],
        "jobs": [],
        "host_total_bytes": 128 * 1024**3,
        "host_available_bytes": 64 * 1024**3,
        "memory_pressure_some": 0.0, "memory_pressure_full": 0.0,
        "cpu_pressure_some": 0.0, "cpu_pressure_full": 0.0,
    }
    checks = fm.qualify_host(host, queue_root=root, roster_path=roster,
                             broker_call=_healthy_broker(),
                             runtime_root=_busy_runtime(tmp_path),
                             gpu_sample=sample, now=now)
    assert checks["ok"] is True, checks
    offer = checks["checks"]["offer"]
    assert offer["capacity"]["gpu"] == 0
    assert offer["detail"]["gpu_capacity_trusted"] is True
    assert offer["detail"]["foreign_gpu_processes"] == 1
    assert "deferred-to-per-action-admission" in offer["gpu_admission"]
    from prismabuild import pool as _pool

    q = _pool.PoolQueue(root)
    q.announce(host=host, tags=["x86", host], has_gpu=True,
               capacity={"cpu": 4, "mem_gb": 16, "gpu": 0},
               runtime_commit="c" * 40)
    key = "d" * 64
    q.publish(action_key=key, cas_root=q.root / "cas",
              checkout_root=q.root / "co", worker_script=q.root / "worker.py",
              resources={"gpu": 1}, needs_gpu=True, max_attempts=1,
              retry_safe=True, tags=["x86"])
    assert q.placeable(q.ready_items()[0]) is False


def test_terminal_rejects_malformed_and_stale_counters(
    queue: pool.PoolQueue,
) -> None:
    """Exact-attempt proof is fail-closed: the counter must be typed and
    exactly one more than the snapshot's; anything else never matches."""
    key = "f" * 64
    snapshot = _publish_claim(queue, key, max_attempts=1, retry_safe=False)
    base = {"action_key": key, "published_unix": snapshot["published_unix"],
            "claimed_by": snapshot["claimed_by"],
            "claimed_unix": snapshot["claimed_unix"]}

    def check(record, expect):
        path = queue.item_path(pool.DONE, key)
        path.write_text(json.dumps(record))
        try:
            assert (fm.terminal_of(queue, snapshot) is not None) == expect, record
        finally:
            path.unlink(missing_ok=True)

    check({**base, "attempts": snapshot["attempts"]}, False)  # current, not advanced
    check({**base, "attempts": snapshot["attempts"] + 2}, False)  # skips ahead
    check({k: v for k, v in {**base, "attempts": 1}.items()
           if k != "attempts"}, False)  # counter-less archive shape
    check({k: v for k, v in {**base, "attempts": 1}.items()
           if k != "claimed_by"}, False)  # identity incomplete
    check({**base, "attempts": True}, False)  # bool is not a counter
    check({**base, "attempts": snapshot["attempts"] + 1}, True)  # exact transition




def test_resign_resumes_withdrawn_row_after_crash(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """Crash between withdraw and publish loses nothing: a new supervisor
    incarnation adopts the authoritative withdrawn intent (old owner
    provably gone), waits for the exact terminal, publishes exactly one
    linked successor, and reports resigned."""
    import threading

    key = "r" * 64
    host = socket.gethostname()
    old_owner = f"{host}:supervisor-111:222"
    snapshot = _publish_claim(queue, key, max_attempts=3)
    # Run 1 (crashed): withdrew, then died before terminal/publish.
    queue.withdraw(key, reason=f"resign {old_owner}: crash",
                   by=old_owner)
    auth, _ = authority
    gate = Path(auth.maintenance_path)
    _incarnation(monkeypatch)  # new incarnation, same box
    _sealed_shape(monkeypatch)
    errors: list = []
    finished = threading.Event()

    def holder_concludes():
        try:
            queue.finish(key, status="failed",
                         detail={"termination_reason": "resign-withdrawn"},
                         claim_snapshot=snapshot)
            finished.set()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    finisher = threading.Thread(target=holder_concludes, daemon=True)
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        finisher.start()
        try:
            out = fm.resign(host, reason="resume after crash",
                            queue_root=queue.root, gate=gate,
                            broker_call=_broker_call(authority),
                            live=[], wait_s=40.0)
        finally:
            finisher.join(timeout=10.0)
    assert not errors, errors
    assert finished.is_set()
    assert out["status"] == "resigned", out
    ready = queue.ready_items()
    requeued = [item for item in ready if item.get("action_key") == key]
    assert len(requeued) == 1
    assert int(requeued[0].get("attempts", -1)) == 1
    assert int(requeued[0].get("attempt_history_missing_before", -1)) == 1
    assert requeued[0].get("resigned_by") == old_owner
    assert requeued[0]["supersedes_withdrawal"]["withdrawn_by"] == old_owner


def test_resign_preserves_newer_unrelated_publication(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """An operator resubmission landing mid-resign is preserved byte-identical:
    no adoption without exact lineage, no overwrite, fence retained."""
    import threading

    key = "s" * 64
    host = socket.gethostname()
    old_owner = f"{host}:supervisor-111:222"
    snapshot = _publish_claim(queue, key, max_attempts=3)
    queue.withdraw(key, reason=f"resign {old_owner}: crash", by=old_owner)
    auth, _ = authority
    gate = Path(auth.maintenance_path)
    _incarnation(monkeypatch)
    _sealed_shape(monkeypatch)
    published: list = []
    errors: list = []
    refused: list = []

    def operator_and_holder():
        try:
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                if queue.item_path(pool.WITHDRAWN, key).exists():
                    break
                time.sleep(0.2)
            # Operator resubmits the same work while resign is waiting.
            queue.publish(action_key=key, cas_root=queue.root / "cas",
                          checkout_root=queue.root / "co",
                          worker_script=queue.root / "worker.py",
                          resources={"cpu": 1}, max_attempts=1, retry_safe=True,
                          tags=["x86"])
            published.append(json.loads(
                queue.item_path(pool.READY, key).read_text()))
            # Holder concludes the withdrawn attempt afterwards: against the
            # newer publication its finish must be refused (or entombed),
            # never overwrite the foreign row.
            try:
                queue.finish(key, status="failed",
                             detail={"termination_reason": "resign-withdrawn"},
                             claim_snapshot=snapshot)
            except pool.PoolContractError as exc:
                refused.append(exc)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    worker = threading.Thread(target=operator_and_holder, daemon=True)
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        worker.start()
        try:
            out = fm.resign(host, reason="preserve foreign row",
                            queue_root=queue.root, gate=gate,
                            broker_call=_broker_call(authority),
                            live=[], wait_s=25.0)
        finally:
            worker.join(timeout=10.0)
    assert not errors, errors
    assert published, "operator publication never landed"
    assert out["status"] == "resigning", out
    live = json.loads(queue.item_path(pool.READY, key).read_text())
    assert live == published[0], "resign must not touch the foreign row"


def test_resign_second_run_publishes_no_duplicate(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """A resign rerun after a completed handoff adopts the exact successor
    instead of publishing a second one."""
    import threading

    key = "t" * 64
    _incarnation(monkeypatch)
    _sealed_shape(monkeypatch)
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

    def run_resign():
        return fm.resign(host, reason="handoff",
                         queue_root=queue.root, gate=gate,
                         broker_call=_broker_call(authority),
                         live=[], wait_s=40.0)

    finisher = threading.Thread(target=holder_concludes, daemon=True)
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        finisher.start()
        try:
            first = run_resign()
        finally:
            finisher.join(timeout=10.0)
    assert not errors, errors
    assert first["status"] == "resigned", first
    before = json.loads(queue.item_path(pool.READY, key).read_text())
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        second = run_resign()
    assert second["status"] == "resigned", second
    after = json.loads(queue.item_path(pool.READY, key).read_text())
    assert after == before, "rerun must not publish a duplicate successor"
    assert len([i for i in queue.ready_items()
                if i.get("action_key") == key]) == 1
