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

from prismabuild import core as pb_core  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import adaptive_cpu  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import fleet_membership as fm  # noqa: E402
import worker_loop  # noqa: E402
import resource_broker as broker_mod  # noqa: E402
import tier_loop  # noqa: E402


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
              "maintenance_force_end", "maintenance_takeover"}

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
    old = f"{socket.gethostname()}:supervisor-4194304:1"
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
    """Proven absence drains with either the SDK or the legacy census."""
    drained, state, _ = fm.reader_refs_gate(queue, socket.gethostname(), None)
    assert drained is True and state in {"drained-absent", "refs-drained"}


#: What ``core._collect_worker_evidence`` reports on an x86 box with no
#: CUDA device, which is the platform ``--class x86`` requires.
X86_EVIDENCE = {"system": "linux", "machine": "x86_64", "accelerators": []}


def _busy_roster(tmp_path: Path, host: str, args: list, monkeypatch) -> Path:
    """A roster row for ``host`` declaring ``--class x86``, attested as x86.

    ``worker_evidence`` compares the roster's ``--class`` with what
    ``core._collect_worker_evidence`` reads off the live box, and refuses a
    mismatch.  These rows declare x86, so the tests qualified only on
    dl380g10.  On a GB10, which attests ``linux-aarch64-sm121``, qualification
    refused before the phase under test (#918).  The stub replaces the
    attestation, not the check, so the class comparison still runs;
    ``test_join_refuses_a_roster_class_the_live_host_does_not_attest`` in
    ``test_fleet_membership_join_resign.py`` keeps the refusal honest against
    the live box.
    """

    assert args[args.index("--class") + 1] == "x86", args
    monkeypatch.setattr(pb_core, "_collect_worker_evidence",
                        lambda **_kwargs: dict(X86_EVIDENCE))
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
    roster = _busy_roster(tmp_path, host, ["--class", "x86", "--mem-gb", "96"],
                          monkeypatch)
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
                          ["--class", "x86", "--gpu", "--mem-gb", "16"],
                          monkeypatch)
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
    old_owner = f"{host}:supervisor-4194304:1"
    snapshot = _publish_claim(queue, key, max_attempts=3)
    _sealed_shape(monkeypatch)
    # Run 1 (crashed): planned, withdrew with the proven carrier, then
    # died before terminal/publish. Run 2 must adopt the proven marker,
    # not stack a second decision.
    crashed_plan = queue.plan_requeue(dict(snapshot))
    queue.withdraw(key, reason=f"resign {old_owner}: crash",
                   by=old_owner,
                   membership_handoff=crashed_plan["snapshot"])
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
    old_owner = f"{host}:supervisor-4194304:1"
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

    key = "5" * 64
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


def test_chained_resign_preserves_budget_a_b_c(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """A resigns -> B claims -> B resigns -> C completes, budgets intact.

    Three attempts on max_attempts=4: every successor carries attempts+1,
    the missing-prefix link, and resign lineage, with no counter resets
    and no duplicate successors. Exercises the generalized prefix chain
    through the real resign path twice."""
    import threading

    _sealed_shape(monkeypatch)
    _incarnation(monkeypatch)
    key = "e" * 64
    host = socket.gethostname()
    auth, _ = authority
    gate = Path(auth.maintenance_path)
    owner = f"{host}:supervisor-9:8"
    _publish_claim(queue, key, max_attempts=4)
    errors: list = []

    def finish_when_withdrawn():
        deadline = time.monotonic() + 40.0
        while time.monotonic() < deadline:
            if queue.item_path(pool.WITHDRAWN, key).exists():
                break
            time.sleep(0.2)
        snap = json.loads(queue.item_path(pool.CLAIMED, key).read_text())
        queue.finish(key, status="failed",
                     detail={"termination_reason": "chain"},
                     claim_snapshot=snap)

    def do_resign():
        with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
            return fm.resign(host, reason="chain",
                             queue_root=queue.root, gate=gate,
                             broker_call=_broker_call(authority),
                             live=[], wait_s=60.0)

    t1 = threading.Thread(target=lambda: _finish_or_error(finish_when_withdrawn, errors))
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate), \
         mock.patch.object(worker_loop, "PARKED_ROOT", gate.parent / "rollout" / "parked"):
        t1.start()
        try:
            first = do_resign()
        finally:
            t1.join(timeout=15.0)
    assert not errors, errors
    assert first["status"] == "resigned", first
    generation_b = json.loads(queue.item_path(pool.READY, key).read_text())
    assert int(generation_b.get("attempts")) == 1
    assert int(generation_b.get("attempt_history_missing_before")) == 1
    assert int(generation_b.get("max_attempts")) == 4
    assert generation_b.get("resigned_by") == owner

    snap_b = queue.claim(tags=["x86"], owner=f"{host}:1:q2", capacity={"cpu": 4})
    assert snap_b is not None and int(snap_b["attempts"]) == 1
    t2 = threading.Thread(target=lambda: _finish_or_error(finish_when_withdrawn, errors))
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate), \
         mock.patch.object(worker_loop, "PARKED_ROOT", gate.parent / "rollout" / "parked"):
        t2.start()
        try:
            second = do_resign()
        finally:
            t2.join(timeout=15.0)
    assert not errors, errors
    assert second["status"] == "resigned", (
        second.get("reason"), second.get("handled"))
    generation_c = json.loads(queue.item_path(pool.READY, key).read_text())
    assert int(generation_c.get("attempts")) == 2
    assert int(generation_c.get("attempt_history_missing_before")) == 2
    assert int(generation_c.get("max_attempts")) == 4
    assert generation_c.get("resigned_by") == owner
    assert generation_c["supersedes_withdrawal"]["withdrawn_by"] == owner

    snap_c = queue.claim(tags=["x86"], owner=f"{host}:1:q3", capacity={"cpu": 4})
    assert snap_c is not None and int(snap_c["attempts"]) == 2
    queue.finish(key, status="executed", detail={}, claim_snapshot=snap_c)
    done = json.loads(queue.item_path(pool.DONE, key).read_text())
    assert int(done.get("attempts")) == 3
    assert int(done.get("max_attempts")) == 4


def _finish_or_error(fn, errors):
    try:
        fn()
    except BaseException:  # noqa: BLE001
        import traceback
        errors.append(traceback.format_exc())


def test_lineage_rejects_coincident_counter_foreign_row(
    queue: pool.PoolQueue,
) -> None:
    """Same key, same counter, different generation: lineage says foreign.

    A ready occupant with attempts == prior+1 but a supersedes link to
    another decision (or none at all) is preserved, never adopted as our
    successor."""
    key = "f" * 64
    snapshot = _publish_claim(queue, key, max_attempts=3)
    snap = {"action_key": key, "published_unix": snapshot["published_unix"],
            "attempts": snapshot["attempts"],
            "claimed_by": snapshot["claimed_by"],
            "claimed_unix": snapshot["claimed_unix"]}
    status = fm.lineage_status(queue, snap, "resign:someone")
    assert status["terminal"] is None and not status["foreign"]
    # A foreign publication with a coincident counter and no linkage.
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1}, max_attempts=3, retry_safe=True,
                  tags=["x86"])
    status = fm.lineage_status(queue, snap, "resign:someone")
    assert status["foreign"] is True and not status["successor_exact"]
    # Malformed terminal identity never matches either.
    bad = dict(snap, claimed_by="")
    assert fm.terminal_of(queue, bad) is None


def test_takeover_keeps_gate_closed_and_resumes(
    queue: pool.PoolQueue, live_broker, tmp_path: Path, monkeypatch
) -> None:
    """A crashed drain is taken over without opening admission: the new
    incarnation presents the exact epoch through the real broker admin,
    the gate stays draining on the same epoch, and handoffs resume."""
    import threading

    key = "0" * 64
    host = socket.gethostname()
    old_owner = f"{host}:supervisor-4194304:1"
    _sealed_shape(monkeypatch)
    snapshot = _publish_claim(queue, key, max_attempts=3)
    auth, call = live_broker
    gate = Path(auth.maintenance_path)
    # Crashed run 1 went through the REAL socket server: drain held by the
    # (now dead) old owner. The transport converts the held-by refusal to
    # OSError, which resign must survive via gate-state takeover (never by
    # parsing refusal prose).
    began = call({"op": "maintenance_begin", "reason": "run1",
                  "owner": old_owner})
    assert began["draining"] is True
    epoch = json.loads(gate.read_text())["changed_unix"]
    # Live new incarnation (real pid + starttime so the broker's own
    # liveness check passes takeover); same box.
    live_owner = (f"{host}:supervisor-{os.getpid()}:"
                  f"{_proc_starttime(os.getpid())}")
    monkeypatch.setattr(fm, "supervisor_incarnation",
                        lambda: (live_owner, None))
    errors: list = []
    finished = threading.Event()

    def holder_concludes():
        try:
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                if queue.item_path(pool.WITHDRAWN, key).exists():
                    break
                time.sleep(0.2)
            queue.finish(key, status="failed",
                         detail={"termination_reason": "takeover"},
                         claim_snapshot=snapshot)
            finished.set()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    finisher = threading.Thread(target=holder_concludes, daemon=True)
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        finisher.start()
        try:
            out = fm.resign(host, reason="takeover resume",
                            queue_root=queue.root, gate=gate,
                            broker_call=call,
                            live=[], wait_s=40.0)
        finally:
            finisher.join(timeout=10.0)
    assert not errors, errors
    assert finished.is_set()
    assert out["status"] == "resigned", out
    gate_now = json.loads(gate.read_text())
    assert gate_now["draining"] is True, "takeover must keep the gate closed"
    assert gate_now["changed_unix"] == epoch, "epoch must not move"
    assert gate_now["owner"] != old_owner
    ready = queue.ready_items()
    assert len([i for i in ready if i.get("action_key") == key]) == 1


def test_join_refuses_unsettled_handoff_drain(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """JOIN must not clear a membership drain with unsettled obligations:
    it refuses until a resign adopts and settles them."""
    import threading

    key = "1" * 64
    host = socket.gethostname()
    old_owner = f"{host}:supervisor-4194304:1"
    _sealed_shape(monkeypatch)
    _incarnation(monkeypatch)
    snapshot = _publish_claim(queue, key, max_attempts=3)
    crashed_plan = queue.plan_requeue(dict(snapshot))
    queue.withdraw(key, reason=f"resign {old_owner}: crash", by=old_owner,
                   membership_handoff=crashed_plan["snapshot"])
    auth, _ = authority
    gate = Path(auth.maintenance_path)
    auth.admin(0, {"op": "maintenance_begin", "reason": "run1",
                   "owner": old_owner})
    queue.ensure_layout()
    roster = _busy_roster(tmp_path, host, ["--class", "x86"], monkeypatch)
    # Join must read the REAL queue (holding the unsettled row) through a
    # namespace that proves shared: patch only the mount identity.
    monkeypatch.setattr(fm, "_mount_identity", lambda path: {
        "source": "dl380g10:/storage_pool/shared", "fstype": "nfs4",
        "mountpoint": "/mnt/shared"})
    refused = fm.join(host, reason="too early", roster_path=roster,
                      queue_root=queue.root, gate=gate,
                      runtime_root=_busy_runtime(tmp_path),
                      broker_call=_broker_call(authority))
    assert refused["status"] == "refused", refused
    assert refused["phase"] == "unsettled-handoff", refused
    assert gate.read_text() and json.loads(gate.read_text())["draining"] is True


def test_interrupted_request_settles_through_reconciler(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """The requester disappearing mid-handoff loses nothing: run 1 withdraws
    and returns (deadline expiry = crash), the holder concludes normally,
    the deployed reconciler path publishes the successor with no resign
    running, and run 2 adopts it instead of duplicating it."""
    import threading

    _sealed_shape(monkeypatch)
    _incarnation(monkeypatch)
    key = "2" * 64
    host = socket.gethostname()
    snapshot = _publish_claim(queue, key, max_attempts=3)
    auth, _ = authority
    gate = Path(auth.maintenance_path)

    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        first = fm.resign(host, reason="dies after withdraw",
                          queue_root=queue.root, gate=gate,
                          broker_call=_broker_call(authority),
                          live=[], wait_s=0.0)
    assert first["status"] == "resigning", first
    assert queue.item_path(pool.WITHDRAWN, key).exists()
    assert not queue.item_path(pool.READY, key).exists()
    # Ordinary holder finish (no resign running anywhere).
    queue.finish(key, status="failed",
                 detail={"termination_reason": "reconciler"},
                 claim_snapshot=snapshot)
    # The deployed reconciliation call the worker loops drive every poll.
    settled = fm.reconcile_membership(queue, host)
    assert settled["published"] == [key[:12]], settled
    assert settled["retained"] == []
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate):
        second = fm.resign(host, reason="adopts reconciled row",
                           queue_root=queue.root, gate=gate,
                           broker_call=_broker_call(authority),
                           live=[], wait_s=40.0)
    assert second["status"] == "resigned", second
    ready = [i for i in queue.ready_items() if i.get("action_key") == key]
    assert len(ready) == 1 and int(ready[0]["attempts"]) == 1


def test_dead_owner_proof_matrix() -> None:
    """Malformed stat and foreign hosts never prove gone; only absent pids
    and reused start times do."""
    import resource_broker as _broker

    host = socket.gethostname()
    assert _broker._dead_supervisor_owner("client-upgrade") is False
    assert _broker._dead_supervisor_owner(f"otherbox:supervisor-1:2") is False
    # A named non-decimal starttime proves nothing: it always differs from
    # a real field-22 read, which must not read as PID reuse.
    assert _broker._dead_supervisor_owner(
        f"{host}:supervisor-1:not-a-starttime") is False
    assert _broker._dead_supervisor_owner(
        f"{host}:supervisor-1:unknown") is False
    assert _broker._dead_supervisor_owner(
        f"{host}:supervisor-1:0") is False
    # Non-ASCII decimals (isdigit True, int() rejects) prove nothing either.
    assert _broker._dead_supervisor_owner(
        f"{host}:supervisor-1:\u00b2") is False
    assert _broker._dead_supervisor_owner(
        f"{host}:supervisor-{2 ** 22}:1") is True
    me = (f"{host}:supervisor-{os.getpid()}:"
          f"{_proc_starttime(os.getpid())}")
    assert _broker._live_supervisor_owner(me) is True
    assert _broker._dead_supervisor_owner(me) is False
    assert _broker._dead_supervisor_owner(f"{host}:supervisor-9:unknown") is False


def test_takeover_refuses_open_gate_and_stale_epoch(authority) -> None:
    """Takeover needs a live drain at the named epoch: open gates and moved
    epochs refuse instead of minting or clearing anything."""
    auth, _ = authority
    host = socket.gethostname()
    me = (f"{host}:supervisor-{os.getpid()}:"
          f"{_proc_starttime(os.getpid())}")
    with pytest.raises(ValueError, match="epoch changed"):
        auth.admin(0, {"op": "maintenance_takeover", "owner": me,
                       "reason": "t", "expected_changed_unix": 1.0})
    auth.admin(0, {"op": "maintenance_begin", "reason": "t",
                   "owner": f"{host}:supervisor-1:0"})
    auth.admin(0, {"op": "maintenance_end",
                   "owner": f"{host}:supervisor-1:0"})
    opened_epoch = json.loads(
        Path(auth.maintenance_path).read_text())["changed_unix"]
    with pytest.raises(ValueError, match="no membership drain"):
        auth.admin(0, {"op": "maintenance_takeover", "owner": me,
                       "reason": "t", "expected_changed_unix": opened_epoch})
    old = f"{host}:supervisor-1:0"
    auth.admin(0, {"op": "maintenance_begin", "reason": "t", "owner": old})
    epoch = json.loads(Path(auth.maintenance_path).read_text())["changed_unix"]
    with pytest.raises(ValueError, match="epoch changed"):
        auth.admin(0, {"op": "maintenance_takeover", "owner": me,
                       "reason": "t", "expected_changed_unix": epoch + 5000})
    with pytest.raises(ValueError, match="invalid maintenance"):
        auth.admin(0, {"op": "maintenance_takeover", "owner": me,
                       "reason": "t"})
    assert auth.admin(0, {"op": "maintenance_status"})["draining"] is True


def test_lineage_exact_requires_full_fields(queue: pool.PoolQueue, monkeypatch) -> None:
    """Same owner and same decision timestamp never suffice: altered
    attempts, budget, or parent generation must refuse exact.

    Goes through the real proven handoff (publish/claim/plan/withdraw
    with carrier/finish/publish) so the exact READY row carries the
    pool's own prefix chain over a proven decision; then mutates one
    field at a time in the READY occupant and proves ``lineage_status``
    refuses each (foreign, never exact). Reuses the queue's
    ``_preemption_prefix_valid`` chain owner — no duplicate validator.
    """
    _sealed_shape(monkeypatch)
    key = "3" * 64
    host = socket.gethostname()
    owner = f"{host}:supervisor-9:8"
    snapshot = _publish_claim(queue, key, max_attempts=4)
    pre = queue.plan_requeue(dict(snapshot))
    queue.withdraw(key, reason=f"resign {owner}: t", by=owner,
                   membership_handoff=pre["snapshot"])
    queue.finish(key, status="failed", detail={}, claim_snapshot=snapshot)
    live = json.loads(queue.item_path(pool.WITHDRAWN, key).read_text())
    snap = {"action_key": key, "published_unix": live["published_unix"],
            "attempts": live["attempts"], "claimed_by": live["claimed_by"],
            "claimed_unix": live["claimed_unix"]}
    plan = queue.plan_requeue(dict(live))
    queue.publish(**plan["arguments"], preempted_claim=plan["snapshot"],
                  handoff_by=owner)
    # Exact via the retired immutable decision (live retired on publish).
    status = fm.lineage_status(queue, snap, owner)
    assert status["successor_exact"] is True, status
    assert status["foreign"] is False
    ready_path = queue.item_path(pool.READY, key)
    exact_ready = json.loads(ready_path.read_text())

    def mutated(**overrides):
        row = dict(exact_ready)
        row.update(overrides)
        if "supersedes_withdrawal" in overrides:
            row["supersedes_withdrawal"] = overrides["supersedes_withdrawal"]
        ready_path.write_text(json.dumps(row))
        return fm.lineage_status(queue, snap, owner)

    # Altered counter with same owner/timestamp refuses.
    st = mutated(attempts=int(exact_ready["attempts"]) + 1)
    assert st["successor_exact"] is False and st["foreign"] is True, st
    # Altered budget refuses.
    st = mutated(max_attempts=int(exact_ready["max_attempts"]) + 1)
    assert st["successor_exact"] is False and st["foreign"] is True, st
    # Altered parent generation refuses (same withdrawn_unix, other parent).
    link = dict(exact_ready["supersedes_withdrawal"])
    link["published_unix"] = float(link["published_unix"]) + 1000.0
    st = mutated(supersedes_withdrawal=link)
    assert st["successor_exact"] is False and st["foreign"] is True, st
    # Altered owner refuses even with timestamps intact.
    st = mutated(resigned_by="otherbox:supervisor-1:2")
    assert st["successor_exact"] is False and st["foreign"] is True, st
    # Restore exact for other tests' isolation (file rewritten per test tmp).
    ready_path.write_text(json.dumps(exact_ready))
    assert fm.lineage_status(queue, snap, owner)["successor_exact"] is True


def test_closed_gate_poll_settles_through_drain_path(tmp_path: Path, monkeypatch) -> None:
    """A resigned worker settles owed handoffs through its real closed-gate
    poll — not a helper direct call.

    Drives the actual ``worker_loop._run_loop`` with a closed maintenance
    gate and ``--once``: the drain branch (after the generation fence, no
    claim) must publish the matured successor via the existing retry path.
    The open-gate hook is unreachable under a closed gate (``continue``),
    so this proves the drain-path reconciliation root required.
    """
    import importlib.util
    import sys as _sys

    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = f"{host}:supervisor-9:8"
    # Real queue at the loop's hardcoded SH/"pb-queue" address.
    real_queue = pool.PoolQueue(tmp_path / "pb-queue")
    key = "4" * 64
    real_queue.publish(action_key=key, cas_root=real_queue.root / "cas",
                       checkout_root=real_queue.root / "co",
                       worker_script=real_queue.root / "worker.py",
                       resources={"cpu": 1}, max_attempts=3, retry_safe=True,
                       tags=["x86"])
    snap = real_queue.claim(tags=["x86"], owner=f"{host}:1:q1",
                            capacity={"cpu": 4})
    assert snap is not None
    drain_plan = real_queue.plan_requeue(dict(snap))
    real_queue.withdraw(key, reason=f"resign {owner}: drain poll", by=owner,
                        membership_handoff=drain_plan["snapshot"])
    real_queue.finish(key, status="failed", detail={}, claim_snapshot=snap)
    assert real_queue.item_path(pool.WITHDRAWN, key).exists()
    assert not real_queue.item_path(pool.READY, key).exists()

    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": True, "changed_unix": 1.0,
                                "reason": "test drain", "owner": owner}))
    parked = tmp_path / "rollout" / "parked"
    parked.mkdir(parents=True)

    source = Path(__file__).resolve().parents[1] / "tools/fleet/worker_loop.py"
    spec = importlib.util.spec_from_file_location("drain_poll_loop", source)
    assert spec is not None and spec.loader is not None
    loop = importlib.util.module_from_spec(spec)
    _sys.modules["drain_poll_loop"] = loop
    spec.loader.exec_module(loop)
    monkeypatch.setattr(loop, "SH", tmp_path)
    monkeypatch.setattr(loop, "MAINTENANCE_GATE", gate)
    monkeypatch.setattr(loop, "PARKED_ROOT", parked)
    monkeypatch.setattr(loop, "loaded_runtime_commit", lambda: "test")
    monkeypatch.setattr(loop, "published_commit", lambda: "test")
    monkeypatch.setattr(loop, "_generation_at", lambda path: "test")
    monkeypatch.setattr(loop, "generation_drift",
                        lambda loaded_commit=None, loaded_generation=None: None)
    monkeypatch.setattr(loop.cpu_topology, "inherited_tiers", lambda: None)
    monkeypatch.setattr(loop.sys, "argv",
                        ["worker_loop.py", "--all-cores", "--cpu-slots", "1",
                         "--poll-s", "0", "--once"])
    # Reconciler resolves its owner from this lane's incarnation.
    monkeypatch.setattr(fm, "supervisor_incarnation", lambda: (owner, None))
    rc = loop._run_loop(lambda: False)
    assert rc == 0
    ready_path = real_queue.item_path(pool.READY, key)
    assert ready_path.exists(), "drain poll must publish the owed successor"
    ready = json.loads(ready_path.read_text())
    assert int(ready.get("attempts")) == 1
    assert ready.get("resigned_by") == owner
    link = ready.get("supersedes_withdrawal")
    assert isinstance(link, dict) and link.get("withdrawn_by") == owner


def _live_owner() -> str:
    """This box's provably live supervisor owner (real pid + starttime)."""
    host = socket.gethostname()
    owner = (f"{host}:supervisor-{os.getpid()}:"
             f"{_proc_starttime(os.getpid())}")
    assert broker_mod._live_supervisor_owner(owner) is True
    return owner


@pytest.fixture()
def live_broker(authority, tmp_path: Path):
    """Real Authority behind a real Unix-socket broker, driven through the
    real ResourceScope socket client. The privileged peer boundary is
    controlled in-fixture only (see _RootPeerSocket); no live privilege
    is touched."""
    import threading
    from prismabuild.resource_scope import broker_request

    auth, _ = authority

    class _RootPeerSocket:
        """Accepted-socket proxy reporting root peer creds.

        The privileged peer boundary, controlled in-fixture only: the
        server side reads uid 0 off its own accepted socket while the
        client speaks the real framing over a real socketpair path. No
        live privilege is touched and no global socket behavior changes.
        """

        def __init__(self, real):
            self._real = real

        def getsockopt(self, level, optname, *rest):
            if (level == socket.SOL_SOCKET
                    and optname == socket.SO_PEERCRED):
                import struct
                return struct.pack("3i", os.getpid(), 0, 0)
            return self._real.getsockopt(level, optname, *rest)

        def __getattr__(self, name):
            return getattr(self._real, name)

    class _RootPeerServer(broker_mod.Server):
        def get_request(self):
            real, addr = super().get_request()
            return _RootPeerSocket(real), addr

    sock = tmp_path / "resources.sock"
    server = _RootPeerServer(str(sock), broker_mod.Handler)
    server.authority = auth
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05},
        daemon=True)
    thread.start()
    try:
        yield auth, lambda payload: broker_request(
            dict(payload), socket_path=sock)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


def test_broker_end_refuses_reason_field(authority) -> None:
    """The field contract itself: reason rides begin/takeover only."""
    auth, _ = authority
    auth.admin(0, {"op": "maintenance_begin", "reason": "t",
                   "owner": f"{socket.gethostname()}:supervisor-4194304:1"})
    epoch = json.loads(Path(auth.maintenance_path).read_text())["changed_unix"]
    with pytest.raises(ValueError, match="invalid maintenance fields"):
        auth.admin(0, {"op": "maintenance_end", "owner": "x", "reason": "r",
                       "expected_changed_unix": epoch})


def test_takeover_protocol_over_real_socket(live_broker) -> None:
    """Dead-owner takeover through Socket/Handler/ResourceScope: the wire
    surfaces the held-by refusal as OSError (never a Python
    PermissionError), takeover keeps the gate closed on the same epoch."""
    auth, call = live_broker
    host = socket.gethostname()
    old = f"{host}:supervisor-4194304:1"
    me = _live_owner()
    began = call({"op": "maintenance_begin", "owner": old, "reason": "run1"})
    assert began["ok"] is True and began["draining"] is True
    epoch = json.loads(Path(auth.maintenance_path).read_text())["changed_unix"]
    with pytest.raises(OSError, match="refused"):
        call({"op": "maintenance_begin", "owner": me, "reason": "run2",
              "expected_changed_unix": epoch})
    took = call({"op": "maintenance_takeover", "owner": me,
                 "reason": "resume", "expected_changed_unix": epoch})
    assert took["ok"] is True and took["draining"] is True
    gate = json.loads(Path(auth.maintenance_path).read_text())
    assert gate["draining"] is True
    assert gate["changed_unix"] == epoch
    assert gate["owner"] == me
    assert gate["takeover_of"] == old


def test_takeover_refuses_live_and_foreign_holds_over_real_socket(
    live_broker,
) -> None:
    """Takeover of a live membership hold and of a foreign (operator)
    hold both refuse over the wire; the gate is untouched either way."""
    auth, call = live_broker
    host = socket.gethostname()
    me = _live_owner()
    began = call({"op": "maintenance_begin", "owner": me, "reason": "t"})
    assert began["ok"] is True
    epoch = json.loads(Path(auth.maintenance_path).read_text())["changed_unix"]
    with pytest.raises(OSError, match="refused"):
        call({"op": "maintenance_takeover", "owner": me, "reason": "t",
              "expected_changed_unix": epoch})
    gate = json.loads(Path(auth.maintenance_path).read_text())
    assert gate["owner"] == me and gate["changed_unix"] == epoch
    ended = call({"op": "maintenance_end", "owner": me,
                  "expected_changed_unix": epoch})
    assert ended["ok"] is True and ended["draining"] is False
    call({"op": "maintenance_begin", "owner": "upgrade-window",
          "reason": "t"})
    epoch2 = json.loads(Path(auth.maintenance_path).read_text())["changed_unix"]
    with pytest.raises(OSError, match="refused"):
        call({"op": "maintenance_takeover", "owner": me, "reason": "t",
              "expected_changed_unix": epoch2})
    gate = json.loads(Path(auth.maintenance_path).read_text())
    assert gate["draining"] is True and gate["owner"] == "upgrade-window"


def test_lineage_treats_unreadable_ready_as_unknown(
    queue: pool.PoolQueue, monkeypatch
) -> None:
    """An unreadable ready slot is unknown, never absent: nothing plans a
    publication over it — the reconciler retains and JOIN keeps the fence."""
    _incarnation(monkeypatch)
    _sealed_shape(monkeypatch)
    key = "6" * 64
    host = socket.gethostname()
    owner = f"{host}:supervisor-9:8"
    snapshot = _publish_claim(queue, key, max_attempts=3)
    pre = queue.plan_requeue(dict(snapshot))
    queue.withdraw(key, reason=f"resign {owner}: t", by=owner,
                   membership_handoff=pre["snapshot"])
    queue.finish(key, status="failed", detail={}, claim_snapshot=snapshot)
    live = json.loads(queue.item_path(pool.WITHDRAWN, key).read_text())
    snap = {"action_key": key, "published_unix": live["published_unix"],
            "attempts": live["attempts"], "claimed_by": live["claimed_by"],
            "claimed_unix": live["claimed_unix"]}
    queue.item_path(pool.READY, key).mkdir()  # read_text -> IsADirectoryError
    status = fm.lineage_status(queue, snap, owner)
    assert status["successor"] is None
    assert status["successor_unknown"] is True
    assert status["successor_exact"] is False
    owed, _ = fm.resume_owed(queue, host, owner)
    assert [r["action_key"] for r in owed] == [key]
    assert owed[0]["plan"] is None
    rec = fm.reconcile_membership(queue, host, owner)
    assert rec["published"] == [] and rec["adopted"] == []
    assert any("unreadable" in str(r.get("reason", "")) for r in rec["retained"])


def test_unreadable_withdrawn_census_keeps_gate_closed(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """An unreadable withdrawn/ census is unknown, never empty.

    ``Path.glob`` suppresses directory OSError and yields no entries, so
    a permission-denied census must not read as "nothing owed" with the
    gate opening over unknown rows: ``resume_owed`` reports skipped via
    error-preserving ``os.scandir`` materialization, and JOIN retains the
    closed gate. Runs a real chmod when non-root; under a root runner
    the same EACCES comes out of the real census call itself (never a
    faked ``resume_owed`` return). Mode is restored in ``finally``.
    """
    host = socket.gethostname()
    owner = _incarnation(monkeypatch)
    auth, _ = authority
    gate = Path(auth.maintenance_path)
    auth.admin(0, {"op": "maintenance_begin", "reason": "t", "owner": owner})
    queue.ensure_layout()
    withdrawn = queue.dir(pool.WITHDRAWN)
    roster = _busy_roster(tmp_path, host, ["--class", "x86"], monkeypatch)
    monkeypatch.setattr(fm, "_mount_identity", lambda path: {
        "source": "dl380g10:/storage_pool/shared", "fstype": "nfs4",
        "mountpoint": "/mnt/shared"})

    def attempt_join():
        return fm.join(host, reason="census dark", roster_path=roster,
                       queue_root=queue.root, gate=gate,
                       runtime_root=_busy_runtime(tmp_path),
                       broker_call=_broker_call(authority))

    if os.geteuid() != 0:
        os.chmod(withdrawn, 0)
        try:
            owed, skipped = fm.resume_owed(queue, host, owner)
            assert owed == [] and skipped != []
            out = attempt_join()
        finally:
            os.chmod(withdrawn, 0o755)
    else:
        real_scandir = os.scandir

        def denied_scandir(path, *args, **kwargs):
            if Path(path) == withdrawn:
                raise PermissionError(13, "Permission denied")
            return real_scandir(path, *args, **kwargs)

        monkeypatch.setattr(os, "scandir", denied_scandir)
        owed, skipped = fm.resume_owed(queue, host, owner)
        assert owed == [] and skipped != []
        out = attempt_join()
    assert out["status"] == "refused", out
    assert out["phase"] == "unsettled-unknown", out
    assert json.loads(gate.read_text())["draining"] is True


def test_unreadable_claimed_census_retains_resignation(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """A dark claimed directory cannot establish that the worker resigned."""
    host = socket.gethostname()
    _incarnation(monkeypatch)
    auth, _ = authority
    gate = Path(auth.maintenance_path)
    queue.ensure_layout()
    claimed = queue.dir(pool.CLAIMED)

    def inspect_and_resign():
        owned, unknown = fm.claimed_census(queue, host)
        assert owned == [] and unknown, "unreadable claims were reported empty"
        out = fm.resign(host, reason="claim census dark", queue_root=queue.root,
                        gate=gate, broker_call=_broker_call(authority),
                        live=[], wait_s=0)
        assert out["status"] == "resigning", out
        assert json.loads(gate.read_text())["draining"] is True

    if os.geteuid() != 0:
        os.chmod(claimed, 0)
        try:
            inspect_and_resign()
        finally:
            os.chmod(claimed, 0o755)
    else:
        real_scandir = os.scandir

        def denied_scandir(path, *args, **kwargs):
            if Path(path) == claimed:
                raise PermissionError(13, "Permission denied")
            return real_scandir(path, *args, **kwargs)

        monkeypatch.setattr(os, "scandir", denied_scandir)
        inspect_and_resign()


def test_resign_staged_consumer_drains_through_handoff_carrier(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """The ordinary membership drain path threads the handoff carrier: a
    staged consumer resigns with its proven plan, the frozen window
    survives the drain, and the successor carries the leads block."""
    import threading

    _sealed_shape(monkeypatch)
    host = socket.gethostname()
    owner = _incarnation(monkeypatch)
    tier = "prismabuild-stage:dl380g10"
    manifest = "ab" * 32
    gib = 1 << 30
    consumer, mover = "d4" * 32, "d5" * 32
    queue.mint_tier_capacity(tier, {"stage_gib": 8})
    plan = residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=tier,
        stage_root="/stage/prewarm", manifest_sha256=manifest,
        manifest_bytes=2 * gib, phases=[{
            "name": "phase-0000", "start_bytes": 0, "end_bytes": 2 * gib,
            "stage_gib": 2,
            "mover_row": {
                "action_key": mover,
                "cas_root": str(queue.root / "cas"),
                "checkout_root": str(queue.root / "co"),
                "worker_script": str(queue.root / "worker.py"),
                "tags": ["dl380g10"],
                "resources": {f"stage_gib@{tier}": 2, "mem_gb": 1},
                "retry_safe": True, "max_attempts": 3,
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": tier,
                    "manifest_sha256": manifest, "manifest_bytes": 2 * gib,
                    "range_start_bytes": 0, "range_end_bytes": 2 * gib}},
            "egress_row": {
                "action_key": "d6" * 32,
                "cas_root": str(queue.root / "cas"),
                "checkout_root": str(queue.root / "co"),
                "worker_script": str(queue.root / "worker.py"),
                "tags": ["dl380g10"], "resources": {"mem_gb": 1}}}])
    residency_plan.freeze(queue, plan)
    block = {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": tier,
             "manifest_sha256": manifest, "manifest_bytes": 2 * gib,
             "leads": residency_plan.leads_for(plan)}
    queue.publish(action_key=consumer, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1},
                  max_attempts=3, retry_safe=True, tags=["x86"],
                  residency=block)
    events = tier_loop.residency_window(
        queue, tiers={tier: {"tier_id": tier, "tier": "stage",
                             "mountpoint": str(tmp_path / "stage")}})
    assert mover in [e["action_key"] for e in events
                     if e["event"] == "mover-published"]
    snap_m = queue.claim(tags=["dl380g10"], owner=f"{host}:1:sm",
                         capacity={"cpu": 4, "mem_gb": 16})
    assert snap_m is not None and snap_m["action_key"] == mover
    queue.record_move(mover, {
        "tier_id": tier, "manifest_sha256": manifest,
        "range_start_bytes": 0, "range_end_bytes": 2 * gib,
        "bytes_staged": 2 * gib, "complete": True, "errors": []})
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": tier, "stage_root": str(tmp_path / "stage"),
        "manifest_sha256": manifest,
        "entries": {residency_map.residency_map_key("/pool/a.bin", 0): {
            "stage_path": str(tmp_path / "stage" / "a.bin"),
            "bytes": 4096, "offset": 0, "sha256": "a" * 64}}})
    queue.finish(mover, status="executed", detail={}, claim_snapshot=snap_m)
    assert tier_loop.compose_map(queue, consumer) == queue.residency_map_path(
        consumer)
    snap = queue.claim(tags=["x86"], owner=f"{host}:1:sc",
                       capacity={"cpu": 4, "mem_gb": 16})
    assert snap is not None and snap["action_key"] == consumer

    auth, _ = authority
    gate = Path(auth.maintenance_path)
    errors: list = []
    finished = threading.Event()

    def holder_concludes():
        try:
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                if queue.item_path(pool.WITHDRAWN, consumer).exists():
                    break
                time.sleep(0.2)
            queue.finish(consumer, status="failed",
                         detail={"termination_reason": "resign-test"},
                         claim_snapshot=snap)
            finished.set()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    finisher = threading.Thread(target=holder_concludes, daemon=True)
    with mock.patch.object(worker_loop, "MAINTENANCE_GATE", gate), \
         mock.patch.object(worker_loop, "PARKED_ROOT",
                           gate.parent / "rollout" / "parked"):
        finisher.start()
        try:
            out = fm.resign(host, reason="staged drain",
                            queue_root=queue.root, gate=gate,
                            broker_call=_broker_call(authority),
                            live=[], wait_s=40.0)
        finally:
            finisher.join(timeout=10.0)
    assert not errors, errors
    assert finished.is_set()
    assert out["status"] == "resigned", (out.get("reason"), out.get("phase"))
    ready = json.loads(queue.item_path(pool.READY, consumer).read_text())
    assert ready["residency"] == block
    assert int(ready["attempts"]) == 1
    assert residency_plan.superseded(queue, plan) is None


def test_shape_only_cancellation_is_never_revived(
    queue: pool.PoolQueue, authority, tmp_path: Path, monkeypatch
) -> None:
    """An ordinary supervisor-shaped cancellation is not owed: the real
    resume_owed reports it retained (never revived), and JOIN keeps the
    fence on unknown rather than adopting or opening."""
    key = "7" * 64
    host = socket.gethostname()
    old_owner = f"{host}:supervisor-4194304:1"
    _sealed_shape(monkeypatch)
    _incarnation(monkeypatch)
    snapshot = _publish_claim(queue, key, max_attempts=3)
    queue.withdraw(key, reason="ordinary cancel", by=old_owner)
    queue.finish(key, status="failed",
                 detail={"termination_reason": "cancelled"},
                 claim_snapshot=snapshot)
    owed, skipped = fm.resume_owed(queue, host, f"{host}:supervisor-9:8")
    assert owed == []
    assert any("without handoff proof" in entry for entry in skipped), skipped
    auth, _ = authority
    gate = Path(auth.maintenance_path)
    auth.admin(0, {"op": "maintenance_begin", "reason": "run1",
                   "owner": old_owner})
    queue.ensure_layout()
    roster = _busy_roster(tmp_path, host, ["--class", "x86"], monkeypatch)
    monkeypatch.setattr(fm, "_mount_identity", lambda path: {
        "source": "dl380g10:/storage_pool/shared", "fstype": "nfs4",
        "mountpoint": "/mnt/shared"})
    out = fm.join(host, reason="too early", roster_path=roster,
                  queue_root=queue.root, gate=gate,
                  runtime_root=_busy_runtime(tmp_path),
                  broker_call=_broker_call(authority))
    assert out["status"] == "refused", out
    assert out["phase"] == "unsettled-unknown", out
    assert json.loads(gate.read_text())["draining"] is True
