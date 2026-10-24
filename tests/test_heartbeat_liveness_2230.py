"""Attempt-bound heartbeats survive contention without borrowing another lease."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from pathlib import Path
import subprocess
import json
import sys
import threading

import pytest

from prismabuild import census, claim_identity, pool, record_store
from test_pool_transition_lock import publish
from test_checkout_recovery_1465 import case, make_case  # noqa: F401
from prismabuild import checkout_recovery


@pytest.fixture
def holder(tmp_path, monkeypatch):
    clock = [2_000_000_000.]
    monkeypatch.setattr(pool, "_now", lambda: clock[0])
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    publish(queue, key)
    item = queue.claim(owner="worker", capacity={"cpu": 2})
    assert item
    queue.write_lease(key, owner="worker", claim_snapshot=item,
                      progress_observation={"accepted_count": 0, "staged_wait_exempt_s": 0.,
                                            "quiet_s": 0., "grace_s": 720.})
    return queue, key, item, clock


@contextmanager
def _busy_key(queue, key):
    acquired, release = threading.Event(), threading.Event()
    def hold():
        with queue.action_transition_lock(key):
            acquired.set()
            assert release.wait(60), "test did not release its key lock"
    thread = threading.Thread(target=hold)
    thread.start()
    assert acquired.wait(60)
    try:
        yield
    finally:
        release.set()
        thread.join(60)
        assert not thread.is_alive()


@pytest.mark.parametrize("judge", ["reaper", "widow", "mover", "order"])
def test_twelve_contended_beats_keep_every_judge_live(holder, judge):
    # r7 N2 / r8 liveness and N3, without the later lifetime/certificate layer.
    queue, key, item, clock = holder
    path = queue.records.lease_path(key)
    original = path.read_bytes()
    start = clock[0]
    def beats():
        for step in range(12):
            clock[0] = start + (step + 1) * pool.HEARTBEAT_S
            queue.write_lease(key, owner="worker", claim_snapshot=item)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with _busy_key(queue, key):
            executor.submit(beats).result(timeout=60)
            assert path.read_bytes() == original
    if judge == "reaper":
        assert queue.lease_age(key) == 0.
        assert queue.reap_stale() == []
        assert queue.records.item_path(record_store.CLAIMED, key).exists()
    elif judge == "widow":
        queue.records.item_path(record_store.CLAIMED, key).unlink()
        assert queue.sweep_widowed_leases() == []
        assert path.exists() and queue.ledger().holder_tokens(key) == {"cpu": 1}
    elif judge == "mover":
        assert queue._mover_lease(key, item, now=clock[0])["lease_live"] is True
    else:
        live, evidence = queue._claim_order_ahead_live(key, now=clock[0], window_s=60.,
            prior={"evidence_unix": start, "accepted_count": 0, "staged_wait_exempt_s": 0.})
        assert live and evidence["evidence"] == "ahead-within-grace"
        assert evidence["heartbeat_age_s"] == 0.
        assert evidence["evidence_unix"] == clock[0]


@pytest.mark.parametrize("contended,point", [
    (False, "initial-claim"), (False, "process-identity"),
    (False, "lease-read"), (False, "final-claim"), (False, "publish"),
    (True, "lease-read"), (True, "final-claim"),
    (True, "process-identity"), (True, "publish"),
])
def test_gated_heartbeat_io_never_requeues_or_admits_a_second_run(holder, monkeypatch, contended, point):
    """Control every heartbeat I/O boundary, then judge without lock contention."""
    queue, key, item, clock = holder
    path = queue.records.lease_path(key)
    before = path.read_bytes()
    entered, resume = threading.Event(), threading.Event()
    local = threading.local()
    read = record_store.read_document
    initial = queue.records.item
    ticks = pool._contained_worker_start_ticks
    replace = queue.records.replace_record_at
    def gate(at):
        if threading.current_thread().name == "gated-beat" and at == point and not entered.is_set():
            entered.set()
            assert resume.wait(60), "heartbeat I/O gate was not released"
    def read_claim(*args, **kwargs):
        gate("initial-claim")
        local.initial = True
        try:
            return initial(*args, **kwargs)
        finally:
            local.initial = False
    def read_record(target):
        if target == path:
            gate("lease-read")
        elif target == queue.records.item_path(record_store.CLAIMED, key) and not getattr(local, "initial", False):
            gate("final-claim")
        return read(target)
    def read_identity(pid):
        gate("process-identity")
        return ticks(pid)
    def publish_record(target, payload):
        if target == path or "lease-heartbeats" in Path(target).parts:
            gate("publish")
        return replace(target, payload)
    monkeypatch.setattr(queue.records, "item", read_claim)
    monkeypatch.setattr(record_store, "read_document", read_record)
    monkeypatch.setattr(pool, "_contained_worker_start_ticks", read_identity)
    monkeypatch.setattr(queue.records, "replace_record_at", publish_record)
    errors = []
    start = clock[0]
    def beats():
        try:
            for step in range(12):
                clock[0] = start + (step + 1) * pool.HEARTBEAT_S
                queue.write_lease(key, owner="worker", claim_snapshot=item)
        except BaseException as exc:
            errors.append(exc)
    with _busy_key(queue, key) if contended else nullcontext():
        thread = threading.Thread(target=beats, name="gated-beat")
        thread.start()
        try:
            assert entered.wait(60), f"heartbeat did not reach {point}"
            with queue.action_transition_lock(key, blocking=False) as acquired:
                assert acquired is False
            clock[0] += 1
        finally:
            resume.set()
            thread.join(60)
        assert not thread.is_alive() and not errors
    # No competing lock remains to make the judges skip the claim.
    assert queue.lease_age(key) == 0.
    assert queue.reap_stale() == []
    assert queue.claim(owner="successor", capacity={"cpu": 2}) is None
    current = record_store.value_or_raise(queue.records.item(record_store.CLAIMED, key))
    assert current["claimed_by"] == "worker" and current["attempts"] == 0
    assert queue.ledger().holder_tokens(key) == {"cpu": 1}
    if contended:
        assert path.read_bytes() == before


@pytest.mark.parametrize("field", ["claimed_by", "claimed_unix", "published_unix", "attempts",
                                    "host", "pid", "pid_start_ticks", "action_key"])
def test_foreign_stamp_never_renews_another_incarnation(holder, field):
    queue, key, item, clock = holder
    lease = record_store.read_document(queue.records.lease_path(key))
    stamp = dict(lease)
    clock[0] += pool.LEASE_TIMEOUT_S + 1
    stamp["heartbeat_unix"] = clock[0]
    stamp[field] = "foreign"
    queue.records.replace_record_at(queue._heartbeat_stamp_path(lease), stamp)
    assert queue.lease_age(key) > pool.LEASE_TIMEOUT_S
    assert not queue._mover_lease(key, item, now=clock[0])["lease_live"]


@pytest.mark.parametrize("value", [None, True, "100", float("nan"), float("inf"), 10**400, "excessive-skew"])
def test_invalid_stamp_never_extends_liveness(holder, value):
    queue, key, item, clock = holder
    lease = record_store.read_document(queue.records.lease_path(key))
    clock[0] += pool.LEASE_TIMEOUT_S + 1
    stamp = dict(lease, heartbeat_unix=(clock[0] + pool.OFFER_FUTURE_TOLERANCE_S + 1
                                      if value == "excessive-skew" else value))
    path = queue._heartbeat_stamp_path(lease)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(stamp))
    assert queue.lease_age(key) > pool.LEASE_TIMEOUT_S


@pytest.mark.parametrize("value", [None, True, "100", float("nan"), float("inf"), 10**400])
def test_invalid_lease_time_is_unknown_to_every_judge(holder, value):
    queue, key, item, clock = holder
    lease = record_store.read_document(queue.records.lease_path(key))
    lease["heartbeat_unix"] = value
    queue.records.lease_path(key).write_text(json.dumps(lease))
    with pytest.raises(pool.PoolContractError, match="heartbeat"):
        queue.lease_age(key)
    assert queue._mover_lease(key, item, now=clock[0]) is None
    assert queue._ahead_within_grace(queue._effective_heartbeat(lease), {"quiet_s": 0, "grace_s": 720}) is None
    queue.records.item_path(record_store.CLAIMED, key).unlink()
    assert queue.sweep_widowed_leases() == []
    assert queue.ledger().holder_tokens(key) == {"cpu": 1}


def test_delayed_predecessor_stamp_cannot_refresh_a_successor(holder):
    queue, key, first, clock = holder
    old = record_store.read_document(queue.records.lease_path(key))
    queue.finish(key, status="executed", claim_snapshot=first)
    clock[0] += 1
    publish(queue, key)
    second = queue.claim(owner="worker", capacity={"cpu": 2})
    before = queue.records.lease_path(key).read_bytes()
    clock[0] += pool.LEASE_TIMEOUT_S + 1
    queue.records.replace_record_at(queue._heartbeat_stamp_path(old), dict(old, heartbeat_unix=clock[0]))
    assert queue.lease_age(key) > pool.LEASE_TIMEOUT_S
    with _busy_key(queue, key):
        with pytest.raises(pool.PoolContractError, match="changed"):
            queue.write_lease(key, owner="worker", claim_snapshot=first)
    assert queue.records.lease_path(key).read_bytes() == before
    queue.write_lease(key, owner="worker", claim_snapshot=second)
    assert queue.lease_age(key) == 0.


@pytest.mark.parametrize("landing", ["claim", "lease"])
def test_cross_thread_predecessor_publish_excludes_successor(holder, monkeypatch, landing):
    # Corrected r6 R3 / r7 probe: successor is a different thread.
    queue, key, first, clock = holder
    entered, resume, tested, proceed = (threading.Event() for _ in range(4))
    original = queue.records.replace_record_at
    def stall(path, payload):
        if Path(path) == queue.records.lease_path(key) and threading.current_thread().name == "beat":
            entered.set()
            assert resume.wait(60), "heartbeat gate was not released"
        return original(path, payload)
    monkeypatch.setattr(queue.records, "replace_record_at", stall)
    errors, exclusion = [], []
    def beat():
        try:
            queue.write_lease(key, owner="worker", claim_snapshot=first)
        except BaseException as exc:
            errors.append(exc)
    def finish():
        try:
            with queue.action_transition_lock(key, blocking=False) as acquired:
                exclusion.append(acquired)
            tested.set()
            assert proceed.wait(60), "successor gate was not released"
            queue.finish(key, status="executed", claim_snapshot=first)
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=beat, name="beat")
    successor = threading.Thread(target=finish)
    thread.start()
    try:
        assert entered.wait(60), "heartbeat did not reach publication"
        successor.start()
        assert tested.wait(60), "competitor did not test the real flock"
        assert exclusion == [False]
    finally:
        resume.set()
        thread.join(60)
        proceed.set()
        if successor.ident is not None:
            successor.join(60)
    assert not errors and not thread.is_alive() and not successor.is_alive()
    clock[0] += 1
    publish(queue, key)
    second = queue.claim(owner="worker", capacity={"cpu": 2})
    if landing == "lease":queue.write_lease(key, owner="worker", claim_snapshot=second)
    before = queue.records.lease_path(key).read_bytes()
    with pytest.raises(pool.PoolContractError, match="changed"):
        queue.write_lease(key, owner="worker", claim_snapshot=first)
    assert queue.records.lease_path(key).read_bytes() == before


@pytest.mark.parametrize("host,alive,observed,age,due", [
    ("local", True, 42, 0., False), ("local", False, None, 0., True),
    ("local", True, 43, 0., True), ("remote", False, None, 0., False),
    ("remote", True, 42, 10000., True), ("local", True, None, 0., False),
])
def test_cleanup_detection_requires_age_or_exact_process_death(holder, monkeypatch, host, alive, observed, age, due):
    queue, key, item, clock = holder
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "local")
    monkeypatch.setattr(pool, "_process_alive", lambda pid: alive)
    monkeypatch.setattr(pool, "_contained_worker_start_ticks", lambda pid: observed)
    lease = {"action_key":key, "pid":123, "pid_start_ticks":42, "host":host,
             "heartbeat_unix":clock[0] - age}
    assert queue._heartbeat_cleanup_due(lease) is due
    if age == 0.:
        del lease["pid_start_ticks"]
        assert not queue._heartbeat_cleanup_due(lease)


def test_actually_dead_owner_does_not_receive_infinite_stamp_credit(holder):
    queue, key, item, clock = holder
    child = subprocess.Popen([sys.executable, "-c", "import sys; sys.stdin.buffer.read()"], stdin=subprocess.PIPE)
    try:
        lease = record_store.read_document(queue.records.lease_path(key))
        lease.update(pid=child.pid, pid_start_ticks=pool._contained_worker_start_ticks(child.pid))
        queue.records.replace_record_at(queue.records.lease_path(key), lease)
        queue.records.replace_record_at(queue._heartbeat_stamp_path(lease), lease)
        child.stdin.close();child.wait(timeout=60)
        assert queue._heartbeat_cleanup_due(lease)
        assert queue.reap_stale() == []
        assert queue.ledger().holder_tokens(key) == {"cpu": 1}
        clock[0] += pool.LEASE_TIMEOUT_S
        assert queue.reap_stale() == []
        assert queue.records.item_path(record_store.CLAIMED, key).exists()
        clock[0] += 1
        assert queue.reap_stale() == [key]
        assert not queue.ledger().holder_tokens(key)
    finally:
        if child.poll() is None:child.kill();child.wait(timeout=60)


def test_checkout_recovery_refuses_a_contended_live_terminal_lease(case, monkeypatch):
    monkeypatch.setattr(pool, "_now", lambda: 2_000_000_000.)
    queue, key = case.queue.queue, case.key
    lease = {**case.claim, "schema": pool.POOL_LEASE_SCHEMA_V1, "action_key": key,
             "owner": case.claim["claimed_by"], "host": case.claim["claimed_host"],
             "pid": 99999999, "pid_start_ticks": 42, "child_pid": None,
             "heartbeat_unix": pool._now() - pool.LEASE_TIMEOUT_S - 60}
    queue.records.replace_record_at(queue.records.lease_path(key), lease)
    stamp = dict(lease, heartbeat_unix=pool._now())
    queue.records.replace_record_at(queue._heartbeat_stamp_path(lease), stamp)
    with pytest.raises(ValueError, match="lease.*live"):
        checkout_recovery._checkout_recovery_terminal(queue, key, case.proc_root)
    assert case.directory.exists()


def test_final_lease_read_cannot_borrow_successor_identity(holder, monkeypatch):
    queue, key, item, clock = holder
    path = queue.records.lease_path(key)
    before = path.read_bytes()
    original = record_store.read_document
    reads = []
    def changed(path):
        value = original(path)
        if path == queue.records.lease_path(key):
            reads.append(path)
            if len(reads) == 1:
                return dict(value, attempts=value["attempts"] + 1)
        return value
    monkeypatch.setattr(record_store, "read_document", changed)
    with pytest.raises(pool.AmbiguousClaimHolder, match="lease"):
        queue.write_lease(key, owner="worker", claim_snapshot=item)
    assert path.read_bytes() == before
    assert len(reads) == 1


def test_heartbeat_readers_use_the_shared_observation():
    import ast
    root = Path(__file__).resolve().parents[1] / "src" / "prismabuild"
    found = set()
    for name in ("pool", "lifecycle", "checkout_recovery", "census"):
        class Reads(ast.NodeVisitor):
            owner = None
            def visit_FunctionDef(self, node):
                prior = self.owner
                self.owner = node.name
                self.generic_visit(node)
                self.owner = prior
            def visit_Call(self, node):
                if (isinstance(node.func, ast.Attribute) and node.func.attr == "get" and node.args
                        and isinstance(node.args[0], ast.Constant) and node.args[0].value == "heartbeat_unix"):
                    found.add((name, self.owner))
                self.generic_visit(node)
            def visit_Subscript(self, node):
                if isinstance(node.slice, ast.Constant) and node.slice.value == "heartbeat_unix":
                    found.add((name, self.owner))
                self.generic_visit(node)
        Reads().visit(ast.parse((root / (name + ".py")).read_text()))
    # Execution samples retain their own age even when a liveness stamp renews.
    assert found == {("pool", "_effective_heartbeat"), ("census", "_execution_observation")}


def test_status_census_reports_the_contended_heartbeat(holder, monkeypatch):
    queue, key, item, clock = holder
    def beat():
        clock[0] += pool.LEASE_TIMEOUT_S + 1
        queue.write_lease(key, owner="worker", claim_snapshot=item)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with _busy_key(queue, key):
            executor.submit(beat).result(timeout=60)
    monkeypatch.setattr(census.time, "time", lambda: clock[0])
    result = census.action_rows(queue.root)
    row = next(row for row in result.value["jobs"] if row["action_key"] == key)
    assert row["stale"] is False and row["lease_age_s"] == 0.


@pytest.mark.parametrize("judge", ["mover", "order"])
@pytest.mark.parametrize("field,value", [("owner", "foreign"), ("host", "foreign"), ("attempts", 999)])
def test_fresh_foreign_lease_does_not_renew_this_claim(holder, judge, field, value):
    queue, key, item, clock = holder
    lease = record_store.read_document(queue.records.lease_path(key))
    lease[field] = value
    queue.records.replace_record_at(queue.records.lease_path(key), lease)
    if judge == "mover":
        assert queue._mover_lease(key, item, now=clock[0]) is None
    else:
        live, evidence = queue._claim_order_ahead_live(key, now=clock[0], window_s=60.,
            prior={"evidence_unix": clock[0] - 120, "accepted_count": 0, "staged_wait_exempt_s": 0.})
        assert not live and evidence["evidence"] == "unread"


def test_partial_legacy_claim_keeps_its_lease_format_and_cannot_stamp(tmp_path, monkeypatch):
    monkeypatch.setattr(pool, "_now", lambda: 2_000_000_000.)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    key = "a" * 64
    queue.records.replace_record_at(queue.records.item_path(record_store.CLAIMED, key),
        {"action_key": key, "claimed_by": "legacy", "claimed_host": pool.socket.gethostname()})
    queue.write_lease(key, owner="legacy")
    path = queue.records.lease_path(key)
    before = path.read_bytes()
    lease = record_store.read_document(path)
    assert set(lease) == {"schema", "action_key", "owner", "host", "pid", "child_pid", "heartbeat_unix"}
    with _busy_key(queue, key):
        queue.write_lease(key, owner="legacy")
    assert not queue._heartbeat_stamp_path(lease).exists()
    assert path.read_bytes() == before
    assert queue.lease_age(key) == 0.
