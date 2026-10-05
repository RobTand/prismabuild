"""A cancellation keeps its decision and the worker's authenticated ending."""
from __future__ import annotations

import os
import signal
import threading
import json
import stat
import sys
from pathlib import Path

import pytest

from test_pool_resource_scope import scoped  # noqa: F401
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_gang_teardown_1517 import _both_claimed
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from prismabuild import pool, resource_scope

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun
import pbmcp
import pbstatus
import pbwait
import pool_reset
import stage_release


@pytest.fixture
def gang_broker(tmp_path_factory, monkeypatch, request):
    # The scope and gang fixtures each create checkout/, so keep their private
    # roots distinct while reusing the same broker fault-injection fixture.
    return scoped.__wrapped__(tmp_path_factory.mktemp("gang-broker"), monkeypatch, request)


def _start(queue, item, monkeypatch):
    # Reuse the broker fixture, but retain a positive export verdict as the
    # real worker does. No launcher, container or live broker is contacted.
    monkeypatch.setattr(resource_scope.ResourceScope, "export_stopped_verdict",
                        lambda scope: {"ok": True, "scope_id": scope.unit,
                                       "stopped": True, "empty": True,
                                       "released": True, "settled": True,
                                       "stop_reason": "withdrawn"})
    monkeypatch.setattr(queue, "_cleanup_action_containers",
                        lambda record: {"complete": True, "used": True,
                                        "removed": ["owned-container"], "remaining": []})
    queue._start_resource_scope(item)
    return pool._read_json(queue.item_path(pool.CLAIMED, item["action_key"]))


def _conclude(queue, item, monkeypatch):
    live = _start(queue, item, monkeypatch)
    key = live["action_key"]
    queue.withdraw(key, by="rob", reason="cancel this run", signal_child=False)
    path = queue.item_path(pool.WITHDRAWN, key)
    decision = pool._read_json(path)
    immutable = queue.withdrawal_decision_path(decision)
    decision_bytes = immutable.read_bytes()
    assert queue.finish(key, status="withdrawn", detail={"returncode": -15,
                        "stdout": "last output\n", "stderr": "stopped\n"},
                        claim_snapshot=live) == path
    record = pool._read_json(path)
    assert record["resource_scope_cleanup"]["export"]["stopped"] is True
    assert all(record[field] == value for field, value in decision.items()), "decision fields changed"
    assert immutable.read_bytes() == decision_bytes
    return queue, live, path, record


@pytest.mark.parametrize("retried", [False, True])
def test_running_withdrawal_retains_cleanup_and_immutable_attempt(scoped, monkeypatch, retried):
    queue, item, calls = scoped
    if retried:
        queue.finish(item["action_key"], status="failed", detail={"stderr": "prior failure"})
        item = queue.claim(capacity={"cpu": 1, "mem_gb": 2})
    queue, live, path, record = _conclude(queue, item, monkeypatch)
    cleanup = record["resource_scope_cleanup"]
    assert cleanup["nonce"] == record["resource_scope"]["nonce"] == live["resource_scope_intent"]["nonce"]
    assert all(cleanup["export"][field] is True for field in ("stopped", "empty", "released", "settled"))
    assert record["container_cleanup"]["removed"] == ["owned-container"]
    assert record["finished_host"] == pool.socket.gethostname()
    assert record["finished_unix"] >= live["claimed_unix"]
    retained = record["withdrawn_attempt"]
    outcomes = queue.attempt_outcomes(retained)
    assert [attempt["status"] for attempt in outcomes] == (["failed"] if retried else []) + ["withdrawn"]
    assert outcomes[-1]["disposition"] == pool.WITHDRAWN
    assert outcomes[-1]["detail"]["resource_scope_cleanup"] == cleanup
    assert outcomes[-1]["detail"]["resource_scope"] == live["resource_scope"]
    assert outcomes[-1]["detail"]["container_cleanup"] == record["container_cleanup"]
    assert outcomes[-1]["stdout"] == "last output\n"
    attempt_path = queue.attempt_path(retained, retained["attempts"])
    assert not attempt_path.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    before = path.read_bytes(), attempt_path.read_bytes()
    queue.finish(live["action_key"], status="withdrawn")
    queue.withdraw(live["action_key"], by="someone else", reason="second request", signal_child=False)
    assert (path.read_bytes(), attempt_path.read_bytes()) == before
    assert queue.ledger().held() == {}
    assert not queue.lease_path(live["action_key"]).exists()
    assert all(not queue.item_path(state, live["action_key"]).exists()
               for state in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED))


def test_running_gang_teardown_retains_the_withdrawn_siblings_cleanup(gang_broker, gang_fleet, monkeypatch, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, first, second = _both_claimed(queue, publish, finish, gclaim, members)
    # These are the fixture hosts' CPU indices, not this shard's mask. The
    # real subprocesses below still inherit PrismaBuild's admitted affinity.
    monkeypatch.setattr(pool.os, "sched_getaffinity", lambda pid: set(range(20)))
    fail = tmp_path / "fail-now"
    stop = tmp_path / "stop-now"
    launched = {}
    started = {key: threading.Event() for key in (first, second)}
    original_popen = pool.subprocess.Popen
    monkeypatch.setattr(pool.socket, "gethostname",
                        lambda: "sparklina" if threading.current_thread().name == "member-zero" else "sparky")
    monkeypatch.setattr(resource_scope.ResourceScope, "export_stopped_verdict",
                        lambda scope: {"ok": True, "scope_id": scope.unit, "stopped": True,
                                       "empty": True, "released": True, "settled": True,
                                       "tickets_pending": False})
    monkeypatch.setattr(queue, "_cleanup_action_containers",
                        lambda record: {"complete": True, "used": True, "removed": [], "remaining": []})

    def payload(scope, argv, **kwargs):
        gate = fail if scope.action_key == second else stop
        program = ("from pathlib import Path; import time\n"
                   + "while not Path(" + repr(str(gate)) + ").exists():\n time.sleep(0.01)\n"
                   + "raise SystemExit(" + ("1" if scope.action_key == second else "0") + ")")
        return [sys.executable, "-c", program, scope.action_key]

    def launch(argv, **kwargs):
        process = original_popen(argv, **kwargs)
        launched[argv[-1]] = process
        started[argv[-1]].set()
        return process

    monkeypatch.setattr(resource_scope.ResourceScope, "wrap_argv", payload)
    monkeypatch.setattr(pool.subprocess, "Popen", launch)
    outcomes, errors = {}, []
    # The admission fixture freezes wall time; isolate the signal ladder
    # (tested elsewhere) while stopping this exact real payload group.
    monkeypatch.setattr(pool, "terminate_action", lambda pid: os.killpg(pid, signal.SIGTERM))
    snapshots = {key: pool._read_json(queue.item_path(pool.CLAIMED, key)) for key in (first, second)}

    def execute(key):
        try:
            outcomes[key] = queue.execute(snapshots[key], containment=True, heartbeat_s=0.05)
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=execute, args=(key,), name=name)
               for key, name in ((first, "member-zero"), (second, "member-one"))]
    try:
        for thread in threads:
            thread.start()
        assert all(event.wait(10) for event in started.values()), errors
        # Both members are past the real start barrier: their payload processes
        # are launched and still running, not just claimed or barrier-waiting.
        assert all(process.poll() is None for process in launched.values())
        fail.touch()
        threads[1].join(10)
        assert not threads[1].is_alive() and not errors, errors
        queue.finish(second, status=outcomes[second]["status"], detail=outcomes[second],
                     claim_snapshot=snapshots[second])
        failed = pool._read_json(queue.item_path(pool.FAILED, second))
        assert failed["status"] == "failed"
        assert failed["resource_scope_cleanup"]["nonce"] == snapshots[second]["resource_scope"]["nonce"]
        assert failed["resource_scope_cleanup"]["export"]["stopped"] is True
        assert queue.attempt_outcomes(failed)[-1]["status"] == "failed"
        decision = pool._read_json(queue.item_path(pool.WITHDRAWN, first))
        assert decision["withdrawn_by"] == "gang:" + group
        threads[0].join(20)
        assert not threads[0].is_alive() and not errors, errors
        assert outcomes[first]["status"] == "withdrawn"
        monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
        queue.finish(first, status="withdrawn", detail=outcomes[first], claim_snapshot=snapshots[first])
        record = pool._read_json(queue.item_path(pool.WITHDRAWN, first))
        assert record["resource_scope_cleanup"]["nonce"] == snapshots[first]["resource_scope"]["nonce"]
        assert all(record[field] == value for field, value in decision.items())
        assert queue.attempt_outcomes(record["withdrawn_attempt"])[-1]["status"] == "withdrawn"
        assert record["finished_host"] == "sparklina"
        assert all(not queue.item_path(state, first).exists() for state in (pool.DONE, pool.FAILED, pool.READY))
    finally:
        fail.touch()
        stop.touch()
        for process in launched.values():
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
        for thread in threads:
            thread.join(10)


def test_ready_withdrawal_adds_no_worker_evidence(scoped):
    queue, item, calls = scoped
    # Publish a distinct generation that was never claimed.
    key = "c" * 64
    queue.publish(action_key=key, cas_root=item["cas_root"], checkout_root=item["checkout_root"],
                  worker_script=item["worker_script"], resources={"cpu": 1, "mem_gb": 1})
    queue.withdraw(key, by="rob", signal_child=False)
    record = pool._read_json(queue.item_path(pool.WITHDRAWN, key))
    assert record["withdrawn_from"] == pool.READY
    assert not any(field in record for field in ("resource_scope", "resource_scope_cleanup",
                   "container_cleanup", "finished_unix", "finished_host", "withdrawn_attempt"))
    assert not (queue.root / pool.ATTEMPTS / key).exists()


@pytest.mark.parametrize("reader", ["outcome_summary", "adopted_attempt_summary", "terminal_outcome_covers",
                                    "reclaim_terminal_reservation", "pool_reset", "pbstatus", "pbrun", "pbwait"])
def test_readers_accept_a_concluded_withdrawal(scoped, monkeypatch, reader):
    queue, item, calls = scoped
    queue, live, path, record = _conclude(queue, item, monkeypatch)
    key = live["action_key"]
    if reader == "outcome_summary":
        summary = pbrun.outcome_summary(queue, path, record)
        assert summary["status"] == "withdrawn" and summary["adopted"] is None
        assert summary["returncode"] is None
    elif reader == "adopted_attempt_summary":
        adopted = queue.adopted_attempt_summary(record["withdrawn_attempt"])
        assert adopted["status"] == adopted["disposition"] == "withdrawn"
    elif reader == "terminal_outcome_covers":
        assert queue.terminal_outcome_covers(record, action_key=key) is None
        assert queue.withdrawal_covers(record, action_key=key)["reason"] == "cancel this run"
    elif reader == "reclaim_terminal_reservation":
        with pytest.raises(pool.PoolContractError, match="terminal status is withdrawn/withdrawn"):
            queue.reclaim_terminal_reservation(key)
    elif reader == "pool_reset":
        plans, skipped = pool_reset.plan_resets(queue, cas_root=Path(item["cas_root"]))
        assert plans == skipped == []  # Only failed/ is eligible for reset.
    elif reader == "pbstatus":
        rows = [row for row in pbstatus.read_endings(queue.root, limit=10) if row["action_key"] == key]
        assert rows and all(row["status"] == "withdrawn" and row["unreadable"] is None for row in rows)
    elif reader == "pbrun":
        assert pbrun.await_outcome(queue, key, wait_s=1) == pbrun.WITHDRAWN_EXIT
    else:
        assert pbwait._from_record(queue, path, record)["status"] == "withdrawn"


def test_superseding_publication_does_not_gain_the_old_attempts_evidence(scoped, monkeypatch):
    queue, item, calls = scoped
    live = _start(queue, item, monkeypatch)
    key = item["action_key"]
    queue.withdraw(key, by="rob", reason="old generation", signal_child=False)
    decision_path = queue.withdrawal_decision_path(live)
    decision_bytes = decision_path.read_bytes()
    queue.publish(action_key=key, cas_root=item["cas_root"], checkout_root=item["checkout_root"],
                  worker_script=item["worker_script"], resources={"cpu": 1, "mem_gb": 2})
    queue.withdraw(key, by="second operator", reason="new generation", signal_child=False)
    new_path = queue.item_path(pool.WITHDRAWN, key)
    new_bytes = new_path.read_bytes()
    entomb = queue._entomb_claim
    monkeypatch.setattr(queue, "_entomb_claim", lambda *args, **kwargs: (None, False))
    queue.finish(key, status="withdrawn", claim_snapshot=live)
    monkeypatch.setattr(queue, "_entomb_claim", entomb)
    queue.finish(key, status="withdrawn", claim_snapshot=live)
    assert new_path.read_bytes() == new_bytes
    assert decision_path.read_bytes() == decision_bytes
    # Superseded cancellations retain the old worker's evidence outside the
    # current terminal slot; it may not revive a marker over the successor.
    retained = [pool._read_json(path) for path in queue.superseded_dir().glob(key + "*.json")]
    matching = [record for record in retained if record.get("withdrawn_attempt")]
    assert len(matching) == 1
    assert matching[0]["published_unix"] == live["published_unix"]
    assert matching[0]["resource_scope_cleanup"]["nonce"] == live["resource_scope"]["nonce"]


def _publish_successor(queue, item):
    # A replacement publication: a new generation of the same key, still READY.
    queue.publish(action_key=item["action_key"], cas_root=item["cas_root"],
                  checkout_root=item["checkout_root"], worker_script=item["worker_script"],
                  resources=dict(item["resources"]), max_attempts=2, retry_safe=True)


def test_late_withdrawn_finish_archives_the_withdrawn_disposition(scoped):
    """A cancelled attempt finishing after a successor was published is withdrawn/withdrawn."""
    queue, item, calls = scoped
    key = item["action_key"]
    queue.withdraw(key, by="rob", reason="cancel this run", signal_child=False)
    # The reaper concludes a withdrawal-covered claim without filing an
    # attempt, so this worker's finish becomes a late one.
    queue.reap_stale(timeout_s=-1)
    assert not queue.item_path(pool.CLAIMED, key).exists()
    decision_path = queue.item_path(pool.WITHDRAWN, key)
    decision = pool._read_json(decision_path)
    immutable = queue.withdrawal_decision_path(decision)
    immutable_bytes = immutable.read_bytes()
    _publish_successor(queue, item)
    # A re-submission retires the visible marker; the durable decision stays.
    assert not decision_path.exists()
    result = queue.finish(key, status="withdrawn", detail={"returncode": -15,
                          "stdout": "last output\n", "stderr": "stopped\n"},
                          claim_snapshot=item)
    assert result == queue.attempt_path(item, 1)
    archived = pool._read_json(result)
    assert archived["status"] == pool.WITHDRAWN
    assert archived["disposition"] == pool.WITHDRAWN
    # The verifying reader checks the attempt's identity, digests and adopted
    # summary, and reports no terminal: only cancellation owns this ending.
    assert queue.archived_generation_outcomes(key, generation=item["published_unix"]) == []
    assert immutable.read_bytes() == immutable_bytes
    assert queue.item_path(pool.READY, key).exists()
    assert not any(queue.item_path(state, key).exists()
                   for state in (pool.CLAIMED, pool.DONE, pool.FAILED))
    assert queue.ledger().held() == {}


def test_late_withdrawn_finish_after_a_charged_retry_reads_back_withdrawn(scoped):
    """The charged retry's slot cannot turn a late cancellation into a failure."""
    queue, item, calls = scoped
    key = item["action_key"]
    queue.finish(key, status="failed", detail={"stderr": "prior failure"})
    item = queue.claim(capacity={"cpu": 1, "mem_gb": 2})
    assert item["attempts"] == 1
    queue.withdraw(key, by="rob", reason="cancel this run", signal_child=False)
    queue.reap_stale(timeout_s=-1)
    _publish_successor(queue, item)
    result = queue.finish(key, status="withdrawn", detail={"returncode": -15},
                          claim_snapshot=item)
    assert result == queue.attempt_path(item, 2)
    archived = pool._read_json(result)
    assert archived["status"] == pool.WITHDRAWN
    assert archived["disposition"] == pool.WITHDRAWN
    # With a charged retry this attempt was on the failure ladder pre-#1537:
    # the verifying reader refused the whole generation as a conflicting
    # disposition instead of answering the cancellation.
    assert queue.archived_generation_outcomes(key, generation=item["published_unix"]) == []
    assert queue.ledger().held() == {}


def test_late_scope_withdrawn_finish_archives_the_withdrawn_disposition(scoped, monkeypatch):
    """The exact-scope late-finish recovery keeps the withdrawn disposition too."""
    queue, item, calls = scoped
    live = _start(queue, item, monkeypatch)
    key = live["action_key"]
    queue.withdraw(key, by="rob", reason="cancel this run", signal_child=False)
    queue.reap_stale(timeout_s=-1)
    _publish_successor(queue, item)
    result = queue.finish(key, status="withdrawn", detail={"returncode": -15,
                          "stdout": "last output\n", "stderr": "stopped\n"},
                          claim_snapshot=live)
    assert result == queue.attempt_path(live, 1)
    archived = pool._read_json(result)
    assert archived["status"] == pool.WITHDRAWN
    assert archived["disposition"] == pool.WITHDRAWN
    assert archived["detail"]["resource_scope_cleanup"]["nonce"] == live["resource_scope"]["nonce"]
    assert not list(queue.dir(pool.CLAIMED).glob(f"{key}.*{pool.LATE_FINISH_SUFFIX}"))
    retained = [pool._read_json(path) for path
                in queue.superseded_dir().glob(f"{key}.*.late-finish.json")]
    assert len(retained) == 1 and retained[0]["status"] == pool.WITHDRAWN
    assert queue.archived_generation_outcomes(key, generation=live["published_unix"]) == []


@pytest.mark.parametrize("view", ["pbwait_json", "pb_action", "pb_receipts", "pb_log"])
def test_public_views_expose_a_stopped_withdrawn_attempt(scoped, monkeypatch, capsys, view):
    queue, item, calls = scoped
    queue, live, path, record = _conclude(queue, item, monkeypatch)
    key = live["action_key"]
    session = pbmcp.Session(queue_root=queue.root, cas_root=Path(item["cas_root"]), repo_link=queue.root / "repo")
    if view == "pbwait_json":
        monkeypatch.setattr(pbrun, "SH", queue.root.parent)
        root = queue.root.parent / "pb-queue"
        queue.root.rename(root)
        queue.root = root
        assert pbwait.main(["--json", "--wait-s", "0", key]) != 0
        body = json.loads(capsys.readouterr().out)[0]
        assert body["status"] == "withdrawn" and body["succeeded"] is False, body
    elif view == "pb_action":
        body = session.call("pb_action", {"key_prefix": key})
        assert body["adopted_attempt"] is None
        assert body["attempts_detail"][-1]["status"] == "withdrawn"
        assert body["receipt"]["present"] is False
        body = body["outcome"]
    elif view == "pb_receipts":
        body = session.call("pb_receipts", {"keys": [key]})["receipts"][0]
        assert body["verdict"] == "withdrawn" and body["receipt"]["present"] is False
    else:
        body = session.call("pb_log", {"key_prefix": key, "stream": "stdout"})
        assert body["state"] == "withdrawn"
        assert body["log"]["lines"] == ["last output"]
        return
    assert body["withdrawn_attempt"]["status"] == "withdrawn"
    assert body["resource_scope_cleanup"] == record["resource_scope_cleanup"]
    assert body["resource_scope_cleanup"]["nonce"] == live["resource_scope"]["nonce"]


def test_stage_release_accepts_only_additive_completed_withdrawal(scoped, monkeypatch):
    queue, item, calls = scoped
    queue, live, path, record = _conclude(queue, item, monkeypatch)
    key = live["action_key"]
    assert stage_release._require_exact_withdrawal(queue, key) == record
    for changed in ({**record, "reason": "forged"},
                    {**record, "finished_unix": -1},
                    {**record, "unattested_field": True}):
        path.write_text(json.dumps(changed))
        with pytest.raises(pool.PoolContractError, match="withdrawal lacks its exact immutable decision"):
            stage_release._require_exact_withdrawal(queue, key)
    path.write_text(json.dumps(record))
    assert stage_release._require_exact_withdrawal(queue, key) == record
