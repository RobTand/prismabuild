"""Review controls use real queue effects, then interrupt before conclusion."""

from admitted_queue_fixture import simulated_capacity
from contextlib import ExitStack
import json
import os
from pathlib import Path
import subprocess

import pytest

from prismabuild import pool, reader_lease, record_store, resource_scope
from test_lifecycle_recovery_crashes import KEY, _history, _interrupt_after, _publication
from test_lifecycle_real_scope import _closed, _owned_scope


def _new_claim(root):
    queue = pool.PoolQueue(root / "queue")
    queue.publish(action_key=KEY, cas_root=root / "cas", checkout_root=root / "checkout",
                  worker_script=root / "worker.py", resources={"cpu": 1},
                  retry_safe=True, max_attempts=2)
    item = queue.claim(owner="original", capacity=simulated_capacity({"cpu": 1}))
    assert item is not None
    return queue, item


def _archive_then_crash(queue, item, root, ending):
    reached = root / "archive-interrupted"
    child = os.fork()
    if child == 0:
        try:
            archive = queue.archive_attempt

            def interrupted(*args, **kwargs):
                archive(*args, **kwargs)
                reached.write_text(kwargs["status"])
                os._exit(91)

            queue.archive_attempt = interrupted
            if ending == "executed":
                queue.finish(KEY, status="executed", detail={"stdout": "actual worker result"},
                             claim_snapshot=item)
            else:
                queue.reap_stale(timeout_s=-1)
            os._exit(92)
        except BaseException:
            import traceback
            traceback.print_exc()
            os._exit(93)
    waited, status = os.waitpid(child, 0)
    assert waited == child and os.waitstatus_to_exitcode(status) == 91
    assert reached.read_text() == ending
    assert queue.records.item_path(record_store.CLAIMED, KEY).exists()


def _cancel_after_archive(queue, item, root, ending):
    _archive_then_crash(queue, item, root, ending)
    attempt = queue.records.attempt_path(item, 1)
    saved = {p: p.read_bytes() for p in attempt.parent.rglob("*") if p.is_file()}
    # A remote decision cannot conclude a live claim. The real reaper owns
    # containment and release after this immutable decision lands.
    queue.withdraw(KEY, signal_child=False, by="operator", reason="after immutable attempt")
    marker = queue.records.item_path(record_store.WITHDRAWN, KEY)
    decision = queue.records.withdrawal_decision_path(record_store.read_document(marker))
    saved.update({marker: marker.read_bytes(), decision: decision.read_bytes()})
    return saved


@pytest.mark.parametrize("ending", ["lease_lost", "executed"])
def test_withdrawn_reaper_releases_a_verified_prior_attempt(tmp_path, ending):
    queue, item = _new_claim(tmp_path)
    saved = _cancel_after_archive(queue, item, tmp_path, ending)
    for _ in range(3):
        queue = pool.PoolQueue(queue.root)
        assert queue.reap_stale(timeout_s=-1) == []
    assert not queue.records.item_path(record_store.CLAIMED, KEY).exists()
    assert not queue.records.lease_path(KEY).exists()
    assert queue.ledger().held() == {}
    assert not list(queue.records.finishing_paths())
    assert {p: p.read_bytes() for p in saved} == saved
    assert not queue.records.attempt_path(item, 2).exists()


@pytest.mark.parametrize("fault", ["unknown", "malformed", "wrong-generation"])
def test_withdrawn_reaper_retains_unverifiable_attempt(tmp_path, monkeypatch, fault):
    queue, item = _new_claim(tmp_path)
    _cancel_after_archive(queue, item, tmp_path, "executed")
    path = queue.records.attempt_path(item, 1)
    if fault != "unknown":
        value = json.loads(path.read_bytes())
        value["published_unix"] = -1
        mode = path.stat().st_mode
        path.chmod(0o600)
        path.write_text("{" if fault == "malformed" else json.dumps(value))
        path.chmod(mode)
    else:
        monkeypatch.setattr(queue.records, "attempt_presence", lambda *a: record_store.Unknown(
            "unavailable attempt", error=OSError("archive read fault")))
    before = [queue.records.item_path(record_store.CLAIMED, KEY).read_bytes(),
              queue.records.lease_path(KEY).read_bytes()]
    for _ in range(2):
        assert queue.reap_stale(timeout_s=-1) == []
    assert [queue.records.item_path(record_store.CLAIMED, KEY).read_bytes(),
            queue.records.lease_path(KEY).read_bytes()] == before
    assert queue.ledger().held() == {"cpu": 1}


@pytest.mark.parametrize("ending", ["lease_lost", "executed"])
def test_withdrawn_recovery_preserves_real_containment_and_live_successor(
    tmp_path, ending, monkeypatch, request, capsys,
):
    assert resource_scope.BROKER_SOCKET.exists(), "real PB broker required for this control"
    queue, item = _new_claim(tmp_path)
    with ExitStack() as stack:
        scope, process, _ = _owned_scope(queue, item, stack)
        assert process.poll() is None
        saved = _cancel_after_archive(queue, item, tmp_path, ending)
        stopped_claim = record_store.read_document(queue.records.item_path(record_store.CLAIMED, KEY))
        release = queue._release_reservation

        def checked_release(*args, **kwargs):
            proof = record_store.read_document(reader_lease.attestation_path(queue, KEY, scope.nonce))
            assert proof is not None
            assert proof["scope_id"] == scope.unit and proof["nonce"] == scope.nonce
            assert proof["scope_empty"] is True and proof["released"] is True
            assert proof["settled"] is True and proof["tickets_pending"] is False
            assert not scope.cgroup_path.exists()
            return release(*args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(queue, "_release_reservation", checked_release)
            queue.reap_stale(timeout_s=-1)
        assert not queue.records.item_path(record_store.CLAIMED, KEY).exists()
        assert queue.ledger().held() == {}
        assert {p: p.read_bytes() for p in saved} == saved
        _closed(stopped_claim, scope, process, queue, request, capsys)
        assert process.returncode != 0 and not scope.cgroup_path.exists()
        _publication(queue, tmp_path)
        successor = queue.claim(owner="live successor", capacity=simulated_capacity({"cpu": 1}))
        assert successor is not None
        new_scope, new_process, _ = _owned_scope(queue, successor, stack)
        paths = [queue.records.item_path(record_store.CLAIMED, KEY), queue.records.lease_path(KEY)]
        before = [p.read_bytes() for p in paths]
        for _ in range(2):
            assert queue.reap_stale(timeout_s=3600) == []
            queue.finish(KEY, status="executed", claim_snapshot=item)
        assert [p.read_bytes() for p in paths] == before
        assert queue.ledger().held() == {"cpu": 1}
        assert new_process.poll() is None
        assert queue.records.attempt_path(item, 1).read_bytes() == saved[queue.records.attempt_path(item, 1)]
        with capsys.disabled():
            print("review-recovery-scope-proof: " + json.dumps({
                "nodeid": request.node.nodeid, "old_scope_id": scope.unit,
                "successor_scope_id": new_scope.unit, "successor_alive": True,
                "proof": record_store.read_document(reader_lease.attestation_path(queue, KEY, scope.nonce)),
                "claim_and_lease_unchanged": True}), flush=True)
        result = queue.finish(KEY, status="executed", claim_snapshot=successor)
        _closed(record_store.read_document(result), new_scope, new_process, queue, request, capsys)


def test_claim_recovery_starts_with_documented_interpreter_and_no_pythonpath():
    root = Path(__file__).resolve().parents[1]
    env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONHOME"}}
    result = subprocess.run(["/usr/bin/python3", "tools/fleet/qualify_claim_recovery.py", "--help"],
                            cwd=root, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert "--late-status" in result.stdout and "--real-scope" in result.stdout
    refused = subprocess.run([
        "/usr/bin/python3", "tools/fleet/qualify_claim_recovery.py", "peer",
        "--root", "/tmp/c07-refused-control", "--late-status", "executed",
    ], cwd=root, env=env, capture_output=True, text=True, timeout=30)
    assert refused.returncode == 2
    assert "--root must be a fresh directory beneath /mnt/shared/pb-qualification" in refused.stderr


@pytest.mark.parametrize("fault,reason", [
    ("missing-archive", "superseded archive unavailable"),
    ("wrong-conclusion", "superseded archive does not match"),
])
def test_historical_retirement_reports_why_it_retains_authority(tmp_path, capsys, fault, reason):
    queue, item, _ = _history(tmp_path, False)
    _interrupt_after(queue, item, tmp_path, "publication", False)
    terminal = record_store.read_document(queue.records.item_path(record_store.DONE, KEY))
    path = Path(terminal["supersedes_terminal"]["superseded_path"])
    if fault == "missing-archive":
        path.unlink()
    else:
        value = record_store.read_document(path)
        value["superseded_by_published_unix"] = -1
        queue.records.replace_record_at(path, value)
    tombstones = list(queue.records.finishing_paths())
    prior = queue.records.item_path(record_store.FAILED, KEY)
    saved = prior.read_bytes()
    for _ in range(2):
        assert queue.sweep_finish_tombstones(grace_s=-1) == []
        assert prior.read_bytes() == saved
        assert list(queue.records.finishing_paths()) == tombstones
        diagnostic = capsys.readouterr().err
        assert KEY in diagnostic and reason in diagnostic
