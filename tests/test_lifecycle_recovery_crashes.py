"""Kill a real finisher after durable effects, then retry the same queue.

Fault hooks call the real operation before exiting the child. The parent
checks filed bytes, immutable attempts, reservations and exact successor
ownership after reopening the queue. No final-state result is mocked.
"""
import json
import os
from pathlib import Path

import pytest

from prismabuild import pool


REGULAR_PHASES = ("attempt", "entomb", "lease", "reservation", "prior_archive",
                  "publication", "prior_unlink", "tombstone")
CANCEL_PHASES = ("attempt", "decision_proof", "entomb", "lease", "reservation", "tombstone")
KEY = "c" * 64


def _publication(queue, root):
    queue.publish(action_key=KEY, cas_root=root / "cas", checkout_root=root / "checkout",
                  worker_script=root / "worker.py", resources={"cpu": 1},
                  max_attempts=1)


def _history(root, cancelled):
    queue = pool.PoolQueue(root / "queue")
    if not cancelled:
        _publication(queue, root)
        prior = queue.claim(owner="prior", capacity={"cpu": 1})
        queue.finish(KEY, status="failed", detail={"stderr": "prior generation"}, claim_snapshot=prior)
    _publication(queue, root)
    item = queue.claim(owner="original", capacity={"cpu": 1})
    assert item is not None
    decision = None
    if cancelled:
        queue.withdraw(KEY, signal_child=False, by="operator", reason="cancel exact generation")
        marker = pool.record_store.read_document(queue.records.item_path(pool.record_store.WITHDRAWN, KEY))
        path = queue.records.withdrawal_decision_path(marker)
        decision = path, path.read_bytes()
    return queue, item, decision


def _interrupt_after(queue, item, root, phase, cancelled):
    reached = root / "interrupted-phase"
    child = os.fork()
    if child == 0:
        try:
            def stop():
                reached.write_text(phase)
                os._exit(91)

            archive = queue.archive_attempt
            entomb = queue._entomb_claim
            release = queue._release_reservation
            replace = pool.record_store.RecordStore.replace_record_at
            supersede = pool.record_store.RecordStore.file_superseded
            unlink = Path.unlink

            def archived(*args, **kwargs):
                result = archive(*args, **kwargs)
                if phase == "attempt":
                    stop()
                return result

            def entombed(*args, **kwargs):
                result = entomb(*args, **kwargs)
                if phase == "entomb" and result[1]:
                    stop()
                return result

            def released(*args, **kwargs):
                result = release(*args, **kwargs)
                if phase == "reservation":
                    stop()
                return result

            def replaced(store, path, record):
                result = replace(store, path, record)
                if ((phase == "publication" and path == store.item_path(pool.record_store.DONE, KEY))
                        or (phase == "decision_proof" and "withdrawn_attempt" in record)):
                    stop()
                return result

            def superseded(store, *args, **kwargs):
                result = supersede(store, *args, **kwargs)
                if phase == "prior_archive" and kwargs.get("kind") == "failed-terminal":
                    stop()
                return result

            def unlinked(path, *args, **kwargs):
                result = unlink(path, *args, **kwargs)
                if ((phase == "lease" and path == queue.records.lease_path(KEY))
                        or (phase == "prior_unlink" and path == queue.records.item_path(pool.record_store.FAILED, KEY))
                        or (phase == "tombstone" and path.name.endswith(pool.record_store.TOMBSTONE_SUFFIX))):
                    stop()
                return result

            queue.archive_attempt, queue._entomb_claim = archived, entombed
            queue._release_reservation = released
            pool.record_store.RecordStore.replace_record_at = replaced
            pool.record_store.RecordStore.file_superseded = superseded
            Path.unlink = unlinked
            queue.finish(KEY, status="withdrawn" if cancelled else "executed",
                         detail={"stdout": "completed payload", "returncode": 0}, claim_snapshot=item)
            os._exit(92)
        except BaseException:
            import traceback
            traceback.print_exc()
            os._exit(93)
    waited, status = os.waitpid(child, 0)
    assert waited == child and os.waitstatus_to_exitcode(status) == 91
    assert reached.read_text() == phase


def _recover(root, item, cancelled, *, reaper=False):
    restarted = pool.PoolQueue(root / "queue")
    restarted.sweep_finish_tombstones(grace_s=-1)
    if reaper:
        restarted.reap_stale(timeout_s=-1)
    else:
        restarted.finish(KEY, status="withdrawn" if cancelled else "executed",
                         detail={"stdout": "completed payload", "returncode": 0}, claim_snapshot=item)
    return restarted


def _assert_endpoint(queue, item, cancelled, decision):
    state = pool.record_store.WITHDRAWN if cancelled else pool.record_store.DONE
    record = pool.record_store.read_document(queue.records.item_path(state, KEY))
    assert record["published_unix"] == item["published_unix"]
    attempt_owner = record["withdrawn_attempt"] if cancelled else record
    outcomes = queue.attempt_outcomes(attempt_owner)
    assert len(outcomes) == 1
    assert outcomes[0]["status"] == ("withdrawn" if cancelled else "executed")
    assert outcomes[0]["stdout"] == "completed payload"
    if decision:
        assert decision[0].read_bytes() == decision[1]
        original = json.loads(decision[1])
        assert all(record[field] == value for field, value in original.items())
    assert queue.ledger().held() == {}
    assert queue.ledger().available() == {"cpu": 1}
    assert not queue.records.lease_path(KEY).exists()
    assert not list(queue.records.finishing_paths())
    assert all(not queue.records.item_path(other, KEY).exists()
               for other in (pool.record_store.CLAIMED, pool.record_store.READY, pool.record_store.FAILED))


@pytest.mark.parametrize("phase", REGULAR_PHASES)
def test_regular_finish_retries_after_each_durable_phase(tmp_path, phase):
    queue, item, decision = _history(tmp_path, False)
    _interrupt_after(queue, item, tmp_path, phase, False)
    restarted = _recover(tmp_path, item, False)
    _assert_endpoint(restarted, item, False, decision)


@pytest.mark.parametrize("phase", CANCEL_PHASES)
def test_cancelled_finish_retries_after_each_durable_phase(tmp_path, phase):
    queue, item, decision = _history(tmp_path, True)
    _interrupt_after(queue, item, tmp_path, phase, True)
    restarted = _recover(tmp_path, item, True)
    _assert_endpoint(restarted, item, True, decision)


def test_reaper_adopts_cancelled_execution_proof_before_removing_claim(tmp_path):
    queue, item, decision = _history(tmp_path, True)
    _interrupt_after(queue, item, tmp_path, "attempt", True)
    restarted = _recover(tmp_path, item, True, reaper=True)
    _assert_endpoint(restarted, item, True, decision)


def test_reaper_adopts_worker_terminal_and_retires_prior_generation(tmp_path):
    queue, item, decision = _history(tmp_path, False)
    _interrupt_after(queue, item, tmp_path, "entomb", False)
    restarted = _recover(tmp_path, item, False, reaper=True)
    _assert_endpoint(restarted, item, False, decision)


@pytest.mark.parametrize("cancelled", [False, True])
def test_late_retry_preserves_a_live_successor_after_interrupted_conclusion(tmp_path, cancelled):
    queue, item, decision = _history(tmp_path, cancelled)
    _interrupt_after(queue, item, tmp_path, "reservation", cancelled)
    restarted = _recover(tmp_path, item, cancelled)
    _publication(restarted, tmp_path)
    successor = restarted.claim(owner="successor", capacity={"cpu": 1})
    assert successor is not None
    paths = [restarted.records.item_path(pool.record_store.CLAIMED, KEY), restarted.records.lease_path(KEY)]
    before = [path.read_bytes() for path in paths]
    restarted.finish(KEY, status="withdrawn" if cancelled else "executed",
                     detail={"stdout": "late report"}, claim_snapshot=item)
    assert [path.read_bytes() for path in paths] == before
    assert restarted.ledger().held() == {"cpu": 1}
    assert restarted.ledger().held_keys() == [KEY]
