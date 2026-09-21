"""Independent maintenance review: archive gaps and real reconciliation records."""
from test_pbrun_recovers_overwritten_generation import _publish, _run, KEY, queue
from test_pool_broker_disconnect import make_failed_attempt
from prismabuild import pool
from prismabuild.pool_reconcile import reconcile
import pytest


def test_missing_first_attempt_is_not_inferred_as_authorized_history(queue):
    generation = _publish(queue, max_attempts=2, retry_safe=True)
    _run(queue, status="failed", returncode=7, stdout="first attempt\n")
    _run(queue, status="executed", returncode=0, stdout="retry completed\n")
    queue.attempt_path({"action_key": KEY, "published_unix": generation}, 1).unlink()
    with pytest.raises(pool.PoolContractError):
        queue.archived_generation_outcomes(KEY, generation=generation)


def test_real_receipt_reconciliation_is_not_a_numbered_attempt(tmp_path, monkeypatch):
    q, cas, action, ending = make_failed_attempt(tmp_path, monkeypatch)
    key = action["action_key"]
    supplement = reconcile(q, key, cas=cas,
                           generation=q.attempt_generation(ending),
                           attempt=ending["attempts"])
    assert supplement["payload_status"] == "verified"
    records = q.archived_generation_outcomes(key, generation=ending["published_unix"])
    assert len(records) == 1
    assert records[0][1]["status"] == "failed"
    assert records[0][1]["detail"]["returncode"] == 125
