"""A recovered retry finisher cannot reset a later claim's attempt history."""

from admitted_queue_fixture import simulated_admission

from admitted_queue_fixture import simulated_capacity
from fleet_world import Clock as FleetClock
import json
import os
import signal
import time

import pytest

from fleet_world import Clock
from prismabuild import pool, record_store
from test_lifecycle_recovery_crashes import KEY
from test_resumed_finish_authority import _resumed_finish_exit, _resumed_finish_message


@pytest.mark.parametrize("prior_attempts", [0, 1])
def test_a_resumed_retry_preserves_the_later_attempt_counter(tmp_path, monkeypatch, prior_attempts):
    clock = Clock(now=time.time() - 120)
    FleetClock(source=lambda: clock.now).install(monkeypatch, scope="pool")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key=KEY, cas_root=tmp_path / "cas", checkout_root=tmp_path / "checkout",
                  worker_script=tmp_path / "worker.py", resources={"cpu": 1},
                  retry_safe=True, max_attempts=4)
    for index in range(prior_attempts):
        previous = queue.claim(owner=f"previous-{index}", **simulated_admission(queue, simulated_capacity({"cpu": 1})))
        assert previous is not None
        queue.finish(KEY, status="failed", detail={"stderr": f"previous-{index}"}, claim_snapshot=previous)
    original = queue.claim(owner="paused retry", **simulated_admission(queue, simulated_capacity({"cpu": 1})))
    assert original is not None and original["attempts"] == prior_attempts
    go_read, go_write = os.pipe()
    event_read, event_write = os.pipe()
    child = os.fork()
    if child == 0:
        try:
            os.close(go_write)
            os.close(event_read)
            release = queue._release_reservation

            def returned_reservation(*args, **kwargs):
                value = release(*args, **kwargs)
                os.write(event_write, b'{"phase":"released"}\n')
                assert os.read(go_read, 1) == b"R"
                return value

            queue._release_reservation = returned_reservation
            result = queue.finish(KEY, status="failed", detail={"stderr": "paused retry"},
                                  claim_snapshot=original)
            os.write(event_write, json.dumps({"phase": "finished", "result": str(result)}).encode() + b"\n")
            os._exit(0)
        except BaseException:
            import traceback
            traceback.print_exc()
            os._exit(94)
    os.close(go_read)
    os.close(event_write)
    finished = False
    try:
        assert _resumed_finish_message(event_read)["phase"] == "released"
        clock.advance(120)
        assert queue.sweep_finish_tombstones(grace_s=60) == [KEY]
        assert queue.reap_stale(timeout_s=-1) == [KEY]
        ready = queue.records.ready_publication(KEY)
        assert ready["attempts"] == prior_attempts + 1
        current = queue.claim(owner="later retry", **simulated_admission(queue, simulated_capacity({"cpu": 1})))
        assert current is not None and current["attempts"] == prior_attempts + 1
        claim_path = queue.records.item_path(record_store.CLAIMED, KEY)
        lease_path = queue.records.lease_path(KEY)
        saved_live = claim_path.read_bytes(), lease_path.read_bytes()
        archive = queue.archive_attempt

        def archive_before_resuming(*args, **kwargs):
            nonlocal finished
            result = archive(*args, **kwargs)
            assert kwargs["attempt"] == prior_attempts + 2
            os.write(go_write, b"R")
            assert _resumed_finish_message(event_read)["phase"] == "finished"
            _resumed_finish_exit(child)
            finished = True
            assert (claim_path.read_bytes(), lease_path.read_bytes()) == saved_live
            return result

        with monkeypatch.context() as patch:
            patch.setattr(queue, "archive_attempt", archive_before_resuming)
            queue.finish(KEY, status="failed", detail={"stderr": "later retry"}, claim_snapshot=current)
        ready = queue.records.ready_publication(KEY)
        assert ready["attempts"] == prior_attempts + 2, "resumed retry reset the attempt counter"
        attempt = queue.records.attempt_path(current, prior_attempts + 2)
        saved_attempt = attempt.read_bytes()
        next_claim = queue.claim(owner="next retry", **simulated_admission(queue, simulated_capacity({"cpu": 1})))
        assert next_claim is not None and next_claim["attempts"] == prior_attempts + 2
        terminal = queue.finish(KEY, status="executed", detail={"stdout": "new execution"},
                                claim_snapshot=next_claim)
        result = record_store.read_document(terminal)
        assert result["attempts"] == prior_attempts + 3 and result["status"] == "executed"
        assert queue.records.attempt_path(next_claim, prior_attempts + 3).is_file()
        assert attempt.read_bytes() == saved_attempt
        assert len(queue.attempt_outcomes(result)) == prior_attempts + 3
        assert not queue.records.finishing_paths() and queue.ledger().held() == {}
    finally:
        os.close(go_write)
        os.close(event_read)
        if not finished:
            try:
                os.kill(child, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(child, 0)
