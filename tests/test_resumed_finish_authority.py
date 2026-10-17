"""A real recovered finisher must not publish after its marker was retired."""
import json
import os
import select
import signal
import time

import pytest

from fleet_world import Clock
from prismabuild import pool, record_store
from test_lifecycle_recovery_crashes import KEY, _publication


def _resumed_finish_message(descriptor):
    assert select.select([descriptor], [], [], 15)[0], "finisher boundary did not respond"
    raw = bytearray()
    while not raw.endswith(b"\n"):
        part = os.read(descriptor, 65536)
        assert part, "finisher control closed before its boundary"
        raw.extend(part)
    return json.loads(raw)


def _resumed_finish_exit(child):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        waited, status = os.waitpid(child, os.WNOHANG)
        if waited:
            assert os.waitstatus_to_exitcode(status) == 0
            return
        time.sleep(0.01)
    pytest.fail("resumed finisher did not exit")


@pytest.mark.parametrize("latest_status", ["executed", "failed"])
def test_a_restored_finisher_cannot_replace_a_later_generations_terminal(
    tmp_path, monkeypatch, latest_status,
):
    clock = Clock(now=time.time() - 120)
    monkeypatch.setattr(pool, "_now", lambda: clock.now)
    queue = pool.PoolQueue(tmp_path / "queue")
    _publication(queue, tmp_path)
    first = queue.claim(owner="first", capacity={"cpu": 1})
    assert first is not None
    queue.finish(KEY, status="failed", detail={"stderr": "first"}, claim_snapshot=first)
    clock.advance(1)
    _publication(queue, tmp_path)
    middle = queue.claim(owner="middle", capacity={"cpu": 1})
    assert middle is not None
    go_read, go_write = os.pipe()
    event_read, event_write = os.pipe()
    child = os.fork()
    if child == 0:
        try:
            os.close(go_write)
            os.close(event_read)
            release = queue._release_reservation

            def released(*args, **kwargs):
                result = release(*args, **kwargs)
                marks = [str(path) for path in queue.records.finishing_paths()]
                os.write(event_write, json.dumps({"phase": "released", "marks": marks}).encode() + b"\n")
                assert os.read(go_read, 1) == b"F"
                return result

            queue._release_reservation = released
            result = queue.finish(KEY, status="executed", detail={"stdout": "middle"},
                                  claim_snapshot=middle)
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
        paused = _resumed_finish_message(event_read)
        assert paused["phase"] == "released" and len(paused["marks"]) == 1
        mark = queue.records.dir(record_store.CLAIMED) / os.path.basename(paused["marks"][0])
        assert mark.exists()
        assert not queue.records.item_path(record_store.CLAIMED, KEY).exists()
        assert not queue.records.lease_path(KEY).exists()
        assert queue.ledger().held() == {}
        clock.advance(120)
        assert queue.sweep_finish_tombstones(grace_s=60) == [KEY]
        restored = record_store.read_document(queue.records.item_path(record_store.CLAIMED, KEY))
        assert restored["published_unix"] == middle["published_unix"]
        assert not mark.exists()
        assert queue.reap_stale(timeout_s=-1) == [KEY]
        assert not queue.records.finishing_paths()
        assert not queue.records.item_path(record_store.CLAIMED, KEY).exists()
        clock.advance(1)
        _publication(queue, tmp_path)
        latest = queue.claim(owner="latest", capacity={"cpu": 1})
        assert latest is not None
        destination = queue.finish(KEY, status=latest_status, detail={"stdout": "latest"},
                                   claim_snapshot=latest)
        saved = destination.read_bytes()
        attempt = queue.records.attempt_path(latest, 1)
        saved_attempt = attempt.read_bytes()
        archives = {str(path): path.read_bytes() for path, _ in queue.records.superseded_records(KEY)}
        os.write(go_write, b"F")
        returned = _resumed_finish_message(event_read)
        assert returned["phase"] == "finished"
        _resumed_finish_exit(child)
        finished = True
        actual = record_store.read_document(destination)
        assert actual is not None, "resumed finisher unlinked the later terminal"
        assert actual["published_unix"] == latest["published_unix"], "resumed finisher replaced the later terminal"
        assert destination.read_bytes() == saved
        assert attempt.read_bytes() == saved_attempt
        assert {str(path): path.read_bytes() for path, _ in queue.records.superseded_records(KEY)} == archives
        assert returned["result"] == str(queue.records.attempt_path(middle, 1))
        assert queue.ledger().held() == {}
        assert not queue.records.finishing_paths()
    finally:
        os.close(go_write)
        os.close(event_read)
        if not finished:
            try:
                os.kill(child, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(child, 0)
