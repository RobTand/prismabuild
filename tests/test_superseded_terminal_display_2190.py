from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools/fleet")]

from prismabuild import action_client, action_wait, census, core as pb, pool
import pbrun
import pbstatus
import pbwait
from admitted_queue_fixture import AdmittedQueueFixture
from submission_support import patch_store
from test_verified_action_result import _standard_action, _publish_result


def _rerun(tmp_path, monkeypatch, prior_status, prior_returncode):
    clock = [1_700_000_000.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _standard_action(checkout, command=["/bin/echo", "superseded-display"])
    key = action["action_key"]
    _, receipt = _publish_result(tmp_path, action, b"result\n")
    for status, returncode in ((prior_status, prior_returncode), ("executed", 0)):
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(checkout), worker_script="/worker.py", max_attempts=1)
        assert queue.claim(owner="display-worker") is not None
        detail = {} if returncode is None else {"returncode": returncode}
        if status == "executed":
            detail["stdout"] = json.dumps({"status": "published", "receipt": receipt}) + "\n"
        path = queue.finish(key, status=status, detail=detail)
        clock[0] += 1
    return queue, path, json.loads(path.read_text())


@pytest.mark.parametrize("prior_status,returncode,classification", [
    ("failed", 37, "flaky_rerun"),
    ("lease_lost_max_attempts", None, "infrastructure_rerun"),
])
@pytest.mark.parametrize("mode", ["--json", "--endings"])
def test_wait_modes_display_prior_generation_without_changing_verdict(
    tmp_path, monkeypatch, capsys, prior_status, returncode, classification, mode,
):
    queue, path, record = _rerun(tmp_path, monkeypatch, prior_status, returncode)
    original = path.read_bytes()
    patch_store(monkeypatch, client=pbrun, root=tmp_path)
    monkeypatch.setattr(pbwait.pool, "PoolQueue", lambda _root: queue)
    capsys.readouterr()
    exit_code = pbwait.main([mode, "--wait-s", "0", record["action_key"]])
    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0, json.dumps(output)
    row = output[0] if mode == "--json" else output["endings"][0]
    expected = {"state": "failed", "status": prior_status, "attempts": 1,
                "finished_unix": 1_700_000_000.0, "classification": classification}
    if returncode is not None:
        expected["returncode"] = returncode
    assert row["superseded"] == expected
    assert row["status"] == "executed" and row["succeeded"] is True
    assert path.read_bytes() == original
    assert "detail" not in record["supersedes_terminal"]


@pytest.mark.parametrize("prior_status,returncode,classification", [
    ("failed", 37, "flaky_rerun"),
    ("lease_lost", None, "infrastructure_rerun"),
])
def test_status_ending_rows_share_the_bounded_display(
    tmp_path, monkeypatch, prior_status, returncode, classification,
):
    queue, path, record = _rerun(tmp_path, monkeypatch, prior_status, returncode)
    row = census.endings(queue.root)[0]
    summary = action_client.outcome_summary(queue, path, record)["superseded"]
    assert row["superseded"] == summary
    text = "\n".join(pbstatus.ending_lines([row]))
    assert classification in text and prior_status in text
    assert "attempts=1" in text and "finished_unix=1700000000.0" in text
    assert row["status"] == "executed"


@pytest.mark.parametrize("claimed", [False, True])
def test_status_jobs_display_history_only_when_the_record_carries_it(tmp_path, monkeypatch, claimed):
    queue, path, record = _rerun(tmp_path, monkeypatch, "lease_lost", None)
    key = record["action_key"]
    history = record["supersedes_terminal"]
    path.unlink()
    queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                  checkout_root="/checkout", worker_script="/worker.py", max_attempts=1)
    ready = queue.records.item_path(pool.record_store.READY, key)
    current = json.loads(ready.read_text())
    current["supersedes_terminal"] = history
    ready.write_text(json.dumps(current))
    if claimed:
        assert queue.claim(owner="next-display-worker") is not None
    row = census.action_rows(queue.root).value["jobs"][0]
    assert row["superseded"]["classification"] == "infrastructure_rerun"
    assert "infrastructure_rerun" in "\n".join(pbstatus.pool_job_lines([row], {"empty": False}))


def test_history_is_bounded_and_unavailable_history_cannot_change_success(tmp_path, monkeypatch):
    queue, path, record = _rerun(tmp_path, monkeypatch, "failed", 37)
    record["supersedes_terminal"]["attempt_history"] = [{"huge": "x" * 1000}] * 100
    record["supersedes_terminal"]["supersedes_terminal"] = record["supersedes_terminal"]
    summary = action_client.outcome_summary(queue, path, record)
    assert len(json.dumps(summary["superseded"])) < 256
    assert summary["superseded"]["returncode"] == 37
    Path(record["supersedes_terminal"]["superseded_path"]).unlink()
    summary = action_client.outcome_summary(queue, path, record)
    assert summary["succeeded"] is True
    assert summary["superseded"]["classification"] is None
    assert "returncode" not in summary["superseded"]


def test_no_supersession_preserves_existing_rows_and_tables(tmp_path, monkeypatch):
    queue, path, record = _rerun(tmp_path, monkeypatch, "failed", 37)
    record.pop("supersedes_terminal")
    path.write_text(json.dumps(record))
    summary = action_client.outcome_summary(queue, path, record)
    assert "superseded" not in summary
    row = pbwait._from_ending(queue, (path, record))
    assert "superseded" not in row
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    context = {}
    consumer_row = pbwait._from_ending(queue, (path, record), context=context)
    consumer = pbwait.consumer_ending_row(queue, consumer_row, cas=cas, context=context)
    assert "superseded" not in consumer and "superseded" not in consumer["diagnostics"]
    ending = census.endings(queue.root)[0]
    assert "superseded" not in ending
    plain = "\n".join(pbstatus.ending_lines([ending]))
    assert "superseded" not in plain
    assert pbwait.verdict([row]) == 0
    assert pbwait.consumer_endings_verdict([consumer]) == 0, consumer


@pytest.mark.parametrize("fault", ["missing", "oversized", "wrong-key", "outside", "symlink"])
def test_history_read_is_confined_and_does_not_supply_a_verdict(tmp_path, monkeypatch, fault):
    queue, path, record = _rerun(tmp_path, monkeypatch, "failed", 37)
    history = record["supersedes_terminal"]
    archive = Path(history["superseded_path"])
    if fault == "missing":
        archive.unlink()
    elif fault == "oversized":
        archive.write_bytes(b" " * (8 * 1024 * 1024 + 1))
    elif fault == "wrong-key":
        prior = json.loads(archive.read_text())
        prior["action_key"] = "c" * 64
        archive.write_text(json.dumps(prior))
    else:
        outside = tmp_path / "outside.json"
        outside.write_bytes(archive.read_bytes())
        if fault == "outside":
            history["superseded_path"] = str(outside)
        else:
            archive.unlink()
            archive.symlink_to(outside)
    summary = action_client.outcome_summary(queue, path, record)
    assert summary["succeeded"] is True
    assert summary["superseded"]["classification"] is None
    assert "returncode" not in summary["superseded"]


@pytest.mark.parametrize("status,returncode,current_status,classification", [
    ("failed", 0, "executed", None),
    ("failed", True, "executed", None),
    ("failed", 37, "failed", None),
    ("failed", 37, "cache_hit", "flaky_rerun"),
    ("lease_lost", 37, "executed", "flaky_rerun"),
    ("failed", None, "executed", None),
])
def test_rerun_classification_requires_the_actual_exit_evidence(
    tmp_path, monkeypatch, status, returncode, current_status, classification,
):
    queue, path, record = _rerun(tmp_path, monkeypatch, "failed", 37)
    record["status"] = current_status
    history = record["supersedes_terminal"]
    history["status"] = status
    history.pop("superseded_path")
    history["detail"] = {"returncode": returncode}
    summary = action_client.superseded_summary(queue, record)["superseded"]
    assert summary["classification"] == classification


def test_without_history_no_archive_read_occurs(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(action_client.action_wait, "read_observation",
                        lambda *args, **kwargs: pytest.fail("no supersession needs no history read"))
    assert action_client.superseded_summary(queue, {"status": "executed"}) == {}


def test_unavailable_history_read_does_not_replace_the_current_ending(tmp_path, monkeypatch):
    queue, path, record = _rerun(tmp_path, monkeypatch, "failed", 37)

    def unavailable(*args, **kwargs):
        raise action_client.action_wait.OutcomeObservationTimedOut("history read timed out")

    monkeypatch.setattr(action_client.action_wait, "read_observation", unavailable)
    summary = action_client.outcome_summary(queue, path, record)
    assert summary["succeeded"] is True
    assert summary["superseded"]["status"] == "failed"
    assert summary["superseded"]["classification"] is None


@pytest.fixture
def archive_reader_fault(monkeypatch):
    original = action_wait._reader.bounded
    original_observation = action_wait.read_observation
    calls = []
    identity = {"section": "superseded terminal display", "pid": 123456789,
                "starttime_ticks": 54321, "since_unix": 1_700_000_000.0}
    state = {"alive": True, "unknown": False, "status": "timed_out", "fault": True,
             "histories": []}

    def bounded(section, read, **kwargs):
        if section != "superseded terminal display":
            return original(section, read, **kwargs)
        calls.append(os.getpid())
        if not state["fault"]:
            return original(section, read, **kwargs)
        retained = dict(identity)
        if state["unknown"]:
            retained["pid"] = "unknown-pid"
        kwargs["abandoned"].append(retained)
        return {"status": state["status"], "elapsed_s": 5,
                "type": "OSError", "error": "archive reader failed"}

    def observation(section, read, **kwargs):
        try:
            return original_observation(section, read, **kwargs)
        finally:
            if section == "superseded terminal display":
                state["histories"].append(action_wait.client_reader_history())

    monkeypatch.setattr(action_wait._reader, "bounded", bounded)
    monkeypatch.setattr(action_wait, "read_observation", observation)
    monkeypatch.setattr(action_wait, "reap_retained",
                        lambda readers: list(readers) if state["alive"] else [])
    monkeypatch.setattr(action_client, "_SUPERSEDED_READER_FAULT", None, raising=False)
    return state, calls, identity


@pytest.mark.parametrize("fault", ["retained", "unknown", "failed"])
def test_census_archive_ownership_fault_is_visible_and_fences_later_reads(
    tmp_path, monkeypatch, capsys, archive_reader_fault, fault,
):
    queue, path, record = _rerun(tmp_path, monkeypatch, "failed", 37)
    state, calls, identity = archive_reader_fault
    state["unknown"] = fault == "unknown"
    state["status"] = "error" if fault == "failed" else "timed_out"
    for _iteration in range(2):
        row = census.endings(queue.root)[0]
        assert row["status"] == "executed"
        assert row["superseded"]["classification"] is None
    assert calls == [os.getpid()]
    diagnostic = capsys.readouterr().err
    expected_identity = "unknown-pid" if fault == "unknown" else str(identity["pid"])
    assert "superseded terminal display" in diagnostic and expected_identity in diagnostic
    if fault != "unknown":
        assert str(identity["starttime_ticks"]) in diagnostic
    state.update(alive=False, fault=False)
    recovered = census.endings(queue.root)[0]
    if fault == "unknown":
        assert len(calls) == 1
        assert recovered["superseded"]["classification"] is None
    else:
        assert len(calls) == 2
        assert recovered["superseded"]["classification"] == "flaky_rerun"


@pytest.mark.parametrize("surface", ["jobs", "endings"])
def test_bounded_status_census_does_not_spawn_an_unowned_archive_grandchild(
    tmp_path, monkeypatch, surface,
):
    queue, path, record = _rerun(tmp_path, monkeypatch, "failed", 37)
    if surface == "jobs":
        path.unlink()
        queue.publish(action_key=record["action_key"], cas_root=str(tmp_path / "cas"),
                      checkout_root="/checkout", worker_script="/worker.py", max_attempts=1)
        ready = queue.records.item_path(pool.record_store.READY, record["action_key"])
        current = json.loads(ready.read_text())
        current["supersedes_terminal"] = record["supersedes_terminal"]
        ready.write_text(json.dumps(current))
    original = action_wait.read_observation

    def observation(section, read, **kwargs):
        if section == "superseded terminal display":
            raise AssertionError("bounded census must own the archive read directly")
        return original(section, read, **kwargs)

    def census_rows():
        if surface == "jobs":
            return census.action_rows(
                queue.root, superseded_reader=action_client.read_superseded_archive).value["jobs"]
        return census.endings(queue.root, superseded_reader=action_client.read_superseded_archive)

    monkeypatch.setattr(action_wait, "read_observation", observation)
    rows = action_wait.read_observation("status census", census_rows, budget_s=5)
    assert rows[0]["superseded"]["returncode"] == 37


@pytest.mark.parametrize("mode", ["--json", "--endings"])
@pytest.mark.parametrize("unknown", [False, True])
def test_wait_preserves_verdict_and_archive_ownership_across_observations(
    tmp_path, monkeypatch, capsys, archive_reader_fault, mode, unknown,
):
    queue, path, record = _rerun(tmp_path, monkeypatch, "failed", 37)
    state, calls, identity = archive_reader_fault
    state["unknown"] = unknown
    patch_store(monkeypatch, client=pbrun, root=tmp_path)
    monkeypatch.setattr(pbwait.pool, "PoolQueue", lambda _root: queue)
    capsys.readouterr()
    for _iteration in range(2):
        assert pbwait.main([mode, "--wait-s", "0", record["action_key"]]) == 0
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        row = output[0] if mode == "--json" else output["endings"][0]
        assert row["succeeded"] is True and row["status"] == "executed"
        assert row["superseded"]["classification"] is None
        expected_identity = "unknown-pid" if unknown else str(identity["pid"])
        assert expected_identity in captured.err
    history, = state["histories"]
    assert history.abandoned_count == 1
    assert history.complete is not unknown
    assert len(history.identities) == (0 if unknown else 1)
    assert calls == [os.getpid()]


@pytest.mark.parametrize("surface", ["jobs", "endings"])
def test_shared_census_does_not_start_an_implicit_archive_reader(
    tmp_path, monkeypatch, surface,
):
    queue, path, record = _rerun(tmp_path, monkeypatch, "failed", 37)
    if surface == "jobs":
        path.unlink()
        queue.publish(action_key=record["action_key"], cas_root=str(tmp_path / "cas"),
                      checkout_root="/checkout", worker_script="/worker.py", max_attempts=1)
        ready = queue.records.item_path(pool.record_store.READY, record["action_key"])
        current = json.loads(ready.read_text())
        current["supersedes_terminal"] = record["supersedes_terminal"]
        ready.write_text(json.dumps(current))

    def implicit_reader(path):
        pytest.fail("shared census must not spawn an archive grandchild")

    monkeypatch.setattr(action_client, "_read_superseded_archive", implicit_reader)
    rows = (census.action_rows(queue.root).value["jobs"] if surface == "jobs"
            else census.endings(queue.root))
    assert rows[0]["superseded"]["status"] == "failed"
    assert rows[0]["superseded"]["classification"] is None
    assert "returncode" not in rows[0]["superseded"]
