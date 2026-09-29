"""A conflicting queue summary is an inspectable refusal, never a success."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import core as pb, pool  # noqa: E402
import pbrun  # noqa: E402
import pbwait  # noqa: E402

KEY = "e" * 64
OTHER = "f" * 64


def _ending(tmp_path: Path, key: str = KEY, *, status: str = "failed"):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", max_attempts=1)
    assert queue.claim(owner="worker:1") is not None
    path = queue.finish(key, status=status, detail={
        "returncode": 0 if status == "executed" else 37,
        "stdout": "immutable causal output\n", "stderr": "original cause\n",
    })
    return queue, path, json.loads(path.read_text(encoding="utf-8"))


def _fingerprints(queue):
    # Admission snapshots can publish asynchronously after the fixture's
    # claim. They are telemetry, not the terminal/attempt/log evidence this
    # read-only observer must preserve.
    return {str(p.relative_to(queue.root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in queue.root.rglob("*")
            if p.is_file() and p.relative_to(queue.root).parts[0] != pool.RESERVATIONS}


@pytest.mark.parametrize("status", ["failed", "executed"])
def test_conflicting_detail_is_a_read_only_structured_refusal(tmp_path, status):
    queue, path, record = _ending(tmp_path, status=status)
    record["detail"]["returncode"] = 99
    path.write_text(json.dumps(record), encoding="utf-8")
    before = _fingerprints(queue)
    row = pbwait.wait_one(queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
                          deadline=time.monotonic(), lane_root=tmp_path / "lane")
    assert row["status"] == "record_error"
    assert not row["succeeded"]
    assert pbwait.verdict([row]) == 74
    evidence = row["integrity_error"]
    assert evidence["kind"] == "terminal_summary_conflict"
    assert evidence["mismatched_fields"] == ["detail"]
    assert evidence["detail_fields"] == ["returncode"]
    assert evidence["summary_path"] == str(path)
    assert evidence["attempt_path"] == str(queue.attempt_path(record, 1))
    assert evidence["queue_detail_sha256"] != evidence["adopted_detail_sha256"]
    attempt = evidence["immutable_attempt"]
    assert attempt["status"] == status
    assert attempt["returncode"] == (0 if status == "executed" else 37)
    for stream in ("stdout", "stderr"):
        log = queue.root / attempt["logs"][stream]["path"]
        assert hashlib.sha256(log.read_bytes()).hexdigest() == attempt["logs"][stream]["sha256"]
    assert "immutable attempt" in row["note"]
    assert str(queue.attempt_path(record, 1)) in pbwait.render([row])
    assert _fingerprints(queue) == before
    # The existing synchronous launcher still refuses, rather than adopting
    # a diagnostic snapshot as a successful terminal outcome.
    with pytest.raises(pool.PoolContractError, match="adopted immutable"):
        pbrun.await_outcome(queue, KEY, wait_s=0)


def test_conflict_does_not_abort_other_keys_and_json_is_supported(tmp_path, monkeypatch, capsys):
    queue, path, record = _ending(tmp_path)
    record["detail"]["returncode"] = 99
    path.write_text(json.dumps(record), encoding="utf-8")
    _ending(tmp_path, OTHER, status="executed")
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbwait.pool, "PoolQueue", lambda _root: queue)
    assert pbwait.main(["--json", "--wait-s", "0", KEY, OTHER]) == 74
    rows = json.loads(capsys.readouterr().out)
    assert [r["action_key"] for r in rows] == [KEY, OTHER]
    assert [r["status"] for r in rows] == ["record_error", "executed"]
    assert rows[0]["integrity_error"]["immutable_attempt"]["returncode"] == 37


@pytest.mark.parametrize("stdout", ["", "historical output\n", "x" * (70 * 1024)],
                         ids=["empty", "short", "long"])
def test_legacy_stream_summary_remains_a_refusal_with_verified_references(tmp_path, stdout):
    # The old writer stored whole streams without the metadata introduced
    # by #1203. Construct that shape from a real immutable ending.
    queue = pool.PoolQueue(tmp_path / "legacy")
    queue.ensure_layout()
    queue.publish(action_key=KEY, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", max_attempts=1)
    assert queue.claim(owner="worker:1") is not None
    path = queue.finish(KEY, status="failed", detail={
        "returncode": 37, "stdout": stdout, "stderr": "",
    })
    record = json.loads(path.read_text())
    metadata = sorted(f"{stream}_{field}" for stream in ("stdout", "stderr")
                      for field in ("bytes", "truncated", "log"))
    for field in metadata:
        del record["detail"][field]
    record["detail"]["stdout"] = stdout
    path.write_text(json.dumps(record))
    before = _fingerprints(queue)
    row = pbwait.wait_one(queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
                          deadline=time.monotonic(), lane_root=tmp_path / "lane")
    diagnostic = row["integrity_error"]
    assert row["status"] == "record_error" and not row["succeeded"]
    assert diagnostic["detail_fields"] == sorted(metadata + (["stdout"] if len(stdout) > 65536 else []))
    assert diagnostic["generation"] == queue.attempt_generation(record)
    assert diagnostic["immutable_attempt"]["logs"]["stdout"]["bytes"] == len(stdout)
    assert row["returncode"] is None and row["receipt_published"] is None
    assert _fingerprints(queue) == before


@pytest.mark.parametrize("field,value", [("status", "executed"),
                                         ("finished_host", "another-host"),
                                         ("finished_unix", 0)])
def test_other_summary_disagreements_are_per_key_refusals(tmp_path, field, value):
    queue, path, record = _ending(tmp_path)
    record[field] = value
    path.write_text(json.dumps(record))
    row = pbwait.wait_one(queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
                          deadline=time.monotonic(), lane_root=tmp_path / "lane")
    assert row["integrity_error"]["mismatched_fields"] == [field]
    assert pbwait.verdict([row]) == 74


def test_diagnostic_is_bounded_and_does_not_dump_detail_values(tmp_path):
    queue, path, record = _ending(tmp_path)
    for i in range(40):
        record["detail"][f"field_{i:02}_" + "x" * 200] = "DO_NOT_PRINT_SECRET"
    path.write_text(json.dumps(record))
    row = pbwait.wait_one(queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
                          deadline=time.monotonic(), lane_root=tmp_path / "lane")
    diagnostic = row["integrity_error"]
    assert diagnostic["detail_field_count"] == 40
    assert len(diagnostic["detail_fields"]) == 32
    assert all(len(field) <= 128 for field in diagnostic["detail_fields"])
    assert "DO_NOT_PRINT_SECRET" not in json.dumps(row)
    assert len(json.dumps(diagnostic)) < 8000


def test_unverified_attempt_bytes_are_not_reported_as_authoritative(tmp_path):
    queue, path, record = _ending(tmp_path)
    record["detail"]["returncode"] = 99
    path.write_text(json.dumps(record), encoding="utf-8")
    attempt = queue.attempt_outcomes(record)[-1]
    logs = attempt["logs"]
    assert isinstance(logs, dict)
    log = queue.root / logs["stdout"]["path"]
    log.chmod(0o644)
    log.write_text("forged log\n", encoding="utf-8")
    row = pbwait.wait_one(queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
                          deadline=time.monotonic(), lane_root=tmp_path / "lane")
    assert row["status"] == "record_error"
    assert not row["succeeded"]
    assert "integrity_error" not in row
    assert pbwait.verdict([row]) == 74
