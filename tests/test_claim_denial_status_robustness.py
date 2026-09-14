"""Optional denial evidence must not hide valid jobs or their prior decisions."""
import json
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))
from prismabuild import pool
import pbstatus


@pytest.fixture
def ready_denial(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "d" * 64
    queue.publish(action_key=key, cas_root=tmp_path / "cas",
                  checkout_root="/mnt/shared/status-fixture",
                  worker_script=ROOT / "tools/prismabuild_worker.py",
                  resources={"cpu": 1}, tags=["box"])
    record = json.loads(queue.item_path(pool.READY, key).read_text())
    path = queue.ledger("box").base / "adaptive" / pool.CLAIM_DENIALS
    path.parent.mkdir(parents=True, exist_ok=True)
    denial = {"action_key": key, "published_unix": record["published_unix"],
              "host": "box", "reason": "adaptive_cpu_refused",
              "evidence": {"decision": {"reason": "host_pressure"}},
              "denied_unix": time.time()}
    def write(value):
        path.write_text(json.dumps({"schema": pool.CLAIM_DENIALS_SCHEMA_V1,
                                    "records": {"fixture": value}}))
    write(denial)
    return queue, key, path, denial, write


@pytest.mark.parametrize("evidence", [None, [], "broken"])
def test_bad_optional_evidence_cannot_hide_a_ready_job(ready_denial, evidence):
    queue, key, path, denial, write = ready_denial
    write({**denial, "evidence": evidence})
    result = pbstatus.read_pool(queue.root)
    row = next(row for row in result["jobs"] if row["action_key"] == key)
    assert row["state"] == "READY"
    assert result["queue"]["ready"] == 1
    assert not row["admission_denials"]
    assert any("claim denial" in note for note in result["notes"])


def test_claim_keeps_prior_host_denial_visible_with_age(ready_denial, monkeypatch):
    queue, key, path, denial, write = ready_denial
    now = time.time()
    write({**denial, "denied_unix": now - 120})
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "box")
    assert queue.claim(tags=["box"], owner="box:fixture")["action_key"] == key
    result = pbstatus.read_pool(queue.root)
    row = next(row for row in result["jobs"] if row["action_key"] == key)
    assert row["state"] == "CLAIMED"
    assert row["admission_denials"][0]["age_s"] >= 120
    text = "\n".join(pbstatus.pool_job_lines([row], result["queue"]))
    assert "box: adaptive_cpu_refused/host_pressure" in text
    assert "ago" in text


def test_denial_read_error_preserves_job_but_reports_incomplete(ready_denial, monkeypatch):
    queue, key, path, denial, write = ready_denial
    original = pbstatus._pool_sidecar
    monkeypatch.setattr(pbstatus, "_pool_sidecar", lambda candidate:
                        PermissionError("denial read denied") if candidate == path
                        else original(candidate))
    result = pbstatus.read_pool(queue.root)
    assert result["jobs"][0]["state"] == "READY"
    assert result["queue"]["ready"] == 1
    assert result["queue"]["complete"] is False
    assert any("denial read denied" in note for note in result["notes"])


def test_status_explains_the_observed_memory_shortage(ready_denial):
    queue, key, path, denial, write = ready_denial
    write({**denial, "reason": "reservation_unavailable", "evidence": {
        "token_shortage": {"resource": "mem_gb", "requested": 8, "available": 3},
    }})
    result = pbstatus.read_pool(queue.root)
    row = result["jobs"][0]
    assert row["admission_denials"][0]["evidence"]["token_shortage"]["available"] == 3
    text = "\n".join(pbstatus.pool_job_lines([row], result["queue"]))
    assert "mem_gb: requested 8, available 3; waiting for release" in text
    assert "ago" in text


@pytest.mark.parametrize("shortage", [None, [], "broken", {},
    {"resource": "mem_gb", "requested": True, "available": 0},
    {"resource": "mem_gb", "requested": 8, "available": -1},
    {"resource": "mem_gb", "requested": 8, "available": 8},
    {"resource": [], "requested": 8, "available": 3},
])
def test_bad_shortage_detail_keeps_the_original_denial_readable(ready_denial, shortage):
    queue, key, path, denial, write = ready_denial
    write({**denial, "evidence": {"token_shortage": shortage}})
    result = pbstatus.read_pool(queue.root)
    text = "\n".join(pbstatus.pool_job_lines(result["jobs"], result["queue"]))
    assert "box: adaptive_cpu_refused" in text
    assert "waiting for release" not in text
