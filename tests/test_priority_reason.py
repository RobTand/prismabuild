"""Priority explanations follow submissions without changing sealed work."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import pool  # noqa: E402
import pbcampaign  # noqa: E402
import pbrun  # noqa: E402
import pbstatus  # noqa: E402
from test_pbrun_detach import _checkout, _queue, _run_pbrun, _one_json_line  # noqa: E402

KEY = "a" * 64
REASON = "G2 re-plan gate, coordinator-approved"


def test_campaign_and_pbrun_reasons_do_not_change_action_identity(tmp_path, monkeypatch, capsys):
    work, queue = _checkout(tmp_path), _queue(tmp_path)
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    manifest = tmp_path / "campaign.json"
    manifest.write_text(json.dumps([{
        "argv": ["/bin/bash", "-lc", "printf ok"], "cwd": str(work),
        "priority": 1, "priority_reason": REASON,
    }]))
    assert pbcampaign.main(["--detach", str(manifest)]) == 0
    campaign = _one_json_line(capsys.readouterr())
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach", "--priority", "1",
                      "--priority-reason", "same approval, different wording") == 0
    direct = _one_json_line(capsys.readouterr())
    assert direct["action_key"] == campaign["action_key"]
    assert direct["status"] == "attached"
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach", "--priority", "1") == 0
    assert _one_json_line(capsys.readouterr())["action_key"] == direct["action_key"]
    # Attaching does not silently replace the existing generation's reason.
    record = json.loads(queue.item_path(pool.READY, direct["action_key"]).read_text())
    assert record["priority_reason"] == REASON


def test_reason_survives_claim_and_is_visible_in_status(tmp_path):
    from admitted_queue_fixture import AdmittedQueueFixture
    queue = AdmittedQueueFixture(
        _queue(tmp_path), capacity={"cpu": 2, "mem_gb": 4},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.publish(action_key=KEY, cas_root=tmp_path / "cas", worker_script="/worker.py",
                  checkout_root="/checkout", priority=1, priority_reason=REASON)
    ready = pbstatus.read_pool(queue.root)
    assert ready["jobs"][0]["priority"] == 1
    assert ready["jobs"][0]["priority_reason"] == REASON
    assert REASON in "\n".join(pbstatus.pool_job_lines(ready["jobs"], ready["queue"]))
    assert queue.claim(owner="worker:1") is not None
    claimed = json.loads(queue.item_path(pool.CLAIMED, KEY).read_text())
    assert claimed["priority_reason"] == REASON
    observed = pbstatus.read_pool(queue.root)
    assert observed["jobs"][0]["priority_reason"] == REASON


def test_above_default_priority_without_a_reason_is_still_allowed(tmp_path):
    queue = _queue(tmp_path)
    path = queue.publish(action_key=KEY, cas_root=tmp_path / "cas",
                         worker_script="/worker.py", checkout_root="/checkout", priority=1)
    assert "priority_reason" not in json.loads(path.read_text())


@pytest.mark.parametrize("value", [17, "", "  ", "bad\x00note", "line\nline", "x" * 1025],
                         ids=["integer", "empty", "blank", "nul", "multiline", "too-long"])
def test_bad_reason_is_refused_before_queue_evidence_changes(tmp_path, value):
    queue = _queue(tmp_path)
    before = {str(p.relative_to(queue.root)): p.read_bytes()
              for p in queue.root.rglob("*.json")}
    with pytest.raises(pool.PoolContractError, match="priority_reason"):
        queue.publish(action_key=KEY, cas_root=tmp_path / "cas", worker_script="/worker.py",
                      checkout_root="/checkout", priority=1, priority_reason=value)
    # The public transition wrapper may create locks, never queue evidence.
    assert {str(p.relative_to(queue.root)): p.read_bytes()
            for p in queue.root.rglob("*.json")} == before


def test_campaign_rejects_invalid_reason_before_publishing_any_row(tmp_path, monkeypatch):
    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    manifest = tmp_path / "campaign.json"
    manifest.write_text(json.dumps([
        {"argv": ["/bin/true"], "cwd": str(work)},
        {"argv": ["/bin/true"], "cwd": str(work), "priority_reason": "\n"},
    ]))
    with pytest.raises(pbcampaign.ManifestError, match="priority_reason"):
        pbcampaign.load_manifest(str(manifest))
    assert not list(queue.dir(pool.READY).glob("*.json"))


def test_reason_is_refused_clearly_by_an_older_queue_runtime():
    class OldQueue:
        def publish(self, *, action_key, priority):
            raise AssertionError("publication must not be attempted")

    action = {
        "action_key": KEY,
        "params": {"demand": {"cpu": 1}, "placement": {"required_tags": []},
                   "checkout_snapshot": {}},
        "environment": {"variables": {pbrun.CONTAINER_OWNER_ENV: "fixture"}},
    }
    args = pbrun.parse_args(["--priority-reason", REASON, "--", "/bin/true"])
    with pytest.raises(pool.PoolContractError, match="loaded queue runtime.*priority_reason"):
        pbrun.publication_row(action, args=args, queue=OldQueue())


def test_an_argument_shaped_campaign_reason_is_only_a_value():
    row = {"argv": ["/bin/true"], "priority_reason": "--measurement requested later"}
    args = pbrun.parse_args(pbcampaign.pbrun_argv(row))
    assert args.priority_reason == row["priority_reason"]
    assert not args.measurement


def test_a_bad_slurm_annotation_does_not_hide_other_jobs(tmp_path, monkeypatch):
    monkeypatch.setattr(pbstatus, "_scheduler_output", lambda *_a, **_k:
        f"42|PENDING|cpu|None|0:00|10:00|tester|pb-{KEY[:12]}|Priority\n"
        "43|RUNNING|cpu|node|0:01|10:00|tester|ordinary|None\n")
    monkeypatch.setattr(pbstatus, "_submission", lambda *_a, **_k:
        ({"action_key": KEY, "job_id": "42", "priority_reason": "\n"}, None))
    jobs = pbstatus.read_jobs(lane_root=tmp_path)
    assert [job["job_id"] for job in jobs] == ["42", "43"]
    assert jobs[0]["priority_reason"] is None
    assert "priority_reason" in jobs[0]["note"]
