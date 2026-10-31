"""The gang switch reaches worker offers, submission and ordinary admission."""
from __future__ import annotations

import json

import pbrun
import pytest

from prismabuild import contracts, pool, submission
from test_worker_loop_offers_what_is_free import BASE, _run, sample


def _worker_offer(tmp_path, monkeypatch, *, enabled):
    monkeypatch.setenv("PRISMABUILD_GANG_ADMISSION", "1" if enabled else "0")
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "switch-test-host")
    queue = _run(tmp_path, [*BASE, "--once"], gpu_sample=sample())
    offer = json.loads((queue.root / pool.WORKERS / "switch-test-host.json").read_text())
    return queue, offer


@pytest.mark.parametrize("enabled", [False, True])
def test_worker_announcement_follows_the_gang_switch(tmp_path, monkeypatch, enabled):
    _, offer = _worker_offer(tmp_path, monkeypatch, enabled=enabled)

    assert (contracts.GANG_TAG in offer["tags"]) is enabled


def test_disabled_worker_causes_the_real_submission_refusal(tmp_path, monkeypatch):
    queue, _ = _worker_offer(tmp_path, monkeypatch, enabled=False)
    parsed = pbrun.parse_args(["--cwd", str(tmp_path), "--", "/bin/true"])
    request = pbrun.request_from_args(parsed)
    ordinary = {"params": {"placement": {"required_tags": ["gb10"]},
                            "demand": {"cpu": 1, "gpu": 1, "mem_gb": 8}}}
    submission.announce_placement(queue, ordinary, args=request, cwd=tmp_path,
                                  portable_checkout=True)
    gang = {"params": {**ordinary["params"],
                       "placement": {"required_tags": ["gb10", contracts.GANG_TAG]}}}

    with pytest.raises(submission.SubmissionRefused, match="no recorded worker can run") as refused:
        submission.announce_placement(queue, gang, args=request, cwd=tmp_path,
                                      portable_checkout=True)

    assert "Enable gang admission" in str(refused.value)
    assert queue.ready_items() == []


def test_non_gang_claim_bytes_are_unchanged_by_the_gang_switch(tmp_path, monkeypatch):
    monkeypatch.setattr(pool, "_now", lambda: 1000.0)
    records = []
    for enabled in (False, True):
        worker_root = tmp_path / str(enabled)
        worker_root.mkdir()
        queue, offer = _worker_offer(worker_root, monkeypatch, enabled=enabled)
        key = "c" * 64
        queue.publish(action_key=key, cas_root=tmp_path / "cas", checkout_root=tmp_path,
                      worker_script="worker.py", resources={"cpu": 1, "mem_gb": 1},
                      max_attempts=1)

        claimed = queue.claim(tags=offer["tags"], capacity=offer["capacity"],
                              owner="same-worker", has_gpu=True)

        assert claimed is not None and claimed["action_key"] == key
        records.append(queue.records.item_path(pool.record_store.CLAIMED, key).read_bytes())

    assert records[0] == records[1]
