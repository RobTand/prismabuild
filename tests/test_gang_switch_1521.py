"""The gang capability is owned by its switch, not an arbitrary extra tag."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from test_worker_loop_offers_what_is_free import BASE, _run, sample
from prismabuild import _gang, pool

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools/fleet"))
import pbrun  # noqa: E402


@pytest.mark.parametrize("host", ["sparklina", "sparky"])
def test_a_disabled_worker_cannot_offer_the_gang_capability_via_an_extra_tag(
        tmp_path, monkeypatch, host):
    monkeypatch.setenv("PRISMABUILD_GANG_ADMISSION", "0")
    monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
    queue = _run(tmp_path, [*BASE, "--once", "--tag", _gang.TAG], gpu_sample=sample())
    offer = json.loads((queue.root / "workers" / f"{host}.json").read_text())
    assert _gang.TAG not in offer["tags"], "a disabled worker offered gang-v1 through --tag"


def test_a_gang_submission_is_refused_when_the_recorded_workers_are_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("PRISMABUILD_GANG_ADMISSION", "0")
    queue = _run(tmp_path, [*BASE, "--once"], gpu_sample=sample())
    args = pbrun.parse_args(["--cwd", str(tmp_path), "--tag", "gb10", "--",
                             "/bin/true"])
    action = {"params": {"placement": {"required_tags": ["gb10", _gang.TAG]},
                         "demand": {"cpu": 1, "gpu": 1, "mem_gb": 8}}}
    with pytest.raises(SystemExit, match="no recorded worker"):
        pbrun.announce_placement(queue, action, args=args, cwd=tmp_path, portable_checkout=True)


def test_non_gang_claim_bytes_are_unchanged_by_the_gang_switch(tmp_path, monkeypatch):
    monkeypatch.setattr(pool, "_now", lambda: 1000.0)
    records = []
    for value in ("0", "1"):
        monkeypatch.setenv("PRISMABUILD_GANG_ADMISSION", value)
        queue = pool.PoolQueue(tmp_path / f"queue-{value}")
        key = "c" * 64
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py",
                      resources={"cpu": 1}, max_attempts=1)
        tags = ["x86", "test-host"] + ([_gang.TAG] if value == "1" else [])
        assert queue.claim(tags=tags, capacity={"cpu": 1}, owner="same-worker")["action_key"] == key
        records.append(queue.item_path(pool.CLAIMED, key).read_bytes())
    assert records[0] == records[1]
