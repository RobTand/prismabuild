"""A slightly faster worker clock must not hide the fleet's only x86 box."""

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
from prismabuild import pool
import pbstatus


def _offers(tmp_path, monkeypatch, ahead_s):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(pool, "_now", lambda: 1000.0)
    monkeypatch.setattr(pbstatus.time, "time", lambda: 1000.0)
    queue.announce(host="sparky", tags=["gb10"], has_gpu=True,
                   capacity={"cpu": 20, "mem_gb": 104, "gpu": 1})
    queue.announce(host="dl380g10", tags=["x86"], has_gpu=False,
                   capacity={"cpu": 80, "mem_gb": 96}, timeout_ceiling_s=3600)
    path = queue.root / pool.WORKERS / "dl380g10.json"
    record = json.loads(path.read_text())
    record["announced_unix"] = 1000.0 + ahead_s
    path.write_text(json.dumps(record))
    return queue


@pytest.mark.parametrize("max_age_s", [0, 5, 120, float("inf")])
@pytest.mark.parametrize("ahead_s", [0.001, 7.0, 60.0])
def test_bounded_future_offer_keeps_x86_placeable(tmp_path, monkeypatch, max_age_s, ahead_s):
    queue = _offers(tmp_path, monkeypatch, ahead_s)
    intent = {"tags": ["x86"], "resources": {"cpu": 4, "mem_gb": 8}}
    assert queue.placeable(intent, max_age_s=max_age_s) is True
    assert queue.placeable_hosts(intent, max_age_s=max_age_s) == ["dl380g10"]
    assert "x86" in queue.offered_tags(max_age_s=max_age_s)


@pytest.mark.parametrize("ahead_s", [60.001, 3600.0])
def test_unbounded_capability_age_does_not_allow_unbounded_future_skew(tmp_path, monkeypatch, ahead_s):
    queue = _offers(tmp_path, monkeypatch, ahead_s)
    assert queue.placeable({"tags": ["x86"]}, max_age_s=float("inf")) is False


@pytest.mark.parametrize("ahead_s, expected_state", [(7.0, "live"), (61.0, "stale")])
def test_status_reports_future_skew_without_granting_admission(tmp_path, monkeypatch, ahead_s, expected_state):
    queue = _offers(tmp_path, monkeypatch, ahead_s)
    directory = queue.ledger("dl380g10").base / "adaptive"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "cpu-sample.json").write_text(json.dumps({"sampled_unix": 1007.0}))
    result = pbstatus.read_pool(queue.root)
    node = next(node for node in result["nodes"] if node["node"] == "dl380g10")
    assert node["state"] == expected_state
    assert node["offer_clock_skew_s"] == ahead_s
    if expected_state == "live":
        assert node["age_s"] == 0
    assert node["admission"]["cpu"]["state"] == "stale"
    assert any("dl380g10" in note and "clock skew" in note for note in result["notes"])
