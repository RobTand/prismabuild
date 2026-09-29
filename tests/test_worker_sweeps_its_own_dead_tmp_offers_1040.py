"""Offer temporaries a killed writer left behind (#1040)."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
from prismabuild import pool
import pbstatus

WORKER = ROOT / "tools/fleet/worker_loop.py"
UUID = "0123456789abcdef0123456789abcdef"
OLD = 1_000_000.0
NOW = OLD + 10_000.0


@pytest.fixture
def worker():
    spec = importlib.util.spec_from_file_location("offer_sweep_test", WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dead_pid():
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def _tmp(directory, host, pid, mtime=OLD):
    path = directory / f".{host}.json.{pid}.{UUID}.tmp"
    path.write_text("{")
    os.utime(path, (mtime, mtime))
    return path


def test_dead_writers_old_tmp_is_swept(worker, tmp_path):
    stale = _tmp(tmp_path, "box", _dead_pid())
    assert worker.sweep_own_dead_offer_tmp(tmp_path, "box", now=NOW) == [stale.name]
    assert not stale.exists()


def test_live_writers_tmp_is_kept(worker, tmp_path):
    live = _tmp(tmp_path, "box", os.getpid())
    assert worker.sweep_own_dead_offer_tmp(tmp_path, "box", now=NOW) == []
    assert live.exists()


def test_fresh_tmp_of_a_dead_pid_is_kept(worker, tmp_path):
    fresh = _tmp(tmp_path, "box", _dead_pid(), mtime=NOW - 1)
    assert worker.sweep_own_dead_offer_tmp(tmp_path, "box", now=NOW) == []
    assert fresh.exists()


def test_another_boxes_tmp_and_the_offer_are_never_touched(worker, tmp_path):
    other = _tmp(tmp_path, "other", _dead_pid())
    prefix = _tmp(tmp_path, "box-2", _dead_pid())
    offer = tmp_path / "box.json"
    offer.write_text("{}")
    os.utime(offer, (OLD, OLD))
    assert worker.sweep_own_dead_offer_tmp(tmp_path, "box", now=NOW) == []
    assert other.exists() and prefix.exists() and offer.exists()


def test_a_symlink_named_like_a_tmp_is_not_followed(worker, tmp_path):
    target = tmp_path / "keep"
    target.write_text("x")
    link = tmp_path / f".box.json.{_dead_pid()}.{UUID}.tmp"
    link.symlink_to(target)
    assert worker.sweep_own_dead_offer_tmp(tmp_path, "box", now=NOW) == []
    assert target.exists()


def test_missing_directory_is_not_an_error(worker, tmp_path):
    assert worker.sweep_own_dead_offer_tmp(tmp_path / "absent", "box", now=NOW) == []


def test_day_old_offer_is_marked_retired_and_kept(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(pool, "_now", lambda: 1000.0)
    queue.announce(host="gone", tags=["x86"], has_gpu=False,
                   capacity={"cpu": 4, "mem_gb": 8})
    monkeypatch.setattr(pbstatus.time, "time", lambda: 1000.0 + 2 * 86400.0)
    result = pbstatus.read_pool(queue.root)
    node = next(node for node in result["nodes"] if node["node"] == "gone")
    assert node["state"] == "retired"
    assert (queue.root / pool.WORKERS / "gone.json").exists()
    assert json.loads((queue.root / pool.WORKERS / "gone.json").read_text())["host"] == "gone"


def test_an_hour_old_offer_is_stale_not_retired(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(pool, "_now", lambda: 1000.0)
    queue.announce(host="slow", tags=[], has_gpu=False,
                   capacity={"cpu": 4, "mem_gb": 8})
    monkeypatch.setattr(pbstatus.time, "time", lambda: 1000.0 + 3600.0)
    node = next(n for n in pbstatus.read_pool(queue.root)["nodes"] if n["node"] == "slow")
    assert node["state"] == "stale"
