"""A slow shared-directory scan must not extend worker offer lifetimes."""

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool


@pytest.mark.parametrize("announced", [1001.0, float("inf")], ids=["future", "positive-infinity"])
def test_invalid_offer_time_cannot_vouch_for_a_worker(tmp_path, monkeypatch, announced):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(pool, "_now", lambda: 1000.0)
    queue.announce(host="current", tags=[], has_gpu=False)
    queue.announce(host="invalid", tags=[], has_gpu=False)
    path = queue.root / pool.WORKERS / "invalid.json"
    record = json.loads(path.read_text())
    record["announced_unix"] = announced
    path.write_text(json.dumps(record))

    assert [offer["host"] for offer in queue.offers(max_age_s=30)] == ["current"]


@pytest.mark.parametrize("stall_at", ["enumeration", "first_read", "last_read"])
def test_offers_expire_at_scan_completion(tmp_path, monkeypatch, stall_at):
    queue = pool.PoolQueue(tmp_path / "queue")
    clock = [1000.0]
    monkeypatch.setattr(pool, "_now", lambda: clock[0])
    queue.announce(host="a-old", tags=[], has_gpu=False)
    clock[0] = 1060.0
    queue.announce(host="z-current", tags=[], has_gpu=False)
    clock[0] = 1000.0

    original_glob = Path.glob
    original_read = pool._read_json

    def stalled_glob(directory, pattern):
        paths = list(original_glob(directory, pattern))
        if directory == queue.root / pool.WORKERS and stall_at == "enumeration":
            clock[0] = 1060.0
        return iter(paths)

    def stalled_read(path, **kwargs):
        record = original_read(path, **kwargs)
        if ((stall_at == "first_read" and path.name == "a-old.json")
                or (stall_at == "last_read" and path.name == "z-current.json")):
            clock[0] = 1060.0
        return record

    monkeypatch.setattr(Path, "glob", stalled_glob)
    monkeypatch.setattr(pool, "_read_json", stalled_read)

    assert [offer["host"] for offer in queue.offers(max_age_s=30)] == ["z-current"]
