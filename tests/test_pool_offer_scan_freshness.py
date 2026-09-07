"""A slow shared-directory scan must not extend worker offer lifetimes."""

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool


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
