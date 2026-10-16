"""Unavailable receipts fence the queue while an idle worker can prove park."""
from functools import partial
import os
from pathlib import Path
import stat
import sys

import pytest

from prismabuild import pool, role_session
from test_a_draining_worker_stays_visible import _load_loop
import fleet_membership


@pytest.mark.parametrize("missing,closed", [
    pytest.param("loaded.json", True, id="loaded-missing-closed"),
    pytest.param("published.json", True, id="published-missing-closed"),
    pytest.param("loaded.json", False, id="loaded-missing-open"),
    pytest.param("published.json", False, id="published-missing-open"),
    pytest.param(None, True, id="complete-closed"),
])
def test_idle_physical_park_does_not_require_receipt_identity(
        tmp_path, monkeypatch, missing, closed):
    gate = {"draining": closed, "changed_unix": 101.5}
    loop = _load_loop(tmp_path, monkeypatch, gate)
    monkeypatch.setattr(loop.container_images, "InventoryCache", partial(
        loop.container_images.InventoryCache, root=tmp_path / "images"))
    parked = role_session.PARKED_ROOT
    parked.mkdir(parents=True)
    if missing:
        (tmp_path / missing).unlink()
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.records.ensure_layout()
    key = "a" * 64
    queue.publish(action_key=key, cas_root=tmp_path / "cas", checkout_root=tmp_path,
                  worker_script="never-authorized.py", resources={"cpu": 1, "mem_gb": 1})
    ready = queue.records.item_path(pool.record_store.READY, key)
    original = ready.read_bytes()
    queue_root = str(queue.root)
    queue_io = []
    observing = [False]

    def audit(event, arguments):
        if not observing[0]:
            return
        for value in arguments:
            if isinstance(value, (str, bytes, os.PathLike)):
                path = os.fsdecode(value)
                if path == queue_root or path.startswith(queue_root + os.sep):
                    queue_io.append((event, path))

    sys.addaudithook(audit)
    affinity = os.sched_getaffinity(0)
    observing[0] = missing is not None
    try:
        code = loop.main()
    finally:
        observing[0] = False
    assert os.sched_getaffinity(0) == affinity
    assert code == (75 if missing else 0)
    assert queue_io == [], "UNKNOWN reached the private queue"
    assert ready.read_bytes() == original
    assert list(queue.records.dir(pool.record_store.CLAIMED).iterdir()) == []
    assert list(queue.records.dir(pool.record_store.DONE).iterdir()) == []
    assert list(queue.records.dir(pool.record_store.FAILED).iterdir()) == []
    assert not (queue.root / role_session.GENERATION_DRIFT_DIR).exists()
    offers = list((queue.root / pool.record_store.WORKERS).glob("*.json"))
    assert len(offers) == (0 if missing else 1)
    if closed:
        start = Path("/proc/self/stat").read_text().rpartition(")")[2].split()[19]
        expected = fleet_membership.expected_markers([(os.getpid(), start)], gate)
        actual = fleet_membership.parked_markers(parked, gate)
        assert {path.name for path in actual} == expected, "idle worker left no park proof"
        assert list(parked.iterdir()) == actual
        marker = actual[0]
        identity = marker.lstat()
        assert stat.S_ISREG(identity.st_mode) and identity.st_nlink == 1
        assert identity.st_uid == os.geteuid()
        assert identity.st_mode & 0o022 == 0
        assert marker.read_bytes() == b""
    else:
        assert list(parked.iterdir()) == []
