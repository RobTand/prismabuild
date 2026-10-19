"""Physical park remains available after a real offer loses its receipt."""
from functools import partial
import json
import os
from pathlib import Path
import stat
import sys

import pytest

from prismabuild import generation_receipt, pool, role_session
from test_a_draining_worker_stays_visible import _load_loop
import fleet_membership


@pytest.mark.parametrize("closed", [
    pytest.param(True, id="closed-after-real-offer"),
    pytest.param(False, id="open-after-real-offer-control"),
])
def test_preclaim_unknown_preserves_physical_park_without_queue_work(
        tmp_path, monkeypatch, capsys, closed):
    loop = _load_loop(tmp_path, monkeypatch, {"draining": False, "changed_unix": 101.5})
    monkeypatch.setattr(loop.container_images, "InventoryCache", partial(
        loop.container_images.InventoryCache, root=tmp_path / "images"))
    parked = role_session.PARKED_ROOT
    parked.mkdir(parents=True)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.records.ensure_layout()
    key = "a" * 64
    queue.publish(action_key=key, cas_root=tmp_path / "cas", checkout_root=tmp_path,
                  worker_script="never-authorized.py", resources={"cpu": 1, "mem_gb": 1})
    ready = queue.records.item_path(pool.record_store.READY, key)
    original = ready.read_bytes()
    gate = {"draining": closed, "changed_unix": 202.5}
    history = []
    decisions = []
    queue_io = []
    observing = [False]
    queue_root = str(queue.root)
    publish_offer = loop.publish_offer

    def publish(announce, **options):
        result = publish_offer(announce, **options)
        assert result.status == "published", result
        assert len(list((queue.root / pool.record_store.WORKERS).glob("*.json"))) == 1
        history.append("actual-offer-published")
        role_session.RUNTIME_VERSION.unlink()
        replacement = role_session.MAINTENANCE_GATE.with_suffix(".next")
        replacement.write_text(json.dumps(gate))
        os.replace(replacement, role_session.MAINTENANCE_GATE)
        return result

    monkeypatch.setattr(loop, "publish_offer", publish)
    previous_profile = sys.getprofile()

    def profile(frame, event, value):
        if event == "return" and frame.f_code == generation_receipt.compare.__code__:
            decisions.append(value)
            if isinstance(value, generation_receipt.Unknown):
                history.append("actual-unknown-observed")
                observing[0] = True
        if previous_profile is not None:
            previous_profile(frame, event, value)

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
    sys.setprofile(profile)
    try:
        code = loop.main()
    finally:
        observing[0] = False
        sys.setprofile(previous_profile)
    assert os.sched_getaffinity(0) == affinity
    assert history == ["actual-offer-published", "actual-unknown-observed"]
    assert len(decisions) == 2
    assert isinstance(decisions[0], generation_receipt.Match)
    assert isinstance(decisions[1], generation_receipt.Unknown)
    assert decisions[1].loaded == decisions[0].identity
    assert "runtime receipt unknown at claim boundary" in capsys.readouterr().out
    assert code == 75
    assert queue_io == [], "preclaim UNKNOWN touched the private queue"
    assert ready.read_bytes() == original
    assert list(queue.records.dir(pool.record_store.CLAIMED).iterdir()) == []
    assert list(queue.records.dir(pool.record_store.DONE).iterdir()) == []
    assert list(queue.records.dir(pool.record_store.FAILED).iterdir()) == []
    assert not (queue.root / role_session.GENERATION_DRIFT_DIR).exists()
    assert len(list((queue.root / pool.record_store.WORKERS).glob("*.json"))) == 1
    if closed:
        start = Path("/proc/self/stat").read_text().rpartition(")")[2].split()[19]
        expected = fleet_membership.expected_markers([(os.getpid(), start)], gate)
        actual = fleet_membership.parked_markers(parked, gate)
        assert {path.name for path in actual} == expected, "preclaim UNKNOWN left no park proof"
        assert list(parked.iterdir()) == actual
        marker = actual[0]
        identity = marker.lstat()
        assert stat.S_ISREG(identity.st_mode) and identity.st_nlink == 1
        assert identity.st_uid == os.geteuid()
        assert identity.st_mode & 0o022 == 0
        assert marker.read_bytes() == b""
    else:
        assert list(parked.iterdir()) == []
