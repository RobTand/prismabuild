"""Supervisor disk deadlines, using ordinary files and fake statvfs (not live qualification)."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

import pytest

from prismabuild import local_scratch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import supervise  # noqa: E402

_REAL_SPOOL_BUDGET = supervise._spool_budget


@pytest.fixture
def disk(tmp_path, monkeypatch):
    roster = tmp_path / "fleet_boxes.json"
    roster.write_text(json.dumps({"local_disk_free_floor_percent": 5,
                                 "boxes": {"boxa": {
        "loops": 2, "args": ["--spool-gb", "auto"],
        "local_disk": str(tmp_path),
    }}}))
    monkeypatch.setattr(local_scratch, "OFFER_ROOT", tmp_path / "offers")
    monkeypatch.setattr(supervise, "CONFIG", roster)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    monkeypatch.setattr(supervise, "_spool_ledger_base", lambda host: tmp_path)
    monkeypatch.setattr(supervise, "_SPOOL_VERDICTS", {})
    monkeypatch.setattr(supervise, "_SPOOL_LIVE", {})
    monkeypatch.setattr(supervise, "_SPOOL_WAITING", {})
    monkeypatch.setattr(supervise, "_spool_budget",
                        lambda *a, **kw: ("offer", 200, "fake statvfs"))
    return tmp_path, roster


@pytest.mark.parametrize("failed,work,expected", [
    (False, 0.5, [0.0, 5.0, 10.0]),
    (True, 0.5, [0.0, 5.0, 10.0]),
    (False, 6.0, [0.0, 10.0, 20.0]),
])
def test_deadline_counts_work_and_skips_missed_slots(disk, monkeypatch,
                                                    failed, work, expected):
    observer = supervise._SpoolObservationProducer("boxa", 0, (2, []))
    now = [0.0]
    starts = []

    class Stop:
        def wait(self, timeout):
            if len(starts) == 3:
                return True
            now[0] += timeout
            return False

        def is_set(self):
            return False

    def sample(*args, **kwargs):
        starts.append(now[0])
        now[0] += work
        if failed:
            raise OSError("unavailable statvfs")
        return []

    observer._stop = Stop()
    monkeypatch.setattr(supervise.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(supervise, "spool_declaration", sample)
    observer._loop()
    assert starts == expected


@pytest.mark.parametrize("stage", ["statvfs", "ledger"])
def test_close_fences_inflight_result_and_is_idempotent(disk, monkeypatch, stage):
    base, _ = disk
    entered, release = threading.Event(), threading.Event()

    def sample(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        if stage == "ledger":
            return {"spool_gb": 200}
        return "offer", 200, "delayed fake statvfs"

    if stage == "ledger":
        ledger = SimpleNamespace(capacity=sample)
        queue = SimpleNamespace(ledger=lambda host: ledger)
        monkeypatch.setattr(supervise, "_queue_root", lambda: base)
        monkeypatch.setattr(supervise.pool, "PoolQueue", lambda *a, **kw: queue)
        monkeypatch.setattr(supervise, "_spool_budget", lambda *a, **kw: ("wait",))
    else:
        monkeypatch.setattr(supervise, "_spool_budget", sample)
    observer = supervise._SpoolObservationProducer("boxa", 0, (2, []))
    observer.start()
    try:
        assert entered.wait(3)
        observer.close()  # Does not join the blocked numeric read.
        observer.close()
    finally:
        release.set()
        observer._thread.join(3)
    assert not observer._thread.is_alive()
    assert local_scratch.read_spool_offer(base) == 0
    assert not supervise._SPOOL_VERDICTS


def test_supervision_reads_shape_without_another_disk_census(disk, monkeypatch):
    base, roster = disk
    calls = []

    def sample(*args, **kwargs):
        calls.append(True)
        return "offer", 200, "fake statvfs"

    monkeypatch.setattr(supervise, "_spool_budget", sample)
    initial = supervise.declared_shape("boxa", 0, record=False)
    observer = supervise._SpoolObservationProducer("boxa", 0, initial)
    for _ in range(20):
        assert observer.shape() == (2, ["--spool-gb", "200"])
    document = json.loads(roster.read_text())
    document["boxes"]["boxa"]["loops"] = 3
    document["boxes"]["boxa"]["args"] = ["--spool-gb", "100"]
    roster.write_text(json.dumps(document))
    # An unobserved declaration cannot cause a second sampler on a claim or
    # supervision pass.  It gets no disk kind until the owner observes it.
    assert observer.shape() == (3, [])
    assert calls == [True]
    assert not local_scratch.spool_offer_path(base).exists()
    observer.close()


def test_equal_capacity_recovery_renews_observation_time(disk, monkeypatch):
    base, _ = disk
    now = [100.0]
    monkeypatch.setattr(supervise.time, "time", lambda: now[0])
    first = supervise.declared_shape("boxa", 0)
    assert local_scratch.read_spool_offer(base) == 200
    now[0] += 5.001
    assert local_scratch.read_spool_offer(base) == 0
    assert supervise.declared_shape("boxa", 0) == first
    assert local_scratch.read_spool_offer(base) == 200
    record = json.loads(local_scratch.spool_offer_path(base).read_text())
    assert record["measured_unix"] == now[0]


def test_long_measurement_is_not_new_proof_at_publication(disk, monkeypatch):
    base, _ = disk
    now = [100.0]
    monkeypatch.setattr(supervise.time, "time", lambda: now[0])

    def sample(*args, **kwargs):
        now[0] += 6
        return "offer", 200, "slow fake statvfs"

    monkeypatch.setattr(supervise, "_spool_budget", sample)
    supervise.declared_shape("boxa", 0)
    assert local_scratch.read_spool_offer(base) == 0


def test_absent_roster_recovers_on_disk_deadline(disk, monkeypatch):
    observer = supervise._SpoolObservationProducer("boxa", 0, (2, []))
    config = supervise._config("boxa")
    now, attempts, sampled = [0.0], [], []

    class Stop:
        def wait(self, timeout):
            if len(attempts) == 2:
                return True
            now[0] += timeout
            return False

        def is_set(self):
            return False

    def configuration(host):
        attempts.append(now[0])
        if len(attempts) == 1:
            raise SystemExit("box temporarily absent")
        return config

    observer._stop = Stop()
    monkeypatch.setattr(supervise.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(supervise, "_config", configuration)
    monkeypatch.setattr(supervise, "spool_declaration",
                        lambda *a, **kw: sampled.append(now[0]))
    observer._loop()
    assert attempts == [0.0, 5.0]
    assert sampled == [5.0]


def test_refused_disk_retries_without_roster_change(disk, monkeypatch):
    base, _ = disk
    monkeypatch.setattr(supervise, "_spool_budget",
                        lambda *a, **kw: ("refuse", "unavailable"))
    assert supervise.declared_shape("boxa", 0) == (2, [])
    monkeypatch.setattr(supervise, "_spool_budget",
                        lambda *a, **kw: ("offer", 200, "recovered"))
    assert supervise.declared_shape("boxa", 0) == (2, ["--spool-gb", "200"])
    assert local_scratch.read_spool_offer(base) == 200


def test_holder_deferral_expires_proof_without_sampling_or_renewing(disk, monkeypatch):
    base, _ = disk
    now = [100.0]
    monkeypatch.setattr(supervise.time, "time", lambda: now[0])
    initial = supervise.declared_shape("boxa", 0)
    record = local_scratch.spool_offer_path(base).read_bytes()
    ledger = SimpleNamespace(base=base)
    queue = SimpleNamespace(ledger=lambda host: ledger)
    monkeypatch.setattr(supervise.pool, "PoolQueue", lambda *a, **kw: queue)
    monkeypatch.setattr(supervise, "_queue_root", lambda: base)
    # Use the real acquisition path, whose holder gate must precede statvfs.
    from prismabuild import produced_spool
    monkeypatch.setattr(produced_spool, "_local_disk", lambda path: None)
    monkeypatch.setattr(supervise, "_held_spool", lambda ledger: 10)
    monkeypatch.setattr(supervise, "_spool_budget", _REAL_SPOOL_BUDGET)
    monkeypatch.setattr(supervise.os, "statvfs",
                        lambda path: pytest.fail("sampled a disk with a holder"))
    now[0] += 5.001
    assert supervise.declared_shape("boxa", 0) == initial
    assert local_scratch.spool_offer_path(base).read_bytes() == record
    assert local_scratch.read_spool_offer(base) == 0
