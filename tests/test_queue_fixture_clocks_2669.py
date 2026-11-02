"""Scheduling delay must not change logical admission in shared fixtures."""
from types import SimpleNamespace
import time

import pytest

from fleet_world import Clock, queue_world
from queue_contention_2669 import cpu_contention  # noqa: F401, Bazel plugin input
from prismabuild import _bounded_reader as reader
from prismabuild import _measurement_reservation as reservation
from prismabuild import adaptive_cpu, pool


@pytest.fixture
def delayed_world(tmp_path, monkeypatch):
    wall = Clock(now=2_000_000.)
    monkeypatch.setattr(time, "time", lambda: wall.now)
    monkeypatch.setattr(reader, "time", SimpleNamespace(
        **{**vars(time), "monotonic": lambda: wall.now}))
    world = queue_world(tmp_path, monkeypatch, clock=Clock(now=wall.now))
    monkeypatch.setattr(pool.cpu_admission, "action_identity", lambda _: ("shape", False))
    return world, wall


def denial(queue, key):
    records = adaptive_cpu.read_json(
        adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS)
    return next(row for row in records["records"].values() if row["action_key"] == key)


def test_owned_census_survives_descheduling_before_release(delayed_world, monkeypatch):
    world, wall = delayed_world
    queue = world.queue
    key = "a" * 64
    world.actions.publish(key, resources={"cpu": 1})
    capture = reader._capture_ownership
    scans = queue.root / "real-census-scans"
    real_scan = reservation._capture

    def scan(active):
        with scans.open("a") as stream:
            stream.write("scan\n")
        return real_scan(active)

    def delayed_ownership(*args, **kwargs):
        identity = capture(*args, **kwargs)
        wall.advance(6.)
        return identity

    monkeypatch.setattr(reservation, "_capture", scan)
    monkeypatch.setattr(reader, "_capture_ownership", delayed_ownership)
    claimed = queue.claim(capacity={"cpu": 2})
    if claimed is None:
        print("census refusal:", denial(queue, key))
    assert claimed is not None, "claim() returned None after census deadline scheduling delay"
    assert claimed["action_key"] == key
    assert scans.read_text().splitlines() == ["scan", "scan"]
    assert queue.ledger().holder_tokens(key) == {"cpu": 1}
    assert queue.records.item_path(pool.record_store.CLAIMED, key).exists()


def test_hot_cpu_sample_stays_hot_across_descheduling(delayed_world):
    world, wall = delayed_world
    queue = world.queue
    key = "b" * 64
    world.actions.publish(key, resources={"cpu": 1, "mem_gb": 1})
    hot = {"sampled_unix": time.time(), "busy_cpus": 2., "psi_some": .2,
           "cpu_count": 2, "interval_s": 1.}
    wall.advance(6.)
    print("sample age at claim:", time.time() - hot["sampled_unix"])
    claimed = queue.claim(capacity={"cpu": 2, "mem_gb": 2},
                          cpu_tiers={"preferred": [0, 1], "fallback": []},
                          adaptive_cpu=True, cpu_sampler=lambda: hot)
    assert claimed is None, "stale hot sample admitted the action"
    assert denial(queue, key)["reason"] == "adaptive_cpu_refused"
    assert not queue.ledger().held_keys()
