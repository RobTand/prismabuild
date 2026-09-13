"""Discrete-VRAM receipt telemetry is sampled from broker snapshots (#548)."""
from __future__ import annotations

import time

from prismabuild import box_window
from prismabuild import pool


KEY = "a" * 64
NONCE = "b" * 32
SCOPE = "prismabuild-job" + "c" * 32 + ".slice"
GIB = 1024 ** 3


def _sample(*, sample_id="one", sampled_unix=None, used=4 * GIB,
            total=16 * GIB, free=None):
    if sampled_unix is None:
        sampled_unix = time.time()
    if free is None:
        free = total - used
    return {
        "schema": "prismabuild.gpu_capacity.v1",
        "sample_id": sample_id,
        "sampled_unix": sampled_unix,
        "complete": True,
        "attributed": True,
        "devices": [{
            "uuid": "GPU-amd", "name": "AMD Radeon RX 9070 XT",
            "sampled_unix": sampled_unix,
            "telemetry_class": "memory_only", "memory_domain": "discrete",
            "memory_total_bytes": total, "memory_free_bytes": free,
            "memory_used_bytes": used,
        }],
        "jobs": [{"action_key": KEY, "nonce": NONCE, "scope_id": SCOPE,
                  "complete": True}],
    }


def test_discrete_framebuffer_window_peaks_valid_distinct_broker_samples():
    window = box_window.DiscreteFramebufferWindow(KEY, NONCE, SCOPE)
    now = time.time()
    assert window.observe(_sample(sample_id="first", sampled_unix=now - 1, used=4 * GIB), now=now)
    assert not window.observe(_sample(sample_id="first", sampled_unix=now - .5, used=12 * GIB), now=now)
    assert window.observe(_sample(sample_id="second", sampled_unix=now - .5, used=10 * GIB), now=now)

    group = window.group()
    assert group == {
        "source": "broker_gpu_capacity", "telemetry_class": "memory_only",
        "memory_domain": "discrete", "device_uuid": "GPU-amd",
        "device_name": "AMD Radeon RX 9070 XT", "samples": 2,
        "first_sampled_unix": group["first_sampled_unix"],
        "last_sampled_unix": group["last_sampled_unix"],
        "framebuffer_total_bytes": 16 * GIB,
        "framebuffer_used_bytes_peak": 10 * GIB,
        "framebuffer_free_bytes_min": 6 * GIB,
    }
    assert group["last_sampled_unix"] >= group["first_sampled_unix"]


def test_framebuffer_window_rejects_missing_malformed_stale_and_non_discrete_samples():
    window = box_window.DiscreteFramebufferWindow(KEY, NONCE, SCOPE)
    now = time.time()
    bad = [
        {},
        _sample(sampled_unix=now - 6),
        _sample(total=16 * GIB, free=13 * GIB, used=4 * GIB),
        {**_sample(), "devices": [{**_sample()["devices"][0], "memory_domain": "shared_system"}]},
        {**_sample(), "jobs": []},
    ]
    for sample in bad:
        assert not window.observe(sample, now=now)
    assert window.group() is None


def test_framebuffer_window_keeps_whole_device_measurement_when_global_attribution_is_incomplete():
    now = time.time()
    window = box_window.DiscreteFramebufferWindow(KEY, NONCE, SCOPE)
    sample = _sample(sampled_unix=now - 1)
    sample.update(complete=False, attributed=False,
                  foreign_processes=[{"pid": 9, "used_bytes": None}])
    assert window.observe(sample, now=now)
    assert window.group()["framebuffer_used_bytes_peak"] == 4 * GIB


def test_resource_profile_places_sampled_framebuffer_in_box_window(monkeypatch, tmp_path):
    now = time.time()
    accumulator = box_window.DiscreteFramebufferWindow(KEY, NONCE, SCOPE)
    assert accumulator.observe(_sample(sampled_unix=now - 1), now=now)
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(queue, "_box_window", lambda *args: {
        "schema": box_window.BOX_WINDOW_SCHEMA_V1, "source": "unavailable",
        "reason": "no recorder answered",
    })
    profile = queue._resource_profile(
        {"claimed_unix": now - 2},
        {"gpu_framebuffer_window": accumulator.group()},
    )
    gpu = profile["box_window"]["gpu"]
    assert profile["box_window"]["source"] == "broker_gpu_capacity"
    assert "reason" not in profile["box_window"]
    assert gpu["source"] == "broker_gpu_capacity"
    assert gpu["framebuffer_used_bytes_peak"] == 4 * GIB
    summary = pool.resource_profile_summary({"resource_profile": profile})
    assert summary["gpu_framebuffer_used_peak_bytes"] == 4 * GIB
    assert "vram=4.0G/16.0G" in pool.describe_resource_profile(summary)


def test_scope_sampler_uses_shared_broker_snapshot_without_a_finish_probe(monkeypatch):
    now = time.time()

    class Scope:
        _pool_accounting_valid = True
        _framebuffer_window = box_window.DiscreteFramebufferWindow(KEY, NONCE, SCOPE)

        def sample(self):
            return {"complete": True}

        def _request(self, operation):
            assert operation == "status"
            return {}

        def write_telemetry(self, telemetry):
            self.telemetry = telemetry

    scope = Scope()
    reads = []
    monkeypatch.setattr(pool.gpu_admission, "trusted_sample", lambda: reads.append(1) or _sample(
        sampled_unix=now - 1))
    monkeypatch.setattr(pool, "_now", lambda: now)
    pool.PoolQueue._sample_resource_scope(scope)
    assert reads == [1]
    assert scope._framebuffer_window.group()["samples"] == 1


def test_scope_sampler_keeps_prior_framebuffer_peak_when_snapshot_read_fails(monkeypatch):
    now = time.time()

    class Scope:
        _pool_accounting_valid = True
        _framebuffer_window = box_window.DiscreteFramebufferWindow(KEY, NONCE, SCOPE)

        def sample(self):
            return {"complete": True}

        def _request(self, operation):
            return {}

        def write_telemetry(self, telemetry):
            self.telemetry = telemetry

    scope = Scope()
    assert scope._framebuffer_window.observe(_sample(sampled_unix=now - 1), now=now)
    monkeypatch.setattr(pool.gpu_admission, "trusted_sample",
                        lambda: (_ for _ in ()).throw(OSError("read failed")))
    monkeypatch.setattr(pool, "_now", lambda: now)
    telemetry = pool.PoolQueue._sample_resource_scope(scope)
    assert telemetry["complete"] is True
    assert "snapshot unavailable: OSError" in telemetry["gpu_framebuffer_error"]
    assert scope._framebuffer_window.group()["framebuffer_used_bytes_peak"] == 4 * GIB
