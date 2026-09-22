"""Receipts, metrics and placement divide GPU watts by a GPU-only reference.

Issue #806, residual leg.  PR #813 gave *admission* a GPU-only reference:
``adaptive_gpu.admission_power_reference``.  Everything else that publishes a
power ratio -- the receipt's box window, the ending summary ``pbstatus`` and
``pbrun`` print, the ``pbmetrics`` gauges, and the cross-resource placement
proxy -- still divided a GPU-only ``power.draw`` by the 140 W whole-SoC TDP,
and printed the quotient with no scope beside it.

These tests pin two things.  One reference: the reporting path resolves it
through the same ``adaptive_gpu`` helper admission uses, so a measured peak
recorded by admission is the number a receipt divides by too.  One label:
every published ratio carries the scope it was taken against, so a reader can
tell a measured GPU ceiling from the SoC envelope that is only a fallback.
"""
from __future__ import annotations

import math
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import adaptive_gpu, box_capacity, box_window, pool  # noqa: E402

GIB = 1024**3
DECLARED = {"cpu": 10, "gpu": 1, "mem_gb": 72}

#: A GB10 as ``gpu_capacity`` publishes it: GPU-only ``power_w``, no
#: programmable limit, and the vendor SoC TDP carried as provenance.
def gb10_device(**overrides):
    device = {
        "uuid": "GPU-1", "name": "NVIDIA GB10", "power_w": 70.0,
        "power_limit_w": None, "power_reference_w": 140.0,
        "power_reference_scope": "soc_tdp",
        "power_reference_source": "https://docs.nvidia.com/dgx/dgx-spark/hardware.html",
        "power_measurement_scope": "gpu", "memory_domain": "shared_system",
        "memory_total_bytes": 120 * GIB, "memory_free_bytes": 80 * GIB,
        "memory_used_bytes": 40 * GIB, "limited": False,
    }
    device.update(overrides)
    return device


def gpu_sample(device):
    now = time.time()
    return {
        "schema": box_capacity.GPU_CAPACITY_SCHEMA, "sample_id": "a" * 32,
        "sampled_unix": now, "published_unix": now,
        "complete": True, "attributed": True, "devices": [device],
        "host_total_bytes": 120 * GIB, "host_available_bytes": 80 * GIB,
        "memory_pressure_some": 0.0, "memory_pressure_full": 0.0,
        "cpu_pressure_some": 0.0, "cpu_pressure_full": 0.0,
        "foreign_processes": [], "jobs": [],
    }


# --------------------------------------------------------------------------
# The shared derivation.


def test_reporting_reference_is_the_reference_admission_uses():
    """One helper, so a receipt and a refusal cannot name different watts."""

    device, state = gb10_device(), {"power_peaks": {"GPU-1": 114.0}}

    assert adaptive_gpu.reporting_power_reference(device, state) == \
        adaptive_gpu.admission_power_reference(device, state)
    assert adaptive_gpu.reporting_power_reference(device, state)[:2] == (114.0, "measured_peak")
    assert adaptive_gpu.reporting_power_reference(device, {})[:2] == (100.0, "declared_fallback")


def test_soc_tdp_survives_only_as_a_labelled_fallback():
    """An unbacked device may still be reported -- never as an unlabelled ratio."""

    watts, scope, _ = adaptive_gpu.reporting_power_reference(
        gb10_device(name="NVIDIA Unknown-Q"), {})

    assert (watts, scope) == (140.0, "soc_tdp")
    # ...and admission itself still has no reference for it, so this fallback
    # is a reporting affordance and never an admission grade one.
    assert adaptive_gpu.admission_power_reference(
        gb10_device(name="NVIDIA Unknown-Q"), {}) == (None, None, None)


def test_a_device_with_no_reference_scope_reports_nothing():
    """Unchanged: a reference nobody scoped is not promoted into a denominator."""

    assert adaptive_gpu.reporting_power_reference(
        gb10_device(power_reference_scope="unknown"), {}) == (None, None, None)


# --------------------------------------------------------------------------
# Receipts: box window -> ending summary -> the line pbstatus and pbrun print.


def test_box_window_reference_is_gpu_only_and_scoped(monkeypatch):
    from prismabuild import gpu_capacity

    monkeypatch.setattr(gpu_capacity, "devices",
                        lambda **_: ([gb10_device()], []))

    assert box_window.gpu_power_reference() == {
        "power_reference_w": 100.0, "power_reference_scope": "declared_fallback",
        "power_reference_source": adaptive_gpu.DECLARED_GPU_POWER_REFERENCE_SOURCE,
    }
    peaked = box_window.gpu_power_reference(state={"power_peaks": {"GPU-1": 114.0}})
    assert peaked["power_reference_w"] == 114.0
    assert peaked["power_reference_scope"] == "measured_peak"


def test_ending_summary_and_its_line_carry_the_scope():
    """``gpu=70.0W/140.0W(50%)`` said nothing about what 140 W was."""

    detail = {"resource_profile": {"box_window": {"gpu": {
        "source": "pqteld", "power_w_peak": 70.0,
        "power_reference_w": 100.0, "power_reference_scope": "declared_fallback",
        "power_peak_fraction_of_reference": 0.70,
    }}}}
    summary = pool.resource_profile_summary(detail)

    assert "gpu_power_reference_scope" in pool.RESOURCE_SUMMARY_FIELDS
    assert summary["gpu_power_reference_scope"] == "declared_fallback"
    assert summary["gpu_power_reference_w"] == 100.0
    described = pool.describe_resource_profile(summary)
    assert "gpu=70.0W/100.0W(70%,declared_fallback)" in described


def test_an_ending_with_no_scope_says_so_rather_than_implying_one():
    """Records filed before this change carry the field as ``None``."""

    summary = pool.resource_profile_summary({"resource_profile": {"box_window": {"gpu": {
        "power_w_peak": 70.0, "power_reference_w": 140.0,
        "power_peak_fraction_of_reference": 0.5}}}})

    assert summary["gpu_power_reference_scope"] is None
    assert "gpu=70.0W/140.0W(50%,unknown)" in pool.describe_resource_profile(summary)


# --------------------------------------------------------------------------
# Cross-resource placement.


def test_placement_fraction_is_gpu_only_and_scoped():
    """105 W of a GB10 is 105 % of its measured ceiling, not 75 % of the SoC."""

    seen = box_capacity.observe(
        DECLARED, {}, gpu_sample=gpu_sample(gb10_device(power_w=105.0)),
        mem_gb=100, load1=3)

    assert seen.detail["gpu_power_reference_w"] == 100.0
    assert seen.detail["gpu_power_reference_scope"] == "declared_fallback"
    assert seen.detail["gpu_power_measured_fraction"] == pytest.approx(1.05)


def test_placement_reads_the_peak_admission_recorded():
    seen = box_capacity.observe(
        DECLARED, {}, gpu_sample=gpu_sample(gb10_device(power_w=57.0)),
        mem_gb=100, load1=3, gpu_state={"power_peaks": {"GPU-1": 114.0}})

    assert seen.detail["gpu_power_reference_w"] == 114.0
    assert seen.detail["gpu_power_reference_scope"] == "measured_peak"
    assert seen.detail["gpu_power_measured_fraction"] == pytest.approx(0.5)


def test_placement_keeps_the_labelled_soc_fallback_for_an_unbacked_device():
    seen = box_capacity.observe(
        DECLARED, {}, gpu_sample=gpu_sample(
            gb10_device(name="NVIDIA Unknown-Q", power_w=105.0)),
        mem_gb=100, load1=3)

    assert seen.detail["gpu_power_reference_scope"] == "soc_tdp"
    assert seen.detail["gpu_power_measured_fraction"] == pytest.approx(0.75)


# --------------------------------------------------------------------------
# Metrics.


def test_pbmetrics_never_maxes_two_denominators_together():
    """A window spanning the runtime change holds both scopes; ``max`` of the
    two fractions would be a number taken against neither reference."""

    import pbmetrics

    metrics = pbmetrics.Metrics()
    rows = [
        {"host": "sparky", "status": "succeeded", "gpu_power_peak_fraction": 0.50,
         "gpu_power_reference_w": 140.0, "gpu_power_reference_scope": None},
        {"host": "sparky", "status": "succeeded", "gpu_power_peak_fraction": 0.70,
         "gpu_power_reference_w": 100.0, "memory_peak_bytes": 4 * GIB,
         "gpu_power_reference_scope": "declared_fallback"},
    ]
    pbmetrics._box_window_peaks(metrics, rows)
    rendered = metrics.render()

    assert 'metric="gpu_power_peak_fraction",scope="declared_fallback"' in rendered
    assert 'metric="gpu_power_peak_fraction",scope="unknown"' in rendered
    # Metrics with no reference carry an empty scope, which Prometheus reads
    # as no label at all, so one family keeps one label set.
    assert 'metric="memory_peak_bytes",scope=""' in rendered
