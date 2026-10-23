"""#2084 acceptance: qualified reference windows are separate from admission.

Interchange contract for this slice (all host names/series here are synthetic):
PRISMABUILD_CPU_AMBIENT_POLICY selects a portable JSON policy, supplied by the
worker from fleet configuration. Paths in it are relative to the policy file.
The selected reference is immutable input, NOT an adaptive snapshot. Production
must validate its entire window, not silently drop invalid rows or clip a line.
No fixture below is empirical host qualification for #2085/#1322.
"""
from __future__ import annotations

from copy import deepcopy
import math
import statistics

import pytest

from fleet_world import FleetWorld
from prismabuild import adaptive_cpu
from prismabuild.digest_primitives import canonical_sha256, sorted_json

TIERS = {"preferred": [2, 4], "fallback": [6, 8]}
CPUS = [2, 4, 6, 8]
ATTRIBUTION = "control-kernel-irq-max-v1"
T0 = 2_000_000.0


def reference_window(host="ambient-test-a", levels=(0.02, 0.04)):
    samples = []
    for index, level in enumerate(levels * 3):
        samples.append({
            "sampled_unix": T0 - 600 + index * 50,
            "interval_s": 1.0,
            "complete": True,
            "holder_free": True,
            "holder_tail_free": True,
            "foreign_per_cpu_busy": {str(cpu): level for cpu in CPUS},
        })
    return {
        "schema": "prismabuild.cpu-ambient-reference.v1",
        "host": host,
        "topology": deepcopy(TIERS),
        "attribution_version": ATTRIBUTION,
        "cadence_s": 50.0,
        "qualification": {"approved": True, "record": "synthetic-test-only"},
        "samples": samples,
    }


@pytest.fixture
def ambient_world(tmp_path, monkeypatch):
    now = {"unix": T0}
    monkeypatch.setattr(adaptive_cpu.time, "time", lambda: now["unix"])
    FleetWorld.use_host(monkeypatch, "ambient-test-a")
    world = FleetWorld(tmp_path)
    ledger = world.queue.ledger()
    ledger.configure_cpu_tiers(TIERS)
    ledger.ensure_capacity({"cpu": 4, "mem_gb": 8})
    policy_path = tmp_path / "ambient-policy.json"
    monkeypatch.setenv("PRISMABUILD_CPU_AMBIENT_POLICY", str(policy_path))

    class WindowHarness:
        def install(self, reference, *, k=2.0, minimum_samples=6, configured_host="ambient-test-a"):
            self.reference_path = tmp_path / "reference.json"
            # Raw import boundary must reject even non-standard NaN/Infinity;
            # a production canonical writer would reject these before import.
            self.reference_path.write_text(sorted_json(reference))
            try:
                self.reference_id = canonical_sha256(reference)
            except ValueError:  # Deliberately malformed finite-JSON cases below.
                self.reference_id = None
            policy_path.write_text(sorted_json({
                "schema": "prismabuild.cpu-ambient-policy.v1",
                "hosts": {configured_host: {
                    "reference": "reference.json", "k": k,
                    "minimum_samples": minimum_samples,
                    "minimum_span_s": 250.0,
                    "minimum_spacing_s": 40.0, "maximum_spacing_s": 60.0,
                    "maximum_reference_samples": 64, "maximum_reference_bytes": 65536,
                }},
            }))

        def sample(self, level=0.08, **changes):
            per_cpu = {str(cpu): level for cpu in CPUS}
            return {"sampled_unix": now["unix"], "cpu_count": 4,
                    "interval_s": 1.0, "psi_some": 0.0,
                    "busy_cpus": sum(per_cpu.values()),
                    "per_cpu_busy": dict(per_cpu),
                    "foreign_per_cpu_busy": dict(per_cpu), **changes}

        def controller(self, sample=None):
            controller = adaptive_cpu.Controller(ledger, TIERS,
                sampler=lambda: self.sample() if sample is None else sample)
            # Diagnostic PIDs are not part of this synthetic observation.
            monkeypatch.setattr(controller, "_foreign_pids", lambda cpus, stamp: {})
            return controller

        def verdict(self, sample=None):
            return self.controller()._measurement_foreign_ambient(
                self.sample() if sample is None else sample, [], 2)

        def advance(self, seconds):
            now["unix"] += seconds

    return WindowHarness()


def policy_evidence(verdict):
    assert verdict is not None, "complete observations must have a typed verdict"
    evidence = verdict["evidence"].get("ambient_policy")
    assert isinstance(evidence, dict), (
        "typed ambient evidence must distinguish qualified max+k*pstdev from "
        "the unmeasured 0.10 fallback")
    return evidence


@pytest.mark.parametrize("host,levels,current,expected", [
    ("ambient-test-a", (0.02, 0.04), 0.08, True),
    ("ambient-test-b", (0.14, 0.18), 0.19, False),
])
def test_distinct_host_windows_replace_the_flat_line(ambient_world, monkeypatch, host, levels, current, expected):
    h = ambient_world
    FleetWorld.use_host(monkeypatch, host)
    ref = reference_window(host=host, levels=levels)
    h.install(ref, configured_host=host)
    verdict = h.verdict(h.sample(current))
    assert verdict["exceeds"] is expected, (
        "use this host's qualified per-CPU window, not PER_CPU_FOREIGN_MAX")
    assert verdict["clear"] is (not expected)
    evidence = policy_evidence(verdict)
    assert evidence["basis"] == "measured"
    assert evidence["reference_id"] == h.reference_id
    assert evidence["k"] == 2.0
    assert evidence["count"] == 6
    assert evidence["span_s"] == 250.0
    values = list(levels * 3)
    for cpu in ("2", "4"):
        line = evidence["per_cpu"][cpu]
        assert line["max"] == max(values)
        assert line["stdev"] == pytest.approx(statistics.pstdev(values), abs=1e-15)
        assert line["threshold"] == max(values) + 2 * statistics.pstdev(values)


@pytest.mark.parametrize("offset,exceeds", [(-1, False), (0, False), (1, True)])
def test_population_stdev_strict_unrounded_boundary(ambient_world, offset, exceeds):
    h = ambient_world
    ref = reference_window(levels=(0.011234567, 0.034567891))
    h.install(ref, k=1.5)
    values = [r["foreign_per_cpu_busy"]["2"] for r in ref["samples"]]
    line = max(values) + 1.5 * statistics.pstdev(values)
    value = line if offset == 0 else math.nextafter(line, math.inf if offset > 0 else -math.inf)
    verdict = h.verdict(h.sample(value))
    assert verdict["exceeds"] is exceeds, "strict > must precede diagnostic rounding"
    assert policy_evidence(verdict)["per_cpu"]["2"]["threshold"] == line


def test_per_cpu_maxima_are_not_pooled_across_the_host(ambient_world):
    h = ambient_world
    ref = reference_window()
    for row in ref["samples"]:
        row["foreign_per_cpu_busy"]["4"] = 0.3
    h.install(ref)
    sample = h.sample(0.08)
    sample["foreign_per_cpu_busy"]["4"] = 0.29
    sample["per_cpu_busy"]["4"] = 0.29
    sample["busy_cpus"] = sum(sample["per_cpu_busy"].values())
    verdict = h.verdict(sample)
    assert verdict["exceeds"] is True
    assert verdict["evidence"]["foreign_cpus"] == [2]
    evidence = policy_evidence(verdict)
    assert evidence["per_cpu"]["4"]["stdev"] == 0
    assert evidence["per_cpu"]["4"]["threshold"] == 0.3


@pytest.mark.parametrize("defect", [
    "wrong_host", "wrong_ids_same_count", "wrong_tiers", "wrong_attribution",
    "unapproved", "short_count", "short_span", "duplicate_time", "bad_cadence",
    "no_interval", "incomplete", "partial_cpu", "nan", "infinite", "boolean",
    "negative", "above_one", "holder", "holder_tail", "missing_holder_proof",
    "future_time", "disabled_detector", "too_many_rows", "oversized_reference",
    "nan_time", "negative_interval",])
def test_invalid_reference_is_unmeasured_not_repaired(ambient_world, defect):
    h = ambient_world
    ref = reference_window()
    row = ref["samples"][0]
    if defect == "wrong_host":
        ref["host"] = "ambient-test-b"
    elif defect == "wrong_ids_same_count":
        ref["topology"] = {"preferred": [0, 1], "fallback": [6, 8]}
    elif defect == "wrong_tiers":
        ref["topology"] = {"preferred": [6, 8], "fallback": [2, 4]}
    elif defect == "wrong_attribution":
        ref["attribution_version"] = "older-unadjusted-attribution"
    elif defect == "unapproved":
        ref["qualification"]["approved"] = False
    elif defect == "short_count":
        ref["samples"].pop()
    elif defect == "short_span":
        for index, sample in enumerate(ref["samples"]):
            sample["sampled_unix"] = T0 - 600 + index
    elif defect == "duplicate_time":
        ref["samples"][1]["sampled_unix"] = row["sampled_unix"]
    elif defect == "bad_cadence":
        ref.pop("cadence_s")
    elif defect == "no_interval":
        row.pop("interval_s")
    elif defect == "incomplete":
        row["complete"] = False
    elif defect == "partial_cpu":
        row["foreign_per_cpu_busy"].pop("8")
    elif defect in ("nan", "infinite", "boolean", "negative", "above_one"):
        row["foreign_per_cpu_busy"]["8"] = {
            "nan": float("nan"), "infinite": float("inf"), "boolean": True,
            "negative": -0.01, "above_one": 1.01}[defect]
    elif defect == "holder":
        row["holder_free"] = False
    elif defect == "holder_tail":
        row["holder_tail_free"] = False
    elif defect == "missing_holder_proof":
        row.pop("holder_tail_free")
    elif defect == "future_time":
        ref["samples"][-1]["sampled_unix"] = T0 + 10
    elif defect == "disabled_detector":
        for sample in ref["samples"]:
            sample["foreign_per_cpu_busy"] = {str(cpu): 1.0 for cpu in CPUS}
    elif defect == "too_many_rows":
        ref["samples"] = [dict(deepcopy(row), sampled_unix=T0 - 10000 + i * 50)
                          for i in range(65)]
    elif defect == "oversized_reference":
        ref["qualification"]["record"] = "x" * 65537
    elif defect == "nan_time":
        row["sampled_unix"] = float("nan")
    elif defect == "negative_interval":
        row["interval_s"] = -1.0
    h.install(ref)
    verdict = h.verdict(h.sample(0.11))
    assert verdict["exceeds"] is True, "invalid calibration must not disable detection"
    evidence = policy_evidence(verdict)
    assert evidence["basis"] == "unmeasured"
    assert evidence["reason"], "record why qualification/reference validation failed"
    assert evidence["per_cpu"]["2"]["threshold"] == 0.10


@pytest.mark.parametrize("k", [-1.0, True, float("nan"), float("inf")])
def test_invalid_k_cannot_create_measured_policy(ambient_world, k):
    h = ambient_world
    h.install(reference_window(), k=k)
    evidence = policy_evidence(h.verdict(h.sample(0.11)))
    assert evidence["basis"] == "unmeasured"
    assert evidence["reason"]


def test_missing_reference_has_explicit_rolling_compatible_fallback(ambient_world):
    h = ambient_world
    h.install(reference_window())
    h.reference_path.unlink()
    verdict = h.verdict(h.sample(0.11))
    assert verdict["exceeds"] is True
    assert policy_evidence(verdict)["basis"] == "unmeasured"


def test_sustained_load_cannot_train_reference_and_clear_recovers_after_restart(ambient_world):
    h = ambient_world
    h.install(reference_window())
    original = h.reference_path.read_bytes()
    item = {"action_key": "synthetic-measurement", "resources": {"cpu": 2}}
    for _ in range(8):
        # Real idle-history persistence plus new controllers on each pass.
        # Span exceeds provisional promotion AND the independent fairness budget.
        controller = h.controller(h.sample(0.08))
        result = controller.decision(item, {"cpu": 2}, identity=("shape", True))
        assert result is None, "a currently refused load must not train away its refusal"
        assert controller.last_decision["reason"] == "measurement_foreign_ambient"
        assert h.reference_path.read_bytes() == original
        h.advance(61)
    controller = h.controller(h.sample(0.05))
    assert controller.decision(item, {"cpu": 2}, identity=("shape", True)) is not None
    decision = controller.last_decision
    assert decision["reason"] == "admitted"
    clear = decision["measurement_pool_drain"]
    assert clear["foreign_clear"] is True
    assert clear["ambient_policy"]["reference_id"] == h.reference_id
    assert h.reference_path.read_bytes() == original


def test_approved_reference_can_be_explicitly_replaced(ambient_world):
    h = ambient_world
    h.install(reference_window())
    first_id = h.reference_id
    assert h.verdict(h.sample(0.08))["exceeds"] is True
    h.install(reference_window(levels=(0.05, 0.07)))
    verdict = h.verdict(h.sample(0.08))
    assert verdict["clear"] is True
    evidence = policy_evidence(verdict)
    assert evidence["reference_id"] != first_id
    assert evidence["reference_id"] == h.reference_id


def test_measured_policy_preserves_assigned_and_held_cpu_scope(ambient_world, monkeypatch):
    h = ambient_world
    h.install(reference_window())
    sample = h.sample(0.01)
    sample["foreign_per_cpu_busy"].update({"2": 0.5, "8": 0.8})
    sample["per_cpu_busy"].update({"2": 0.5, "8": 0.8})
    sample["busy_cpus"] = sum(sample["per_cpu_busy"].values())
    controller = h.controller(sample)
    monkeypatch.setattr(controller, "_held_cpus", lambda holders: {2})
    verdict = controller._measurement_foreign_ambient(sample, [], 2)
    assert verdict["clear"] is True, "CPU 2 is PB-held; CPU 8 is outside the allocation"
    assert policy_evidence(verdict)["basis"] == "measured"


def test_shared_snapshot_does_not_override_selected_reference(ambient_world):
    h = ambient_world
    h.install(reference_window())
    controller = h.controller()
    adaptive_cpu.write_json(controller.ledger.base / "adaptive" / "cpu-ambient-policy.json", {
        "threshold": 1.0, "qualification": {"approved": True}})
    verdict = h.verdict(h.sample(0.08))
    assert verdict["exceeds"] is True
    assert policy_evidence(verdict)["reference_id"] == h.reference_id
