"""Qualification reasons and funding arithmetic over already observed values."""
import copy

import pytest

from prismabuild import candidate_policy as policy


@pytest.mark.parametrize("state,reason", [
    ("lead_not_resident", "residency_lead_not_resident"),
    ("lead_unpinned", "residency_lead_unpinned"),
    ("map_not_composed", "residency_map_not_composed"),
    ("map_stale", "residency_map_stale"),
    ("map_unreadable", "residency_map_unreadable"),
    ("plan_unreadable", "residency_plan_unreadable"),
    ("ram_epoch_stale", "residency_ram_epoch_stale"),
    ("prelaunch_undeclared", "residency_prelaunch_undeclared"),
    ("map_incomplete", "residency_map_incomplete"),
], ids=["waiting", "unpinned", "uncomposed", "stale-map", "unreadable-map",
        "unreadable-plan", "old-epoch", "prelaunch-shape", "incomplete-map"])
def test_each_residency_refusal_keeps_its_wire_reason(state, reason):
    assert policy.residency_denial({"state": state}) == reason


@pytest.mark.parametrize("state", [
    "resident", "not_requested", "no_leads", "unknown-state",
], ids=["resident", "advisory", "no-leads", "other-state"])
def test_other_residency_states_do_not_add_a_refusal(state):
    assert policy.residency_denial({"state": state}) is None


@pytest.mark.parametrize("verdict,reason", [
    ({"state": "lead_not_resident", "pending": [{"status": "failed"}]},
     "residency_lead_terminal"),
    ({"state": "lead_unpinned", "pending": [{"status": "unpinned"}]},
     "residency_lead_terminal"),
    ({"state": "lead_not_resident", "pending": [{"status": "failed"}, {"status": "dropped"}]},
     "residency_lead_terminal"),
    ({"state": "lead_not_resident", "pending": [{"status": "failed"}, {"status": "claimed"}]},
     "residency_lead_not_resident"),
    ({"state": "lead_not_resident", "pending": [{"status": "ready"}]},
     "residency_lead_not_resident"),
    ({"state": "lead_not_resident", "pending": [{"status": "absent"}]},
     "residency_lead_not_resident"),
    ({"state": "lead_not_resident", "pending": [{"status": None}]},
     "residency_lead_not_resident"),
    ({"state": "lead_not_resident", "pending": [{}]}, "residency_lead_not_resident"),
    ({"state": "lead_not_resident", "pending": []}, "residency_lead_not_resident"),
    ({"state": "lead_not_resident", "pending": ({"status": "failed"},)},
     "residency_lead_not_resident"),
    ({"state": "lead_not_resident", "pending": ["failed"]}, "residency_lead_not_resident"),
    ({"state": "lead_not_resident", "pending": [{"status": False}]},
     "residency_lead_terminal"),
    ({"state": "map_unreadable", "pending": [{"status": "failed"}]},
     "residency_map_unreadable"),
    ({"state": "prelaunch_undeclared", "pending": [{"status": "failed"}]},
     "residency_prelaunch_undeclared"),
], ids=["failed", "unpinned", "all-ended", "running-successor", "queued-successor",
        "absent", "null-status", "missing-status", "empty", "non-list", "non-mapping",
        "false-is-not-null", "map-error", "shape-error"])
def test_terminal_reason_requires_a_nonempty_list_of_known_endings(verdict, reason):
    original = copy.deepcopy(verdict)
    assert policy.residency_denial(verdict) == reason
    assert verdict == original


def test_missing_residency_state_remains_an_error():
    with pytest.raises(KeyError, match="state"):
        policy.residency_denial({})


class UnreadablePending(dict):
    def __init__(self, state, error):
        super().__init__(state=state)
        self.error = error

    def get(self, key, default=None):
        if key == "pending":
            raise self.error
        return super().get(key, default)


def test_allowed_residency_does_not_read_terminal_evidence():
    verdict = UnreadablePending("resident", RuntimeError("unavailable pending evidence"))
    assert policy.residency_denial(verdict) is None


def test_terminal_evidence_failure_preserves_the_original_exception():
    error = OSError("terminal evidence unavailable")
    with pytest.raises(OSError) as caught:
        policy.residency_denial(UnreadablePending("lead_not_resident", error))
    assert caught.value is error


@pytest.mark.parametrize("demand,present,reason", [
    ({"cpu": 1}, False, "admission_capacity_required"),
    ({}, False, "admission_capacity_required"),
    ({}, True, "admission_demand_required"),
    ({"cpu": 1}, True, None),
    ({"spool_gb": 2}, True, None),
], ids=["missing-capacity", "capacity-before-demand", "empty-host-demand",
        "cpu-demand", "other-host-kind"])
def test_admission_requires_observed_capacity_then_declared_host_demand(demand, present, reason):
    assert policy.admission_denial(demand, capacity_present=present) == reason


class UnreadableDemand(dict):
    def __init__(self, error):
        super().__init__()
        self.error = error

    def __bool__(self):
        raise self.error


def test_missing_capacity_does_not_inspect_host_demand():
    demand = UnreadableDemand(RuntimeError("demand unavailable"))
    assert policy.admission_denial(demand, capacity_present=False) == "admission_capacity_required"


def test_present_capacity_keeps_the_original_demand_error():
    error = ValueError("demand unavailable")
    with pytest.raises(ValueError) as caught:
        policy.admission_denial(UnreadableDemand(error), capacity_present=True)
    assert caught.value is error


@pytest.mark.parametrize("reservation,total,refused", [
    ({"cpu": 2, "gpu": 1}, {"cpu": 2, "gpu": 1}, False),
    ({"cpu": 2, "gpu": 1}, {"cpu": 8, "gpu": 1}, False),
    ({"cpu": 3}, {"cpu": 2}, True),
    ({"cpu": 1, "mem_gb": 1}, {"cpu": 2}, True),
    ({"gpu": 1}, {"cpu": 2}, True),
    ({}, {}, False),
], ids=["exact-normalized-gpu", "spare-cpu", "cpu-too-wide", "missing-memory",
        "missing-gpu", "empty"])
def test_total_fit_uses_the_reservation_and_missing_kinds_are_zero(reservation, total, refused):
    assert policy.never_fits(reservation, total) is refused


def test_total_fit_stops_after_the_first_shortfall():
    assert policy.never_fits({"cpu": 2, "mem_gb": "unreadable"}, {"cpu": 1}) is True


def test_total_fit_does_not_coerce_a_malformed_need():
    with pytest.raises(TypeError):
        policy.never_fits({"cpu": "2"}, {"cpu": 3})


@pytest.mark.parametrize("demand,adaptive,remainder", [
    ({"cpu": 4, "mem_gb": 8, "gpu": 1, "spool_gb": 3},
     {"funded": {"cpus": ["0", "1"], "mem_gb": 5}},
     {"cpu": 2, "mem_gb": 3, "gpu": 1, "spool_gb": 3}),
    ({"cpu": 2, "mem_gb": 4}, {"funded": {"cpus": ["0", "1"], "mem_gb": 4}}, {}),
    ({"cpu": 1, "mem_gb": 2}, {"funded": {"cpus": ["0", "1"], "mem_gb": 4}}, {}),
    ({"gpu": 1}, {"funded": {"cpus": ["0", "1"], "mem_gb": 4}}, {"gpu": 1}),
    ({"cpu": "4", "mem_gb": "5", "gpu": "1"},
     {"funded": {"cpus": ("0",), "mem_gb": "2"}}, {"cpu": 3, "mem_gb": 3, "gpu": 1}),
    ({"mem_gb": 4}, {"funded": {"cpus": [], "mem_gb": 3.9}}, {"mem_gb": 1}),
], ids=["partial-with-other-kinds", "exactly-covered", "over-covered", "unrelated-kind",
        "existing-integer-coercions", "existing-memory-truncation"])
def test_funded_remainder_uses_cpu_names_and_memory_without_mutating_inputs(demand, adaptive, remainder):
    before_demand = copy.deepcopy(demand)
    before_adaptive = copy.deepcopy(adaptive)
    result = policy.funded_remainder(demand, adaptive)
    assert result == remainder
    assert result is not demand
    assert demand == before_demand
    assert adaptive == before_adaptive


@pytest.mark.parametrize("demand,adaptive,error", [
    ({"cpu": 1}, {}, KeyError),
    ({"cpu": 1}, {"funded": {}}, KeyError),
    ({"cpu": 1}, {"funded": {"cpus": []}}, KeyError),
    ({"cpu": 1}, {"funded": {"cpus": None, "mem_gb": "invalid"}}, TypeError),
    ({"cpu": 1}, {"funded": {"cpus": [], "mem_gb": "invalid"}}, ValueError),
    ({"cpu": "invalid"}, {"funded": {"cpus": [], "mem_gb": 0}}, ValueError),
], ids=["missing-funded", "missing-cpus", "missing-memory", "cpu-length-before-memory",
        "invalid-memory", "invalid-need"])
def test_malformed_funding_keeps_its_existing_exception_class(demand, adaptive, error):
    with pytest.raises(error):
        policy.funded_remainder(demand, adaptive)


def test_cpu_funding_read_failure_precedes_memory_conversion_and_keeps_exception_identity():
    error = OSError("funded CPU names unavailable")

    class UnreadableCpus:
        def __len__(self):
            raise error

    with pytest.raises(OSError) as caught:
        policy.funded_remainder({"cpu": 1}, {"funded": {"cpus": UnreadableCpus(), "mem_gb": "invalid"}})
    assert caught.value is error
