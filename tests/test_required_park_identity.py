"""A matching filename cannot replace a concrete current drain identity."""
import json

import pytest

import fleet_membership
from test_park_proof_real_writer import parked_role


@pytest.fixture(params=[
    pytest.param(("stamp", True), id="bool-true"),
    pytest.param(("stamp", False), id="bool-false"),
    pytest.param(("stamp", "101.5"), id="numeric-string"),
    pytest.param(("stamp", "epoch"), id="opaque-string"),
    pytest.param(("stamp", float("nan")), id="nan"),
    pytest.param(("stamp", float("inf")), id="positive-infinity"),
    pytest.param(("stamp", float("-inf")), id="negative-infinity"),
    pytest.param(("stamp", None), id="null"),
    pytest.param(("missing-key", None), id="missing-key"),
    pytest.param(("missing-file", None), id="missing-file"),
    pytest.param(("malformed-file", None), id="malformed-file"),
])
def invalid_drain(request, parked_role):
    kind, stamp = request.param
    with parked_role() as role:
        if kind == "stamp":
            role.set_gate(stamp)
        elif kind == "missing-key":
            role.gate.write_text(json.dumps({"draining": True}))
        elif kind == "missing-file":
            role.gate.unlink()
        else:
            role.gate.write_text("{not JSON")
        role.post()
        yield role


def test_host_cannot_certify_a_matching_marker_without_known_gate_identity(
        invalid_drain):
    observed = invalid_drain.client.drain_evidence(
        {"draining": True, "active_scopes": 0})
    assert observed.get("drained") is False, observed


def test_membership_accepts_no_markers_without_known_gate_identity(invalid_drain):
    gate = fleet_membership.read_gate(invalid_drain.gate)
    accepted = fleet_membership.parked_markers(invalid_drain.parked, gate)
    assert accepted == [], [path.name for path in accepted]


@pytest.mark.parametrize("stamp", [0, 101, 101.5])
def test_finite_numeric_gate_identity_accepts_the_exact_real_writer(parked_role, stamp):
    with parked_role() as role:
        role.set_gate(stamp)
        pid, starttime, _ = role.census[0]
        expected = f"{pid}-{starttime}-{stamp}"
        assert role.post() == expected
        observed = role.client.drain_evidence({"draining": True, "active_scopes": 0})
        assert observed == {"drained": True, "unparked": [], "active_scopes": 0}
        gate = fleet_membership.read_gate(role.gate)
        accepted = fleet_membership.parked_markers(role.parked, gate)
        assert [path.name for path in accepted] == [expected]
