"""Legacy matching markers cannot certify unknown required drain identity."""
import json

import pytest

import fleet_membership
from test_client_proof_prior_trust import tmp_path
from test_park_proof_real_writer import parked_role


@pytest.fixture(params=[
    pytest.param(("stamp", True, "True"), id="bool-true"),
    pytest.param(("stamp", False, "False"), id="bool-false"),
    pytest.param(("stamp", "101.5", "101.5"), id="numeric-string"),
    pytest.param(("stamp", "epoch", "epoch"), id="opaque-string"),
    pytest.param(("stamp", float("nan"), "nan"), id="nan"),
    pytest.param(("stamp", float("inf"), "inf"), id="positive-infinity"),
    pytest.param(("stamp", float("-inf"), "_inf"), id="negative-infinity"),
    pytest.param(("stamp", None, "unknown"), id="null"),
    pytest.param(("missing-key", None, "unknown"), id="missing-key"),
    pytest.param(("missing-file", None, "unknown"), id="missing-file"),
    pytest.param(("malformed-file", None, "unknown"), id="malformed-file"),
])
def invalid_drain(request, parked_role):
    kind, stamp, historical_key = request.param
    with parked_role() as role:
        pid, starttime, _ = role.census[0]
        valid = role.parked / f"{pid}-{starttime}-101.5"
        assert role.post() == valid.name
        inode = valid.stat().st_ino
        if kind == "stamp":
            role.set_gate(stamp)
        elif kind == "missing-key":
            role.gate.write_text(json.dumps({"draining": True}))
        elif kind == "missing-file":
            role.gate.unlink()
        else:
            role.gate.write_text("{not JSON")
        retained = role.parked / f"{pid}-{starttime}-{historical_key}"
        valid.rename(retained)
        assert retained.stat().st_ino == inode
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
