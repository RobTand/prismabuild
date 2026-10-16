"""Different finite drains must never share a park acknowledgement."""
import pytest

import fleet_membership
from test_client_proof_prior_trust import tmp_path
from test_park_proof_real_writer import parked_role


def _reader_results(role):
    host = role.client.drain_evidence({"draining": True, "active_scopes": 0})
    gate = fleet_membership.read_gate(role.gate)
    member = fleet_membership.parked_markers(role.parked, gate)
    return host.get("drained"), [path.name for path in member]


@pytest.mark.parametrize("prior,current", [(1e-20, 1e20), (1e20, 1e-20)])
def test_real_marker_for_a_different_scientific_drain_is_rejected(
        parked_role, prior, current):
    with parked_role() as role:
        role.set_gate(prior)
        assert role.post() is not None
        role.set_gate(current)
        assert _reader_results(role) == (False, [])


@pytest.mark.parametrize("stamp", [1e-20, 1e20])
def test_real_marker_for_the_same_scientific_drain_is_accepted(parked_role, stamp):
    with parked_role() as role:
        role.set_gate(stamp)
        marker = role.post()
        assert marker is not None
        assert _reader_results(role) == (True, [marker])


@pytest.mark.parametrize("stamp,key", [
    (1, "1"),
    (1.0, "1.0"),
    (17.25, "17.25"),
    (-17.25, "_17.25"),
    (-0.0, "_0.0"),
    (1e-20, "1em20"),
    (1e20, "1ep20"),
    (-1e-20, "_1em20"),
    (-1e20, "_1ep20"),
])
def test_real_writer_preserves_the_literal_numeric_wire_contract(parked_role, stamp, key):
    with parked_role() as role:
        role.set_gate(stamp)
        pid, starttime, _ = role.census[0]
        expected = f"{pid}-{starttime}-{key}"
        assert role.post() == expected
        assert (role.parked / expected).read_bytes() == b""


@pytest.mark.parametrize("stamp", [1e-20, 1e20])
def test_ambiguous_legacy_exponent_marker_never_certifies_either_drain(
        parked_role, stamp):
    with parked_role() as role:
        pid, starttime, _ = role.census[0]
        valid = role.parked / f"{pid}-{starttime}-101.5"
        assert role.post() == valid.name
        inode = valid.stat().st_ino
        legacy = role.parked / f"{pid}-{starttime}-1e_20"
        valid.rename(legacy)
        assert legacy.stat().st_ino == inode
        role.set_gate(stamp)
        assert _reader_results(role) == (False, [])
