"""Pure strict physical identity predicates."""
import pytest

from prismabuild import park_proof


@pytest.mark.parametrize("pid", [True, False, 0, -1, 1.0, "1", None])
def test_process_pid_must_be_a_positive_nonbool_integer(pid):
    assert isinstance(park_proof.process_identity(pid, "0"), park_proof.ProofUnknown)


@pytest.mark.parametrize("start", ["", "unknown", "-1", "1.0", " 1", "١", 0, True, None])
def test_process_start_ticks_must_be_ascii_decimal(start):
    assert isinstance(park_proof.process_identity(1, start), park_proof.ProofUnknown)


@pytest.mark.parametrize("start", ["0", "1", "0001"])
def test_zero_and_ascii_digit_start_ticks_are_known(start):
    identity = park_proof.process_identity(1, start)
    assert identity == park_proof.ProcessIdentity(1, start)


@pytest.mark.parametrize("gate", [
    None, {}, {"draining": False, "changed_unix": 1},
    {"draining": 1, "changed_unix": 1},
    {"draining": True}, {"draining": True, "changed_unix": True},
    {"draining": True, "changed_unix": "1"},
    {"draining": True, "changed_unix": float("nan")},
    {"draining": True, "changed_unix": float("inf")},
    {"draining": True, "changed_unix": 10 ** 400},
])
def test_unknown_drain_never_proves_even_an_empty_census(gate):
    evidence = park_proof.acknowledgements(
        processes=[], markers=set(), gate_before=gate, gate_after=gate)
    assert evidence.complete is False
    assert evidence.unknown


@pytest.mark.parametrize("before,after", [(1, 1.0), (1.0, 1), (1e-20, 1e20), (1e20, 1e-20)])
def test_gate_bracketing_uses_exact_wire_identity_for_an_empty_census(before, after):
    evidence = park_proof.acknowledgements(
        processes=[], markers=set(),
        gate_before={"draining": True, "changed_unix": before},
        gate_after={"draining": True, "changed_unix": after})
    assert evidence.complete is False
    assert evidence.unknown


@pytest.mark.parametrize("stamp", [0, -0.0, 1, 1.0, -17.25, 1e-20, 1e20])
def test_unchanged_known_drain_can_prove_an_empty_census(stamp):
    gate = {"draining": True, "changed_unix": stamp}
    evidence = park_proof.acknowledgements(
        processes=[], markers=set(), gate_before=gate, gate_after=dict(gate))
    assert evidence.complete is True
