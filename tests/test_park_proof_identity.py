"""Strict physical identity and actual writer acknowledgements."""
import pytest

from prismabuild import park_proof
from test_park_proof_real_writer import parked_role


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


def test_actual_writer_acknowledges_only_the_exact_census_and_current_gate(parked_role):
    with parked_role() as role:
        name = role.post()
        assert name is not None
        gate = park_proof.read_gate(role.gate)
        pid, start, _ = role.census[0]
        def observe(processes, *, after=gate):
            return park_proof.acknowledgements(
                processes=processes, markers={name},
                gate_before=gate, gate_after=after)

        assert observe([(pid, start)]).complete is True
        replaced = observe([(pid, str(int(start) + 1))])
        assert replaced.complete is False and replaced.unparked == (pid,)
        foreign = observe([(pid + 1, start)])
        assert foreign.complete is False and foreign.unparked == (pid + 1,)
        unknown = observe([(pid, "unknown")])
        assert unknown.complete is False and unknown.unknown
        assert observe([(True, start)]).complete is False
        assert observe([(pid, start)], after=None).complete is False


@pytest.mark.parametrize("pid,start", [("True", "1"), ("0", "1"), ("01", "1"),
                                       ("1", "unknown"), ("1", "١")])
def test_member_reader_rejects_malformed_retained_process_names(parked_role, pid, start):
    with parked_role() as role:
        name = role.post()
        assert name is not None
        marker = role.parked / name
        inode = marker.stat().st_ino
        retained = role.parked / f"{pid}-{start}-101.5"
        marker.rename(retained)
        assert retained.stat().st_ino == inode
        assert park_proof.parked_markers(role.parked, park_proof.read_gate(role.gate)) == []
