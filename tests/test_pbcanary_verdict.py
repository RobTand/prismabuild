"""Unit tests for the pbcanary verdict machinery (RobTand/prismabuild#688).

The verdict input contract is a list of per-leg result dicts
``{leg, ok, reason, receipt_ref}``. These tests cover verdict inputs only:
a ``FakeGateway`` stands in for crew A's submission path and produces
canned per-leg dicts, which are fed straight to ``verdict()``. Nothing
here touches the live queue, CAS, or ``publish_runtime`` (owned by another
crew); the conftest's live-store guards stay quiet because no store is
reached at all.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from pbcanary_verdict import verdict  # noqa: E402


def _ok(leg, receipt_ref="cas:done/abc123", **extra):
    return {"leg": leg, "ok": True, "reason": None, "receipt_ref": receipt_ref,
            **extra}


class FakeGateway:
    """Stands in for the driver's submission path: replays canned legs."""

    def __init__(self, legs):
        self._legs = [dict(entry) for entry in legs]

    def run(self):
        return [dict(entry) for entry in self._legs]


def _green_legs():
    return [
        _ok(1),
        _ok(2),
        _ok(3),
        _ok("4-sparky", digest="9f2c"),
        _ok("4-sparklina", digest="9f2c"),
    ]


def test_green_run_is_exit_0_with_envelope_equality() -> None:
    code, summary = verdict(FakeGateway(_green_legs()).run())

    assert code == 0
    assert summary["exit_code"] == 0
    assert summary["verified"] is True
    assert summary["failed_leg"] is None
    assert "leg-4 envelopes equal" in summary["stderr_message"]


def test_single_entry_leg4_with_matching_pair_is_exit_0() -> None:
    legs = [_ok(1), _ok(2), _ok(3),
            _ok(4, digest_a="9f2c", digest_b="9f2c")]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 0
    assert summary["verified"] is True


def test_corrupted_digest_is_exit_1_naming_the_leg() -> None:
    legs = [
        _ok(1),
        {"leg": 2, "ok": False,
         "reason": "artifact-digest mismatch: expected 9f2c got 0000",
         "receipt_ref": "cas:done/def456"},
        _ok(3),
        _ok("4-sparky", digest="9f2c"),
        _ok("4-sparklina", digest="9f2c"),
    ]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 1
    assert summary["verified"] is False
    assert summary["failed_leg"] == "2"
    assert summary["failed_check"] == "artifact-digest"
    assert "2" in summary["stderr_message"]
    assert "artifact-digest mismatch" in summary["stderr_message"]


def test_leg4_envelope_inequality_is_exit_1_naming_leg_4() -> None:
    legs = [
        _ok(1),
        _ok(2),
        _ok(3),
        _ok("4-sparky", digest="9f2c"),
        _ok("4-sparklina", digest="0000"),
    ]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 1
    assert summary["failed_leg"] == "leg-4"
    assert summary["failed_check"] == "envelope-equality"
    assert "leg-4" in summary["stderr_message"]


def test_unreachable_queue_is_exit_2() -> None:
    legs = [{"leg": "submit", "ok": False,
             "reason": "precondition refused: queue unreachable",
             "receipt_ref": None}]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 2
    assert summary["verified"] is False
    assert summary["failed_check"] == "precondition"
    assert "precondition refused" in summary["stderr_message"]


def test_precondition_refusal_dominates_a_leg_failure() -> None:
    legs = [
        {"leg": 1, "ok": False,
         "reason": "artifact-digest mismatch: expected 9f2c got 0000",
         "receipt_ref": "cas:done/def456"},
        {"leg": 2, "ok": False,
         "reason": "precondition refused: generation root absent",
         "receipt_ref": None},
    ]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 2
    assert summary["failed_check"] == "precondition"


def test_did_not_test_is_never_passed() -> None:
    for legs in ([], [{"leg": 1, "ok": False,
                       "reason": "did not test: claim timed out",
                       "receipt_ref": None}]):
        code, summary = verdict(FakeGateway(legs).run())
        assert code in (1, 2)
        assert summary["verified"] is False


def test_missing_leg_is_exit_1_naming_the_leg() -> None:
    legs = [_ok(1), _ok(2), _ok(3)]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 1
    assert summary["failed_leg"] == "leg-4"
    assert summary["failed_check"] == "leg-executed"
    assert "leg-4" in summary["stderr_message"]


def test_verdict_output_shape_is_driver_stable() -> None:
    code, summary = verdict(FakeGateway(_green_legs()).run())

    assert isinstance(code, int)
    assert isinstance(summary, dict)
    assert set(("exit_code", "verified", "legs", "failed_leg",
                "failed_check", "detail", "stderr_message")) <= set(summary)
    assert set(summary["legs"]) == {"1", "2", "3", "4-sparky", "4-sparklina"}


# --------------------------------------------------------------------------
# Issue #690: an ok leg-4 entry must carry digest evidence, not nothing
# --------------------------------------------------------------------------


def test_ok_leg4_entry_with_no_digest_evidence_is_exit_1() -> None:
    """The vacuous pass: no digests proves nothing, so the leg fails.

    Before #690 an ok leg-4 entry lacking every digest field contributed
    nothing to the comparison, and an empty comparison passed vacuously.
    """
    legs = [_ok(1), _ok(2), _ok(3), _ok(4)]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 1
    assert summary["failed_leg"] == "leg-4"
    assert summary["failed_check"] == "envelope-equality"
    assert "no digest evidence" in summary["stderr_message"]


def test_two_ok_leg4_entries_without_digests_still_fail() -> None:
    """Both box entries lacking digests is equally unproven."""
    legs = [_ok(1), _ok(2), _ok(3), _ok("4-sparky"), _ok("4-sparklina")]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 1
    assert summary["failed_check"] == "envelope-equality"


def test_envelope_equal_true_alone_is_not_digest_evidence() -> None:
    """The flag is read but never substitutes for digests (#690)."""
    legs = [_ok(1), _ok(2), _ok(3), _ok(4, envelope_equal=True)]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 1
    assert summary["failed_check"] == "envelope-equality"


def test_incomplete_digest_pair_is_exit_1() -> None:
    """A lone ``digest_a`` is a partial pair, not evidence."""
    legs = [_ok(1), _ok(2), _ok(3), _ok(4, digest_a="9f2c")]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 1
    assert "incomplete digest pair" in summary["stderr_message"]


# --------------------------------------------------------------------------
# Issue #690 follow-up: digest evidence must be an actual nonempty string,
# and malformed or partial fields must not be masked by another valid digest
# --------------------------------------------------------------------------


#: Leg-4 extras that are not usable digest evidence: null, empty, blank and
#: non-string values (alone, in pairs, or hiding beside a valid sibling).
MALFORMED_DIGEST_EXTRAS = [
    pytest.param({"digest_a": None, "digest_b": None}, id="null-pair"),
    pytest.param({"digest_a": "", "digest_b": ""}, id="empty-pair"),
    pytest.param({"digest_a": "   ", "digest_b": "   "}, id="blank-pair"),
    pytest.param({"digest_a": 0, "digest_b": 0}, id="int-pair"),
    pytest.param({"digest_a": ["9f2c"], "digest_b": ["9f2c"]}, id="list-pair"),
    pytest.param({"digest_a": None, "digest_b": None, "envelope_equal": True},
                 id="null-pair-flag-true"),
    pytest.param({"digest_a": "9f2c", "digest_b": None}, id="half-null-pair"),
    pytest.param({"digest_a": "9f2c"}, id="partial-pair"),
    pytest.param({"digest": ""}, id="empty-single"),
    pytest.param({"digest": "   "}, id="blank-single"),
    pytest.param({"digest": 0}, id="int-single"),
    pytest.param({"digest": None}, id="null-single"),
    pytest.param({"digest_a": None, "digest_b": None, "digest": "abcd"},
                 id="null-pair-masked-by-single"),
    pytest.param({"digest_a": "9f2c", "digest": "abcd"},
                 id="partial-pair-masked-by-single"),
]


@pytest.mark.parametrize("extra", MALFORMED_DIGEST_EXTRAS)
def test_malformed_leg4_digest_evidence_is_exit_1(extra: dict) -> None:
    """No coercion, no masking: only nonempty strings are digest evidence.

    Before this fix ``_leg4_digests`` converted every candidate with
    ``str``, so ``digest_a=None, digest_b=None`` compared equal as the
    string ``"None"`` and passed, as did equal empty, blank and non-string
    values; a valid extra digest also masked a partial pair. Each of these
    must answer exit 1 envelope-equality.
    """
    legs = [_ok(1), _ok(2), _ok(3), _ok(4, **extra)]

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 1, (extra, summary["stderr_message"])
    assert summary["verified"] is False
    assert summary["failed_leg"] == "leg-4"
    assert summary["failed_check"] == "envelope-equality"


#: Leg-4 forms that are genuine digest evidence and must stay exit 0.
VALID_DIGEST_LEGS = [
    pytest.param(
        [_ok("4-sparky", digest="9f2c"), _ok("4-sparklina", digest="9f2c")],
        id="single-digest-per-box"),
    pytest.param([_ok(4, digest_a="9f2c", digest_b="9f2c")],
                 id="matching-pair"),
    pytest.param([_ok(4, envelope_a="9f2c", envelope_b="9f2c")],
                 id="matching-envelope-pair"),
    pytest.param([_ok(4, digest_sparky="9f2c", digest_sparklina="9f2c")],
                 id="matching-sparky-pair"),
    pytest.param([_ok(4, envelope_digest="9f2c")], id="single-envelope-digest"),
    pytest.param([_ok(4, artifact_digest="9f2c")],
                 id="single-artifact-digest"),
    pytest.param([_ok(4, digest_a="a" * 64, digest_b="a" * 64)],
                 id="matching-hex-pair"),
    pytest.param([_ok(4, digest_a="9f2c", digest_b="9f2c",
                      envelope_equal=True)], id="matching-pair-flag-true"),
]


@pytest.mark.parametrize("leg4", VALID_DIGEST_LEGS)
def test_valid_leg4_digest_forms_stay_green(leg4: list) -> None:
    """A complete pair or a single-value digest key remains exit 0."""
    legs = [_ok(1), _ok(2), _ok(3)] + leg4

    code, summary = verdict(FakeGateway(legs).run())

    assert code == 0, summary["stderr_message"]
    assert summary["verified"] is True
