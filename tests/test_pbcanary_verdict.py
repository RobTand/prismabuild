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
