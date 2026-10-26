"""Pin canary receipt exit-code precedence before consolidating its readers."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from pbcanary_legs import leg3, leg4
from pbcanary_legs.common import receipt_returncode


@pytest.mark.parametrize(
    "receipt, expected",
    [
        ({}, None),
        ({"returncode": 0, "detail": {"returncode": 7}, "action_returncode": 9}, 0),
        ({"returncode": -15}, -15),
        ({"detail": {"returncode": 7}, "action_returncode": 9}, 7),
        ({"action_returncode": 9}, 9),
        ({"returncode": True, "detail": {"returncode": 7}}, 7),
        ({"detail": {"returncode": False}, "action_returncode": -9}, -9),
        ({"returncode": "1", "detail": {"returncode": 0.0}, "action_returncode": 2}, 2),
        ({"returncode": None, "detail": [], "action_returncode": False}, None),
    ],
)
def test_canary_receipt_exit_code_precedence(receipt, expected):
    assert receipt_returncode(receipt) == expected


@pytest.mark.parametrize("leg", [leg3, leg4], ids=["leg3", "leg4"])
@pytest.mark.parametrize(
    "receipt",
    [{"returncode": -15}, {"detail": {"returncode": 7}}, {"action_returncode": 9}],
)
def test_canary_verifier_refuses_failed_execution_before_missing_envelope(leg, receipt):
    if leg is leg3:
        ok, reason = leg.verify(receipt, {})
    else:
        ok, reason = leg.verify(receipt, {}, {})
    assert not ok
    assert "action returncode" in reason
    assert "envelope" not in reason
