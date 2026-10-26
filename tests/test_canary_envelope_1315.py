"""One envelope extractor for canary legs 3-4 (issue #1315).

``common.extract_envelope`` owns the scan, parametrized by schema marker.
Both verifiers pass their marker directly. Tests pin both markers and the
leg-4 requirement for raw envelope bytes.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from pbcanary_legs import leg3, leg4  # noqa: E402
from pbcanary_legs.common import extract_envelope  # noqa: E402


def _receipt(schema: str, *, where: str = "envelope") -> dict:
    body = {"schema": schema, "input_digest": "d" * 64}
    text = '{"schema": "%s", "input_digest": "%s"}' % (schema, "d" * 64)
    if where == "envelope":
        return {"envelope": text}
    if where == "parsed":
        return {"envelope": dict(body)}
    if where == "stdout":
        return {"stdout": "noise\n" + text + "\n"}
    if where == "detail":
        return {"detail": {"stdout": text}}
    raise AssertionError(where)


def test_shared_helper_finds_both_markers():
    for schema in (leg3.LEG3_SCHEMA, leg4.LEG4_SCHEMA):
        for where, expect_raw in (
            ("envelope", True),
            ("parsed", False),
            ("stdout", True),
            ("detail", True),
        ):
            raw, parsed, found = extract_envelope(
                _receipt(schema, where=where), schema=schema
            )
            assert parsed is not None and parsed["schema"] == schema, where
            assert found, where
            assert (raw is None) == (not expect_raw), where


def test_shared_helper_rejects_other_marker():
    raw, parsed, where = extract_envelope(
        _receipt(leg3.LEG3_SCHEMA), schema=leg4.LEG4_SCHEMA
    )
    assert (raw, parsed, where) == (None, None, "receipt[envelope]")


@pytest.mark.parametrize("where", ["envelope", "parsed", "stdout", "detail"])
def test_leg4_verifier_requires_raw_envelope_bytes(where):
    receipt = _receipt(leg4.LEG4_SCHEMA, where=where)
    ok, reason = leg4.verify(receipt, receipt, {"input_digest": "d" * 64})
    assert ok == (where != "parsed"), reason
    if where == "parsed":
        assert "envelope bytes absent" in reason


def test_leg_verifiers_reject_each_others_marker():
    ok, reason = leg3.verify(_receipt(leg4.LEG4_SCHEMA), {})
    assert not ok and reason == "leg3: no leg3 envelope in receipt"
    receipt = _receipt(leg3.LEG3_SCHEMA)
    ok, reason = leg4.verify(receipt, receipt, {})
    assert not ok and "envelope bytes absent" in reason
    assert extract_envelope("not a receipt", schema=leg3.LEG3_SCHEMA) == (None, None, "")
