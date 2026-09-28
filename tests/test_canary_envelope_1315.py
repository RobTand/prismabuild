"""One envelope extractor for canary legs 3-4 (issue #1315).

``common.extract_envelope`` owns the scan, parametrized by schema marker;
each leg's thin ``_extract_envelope`` passes its own marker. The tests pin
both markers through the shared helper and through each leg wrapper, so the
verify legs stay in lockstep. tmp_path-free: pure receipt dicts, no fleet.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from pbcanary_legs import leg3, leg4  # noqa: E402
from pbcanary_legs.common import extract_envelope  # noqa: E402


def _receipt(schema: str, *, where: str = "envelope") -> dict:
    body = {"schema": schema, "digest": "d" * 64}
    text = '{"schema": "%s", "digest": "%s"}' % (schema, "d" * 64)
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


def test_leg_wrappers_agree_with_shared_helper():
    for leg in (leg3, leg4):
        schema = leg3.LEG3_SCHEMA if leg is leg3 else leg4.LEG4_SCHEMA
        for where in ("envelope", "parsed", "stdout", "detail"):
            receipt = _receipt(schema, where=where)
            assert leg._extract_envelope(receipt) == extract_envelope(
                receipt, schema=schema
            ), where


def test_leg_wrappers_reject_each_others_marker():
    assert leg3._extract_envelope(_receipt(leg4.LEG4_SCHEMA))[1] is None
    assert leg4._extract_envelope(_receipt(leg3.LEG3_SCHEMA))[1] is None
    assert leg3._extract_envelope("not a receipt") == (None, None, "")
