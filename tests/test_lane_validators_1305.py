"""Lane validators refuse NUL/control text like the core owner (issue #1305).

The slurm and dagster lanes used to validate text with a pattern-only check,
while ``core._text`` hardened (NUL/control refusal, issue #21). Both lanes now
delegate to the core owner, so the hardened rule applies to them too -- a
deliberate behaviour change. These tests pin the new refusal per lane, plus
the preserved agreement on valid input and the lane error types.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import dagster as dg  # noqa: E402
from prismabuild import slurm as ps  # noqa: E402


TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}\Z")


def test_slurm_token_refuses_nul_and_control():
    for bad in ("ab\x00cd", "ab\ncd", "ab\x1fcd"):
        try:
            ps._token(bad, where="probe", pattern=TOKEN)
        except pb.ActionContractError as exc:
            assert "NUL" in str(exc)
        else:
            raise AssertionError(f"slurm accepted {bad!r}")
    assert ps._token("ok-token_1.2", where="probe", pattern=TOKEN) == "ok-token_1.2"


def test_dagster_text_refuses_nul_and_control():
    for bad in ("ab\x00cd", "ab\ncd", "ab\x1fcd"):
        try:
            dg._text(bad, where="probe", pattern=TOKEN)
        except dg.DagsterGraphError as exc:
            assert "NUL" in str(exc)
        else:
            raise AssertionError(f"dagster accepted {bad!r}")
    assert dg._text("ok-token_1.2", where="probe", pattern=TOKEN) == "ok-token_1.2"


def test_lane_integers_agree_with_core():
    assert ps._nonnegative_integer(3, where="p") == 3
    assert dg._nonnegative_integer(3, where="p") == 3
    assert ps._positive_integer(3, where="p") == 3
    assert dg._positive_integer(3, where="p") == 3
    assert ps._positive_finite(2.5, where="p") == 2.5
    assert dg._positive_finite(2.5, where="p") == 2.5
    assert ps._sha256("a" * 64, where="p") == "a" * 64
    for bad_call in (
        lambda: ps._nonnegative_integer(-1, where="p"),
        lambda: dg._nonnegative_integer(-1, where="p"),
        lambda: ps._positive_integer(0, where="p"),
        lambda: dg._positive_integer(0, where="p"),
    ):
        try:
            bad_call()
        except (pb.ActionContractError, dg.DagsterGraphError):
            pass
        else:
            raise AssertionError("lane accepted an invalid integer")
