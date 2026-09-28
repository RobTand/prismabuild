"""Pin lane validator messages the #1305 tests leave unpinned (issue #1320).

Follow-up to merged #1306: the delegation changed three messages no test
pins, and ``_positive_finite`` has no NaN/inf/bool coverage. All pins go
through the lane wrappers (slurm raises ActionContractError, dagster raises
DagsterGraphError), never the core owner directly -- except the dagster
``_sha256`` case, which exercises the delegated ``pb._sha256`` with the lane
fail factory, the path dagster callers take.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import dagster as dg  # noqa: E402
from prismabuild import slurm as ps  # noqa: E402


TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}\Z")


def test_non_str_and_empty_report_non_empty_string():
    for bad in (123, None, b"gb10", "", []):
        with pytest.raises(pb.ActionContractError,
                           match="must be a non-empty string"):
            ps._token(bad, where="slurm account", pattern=TOKEN)
    for bad in (123, None, ""):
        with pytest.raises(dg.DagsterGraphError,
                           match="must be a non-empty string"):
            dg._text(bad, where="dagster name", pattern=TOKEN)


def test_slurm_sha256_reports_invalid_value():
    with pytest.raises(pb.ActionContractError,
                       match="has an invalid value"):
        ps._sha256("xyz", where="slurm x.runtime_sha256")
    assert ps._sha256("0" * 64, where="slurm x.runtime_sha256") == "0" * 64


def test_dagster_sha256_reports_invalid_value():
    with pytest.raises(dg.DagsterGraphError,
                       match="has an invalid value"):
        pb._sha256("xyz", where="dagster x.sha256", fail=dg._fail)
    assert pb._sha256("0" * 64, where="dagster x.sha256",
                      fail=dg._fail) == "0" * 64


@pytest.mark.parametrize("bad", [float("nan"), float("inf"),
                                 float("-inf"), True, False, "3"])
def test_positive_finite_refuses_non_finite(bad):
    with pytest.raises(pb.ActionContractError,
                       match="must be a positive finite number"):
        ps._positive_finite(bad, where="slurm poll")
    with pytest.raises(dg.DagsterGraphError,
                       match="must be a positive finite number"):
        dg._positive_finite(bad, where="dagster poll")


@pytest.mark.parametrize("good", [1, 2.5])
def test_positive_finite_accepts_finite(good):
    assert ps._positive_finite(good, where="slurm poll") == float(good)
    assert dg._positive_finite(good, where="dagster poll") == float(good)
