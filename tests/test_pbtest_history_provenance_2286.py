"""Shard reports identify the exact input used for duration predictions."""
import hashlib
import json

import pytest

from test_pbtest_automatic_duration_packing_2286 import _project
from test_pbtest_balances_shards_by_measured_duration import (
    _dispatch, _prior_run, pbtest,
)


@pytest.mark.parametrize("mode", ["explicit", "automatic"])
def test_report_identifies_history_source_and_consumed_bytes(tmp_path, monkeypatch, mode):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    checkout = _project(tmp_path)
    prior = _prior_run(tmp_path, {"tests/test_long.py": 60.0})
    arguments = ["--shards", "1"]
    if mode == "explicit":
        arguments += ["--history", str(prior)]
    else:
        rows = json.loads(prior.read_text())
        rows[0]["verdict"] = {"green": True}
        pbtest.pbtest_history.remember_report(checkout, ["tests"], rows)
        prior = pbtest.pbtest_history.cached_report(checkout, ["tests"])
    expected = {"mode": mode, "reports": [
        {"path": str(prior), "sha256": hashlib.sha256(prior.read_bytes()).hexdigest()}]}

    code, results = _dispatch(checkout, monkeypatch, arguments)

    assert code == 0
    assert results[0]["predicted_s"] == 180.0
    assert results[0]["history_source"] == expected
