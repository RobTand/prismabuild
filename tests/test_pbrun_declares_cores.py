"""Cores are a demand.  They were the one resource the ledger could not see.

On 2026-09-04 four ``pytest -n 24`` actions, each declaring ``mem_gb=4``, were
admitted to one 80-core box at the same time.  The ledger was satisfied --
16 GB of 60 -- and the box was at load average 371 running four copies of the
same suite against a checkout ten merges stale.  Memory was never the binding
constraint on that work, and memory was all admission could weigh.

The default is one, so every action already in flight keeps the admission it
had; what this adds is the ability for an action that will take twenty-four
cores to say so.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun


class _Stop(Exception):
    pass


def _demand(argv, monkeypatch, tmp_path):
    captured = []

    def _stop(body, *_a, **_kw):
        captured.append(body)
        raise _Stop()

    monkeypatch.setattr(pbrun.pb, "seal_action", _stop)
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--cwd", str(tmp_path), *argv])
    with pytest.raises(_Stop):
        pbrun.main()
    return captured[0]["params"]["demand"]


def test_an_ordinary_action_asks_for_one_core(monkeypatch, tmp_path):
    assert _demand(["--", "true"], monkeypatch, tmp_path)["cpu"] == 1


def test_a_parallel_run_can_say_what_it_will_take(monkeypatch, tmp_path):
    demand = _demand(["--cpus", "24", "--", "true"], monkeypatch, tmp_path)
    assert demand["cpu"] == 24
    # And it is a demand like any other, so it composes with the rest.
    assert demand["mem_gb"] == 4


def test_an_explicit_demand_still_wins(monkeypatch, tmp_path):
    """``--demand`` is the escape hatch; the shorthand must not overwrite it."""

    demand = _demand(["--demand", "cpu=8", "--cpus", "24", "--", "true"],
                     monkeypatch, tmp_path)
    assert demand["cpu"] == 8


def test_zero_cores_is_refused(monkeypatch, tmp_path):
    with pytest.raises(SystemExit):
        _demand(["--cpus", "0", "--", "true"], monkeypatch, tmp_path)


def test_a_gpu_action_also_declares_its_cores(monkeypatch, tmp_path):
    assert _demand(["--gpu", "--", "true"], monkeypatch, tmp_path)["cpu"] == 1
