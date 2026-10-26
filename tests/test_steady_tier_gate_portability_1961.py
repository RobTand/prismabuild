"""The portable steady-cycle gate checks reuse, not a particular CPU's speed.

#1961 names this gate on portable-x86, whereas its absolute CPU allowance was
calibrated on GB10 cores. Exercise the actual steady-cycle regression with
an affine, unit-preserving CPU clock: the work and filesystem observations stay
real, but an unchanged cycle costs more CPU seconds. This is a deterministic
counterexample to the portability assumption, not a reconstruction of the
unavailable historical gate traceback.
"""
from __future__ import annotations

import copy
import time

import pytest

from test_a_steady_tier_cycle_rereads_only_what_changed import loop  # noqa: F401
from test_a_steady_tier_cycle_rereads_only_what_changed import (
    test_a_steady_cycle_reads_only_what_changed as steady_regression,
)
from prismabuild import tier_cycle


READ_COUNTERS = (
    "records_parsed", "records_listed", "census_parses", "census_listed",
)


def test_portable_steady_gate_keeps_read_guards_without_a_machine_cpu_budget(
    loop, monkeypatch,
):
    real_cpu = time.process_time
    origin = real_cpu()
    cpu_samples = []

    def slower_cpu():
        # CPU seconds remain CPU seconds; this models a slower interpreter/CPU,
        # not a clock-unit bug. No wall/monotonic clock or production work is
        # replaced, and a relative cost comparison can still use this clock.
        value = origin + 10_000 * (real_cpu() - origin)
        cpu_samples.append(value)
        return value

    monkeypatch.setattr(time, "process_time", slower_cpu)
    real_cycle = tier_cycle.cycle
    observations = []

    def observe_cycle(*args, **kwargs):
        result = real_cycle(*args, **kwargs)
        observations.append(copy.deepcopy(tier_cycle.LAST_CYCLE))
        return result

    monkeypatch.setattr(tier_cycle, "cycle", observe_cycle)
    steady_regression(loop, monkeypatch)

    assert len(observations) >= 3, "retain real cold, warm and steady cycles"
    assert len(cpu_samples) >= 2, "retain CPU measurement as diagnostic evidence"
    assert any(b - a > 0.636 for a, b in zip(cpu_samples, cpu_samples[1:])), (
        "the clock must exercise CPU cost above the old GB10-derived budget"
    )
    steady = observations[-1]
    assert steady["completed"] is True
    assert all(steady["reads"][name] == 0 for name in READ_COUNTERS)
    assert steady["reads"]["records_kept"] > 0
    assert steady["reads"]["census_kept"] >= 6

    # A fix must not consist of dropping the structural assertions. Replay the
    # real regression with each forbidden observation independently injected;
    # it must still reject the regression even on the slower modeled CPU.
    for name in READ_COUNTERS:
        injected = []

        def changed_cycle(*args, **kwargs):
            result = real_cycle(*args, **kwargs)
            tier_cycle.LAST_CYCLE["reads"][name] = 1
            injected.append(name)
            return result

        with monkeypatch.context() as patch:
            patch.setattr(tier_cycle, "cycle", changed_cycle)
            with pytest.raises(AssertionError):
                steady_regression(loop, patch)
        assert len(injected) >= 3, "exercise the measured cycle's read guard"
