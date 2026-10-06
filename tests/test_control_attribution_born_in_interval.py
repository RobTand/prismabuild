"""A PrismaBuild loop born inside the sampling interval is not foreign CPU.

``attributed_ticks`` used to credit a control-plane thread only when the same
thread appeared in the previous sample, so a worker loop spawned after it had
every tick it burned (python start-up, 10 to 40 percent of a CPU) counted as
foreign load, and measurement rows starved on their own host's loop churn.  A
process born after the previous host sample, with zero migrations, ran on one
CPU for its whole life, all inside the interval: its ticks are provably
control-plane work.  Everything else stays unproven.
"""
import pytest

from prismabuild import adaptive_cpu, pool
from prismabuild.control_cpu import attributed_ticks

BORN_AFTER = 1000  # boot-relative clock ticks of the previous host sample


def _newborn(**over):
    return {'start': 1500, 'cpu': 11, 'migrations': 0, 'ticks': 30,
            'attribution_kind': 'control', 'process_identity': ['/runtime/worker_loop.py', 1500],
            **over}


def test_a_control_thread_born_in_the_interval_is_credited():
    credited = attributed_ticks({}, {'7': _newborn()}, {'11': 34, '12': 0},
                                kind='control', born_after=BORN_AFTER)
    assert credited == {'11': 30, '12': 0}


def test_without_the_birth_bound_a_new_thread_is_still_unproven():
    """The default keeps the conservative behaviour for every other caller."""
    assert attributed_ticks({}, {'7': _newborn()}, {'11': 34, '12': 0},
                            kind='control') == {'11': 0, '12': 0}


@pytest.mark.parametrize('changed', [
    {'start': 1000},        # born at or before the previous sample: part of its ticks precede the interval
    {'start': 900},
    {'migrations': 1},      # it moved, so its CPU is a guess
    {'ticks': -1},
    {'start': -1},
])
def test_a_thread_not_provably_born_inside_the_interval_is_not_credited(changed):
    assert attributed_ticks({}, {'7': _newborn(**changed)}, {'11': 34, '12': 0},
                            kind='control', born_after=BORN_AFTER) == {'11': 0, '12': 0}


def test_a_newborn_kernel_thread_is_not_credited_by_the_control_rule():
    record = _newborn(attribution_kind='kernel', flags=0x00200000)
    assert attributed_ticks({}, {'7': record}, {'11': 34, '12': 0},
                            kind='kernel', born_after=BORN_AFTER) == {'11': 0, '12': 0}


def test_credit_never_exceeds_the_cpu_that_was_actually_busy():
    assert attributed_ticks({}, {'7': _newborn(ticks=100)}, {'11': 34, '12': 0},
                            kind='control', born_after=BORN_AFTER) == {'11': 0, '12': 0}


def test_the_sampler_does_not_report_a_fresh_loops_startup_as_foreign(tmp_path, monkeypatch):
    """Through the real sampler: previous sample has no loop, the next one has a newborn."""
    ledger = pool.ResourceLedger(tmp_path / 'ledger')
    controller = adaptive_cpu.Controller(ledger, {'preferred': [11, 12], 'fallback': []})
    hosts = iter([
        {'sampled_unix': 100., 'cpus': {'11': [20, 100], '12': [0, 100]}, 'psi_total': 0},
        {'sampled_unix': 102., 'cpus': {'11': [54, 300], '12': [0, 300]}, 'psi_total': 1260000},
    ])
    tasks = iter([{}, {}, {'7': _newborn(ticks=30)}, {'7': _newborn(ticks=30)}])
    monkeypatch.setattr(adaptive_cpu, 'counters', lambda _: next(hosts))
    monkeypatch.setattr(adaptive_cpu, 'control_plane_counters', lambda _: next(tasks), raising=False)
    # The previous sample (t=100) is at boot-relative tick 1000.
    monkeypatch.setattr(adaptive_cpu, 'born_after_ticks', lambda unix: 1000, raising=False)
    assert controller.sample() == {}
    observed = controller.sample()
    assert observed['per_cpu_busy']['11'] == pytest.approx(.17)  # raw load retained
    assert observed['control_plane_busy']['11'] == pytest.approx(.15)
    assert observed['foreign_per_cpu_busy']['11'] == pytest.approx(.02)
