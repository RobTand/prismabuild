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

BORN_AFTER = 1000  # boot-relative clock ticks read after the previous host sample's counters


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


def _uptime(tmp_path, seconds):
    if seconds is None:  # an unreadable boot clock
        return tmp_path / 'absent'
    proc = tmp_path / 'proc'
    proc.mkdir(exist_ok=True)
    (proc / 'uptime').write_text(f'{seconds:.2f} 12345.67\n')
    return proc


def _sampler(tmp_path, monkeypatch, uptimes, newborn):
    """The real sampler and the real ``boot_ticks`` over a scripted /proc/uptime.

    Nothing about the boundary is mocked: each host sample reads the boot clock
    through ``control_cpu.boot_ticks`` and the next one consumes what the
    previous one persisted.
    """
    from prismabuild import control_cpu
    ledger = pool.ResourceLedger(tmp_path / 'ledger')
    controller = adaptive_cpu.Controller(ledger, {'preferred': [11, 12], 'fallback': []})
    hosts = iter([
        {'sampled_unix': 100., 'cpus': {'11': [20, 100], '12': [0, 100]}, 'psi_total': 0},
        {'sampled_unix': 102., 'cpus': {'11': [54, 300], '12': [0, 300]}, 'psi_total': 1260000},
    ])
    tasks = iter([{}, {}, {'7': newborn}, {'7': newborn}])
    clock = iter(uptimes)
    monkeypatch.setattr(adaptive_cpu, 'counters', lambda _: next(hosts))
    monkeypatch.setattr(adaptive_cpu, 'control_plane_counters', lambda _: next(tasks), raising=False)
    monkeypatch.setattr(adaptive_cpu, 'boot_ticks',
                        lambda: control_cpu.boot_ticks(proc_root=_uptime(tmp_path, next(clock))))
    assert controller.sample() == {}
    return controller.sample()


def test_the_sampler_does_not_report_a_fresh_loops_startup_as_foreign(tmp_path, monkeypatch):
    """Previous sample at boot tick 1000 (uptime 10.00 s); a loop born at tick 1500."""
    observed = _sampler(tmp_path, monkeypatch, [10.0, 12.0], _newborn(start=1500, ticks=30))
    assert observed['per_cpu_busy']['11'] == pytest.approx(.17)  # raw load retained
    assert observed['control_plane_busy']['11'] == pytest.approx(.15)
    assert observed['foreign_per_cpu_busy']['11'] == pytest.approx(.02)


def test_a_process_born_before_the_previous_sample_is_foreign_whatever_the_wall_clock_says(
        tmp_path, monkeypatch):
    """The wall-clock stamps in the samples (100 and 102) are scripted and never consulted.

    A process that started at tick 900 predates the previous sample (tick 1000);
    a boundary derived from wall-clock time and a later ``btime`` could place it
    inside the interval after a clock step.  The boot-clock boundary cannot.
    """
    observed = _sampler(tmp_path, monkeypatch, [10.0, 12.0], _newborn(start=900, ticks=30))
    assert observed['control_plane_busy']['11'] == pytest.approx(0.0)
    assert observed['foreign_per_cpu_busy']['11'] == pytest.approx(.17)


def test_without_a_persisted_boundary_nothing_is_credited(tmp_path, monkeypatch):
    """A previous sample written before this change carries no boot_ticks."""
    observed = _sampler(tmp_path, monkeypatch, [None, 12.0], _newborn(start=1500, ticks=30))
    assert observed['control_plane_busy']['11'] == pytest.approx(0.0)


def test_boot_ticks_reads_the_boot_clock_and_refuses_what_it_cannot_read(tmp_path):
    from prismabuild.control_cpu import boot_ticks
    assert boot_ticks(proc_root=_uptime(tmp_path, 10.0)) == 10 * __import__('os').sysconf('SC_CLK_TCK')
    assert boot_ticks(proc_root=tmp_path / 'absent') is None
    (tmp_path / 'proc' / 'uptime').write_text('not-a-number\n')
    assert boot_ticks(proc_root=tmp_path / 'proc') is None
