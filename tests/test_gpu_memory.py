"""GPU bytes are attributed to exact owned scopes; ambiguous jobs stay alive."""
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import gpu_memory as gm

SID = 'prismabuild-job' + 'a' * 32 + '.slice'
OTHER = 'prismabuild-job' + 'b' * 32 + '.slice'


def system(tmp_path):
    proc, cgroup = tmp_path / 'proc', tmp_path / 'cgroup'
    (proc / 'pressure').mkdir(parents=True)
    (proc / 'meminfo').write_text('MemTotal: 8388608 kB\nMemAvailable: 4194304 kB\n')
    (proc / 'pressure/memory').write_text('some avg10=0.00 total=1\nfull avg10=0.00 total=0\n')
    path = cgroup / 'prismabuild.slice' / SID
    path.mkdir(parents=True)
    (path / 'memory.current').write_text(str(100 * gm.MIB))
    return proc, cgroup, gm.Scope(SID, path, 1000 * gm.MIB)


def process(proc, pid, group, start=9):
    path = proc / str(pid)
    path.mkdir(exist_ok=True)
    (path / 'cgroup').write_text('0::' + group + '\n')
    (path / 'stat').write_text(f'{pid} (a tricky ) process name) ' + ' '.join(['S'] + ['0'] * 18 + [str(start)]))


def query(monkeypatch, text='', rc=0, effect=None):
    def run(argv, **kwargs):
        assert kwargs['timeout'] <= 5
        if effect:
            effect()
        return subprocess.CompletedProcess(argv, rc, text, '')
    monkeypatch.setattr(gm.subprocess, 'run', run)


def test_exact_nested_scope_attribution_and_foreign_accounting(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    process(proc, 20, f'/prismabuild.slice/{SID}suffix/payload')
    query(monkeypatch, '10, GPU-abc, 512\n20, GPU-abc, 800\n')
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    job = sample.jobs[0]
    assert job.complete
    assert job.gpu_reported_bytes == 512 * gm.MIB
    assert job.lower_bound_bytes == 512 * gm.MIB
    assert job.upper_bound_bytes == 612 * gm.MIB
    assert sample.foreign_gpu_reported_bytes == 800 * gm.MIB
    assert job.processes[0]['pid'] == 10
    json.dumps(sample.as_dict())


def test_unknown_gpu_memory_is_not_zero(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    query(monkeypatch, '10, GPU-abc, [N/A]\n')
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    assert not sample.jobs[0].complete
    assert sample.jobs[0].gpu_reported_bytes is None
    guard = gm.Guard()
    assert guard.observe(sample) == guard.observe(replace(sample, sampled_monotonic=sample.sampled_monotonic + 1)) == []


def test_pid_reuse_invalidates_gpu_attribution(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    group = f'/prismabuild.slice/{SID}/payload'
    process(proc, 10, group, start=8)
    query(monkeypatch, '10, GPU-abc, 2048\n', effect=lambda: process(proc, 10, group, start=9))
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    assert not sample.jobs[0].complete
    assert not sample.jobs[0].processes


def test_new_pid_during_query_is_unknown_this_sample(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    query(monkeypatch, '10, GPU-abc, 2048\n',
          effect=lambda: process(proc, 10, f'/prismabuild.slice/{SID}/payload'))
    assert not gm.collect([scope], proc_root=proc, cgroup_root=cgroup).jobs[0].complete


def test_shared_gpu_bytes_and_host_charges_are_never_summed_for_kill(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    for pid in (10, 11):
        process(proc, pid, f'/prismabuild.slice/{SID}/payload')
    (Path(scope.cgroup_path) / 'memory.current').write_text(str(800 * gm.MIB))
    query(monkeypatch, '10, GPU-abc, 800\n11, GPU-abc, 800\n10, GPU-abc, 800\n')
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    job = sample.jobs[0]
    assert job.gpu_reported_bytes == 1600 * gm.MIB
    assert job.lower_bound_bytes == 800 * gm.MIB
    guard = gm.Guard()
    assert guard.observe(sample) == []
    assert guard.observe(replace(sample, sampled_monotonic=sample.sampled_monotonic + 1)) == []


@pytest.mark.parametrize('output,rc', [('nonsense', 0), ('', 1)])
def test_bad_gpu_query_never_licenses_a_stop(tmp_path, monkeypatch, output, rc):
    proc, cgroup, scope = system(tmp_path)
    query(monkeypatch, output, rc)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete


def test_query_timeout_is_observable(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs['timeout'])
    monkeypatch.setattr(gm.subprocess, 'run', timeout)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    assert not sample.gpu_query_complete
    assert 'TimeoutExpired' in sample.errors[0]


def test_changed_cgroup_inode_cannot_be_stopped(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    def replace_group():
        path = Path(scope.cgroup_path)
        path.rename(path.with_name('retired'))
        path.mkdir()
        (path / 'memory.current').write_text(str(2000 * gm.MIB))
    query(monkeypatch, effect=replace_group)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    assert not sample.jobs[0].complete


def job(sid=SID, used=500, budget=1000, inode=2, complete=True):
    return gm.JobSample(sid, (1, inode), budget * gm.MIB, 100 * gm.MIB,
                        used * gm.MIB, used * gm.MIB, used * gm.MIB,
                        (used + 100) * gm.MIB, complete)


def snapshot(t, *jobs, available=4000, some=0, full=0):
    return gm.Snapshot(t, t, 0.1, 8000 * gm.MIB, available * gm.MIB,
                       some, full, tuple(jobs), True)


def test_two_repeated_overbudget_readings_stop_only_offending_scope():
    guard = gm.Guard()
    assert guard.observe(snapshot(1, job(used=1100), job(OTHER, used=500))) == []
    decisions = guard.observe(snapshot(2, job(used=1200), job(OTHER, used=500)))
    assert [(d.scope_id, d.reason) for d in decisions] == [(SID, 'memory_budget_exceeded')]
    assert decisions[0].cgroup_identity == (1, 2)
    json.dumps(decisions[0].as_dict())


@pytest.mark.parametrize('middle', ['unknown', 'underbudget', 'inode', 'gap', 'clock'])
def test_discontinuous_or_uncertain_budget_readings_reset_confirmation(middle):
    guard = gm.Guard()
    guard.observe(snapshot(10, job(used=1100)))
    mid = {'unknown': snapshot(11, job(used=1100, complete=False)),
           'underbudget': snapshot(11, job(used=500)),
           'inode': snapshot(11, job(used=1100, inode=3)),
           'gap': snapshot(20, job(used=1100)),
           'clock': snapshot(9, job(used=1100))}[middle]
    assert guard.observe(mid) == []


def test_pressure_only_stops_unique_growth_projected_to_exceed_own_budget():
    guard = gm.Guard(gm.Policy(reserve_bytes=500 * gm.MIB))
    for t, used, available in [(1, 400, 1000), (2, 600, 800), (3, 800, 600)]:
        decisions = guard.observe(snapshot(t, job(used=used), job(OTHER, used=300),
                                           available=available, some=2))
    assert [(d.scope_id, d.reason) for d in decisions] == [(SID, 'projected_host_oom')]


@pytest.mark.parametrize('case', ['foreign', 'ambiguous', 'healthy_budget', 'no_pressure'])
def test_host_pressure_does_not_blindly_kill_healthy_or_unattributable_jobs(case):
    guard = gm.Guard(gm.Policy(reserve_bytes=500 * gm.MIB))
    for t in range(1, 5):
        first = job(used=100 + t * 150, budget=5000 if case == 'healthy_budget' else 1000)
        second = job(OTHER, used=100 + t * 150 if case == 'ambiguous' else 100)
        available = (2000 - t * 500) if case == 'foreign' else (1200 - t * 150)
        assert guard.observe(snapshot(t, first, second, available=available,
                                      some=0 if case == 'no_pressure' else 2)) == []


def test_finished_scopes_release_guard_history():
    guard = gm.Guard()
    guard.observe(snapshot(1, job(used=1100)))
    guard.observe(snapshot(2))
    assert guard.over_budget == guard.pressure == {}


def test_collector_rejects_nonbroker_paths(tmp_path):
    with pytest.raises(ValueError, match='exact unique child'):
        gm.collect([gm.Scope(SID, tmp_path, 1)])


def test_census_deadline_marks_telemetry_incomplete(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    clock = iter([0.] + [4.] * 20)
    monkeypatch.setattr(gm.time, 'monotonic', lambda: next(clock))
    def forbidden(*args, **kwargs):
        raise AssertionError('an elapsed collection deadline still launched a command')
    monkeypatch.setattr(gm.subprocess, 'run', forbidden)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete
    assert any('deadline' in error for error in sample.errors)


def test_unreadable_host_pressure_never_selects_a_victim(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    (proc / 'pressure/memory').write_text('some avg10=nan\n')
    query(monkeypatch)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    assert sample.psi_some_avg10 is None and sample.psi_full_avg10 is None
    assert 'memory PSI unavailable' in sample.errors
