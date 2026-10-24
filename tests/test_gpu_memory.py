"""GPU bytes are attributed to exact owned scopes; ambiguous jobs stay alive."""
from dataclasses import replace
import json
import os
from pathlib import Path
import stat
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


def _gm_staged_query(monkeypatch, discovery, measured=None, *, effect=None, clock=None, elapsed=(),
                     measured_failure=None):
    """Keep discovery rows distinct; mutate identities during measurement only.

    Accept PID-only or full-row discovery without prescribing a private API.
    """
    calls = []

    def run(argv, **kwargs):
        assert argv[0] == gm.NVIDIA_SMI
        calls.append((argv, kwargs))
        assert len(calls) <= 2, 'one discovery and one measured query per sample'
        if len(calls) == 2 and effect is not None:
            effect()
        if clock is not None and len(calls) <= len(elapsed):
            clock[0] += elapsed[len(calls) - 1]
        if len(calls) == 2 and measured_failure is not None:
            if measured_failure == 'missing':
                raise FileNotFoundError('fixture vendor disappeared')
            if measured_failure == 'timeout':
                raise subprocess.TimeoutExpired(argv, kwargs['timeout'])
            if measured_failure == 'exit':
                return subprocess.CompletedProcess(argv, 1, '', 'fixture failed')
            assert measured_failure == 'malformed'
            return subprocess.CompletedProcess(argv, 0, 'not-a-pid', '')
        text = discovery if len(calls) == 1 else (discovery if measured is None else measured)
        fields = next(arg.split('=', 1)[1] for arg in argv if arg.startswith('--query-compute-apps='))
        if fields == 'pid':
            text = '\n'.join(dict.fromkeys(row.split(',')[0].strip() for row in text.splitlines()))
        else:
            assert fields == 'pid,gpu_uuid,used_gpu_memory'
        return subprocess.CompletedProcess(argv, 0, text, '')

    monkeypatch.setattr(gm.subprocess, 'run', run)
    return calls


def _gm_identity_reads(monkeypatch, proc, calls):
    reads = []
    original = Path.read_text

    def observe(path, *args, **kwargs):
        if path.parent.parent == proc and path.parent.name.isdigit() and path.name in {'stat', 'cgroup'}:
            reads.append((int(path.parent.name), path.name, len(calls)))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'read_text', observe)
    return reads


def test_exact_nested_scope_attribution_and_foreign_accounting(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    process(proc, 20, f'/prismabuild.slice/{SID}suffix/payload')
    query(monkeypatch, '10, GPU-abc, 512\n20, GPU-abc, 800\n')
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
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
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert not sample.jobs[0].complete
    assert sample.jobs[0].gpu_reported_bytes is None
    guard = gm.Guard()
    assert guard.observe(sample) == guard.observe(replace(sample, sampled_monotonic=sample.sampled_monotonic + 1)) == []


@pytest.mark.parametrize('domain', ['discrete', 'unknown'])
def test_vram_within_its_budget_does_not_violate_smaller_system_budget(tmp_path, monkeypatch, domain):
    proc, cgroup, scope = system(tmp_path)
    scope = replace(scope, gpu_budget_bytes=2000 * gm.MIB)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    query(monkeypatch, '10, GPU-abc, 1500\n')
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': domain})
    guard = gm.Guard()
    assert guard.observe(sample) == []
    assert guard.observe(replace(sample, sampled_monotonic=sample.sampled_monotonic + 1)) == []
    assert sample.jobs[0].lower_bound_bytes == 100 * gm.MIB


def test_explicit_gpu_subset_budget_is_enforced_on_shared_system(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    scope = replace(scope, gpu_budget_bytes=400 * gm.MIB)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    query(monkeypatch, '10, GPU-abc, 512\n')
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    guard = gm.Guard()
    assert guard.observe(sample) == []
    decisions = guard.observe(replace(sample, sampled_monotonic=sample.sampled_monotonic + 1))
    assert [decision.reason for decision in decisions] == ['gpu_memory_budget_exceeded']


def test_mixed_devices_charge_only_shared_allocations_to_system_memory(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    scope = replace(scope, gpu_budget_bytes=3000 * gm.MIB)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    query(monkeypatch, '10, GPU-shared, 500\n10, GPU-discrete, 2000\n')
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-shared': 'shared_system', 'GPU-discrete': 'discrete'})
    result = sample.jobs[0]
    assert result.memory_domain == 'unknown'
    assert result.lower_bound_bytes == result.system_lower_bound_bytes == 500 * gm.MIB
    assert result.upper_bound_bytes == 600 * gm.MIB
    assert result.gpu_lower_bound_bytes == 2000 * gm.MIB
    guard = gm.Guard()
    assert guard.observe(sample) == []
    assert guard.observe(replace(sample, sampled_monotonic=sample.sampled_monotonic + 1)) == []


def test_absent_domain_is_not_inferred_from_gpu_process_presence(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    query(monkeypatch, '10, GPU-abc, 512\n')
    result = gm.collect([scope], proc_root=proc, cgroup_root=cgroup).jobs[0]
    assert result.memory_domain == 'unknown'
    assert result.lower_bound_bytes == result.host_bytes == 100 * gm.MIB


def test_pid_reuse_invalidates_gpu_attribution(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    group = f'/prismabuild.slice/{SID}/payload'
    process(proc, 10, group, start=8)
    calls = _gm_staged_query(monkeypatch, '10, GPU-abc, 2048\n',
                             effect=lambda: process(proc, 10, group, start=9))
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert len(calls) == 2
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete
    assert not sample.jobs[0].processes


def test_new_pid_during_query_is_unknown_this_sample(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    calls = _gm_staged_query(monkeypatch, '', '10, GPU-abc, 2048\n',
                             effect=lambda: process(proc, 10, f'/prismabuild.slice/{SID}/payload'))
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert len(calls) == 2, 'even empty discovery cannot prove the measured inventory is empty'
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete
    assert not sample.jobs[0].processes


def test_shared_gpu_bytes_and_host_charges_are_never_summed_for_kill(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    for pid in (10, 11):
        process(proc, pid, f'/prismabuild.slice/{SID}/payload')
    (Path(scope.cgroup_path) / 'memory.current').write_text(str(800 * gm.MIB))
    query(monkeypatch, '10, GPU-abc, 800\n11, GPU-abc, 800\n10, GPU-abc, 800\n')
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
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
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete


def test_query_timeout_is_observable(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs['timeout'])
    monkeypatch.setattr(gm.subprocess, 'run', timeout)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert not sample.gpu_query_complete
    assert 'TimeoutExpired' in sample.errors[0]


def test_changed_cgroup_inode_cannot_be_stopped(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    def replace_group():
        path = Path(scope.cgroup_path)
        path.rename(path.with_name('retired'))
        path.mkdir()
        (path / 'memory.current').write_text(str(2000 * gm.MIB))
    calls = _gm_staged_query(monkeypatch, '', effect=replace_group)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert len(calls) == 2
    assert not sample.jobs[0].complete


def job(sid=SID, used=500, budget=1000, inode=2, complete=True):
    return gm.JobSample(sid, (1, inode), budget * gm.MIB, 100 * gm.MIB,
                        used * gm.MIB, used * gm.MIB, used * gm.MIB,
                        (used + 100) * gm.MIB, complete, memory_domain="shared_system")


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


def test_discrete_gpu_growth_does_not_predict_system_ram_exhaustion():
    guard = gm.Guard(gm.Policy(reserve_bytes=500 * gm.MIB))
    for t, used, available in [(1, 400, 1000), (2, 600, 800), (3, 800, 600)]:
        sample = replace(job(used=used), memory_domain='discrete', gpu_budget_bytes=2000 * gm.MIB)
        assert guard.observe(snapshot(t, sample, available=available, some=2)) == []


@pytest.mark.parametrize('domain', ['discrete', 'unknown'])
def test_system_ram_budget_remains_independent_of_larger_gpu_allowance(domain):
    guard = gm.Guard()
    over = replace(job(used=500), host_bytes=1100 * gm.MIB,
                   memory_domain=domain, gpu_budget_bytes=5000 * gm.MIB)
    assert guard.observe(snapshot(1, over)) == []
    decisions = guard.observe(snapshot(2, over))
    assert [decision.reason for decision in decisions] == ['memory_budget_exceeded']
    assert decisions[0].evidence['budget_domain'] == 'system'


def test_separate_budget_violations_do_not_share_confirmation_counts():
    guard = gm.Guard()
    base = replace(job(used=500), memory_domain='discrete')
    assert guard.observe(snapshot(1, replace(base, host_bytes=1100 * gm.MIB))) == []
    gpu_over = replace(base, gpu_lower_bound_bytes=1100 * gm.MIB)
    assert guard.observe(snapshot(2, gpu_over)) == []
    assert [d.reason for d in guard.observe(snapshot(3, gpu_over))] == ['gpu_memory_budget_exceeded']


def test_changed_gpu_budget_resets_confirmation():
    guard = gm.Guard()
    sample = replace(job(used=1200), memory_domain='discrete', gpu_budget_bytes=1000 * gm.MIB)
    assert guard.observe(snapshot(1, sample)) == []
    changed = replace(sample, gpu_budget_bytes=1100 * gm.MIB)
    assert guard.observe(snapshot(2, changed)) == []


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
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete
    assert any('deadline' in error for error in sample.errors)


def test_unreadable_host_pressure_never_selects_a_victim(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    (proc / 'pressure/memory').write_text('some avg10=nan\n')
    query(monkeypatch)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert sample.psi_some_avg10 is None and sample.psi_full_avg10 is None
    assert 'memory PSI unavailable' in sample.errors


AMD_DEVICE = {'uuid': 'GPU-9c30c352a59e5b7a', 'vendor': 'amd',
              'telemetry_class': 'memory_only', 'memory_domain': 'discrete'}


def holder(proc, node, pid, group, *, descriptors=1, start=9):
    """A process that has opened the GPU device node, as /proc shows it."""
    process(proc, pid, group, start=start)
    fds = proc / str(pid) / 'fd'
    fds.mkdir(exist_ok=True)
    for index in range(descriptors):
        (fds / str(index)).symlink_to(node)


def amd_system(tmp_path):
    proc, cgroup, scope = system(tmp_path)
    # A real character device, because the census identifies a handle by the
    # device number a regular file does not have.
    return proc, cgroup, scope, gm.Path('/dev/null')


def test_open_gpu_handles_attribute_ownership_without_inventing_bytes(tmp_path, monkeypatch):
    proc, cgroup, scope, node = amd_system(tmp_path)
    holder(proc, node, 10, f'/prismabuild.slice/{SID}/payload', descriptors=2)
    holder(proc, node, 20, '/user.slice/vllm')
    process(proc, 30, '/user.slice/idle')  # holds no handle, so holds no GPU

    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_devices=[AMD_DEVICE], gpu_device_node=node)

    job = sample.jobs[0]
    assert job.complete
    assert [row['pid'] for row in job.processes] == [10]
    assert [row['pid'] for row in sample.foreign_processes] == [20]
    # Ownership is known; bytes are not readable at all on this hardware, and
    # the sample says so rather than reporting a zero that reads as proof.
    assert job.gpu_reported_bytes is None and job.gpu_lower_bound_bytes is None
    assert job.gpu_budget_enforceable is False
    assert sample.gpu_process_bytes is False
    assert sample.foreign_inventory_scope == gm.FOREIGN_SCOPE_HOST_HANDLES
    # A discrete device keeps the scope's system charge in the cgroup counter,
    # so the system-memory bound is unchanged by the missing GPU counter.
    assert job.lower_bound_bytes == 100 * gm.MIB
    assert job.upper_bound_bytes == 100 * gm.MIB
    json.dumps(sample.as_dict())

    # Stable PID identities do not announce new GPU-handle opens. Re-inventory
    # descriptors and membership on the next sample, including foreign users.
    holder(proc, node, 30, '/user.slice/idle')
    process(proc, 20, f'/prismabuild.slice/{SID}/payload')
    fresh = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                       gpu_devices=[AMD_DEVICE], gpu_device_node=node)
    assert fresh.gpu_query_complete and fresh.jobs[0].complete
    assert {row['pid'] for row in fresh.jobs[0].processes} == {10, 20}
    assert [row['pid'] for row in fresh.foreign_processes] == [30]
    assert fresh.foreign_gpu_reported_bytes is None
    assert fresh.jobs[0].gpu_reported_bytes is None
    assert fresh.gpu_process_bytes is False
    assert fresh.foreign_inventory_scope == gm.FOREIGN_SCOPE_HOST_HANDLES


def test_an_unreadable_descriptor_table_refuses_the_handle_census(tmp_path, monkeypatch):
    proc, cgroup, scope, node = amd_system(tmp_path)
    holder(proc, node, 10, f'/prismabuild.slice/{SID}/payload')
    real = gm.Path.iterdir

    def iterdir(self):
        if self.name == 'fd':
            raise PermissionError(13, 'Permission denied')
        return real(self)

    monkeypatch.setattr(gm.Path, 'iterdir', iterdir)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_devices=[AMD_DEVICE], gpu_device_node=node)

    # An unprivileged census cannot see another user's handles, so its empty
    # foreign list would be a claim it has no evidence for.
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete


def test_a_descriptor_on_another_file_is_not_a_gpu_handle(tmp_path):
    proc, cgroup, scope, node = amd_system(tmp_path)
    decoy = tmp_path / 'dxg.log'
    decoy.write_text('')
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    fds = proc / '10' / 'fd'
    fds.mkdir()
    (fds / '1').symlink_to(decoy)

    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_devices=[AMD_DEVICE], gpu_device_node=node)

    assert sample.jobs[0].processes == ()
    assert sample.foreign_processes == ()


def test_a_regular_file_is_refused_as_the_device_node(tmp_path):
    proc, cgroup, scope, _ = amd_system(tmp_path)
    node = tmp_path / 'dxg'
    node.write_text('')

    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_devices=[AMD_DEVICE], gpu_device_node=node)

    assert not sample.gpu_query_complete
    assert any('not a file' in error for error in sample.errors)


class FakeStat:
    def __init__(self, mode, dev, ino, rdev):
        self.st_mode, self.st_dev, self.st_ino, self.st_rdev = mode, dev, ino, rdev


def test_a_containerized_holder_of_the_same_card_is_still_a_holder():
    # A container runtime makes its own node for a passed-through device: same
    # hardware, different filesystem and inode. Identifying a handle by inode
    # would report a GPU user inside a container as holding nothing.
    node = FakeStat(stat.S_IFCHR | 0o666, 6, 400, os.makedev(10, 63))
    inside = FakeStat(stat.S_IFCHR | 0o666, 43, 12, os.makedev(10, 63))
    other_card = FakeStat(stat.S_IFCHR | 0o666, 6, 401, os.makedev(10, 64))
    plain_file = FakeStat(stat.S_IFREG | 0o644, 6, 400, 0)

    assert gm._names_device(node, inside)
    assert not gm._names_device(node, other_card)
    assert not gm._names_device(node, plain_file)


def test_shared_system_hardware_without_per_process_bytes_stays_incomplete(tmp_path):
    proc, cgroup, scope, node = amd_system(tmp_path)
    holder(proc, node, 10, f'/prismabuild.slice/{SID}/payload')

    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_devices=[{**AMD_DEVICE, 'memory_domain': 'shared_system'}],
                        gpu_device_node=node)

    # On shared-system hardware the GPU bytes are also system bytes, so with no
    # per-process counter there is no system-memory lower bound to state.
    job = sample.jobs[0]
    assert not job.complete
    assert job.system_lower_bound_bytes is None


def test_a_missing_device_node_refuses_rather_than_reporting_no_holders(tmp_path):
    proc, cgroup, scope, _ = amd_system(tmp_path)
    node = tmp_path / 'absent-dxg'

    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_devices=[AMD_DEVICE], gpu_device_node=node)

    assert not sample.gpu_query_complete
    assert any('handle inventory unavailable' in error for error in sample.errors)


def test_nvidia_hosts_keep_the_compute_apps_reader_and_its_bytes(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    query(monkeypatch, '10, GPU-abc, 512\n')

    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_devices=[{'uuid': 'GPU-abc', 'vendor': 'nvidia',
                                      'memory_domain': 'shared_system'}])

    assert sample.gpu_process_bytes is True
    assert sample.foreign_inventory_scope == gm.FOREIGN_SCOPE_COMPUTE_APPS
    assert sample.jobs[0].gpu_reported_bytes == 512 * gm.MIB
    assert sample.jobs[0].gpu_budget_enforceable is True


@pytest.mark.parametrize('unrelated', [0, 64])
@pytest.mark.parametrize('active_scope', [False, True])
def test_nvidia_candidate_identity_reads_do_not_scale_with_host_pids(tmp_path, monkeypatch, unrelated, active_scope):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    process(proc, 20, '/user.slice/foreign')
    for pid in range(1000, 1000 + unrelated):
        process(proc, pid, '/unrelated')
    calls = _gm_staged_query(monkeypatch, '10, GPU-abc, 8192\n20, GPU-abc, 4096\n',
                             '10, GPU-abc, 512\n20, GPU-abc, 800\n')
    reads = _gm_identity_reads(monkeypatch, proc, calls)
    enumerations = []
    original_iterdir = Path.iterdir

    def observe_iterdir(path):
        if path == proc:
            enumerations.append(path)
        return original_iterdir(path)

    monkeypatch.setattr(Path, 'iterdir', observe_iterdir)
    sample = gm.collect([scope] if active_scope else [], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert not enumerations, 'NVIDIA candidate attribution must not enumerate host PIDs'
    assert len(calls) == 2
    assert {pid for pid, _, _ in reads} == {10, 20}
    assert len(reads) == 8, 'two identity files, before/after measurement, for each GPU candidate'
    for pid in (10, 20):
        for name in ('stat', 'cgroup'):
            assert [phase for observed, file, phase in reads if observed == pid and file == name] == [1, 2]
    assert sample.gpu_query_complete
    assert sample.foreign_inventory_scope == gm.FOREIGN_SCOPE_COMPUTE_APPS
    if active_scope:
        assert sample.jobs[0].complete
        assert sample.jobs[0].gpu_reported_bytes == 512 * gm.MIB
        assert sample.foreign_gpu_reported_bytes == 800 * gm.MIB
        assert [row['pid'] for row in sample.foreign_processes] == [20]
    else:
        assert sample.jobs == ()
        assert sample.foreign_gpu_reported_bytes == 1312 * gm.MIB
        assert {row['pid'] for row in sample.foreign_processes} == {10, 20}


@pytest.mark.parametrize('outcome', ['empty', 'exit', 'missing', 'timeout', 'malformed'])
def test_nvidia_failed_or_empty_discovery_avoids_identity_census(tmp_path, monkeypatch, outcome):
    proc, cgroup, scope = system(tmp_path)
    for pid in (10, *range(1000, 1064)):
        process(proc, pid, '/unrelated')
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        if outcome == 'missing':
            raise FileNotFoundError('fixture vendor unavailable')
        if outcome == 'timeout':
            raise subprocess.TimeoutExpired(argv, kwargs['timeout'])
        return subprocess.CompletedProcess(argv, int(outcome == 'exit'),
                                           'not-a-pid' if outcome == 'malformed' else '', '')

    monkeypatch.setattr(gm.subprocess, 'run', run)
    reads = _gm_identity_reads(monkeypatch, proc, calls)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    assert reads == [], 'unavailable/empty GPU inventory must not read unrelated process identities'
    assert len(calls) == (2 if outcome == 'empty' else 1)
    assert sample.gpu_query_complete is (outcome == 'empty')
    assert sample.jobs[0].complete is (outcome == 'empty')
    assert sample.jobs[0].gpu_reported_bytes == (0 if outcome == 'empty' else None)
    if outcome != 'empty':
        assert sample.errors
        guard = gm.Guard()
        assert guard.observe(sample) == []
        assert guard.observe(replace(sample, sampled_monotonic=sample.sampled_monotonic + 1)) == []


@pytest.mark.parametrize('failure', ['exit', 'missing', 'timeout', 'malformed'])
def test_nvidia_failed_measurement_does_not_use_discovery_bytes_or_scan_host(tmp_path, monkeypatch, failure):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    for pid in range(1000, 1064):
        process(proc, pid, '/unrelated')
    calls = _gm_staged_query(monkeypatch, '10, GPU-abc, 2048\n', measured_failure=failure)
    reads = _gm_identity_reads(monkeypatch, proc, calls)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert len(calls) == 2
    assert {pid for pid, _, _ in reads} == {10}, 'failure must not fall back to a host identity census'
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete
    assert sample.jobs[0].processes == ()
    assert sample.jobs[0].gpu_reported_bytes is None
    assert sample.foreign_gpu_reported_bytes is None
    assert sample.errors
    guard = gm.Guard()
    assert guard.observe(sample) == []
    assert guard.observe(replace(sample, sampled_monotonic=sample.sampled_monotonic + 1)) == []


@pytest.mark.parametrize('foreign', [False, True])
def test_nvidia_preexisting_but_undiscovered_pid_is_not_attributed(tmp_path, monkeypatch, foreign):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, '/user.slice/foreign' if foreign else f'/prismabuild.slice/{SID}/payload')
    calls = _gm_staged_query(monkeypatch, '', '10, GPU-abc, 2048\n')
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    assert len(calls) == 2
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete
    assert sample.jobs[0].processes == ()
    assert sample.foreign_processes == ()
    assert sample.foreign_gpu_reported_bytes is None


@pytest.mark.parametrize('moving_out', [False, True])
def test_nvidia_cgroup_move_during_measured_query_is_incomplete(tmp_path, monkeypatch, moving_out):
    proc, cgroup, scope = system(tmp_path)
    owned, foreign = f'/prismabuild.slice/{SID}/payload', '/user.slice/foreign'
    process(proc, 10, owned if moving_out else foreign)
    calls = _gm_staged_query(monkeypatch, '10, GPU-abc, 2048\n',
                             effect=lambda: process(proc, 10, foreign if moving_out else owned))
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup)
    assert len(calls) == 2
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete
    assert sample.jobs[0].processes == ()
    assert sample.foreign_processes == ()
    assert sample.foreign_gpu_reported_bytes is None


@pytest.mark.parametrize('name', ['stat', 'cgroup'])
@pytest.mark.parametrize('stage', ['before', 'after'])
def test_nvidia_unreadable_candidate_identity_is_unknown(tmp_path, monkeypatch, name, stage):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    target = proc / '10' / name
    if stage == 'before':
        target.unlink()
    calls = _gm_staged_query(monkeypatch, '10, GPU-abc, 2048\n',
                             effect=target.unlink if stage == 'after' else None)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup,
                        gpu_memory_domains={'GPU-abc': 'shared_system'})
    assert len(calls) == 2
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete
    assert sample.jobs[0].processes == ()
    assert sample.jobs[0].gpu_reported_bytes is None
    assert sample.foreign_gpu_reported_bytes is None
    guard = gm.Guard()
    assert guard.observe(sample) == []
    assert guard.observe(replace(sample, sampled_monotonic=sample.sampled_monotonic + 1)) == []


def test_nvidia_queries_share_collection_deadline(tmp_path, monkeypatch):
    proc, cgroup, scope = system(tmp_path)
    process(proc, 10, f'/prismabuild.slice/{SID}/payload')
    clock = [100.0]
    monkeypatch.setattr(gm.time, 'monotonic', lambda: clock[0])
    calls = _gm_staged_query(monkeypatch, '10, GPU-abc, 512\n', clock=clock, elapsed=(0.6, 0.1))
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup, timeout_s=1, max_collect_s=1)
    assert len(calls) == 2
    assert 0 < calls[0][1]['timeout'] <= 1
    assert 0 < calls[1][1]['timeout'] <= 0.400001
    assert sample.gpu_query_complete
    assert sample.jobs[0].complete


@pytest.mark.parametrize('elapsed', [(1.1,), (0.6, 0.5)])
def test_nvidia_deadline_exhaustion_even_with_empty_rows_is_incomplete(tmp_path, monkeypatch, elapsed):
    proc, cgroup, scope = system(tmp_path)
    clock = [100.0]
    monkeypatch.setattr(gm.time, 'monotonic', lambda: clock[0])
    calls = _gm_staged_query(monkeypatch, '', clock=clock, elapsed=elapsed)
    sample = gm.collect([scope], proc_root=proc, cgroup_root=cgroup, timeout_s=1, max_collect_s=1)
    assert len(calls) == len(elapsed), 'do not launch a measured query after the budget is exhausted'
    assert not sample.gpu_query_complete
    assert not sample.jobs[0].complete
    assert sample.errors
    assert any('deadline' in error or 'timeout' in error.lower() for error in sample.errors)
