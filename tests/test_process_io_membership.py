"""A partial scope census must not retire and recount a surviving root."""
import json
from pathlib import Path

import pytest

from prismabuild import resource_scope


@pytest.fixture
def sampling(monkeypatch, tmp_path):
    scope = resource_scope.ResourceScope('a' * 64, 'b' * 32, 1024,
                                         tmp_path / 'telemetry.json')
    scope.cgroup_path = tmp_path / 'job.slice'
    monkeypatch.setattr(resource_scope, 'CGROUP_ROOT', tmp_path)
    state = {'pids': [42424242, 43434343], 'membership': '0::/job.slice/payload\n',
             'bytes': 100, 'identity': '42424242:100'}
    monkeypatch.setattr(resource_scope, 'scope_pids', lambda _: state['pids'])

    def counters(pid):
        value = state['bytes'] if pid == 42424242 else 20
        identity = state['identity'] if pid == 42424242 else '43434343:200'
        return identity, 1, {name: value for name in resource_scope.IO_COUNTERS}

    monkeypatch.setattr(resource_scope, 'read_process_io', counters)
    original = Path.read_text

    def read(path, *args, **kwargs):
        if path == Path('/proc/42424242/cgroup'):
            if isinstance(state['membership'], Exception):
                raise state['membership']
            return state['membership']
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'read_text', read)
    assert scope.sample_process_io()['wchar'] == 120
    state['pids'] = [43434343]
    return scope, state


@pytest.mark.parametrize('membership', ['0::/job.slice\n', '0::/job.slice/payload\n'])
def test_omitted_live_root_is_resampled(sampling, membership):
    scope, state = sampling
    state.update(membership=membership, bytes=150)
    during = scope.sample_process_io()
    assert during['retired']['wchar'] == 0
    assert during['wchar'] == 170
    assert during['processes_observed'] == 2
    state.update(pids=[42424242, 43434343], bytes=200)
    assert scope.sample_process_io()['wchar'] == 220


@pytest.mark.parametrize('membership', [PermissionError('unreadable membership'),
                                       OSError('procfs outage'), 'malformed\n'])
def test_unknown_membership_retains_prior_counters_across_reconstruction(sampling, membership):
    scope, state = sampling
    state.update(membership=membership, bytes=9000)
    during = scope.sample_process_io()
    assert during['retired']['wchar'] == 0
    assert during['wchar'] == 120, 'unknown membership must not add counters'
    assert during['processes_unreadable'] == 1 and during['errors']
    scope.telemetry_path.write_text(json.dumps({'nonce': scope.nonce, 'process_io': during}))
    scope._process_io = None
    state.update(pids=[42424242, 43434343], bytes=150)
    after = scope.sample_process_io()
    assert after['wchar'] == 170 and after['processes_observed'] == 2


@pytest.mark.parametrize('membership', [FileNotFoundError('gone'),
                                       '0::/other.slice\n', '0::/job.slice-other\n'])
def test_confirmed_departure_retires_only_the_previous_root(sampling, membership):
    scope, state = sampling
    state.update(membership=membership, bytes=9000)
    after = scope.sample_process_io()
    assert after['retired']['wchar'] == 100 and after['wchar'] == 120
    assert after['processes_live'] == 1 and after['processes_unreadable'] == 0


def test_omitted_pid_reuse_is_a_new_incarnation(sampling):
    scope, state = sampling
    state.update(identity='42424242:300', bytes=7)
    after = scope.sample_process_io()
    assert after['wchar'] == 127
    assert after['retired']['wchar'] == 100
    assert after['processes_observed'] == 3


def test_real_contained_process_survives_a_census_omission(monkeypatch, tmp_path):
    import subprocess
    import sys

    child = subprocess.Popen([sys.executable, '-c',
                              "import sys; print('ready', flush=True); sys.stdin.read()"],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    scope = resource_scope.ResourceScope('a' * 64, 'b' * 32, 1024,
                                         tmp_path / 'telemetry.json')
    try:
        assert child.stdout.readline() == 'ready\n'
        membership = next(line.split(':', 2)[2] for line in
                          Path(f'/proc/{child.pid}/cgroup').read_text().splitlines()
                          if line.startswith('0::'))
        scope.cgroup_path = resource_scope.CGROUP_ROOT / membership.lstrip('/')
        monkeypatch.setattr(resource_scope, 'scope_pids', lambda _: [child.pid])
        before = scope.sample_process_io()
        assert before['wchar'] >= 6
        monkeypatch.setattr(resource_scope, 'scope_pids', lambda _: [])
        after = scope.sample_process_io()
        assert after['retired']['wchar'] == 0 and after['processes_live'] == 1
        assert after['wchar'] == before['wchar']
    finally:
        child.stdin.close()
        child.wait(timeout=10)
        child.stdout.close()
