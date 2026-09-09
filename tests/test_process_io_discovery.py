"""An incomplete process census is diagnostic evidence, not an empty scope."""
import errno
from pathlib import Path

import pytest

from prismabuild import resource_scope


@pytest.fixture
def discovery(monkeypatch, tmp_path):
    scope = resource_scope.ResourceScope('a' * 64, 'b' * 32, 1024,
                                         tmp_path / 'telemetry.json')
    # An unreadable/nonexistent hierarchy forces the worker's procfs fallback.
    scope.cgroup_path = tmp_path / 'job.slice'
    monkeypatch.setattr(resource_scope, 'CGROUP_ROOT', tmp_path)
    state = {'entries': ['42424242', '43434343'],
             'membership': '0::/job.slice/payload\n'}
    original_listdir, original_read = resource_scope.os.listdir, Path.read_text

    def listdir(path):
        if str(path) == '/proc':
            if isinstance(state['entries'], Exception):
                raise state['entries']
            return state['entries']
        return original_listdir(path)

    def read(path, *args, **kwargs):
        if path == Path('/proc/42424242/cgroup'):
            if isinstance(state['membership'], Exception):
                raise state['membership']
            return state['membership']
        if path == Path('/proc/43434343/cgroup'):
            return '0::/job.slice\n'
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(resource_scope.os, 'listdir', listdir)
    monkeypatch.setattr(Path, 'read_text', read)
    monkeypatch.setattr(resource_scope, 'read_process_io', lambda pid: (
        f'{pid}:100', 1, {name: 100 for name in resource_scope.IO_COUNTERS}))
    return scope, state


@pytest.mark.parametrize('failure', [PermissionError('denied'),
                                     OSError(errno.EIO, 'procfs unavailable')])
def test_enumeration_failure_is_not_a_successfully_empty_census(discovery, failure):
    scope, state = discovery
    state['entries'] = failure
    record = scope.sample_process_io()
    assert record.get('errors'), 'procfs outage was reported as an empty successful census'
    assert record['processes_observed'] == 0


@pytest.mark.parametrize('failure', [PermissionError('denied'),
                                     OSError(errno.EIO, 'procfs unavailable'),
                                     'malformed membership\n', '0:bad:/job.slice\n'])
def test_unknown_membership_reports_partial_discovery_and_keeps_peer(discovery, failure):
    scope, state = discovery
    state['membership'] = failure
    record = scope.sample_process_io()
    assert record.get('errors'), 'unknown discovery was silently omitted'
    assert record['wchar'] == 100 and record['processes_observed'] == 1


@pytest.mark.parametrize('membership', [FileNotFoundError('departed'),
                                       ProcessLookupError('departed'),
                                       '0::/other.slice\n'])
def test_departure_and_other_scopes_are_not_discovery_failures(discovery, membership):
    scope, state = discovery
    state['membership'] = membership
    record = scope.sample_process_io()
    assert not record.get('errors')
    assert record['wchar'] == 100 and record['processes_observed'] == 1


def test_discovery_failure_retains_known_members_and_reports_unknown_ones(discovery):
    scope, state = discovery
    assert scope.sample_process_io()['wchar'] == 200
    state['entries'] = PermissionError('denied')
    record = scope.sample_process_io()
    assert record.get('errors'), 'known-member recovery does not prove a complete census'
    assert record['wchar'] == 200 and record['retired']['wchar'] == 0


def test_successful_procfs_fallback_is_not_an_error(discovery):
    scope, _ = discovery
    record = scope.sample_process_io()
    assert not record.get('errors')
    assert record['wchar'] == 200 and record['processes_observed'] == 2


def test_many_unknown_memberships_have_a_bounded_diagnostic(discovery, monkeypatch):
    scope, state = discovery
    state['entries'] = [str(pid) for pid in range(10000, 15000)]
    original = Path.read_text

    def read(path, *args, **kwargs):
        if path.parts[:2] == ('/', 'proc') and path.name == 'cgroup':
            raise PermissionError('denied')
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'read_text', read)
    record = scope.sample_process_io()
    assert len(record['errors']) == 1 and '5000' in record['errors'][0]
    assert len(record['errors'][0]) < 200


def test_missing_scope_membership_path_is_diagnostic(discovery):
    scope, _ = discovery
    scope.cgroup_path = Path('/outside-configured-cgroup-root')
    record = scope.sample_process_io()
    assert record.get('errors') and record['processes_observed'] == 0
