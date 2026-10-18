"""PT draft controls adapted to the complete image observation owner."""
import errno
import os
import time
from pathlib import Path

import pytest

import capture_proof_runtime as capture
import proof_container_runtime as runtime
from test_proof_runtime_observation import image_paths
from test_proof_runtime_required_graph import (
    HOST_EXECUTABLE, NATIVE, graph_fixture_elf, graph_outcome, required_graph_tree,
)


def test_interpreter_aliases_preserve_distinct_logical_seeds(required_graph_tree):
    tree = required_graph_tree
    host_seed, image_seed = '/lib/host-loader', '/lib64/image-loader'
    tree.host_file(HOST_EXECUTABLE, graph_fixture_elf(interpreter=host_seed, marker=b'host-pt'))
    tree.image_file(HOST_EXECUTABLE, graph_fixture_elf(interpreter=image_seed, marker=b'image-pt'))
    tree.image_link('/lib64', '/usr/lib')
    tree.image_link('/usr/lib/host-loader', 'fixture-native/ld-fixture.so')
    tree.image_link('/usr/lib/image-loader', NATIVE + '/ld-fixture.so')
    closure = tree.complete()
    closure['loader']['diagnostics']['executable']['path'] = host_seed
    assert closure['plan']['pt_seeds'] == [
        dict(origin='host', path=host_seed), dict(origin='image', path=image_seed)]
    for spelling in (host_seed, image_seed):
        outcome = graph_outcome(closure, spelling)
        assert outcome['status'] == 'resolved'
        assert outcome['terminal'] == NATIVE + '/ld-fixture.so'
    assert [row['target'] for row in graph_outcome(closure, host_seed)['chain']] == [
        'usr/lib', 'fixture-native/ld-fixture.so']
    assert [row['target'] for row in graph_outcome(closure, image_seed)['chain']] == [
        '/usr/lib', NATIVE + '/ld-fixture.so']
    exact_aliases = {'/lib', '/lib64', '/usr/lib/host-loader', '/usr/lib/image-loader'}
    retained = {root['path']: root for root in closure['roots']}
    assert exact_aliases <= retained.keys()
    assert all(set(retained[path]['entries']) == {'.'} for path in exact_aliases)
    assert '/usr/lib' not in retained
    tree.audit(closure)


@pytest.mark.parametrize('state', ['missing', 'unknown', 'cycle'])
def test_interpreter_aliases_refuse_unobserved_terminals(required_graph_tree, monkeypatch, state):
    tree = required_graph_tree
    logical, target = '/required-loader', '/target-loader'
    tree.host_file(HOST_EXECUTABLE, graph_fixture_elf(interpreter=logical, marker=b'required-pt'))
    tree.image_link(logical, target.lstrip('/'))
    if state == 'unknown':
        tree.image_file(target, graph_fixture_elf(marker=b'present-but-unobservable'))
    elif state == 'cycle':
        tree.image_link(target, logical.lstrip('/'))
    inputs = tree.inputs()
    calls = []
    if state == 'unknown':
        original_stat = os.stat

        def unobservable_pt_component(path, *args, **kwargs):
            if isinstance(path, (str, bytes, os.PathLike)) and os.fspath(path) in (
                    target.lstrip('/'), str(tree.physical('image', target))):
                calls.append(dict(path=os.fspath(path), dir_fd=kwargs.get('dir_fd'),
                                  follow_symlinks=kwargs.get('follow_symlinks')))
                raise OSError(errno.EACCES, 'scoped required alias lookup failure', os.fspath(path))
            return original_stat(path, *args, **kwargs)

        monkeypatch.setattr(os, 'stat', unobservable_pt_component)
        with pytest.raises(OSError, match='scoped required alias lookup failure') as caught:
            tree.complete(inputs)
        assert caught.value.errno == errno.EACCES
        assert len(calls) == 1, 'the required image component was not observed exactly once'
        assert calls[0]['dir_fd'] is not None
        assert calls[0]['follow_symlinks'] is False
    else:
        message = 'cycle|expansion' if state == 'cycle' else 'required.*absen|absen.*required'
        with pytest.raises(ValueError, match=message):
            tree.complete(inputs)


@pytest.mark.parametrize('kind', ['file', 'symlink', 'directory'])
def test_interpreter_aliases_recheck_covered_members(required_graph_tree, kind):
    tree = required_graph_tree
    path = '/covered'
    if kind == 'file':
        tree.image_file(path, graph_fixture_elf(marker=b'old'))
        logical = path
    elif kind == 'symlink':
        tree.image_link(path, '/old-loader')
        tree.image_file('/old-loader', graph_fixture_elf(marker=b'old-target'))
        tree.image_file('/new-loader', graph_fixture_elf(marker=b'new-target'))
        logical = path
    else:
        tree.directory('image', path)
        tree.image_file(path + '/loader', graph_fixture_elf(marker=b'covered-directory'))
        logical = path + '/loader'
    tree.image_paths.append(path)
    tree.host_file(HOST_EXECUTABLE, graph_fixture_elf(interpreter=logical, marker=b'covered-pt'))
    inputs = tree.inputs()
    captured_root = next(root for root in inputs[1]['roots'] if root['path'] == path)
    physical = tree.physical('image', path)
    if kind == 'file':
        tree.image_file(path, graph_fixture_elf(marker=b'new'))
        assert runtime.digest(physical.read_bytes()) != captured_root['entries']['.']['sha256']
    elif kind == 'symlink':
        physical.unlink()
        tree.image_link(path, '/new-loader')
        assert os.readlink(physical) != captured_root['entries']['.']['target']
    else:
        physical.chmod(0o700)
        assert physical.lstat().st_mode & 0o7777 != captured_root['entries']['.']['mode']
    with pytest.raises(ValueError, match='covered image runtime member changed'):
        tree.complete(inputs)


@pytest.mark.parametrize('kind', ['file', 'symlink'])
def test_interpreter_aliases_charge_shared_budget_before_data_reads(required_graph_tree, monkeypatch, kind):
    tree = required_graph_tree
    logical = '/over-budget'
    if kind == 'file':
        tree.image_file(logical, b'not read beyond the aggregate byte frontier')
    else:
        tree.image_link(logical, 'unread-target')
    host, image, checkpoint, budget = tree.inputs()
    deadline = budget['deadline']
    observer = capture.ImageClosureObserver(host, image, budget, checkpoint, tree.image_root)
    reads, lookups = [], []
    real_stat, real_open, real_readlink = os.stat, Path.open, os.readlink

    def budgeted_pt_stat(path, *args, **kwargs):
        if isinstance(path, (str, bytes, os.PathLike)) and os.fspath(path) in (
                logical.lstrip('/'), str(tree.physical('image', logical))):
            lookups.append(dict(path=os.fspath(path), dir_fd=kwargs.get('dir_fd'),
                                follow_symlinks=kwargs.get('follow_symlinks')))
        return real_stat(path, *args, **kwargs)

    def budgeted_pt_open(path, *args, **kwargs):
        if path.name == logical.lstrip('/'):
            reads.append(('open', str(path)))
        return real_open(path, *args, **kwargs)

    def budgeted_pt_readlink(path, *args, **kwargs):
        if isinstance(path, (str, bytes, os.PathLike)) and Path(path).name == logical.lstrip('/'):
            reads.append(('readlink', os.fspath(path)))
        return real_readlink(path, *args, **kwargs)

    monkeypatch.setattr(os, 'stat', budgeted_pt_stat)
    monkeypatch.setattr(Path, 'open', budgeted_pt_open)
    monkeypatch.setattr(os, 'readlink', budgeted_pt_readlink)
    counter = 'regular_bytes' if kind == 'file' else 'members'
    monkeypatch.setitem(runtime.CAPTURE_BOUNDS, counter, budget[counter])
    with pytest.raises(ValueError, match='capture geometry exceeds'):
        observer.lookup(logical)
    assert lookups, 'the actual anchored lookup never reached the over-budget member'
    assert lookups[0]['dir_fd'] is not None
    assert lookups[0]['follow_symlinks'] is False
    assert reads == [], 'over-budget alias data was read before shared-budget refusal'
    assert budget['deadline'] == deadline
    assert all(root['path'] != logical for root in observer.roots)


@pytest.mark.parametrize('primary', [None, 'relative-loader'], ids=['missing', 'relative'])
def test_image_capture_cli_requires_explicit_absolute_primary_interpreter(tmp_path, monkeypatch, capsys, primary):
    def forbidden_image_inventory(*args, **kwargs):
        pytest.fail('image inventory began without an explicit absolute host interpreter')

    monkeypatch.setattr(capture, 'runtime_capture', forbidden_image_inventory)
    output = tmp_path / 'image-members.tar.gz'
    argv = ['--capture-image', '--output', str(output)]
    if primary is not None:
        argv.extend(['--primary-interpreter', primary])
    with pytest.raises(SystemExit) as caught:
        capture.main(argv)
    assert caught.value.code == 2
    assert 'image capture requires an explicit absolute primary interpreter' in capsys.readouterr().err
    assert not output.exists()


def test_provider_header_observation_is_admitted_before_open(tmp_path, monkeypatch):
    provider = tmp_path / 'provider.so'
    provider.write_bytes(graph_fixture_elf())
    mappings = tmp_path / 'maps'
    mappings.write_text('1000-2000 r-xp 0 00:00 0 ' + str(provider) + '\n')
    actual_path, actual_open = runtime.Path, Path.open
    monkeypatch.setattr(runtime, 'Path', lambda value: mappings if str(value) == '/proc/self/maps' else actual_path(value))
    opened = []
    def provider_open(path, *args, **kwargs):
        if path == provider:
            opened.append(str(path))
        return actual_open(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', provider_open)
    monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'regular_bytes', 0)
    probes = []
    with pytest.raises(ValueError, match='capture geometry exceeds'):
        runtime.native_provider_paths(budget=dict(members=0, regular_bytes=0,
            deadline=time.monotonic() + 30), probes=probes)
    assert opened == []
    assert probes == []


def test_image_cache_listing_follows_inventory_admission(monkeypatch, image_paths):
    calls = []
    monkeypatch.setattr(capture, 'native_provider_paths', lambda **options: [])
    def cache_listing(*args, **kwargs):
        calls.append(args)
        return '0 libs found in cache fixture:\n'
    monkeypatch.setattr(capture.subprocess, 'check_output', cache_listing)
    monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'regular_bytes', 0)
    with pytest.raises(ValueError, match='capture geometry exceeds'):
        capture.runtime_capture('image')
    assert calls == []


def test_completion_profile_keeps_executable_bytes_in_archive(required_graph_tree):
    tree = required_graph_tree
    tree.host_file(HOST_EXECUTABLE, graph_fixture_elf(interpreter='/lib/ld-fixture.so',
        marker=b'H' * (8 * 1024**2)))
    tree.image_file(HOST_EXECUTABLE, graph_fixture_elf(interpreter='/lib/ld-fixture.so',
        marker=b'I' * (8 * 1024**2)))
    closure = tree.complete()
    assert len(runtime.canonical(closure)) < runtime.CAPTURE_BOUNDS['profile_bytes']
    tree.audit(closure)
