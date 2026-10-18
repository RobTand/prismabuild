"""Optional runtime paths must distinguish absence from unknown metadata."""
from contextlib import contextmanager
import errno
import json
import os
from pathlib import Path

import pytest

import capture_proof_runtime as capture
import proof_container_runtime as runtime


@contextmanager
def unknown_lookup(monkeypatch, target, operations):
    calls = []
    with monkeypatch.context() as patch:
        for operation in operations:
            original = getattr(os, operation)

            def lookup(path, *args, _original=original, _operation=operation, **kwargs):
                if isinstance(path, (str, bytes, os.PathLike)) and os.fspath(path) == str(target):
                    calls.append(dict(operation=_operation,
                        follow_symlinks=kwargs.get('follow_symlinks', _operation != 'lstat')))
                    raise OSError(errno.ELOOP, 'scoped runtime observation lookup failure', str(target))
                return _original(path, *args, **kwargs)

            patch.setattr(os, operation, lookup)
        try:
            yield calls
        finally:
            print('runtime-observation-fault: ' + json.dumps(dict(path=str(target), operations=calls)))
            assert calls, 'the actual filesystem observation boundary was not reached'


def preload_path(monkeypatch, path):
    real_path = runtime.Path
    monkeypatch.setattr(runtime, 'Path',
        lambda value: path if str(value) == '/etc/ld.so.preload' else real_path(value))


def test_preload_unknown_lookup_refuses_complete_observation(monkeypatch):
    with unknown_lookup(monkeypatch, '/etc/ld.so.preload', ('stat',)):
        with pytest.raises(OSError, match='scoped runtime observation lookup failure') as caught:
            runtime.observe_runtime()
        assert caught.value.errno == errno.ELOOP


def test_preload_readable_nonempty_file_refuses_observation(tmp_path, monkeypatch):
    path = tmp_path / 'preload'
    path.write_bytes(b'/fixture/preloaded.so\n')
    preload_path(monkeypatch, path)
    with pytest.raises(ValueError, match='system-wide loader preloads'):
        runtime.observe_runtime()


def test_preload_readable_empty_file_allows_observation(tmp_path, monkeypatch):
    path = tmp_path / 'empty-preload'
    path.write_bytes(b'\n')
    preload_path(monkeypatch, path)
    actual = runtime.observe_runtime()
    assert actual['executable']['path'] == runtime.sys.executable


def test_preload_genuinely_absent_file_allows_observation(tmp_path, monkeypatch):
    path = tmp_path / 'absent-preload'
    with pytest.raises(FileNotFoundError):
        path.stat()
    preload_path(monkeypatch, path)
    actual = runtime.observe_runtime()
    assert actual['executable']['path'] == runtime.sys.executable
    assert actual['version'] == list(runtime.sys.version_info[:3])


@pytest.fixture
def image_paths(tmp_path, monkeypatch):
    observation = runtime.observe_runtime()
    observation['sys_path'] = []
    stdlib = tmp_path / 'stdlib'
    native_base = tmp_path / 'native'
    for path in (stdlib, native_base / 'fixture-native'):
        path.mkdir(parents=True)
    cache, tool = tmp_path / 'cache', tmp_path / 'ldconfig'
    cache.write_bytes(b'fixture cache')
    tool.write_bytes(b'fixture listing tool')
    translated = {'/usr/lib': native_base, '/usr/lib64': tmp_path / 'extra',
        '/lib': tmp_path / 'absent-lib', '/lib64': tmp_path / 'absent-lib64',
        '/etc/ld.so.cache': cache, '/sbin/ldconfig': tool}
    real_path = capture.Path
    monkeypatch.setattr(capture, 'Path', lambda value: translated.get(str(value), real_path(value)))
    monkeypatch.setattr(capture, 'observe_runtime', lambda: observation)
    monkeypatch.setattr(capture.sysconfig, 'get_path', lambda name: str(stdlib))
    monkeypatch.setattr(capture.sysconfig, 'get_config_var', lambda name: 'fixture-native')
    monkeypatch.setattr(capture.site, 'getsitepackages', lambda: [])
    monkeypatch.setattr(capture.subprocess, 'check_output',
        lambda *args, **kwargs: '0 libs found in cache fixture:\n')
    return observation, translated


@pytest.mark.parametrize('candidate_source', ['site', 'sys_path'])
@pytest.mark.parametrize('state', ['present', 'absent', 'unknown'])
def test_image_startup_directory_observation(tmp_path, monkeypatch, image_paths, candidate_source, state):
    observation, _ = image_paths
    candidate = tmp_path / 'python-startup'
    if state != 'absent':
        candidate.mkdir()
        (candidate / 'startup_only.py').write_text('STARTUP_ONLY = True\n')
    else:
        with pytest.raises(FileNotFoundError):
            candidate.stat()
    if candidate_source == 'site':
        monkeypatch.setattr(capture.site, 'getsitepackages', lambda: [str(candidate)])
    else:
        observation['sys_path'] = [str(candidate)]
    if state == 'unknown':
        with unknown_lookup(monkeypatch, candidate, ('stat',)):
            with pytest.raises(OSError, match='scoped runtime observation lookup failure') as caught:
                capture.runtime_capture('image')
            assert caught.value.errno == errno.ELOOP
        return
    actual = capture.runtime_capture('image')
    roots = {root['path']: root for root in actual['roots']}
    assert (str(candidate) in roots) == (state == 'present')
    assert (str(candidate) in actual['elf_roots']) == (state == 'present')
    if state == 'present':
        assert roots[str(candidate)]['entries']['startup_only.py']['kind'] == 'file'


@pytest.mark.parametrize('kind', ['directory', 'symlink'])
@pytest.mark.parametrize('state', ['present', 'absent', 'unknown'])
def test_image_unused_lib64_observation(tmp_path, monkeypatch, image_paths, kind, state):
    _, translated = image_paths
    candidate = translated['/usr/lib64']
    if state != 'absent':
        if kind == 'directory':
            candidate.mkdir()
            (candidate / 'unused_python_only.py').write_text('UNUSED = True\n')
        else:
            target = tmp_path / 'unused-library-target'
            target.mkdir()
            candidate.symlink_to(target, target_is_directory=True)
    else:
        with pytest.raises(FileNotFoundError):
            candidate.lstat()
    if state == 'unknown':
        with unknown_lookup(monkeypatch, candidate, ('lstat', 'stat')):
            with pytest.raises(OSError, match='scoped runtime observation lookup failure') as caught:
                capture.runtime_capture('image')
            assert caught.value.errno == errno.ELOOP
        return
    actual = capture.runtime_capture('image')
    roots = {root['path']: root for root in actual['roots']}
    assert ('/usr/lib64' in roots) == (state == 'present')
    if state == 'present':
        entries = roots['/usr/lib64']['entries']
        assert entries['.']['kind'] == kind
        if kind == 'directory':
            assert entries['unused_python_only.py']['kind'] == 'file'
