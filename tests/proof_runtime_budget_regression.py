"""Explicit helper regressions, outside the configured test_*.py population."""
from contextlib import contextmanager
from pathlib import Path
import time

import pytest

import capture_proof_runtime as capture
import proof_container_runtime as runtime


def test_image_extra_directory_shares_the_primary_inventory_budget(tmp_path, monkeypatch):
    actual = runtime.observe_runtime()
    observation = dict(actual, sys_path=[])
    stdlib = tmp_path / 'stdlib'
    native_base = tmp_path / 'native'
    extra = tmp_path / 'extra'
    for path in (stdlib, native_base / 'fixture-native', extra):
        path.mkdir(parents=True)
    (extra / 'over-budget').write_bytes(b'one extra member')
    cache, tool = tmp_path / 'cache', tmp_path / 'ldconfig'
    cache.write_bytes(b'fixture cache')
    tool.write_bytes(b'fixture listing tool')
    translated = {'/usr/lib': native_base, '/usr/lib64': extra,
        '/lib': tmp_path / 'absent-lib', '/lib64': tmp_path / 'absent-lib64',
        '/etc/ld.so.cache': cache, '/sbin/ldconfig': tool}
    real_path, real_inventory = capture.Path, capture.inventory
    calls = []
    def image_fixture_path(value):
        return translated.get(str(value), real_path(value))
    def observed_inventory(path, *, budget=None):
        calls.append(dict(path=str(path), budget_present=budget is not None))
        return real_inventory(path, budget=budget)
    monkeypatch.setattr(capture, 'Path', image_fixture_path)
    monkeypatch.setattr(capture, 'observe_runtime', lambda: observation)
    monkeypatch.setattr(capture, 'native_providers', lambda: {})
    monkeypatch.setattr(capture.sysconfig, 'get_path', lambda name: str(stdlib))
    monkeypatch.setattr(capture.sysconfig, 'get_config_var', lambda name: 'fixture-native')
    monkeypatch.setattr(capture.site, 'getsitepackages', lambda: [])
    monkeypatch.setattr(capture.subprocess, 'check_output', lambda *args, **kwargs: '0 libs found in cache fixture:\n')
    monkeypatch.setattr(capture, 'inventory', observed_inventory)
    monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'members', 6)
    try:
        with pytest.raises(ValueError, match='capture geometry exceeds'):
            capture.runtime_capture('image')
    finally:
        print('actual_image_extra_budget_calls:', calls)


def test_inventory_bounds_scandir_frontier_before_complete_directory_enqueue(tmp_path, monkeypatch):
    root = tmp_path / 'many'
    root.mkdir()
    for index in range(4):
        (root / str(index)).write_bytes(b'entry')
    real_scandir = runtime.os.scandir
    yielded = []
    @contextmanager
    def observed_scandir(path):
        with real_scandir(path) as entries:
            def counted_entries():
                for entry in entries:
                    yielded.append(entry.path)
                    yield entry
            yield counted_entries()
    monkeypatch.setattr(runtime.os, 'scandir', observed_scandir)
    monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'members', 2)
    budget = dict(members=0, regular_bytes=0, deadline=time.monotonic() + 30)
    with pytest.raises(ValueError, match='capture geometry exceeds'):
        runtime.inventory(root, budget=budget)
    assert len(yielded) <= 2, 'scandir queued the entire over-budget directory: ' + repr(yielded)


def test_inventory_rejects_regular_byte_limit_before_opening_contents(tmp_path, monkeypatch):
    path = tmp_path / 'unreadable'
    path.write_bytes(b'not read before the byte guard')
    path.chmod(0)
    monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'regular_bytes', 0)
    budget = dict(members=0, regular_bytes=0, deadline=time.monotonic() + 30)
    try:
        with pytest.raises(ValueError, match='capture geometry exceeds'):
            runtime.inventory(path, budget=budget)
    finally:
        path.chmod(0o600)
