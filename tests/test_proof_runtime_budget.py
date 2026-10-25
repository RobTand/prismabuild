"""Capture inventory bounds before reading or enqueueing over-budget members."""
from contextlib import contextmanager
from pathlib import Path
import shutil
import subprocess
import sys
import textwrap
import time

import pytest

import capture_proof_runtime as capture
import proof_capture_capacity as capacity
import proof_container_runtime as runtime


@pytest.mark.parametrize('absolute', [False, True], ids=['relative', 'absolute'])
def test_capture_private_directory_is_absolute_for_docker_bind_source(tmp_path, monkeypatch, absolute):
    monkeypatch.chdir(tmp_path)
    output = Path('_proof_runtime_profiles/profile.json')
    if absolute:
        output = tmp_path / output
    private = capture.capture_private_directory(output)
    assert private == tmp_path / '_proof_runtime_profiles/profile.json.members'
    assert private.is_absolute()
    assert private.is_dir()


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
    def observed_inventory(path, *, budget=None, **options):
        calls.append(dict(path=str(path), budget_present=budget is not None))
        return real_inventory(path, budget=budget, **options)
    monkeypatch.setattr(capture, 'Path', image_fixture_path)
    monkeypatch.setattr(capture, 'observe_runtime', lambda: observation)
    monkeypatch.setattr(capture, 'native_provider_paths', lambda **options: [])
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
    target_opens = []
    real_open = Path.open
    def observed_open(current, *args, **kwargs):
        if current == path:
            target_opens.append(str(current))
        return real_open(current, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', observed_open)
    monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'regular_bytes', 0)
    budget = dict(members=0, regular_bytes=0, deadline=time.monotonic() + 30)
    with pytest.raises(ValueError, match='capture geometry exceeds'):
        runtime.inventory(path, budget=budget)
    assert target_opens == [], 'over-budget file was opened before refusal'


def test_checkpoint_metadata_refuses_before_file_or_cas_publication(tmp_path, monkeypatch):
    private = tmp_path / 'capture-metadata'
    private.mkdir()
    monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'metadata_bytes', 1)
    with pytest.raises(ValueError, match='capture metadata exceeds'):
        capture.publish_capture_checkpoint(None, private, 'host_inventory', 1, {}, None)
    assert list(private.iterdir()) == []
    monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'metadata_bytes', len(runtime.canonical({})) + 1)
    with pytest.raises(ValueError, match='capture metadata exceeds'):
        capacity.bounded_capture_json({}, prefix='prismabuild-root-capture: ')


def test_capture_refuses_preloaded_product_outside_the_published_generation(tmp_path, monkeypatch):
    import types
    alien = types.ModuleType('prismabuild.unsealed_capture_fixture')
    alien.__file__ = str(tmp_path / 'candidate.py')
    monkeypatch.setitem(capacity.sys.modules, alien.__name__, alien)
    with pytest.raises(ValueError, match='capture imported an unsealed product module'):
        capacity.load_published_capture_floor(tmp_path / 'published-generation', {})


def test_capture_accepts_sealed_dependencies_and_refuses_an_unsealed_module(tmp_path):
    root = Path(__file__).resolve().parents[1]
    published = tmp_path / 'published'
    shutil.copytree(root / 'src/prismabuild', published / 'src/prismabuild',
                    ignore=shutil.ignore_patterns('__pycache__'))
    (published / 'tools').mkdir()
    shutil.copyfile(root / 'tools/prismabuild_worker.py',
                    published / 'tools/prismabuild_worker.py')
    program = textwrap.dedent('''
        import importlib.machinery
        from pathlib import Path
        import sys
        import types
        sys.path.insert(0, sys.argv[1])
        import proof_capture_capacity as capacity
        published = Path(sys.argv[2]).resolve()
        sources = capacity.published_capture_sources(published)
        floor = capacity.load_published_capture_floor(published, sources)
        assert Path(floor.__file__) == published / 'src/prismabuild/filesystem_floor.py'
        assert 'prismabuild.immutable_files' in sys.modules
        capacity.check_published_capture_modules(published, sources)
        name = 'prismabuild.unsealed_capture_fixture'
        module = types.ModuleType(name)
        module.__file__ = str(published / 'src/prismabuild/unsealed_capture_fixture.py')
        module.__spec__ = importlib.machinery.ModuleSpec(name, loader=None,
                                                       origin=module.__file__)
        sys.modules[name] = module
        try:
            capacity.check_published_capture_modules(published, sources)
        except ValueError as error:
            assert str(error) == 'capture imported an unsealed product module: ' + name
        else:
            raise AssertionError('an unsealed module was accepted')
    ''')
    result = subprocess.run([sys.executable, '-B', '-c', program,
                             str(root / 'tests'), str(published)],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
