"""Required native graph controls using actual temporary filesystem objects.

The native audit is static over ELF metadata, so metadata-only ELF fixtures
fully exercise its parser and dependency decisions. They do not run a loader.
Actual loader behavior is proven by a real capture's mounted role observations
and process mappings, outside these tests.
"""
import base64
from contextlib import contextmanager
import copy
from dataclasses import asdict
import errno
import json
import os
import runpy
from pathlib import Path
import shutil
import struct
import time
from types import SimpleNamespace

import pytest

import capture_proof_runtime as capture
import proof_container_runtime as runtime


NATIVE = '/usr/lib/fixture-native'
HOST_EXECUTABLE = '/usr/bin/python3.12'
PRIMARY = '/h/venv/bin/python'
LOADER = '/lib/ld-fixture.so'


def graph_fixture_elf(*, interpreter=None, needed=(), runpath=(), rpath=(), machine=183,
                      marker=b'fixture'):
    """Build ELF64 metadata that the real parser reads, without a compiler."""
    strings = bytearray(b'\0')
    tagged = []
    for tag, values in ((1, needed), (15, rpath), (29, runpath)):
        for value in values:
            tagged.append((tag, len(strings)))
            strings.extend(value.encode() + b'\0')
    interpreter_bytes = interpreter.encode() + b'\0' if interpreter else b''
    count = 2 + bool(interpreter)
    payload_offset = 64 + 56 * count
    dynamic_offset = payload_offset + len(interpreter_bytes)
    string_offset = dynamic_offset + (len(tagged) + 3) * 16
    dynamic = b''.join(struct.pack('<qQ', tag, value) for tag, value in [
        (5, 0x400000 + string_offset), (10, len(strings)), *tagged, (0, 0)])
    length = string_offset + len(strings) + len(marker)
    ident = b'\x7fELF\x02\x01\x01' + bytes(9)
    header = struct.pack('<16sHHIQQQIHHHHHH', ident, 3, machine, 1, 0, 64, 0,
                         0, 64, 56, count, 0, 0, 0)
    segments = [struct.pack('<IIQQQQQQ', 1, 4, 0, 0x400000, 0, length, length, 4096),
                struct.pack('<IIQQQQQQ', 2, 4, dynamic_offset,
                            0x400000 + dynamic_offset, 0, len(dynamic), len(dynamic), 8)]
    if interpreter:
        segments.append(struct.pack('<IIQQQQQQ', 3, 4, payload_offset,
                                    0x400000 + payload_offset, 0,
                                    len(interpreter_bytes), len(interpreter_bytes), 1))
    return header + b''.join(segments) + interpreter_bytes + dynamic + strings + marker


class RequiredGraphFilesystem:
    def __init__(self, root, *, host_executable=HOST_EXECUTABLE):
        self.host_root = root / 'host'
        self.image_root = root / 'image'
        self.host_executable = host_executable
        self.external_host_files = {}
        self.customization_roots = []
        self.customization_probes = []
        self.host_paths = ['/h/stdlib', NATIVE, '/h/venv/bin',
                           '/h/venv/pyvenv.cfg', self.host_executable]
        self.image_paths = ['/i/stdlib', NATIVE, HOST_EXECUTABLE,
                            '/etc/ld.so.cache', '/sbin/ldconfig']
        self.image_executable = HOST_EXECUTABLE
        self.cache_listing = '0 libs found in cache fixture:\n'
        self.diagnostic_raw = 'version.version=\"2.39\"\npath.system_dirs[0]=\"' + NATIVE + '/\"\nuname.release=\"6.8.0\"\n'
        self.host_interpreter = LOADER
        self.image_interpreter = LOADER
        self.host_file('/h/stdlib/plain.py', b'VALUE = 1\n')
        self.host_file(NATIVE + '/ld-fixture.so', graph_fixture_elf(marker=b'host-loader'))
        self.host_file('/h/venv/pyvenv.cfg', b'home = /usr/bin\n')
        self.host_link(PRIMARY, 'python3')
        self.host_link('/h/venv/bin/python3', '/usr/bin/python3')
        self.host_link('/h/venv/bin/python3.12', 'python3')
        self.host_file(self.host_executable, graph_fixture_elf(interpreter=LOADER, marker=b'host'))
        self.image_file('/i/stdlib/plain.py', b'IMAGE_VALUE = 1\n')
        self.image_file(NATIVE + '/ld-fixture.so', graph_fixture_elf(marker=b'hidden-image-loader'))
        self.image_file(HOST_EXECUTABLE, graph_fixture_elf(interpreter=LOADER, marker=b'image'))
        self.image_file('/etc/ld.so.cache', b'actual cache fixture bytes')
        self.image_file('/sbin/ldconfig', graph_fixture_elf(marker=b'cache-tool'))
        self.directory('image', '/h/venv')
        self.image_link('/usr/bin/python3', 'python3.12')
        self.image_link('/lib', 'usr/lib')
        self.image_link('/usr/lib/ld-fixture.so', 'fixture-native/ld-fixture.so')

    def physical(self, origin, spelling):
        if origin == 'host' and spelling in self.external_host_files:
            return self.external_host_files[spelling]
        return (self.host_root if origin == 'host' else self.image_root) / spelling.lstrip('/')

    def directory(self, origin, spelling):
        path = self.physical(origin, spelling)
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(0o755)
        return path

    def file(self, origin, spelling, value):
        path = self.physical(origin, spelling)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
        path.chmod(0o755 if value[:4] == b'\x7fELF' else 0o644)
        return path

    def link(self, origin, spelling, target):
        path = self.physical(origin, spelling)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.symlink_to(target)
        return path

    def host_file(self, spelling, value):
        return self.file('host', spelling, value)

    def image_file(self, spelling, value):
        return self.file('image', spelling, value)

    def host_link(self, spelling, target):
        return self.link('host', spelling, target)

    def image_link(self, spelling, target):
        return self.link('image', spelling, target)

    def snapshot(self, origin):
        paths = self.host_paths if origin == 'host' else self.image_paths
        roots, observations, elf_index = [], {}, {}
        budget = dict(members=0, regular_bytes=0, deadline=time.monotonic() + 30)
        if origin == 'host':
            previous = {item['target'] for item in self.customization_roots}
            paths = [path for path in paths if path not in previous]
            self.customization_roots, self.customization_probes = capture.capture_host_customizations(
                '/h/stdlib', paths, budget, physical=lambda path: self.physical('host', path))
            paths.extend(item['target'] for item in self.customization_roots)
            self.host_paths = paths
        for spelling in paths:
            physical = self.physical(origin, spelling)
            directories = {}
            entries = runtime.inventory(physical, budget=budget,
                                        directory_observations=directories)
            roots.append(dict(origin=origin, path=spelling, entries=entries))
            for path, value in directories.items():
                logical = str(Path(spelling) / Path(path).relative_to(physical))
                observations[logical] = value
            for name, entry in entries.items():
                if entry['kind'] == 'file':
                    logical = str(Path(spelling) / name)
                    elf_index[logical] = runtime.elf_requirements(physical / name)
        executable_path = self.host_executable if origin == 'host' else self.image_executable
        executable_bytes = self.physical(origin, executable_path).read_bytes()
        observation = dict(version=[3, 12, 3], elf_machine=183,
            executable=dict(path=PRIMARY if origin == 'host' else executable_path,
                resolved_path=executable_path, bytes=len(executable_bytes),
                sha256=runtime.digest(executable_bytes)),
            kernel=dict(release='6.8.0', system='Linux', machine='aarch64'),
            loader_environment={}, at_secure=0, sys_path=[], prefix='/h/venv')
        result = dict(glibc='ldd (GNU libc) 2.39\n', observation=observation, roots=roots,
            stdlib='/h/stdlib' if origin == 'host' else '/i/stdlib',
            providers={}, elf_roots=['/h/stdlib' if origin == 'host' else '/i/stdlib', executable_path],
            executable_binary=base64.b64encode(executable_bytes).decode(),
            multiarch='fixture-native', native_library_roots=[NATIVE],
            directory_observations=observations, elf_index=elf_index,
            inventory_budget={key: budget[key] for key in ('members', 'regular_bytes')},
            consumed_budget={key: budget[key] for key in ('members', 'regular_bytes')})
        if origin == 'host':
            result.update(customization_roots=self.customization_roots,
                          customization_probes=self.customization_probes)
        if origin == 'image':
            cache = next(root['entries']['.'] for root in roots if root['path'] == '/etc/ld.so.cache')
            tool = next(root['entries']['.'] for root in roots if root['path'] == '/sbin/ldconfig')
            host_pt, = runtime.elf_requirements(self.physical('host', self.host_executable))['interpreters']
            result['pre_completion_diagnostics'] = dict(raw=self.diagnostic_raw,
                command=[host_pt, '--list-diagnostics'],
                executable=dict(path=host_pt, resolved_path=NATIVE + '/ld-fixture.so',
                    **capture.file_descriptor(self.physical('host', NATIVE + '/ld-fixture.so'))))
            result['loader_cache'] = dict(path='/etc/ld.so.cache',
                bytes=cache['bytes'], sha256=cache['sha256'], raw_listing=self.cache_listing,
                tool=dict(path='/sbin/ldconfig', resolved_path='/sbin/ldconfig',
                    bytes=tool['bytes'], sha256=tool['sha256']))
        return result

    def inputs(self):
        host, image = self.snapshot('host'), self.snapshot('image')
        checkpoint_bytes = runtime.canonical(dict(phase='host_inventory', units=1, value=host))
        checkpoint = dict(sha256=runtime.digest(checkpoint_bytes), bytes=len(checkpoint_bytes))
        budget = dict(deadline=time.monotonic() + 30,
            **{key: host['consumed_budget'][key] + image['consumed_budget'][key]
               for key in ('members', 'regular_bytes')})
        return host, image, checkpoint, budget

    def complete(self, inputs=None):
        host, image, checkpoint, budget = self.inputs() if inputs is None else inputs
        completed = capture.complete_image_closure(host, image, budget,
            host_checkpoint=checkpoint, image_root=self.image_root)
        namespace = json.loads(runtime.canonical(asdict(completed)))
        self.host_checkpoint = host
        self.image_checkpoint = dict(image, completed_namespace=copy.deepcopy(namespace),
            consumed_budget=dict(completed.consumed_budget))
        self.host_descriptor = checkpoint
        closure = copy.deepcopy(namespace)
        closure.update(schema=runtime.CLOSURE_SCHEMA, graph_policy='required-runtime-graph-v1',
            elf_machine=183, elf_roots=sorted(set(host['elf_roots'] + image['elf_roots'])))
        loader_entry = self.entry(closure, NATIVE + '/ld-fixture.so')
        host_pt, = host['elf_index'][self.host_executable]['interpreters']
        closure['loader'] = dict(schema='prismabuild.proof_loader_search.v1',
            cache=image['loader_cache'], diagnostics=dict(
                raw=getattr(self, 'post_diagnostic_raw', self.diagnostic_raw),
                executable=dict(path=host_pt, resolved_path=NATIVE + '/ld-fixture.so',
                    bytes=loader_entry['bytes'], sha256=loader_entry['sha256'])))
        return closure

    def entry(self, closure, spelling):
        entries = {str(Path(root['path']) / name): entry for root in closure['roots']
                   for name, entry in root['entries'].items()}
        return entries[spelling]

    def bytes_path(self, closure, spelling):
        expected = self.entry(closure, spelling)
        for root in closure['roots']:
            if Path(spelling).is_relative_to(root['path']):
                path = self.physical(root['origin'], spelling)
                if path.is_file() and runtime.digest(path.read_bytes()) == expected['sha256']:
                    return path
        raise AssertionError('no physical fixture bytes for ' + spelling)

    def checkpoint_agreement(self, closure):
        auditor = runpy.run_path(str(Path(__file__).resolve().parents[1]
            / 'tools/maintenance/verify_proof_runtime_capture.py'))
        auditor['verify_capture_completion_checkpoints'](closure, self.host_checkpoint,
            self.image_checkpoint, self.host_descriptor, runtime.__dict__)

    def audit(self, closure, retained=None):
        self.checkpoint_agreement(closure)
        if retained is None:
            return runtime.audit_runtime_closure(closure, lambda path: self.bytes_path(closure, path),
                content_path=lambda sha: self.content_path(closure, sha))
        return runtime.audit_runtime_closure(closure,
            lambda path: Path(retained[self.entry(closure, path)['sha256']]),
            content_path=lambda sha: Path(retained[sha]))

    def retained_members(self, closure):
        roots = closure['roots'] + closure['completion']['base_host_roots'] + closure['completion']['base_image_roots']
        members = {str(self.physical(root['origin'], str(Path(root['path']) / name))): entry
                   for root in roots for name, entry in root['entries'].items() if entry['kind'] == 'file'}
        archive = self.host_root.parent / 'required-graph-members.tar.gz'
        descriptor = capture.archive_members(archive, members)
        assert descriptor['bytes'] == archive.stat().st_size
        assert descriptor['sha256'] == runtime.digest(archive.read_bytes())
        retained = capture.retain_image_members(archive, self.host_root.parent / 'retained-by-sha')
        assert set(retained) == {entry['sha256'] for entry in members.values()}
        return retained

    def content_path(self, closure, sha):
        roots = closure['roots'] + closure['completion']['base_host_roots'] + closure['completion']['base_image_roots']
        for root in roots:
            for name, entry in root['entries'].items():
                if entry['kind'] == 'file' and entry['sha256'] == sha:
                    path = self.physical(root['origin'], str(Path(root['path']) / name))
                    if runtime.digest(path.read_bytes()) == sha:
                        return path
        raise AssertionError('no actual retained fixture bytes for ' + sha)

    def mount_host_roots(self):
        for spelling in self.host_paths:
            source = self.physical('host', spelling)
            destination = self.physical('image', spelling)
            if destination.is_symlink() or destination.is_file():
                destination.unlink()
            elif destination.is_dir():
                shutil.rmtree(destination)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source.is_dir():
                shutil.copytree(source, destination, symlinks=True)
            else:
                shutil.copy2(source, destination, follow_symlinks=False)


@pytest.fixture
def required_graph_tree(tmp_path):
    return RequiredGraphFilesystem(tmp_path)


def graph_outcome(closure, spelling):
    return runtime.runtime_outcome_record(runtime.replay_runtime_path(closure, spelling))


def test_off_graph_absence_is_retained_and_replayed(required_graph_tree):
    tree = required_graph_tree
    spelling = NATIVE + '/pkgconfig/unused.pc'
    tree.host_link(spelling, '/etc/alternatives/unused.pc')
    tree.directory('image', '/etc/alternatives')
    tree.image_file('/etc/alternatives/other', b'complete name enumeration')
    closure = tree.complete()
    outcome = graph_outcome(closure, spelling)
    assert outcome == closure['link_outcomes'][spelling]
    assert outcome['status'] == 'witnessed_absent'
    assert outcome['missing'] == '/etc/alternatives/unused.pc'
    witness = closure['directory_witnesses'][outcome['witness']]
    assert witness['names'] == ['other']
    assert witness['before'] == witness['after']
    assert witness['operation'] == 'lstat_at_nofollow'
    assert witness['errno'] == 'ENOENT'
    assert outcome['chain'][0]['target'] == '/etc/alternatives/unused.pc'
    tree.audit(closure)
    del closure['directory_witnesses'][outcome['witness']]
    with pytest.raises((ValueError, KeyError), match='witness|absence|runtime'):
        tree.audit(closure)


@pytest.mark.parametrize('candidate_source', ['variant', 'cache', 'absolute'])
def test_candidate_promotion_refuses_dangling_link(required_graph_tree, candidate_source):
    tree = required_graph_tree
    tree.directory('image', '/missing')
    tree.host_file('/h/stdlib/mod.so', graph_fixture_elf(
        needed=('/missing/provider',) if candidate_source == 'absolute' else ('libdep.so',),
        runpath=(NATIVE,)))
    tree.host_file(NATIVE + '/libdep.so', graph_fixture_elf(marker=b'good-direct-provider'))
    if candidate_source == 'variant':
        tree.host_link(NATIVE + '/glibc-hwcaps/tune/libdep.so', '/missing/provider')
    elif candidate_source == 'cache':
        tree.cache_listing = '1 libs found in cache fixture:\n libdep.so (libc6,AArch64) => /missing/provider\n'
    with pytest.raises((ValueError, FileNotFoundError), match='absen|unresolved|required|sealed|candidate'):
        tree.audit(tree.complete())


def test_positive_unused_alias_retains_origin_candidate(required_graph_tree):
    """Judge A: a present image alternative returns to a host ELF."""
    tree = required_graph_tree
    tree.host_file('/h/stdlib/mod.so', graph_fixture_elf(
        needed=('libdep.so',), runpath=('$ORIGIN/private',)))
    tree.host_file('/h/stdlib/private/libdep.so', graph_fixture_elf(marker=b'stdlib-provider'))
    tree.host_file(NATIVE + '/private/libdep.so', graph_fixture_elf(marker=b'alias-provider'))
    tree.host_link(NATIVE + '/alias', '/etc/alternatives/core')
    tree.image_link('/etc/alternatives/core', '/h/stdlib/mod.so')
    tree.image_file('/etc/alternatives/private/libdep.so', graph_fixture_elf(machine=62))
    tree.image_file('/h/stdlib/mod.so', b'image bytes hidden by the host root')
    closure = tree.complete()
    assert graph_outcome(closure, NATIVE + '/alias')['terminal'] == '/h/stdlib/mod.so'
    assert graph_outcome(closure, '/etc/alternatives/core')['terminal'] == '/h/stdlib/mod.so'
    assert tree.entry(closure, '/h/stdlib/mod.so')['sha256'] == runtime.digest(
        tree.physical('host', '/h/stdlib/mod.so').read_bytes())
    assert tree.entry(closure, '/etc/alternatives/private/libdep.so')['kind'] == 'file'
    with pytest.raises(ValueError, match='unsupported architecture|architecture.*ABI'):
        tree.audit(closure)


def test_map_gap_cannot_be_classified_as_absence(required_graph_tree):
    tree = required_graph_tree
    spelling, target = NATIVE + '/pkgconfig/present.pc', '/etc/alternatives/present.pc'
    tree.host_link(spelling, target)
    tree.image_file(target, b'this present file was outside the initial roots')
    host, image, checkpoint, budget = tree.inputs()
    assert all(not Path(target).is_relative_to(root['path']) for root in image['roots'])
    closure = tree.complete((host, image, checkpoint, budget))
    assert graph_outcome(closure, spelling)['terminal'] == target
    assert tree.entry(closure, target)['sha256'] == runtime.digest(tree.physical('image', target).read_bytes())
    forged = copy.deepcopy(closure)
    forged['link_outcomes'][spelling] = dict(status='witnessed_absent', spelling=spelling,
        missing=target, remaining=[], chain=closure['link_outcomes'][spelling]['chain'], witness='0' * 64)
    with pytest.raises((ValueError, KeyError), match='witness|absence|outcome|runtime'):
        tree.audit(forged)


def test_late_alias_adds_origin_before_completion(required_graph_tree):
    tree = required_graph_tree
    tree.host_file('/h/stdlib/mod.so', graph_fixture_elf(
        needed=('libdep.so',), runpath=('/generated/providers',)))
    tree.host_file(NATIVE + '/pool/libdep.so', graph_fixture_elf(
        needed=('liblate.so',), runpath=('${ORIGIN}-private',), marker=b'generated-candidate'))
    tree.host_file(NATIVE + '/pool-private/liblate.so', graph_fixture_elf(marker=b'late-good'))
    tree.image_link('/generated/providers', NATIVE + '/pool')
    tree.image_file('/generated/providers-private/liblate.so', graph_fixture_elf(machine=62))
    closure = tree.complete()
    generated = '/generated/providers/libdep.so'
    entries = runtime.runtime_namespace_entries(closure)
    assert generated not in entries
    assert generated not in closure['link_outcomes']
    assert graph_outcome(closure, generated)['terminal'] == NATIVE + '/pool/libdep.so'
    assert entries['/generated/providers']['kind'] == 'symlink'
    assert entries['/generated/providers-private/liblate.so']['kind'] == 'file'
    metadata = {NATIVE + '/pool/libdep.so': runtime.elf_requirements(
        tree.physical('host', NATIVE + '/pool/libdep.so'))}
    declared = runtime.audit_link_outcomes(closure)
    without_generated, _ = runtime.runtime_elf_search_directories(metadata, declared)
    with_generated, _ = runtime.runtime_elf_search_directories(
        metadata, dict(declared, **{generated: NATIVE + '/pool/libdep.so'}))
    assert '/generated/providers-private' not in without_generated
    assert with_generated - without_generated == {'/generated/providers-private'}
    with pytest.raises(ValueError, match='unsupported architecture|architecture.*ABI'):
        tree.audit(closure)


def test_component_walk_preserves_directory_alias_and_dotdot_order(required_graph_tree):
    """Judge E: lookup must visit a component before cancelling its suffix."""
    tree = required_graph_tree
    physical = tree.host_root.parent / 'ordered-component-red'
    physical.mkdir()
    (physical / 'present').write_bytes(b'cannot be reached through the missing component')
    (physical / 'trap').symlink_to(str(physical / 'missing/../present'))
    recorded = runtime.inventory(physical)
    entries = {str(physical / name): entry for name, entry in recorded.items()}
    for ancestor in physical.parents:
        entries[str(ancestor)] = dict(kind='directory', mode=ancestor.stat().st_mode & 0o7777)
    with pytest.raises(ValueError, match='missing|sealed|absen|component'):
        runtime.resolve_runtime_path(entries, str(physical / 'trap'))
    tree.image_link('/route/relative', '../target/dir')
    tree.image_link('/route/absolute', '/target/dir')
    tree.image_file('/target/file', b'real ordered lookup target')
    tree.directory('image', '/target/dir')
    for name in ('relative', 'absolute'):
        tree.host_link(NATIVE + '/' + name, '/route/' + name + '/../file')
    closure = tree.complete()
    for name in ('relative', 'absolute'):
        assert graph_outcome(closure, NATIVE + '/' + name)['terminal'] == '/target/file'
        assert any(row['path'] == '/route/' + name for row in
                   closure['link_outcomes'][NATIVE + '/' + name]['chain'])
    tree.host_link('/h/stdlib/trap', '/missing/../h/stdlib/plain.py')
    with pytest.raises((ValueError, FileNotFoundError), match='absen|required|missing|symlink'):
        tree.complete()
    tree.image_file('/not-directory', b'cannot traverse a regular file')
    tree.physical('host', '/h/stdlib/trap').unlink()
    tree.host_link('/h/stdlib/trap', '/not-directory/../h/stdlib/plain.py')
    with pytest.raises((ValueError, NotADirectoryError), match='directory|component'):
        tree.complete()


@pytest.mark.parametrize('failure', ['eio', 'eloop', 'estale', 'permission', 'scandir', 'changed'])
def test_unknown_or_mutating_absence_observation_refuses(required_graph_tree, monkeypatch, failure):
    tree = required_graph_tree
    tree.host_link(NATIVE + '/unused.pc', '/etc/alternatives/unused.pc')
    parent = tree.directory('image', '/etc/alternatives')
    tree.image_file('/etc/alternatives/sentinel', b'one name before a failed enumeration')
    inputs = tree.inputs()
    seen = []
    negative_checks = []
    real_stat, real_lstat, real_scandir, real_fstat = os.stat, os.lstat, os.scandir, os.fstat
    codes = dict(eio=errno.EIO, eloop=errno.ELOOP, estale=errno.ESTALE, permission=errno.EACCES)

    def absence_lookup_fault(path, *args, **kwargs):
        if isinstance(path, (str, bytes, os.PathLike)) and os.fspath(path) in ('unused.pc', str(parent / 'unused.pc')):
            seen.append(('lookup', os.fspath(path)))
            raise OSError(codes[failure], 'scoped absence lookup failure', os.fspath(path))
        return real_stat(path, *args, **kwargs)

    def absence_lstat_fault(path, *args, **kwargs):
        if isinstance(path, (str, bytes, os.PathLike)) and os.fspath(path) in ('unused.pc', str(parent / 'unused.pc')):
            seen.append(('lookup', os.fspath(path)))
            raise OSError(codes[failure], 'scoped absence lookup failure', os.fspath(path))
        return real_lstat(path, *args, **kwargs)

    @contextmanager
    def incomplete_absence_names(path):
        observed = real_fstat(path) if isinstance(path, int) else real_stat(path)
        is_parent = (observed.st_dev, observed.st_ino) == (parent.stat().st_dev, parent.stat().st_ino)
        with real_scandir(path) as children:
            def interrupted_names():
                for child in children:
                    yield child
                    if is_parent:
                        seen.append(('scandir', child.name))
                        raise OSError(errno.EIO, 'scoped incomplete absence enumeration')
            yield interrupted_names() if is_parent else children

    def changed_absence_stamp(descriptor):
        observed = real_fstat(descriptor)
        if negative_checks and (observed.st_dev, observed.st_ino) == (parent.stat().st_dev, parent.stat().st_ino):
            seen.append(('fstat', descriptor))
            if len(seen) >= 2:
                tree.image_file('/etc/alternatives/mutated', b'change stable directory stamp')
                observed = real_fstat(descriptor)
        return observed

    def changing_absence_lookup(path, *args, **kwargs):
        if isinstance(path, (str, bytes, os.PathLike)) and os.fspath(path) in ('unused.pc', str(parent / 'unused.pc')):
            negative_checks.append(os.fspath(path))
        return real_stat(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        if failure in codes:
            patch.setattr(os, 'stat', absence_lookup_fault)
            patch.setattr(os, 'lstat', absence_lstat_fault)
        elif failure == 'scandir':
            patch.setattr(os, 'scandir', incomplete_absence_names)
        else:
            patch.setattr(os, 'stat', changing_absence_lookup)
            patch.setattr(os, 'fstat', changed_absence_stamp)
        with pytest.raises((OSError, ValueError), match='scoped|changed|stable'):
            tree.complete(inputs)
        assert seen, 'the real absence observation boundary was not reached'

def test_host_mount_authority_wins_after_alias_expansion(required_graph_tree):
    """Judge D: an image alias lands on the host executable mount."""
    tree = required_graph_tree
    closure = tree.complete()
    selected = graph_outcome(closure, PRIMARY)
    assert selected['terminal'] == HOST_EXECUTABLE
    assert [row['target'] for row in selected['chain']] == ['python3', '/usr/bin/python3', 'python3.12']
    assert tree.entry(closure, HOST_EXECUTABLE)['sha256'] == runtime.digest(
        tree.physical('host', HOST_EXECUTABLE).read_bytes())
    assert tree.entry(closure, HOST_EXECUTABLE)['sha256'] != runtime.digest(
        tree.physical('image', HOST_EXECUTABLE).read_bytes())
    tree.host_link(NATIVE + '/exit-host.pc', '/external/exit-host.pc')
    tree.image_file('/external/exit-host.pc', b'image authority after exiting host root')
    tree.host_link(NATIVE + '/hidden.pc', NATIVE + '/absent-under-mount')
    tree.image_file(NATIVE + '/absent-under-mount', b'hidden underneath complete host directory')
    closure = tree.complete()
    assert graph_outcome(closure, NATIVE + '/exit-host.pc')['terminal'] == '/external/exit-host.pc'
    hidden = graph_outcome(closure, NATIVE + '/hidden.pc')
    assert hidden['status'] == 'witnessed_absent'
    assert closure['directory_witnesses'][hidden['witness']]['authority'] == 'host'
    tree.audit(closure)


def test_primary_alias_absence_refuses(required_graph_tree):
    tree = required_graph_tree
    tree.physical('image', '/usr/bin/python3').unlink()
    with pytest.raises((ValueError, FileNotFoundError), match='absen|required|primary|runtime'):
        tree.complete()
    assert not tree.physical('image', '/usr/bin/python3').exists()


@pytest.mark.parametrize('state', ['present', 'missing_host', 'missing_image', 'cycle'])
def test_distinct_pt_seeds_are_independently_required(required_graph_tree, state):
    tree = required_graph_tree
    host_seed, image_seed = '/host-loader', '/image-loader'
    tree.host_file(HOST_EXECUTABLE, graph_fixture_elf(interpreter=host_seed, marker=b'host'))
    tree.image_file(HOST_EXECUTABLE, graph_fixture_elf(interpreter=image_seed, marker=b'image'))
    if state != 'missing_host':
        tree.image_link(host_seed, NATIVE + '/ld-fixture.so')
    if state != 'missing_image':
        tree.image_link(image_seed, '/cycle' if state == 'cycle' else NATIVE + '/ld-fixture.so')
    if state == 'cycle':
        tree.image_link('/cycle', image_seed)
    if state != 'present':
        with pytest.raises((ValueError, FileNotFoundError), match='absen|required|cycle|expansion|runtime'):
            tree.complete()
        return
    closure = tree.complete()
    assert {(row['origin'], row['path']) for row in closure['plan']['pt_seeds']} == {
        ('host', host_seed), ('image', image_seed)}
    for seed in (host_seed, image_seed):
        assert graph_outcome(closure, seed)['terminal'] == NATIVE + '/ld-fixture.so'
    tree.audit(closure)


def test_cache_candidate_failure_is_not_hidden_by_fallback(required_graph_tree):
    tree = required_graph_tree
    tree.host_file('/h/stdlib/mod.so', graph_fixture_elf(needed=('libdep.so',), runpath=(NATIVE,)))
    tree.host_file(NATIVE + '/libdep.so', graph_fixture_elf(marker=b'successful-fallback'))
    tree.host_link(NATIVE + '/cache/libdep.so', '/missing/cache-libdep.so')
    tree.directory('image', '/missing')
    tree.cache_listing = ('1 libs found in cache fixture:\n'
        ' libdep.so (libc6,AArch64) => ' + NATIVE + '/cache/libdep.so\n')
    with pytest.raises((ValueError, FileNotFoundError), match='absen|required|unresolved|candidate'):
        tree.audit(tree.complete())


@pytest.mark.parametrize('boundary', ['member', 'file', 'names', 'deadline'])
def test_completion_and_absence_evidence_share_pre_read_budget(required_graph_tree, monkeypatch, boundary):
    tree = required_graph_tree
    tree.host_link(NATIVE + '/extra.pc', '/external/extra.pc')
    external = tree.image_file('/external/extra.pc', b'not read beyond the byte frontier')
    tree.host_link(NATIVE + '/unused.pc', '/etc/alternatives/unused.pc')
    tree.directory('image', '/etc/alternatives')
    for index in range(8):
        tree.image_file('/etc/alternatives/name-' + str(index), b'name-only witness')
    inputs = tree.inputs()
    budget = inputs[3]
    old_deadline = budget['deadline']
    reads = []
    witness_names = []
    original_open = Path.open

    def budgeted_fixture_open(path, *args, **kwargs):
        if path == external or path.name == external.name and '/proc/self/fd/' in str(path):
            reads.append(str(path))
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'open', budgeted_fixture_open)
    if boundary == 'member':
        monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'members', budget['members'])
    elif boundary == 'file':
        monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'regular_bytes', budget['regular_bytes'])
    elif boundary == 'names':
        tree.physical('host', NATIVE + '/extra.pc').unlink()
        successful = tree.complete()
        preceding = 0
        for charge in successful['completion']['charges']:
            if charge['kind'] == 'absence' and charge['path'] == '/etc/alternatives/unused.pc':
                assert charge['members'] == 9, 'the absence and all eight names must spend the member budget'
                break
            preceding += charge['members']
        else:
            pytest.fail('the real image absence observation was never charged')
        inputs = tree.inputs()
        budget = inputs[3]
        old_deadline = budget['deadline']
        monkeypatch.setitem(runtime.CAPTURE_BOUNDS, 'members', budget['members'] + preceding + 3)
        actual_scandir = os.scandir
        parent = tree.physical('image', '/etc/alternatives')

        @contextmanager
        def bounded_witness_scandir(path):
            observed = os.fstat(path) if isinstance(path, int) else os.stat(path)
            same_parent = (observed.st_dev, observed.st_ino) == (parent.stat().st_dev, parent.stat().st_ino)
            with actual_scandir(path) as children:
                def counted_names():
                    for child in children:
                        if same_parent:
                            witness_names.append(child.name)
                        yield child
                yield counted_names()

        monkeypatch.setattr(os, 'scandir', bounded_witness_scandir)
    else:
        budget['deadline'] = time.monotonic() - 1
        old_deadline = budget['deadline']
    with pytest.raises((ValueError, TimeoutError), match='geometry|budget|deadline|finite'):
        tree.complete(inputs)
    assert budget['deadline'] == old_deadline
    assert reads == [], 'completion read external bytes beyond the shared frontier'
    if boundary == 'names':
        assert 0 < len(witness_names) <= 3, 'the witness frontier consumed a complete over-budget directory'


def test_graph_work_cannot_advance_progress(required_graph_tree, monkeypatch):
    tree = required_graph_tree
    tree.host_link(NATIVE + '/unused.pc', '/etc/alternatives/unused.pc')
    tree.directory('image', '/etc/alternatives')
    checkpoints = []

    def forbidden_graph_checkpoint(*args, **kwargs):
        checkpoints.append((args, kwargs))
        raise AssertionError('graph work reached the durable checkpoint publication owner')

    monkeypatch.setattr(capture, 'publish_capture_checkpoint', forbidden_graph_checkpoint)
    inputs = tree.inputs()
    deadline = inputs[3]['deadline']
    tree.audit(tree.complete(inputs))
    assert checkpoints == []
    assert inputs[3]['deadline'] == deadline


@pytest.mark.parametrize('tamper', ['target', 'witness', 'required', 'elf_index', 'budget',
                                  'checkpoint', 'startup_bytes', 'schema'])
def test_retained_auditor_recomputes_graph_and_absences(required_graph_tree, tamper):
    tree = required_graph_tree
    spelling = NATIVE + '/unused.pc'
    tree.host_link(spelling, '/etc/alternatives/unused.pc')
    tree.host_link('/h/stdlib/sitecustomize.py', '/etc/python3.12/sitecustomize.py')
    startup = b'import sys\nsys.dont_write_bytecode = True\n'
    tree.image_file('/etc/python3.12/sitecustomize.py', startup)
    tree.directory('image', '/etc/alternatives')
    closure = tree.complete()
    retained = tree.retained_members(closure)
    expected = tree.audit(closure, retained)
    assert expected
    forged = copy.deepcopy(closure)
    if tamper == 'target':
        root = next(root for root in forged['roots'] if root['path'] == NATIVE)
        root['entries']['unused.pc']['target'] = '/different/unused.pc'
    elif tamper == 'witness':
        key = forged['link_outcomes'][spelling]['witness']
        forged['directory_witnesses'][key]['names'].append('unused.pc')
    elif tamper == 'required':
        forged['plan']['required_spellings'].remove(PRIMARY)
    elif tamper == 'elf_index':
        forged['completion']['host_elf_index'][HOST_EXECUTABLE]['interpreters'] = []
    elif tamper == 'budget':
        forged['consumed_budget']['members'] -= 1
    elif tamper == 'checkpoint':
        forged['completion']['host_checkpoint']['sha256'] = '0' * 64
    elif tamper == 'startup_bytes':
        path = Path(retained[runtime.digest(startup)])
        path.chmod(0o644)
        path.write_bytes(b'pass\n')
    else:
        forged['schema'] = 'prismabuild.proof_runtime_closure.v1'
    with pytest.raises((ValueError, KeyError), match='runtime|closure|witness|budget|plan|schema|checkpoint|ELF|completion'):
        tree.audit(forged, retained)


def graph_replace_witness(closure, old_key, witness):
    new_key = runtime.digest(runtime.canonical(witness))
    del closure['directory_witnesses'][old_key]
    closure['directory_witnesses'][new_key] = witness
    for outcome in closure['link_outcomes'].values():
        if outcome.get('witness') == old_key:
            outcome['witness'] = new_key
    for charge in closure['completion']['charges']:
        if charge.get('witness') == old_key:
            charge['witness'] = new_key
    return new_key


def graph_sync_namespace(tree, closure):
    namespace = tree.image_checkpoint['completed_namespace']
    tree.image_checkpoint['completed_namespace'] = {
        name: copy.deepcopy(closure[name]) for name in namespace}
    tree.image_checkpoint['consumed_budget'] = copy.deepcopy(closure['consumed_budget'])


@pytest.mark.parametrize('tamper, message', [
    ('plan', 'plan differs from independently derived seeds'),
    ('elf_index', 'ELF index differs from retained bytes'),
    ('witness', 'listing omits a retained child'),
    ('budget', 'charge counters differ from observed work'),
], ids=['plan', 'elf_index', 'witness', 'budget'])
def test_semantic_replay_rejects_coherent_checkpoint_tampering(required_graph_tree, tamper, message):
    tree = required_graph_tree
    tree.host_link(NATIVE + '/unused.pc', '/etc/alternatives/unused.pc')
    tree.host_link(NATIVE + '/present.pc', '/etc/alternatives/present.pc')
    tree.image_file('/etc/alternatives/present.pc', b'positive retained child')
    closure = tree.complete()
    retained = tree.retained_members(closure)
    assert tree.audit(closure, retained)
    forged = copy.deepcopy(closure)
    if tamper == 'plan':
        forged['plan']['required_spellings'].remove(PRIMARY)
    elif tamper == 'elf_index':
        path = '/i/stdlib/plain.py'
        metadata = copy.deepcopy(forged['completion']['host_elf_index'][NATIVE + '/ld-fixture.so'])
        forged['completion']['image_elf_index'][path] = metadata
        tree.image_checkpoint['elf_index'][path] = copy.deepcopy(metadata)
    elif tamper == 'witness':
        old_key = forged['link_outcomes'][NATIVE + '/unused.pc']['witness']
        witness = copy.deepcopy(forged['directory_witnesses'][old_key])
        witness['names'].remove('present.pc')
        new_key = graph_replace_witness(forged, old_key, witness)
        for charge in forged['completion']['charges']:
            if charge.get('witness') == new_key:
                charge['members'] -= 1
                forged['consumed_budget']['members'] -= 1
    else:
        charge = next(charge for charge in forged['completion']['charges']
                      if charge['kind'] == 'absence')
        charge['members'] += 1
        forged['consumed_budget']['members'] += 1
    graph_sync_namespace(tree, forged)
    tree.checkpoint_agreement(forged)
    with pytest.raises(ValueError, match=message):
        tree.audit(forged, retained)


@pytest.mark.parametrize('stage', ['producer', 'auditor'])
def test_host_absence_witness_is_bound_to_checkpoint_observation(required_graph_tree, monkeypatch, stage):
    tree = required_graph_tree
    spelling = NATIVE + '/hidden.pc'
    tree.host_link(spelling, NATIVE + '/absent-under-mount')
    tree.image_file(NATIVE + '/absent-under-mount', b'hidden image positive')
    calls = []

    def changed_host_stamp(parent, name, before, after, names, checkpoint_sha256):
        altered = tuple(value + 1 if index == 1 else value for index, value in enumerate(before))
        calls.append(parent)
        return runtime.HostRuntimeAbsence(parent, name, altered, altered, names, checkpoint_sha256)

    if stage == 'producer':
        monkeypatch.setattr(capture, 'HostRuntimeAbsence', changed_host_stamp)
        with pytest.raises(ValueError, match='host runtime absence.*checkpoint.*observation'):
            tree.complete()
        assert calls == [NATIVE]
        return
    closure = tree.complete()
    retained = tree.retained_members(closure)
    assert tree.audit(closure, retained)
    old_key = closure['link_outcomes'][spelling]['witness']
    witness = copy.deepcopy(closure['directory_witnesses'][old_key])
    original = copy.deepcopy(closure['completion']['host_directory_observations'][NATIVE])
    witness['before'][1] += 1
    witness['after'][1] += 1
    new_key = graph_replace_witness(closure, old_key, witness)
    assert new_key != old_key
    assert witness['before'] == witness['after']
    assert witness['names'] == original['names']
    graph_sync_namespace(tree, closure)
    tree.checkpoint_agreement(closure)
    assert closure['completion']['host_directory_observations'][NATIVE] == original
    with pytest.raises(ValueError, match='host runtime absence.*checkpoint.*observation'):
        tree.audit(closure, retained)


def test_absent_target_appearing_invalidates_mounted_profile(required_graph_tree):
    """Judge F: an image negative is checked again against real files."""
    tree = required_graph_tree
    tree.host_link(NATIVE + '/unused.pc', '/etc/alternatives/unused.pc')
    tree.directory('image', '/etc/alternatives')
    closure = tree.complete()
    tree.mount_host_roots()
    runtime.verify_runtime_negatives(closure, image_root=tree.image_root)
    tree.image_file('/etc/alternatives/unused.pc', b'appeared after the sealed observation')
    with pytest.raises(ValueError, match='absen|negative|changed|witness'):
        runtime.verify_runtime_negatives(closure, image_root=tree.image_root)


@pytest.mark.parametrize('root', ['/h/stdlib', '/h/venv/bin'])
def test_absence_outside_native_library_root_refuses(required_graph_tree, root):
    tree = required_graph_tree
    tree.host_link(root + '/unused', '/etc/alternatives/unused')
    tree.directory('image', '/etc/alternatives')
    with pytest.raises((ValueError, FileNotFoundError), match='absen|required|native|symlink|runtime'):
        tree.complete()


def test_external_sitecustomize_arbitrary_source_is_sealed(required_graph_tree):
    """Judge C follows A2 and keeps the existing Python startup contract."""
    tree = required_graph_tree
    target = '/etc/python3.12/sitecustomize.py'
    content = b'import sys\nsys.dont_write_bytecode = True\n'
    tree.host_link('/h/stdlib/sitecustomize.py', target)
    tree.image_file(target, content)
    closure = tree.complete()
    assert graph_outcome(closure, '/h/stdlib/sitecustomize.py')['terminal'] == target
    assert tree.entry(closure, target)['bytes'] == len(content)
    assert tree.entry(closure, target)['sha256'] == runtime.digest(content)
    tree.audit(closure)
    tree.physical('image', target).unlink()
    with pytest.raises((ValueError, FileNotFoundError), match='absen|required|runtime|symlink'):
        tree.complete()


def test_mounted_positive_navigation_component_change_refuses(required_graph_tree):
    tree = required_graph_tree
    tree.host_link('/h/stdlib/sitecustomize.py', '/etc/python3.12/sitecustomize.py')
    tree.image_file('/etc/python3.12/sitecustomize.py', b'import sys\nsys.dont_write_bytecode = True\n')
    closure = tree.complete()
    tree.mount_host_roots()
    runtime.verify_runtime_negatives(closure, image_root=tree.image_root)
    parent = tree.physical('image', '/etc/python3.12')
    redirected = parent.with_name('redirected-python')
    parent.rename(redirected)
    parent.symlink_to(redirected.name, target_is_directory=True)
    assert (parent / 'sitecustomize.py').read_bytes() == (redirected / 'sitecustomize.py').read_bytes()
    with pytest.raises((OSError, ValueError), match='directory|component|navigation'):
        runtime.verify_runtime_negatives(closure, image_root=tree.image_root)


def test_mount_created_ancestors_close_primary_alias(required_graph_tree):
    tree = required_graph_tree
    shutil.rmtree(tree.physical('image', '/h'))
    closure = tree.complete()
    assert graph_outcome(closure, PRIMARY)['terminal'] == HOST_EXECUTABLE
    assert any(step['path'] == '/usr/bin/python3'
               for step in closure['link_outcomes'][PRIMARY]['chain'])
    for path, names in (('/h', ['stdlib', 'venv']), ('/h/venv', ['bin', 'pyvenv.cfg'])):
        assert closure['components'][path] == dict(origin='mount', entry=dict(kind='directory'), names=names)
    retained = tree.retained_members(closure)
    assert tree.audit(closure, retained)
    runtime.verify_runtime_negatives(closure, image_root=tree.host_root, origins=('host',))
    tree.mount_host_roots()
    runtime.verify_runtime_negatives(closure, image_root=tree.image_root)
    tree.physical('image', '/h/venv').chmod(0o750)
    runtime.verify_runtime_negatives(closure, image_root=tree.image_root)


@pytest.mark.parametrize('kind', ['symlink', 'file'])
def test_image_host_mount_ancestor_refuses_non_directory(required_graph_tree, kind):
    tree = required_graph_tree
    shutil.rmtree(tree.physical('image', '/h'))
    if kind == 'symlink':
        tree.directory('image', '/image-h')
        tree.image_link('/h', '/image-h')
    else:
        tree.image_file('/h', b'cannot create a host bind destination below this file')
    host, image, checkpoint, budget = tree.inputs()
    observer = capture.ImageClosureObserver(host, image, budget, checkpoint, tree.image_root)
    with pytest.raises(ValueError, match='mount.*ancestor.*directory'):
        observer.lookup('/h')
    with pytest.raises(ValueError, match='mount.*ancestor.*directory'):
        tree.complete()


@pytest.mark.parametrize('state', ['off_graph', 'required', 'tampered'])
def test_mount_created_missing_sibling_requires_plan_witness(required_graph_tree, state):
    tree = required_graph_tree
    shutil.rmtree(tree.physical('image', '/h'))
    spelling, missing = NATIVE + '/unused.pc', '/h/venv/not-mounted'
    tree.host_link(spelling, missing)
    if state == 'required':
        tree.host_file('/h/stdlib/mod.so', graph_fixture_elf(needed=(missing,)))
        with pytest.raises(ValueError, match='required.*absent.*not-mounted'):
            tree.complete()
        return
    closure = tree.complete()
    outcome = closure['link_outcomes'][spelling]
    assert outcome['status'] == 'witnessed_absent'
    assert outcome['missing'] == missing
    witness = closure['directory_witnesses'][outcome['witness']]
    assert witness == dict(parent='/h/venv', name='not-mounted', names=['bin', 'pyvenv.cfg'],
                          authority='mount', operation='mount_plan_children')
    retained = tree.retained_members(closure)
    assert tree.audit(closure, retained)
    if state == 'tampered':
        forged = copy.deepcopy(closure)
        forged['components']['/h/venv']['names'].append('zz-invented')
        changed = copy.deepcopy(witness)
        changed['names'].append('zz-invented')
        assert changed['names'] == sorted(set(changed['names']))
        assert changed['name'] not in changed['names']
        assert forged['components']['/h/venv']['names'] == changed['names']
        graph_replace_witness(forged, outcome['witness'], changed)
        graph_sync_namespace(tree, forged)
        tree.checkpoint_agreement(forged)
        with pytest.raises(ValueError, match='mount.*(children|listing|component)'):
            tree.audit(forged, retained)
        return
    tree.mount_host_roots()
    runtime.verify_runtime_negatives(closure, image_root=tree.image_root)
    tree.image_file(missing, b'a mounted sibling appeared')
    with pytest.raises(ValueError, match='mount.*(children|listing)|absen|negative'):
        runtime.verify_runtime_negatives(closure, image_root=tree.image_root)


@pytest.mark.parametrize('change', ['extra_child', 'symlink'])
def test_mount_created_directory_is_verified_after_mount(required_graph_tree, change):
    tree = required_graph_tree
    shutil.rmtree(tree.physical('image', '/h'))
    closure = tree.complete()
    tree.mount_host_roots()
    runtime.verify_runtime_negatives(closure, image_root=tree.image_root)
    if change == 'extra_child':
        tree.image_file('/h/venv/unplanned', b'extra child is outside the mount plan')
    else:
        parent = tree.physical('image', '/h/venv')
        parent.rename(tree.image_root / 'redirected-venv')
        parent.symlink_to('../../redirected-venv', target_is_directory=True)
    with pytest.raises((OSError, ValueError), match='mount|directory|component'):
        runtime.verify_runtime_negatives(closure, image_root=tree.image_root)


def graph_reader_transition_fixture(tree, *, primary_through_reader=False):
    tree.directory('image', '/usr/bin')
    tree.physical('image', '/usr/bin/python3').unlink()
    tree.image_executable = '/usr/local/bin/python3.12'
    tree.image_paths.remove(HOST_EXECUTABLE)
    tree.image_paths.append(tree.image_executable)
    tree.image_file(tree.image_executable,
                    graph_fixture_elf(interpreter=LOADER, marker=b'image-local-reader'))
    tree.image_link('/usr/local/bin/python3', 'python3.12')
    if not primary_through_reader:
        tree.physical('host', '/h/venv/bin/python3').unlink()
        tree.host_link('/h/venv/bin/python3', tree.host_executable)
    return tree


def test_reader_alias_creation_has_separate_namespace_states(required_graph_tree):
    tree = graph_reader_transition_fixture(required_graph_tree)
    closure = tree.complete()
    audit = tree.audit(closure, tree.retained_members(closure))
    assert set(closure['namespace_states']) == {'before_installation', 'after_installation'}
    assert set(audit['namespace_states']) == set(closure['namespace_states'])
    before = runtime.runtime_namespace_state(closure, 'before_installation')
    after = runtime.runtime_namespace_state(closure, 'after_installation')
    absent = runtime.replay_runtime_path(before, '/usr/bin/python3')
    assert isinstance(absent, runtime.RuntimeWitnessedAbsent)
    assert absent.missing == '/usr/bin/python3' and not absent.remaining
    assert before['directory_witnesses'][absent.witness]['authority'] == 'image'
    assert runtime.replay_runtime_path(before, PRIMARY).terminal == HOST_EXECUTABLE
    assert '/usr/bin/python3' not in before['plan']['required_spellings']
    assert '/usr/bin/python3' in after['plan']['required_spellings']
    assert runtime.replay_runtime_path(after, '/usr/bin/python3').terminal == tree.image_executable
    assert absent.witness not in after['directory_witnesses']
    assert not tree.physical('image', '/usr/bin/python3').exists()


def test_reader_creation_cannot_repair_primary_namespace(required_graph_tree):
    tree = graph_reader_transition_fixture(required_graph_tree, primary_through_reader=True)
    with pytest.raises(ValueError, match='required runtime alias.*' + PRIMARY):
        tree.complete()


def test_reader_creation_preserves_direct_python314_primary_route(tmp_path):
    tree = graph_reader_transition_fixture(RequiredGraphFilesystem(
        tmp_path / 'python314-route', host_executable='/usr/bin/python3.14'))
    closure = tree.complete()
    installation = closure['reader_installation']
    assert installation['path'] == '/usr/bin/python3'
    assert installation['target'] == '/usr/local/bin/python3.12'
    assert installation['absent_before']['status'] == 'witnessed_absent'
    assert installation['absent_before']['missing'] == '/usr/bin/python3'
    assert installation['entry'] == dict(kind='symlink', mode=0o777,
                                         target=installation['target'])
    before = runtime.runtime_namespace_state(closure, 'before_installation')
    after = runtime.runtime_namespace_state(closure, 'after_installation')
    assert runtime.replay_runtime_path(before, PRIMARY).terminal == '/usr/bin/python3.14'
    assert runtime.replay_runtime_path(after, PRIMARY).terminal == '/usr/bin/python3.14'
    assert runtime.replay_runtime_path(after, '/usr/bin/python3').terminal == tree.image_executable
    audit = tree.audit(closure, tree.retained_members(closure))
    pre_native = audit['namespace_states']['before_installation']['native_audit']
    post_native = audit['namespace_states']['after_installation']['native_audit']
    assert audit['native_audit'] == pre_native
    assert post_native['symlinks'] == pre_native['symlinks'] + 1
    assert post_native['symlinks_sha256'] != pre_native['symlinks_sha256']


def test_existing_image_reader_retains_both_state_identities(required_graph_tree):
    tree = required_graph_tree
    closure = tree.complete()
    assert closure['reader_installation'] is None
    assert set(closure['namespace_states']) == {'before_installation', 'after_installation'}
    for state in closure['namespace_states']:
        view = runtime.runtime_namespace_state(closure, state)
        assert runtime.replay_runtime_path(view, PRIMARY).terminal == HOST_EXECUTABLE
        assert runtime.replay_runtime_path(view, '/usr/bin/python3').terminal == HOST_EXECUTABLE
        assert view['plan']['reader_identity'] == closure['plan']['reader_identity']
    audit = tree.audit(closure, tree.retained_members(closure))
    assert (audit['namespace_states']['before_installation']['native_audit']
            == audit['namespace_states']['after_installation']['native_audit'])
    assert os.readlink(tree.physical('image', '/usr/bin/python3')) == 'python3.12'


@pytest.mark.parametrize('provider', ['native', 'wrong_machine'])
def test_reader_creation_promotes_off_graph_alias_origin(required_graph_tree, provider):
    tree = graph_reader_transition_fixture(required_graph_tree)
    spelling = NATIVE + '/unused-reader.pc'
    tree.host_link(spelling, '/usr/bin/python3')
    tree.image_file(tree.image_executable, graph_fixture_elf(interpreter=LOADER,
        needed=('libreader.so',), runpath=('$ORIGIN/private',), marker=b'image-reader-origin'))
    tree.image_file('/usr/local/bin/private/libreader.so', graph_fixture_elf(marker=b'physical-origin'))
    tree.host_file(NATIVE + '/private/libreader.so', graph_fixture_elf(marker=b'native-alias-origin'))
    tree.image_file('/usr/bin/private/libreader.so', graph_fixture_elf(
        machine=62 if provider == 'wrong_machine' else 183, marker=b'created-reader-origin'))
    closure = tree.complete()
    before = runtime.runtime_namespace_state(closure, 'before_installation')
    after = runtime.runtime_namespace_state(closure, 'after_installation')
    assert graph_outcome(before, spelling)['status'] == 'witnessed_absent'
    assert graph_outcome(after, spelling)['terminal'] == tree.image_executable
    assert tree.entry(closure, '/usr/bin/private/libreader.so')['kind'] == 'file'
    pre_native = runtime.audit_native_closure(before, lambda path: tree.bytes_path(closure, path))
    retained = tree.retained_members(closure)
    if provider == 'wrong_machine':
        with pytest.raises(ValueError, match='unsupported architecture'):
            tree.audit(closure, retained)
        return
    audit = tree.audit(closure, retained)
    assert audit['namespace_states']['before_installation']['native_audit'] == pre_native
    assert (audit['namespace_states']['after_installation']['native_audit']['search_directories_sha256']
            != pre_native['search_directories_sha256'])


def test_reader_creation_rederives_sibling_negative_parent_names(required_graph_tree):
    tree = graph_reader_transition_fixture(required_graph_tree)
    spelling = NATIVE + '/unrelated.pc'
    tree.host_link(spelling, '/usr/bin/unrelated-absent')
    closure = tree.complete()
    before = runtime.runtime_namespace_state(closure, 'before_installation')
    after = runtime.runtime_namespace_state(closure, 'after_installation')
    pre_negative = graph_outcome(before, spelling)
    post_negative = graph_outcome(after, spelling)
    assert pre_negative['status'] == post_negative['status'] == 'witnessed_absent'
    assert pre_negative['witness'] != post_negative['witness']
    pre_names = before['directory_witnesses'][pre_negative['witness']]['names']
    post_names = after['directory_witnesses'][post_negative['witness']]['names']
    assert 'python3' not in pre_names
    assert post_names == sorted([*pre_names, 'python3'])
    creation_key = closure['reader_installation']['absent_before']['witness']
    assert creation_key not in after['directory_witnesses']
    tree.mount_host_roots()
    observed_before = runtime.observe_reader_namespace(closure, 'before_installation', image_root=tree.image_root)
    assert observed_before['names'] == pre_names
    tree.image_link('/usr/bin/python3', closure['reader_installation']['target'])
    observed_after = runtime.observe_reader_namespace(closure, 'after_installation', image_root=tree.image_root)
    assert observed_after['names'] == post_names
    runtime.verify_runtime_negatives(after, image_root=tree.image_root)
    with pytest.raises(ValueError, match='absen|negative|appeared|directory changed'):
        runtime.verify_runtime_negatives(before, image_root=tree.image_root)


def test_retained_state_auditor_rejects_coherent_stale_reader_negative(required_graph_tree):
    tree = graph_reader_transition_fixture(required_graph_tree)
    closure = tree.complete()
    retained = tree.retained_members(closure)
    assert tree.audit(closure, retained)
    forged = copy.deepcopy(closure)
    before = runtime.runtime_namespace_state(forged, 'before_installation')
    after = runtime.runtime_namespace_state(forged, 'after_installation')
    absence = forged['reader_installation']['absent_before']
    stale_witnesses = dict(after['directory_witnesses'])
    stale_witnesses[absence['witness']] = before['directory_witnesses'][absence['witness']]
    after_record = forged['namespace_states']['after_installation']
    after_record['link_outcomes']['/usr/bin/python3'] = copy.deepcopy(absence)
    after_record['directory_witnesses_sha256'] = runtime.digest(runtime.canonical(stale_witnesses))
    assert '/usr/bin/python3' in after_record['required_spellings']
    graph_sync_namespace(tree, forged)
    tree.checkpoint_agreement(forged)
    with pytest.raises(ValueError, match='namespace|state|reader|stale'):
        tree.audit(forged, retained)


@pytest.mark.parametrize('source', ['image_dangling', 'host_dangling', 'missing_parent'])
def test_reader_creation_refuses_unauthorized_absence_authority(tmp_path, source):
    executable = '/opt/primary/python3.14' if source == 'missing_parent' else HOST_EXECUTABLE
    tree = graph_reader_transition_fixture(RequiredGraphFilesystem(
        tmp_path / source, host_executable=executable))
    if source == 'image_dangling':
        tree.directory('image', '/missing-image-reader')
        tree.image_link('/usr/bin/python3', '/missing-image-reader/python3')
    elif source == 'host_dangling':
        tree.directory('host', '/host-reader-target')
        tree.host_paths.append('/host-reader-target')
        tree.image_link('/usr/bin/python3', '/host-reader-target/python3')
    else:
        shutil.rmtree(tree.physical('image', '/usr/bin'))
    with pytest.raises(ValueError, match='reader|alias.*absen|absen.*reader|required runtime'):
        tree.complete()


def graph_observed_reader_transition(tree, closure):
    tree.mount_host_roots()
    before = runtime.observe_reader_namespace(closure, 'before_installation', image_root=tree.image_root)
    path = tree.image_link('/usr/bin/python3', closure['reader_installation']['target'])
    after = runtime.observe_reader_namespace(closure, 'after_installation', image_root=tree.image_root)
    info = path.lstat()
    custody = dict(path='/usr/bin/python3', existed_before=False, created_symlink=True,
        target=os.readlink(path), uid=info.st_uid, gid=info.st_gid, mode=info.st_mode & 0o7777)
    return before, after, custody


def graph_retained_root_custody_claim(before, after, custody):
    """Pure replay inputs assert root custody; real fixture owners are logged."""
    print('reader-transition actual fixture custody:', dict(
        before_parent_uid=before['parent_uid'], after_parent_uid=after['parent_uid'],
        before_parent_mode=oct(before['parent_mode']), after_parent_mode=oct(after['parent_mode']),
        reader_uid=after['uid'], reader_gid=after['gid']))
    assert before['parent_mode'] == after['parent_mode'] == 0o755
    claimed_before, claimed_after, claimed_custody = map(copy.deepcopy, (before, after, custody))
    for observation in (claimed_before, claimed_after):
        observation['parent_uid'] = observation['parent_gid'] = 0
        if observation['entry'] is not None:
            observation['uid'] = observation['gid'] = 0
    claimed_custody['uid'] = claimed_custody['gid'] = 0
    return claimed_before, claimed_after, claimed_custody


@pytest.mark.parametrize('change', ['target', 'extra_name', 'removed_name'])
def test_reader_transition_replay_rejects_actual_link_or_name_delta(required_graph_tree, change):
    tree = graph_reader_transition_fixture(required_graph_tree)
    tree.image_file('/usr/bin/preserved-name', b'existing complete parent name')
    closure = tree.complete()
    before, after, custody = graph_observed_reader_transition(tree, closure)
    baseline = graph_retained_root_custody_claim(before, after, custody)
    runtime.verify_reader_namespace_transition(closure, *baseline)
    if change == 'target':
        path = tree.physical('image', '/usr/bin/python3')
        path.unlink()
        tree.image_link('/usr/bin/python3', tree.host_executable)
        custody['target'] = os.readlink(path)
        assert custody['target'] != closure['reader_installation']['target']
    elif change == 'extra_name':
        tree.image_file('/usr/bin/unplanned', b'not part of the single-reader insertion')
    else:
        tree.physical('image', '/usr/bin/preserved-name').unlink()
    altered = runtime.observe_reader_namespace(closure, 'after_installation', image_root=tree.image_root)
    claimed = graph_retained_root_custody_claim(before, altered, custody)
    with pytest.raises(ValueError, match='reader|target|names|delta|transition'):
        runtime.verify_reader_namespace_transition(closure, *claimed)


def test_reader_transition_replay_rejects_unstable_parent_claim(required_graph_tree):
    tree = graph_reader_transition_fixture(required_graph_tree)
    closure = tree.complete()
    physical = graph_observed_reader_transition(tree, closure)
    before, after, custody = graph_retained_root_custody_claim(*physical)
    runtime.verify_reader_namespace_transition(closure, before, after, custody)
    changed = copy.deepcopy(after)
    changed['after'][1] += 1
    assert changed['before'] != changed['after']
    with pytest.raises(ValueError, match='stable|changed|parent|observation'):
        runtime.verify_reader_namespace_transition(closure, before, changed, custody)


def test_reader_transition_rejects_actual_nonroot_link_custody(required_graph_tree):
    tree = graph_reader_transition_fixture(required_graph_tree)
    closure = tree.complete()
    before, after, custody = graph_observed_reader_transition(tree, closure)
    path = tree.physical('image', '/usr/bin/python3')
    if os.geteuid() == 0:
        os.chown(path, 1, 1, follow_symlinks=False)
        after = runtime.observe_reader_namespace(closure, 'after_installation', image_root=tree.image_root)
        info = path.lstat()
        custody.update(uid=info.st_uid, gid=info.st_gid)
    assert after['uid'] != 0 or after['gid'] != 0
    assert custody['uid'] == after['uid'] and custody['gid'] == after['gid']
    claimed_before = copy.deepcopy(before)
    claimed_after = copy.deepcopy(after)
    for observation in (claimed_before, claimed_after):
        observation['parent_uid'] = observation['parent_gid'] = 0
    with pytest.raises(ValueError, match='custody|root|uid|gid|owner'):
        runtime.verify_reader_namespace_transition(closure, claimed_before, claimed_after, custody)


@pytest.mark.parametrize('tamper', ['state', 'after_maps', 'target', 'custody'])
def test_retained_capture_auditor_checks_reader_transition_records(required_graph_tree, tamper):
    tree = graph_reader_transition_fixture(required_graph_tree)
    closure = tree.complete()
    assert tree.audit(closure, tree.retained_members(closure))
    physical = graph_observed_reader_transition(tree, closure)
    before_observed, after_observed, custody = graph_retained_root_custody_claim(*physical)
    before = runtime.runtime_namespace_state(closure, 'before_installation')
    after = runtime.runtime_namespace_state(closure, 'after_installation')
    verification = dict(runtime.runtime_closure_verification(before),
        namespace_states=dict(before_installation=before_observed, after_installation=after_observed),
        after_verification=runtime.runtime_closure_verification(after))
    auditor = runpy.run_path(str(Path(__file__).resolve().parents[1]
        / 'tools/maintenance/verify_proof_runtime_capture.py'))
    verify = auditor['verify_capture_reader_states']
    verify(closure, verification, custody, runtime.__dict__)
    forged, forged_custody = copy.deepcopy(verification), copy.deepcopy(custody)
    if tamper == 'state':
        forged['namespace_states']['after_installation']['state'] = 'before_installation'
    elif tamper == 'after_maps':
        maps = forged['after_verification']['member_maps']
        maps[next(iter(maps))]['members'] += 1
        forged['after_verification']['member_maps_sha256'] = runtime.digest(runtime.canonical(maps))
    elif tamper == 'target':
        forged['namespace_states']['after_installation']['entry']['target'] = HOST_EXECUTABLE
        forged_custody['target'] = HOST_EXECUTABLE
    else:
        forged['namespace_states']['after_installation']['uid'] = 1
        forged_custody['uid'] = 1
    with pytest.raises(ValueError, match='capture mounted|reader|namespace|post-installation|custody'):
        verify(closure, forged, forged_custody, runtime.__dict__)


@pytest.mark.parametrize('reader', ['absent', 'existing'])
def test_profile_loader_accepts_and_binds_reader_namespace_states(required_graph_tree, reader):
    tree = required_graph_tree
    if reader == 'absent':
        tree = graph_reader_transition_fixture(tree)
    closure = tree.complete()
    closure['audit'] = tree.audit(closure, tree.retained_members(closure))
    closure['sha256'] = runtime.digest(runtime.canonical(closure))
    profile = dict(schema=runtime.PROFILE_SCHEMA, image='sha256:' + '0' * 64,
        primary={name: copy.deepcopy(tree.host_checkpoint['observation'][name])
                 for name in ('version', 'executable', 'elf_machine')},
        installed_reader=dict(copy.deepcopy(closure['plan']['reader_identity']),
            uid=65534, gid=65534, link_target=closure['plan']['reader_target']),
        closure=closure)
    root = tree.host_root.parent
    path = root / 'sealed-reader-profile.json'
    path.write_bytes(runtime.canonical(profile))
    loaded = runtime.load_profile(path, root)
    assert set(loaded.closure['namespace_states']) == {'before_installation', 'after_installation'}
    assert loaded.installed_reader == profile['installed_reader']
    assert (loaded.closure['reader_installation'] is None) == (reader == 'existing')
    forged = copy.deepcopy(profile)
    forged['installed_reader']['link_target'] = '/not-the-derived-reader'
    path.write_bytes(runtime.canonical(forged))
    with pytest.raises(ValueError, match='reader.*derived|reader.*completion identity'):
        runtime.load_profile(path, root)


def test_reader_absence_parent_names_include_host_file_mount_destination(tmp_path):
    tree = graph_reader_transition_fixture(RequiredGraphFilesystem(
        tmp_path / 'reader-parent-host-mount', host_executable='/usr/bin/python3.14'))
    assert not tree.physical('image', '/usr/bin/python3.14').exists()
    closure = tree.complete()
    installation = closure['reader_installation']
    raw_witness = closure['directory_witnesses'][installation['absent_before']['witness']]
    assert raw_witness['parent'] == '/usr/bin' and raw_witness['authority'] == 'image'
    assert 'python3' not in raw_witness['names']
    assert 'python3.14' not in raw_witness['names']
    before = runtime.runtime_namespace_state(closure, 'before_installation')
    after = runtime.runtime_namespace_state(closure, 'after_installation')
    tree.mount_host_roots()
    mounted_primary = tree.physical('image', '/usr/bin/python3.14')
    assert mounted_primary.is_file()
    assert runtime.digest(mounted_primary.read_bytes()) == tree.entry(closure, '/usr/bin/python3.14')['sha256']
    runtime.verify_runtime_negatives(before, image_root=tree.image_root)
    observed_before = runtime.observe_reader_namespace(closure, 'before_installation', image_root=tree.image_root)
    assert observed_before['names'] == sorted([*raw_witness['names'], 'python3.14'])
    tree.image_link('/usr/bin/python3', installation['target'])
    runtime.verify_runtime_negatives(after, image_root=tree.image_root)
    observed_after = runtime.observe_reader_namespace(closure, 'after_installation', image_root=tree.image_root)
    assert observed_after['names'] == sorted([*observed_before['names'], 'python3'])
    custody = dict(path='/usr/bin/python3', existed_before=False, created_symlink=True,
        target=observed_after['entry']['target'], uid=observed_after['uid'], gid=observed_after['gid'],
        mode=observed_after['entry']['mode'])
    claimed_before, claimed_after, claimed_custody = graph_retained_root_custody_claim(
        observed_before, observed_after, custody)
    runtime.verify_reader_namespace_transition(closure, claimed_before, claimed_after, claimed_custody)
    verification = dict(runtime.runtime_closure_verification(before),
        namespace_states=dict(before_installation=claimed_before, after_installation=claimed_after),
        after_verification=runtime.runtime_closure_verification(after))
    auditor = runpy.run_path(str(Path(__file__).resolve().parents[1]
        / 'tools/maintenance/verify_proof_runtime_capture.py'))
    auditor['verify_capture_reader_states'](closure, verification, claimed_custody, runtime.__dict__)


@pytest.mark.parametrize('missing_count', [4, 32])
def test_runtime_replay_indexes_inventory_once_per_lookup(required_graph_tree, monkeypatch, missing_count):
    tree = required_graph_tree
    for number in range(missing_count):
        tree.host_link(NATIVE + '/unused-' + str(number), '/etc/alternatives/unused-' + str(number))
    tree.directory('image', '/etc/alternatives')
    for number in range(48):
        tree.image_file('/etc/alternatives/present-' + str(number), b'not an alias target')
    closure = tree.complete()
    calls = []
    original = runtime.runtime_namespace_entries
    def observe_flattening(facts):
        calls.append(sum(len(root['entries']) for root in facts['roots']))
        return original(facts)
    monkeypatch.setattr(runtime, 'runtime_namespace_entries', observe_flattening)
    lookup = runtime.runtime_replay_lookup(closure)
    for number in range(missing_count):
        outcome = runtime.walk_runtime_path(NATIVE + '/unused-' + str(number), lookup)
        assert isinstance(outcome, runtime.RuntimeWitnessedAbsent)
        assert outcome.missing == '/etc/alternatives/unused-' + str(number)
    assert len(calls) <= 3, 'one namespace index per lookup, independent of witness count'


CUSTOMIZATION_TARGET = '/etc/python3.14/sitecustomize.py'


def graph_customization_tree(tmp_path, *, image_copy=False, missing=False):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_link('/h/stdlib/sitecustomize.py', CUSTOMIZATION_TARGET)
    tree.directory('host', '/etc/python3.14')
    if not missing:
        source = Path('/etc/hostname')
        assert source.is_file() and source.stat().st_uid == 0
        tree.external_host_files[CUSTOMIZATION_TARGET] = source
    if image_copy:
        tree.image_file(CUSTOMIZATION_TARGET, b'IMAGE_CUSTOMIZATION = True\n')
    return tree


@pytest.mark.parametrize('image_copy', [False, True], ids=['x86-missing-image-parent', 'arm-image-copy'])
def test_customization_host_file_root_resolves_and_shadows_image(tmp_path, monkeypatch, image_copy):
    tree = graph_customization_tree(tmp_path, image_copy=image_copy)
    closure = tree.complete()
    assert closure['completion']['host_customization_roots'] == tree.customization_roots
    assert graph_outcome(closure, '/h/stdlib/sitecustomize.py')['terminal'] == CUSTOMIZATION_TARGET
    roots = [root for root in closure['roots'] if root['path'] == CUSTOMIZATION_TARGET]
    assert len(roots) == 1 and roots[0]['origin'] == 'host'
    assert roots[0]['entries']['.']['sha256'] == runtime.digest(Path('/etc/hostname').read_bytes())
    retained = tree.retained_members(closure)
    assert tree.audit(closure, retained)
    tree.mount_host_roots()
    original_inventory, original_negatives = runtime.inventory, runtime.verify_runtime_negatives
    monkeypatch.setattr(runtime, 'inventory',
        lambda path, **options: original_inventory(tree.physical('image', str(path)), **options))
    monkeypatch.setattr(runtime, 'verify_runtime_negatives',
        lambda view, **options: original_negatives(view, image_root=tree.image_root, **options))
    runtime.verify_closure(SimpleNamespace(closure=closure))
    assert tree.physical('image', CUSTOMIZATION_TARGET).read_bytes() == Path('/etc/hostname').read_bytes()
    tree.physical('image', CUSTOMIZATION_TARGET).write_bytes(b'tampered bound customization')
    with pytest.raises(ValueError, match='inventory|differ|changed'):
        runtime.verify_closure(SimpleNamespace(closure=closure))


def test_customization_missing_host_target_remains_strict(tmp_path):
    tree = graph_customization_tree(tmp_path, missing=True)
    assert not tree.customization_roots and CUSTOMIZATION_TARGET not in tree.host_paths
    with pytest.raises(ValueError, match='required runtime alias is absent outside the permitted native graph'):
        tree.complete()


def test_customization_unjustified_extra_host_file_root_refuses(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_file('/etc/unjustified.py', b'UNJUSTIFIED = True\n')
    tree.host_paths.append('/etc/unjustified.py')
    closure = tree.complete()
    retained = tree.retained_members(closure)
    tree.checkpoint_agreement(closure)
    with pytest.raises(ValueError, match='host root.*selection|unjustified host|host.*root.*deriv'):
        tree.audit(closure, retained)


@pytest.mark.parametrize('tamper', ['derivation', 'custody', 'component'])
def test_customization_semantic_replay_rejects_coherent_tampering(tmp_path, tamper):
    tree = graph_customization_tree(tmp_path)
    closure = tree.complete()
    retained = tree.retained_members(closure)
    assert tree.audit(closure, retained)
    forged = copy.deepcopy(closure)
    completion = forged['completion']
    if tamper == 'derivation':
        completion['host_customization_roots'][0]['target'] = '/etc/unjustified.py'
    else:
        for probe in completion['host_customization_probes']:
            if tamper == 'custody' and probe['path'] == CUSTOMIZATION_TARGET:
                probe['uid'] = 1234
            elif tamper == 'component' and probe['path'] == '/h/stdlib/sitecustomize.py':
                probe['entry']['target'] = '/etc/unjustified.py'
    for name in ('customization_roots', 'customization_probes'):
        tree.host_checkpoint[name] = copy.deepcopy(completion['host_' + name])
    raw = runtime.canonical(dict(phase='host_inventory', units=1, value=tree.host_checkpoint))
    tree.host_descriptor = dict(sha256=runtime.digest(raw), bytes=len(raw))
    completion['host_checkpoint'] = copy.deepcopy(tree.host_descriptor)
    graph_sync_namespace(tree, forged)
    tree.checkpoint_agreement(forged)
    with pytest.raises(ValueError, match='customization.*(selection|inventory)|customization roots'):
        tree.audit(forged, retained)


def graph_ancestor_search_tree(tmp_path, *, present=False):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_file('/h/stdlib/ancestor-extension.so', graph_fixture_elf(
        needed=('libancestor.so',), runpath=('/usr/lib',), marker=b'ancestor-seed'))
    tree.host_file(NATIVE + '/libancestor.so', graph_fixture_elf(marker=b'fallback-provider'))
    tree.image_file('/usr/lib/unrelated-search-child', b'not a recursive inventory')
    if present:
        tree.image_file('/usr/lib/libancestor.so', graph_fixture_elf(marker=b'direct-provider'))
    return tree


def test_runpath_ancestor_absent_child_has_search_witness(tmp_path):
    tree = graph_ancestor_search_tree(tmp_path)
    closure = tree.complete()
    key, witness = next((key, value) for key,value in closure['directory_witnesses'].items()
                        if value['parent'] == '/usr/lib' and value['name'] == 'libancestor.so')
    assert witness['authority'] == 'image' and witness['operation'] == 'lstat_at_nofollow'
    assert witness['errno'] == 'ENOENT'
    assert witness['before'] == witness['after']
    assert witness['names'] == sorted(child.name for child in tree.physical('image', '/usr/lib').iterdir())
    assert '/usr/lib' not in {root['path'] for root in closure['roots']}
    assert any(charge.get('witness') == key and charge['members'] == 1 + len(witness['names'])
               for charge in closure['completion']['charges'])
    assert tree.audit(closure, tree.retained_members(closure))
    tree.mount_host_roots()
    runtime.verify_runtime_negatives(closure, image_root=tree.image_root)
    tree.image_file('/usr/lib/libancestor.so', graph_fixture_elf(marker=b'appeared'))
    with pytest.raises(ValueError, match='absent target appeared'):
        runtime.verify_runtime_negatives(closure, image_root=tree.image_root)


def test_runpath_ancestor_present_child_is_required_narrow_candidate(tmp_path):
    tree = graph_ancestor_search_tree(tmp_path, present=True)
    closure = tree.complete()
    path = '/usr/lib/libancestor.so'
    assert graph_outcome(closure, path)['terminal'] == path
    assert tree.entry(closure, path) == runtime.inventory(tree.physical('image', path))['.']
    root, = [root for root in closure['roots'] if root['path'] == path]
    assert root['origin'] == 'image' and set(root['entries']) == {'.'}
    assert '/usr/lib' not in {root['path'] for root in closure['roots']}
    assert not any(value['parent'] == '/usr/lib' and value['name'] == 'libancestor.so'
                   for value in closure['directory_witnesses'].values())
    assert tree.audit(closure, tree.retained_members(closure))


def test_runpath_ancestor_listing_mutation_refuses(tmp_path, monkeypatch):
    tree = graph_ancestor_search_tree(tmp_path)
    inputs = tree.inputs()
    original = os.scandir
    changed = []
    @contextmanager
    def changing_listing(path):
        with original(path) as children:
            yield children
        if (isinstance(path, int)
                and Path('/proc/self/fd/' + str(path)).resolve() == tree.physical('image', '/usr/lib')):
            tree.image_file('/usr/lib/late-child', b'changed during enumeration')
            changed.append(True)
    monkeypatch.setattr(os, 'scandir', changing_listing)
    with pytest.raises(ValueError, match='image absence parent changed during observation'):
        tree.complete(inputs)
    assert changed, 'the actual ancestor-directory listing was not observed'


def test_runpath_ancestor_fabricated_listing_refuses_semantic_replay(tmp_path):
    tree = graph_ancestor_search_tree(tmp_path)
    closure = tree.complete()
    retained = tree.retained_members(closure)
    assert tree.audit(closure, retained)
    forged = copy.deepcopy(closure)
    old_key, witness = next((key, copy.deepcopy(value))
        for key,value in forged['directory_witnesses'].items()
        if value['parent'] == '/usr/lib' and value['name'] == 'libancestor.so')
    witness['names'].remove(Path(NATIVE).name)
    new_key = graph_replace_witness(forged, old_key, witness)
    for charge in forged['completion']['charges']:
        if charge.get('witness') == new_key:
            charge['members'] -= 1
            forged['consumed_budget']['members'] -= 1
    graph_sync_namespace(tree, forged)
    tree.checkpoint_agreement(forged)
    with pytest.raises(ValueError, match='listing omits a retained child'):
        tree.audit(forged, retained)


def test_reachable_empty_runpath_still_refuses(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_file('/h/stdlib/empty-runpath-extension.so',
                   graph_fixture_elf(runpath=('',), marker=b'empty-runpath-seed'))
    with pytest.raises(ValueError, match='unsupported library search token'):
        tree.complete()


def test_reader_alias_adds_image_interpreter_ancestor_search(tmp_path):
    tree = graph_reader_transition_fixture(RequiredGraphFilesystem(
        tmp_path, host_executable='/usr/bin/python3.14'))
    tree.image_file(tree.image_executable, graph_fixture_elf(interpreter=LOADER,
        needed=('libreader.so',), runpath=('$ORIGIN/../lib',), marker=b'image-reader-rpath'))
    tree.image_file('/usr/local/lib/libreader.so', graph_fixture_elf(marker=b'image-provider'))
    tree.image_paths.append('/usr/local/lib')
    tree.cache_listing = '1 libs found in cache fixture:\n libreader.so (libc6,AArch64) => /usr/local/lib/libreader.so\n'
    closure = tree.complete()
    assert runtime.replay_runtime_path(runtime.runtime_namespace_state(
        closure,'after_installation'),'/usr/bin/python3').terminal == tree.image_executable
    assert any(value['parent'] == '/usr/lib' and value['name'] == 'libreader.so'
               for value in closure['directory_witnesses'].values())
    assert not any(root['path'] == '/usr/lib' for root in closure['roots'])


def test_ancestor_search_preserves_inherited_rpath_candidates(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_file('/h/stdlib/inherited-rpath-seed.so', graph_fixture_elf(
        needed=('libparent.so',), rpath=('/usr/lib',), marker=b'rpath-parent-seed'))
    tree.host_file(NATIVE + '/libparent.so', graph_fixture_elf(
        needed=('libinherited.so',), marker=b'parent-provider'))
    tree.host_file(NATIVE + '/libinherited.so', graph_fixture_elf(marker=b'good-fallback'))
    tree.image_file('/usr/lib/libinherited.so', graph_fixture_elf(machine=62, marker=b'bad-inherited-provider'))
    tree.cache_listing = ('1 libs found in cache fixture:\n'
        ' libparent.so (libc6,AArch64) => ' + NATIVE + '/libparent.so\n')
    with pytest.raises(ValueError, match='unsupported architecture'):
        tree.audit(tree.complete())


def graph_default_search_tree(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.diagnostic_raw = ('version.version=\"2.39\"\npath.system_dirs[0]="/usr/lib/"\n'
        'path.system_dirs[1]="' + NATIVE + '/"\nuname.release="6.8.0"\n')
    tree.host_file('/h/stdlib/tkinter.so', graph_fixture_elf(needed=('libtk.so',)))
    tree.host_file(NATIVE + '/libtk.so', graph_fixture_elf(needed=('libXft.so',)))
    tree.host_file(NATIVE + '/libXft.so', graph_fixture_elf(marker=b'Xft-provider'))
    return tree


def test_loader_alias_origin_cache_hit_resolves(tmp_path):
    tree = graph_reader_transition_fixture(RequiredGraphFilesystem(
        tmp_path, host_executable='/usr/bin/python3.14'))
    tree.image_file(tree.image_executable, graph_fixture_elf(interpreter=LOADER,
        needed=('libpython.so',), runpath=('$ORIGIN/../lib',)))
    tree.image_file('/usr/local/lib/libpython.so', graph_fixture_elf(marker=b'image-provider'))
    tree.image_paths.append('/usr/local/lib')
    tree.cache_listing = '1 libs found in cache fixture:\n libpython.so (libc6,AArch64) => /usr/local/lib/libpython.so\n'
    closure = tree.complete()
    assert tree.audit(closure, tree.retained_members(closure))
    assert any(value['parent'] == '/usr/lib' and value['name'] == 'libpython.so'
               for value in closure['directory_witnesses'].values())


def test_loader_every_origin_requires_a_provider(tmp_path):
    tree = graph_reader_transition_fixture(RequiredGraphFilesystem(
        tmp_path, host_executable='/usr/bin/python3.14'))
    tree.image_file(tree.image_executable, graph_fixture_elf(interpreter=LOADER,
        needed=('libnowhere.so',), runpath=('$ORIGIN/../lib',)))
    tree.image_file('/usr/local/lib/libprivate.so', graph_fixture_elf())
    tree.image_paths.append('/usr/local/lib')
    with pytest.raises(ValueError, match='unresolved runtime ELF dependency'):
        tree.complete()


def test_loader_defaults_complete_transitive_tk_xft_candidates(tmp_path):
    tree = graph_default_search_tree(tmp_path)
    closure = tree.complete()
    misses = {str(Path(value['parent']) / value['name'])
              for value in closure['directory_witnesses'].values()}
    assert {'/usr/lib/libtk.so', '/usr/lib/libXft.so'} <= misses
    assert graph_outcome(closure, NATIVE + '/libtk.so')['terminal'] == NATIVE + '/libtk.so'
    assert graph_outcome(closure, NATIVE + '/libXft.so')['terminal'] == NATIVE + '/libXft.so'
    assert tree.audit(closure, tree.retained_members(closure))


def test_loader_unwitnessed_visited_default_refuses_replay(tmp_path):
    tree = graph_default_search_tree(tmp_path)
    closure = tree.complete()
    retained = tree.retained_members(closure)
    assert tree.audit(closure, retained)
    forged = copy.deepcopy(closure)
    key = next(key for key,value in forged['directory_witnesses'].items()
               if value['parent'] == '/usr/lib' and value['name'] == 'libXft.so')
    del forged['directory_witnesses'][key]
    with pytest.raises(ValueError, match='missing candidate absence evidence|absence witness'):
        runtime.audit_native_closure(runtime.runtime_namespace_state(forged, 'before_installation'),
            lambda path: retained[tree.entry(forged, path)['sha256']])


@pytest.mark.parametrize('change', ['system_dirs', 'kernel'])
def test_loader_pre_post_mounted_diagnostics_must_agree(tmp_path, change):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.post_diagnostic_raw = tree.diagnostic_raw.replace(
        NATIVE + '/', '/usr/lib/' ) if change == 'system_dirs' else tree.diagnostic_raw.replace('6.8.0','6.8.1')
    closure = tree.complete()
    with pytest.raises(ValueError, match='pre-completion.*diagnostics|diagnostics.*changed'):
        tree.audit(closure)


def test_loader_missing_cache_row_refuses_despite_default_hit(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_file('/h/stdlib/cache-seed.so', graph_fixture_elf(needed=('libcached.so',)))
    tree.host_file(NATIVE + '/libcached.so', graph_fixture_elf())
    tree.cache_listing = '1 libs found in cache fixture:\n libcached.so (libc6,AArch64) => /usr/lib/libcached.so\n'
    with pytest.raises(ValueError, match='required runtime alias is absent|cache candidate'):
        tree.complete()


def test_loader_resolution_waits_for_inherited_context_fixed_point(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_file('/h/stdlib/a-seed.so', graph_fixture_elf(needed=('liblate.so',)))
    tree.host_file('/h/stdlib/b-seed.so', graph_fixture_elf(needed=('libparent.so',)))
    tree.host_file('/h/stdlib/private/liblate.so', graph_fixture_elf(marker=b'late-provider'))
    tree.host_file(NATIVE + '/libparent.so', graph_fixture_elf(rpath=('/h/stdlib/private',)))
    tree.cache_listing = '1 libs found in cache fixture:\n libparent.so (libc6,AArch64) => ' + NATIVE + '/libparent.so\n'
    closure = tree.complete()
    assert tree.audit(closure, tree.retained_members(closure))
    assert graph_outcome(closure, '/h/stdlib/private/liblate.so')['terminal'] == '/h/stdlib/private/liblate.so'


@pytest.mark.parametrize('context', ['runpath', 'default'])
@pytest.mark.parametrize('base_present', [False, True])
def test_ancestor_hwcaps_candidates_are_sealed(tmp_path, context, base_present):
    tree = graph_ancestor_search_tree(tmp_path, present=base_present)
    optimized = '/usr/lib/glibc-hwcaps/x86-64-v3/libancestor.so'
    tree.image_file(optimized, graph_fixture_elf(marker=b'optimized-provider'))
    if context == 'default':
        tree.host_file('/h/stdlib/ancestor-extension.so', graph_fixture_elf(needed=('libancestor.so',)))
        tree.diagnostic_raw = 'version.version=\"2.39\"\npath.system_dirs[0]="/usr/lib/"\npath.system_dirs[1]="' + NATIVE + '/"\nuname.release="6.8.0"\n'
    tree.host_file('/h/stdlib/hwcap-miss.so', graph_fixture_elf(needed=('libother.so',), runpath=('/usr/lib',)))
    tree.host_file(NATIVE + '/libother.so', graph_fixture_elf())
    closure = tree.complete()
    assert tree.entry(closure, optimized)['sha256'] == runtime.digest(tree.physical('image', optimized).read_bytes())
    assert tree.audit(closure, tree.retained_members(closure))

    observations = closure['completion']['search_directory_observations']
    assert observations['/usr/lib/glibc-hwcaps']['names'] == ['x86-64-v3']
    sub = '/usr/lib/glibc-hwcaps/x86-64-v3'
    assert observations[sub]['names'] == ['libancestor.so']
    assert any(value['parent'] == sub and value['name'] == 'libother.so'
               for value in closure['directory_witnesses'].values())
    assert all(any(charge['kind'] == 'listing' and charge['path'] == path
                   and charge['members'] == 1 + len(observed['names'])
                   for charge in closure['completion']['charges']) for path,observed in observations.items())
    tree.mount_host_roots()
    runtime.verify_runtime_negatives(closure, image_root=tree.image_root)
    tree.image_file(sub + '/unexpected-name', b'changed listing')
    with pytest.raises(ValueError, match='listing changed'):
        runtime.verify_runtime_negatives(closure, image_root=tree.image_root)


def test_unrelated_rpath_does_not_suppress_default_evidence(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_file('/h/stdlib/a.so', graph_fixture_elf(needed=('libfoo.so',)))
    tree.host_file('/h/stdlib/b.so', graph_fixture_elf(rpath=('/h/stdlib/private',)))
    tree.host_file('/h/stdlib/private/libfoo.so', graph_fixture_elf(marker=b'unrelated-provider'))
    tree.image_file('/usr/lib/libfoo.so', graph_fixture_elf(marker=b'default-provider'))
    tree.diagnostic_raw = 'version.version=\"2.39\"\npath.system_dirs[0]="/usr/lib/"\npath.system_dirs[1]="' + NATIVE + '/"\nuname.release="6.8.0"\n'
    closure = tree.complete()
    assert tree.entry(closure, '/usr/lib/libfoo.so')['kind'] == 'file'
    assert any(value['parent'] == NATIVE and value['name'] == 'libfoo.so'
               for value in closure['directory_witnesses'].values())
    assert tree.audit(closure, tree.retained_members(closure))


def test_ineligible_cache_row_does_not_suppress_default_evidence(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_file('/h/stdlib/a.so', graph_fixture_elf(needed=('libfoo.so',)))
    cached = NATIVE + '/glibc-hwcaps/x86-64-v4/libfoo.so'
    tree.host_file(cached, graph_fixture_elf(marker=b'ineligible-cache-provider'))
    tree.image_file('/usr/lib/libfoo.so', graph_fixture_elf(marker=b'ordinary-default-provider'))
    tree.cache_listing = '1 libs found in cache fixture:\n libfoo.so (libc6,AArch64, hwcap: "x86-64-v4") => ' + cached + '\n'
    tree.diagnostic_raw = 'version.version=\"2.39\"\npath.system_dirs[0]="/usr/lib/"\npath.system_dirs[1]="' + NATIVE + '/"\nuname.release="6.8.0"\n'
    closure = tree.complete()
    assert tree.entry(closure, '/usr/lib/libfoo.so')['kind'] == 'file'
    assert graph_outcome(closure, cached)['terminal'] == cached
    assert tree.audit(closure, tree.retained_members(closure))


@pytest.mark.parametrize('token', ['$ORIGIN_SUFFIX', '$ORIGIN1', '${ORIGIN_SUFFIX}', '$LIB', '${PLATFORM}'])
def test_dynamic_token_identifier_prefix_is_refused(tmp_path, token):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_file('/h/stdlib/token.so', graph_fixture_elf(runpath=('/p/' + token,)))
    tree.image_file('/p/h/stdlib_SUFFIX/libfoo.so', graph_fixture_elf())
    tree.image_file('/p/h/stdlib1/libfoo.so', graph_fixture_elf())
    with pytest.raises(ValueError, match='unsupported library search token'):
        tree.complete()


@pytest.mark.parametrize('token', ['$ORIGIN/x', '${ORIGIN}-x'])
def test_dynamic_origin_token_valid_boundaries_are_preserved(tmp_path, token):
    tree = RequiredGraphFilesystem(tmp_path)
    path = '/h/stdlib/x' if token == '$ORIGIN/x' else '/h/stdlib-x'
    tree.image_file(path + '/libfoo.so', graph_fixture_elf())
    tree.host_file('/h/stdlib/token.so', graph_fixture_elf(needed=('libfoo.so',), runpath=(token,)))
    if path == '/h/stdlib/x':
        tree.host_file(path + '/libfoo.so', graph_fixture_elf())
    closure = tree.complete()
    assert tree.audit(closure, tree.retained_members(closure))


def test_absence_listings_cannot_remove_an_unretained_name(tmp_path):
    tree = graph_ancestor_search_tree(tmp_path)
    tree.host_file('/h/stdlib/another.so', graph_fixture_elf(needed=('libother.so',), runpath=('/usr/lib',)))
    tree.host_file(NATIVE + '/libother.so', graph_fixture_elf())
    closure = tree.complete()
    retained = tree.retained_members(closure)
    forged = copy.deepcopy(closure)
    old_key, witness = next((key,copy.deepcopy(value)) for key,value in forged['directory_witnesses'].items()
        if value['parent'] == '/usr/lib' and value['name'] == 'libancestor.so')
    witness['names'].remove('unrelated-search-child')
    new_key = graph_replace_witness(forged, old_key, witness)
    for charge in forged['completion']['charges']:
        if charge.get('witness') == new_key:
            charge['members'] -= 1
            forged['consumed_budget']['members'] -= 1
    graph_sync_namespace(tree, forged)
    tree.checkpoint_agreement(forged)
    with pytest.raises(ValueError, match='conflicting.*listing|listing.*differs|parent.*stamp'):
        tree.audit(forged, retained)


def test_unused_absolute_search_directory_is_lazy_in_both_owners(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_file('/h/stdlib/unused-runpath.so', graph_fixture_elf(runpath=('/missing',)))
    closure = tree.complete()
    assert not any(value['parent'] == '/missing' for value in closure['directory_witnesses'].values())
    assert tree.audit(closure, tree.retained_members(closure))


def test_legacy_glibc_configuration_is_refused(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    host,image,checkpoint,budget = tree.inputs()
    image['glibc'] = 'ldd (GNU libc) 2.36\n'
    with pytest.raises(ValueError, match='legacy.*hwcap|glibc.*2.37'):
        tree.complete((host,image,checkpoint,budget))


def test_absence_stamp_must_match_retained_parent_component(tmp_path):
    tree = graph_ancestor_search_tree(tmp_path)
    closure = tree.complete()
    retained = tree.retained_members(closure)
    forged = copy.deepcopy(closure)
    for old_key,value in list(forged['directory_witnesses'].items()):
        if value['parent'] != '/usr/lib':
            continue
        witness = copy.deepcopy(value)
        witness['before'][-1] += 1
        witness['after'][-1] += 1
        graph_replace_witness(forged, old_key, witness)
    graph_sync_namespace(tree, forged)
    tree.checkpoint_agreement(forged)
    with pytest.raises(ValueError, match='parent.*stamp|stamp.*directory|directory.*stamp'):
        tree.audit(forged, retained)


def test_hwcaps_positive_listing_mutation_refuses(tmp_path, monkeypatch):
    tree = graph_ancestor_search_tree(tmp_path)
    cap = '/usr/lib/glibc-hwcaps/x86-64-v3'
    tree.image_file(cap + '/libancestor.so', graph_fixture_elf())
    inputs = tree.inputs()
    original = os.scandir
    changed = []
    @contextmanager
    def changing_hwcaps_listing(path):
        with original(path) as children:
            yield children
        if isinstance(path, int) and Path('/proc/self/fd/' + str(path)).resolve() == tree.physical('image', cap):
            tree.image_file(cap + '/late-member', b'mutating')
            changed.append(True)
    monkeypatch.setattr(os, 'scandir', changing_hwcaps_listing)
    with pytest.raises(ValueError, match='listing.*changed|directory.*changed'):
        tree.complete(inputs)
    assert changed


def test_root_navigation_is_not_its_own_directory_child(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    tree.host_link(NATIVE + '/unused-root-leaf', '/off-graph-absent-leaf')
    closure = tree.complete()
    assert any(value['parent'] == '/' and value['name'] == 'off-graph-absent-leaf'
               for value in closure['directory_witnesses'].values())
    assert tree.audit(closure, tree.retained_members(closure))


@pytest.mark.parametrize('resources,accepted', [
    pytest.param(dict(cpu=1, mem_gb=4, gpu=0), True, id='four-gib'),
    pytest.param(dict(cpu=1, mem_gb=2, gpu=0), False, id='old-two-gib'),
    pytest.param(dict(cpu=1, mem_gb=3, gpu=0), False, id='three-gib'),
    pytest.param(dict(cpu=2, mem_gb=4, gpu=0), False, id='two-cpus'),
])
def test_capture_terminal_requires_exact_capture_demand(resources, accepted):
    auditor = runpy.run_path(str(Path(__file__).resolve().parents[1]
        / 'tools/maintenance/verify_proof_runtime_capture.py'))
    allocation = dict(preferred=list(range(5, 5 + resources['cpu'])), fallback=[])
    terminal = dict(cpu_allocation=allocation, resources=resources, priority=-10,
                    detail=dict(cpu_allocation=allocation),
                    finished_host='fixture-worker', claimed_host='fixture-worker')
    request = dict(params=dict(demand=resources,
                               execution_timeout_s=runtime.CAPTURE_BOUNDS['runtime_seconds']))
    producer = dict(worker_id='fixture-worker', evidence=dict(hostname='fixture-worker'))
    if accepted:
        assert auditor['capture_assigned_affinity'](
            request, terminal, producer, runtime.CAPTURE_BOUNDS, runtime.CAPTURE_DEMAND) == [5]
    else:
        with pytest.raises(ValueError, match='one-CPU demand'):
            auditor['capture_assigned_affinity'](
                request, terminal, producer, runtime.CAPTURE_BOUNDS, runtime.CAPTURE_DEMAND)


@pytest.mark.parametrize('role', ['reader', 'candidate'])
@pytest.mark.parametrize('stage', ['completion', 'before_installation', 'after_installation'])
def test_distinct_runtime_loader_refuses_modern_ldd_and_primary(tmp_path, monkeypatch, role, stage):
    tree = RequiredGraphFilesystem(tmp_path)
    legacy = '/opt/legacy/ld.so'
    tree.image_file(legacy, graph_fixture_elf(marker=b'legacy glibc 2.36 loader'))
    tree.image_paths.append(legacy)
    if role == 'reader':
        tree.physical('host', '/h/venv/bin/python3').unlink()
        tree.host_link('/h/venv/bin/python3', HOST_EXECUTABLE)
        tree.image_executable = '/i/reader'
        tree.image_file(tree.image_executable, graph_fixture_elf(interpreter=legacy, marker=b'reader'))
        tree.image_paths.append(tree.image_executable)
        tree.physical('image', '/usr/bin/python3').unlink()
        tree.image_link('/usr/bin/python3', tree.image_executable)
    else:
        tree.host_file('/h/stdlib/extension.so', graph_fixture_elf(interpreter=legacy))
    if stage == 'completion':
        with pytest.raises(ValueError, match='distinct runtime loader is not admitted'):
            tree.complete()
        return
    # Build retained evidence without the producer's PT edge check, then require
    # the independent byte-parsing replay to reject the actual ELF interpreter.
    original = capture.runtime_loader_edges
    def omit_producer_pt(metadata, *args, **kwargs):
        metadata = {path: dict(value, interpreters=[]) for path, value in metadata.items()}
        return original(metadata, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(capture, 'runtime_loader_edges', omit_producer_pt)
        closure = tree.complete()
    with pytest.raises(ValueError, match='distinct runtime loader is not admitted'):
        runtime.audit_native_closure(runtime.runtime_namespace_state(closure, stage),
            lambda path: tree.bytes_path(closure, path))


@pytest.mark.parametrize('version,accepted', [
    ('2.3.6', False), ('2.36.9000', False), (None, False),
    ('2.39', True), ('2.43', True), ('2.39junk', False), ('2.39.1', True),
])
@pytest.mark.parametrize('stage', ['completion', 'audit'])
def test_loader_own_complete_version_controls_admission(tmp_path, version, accepted, stage):
    tree = RequiredGraphFilesystem(tmp_path)
    raw = tree.diagnostic_raw.replace('version.version="2.39"\n',
        '' if version is None else 'version.version=' + json.dumps(version) + '\n')
    if stage == 'completion':
        tree.diagnostic_raw = raw
        operation = tree.complete
    else:
        closure = tree.complete()
        closure['completion']['pre_completion_diagnostics']['raw'] = raw
        closure['loader']['diagnostics']['raw'] = raw
        operation = lambda: runtime.audit_native_closure(closure, lambda path: tree.bytes_path(closure, path))
    if accepted:
        operation()
    else:
        with pytest.raises(ValueError, match='glibc|loader'):
            operation()


@pytest.mark.parametrize('release', [None, 'development'])
@pytest.mark.parametrize('stage', ['completion', 'audit'])
def test_loader_requires_its_own_release_build(tmp_path, release, stage):
    tree = RequiredGraphFilesystem(tmp_path)
    raw = tree.diagnostic_raw + ('' if release is None else 'version.release=' + json.dumps(release) + '\n')
    if stage == 'completion':
        tree.diagnostic_raw = raw
        operation = tree.complete
    else:
        closure = tree.complete()
        closure['completion']['pre_completion_diagnostics']['raw'] = raw
        closure['loader']['diagnostics']['raw'] = raw
        operation = lambda: runtime.audit_native_closure(closure, lambda path: tree.bytes_path(closure, path))
    with pytest.raises(ValueError, match='loader.*release'):
        operation()


def test_ldd_version_is_information_only(tmp_path):
    tree = RequiredGraphFilesystem(tmp_path)
    host, image, checkpoint, budget = tree.inputs()
    image['glibc'] = 'ldd (GNU libc) 2.36\n'
    closure = tree.complete((host, image, checkpoint, budget))
    tree.audit(closure)
