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
    def __init__(self, root):
        self.host_root = root / 'host'
        self.image_root = root / 'image'
        self.host_paths = ['/h/stdlib', NATIVE, '/h/venv/bin',
                           '/h/venv/pyvenv.cfg', HOST_EXECUTABLE]
        self.image_paths = ['/i/stdlib', NATIVE, HOST_EXECUTABLE,
                            '/etc/ld.so.cache', '/sbin/ldconfig']
        self.cache_listing = '0 libs found in cache fixture:\n'
        self.host_interpreter = LOADER
        self.image_interpreter = LOADER
        self.host_file('/h/stdlib/plain.py', b'VALUE = 1\n')
        self.host_file(NATIVE + '/ld-fixture.so', graph_fixture_elf(marker=b'host-loader'))
        self.host_file('/h/venv/pyvenv.cfg', b'home = /usr/bin\n')
        self.host_link(PRIMARY, 'python3')
        self.host_link('/h/venv/bin/python3', '/usr/bin/python3')
        self.host_link('/h/venv/bin/python3.12', 'python3')
        self.host_file(HOST_EXECUTABLE, graph_fixture_elf(interpreter=LOADER, marker=b'host'))
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
        executable_bytes = self.physical(origin, HOST_EXECUTABLE).read_bytes()
        observation = dict(version=[3, 12, 3], elf_machine=183,
            executable=dict(path=PRIMARY if origin == 'host' else HOST_EXECUTABLE,
                resolved_path=HOST_EXECUTABLE, bytes=len(executable_bytes),
                sha256=runtime.digest(executable_bytes)),
            kernel=dict(release='6.8.0', system='Linux', machine='aarch64'),
            loader_environment={}, at_secure=0, sys_path=[])
        result = dict(observation=observation, roots=roots,
            stdlib='/h/stdlib' if origin == 'host' else '/i/stdlib',
            providers={}, elf_roots=['/h/stdlib' if origin == 'host' else '/i/stdlib', HOST_EXECUTABLE],
            executable_binary=base64.b64encode(executable_bytes).decode(),
            multiarch='fixture-native', native_library_roots=[NATIVE],
            directory_observations=observations, elf_index=elf_index,
            inventory_budget={key: budget[key] for key in ('members', 'regular_bytes')},
            consumed_budget={key: budget[key] for key in ('members', 'regular_bytes')})
        if origin == 'image':
            cache = next(root['entries']['.'] for root in roots if root['path'] == '/etc/ld.so.cache')
            tool = next(root['entries']['.'] for root in roots if root['path'] == '/sbin/ldconfig')
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
        host_pt, = host['elf_index'][HOST_EXECUTABLE]['interpreters']
        closure['loader'] = dict(schema='prismabuild.proof_loader_search.v1',
            cache=image['loader_cache'], diagnostics=dict(
                raw='path.system_dirs[0]="' + NATIVE + '/"\nuname.release="6.8.0"\n',
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
