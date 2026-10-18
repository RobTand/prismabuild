"""Capture only the selected CPython runtime and an existing native image."""
import argparse
from contextlib import contextmanager
from dataclasses import asdict
import errno
import io
import base64
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import runpy
import shutil
import site
import stat
import subprocess
import sys
import sysconfig
import tarfile
import time

from proof_container_runtime import (
    CAPTURE_BOUNDS, CAPTURE_PHASES, CAPTURE_PUBLISHED_SOURCES, CLOSURE_SCHEMA, PROFILE_SCHEMA,
    canonical, digest, elf_requirements,
    inventory, loader_cache_entries, loader_default_directories, loader_kernel, merge_runtime_roots, native_provider_paths, observe_runtime,
    resolve_runtime_path, BROAD_ROOTS, CompletedRuntimeNamespace, ImageRuntimeAbsence,
    HostRuntimeAbsence, MountRuntimeAbsence, RuntimeLookupAbsent, RuntimeResolved, RuntimeWitnessedAbsent,
    audit_link_outcomes, audit_runtime_closure, read_runtime_elf, runtime_charge, runtime_completion_plan,
    runtime_outcome_record, runtime_stamp, walk_runtime_path, runtime_elf_search_directories,
    runtime_directory_candidate_spellings, runtime_elf_observation, runtime_capture_work,
    runtime_mount_ancestor, runtime_mount_component, runtime_reader_installation,
    runtime_namespace_state, runtime_namespace_state_record, runtime_host_root_selection,
    runtime_customization_selection, runtime_search_directory_mode, runtime_loader_edges,
    runtime_candidate_name_index,
    require_runtime_loader_observations, require_modern_glibc, require_modern_loader_diagnostics,
    runtime_hwcaps_spellings,
)

from proof_capture_capacity import bounded_capture_json, capture_publication_capacity


CAPTURE_SCHEMA = 'prismabuild.proof_runtime_capture.v3'


class HashedReader:
    def __init__(self, handle):
        self.handle = handle
        self.hasher = hashlib.sha256()

    def read(self, size):
        raw = self.handle.read(size)
        self.hasher.update(raw)
        return raw


class BoundedArchiveWriter:
    def __init__(self, handle):
        self.handle = handle

    def write(self, raw):
        if self.handle.tell() + len(raw) > CAPTURE_BOUNDS['archive_bytes']:
            raise ValueError('capture archive exceeds its sealed byte limit')
        return self.handle.write(raw)

    def __getattr__(self, name):
        return getattr(self.handle, name)


def publish_capture_checkpoint(cas, private, phase, units, value, commit):
    raw = bounded_capture_json(dict(phase=phase, units=units, value=value))
    path = private / (phase + '.json')
    with path.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    blob, _ = cas.ingest_input(path, input_id='proof-capture-' + phase,
                              expected_sha256=digest(raw), expected_bytes=len(raw))
    if not commit(units, phase, unit=phase):
        raise ValueError('the actual PB progress helper did not publish the durable commitment')
    prefix = 'prismabuild-root-capture-checkpoint: '
    print(prefix + bounded_capture_json(dict(phase=phase, units=units, blob=blob), prefix=prefix).decode().rstrip('\n'), flush=True)
    return blob


def file_descriptor(path):
    actual = inventory(path)['.']
    if actual['kind'] != 'file':
        raise ValueError('retained archive must be a regular file')
    return {key: actual[key] for key in ('bytes', 'sha256')}


def regular_members(roots):
    return {str(Path(root['path']) / relative): dict(entry)
            for root in roots for relative, entry in root['entries'].items()
            if entry['kind'] == 'file'}


def archive_members(path, members, replacements=None):
    replacements = replacements or {}
    retained = set()
    with path.open('xb') as output:
        with tarfile.open(fileobj=BoundedArchiveWriter(output), mode='w:gz', compresslevel=1) as archive:
            for source, entry in sorted(members.items()):
                if entry['sha256'] in retained:
                    continue
                if len(retained) >= CAPTURE_BOUNDS['archive_members']:
                    raise ValueError('capture archive exceeds its sealed member limit')
                member = tarfile.TarInfo('by-sha256/' + entry['sha256'])
                member.size, member.mode = entry['bytes'], 0o444
                with Path(replacements.get(entry['sha256'], source)).open('rb') as handle:
                    verified = HashedReader(handle)
                    archive.addfile(member, verified)
                    if handle.read(1) or verified.hasher.hexdigest() != entry['sha256']:
                        raise ValueError('runtime member changed while archiving: ' + source)
                retained.add(entry['sha256'])
        output.flush()
        os.fsync(output.fileno())
    return file_descriptor(path)


def retain_image_members(archive_path, destination):
    destination.mkdir()
    with tarfile.open(archive_path, 'r|gz') as archive:
        for member in archive:
            if not member.isfile() or not member.name.startswith('by-sha256/'):
                raise ValueError('image capture archive contains an unsafe member')
            name = member.name.removeprefix('by-sha256/')
            if len(name) != 64 or any(char not in '0123456789abcdef' for char in name):
                raise ValueError('image capture archive member has no content identity')
            target = destination / name
            with archive.extractfile(member) as source, target.open('xb') as output:
                shutil.copyfileobj(source, output)
                output.flush()
                os.fsync(output.fileno())
            if file_descriptor(target) != dict(bytes=member.size, sha256=name):
                raise ValueError('image capture archive member bytes differ')
            target.chmod(0o444)
    return {path.name: str(path) for path in destination.iterdir()}


def captured_roots(origin, paths, budget, directory_observations=None):
    roots = []
    for path in sorted(set(Path(value).resolve() for value in paths), key=lambda p: (len(p.parts), str(p))):
        if any(path.is_relative_to(root['path']) for root in roots):
            continue
        roots.append(dict(origin=origin, path=str(path), entries=inventory(path, budget=budget,
            directory_observations=directory_observations)))
    return roots


def capture_optional_runtime_aliases(origin, roots, budget):
    for name in ('/lib', '/lib64', '/usr/lib64'):
        path = Path(name)
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        if (stat.S_ISLNK(info.st_mode) or (name == '/usr/lib64' and stat.S_ISDIR(info.st_mode))) and not any(path.is_relative_to(root['path']) for root in roots):
            roots.append(dict(origin=origin, path=name, entries=inventory(path, budget=budget)))


def capture_host_customizations(stdlib, selected_paths, budget, *, physical=Path):
    probes = []
    def lookup(path):
        runtime_charge(budget, members=1)
        location = physical(path)
        try:
            before = location.lstat()
        except FileNotFoundError:
            probes.append(dict(path=path, entry=None))
            raise
        mode = stat.S_IMODE(before.st_mode)
        if stat.S_ISLNK(before.st_mode):
            entry = dict(kind='symlink', mode=mode, target=os.readlink(location))
        elif stat.S_ISDIR(before.st_mode):
            entry = dict(kind='directory', mode=mode)
        elif stat.S_ISREG(before.st_mode):
            entry = dict(kind='file', mode=mode, bytes=before.st_size)
        else:
            raise ValueError('unsupported host customization component: ' + path)
        after = location.lstat()
        if runtime_stamp(before) != runtime_stamp(after) or before.st_uid != after.st_uid:
            raise ValueError('host customization component changed during selection: ' + path)
        probes.append(dict(path=path, entry=entry, uid=before.st_uid,
            before=list(runtime_stamp(before)), after=list(runtime_stamp(after))))
        return dict(entry, uid=before.st_uid)
    roots = runtime_customization_selection(stdlib, selected_paths, lookup)
    return roots, probes


def runtime_capture(origin, budget=None):
    glibc = subprocess.check_output(['ldd', '--version'], text=True, timeout=30)
    require_modern_glibc(glibc)
    if budget is None:
        budget = dict(members=0, regular_bytes=0, deadline=time.monotonic() + CAPTURE_BOUNDS['runtime_seconds'])
    before_budget = {key: budget[key] for key in ('members', 'regular_bytes')}
    pre_inventory_probes = []
    cache_observations = []
    observation = observe_runtime()
    if observation['loader_environment'] or observation['at_secure'] != 0:
        raise ValueError('capture refuses ambient dynamic-loader overrides')
    stdlib = Path(sysconfig.get_path('stdlib')).resolve()
    multiarch = sysconfig.get_config_var('MULTIARCH')
    if not isinstance(multiarch, str) or '/' in multiarch or not multiarch:
        raise ValueError('selected runtime has no native multiarch library directory')
    native = Path('/usr/lib') / multiarch
    if not native.is_dir():
        raise ValueError('selected runtime native provider directory is absent')
    paths = [stdlib, native, Path(observation['executable']['resolved_path'])]
    if origin == 'host':
        configuration = Path(sys.prefix) / 'pyvenv.cfg'
        if not configuration.is_file():
            raise ValueError('the declared primary must be the selected standard venv')
        paths.extend([Path(sys.executable).parent, configuration])
    provider_paths = native_provider_paths(budget=budget, probes=pre_inventory_probes)
    paths.extend(Path(path) for path in provider_paths)
    customization_roots, customization_probes = [], []
    if origin == 'host':
        customization_roots, customization_probes = capture_host_customizations(stdlib, paths, budget)
        paths.extend(Path(item['target']) for item in customization_roots)
    elf_roots = {str(stdlib), observation['executable']['resolved_path']}
    if origin == 'image':
        startup = set()
        for value in [*site.getsitepackages(), *observation['sys_path']]:
            path = Path(value)
            try:
                info = path.stat()
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(info.st_mode):
                path = path.resolve(strict=True)
                if not path.is_relative_to('/source'):
                    startup.add(path)
        paths.extend(startup)
        elf_roots.update(map(str, startup))
        if stdlib.is_relative_to('/usr/local/lib'):
            paths.append(Path('/usr/local/lib'))
        cache = Path('/etc/ld.so.cache')
        tool = Path('/sbin/ldconfig').resolve(strict=True)
        paths.extend([cache, tool])
    directory_observations = {}
    roots = captured_roots(origin, paths, budget, directory_observations)
    if origin == 'image':
        capture_optional_runtime_aliases(origin, roots, budget)
    selected = regular_members(roots)
    providers = {path:dict(resolved_path=path, bytes=selected[path]['bytes'], sha256=selected[path]['sha256'])
                 for path in provider_paths}
    if origin == 'image':
        cache_entry = selected[str(cache)]
        before = {key: cache_entry[key] for key in ('bytes', 'sha256')}
        runtime_charge(budget, members=1, regular_bytes=before['bytes'])
        cache_observations.append(dict(path=str(cache), entry=cache_entry, boundary='listing'))
        listing = subprocess.check_output([str(tool), '-p', '-C', str(cache)], text=True)
        loader_cache_entries(listing)
        rechecked = inventory(cache, budget=budget)['.']
        cache_observations.append(dict(path=str(cache), entry=rechecked, boundary='recheck'))
        if rechecked != cache_entry:
            raise ValueError('image loader cache changed while reading its listing')

    elf_index = {}
    for captured in roots:
        for relative, entry in captured['entries'].items():
            if entry['kind'] != 'file':
                continue
            runtime_charge(budget)
            name = str(Path(captured['path']) / relative)
            elf_index[name] = runtime_elf_observation(name)
    if origin == 'host':
        for item in customization_roots:
            observed = next(probe for probe in reversed(customization_probes)
                            if probe['path'] == item['target'])
            runtime_charge(budget, members=1)
            info = Path(item['target']).lstat()
            if list(runtime_stamp(info)) != observed['after'] or info.st_uid != observed['uid']:
                raise ValueError('host customization target changed during inventory: ' + item['target'])
            customization_probes.append(dict(observed))
    result = dict(glibc=glibc, observation=observation, roots=roots, stdlib=str(stdlib), providers=providers,
                  elf_roots=sorted(elf_roots),
                  executable_binary=base64.b64encode(Path('/proc/self/exe').read_bytes()).decode(),
                  multiarch=multiarch, native_library_roots=[str(native)], elf_index=elf_index,
                  directory_observations=directory_observations,
                  pre_inventory_probes=pre_inventory_probes, cache_observations=cache_observations,
                  inventory_budget={key: budget[key] - before_budget[key] for key in before_budget},
                  consumed_budget={key: budget[key] for key in before_budget})
    if origin == 'host':
        result.update(customization_roots=customization_roots, customization_probes=customization_probes)
        runtime_host_root_selection(result)
    if origin == 'image':
        result['loader_cache'] = dict(path=str(cache), **before, raw_listing=listing,
            tool=dict(path=str(tool), resolved_path=str(tool),
                **{key: selected[str(tool)][key] for key in ('bytes', 'sha256')}))
    return result


class ImageClosureObserver:
    def __init__(self, host, image, budget, checkpoint, image_root):
        self.host = host
        self.image = image
        self.budget = budget
        self.checkpoint = checkpoint
        self.image_root = Path(image_root)
        self.host_roots = host['roots']
        self.image_roots = list(image['roots'])
        self.roots = merge_runtime_roots(self.host_roots, self.image_roots)
        self.components = {}
        self.search_directories = {}
        self.witnesses = {}
        self.negative = {}
        self.observed = {}
        self.charges = []
        self.image_directories = {}
        self.initial_budget = {name: budget[name] for name in ('members', 'regular_bytes')}

    def record_work(self, before, kind, path, **facts):
        self.charges.append(dict(kind=kind, path=path,
            members=self.budget['members'] - before['members'],
            regular_bytes=self.budget['regular_bytes'] - before['regular_bytes'], **facts))

    def image_physical(self, logical):
        return self.image_root / logical.lstrip('/')

    @contextmanager
    def image_parent(self, logical):
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        current = os.open(self.image_root, flags)
        try:
            for name in PurePosixPath(logical).parent.parts[1:]:
                following = os.open(name, flags, dir_fd=current)
                os.close(current)
                current = following
            yield current
        finally:
            os.close(current)

    def host_absence(self, logical):
        parent = str(PurePosixPath(logical).parent)
        name = PurePosixPath(logical).name
        observed = self.host['directory_observations'].get(parent)
        if observed is None or observed['before'] != observed['after'] or name in observed['names']:
            raise ValueError('host absence lacks its complete stable checkpoint enumeration: ' + logical)
        entries = {str(Path(root['path']) / relative): entry
                   for root in self.host_roots for relative, entry in root['entries'].items()}
        expected = sorted(PurePosixPath(path).name for path in entries
                          if path != parent and str(PurePosixPath(path).parent) == parent)
        if observed['names'] != expected or entries.get(parent, {}).get('kind') != 'directory':
            raise ValueError('host absence enumeration differs from its complete root')
        before = {key: self.budget[key] for key in ('members', 'regular_bytes')}
        runtime_charge(self.budget, members=1)
        witness = asdict(HostRuntimeAbsence(parent, name, tuple(observed['before']),
            tuple(observed['after']), tuple(observed['names']), self.checkpoint['sha256']))
        key = digest(canonical(witness))
        self.witnesses[key] = witness
        self.negative[logical] = key
        self.record_work(before, 'host_absence', logical, witness=key)
        raise RuntimeLookupAbsent(key)

    def image_absence(self, descriptor, logical):
        before_budget = {key: self.budget[key] for key in ('members', 'regular_bytes')}
        runtime_charge(self.budget, members=1)
        parent = str(PurePosixPath(logical).parent)
        name = PurePosixPath(logical).name
        before = runtime_stamp(os.fstat(descriptor))
        names = []
        with os.scandir(descriptor) as children:
            for child in children:
                runtime_charge(self.budget, members=1)
                names.append(child.name)
        try:
            os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        except FileNotFoundError as exc:
            if exc.errno != errno.ENOENT:
                raise
        else:
            raise ValueError('absent image target appeared during observation: ' + logical)
        after = runtime_stamp(os.fstat(descriptor))
        if before != after or name in names:
            raise ValueError('image absence parent changed during observation: ' + parent)
        witness = asdict(ImageRuntimeAbsence(parent, name, before, after, tuple(sorted(names))))
        key = digest(canonical(witness))
        self.witnesses[key] = witness
        self.negative[logical] = key
        self.record_work(before_budget, 'absence', logical, witness=key)
        raise RuntimeLookupAbsent(key)

    def retain_image_entry(self, logical, entry):
        if any(Path(logical).is_relative_to(root['path']) for root in self.roots):
            return
        candidate = dict(origin='image', path=logical, entries={'.': entry})
        self.image_roots.append(candidate)
        self.roots = merge_runtime_roots(self.host_roots, self.image_roots)

    def retain_mount_component(self, logical):
        before = {key: self.budget[key] for key in ('members', 'regular_bytes')}
        runtime_charge(self.budget, members=1)
        component = runtime_mount_component(dict(roots=self.roots,
            completion=dict(base_image_roots=self.image['roots'])), logical)
        self.components[logical] = component
        self.observed[logical] = component['entry']
        self.record_work(before, 'component', logical, entry=component['entry'])
        return component['entry']

    def mount_absence(self, logical):
        before = {key: self.budget[key] for key in ('members', 'regular_bytes')}
        runtime_charge(self.budget, members=1)
        parent = str(Path(logical).parent)
        witness = asdict(MountRuntimeAbsence(parent, Path(logical).name,
            tuple(runtime_mount_component(dict(roots=self.roots), parent)['names'])))
        key = digest(canonical(witness))
        self.witnesses[key] = witness
        self.negative[logical] = key
        self.record_work(before, 'mount_absence', logical, witness=key)
        raise RuntimeLookupAbsent(key)

    def lookup(self, logical):
        runtime_charge(self.budget)
        if logical in self.negative:
            raise RuntimeLookupAbsent(self.negative[logical])
        if logical in self.observed:
            return self.observed[logical]
        for root in self.host_roots:
            if Path(logical).is_relative_to(root['path']):
                relative = str(Path(logical).relative_to(root['path']))
                entry = root['entries'].get(relative)
                if entry is None:
                    return self.host_absence(logical)
                self.observed[logical] = entry
                return entry
        mount_children = runtime_mount_ancestor(dict(roots=self.roots), logical)
        parent = self.components.get(str(Path(logical).parent))
        if parent is not None and parent['origin'] == 'mount':
            if mount_children is not None:
                return self.retain_mount_component(logical)
            return self.mount_absence(logical)
        existing = None
        for root in self.roots:
            if Path(logical).is_relative_to(root['path']):
                existing = root['entries'].get(str(Path(logical).relative_to(root['path'])))
                break
        before_budget = {key: self.budget[key] for key in ('members', 'regular_bytes')}
        if logical == '/':
            info = self.image_root.lstat()
            runtime_charge(self.budget, members=1)
            if not stat.S_ISDIR(info.st_mode):
                raise ValueError('image observation anchor is not a directory')
            entry = dict(kind='directory', mode=stat.S_IMODE(info.st_mode))
            stamp = runtime_stamp(info)
        else:
            with self.image_parent(logical) as descriptor:
                parent_before = runtime_stamp(os.fstat(descriptor))
                name = PurePosixPath(logical).name
                try:
                    info = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                except FileNotFoundError as exc:
                    if exc.errno != errno.ENOENT:
                        raise
                    if mount_children is not None:
                        return self.retain_mount_component(logical)
                    return self.image_absence(descriptor, logical)
                stamp = runtime_stamp(info)
                if stat.S_ISDIR(info.st_mode):
                    runtime_charge(self.budget, members=1)
                    child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                    dir_fd=descriptor)
                    try:
                        if runtime_stamp(os.fstat(child)) != stamp:
                            raise ValueError('image navigation directory changed before open: ' + logical)
                    finally:
                        os.close(child)
                    entry = dict(kind='directory', mode=stat.S_IMODE(info.st_mode))
                else:
                    anchored = Path('/proc/self/fd') / str(descriptor) / name
                    entry = inventory(anchored, budget=self.budget)['.']
                if (runtime_stamp(os.stat(name, dir_fd=descriptor, follow_symlinks=False)) != stamp
                        or runtime_stamp(os.fstat(descriptor)) != parent_before):
                    raise ValueError('image component changed during observation: ' + logical)
        runtime_mount_ancestor(dict(roots=self.roots), logical, entry)
        if existing is not None and entry != existing:
            raise ValueError('covered image runtime member changed: ' + logical)
        if entry['kind'] == 'directory':
            self.image_directories[logical] = stamp
            if existing is None:
                self.components[logical] = dict(origin='image', entry=entry, stamp=list(stamp))
        else:
            self.retain_image_entry(logical, entry)
        self.observed[logical] = entry
        self.record_work(before_budget, 'recheck' if existing is not None else 'component', logical,
                         entry=entry)
        return entry

    def complete_directory(self, logical, *, search=False):
        if any(Path(logical).is_relative_to(root['path']) for root in self.roots):
            return
        if any(Path(root['path']).is_relative_to(logical) for root in self.roots):
            return
        if logical in BROAD_ROOTS:
            raise ValueError('required directory is too broad for runtime capture: ' + logical)
        before = {key: self.budget[key] for key in ('members', 'regular_bytes')}
        with self.image_parent(logical) as descriptor:
            anchored = Path('/proc/self/fd') / str(descriptor) / PurePosixPath(logical).name
            observations = {}
            entries = inventory(anchored, budget=self.budget, directory_observations=observations)
        captured = dict(origin='image', path=logical, entries=entries)
        self.image_roots.append(captured)
        self.roots = merge_runtime_roots(self.host_roots, self.image_roots)
        self.components.pop(logical, None)
        self.record_work(before, 'inventory', logical, entries=entries)

    def directory_names(self, logical):
        if any(Path(logical).is_relative_to(root['path']) and root['entries']['.']['kind'] == 'directory'
               for root in self.roots):
            entries = {str(Path(root['path']) / relative): entry
                       for root in self.roots for relative, entry in root['entries'].items()}
            if entries.get(logical, {}).get('kind') != 'directory':
                raise ValueError('complete hwcaps inventory omits its directory')
            return sorted(Path(path).name for path in entries if str(Path(path).parent) == logical and path != logical)
        component = self.components.get(logical, {})
        if component.get('origin') == 'mount':
            return component['names']
        if logical not in self.search_directories:
            before_budget = {key: self.budget[key] for key in ('members', 'regular_bytes')}
            runtime_charge(self.budget, members=1)
            with self.image_parent(str(Path(logical) / '.listing')) as descriptor:
                before = runtime_stamp(os.fstat(descriptor))
                names = []
                with os.scandir(descriptor) as children:
                    for child in children:
                        runtime_charge(self.budget, members=1)
                        names.append(child.name)
                after = runtime_stamp(os.fstat(descriptor))
            if before != after:
                raise ValueError('hwcaps directory listing changed during observation: ' + logical)
            observed = dict(before=list(before), after=list(after), names=sorted(names))
            self.search_directories[logical] = observed
            self.record_work(before_budget, 'listing', logical, observation=observed)
        names = self.search_directories[logical]['names']
        mount = runtime_mount_ancestor(dict(roots=self.roots), logical) or ()
        return sorted(set(names) | set(mount))

    def finish(self):
        for logical, stamp in self.image_directories.items():
            runtime_charge(self.budget)
            if logical == '/':
                actual = self.image_root.lstat()
            else:
                with self.image_parent(logical) as descriptor:
                    actual = os.stat(PurePosixPath(logical).name, dir_fd=descriptor, follow_symlinks=False)
            if runtime_stamp(actual) != stamp:
                raise ValueError('image navigation directory changed before completion: ' + logical)


def discover_image_runtime_graph(observer, host, image, plan, lookup, *, additional_links=()):
    outcomes = {}
    aliases = {}
    metadata_by_path = {}

    def resolve_observation(spelling, *, required=False, search=False):
        outcome = walk_runtime_path(spelling, lookup)
        if isinstance(outcome, RuntimeWitnessedAbsent):
            native = any(Path(spelling).is_relative_to(root)
                         for root in host['native_library_roots'] + image['native_library_roots'])
            if required or spelling in plan['required_spellings'] or not native:
                raise ValueError('required runtime alias is absent outside the permitted native graph: ' + spelling)
        else:
            aliases[spelling] = outcome.terminal
            if lookup(outcome.terminal)['kind'] == 'directory':
                observer.complete_directory(outcome.terminal, search=search)
            elif search:
                raise ValueError('loader search component is not a directory: ' + spelling)
        outcomes[spelling] = runtime_outcome_record(outcome)
        return outcome

    def retain_candidate(spelling):
        outcome = resolve_observation(spelling, required=True)
        if lookup(outcome.terminal)['kind'] != 'file':
            raise ValueError('runtime ELF candidate is not a sealed regular file')
        metadata = candidate_metadata(outcome.terminal)
        if metadata is None:
            raise ValueError('runtime dependency is not a loadable ELF: ' + spelling)
        metadata_by_path[outcome.terminal] = metadata
        return outcome.terminal

    def candidate_metadata(path):
        for captured, origin in ((host, 'host'), (image, 'image')):
            if origin == 'image' and any(Path(path).is_relative_to(root['path']) for root in host['roots']):
                continue
            if path in captured['elf_index']:
                metadata = captured['elf_index'][path]
                break
        else:
            metadata = runtime_elf_observation(observer.image_physical(path))
        if isinstance(metadata, dict) and 'parse_refusal' in metadata:
            raise ValueError('runtime ELF candidate metadata refuses: ' + metadata['parse_refusal'])
        return metadata

    cache = loader_cache_entries(image['loader_cache']['raw_listing']) if 'loader_cache' in image else []
    defaults = loader_default_directories(image['pre_completion_diagnostics']['raw'])
    for _ in range(64):
        before = (len(observer.roots), len(observer.components), len(observer.witnesses),
                  len(outcomes), len(aliases), len(metadata_by_path))
        links = {str(Path(root['path']) / relative): entry
                 for root in observer.roots for relative, entry in root['entries'].items()
                 if entry['kind'] == 'symlink'}
        for spelling in sorted((set(links) | set(additional_links) | set(plan['required_spellings'])) - outcomes.keys()):
            resolve_observation(spelling, required=spelling in plan['required_spellings'])
        entries = {str(Path(root['path']) / relative): entry
                   for root in observer.roots for relative, entry in root['entries'].items()}
        for path, entry in entries.items():
            if (entry['kind'] == 'file' and path not in metadata_by_path
                    and any(Path(path).is_relative_to(seed) for seed in plan['elf_roots'])):
                metadata = candidate_metadata(path)
                if metadata is not None:
                    metadata_by_path[path] = metadata
        runtime_elf_search_directories(metadata_by_path, aliases)
        entries = {str(Path(root['path']) / relative): entry
                   for root in observer.roots for relative, entry in root['entries'].items()}
        candidates_by_name = runtime_candidate_name_index(entries)
        directory_name_cache = {}
        candidate_index = [entries, candidates_by_name, len(observer.roots)]
        def directory_names(spelling, *, required):
            key = (spelling, required)
            if key in directory_name_cache:
                return directory_name_cache[key]
            parent = walk_runtime_path(str(Path(spelling).parent), lookup)
            if not isinstance(parent, RuntimeResolved):
                raise ValueError('hwcaps parent directory is not resolved: ' + spelling)
            expected = str(Path(parent.terminal) / Path(spelling).name)
            outcome = walk_runtime_path(spelling, lookup)
            if isinstance(outcome, RuntimeWitnessedAbsent):
                if required or outcome.missing != expected or outcome.remaining:
                    raise ValueError('required hwcaps directory is not sealed: ' + spelling)
                return None
            aliases[spelling] = outcome.terminal
            if lookup(outcome.terminal)['kind'] != 'directory':
                raise ValueError('hwcaps component is not a directory: ' + spelling)
            names = observer.directory_names(outcome.terminal)
            directory_name_cache[key] = names
            return names

        def directory_candidates(spelling, needed):
            directory = resolve_observation(spelling, required=True, search=True).terminal
            direct_search = runtime_search_directory_mode(observer.roots, directory) == 'direct'
            if candidate_index[2] != len(observer.roots):
                current = {str(Path(root['path']) / relative): entry
                           for root in observer.roots for relative, entry in root['entries'].items()}
                candidate_index[:] = [current, runtime_candidate_name_index(current), len(observer.roots)]
            current_entries = candidate_index[0]
            found = {retain_candidate(str(Path(spelling) / Path(candidate).relative_to(directory)))
                for candidate in runtime_directory_candidate_spellings(
                    current_entries, directory, needed, direct=direct_search,
                    by_name=candidate_index[1])}
            base = None
            for candidate in runtime_hwcaps_spellings(spelling, needed, directory_names):
                parent = walk_runtime_path(str(Path(candidate).parent), lookup)
                probe = walk_runtime_path(candidate, lookup)
                expected = str(Path(parent.terminal) / needed)
                if isinstance(probe, RuntimeWitnessedAbsent):
                    if probe.missing != expected or probe.remaining:
                        retain_candidate(candidate)
                    continue
                if lookup(probe.terminal)['kind'] != 'file':
                    continue
                target = retain_candidate(candidate)
                found.add(target)
                if candidate == str(Path(spelling) / needed):
                    base = target
            return found, base

        _, _, unresolved = runtime_loader_edges(metadata_by_path, aliases, cache, defaults,
            directory_candidates=directory_candidates, resolve_candidate=retain_candidate)
        after = (len(observer.roots), len(observer.components), len(observer.witnesses),
                 len(outcomes), len(aliases), len(metadata_by_path))
        if before == after:
            if unresolved:
                raise ValueError(unresolved[0])
            break
    else:
        raise ValueError('runtime image completion did not converge inside its finite bound')
    return outcomes


def complete_image_closure(host, image, budget, *, host_checkpoint, image_root=Path('/')):
    require_modern_glibc(image['glibc'])
    require_modern_loader_diagnostics(image['pre_completion_diagnostics']['raw'])
    observer = ImageClosureObserver(host, image, budget, host_checkpoint, image_root)
    plan = runtime_completion_plan(host, image)
    # Observe the fixed reader path without granting any primary exemption.
    # Only the shared owner may turn an exact image leaf absence into creation.
    walk_runtime_path(plan['reader_spelling'], observer.lookup)
    walk_runtime_path(plan['reader_target'], observer.lookup)
    facts = dict(roots=observer.roots, components=observer.components,
        directory_witnesses=observer.witnesses, plan=plan,
        completion=dict(host_checkpoint=host_checkpoint, base_image_roots=image['roots'],
                        host_directory_observations=host['directory_observations']))
    installation = runtime_reader_installation(facts)
    before_plan = dict(plan, required_spellings=[path for path in plan['required_spellings']
        if installation is None or path != plan['reader_spelling']])

    def installed_lookup(path):
        if installation is not None and path == installation['path']:
            return installation['entry']
        return observer.lookup(path)

    for _ in range(64):
        before = (len(observer.roots), len(observer.components), len(observer.witnesses))
        outcomes = discover_image_runtime_graph(observer, host, image, before_plan, observer.lookup)
        discover_image_runtime_graph(observer, host, image, plan, installed_lookup,
            additional_links=() if installation is None else (installation['path'],))
        after = (len(observer.roots), len(observer.components), len(observer.witnesses))
        if before == after:
            break
    else:
        raise ValueError('runtime namespace states did not converge inside their finite bound')
    observer.finish()
    links = {str(Path(root['path']) / relative) for root in observer.roots
             for relative, entry in root['entries'].items() if entry['kind'] == 'symlink'}
    completion = dict(host_checkpoint=host_checkpoint,
        host_observation=host['observation'], image_observation=image['observation'],
        host_multiarch=host['multiarch'], image_multiarch=image['multiarch'],
        host_stdlib=host['stdlib'], host_providers=host['providers'],
        host_customization_roots=host['customization_roots'],
        host_customization_probes=host['customization_probes'],
        host_elf_roots=host['elf_roots'], image_elf_roots=image['elf_roots'],
        host_elf_index=host['elf_index'], image_elf_index=image['elf_index'],
        image_loader_cache=image['loader_cache'], image_glibc=image['glibc'],
        search_directory_observations=observer.search_directories,
        pre_completion_diagnostics=image['pre_completion_diagnostics'],
        base_host_roots=host['roots'], base_image_roots=image['roots'],
        host_pre_inventory_probes=host.get('pre_inventory_probes', []),
        image_pre_inventory_probes=image.get('pre_inventory_probes', []),
        host_cache_observations=host.get('cache_observations', []),
        image_cache_observations=image.get('cache_observations', []),
        host_directory_observations=host['directory_observations'],
        image_directory_observations=image['directory_observations'],
        base_consumed_budget=observer.initial_budget, charges=observer.charges)
    result = CompletedRuntimeNamespace(tuple(observer.roots),
        tuple(sorted(set(host['native_library_roots'] + image['native_library_roots']))),
        observer.components, observer.witnesses, {key: outcomes[key] for key in sorted(links)}, plan,
        {key: budget[key] for key in ('members', 'regular_bytes')}, completion,
        installation, {})
    namespace = asdict(result)
    for state in ('before_installation', 'after_installation'):
        view = runtime_namespace_state(namespace, state)
        record = runtime_namespace_state_record(view)
        view['link_outcomes'] = record['link_outcomes']
        audit_link_outcomes(view)
        result.namespace_states[state] = record
    return result


def read_host_capture_checkpoint(path, expected, primary_interpreter):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > CAPTURE_BOUNDS['metadata_bytes']:
        raise ValueError('host checkpoint must be a bounded regular private input')
    raw = path.read_bytes()
    if (len(raw) != expected['bytes'] or digest(raw) != expected['sha256']
            or runtime_stamp(path.lstat()) != runtime_stamp(info)):
        raise ValueError('host checkpoint differs from its durable descriptor')
    value = json.loads(raw)
    if value['phase'] != 'host_inventory' or value['units'] != 1:
        raise ValueError('host checkpoint has a different durable phase or units')
    host = value['value']
    metadata = read_runtime_elf(io.BytesIO(base64.b64decode(host['executable_binary'], validate=True)))
    if metadata['interpreters'] != [primary_interpreter]:
        raise ValueError('host checkpoint PT seed differs from its explicit private input')
    if host['native_library_roots'] != ['/usr/lib/' + host['multiarch']]:
        raise ValueError('host checkpoint native library root differs from its selected MULTIARCH')
    counters = {name: host['consumed_budget'][name] for name in ('members', 'regular_bytes')}
    if counters != runtime_capture_work(host):
        raise ValueError('host checkpoint consumed inventory counters cannot be replayed')
    return host


def capture_private_directory(output):
    output.parent.mkdir(parents=True, exist_ok=True)
    private = output.parent / (output.name + '.members')
    private.mkdir()
    return private.resolve(strict=True)


def main(argv=None):
    deadline = time.monotonic() + CAPTURE_BOUNDS['runtime_seconds']
    def remaining():
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError('capture exceeded its finite execution bound')
        return seconds
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture-image', action='store_true')
    parser.add_argument('--image')
    parser.add_argument('--image-python')
    parser.add_argument('--profile-path')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--cas-root', type=Path)
    parser.add_argument('--reader-uid', type=int, default=65534)
    parser.add_argument('--reader-gid', type=int, default=65534)
    parser.add_argument('--host-checkpoint', type=Path)
    parser.add_argument('--host-checkpoint-sha256')
    parser.add_argument('--host-checkpoint-bytes', type=int)
    parser.add_argument('--primary-interpreter')
    parser.add_argument('--pre-completion-diagnostics', type=Path)
    parser.add_argument('--remaining-s', type=float)
    args = parser.parse_args(argv)
    if args.capture_image:
        if args.output is None:
            parser.error('image capture requires a private archive output')
        if not args.primary_interpreter or not args.primary_interpreter.startswith('/'):
            parser.error('image capture requires an explicit absolute primary interpreter')
        if (args.host_checkpoint is None or not args.host_checkpoint.is_absolute()
                or args.host_checkpoint_bytes is None or args.host_checkpoint_bytes <= 0
                or args.host_checkpoint_bytes > CAPTURE_BOUNDS['metadata_bytes']
                or args.host_checkpoint_sha256 is None or len(args.host_checkpoint_sha256) != 64
                or any(char not in '0123456789abcdef' for char in args.host_checkpoint_sha256)
                or args.remaining_s is None or not math.isfinite(args.remaining_s)
                or not 0 < args.remaining_s <= CAPTURE_BOUNDS['runtime_seconds']):
            parser.error('image capture requires its exact bounded host checkpoint and remaining allowance')
        deadline = min(deadline, time.monotonic() + args.remaining_s)
        host_descriptor = dict(bytes=args.host_checkpoint_bytes, sha256=args.host_checkpoint_sha256)
        host = read_host_capture_checkpoint(args.host_checkpoint, host_descriptor, args.primary_interpreter)
        budget = dict(host['consumed_budget'], deadline=deadline)
        os.makedirs('/home/rob/pbt17', exist_ok=True)
        captured = runtime_capture('image', budget)
        if args.pre_completion_diagnostics is None:
            parser.error('image capture requires mounted pre-completion loader diagnostics')
        captured['pre_completion_diagnostics'] = json.loads(
            args.pre_completion_diagnostics.read_text())
        loader_default_directories(captured['pre_completion_diagnostics']['raw'])
        if loader_kernel(captured['pre_completion_diagnostics']['raw'])[0] != host['observation']['kernel']['release']:
            raise ValueError('pre-completion loader diagnostics belong to a different actual kernel')
        completed = complete_image_closure(host, captured, budget, host_checkpoint=host_descriptor)
        captured['completed_namespace'] = asdict(completed)
        captured['consumed_budget'] = dict(completed.consumed_budget)
        archive_roots = list(captured['roots'])
        archive_roots.extend(root for root in completed.roots
                             if root['origin'] == 'image' and root not in archive_roots)
        captured['archive'] = archive_members(args.output, regular_members(archive_roots))
        remaining()
        print(bounded_capture_json(captured).decode().rstrip('\n'), flush=True)
        return 0
    if not args.image or not args.image_python or not args.profile_path or args.output is None or args.cas_root is None:
        parser.error('host capture requires image, image-python, profile-path, output and cas-root')
    relative = Path(args.profile_path)
    if relative.is_absolute() or '..' in relative.parts:
        raise ValueError('profile output name must be snapshot-local')
    if args.reader_uid <= 0 or args.reader_gid <= 0:
        raise ValueError('reader observation requires real unprivileged UID/GID')
    if not args.image.startswith('sha256:') or len(args.image) != 71:
        raise ValueError('capture requires an existing immutable image ID')
    root = Path(__file__).resolve().parents[1]
    if args.output.absolute() != root / relative or Path.cwd().resolve() != root:
        raise ValueError('profile must be an explicit snapshot-local derived capture output')
    private = capture_private_directory(args.output)
    progress_helper = Path(os.environ['PRISMABUILD_ACTION_PROGRESS_HELPER']).resolve(strict=True)
    progress = runpy.run_path(str(progress_helper))
    if (progress['declared_phases']() != CAPTURE_PHASES
            or progress['declared_allowances']() != {name:120.0 for name in CAPTURE_PHASES}):
        raise ValueError('capture requires its exact finite ordered progress policy')
    published = Path('/mnt/shared/prismabuild-fleet/repo').resolve()
    if progress_helper != published / 'src/prismabuild/progress.py':
        raise ValueError('capture requires aligned producer and progress-helper generations')
    with capture_publication_capacity(private, args.cas_root, published) as capacity:
        record = capture_host(args, root, private, relative, progress_helper, progress, published, remaining, capacity)
    prefix = 'prismabuild-root-capture: '
    print(prefix + bounded_capture_json(record, prefix=prefix).decode().rstrip('\n'), flush=True)
    return 0


def capture_host(args, root, private, relative, progress_helper, progress, published, remaining, capacity):
    owner = runpy.run_path(str(published / 'src/prismabuild/core.py'))
    cas = owner['PrismaBuildCAS'](args.cas_root)
    checkpoints = {}
    budget = dict(members=0, regular_bytes=0, deadline=time.monotonic() + remaining())
    host = runtime_capture('host', budget)
    checkpoints[CAPTURE_PHASES[0]] = publish_capture_checkpoint(cas, private, CAPTURE_PHASES[0], 1, host, progress['commit'])
    threads = dict(OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1')
    if host['observation']['native_threads'] != threads:
        raise ValueError('host capture requires actual native thread ceilings1')
    if host['observation']['environment']['TMPDIR'] != '/home/rob/pbt17':
        raise ValueError('host capture requires its declared private TMPDIR')
    inspected = json.loads(subprocess.check_output([
        'docker', 'image', 'inspect', args.image, '--format',
        '{"id":{{json .Id}},"architecture":{{json .Architecture}}}'], text=True))
    architecture = {62: 'amd64', 183: 'arm64'}[host['observation']['elf_machine']]
    if inspected != dict(id=args.image, architecture=architecture):
        raise ValueError('existing image identity or architecture differs')
    affinity = host['observation']['affinity']
    interpreter, = read_runtime_elf(io.BytesIO(base64.b64decode(host['executable_binary'], validate=True)))['interpreters']
    host_descriptor = {name: checkpoints[CAPTURE_PHASES[0]][name] for name in ('bytes', 'sha256')}
    host_checkpoint_path = private / 'host_inventory.json'
    command = ['docker', 'run', '--rm', '--network', 'none', '--user', '0:0',
        '--entrypoint', args.image_python, '--cpuset-cpus', ','.join(map(str, affinity)),
        '--memory', '1g', '--pids-limit', '128', '--mount', f'type=bind,src={root},dst=/source,readonly',
        '--mount', f'type=bind,src={private},dst=/capture-output',
        '--mount', f'type=bind,src={host_checkpoint_path},dst=/capture-host-checkpoint.json,readonly',
        '--workdir', '/source', '--env', 'PYTHONDONTWRITEBYTECODE=1',
        '--env', 'TMPDIR=/home/rob/pbt17']
    for name in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS'):
        command.extend(['--env', f'{name}=1'])
    command.extend([args.image, '-B', '/source/tests/capture_proof_runtime.py', '--capture-image',
                    '--output', '/capture-output/image-members.tar.gz',
                    '--host-checkpoint', '/capture-host-checkpoint.json',
                    '--host-checkpoint-sha256', host_descriptor['sha256'],
                    '--host-checkpoint-bytes', str(host_descriptor['bytes']),
                    '--primary-interpreter', interpreter, '--remaining-s', str(remaining())])
    diagnostics = command[:command.index(args.image)]
    diagnostics[diagnostics.index('--entrypoint') + 1] = interpreter
    for captured in host['roots']:
        path = captured['path']
        diagnostics.extend(['--mount', f'type=bind,src={path},dst={path},readonly'])
    diagnostics.extend([args.image, '--list-diagnostics'])
    pre_raw = subprocess.check_output(diagnostics, text=True, timeout=min(30, remaining()))
    loader_default_directories(pre_raw)
    if loader_kernel(pre_raw)[0] != host['observation']['kernel']['release']:
        raise ValueError('pre-completion loader diagnostics belong to a different actual kernel')
    pre_diagnostics = dict(raw=pre_raw, command=[interpreter, '--list-diagnostics'],
        executable=dict(path=interpreter, resolved_path=str(Path(interpreter).resolve()),
                        **file_descriptor(Path(interpreter).resolve())))
    pre_path = private / 'pre-completion-diagnostics.json'
    with pre_path.open('xb') as handle:
        handle.write(bounded_capture_json(pre_diagnostics))
    command[command.index(args.image):command.index(args.image)] = [
        '--mount', f'type=bind,src={pre_path},dst=/capture-loader-diagnostics.json,readonly']
    command.extend(['--pre-completion-diagnostics', '/capture-loader-diagnostics.json'])
    command[command.index('--remaining-s') + 1] = str(remaining())
    result = subprocess.run(command, capture_output=True, text=True, timeout=remaining(), check=False)
    if result.returncode:
        raise ValueError('existing image capture failed: ' + result.stderr[-2000:])
    if len(result.stdout.encode()) > CAPTURE_BOUNDS['metadata_bytes']:
        raise ValueError('image checkpoint metadata exceeds its sealed limit')
    image = json.loads(result.stdout)
    namespace = image['completed_namespace']
    if namespace['completion']['host_checkpoint'] != host_descriptor:
        raise ValueError('image completion belongs to a different durable host checkpoint')
    image_archive = private / 'image-members.tar.gz'
    if file_descriptor(image_archive) != image['archive']:
        raise ValueError('actual image member archive differs')
    image_members = retain_image_members(image_archive, private / 'image-by-sha256')
    checkpoints[CAPTURE_PHASES[1]] = publish_capture_checkpoint(cas, private, CAPTURE_PHASES[1], 2, image, progress['commit'])
    if image['observation']['native_threads'] != threads:
        raise ValueError('image capture changed native thread ceilings')
    if any(image['observation']['credentials'][key] != 0 for key in ('uid', 'euid', 'gid', 'egid')):
        raise ValueError('image capture did not use real UID/GID0')
    if image['observation']['affinity'] != affinity or image['observation']['elf_machine'] != host['observation']['elf_machine']:
        raise ValueError('image capture changed actual architecture or assigned affinity')
    roots = namespace['roots']
    entries = {str(Path(root['path']) / name):entry for root in roots for name,entry in root['entries'].items()}
    if resolve_runtime_path(entries, interpreter, namespace['components']) != str(Path(interpreter).resolve()):
        raise ValueError('image loader alias does not select the captured host loader')
    raw_diagnostics = subprocess.check_output(diagnostics, text=True, timeout=min(30, remaining()))
    require_runtime_loader_observations(pre_diagnostics, dict(raw=raw_diagnostics))
    if image['pre_completion_diagnostics'] != pre_diagnostics:
        raise ValueError('image completion changed its mounted loader input')
    if loader_kernel(raw_diagnostics)[0] != host['observation']['kernel']['release']:
        raise ValueError('loader diagnostics belong to a different actual kernel')
    target = namespace['plan']['reader_target']
    reader = dict(namespace['plan']['reader_identity'])
    reader.update(uid=args.reader_uid, gid=args.reader_gid, link_target=target)
    closure = dict(namespace, schema=CLOSURE_SCHEMA, graph_policy='required-runtime-graph-v1',
        roots=sorted(roots, key=lambda item: (item['origin'], item['path'])),
        elf_machine=host['observation']['elf_machine'],
        elf_roots=namespace['plan']['elf_roots'],
        loader=dict(schema='prismabuild.proof_loader_search.v1', cache=image['loader_cache'],
            diagnostics=dict(raw=raw_diagnostics, command=[interpreter, '--list-diagnostics'],
                executable=dict(path=interpreter, resolved_path=str(Path(interpreter).resolve()),
                                **file_descriptor(Path(interpreter).resolve())))))
    closure_members = regular_members(closure['roots'])
    geometry = dict(members=sum(len(root['entries']) for root in roots),
                    regular_bytes=sum(entry['bytes'] for entry in closure_members.values()))
    if any(geometry[name] > CAPTURE_BOUNDS[name] for name in geometry):
        raise ValueError('combined capture geometry exceeds its sealed limits: ' + str(geometry))
    host_contents = {entry['sha256']: path for path, entry in regular_members(host['roots']).items()}
    closure['audit'] = audit_runtime_closure(closure,
        lambda path: image_members.get(closure_members[path]['sha256'], path),
        content_path=lambda sha: image_members[sha] if sha in image_members else host_contents[sha])
    closure['sha256'] = digest(canonical(closure))
    profile = dict(schema=PROFILE_SCHEMA, image=args.image,
        primary={key: host['observation'][key] for key in ('version', 'executable', 'elf_machine')},
        installed_reader=reader, closure=closure)
    raw = canonical(profile) + b'\n'
    if len(raw) > CAPTURE_BOUNDS['profile_bytes']:
        raise ValueError('captured profile exceeds its sealed byte limit')
    with args.output.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    mounted = command[:command.index(args.image)]
    mounted[mounted.index('--entrypoint') + 1] = profile['primary']['executable']['path']
    for captured in closure['roots']:
        if captured['origin'] == 'host':
            path = captured['path']
            mounted.extend(['--mount', f'type=bind,src={path},dst={path},readonly'])
    bootstrap = (
        "import runpy; from pathlib import Path; "
        "owner=runpy.run_path('/source/tests/proof_container_runtime.py'); "
        "profile=owner['load_profile'](" + repr(str(Path('/source') / relative)) + ", Path('/source')); "
        "print(owner['canonical'](owner['observe_container_roles'](profile, " + repr(affinity) + ")).decode(), flush=True)"
    )
    mounted.extend([args.image, '-B', '-S', '-c', bootstrap])
    result = subprocess.run(mounted, capture_output=True, text=True, timeout=remaining(), check=False)
    if result.returncode:
        raise ValueError('mounted runtime capture failed: ' + result.stderr[-2000:])
    roles = json.loads(result.stdout)
    checkpoints[CAPTURE_PHASES[2]] = publish_capture_checkpoint(cas, private, CAPTURE_PHASES[2], 3,
        dict(profile_sha256=digest(raw), profile_json=raw.decode(), roles=roles), progress['commit'])
    members = dict(closure_members)
    original_image_members = regular_members(image['roots'])
    for path, entry in original_image_members.items():
        members['image-original:' + path] = entry
    image_executable = image['observation']['executable']
    members['image-executable:' + image_executable['sha256']] = dict(kind='file',
        bytes=image_executable['bytes'], sha256=image_executable['sha256'])
    attested = [Path('/bin/bash').resolve(), *(published / name for name in CAPTURE_PUBLISHED_SOURCES)]
    if (image_executable['bytes'] + sum(path.stat().st_size for path in attested)
            > CAPTURE_BOUNDS['supplemental_bytes']):
        raise ValueError('capture supplemental producer bytes exceed their sealed limit before reading')
    for path in attested:
        members[str(path)] = inventory(path)['.']
    if sum(entry['bytes'] for path,entry in members.items()
           if path not in closure_members and not path.startswith('image-original:')) > CAPTURE_BOUNDS['supplemental_bytes']:
        raise ValueError('capture supplemental producer bytes exceed their sealed limit')
    archive_path = private / 'closure-members.tar.gz'
    archive = archive_members(archive_path, members, image_members)
    remaining()
    blob, _ = cas.ingest_input(archive_path, input_id='proof-runtime-closure',
                             expected_sha256=archive['sha256'], expected_bytes=archive['bytes'])
    checkpoints[CAPTURE_PHASES[3]] = publish_capture_checkpoint(cas, private, CAPTURE_PHASES[3], 4,
        dict(archive=blob, geometry=geometry), progress['commit'])
    source = {name: digest((root / name).read_bytes()) for name in (
        'tests/capture_proof_runtime.py', 'tests/proof_container_runtime.py',
        'tests/run_proof_container.py', 'tests/proof_capture_capacity.py')}
    remaining()
    record = dict(schema=CAPTURE_SCHEMA, complete=True, profile_path=args.profile_path, capacity=capacity,
        profile_bytes=len(raw), profile_sha256=digest(raw), profile_json=raw.decode(),
        closure_sha256=closure['sha256'], source_sha256=source,
        closure_archive=dict(bytes=blob['bytes'], sha256=blob['sha256']),
        closure_members={path:dict(bytes=entry['bytes'], sha256=entry['sha256'],
                                  archive_member='by-sha256/' + entry['sha256']) for path,entry in sorted(closure_members.items())},
        original_image_members={path:dict(bytes=entry['bytes'], sha256=entry['sha256'],
            archive_member='by-sha256/' + entry['sha256']) for path,entry in sorted(original_image_members.items())},
        cas_root=str(args.cas_root),
        bounds=CAPTURE_BOUNDS, geometry=dict(geometry, archive_bytes=blob['bytes'], profile_bytes=len(raw)),
        progress=dict(phases=list(CAPTURE_PHASES), checkpoints=checkpoints,
            helper=dict(path=str(progress_helper), resolved_path=str(progress_helper), **file_descriptor(progress_helper))),
        image=dict(expected=args.image, actual=inspected['id'], architecture=inspected['architecture']),
        outer=host['observation'], image_runtime=image['observation'], inner=roles['inner'],
        installed_reader=roles['installed_reader'], closure_verification=roles['closure'],
        mapped_native_providers=roles['mapped_native_providers'],
        executable_binaries={host['observation']['executable']['sha256']:host['executable_binary'],
                             image['observation']['executable']['sha256']:image['executable_binary']})
    return record


if __name__ == '__main__':
    raise SystemExit(main())
