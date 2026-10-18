"""Interpret a sealed root-test runtime profile inside its admitted container."""
from dataclasses import asdict, dataclass, field
import base64
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import runpy
import stat
import struct
import subprocess
import sys
import time


PROFILE_SCHEMA = 'prismabuild.proof_runtime_profile.v3'
CLOSURE_SCHEMA = 'prismabuild.proof_runtime_closure.v3'
NAMESPACE_STATES = ('before_installation', 'after_installation')
ATTESTATION_SCHEMA = 'prismabuild.root_test_runtime.v1'
THREADS = ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS')
BROAD_ROOTS = {'/', '/usr', '/usr/bin', '/usr/lib', '/home', '/home/rob', '/etc', '/var', '/mnt'}
CAPTURE_PUBLISHED_SOURCES = tuple('src/prismabuild/' + name + '.py' for name in (
    '__init__', '_bounded_reader', '_gang', '_measurement_reservation',
    'action_edges', 'adaptive_cpu', 'adaptive_gpu', 'adaptive_snapshot',
    'box_capacity', 'box_window', 'container_images', 'control_cpu', 'core',
    'cpu_topology', 'decomposition', 'dependency_digest', 'digest_primitives',
    'filesystem_capacity', 'filesystem_floor', 'gpu_capacity', 'local_dependencies',
    'local_resident', 'local_scratch', 'local_tier', 'materialize', 'movement_actions',
    'pool', 'posix_lock', 'produced_output', 'produced_spool', 'progress',
    'publication_canary', 'reader_lease', 'residency_map', 'residency_plan',
    'resident_sets', 'resource_scope', 'runtime_publication', 'storage_tiers',
    'window_credit')) + ('tools/prismabuild_worker.py',)
CAPTURE_DEMAND = dict(cpu=1, mem_gb=4, gpu=0)
CAPTURE_BOUNDS = dict(runtime_seconds=480, members=20000, regular_bytes=8 * 1024**3,
                      archive_members=20000 + len(CAPTURE_PUBLISHED_SOURCES) + 2,
                      supplemental_bytes=64 * 1024**2, metadata_bytes=64 * 1024**2,
                      archive_bytes=9 * 1024**3, profile_bytes=16 * 1024**2, chunk_bytes=1024**2)
CAPTURE_PHASES = ('host_inventory', 'image_inventory', 'mounted_runtime', 'closure_archive')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate runtime profile field: {key}')
        result[key] = value
    return result


def _absolute(value):
    if not isinstance(value, str) or not value.startswith('/') or value.startswith('//') or str(PurePosixPath(value)) != value:
        raise ValueError(f'noncanonical absolute runtime path: {value!r}')
    if '..' in PurePosixPath(value).parts or any(char in value for char in ',\n\r'):
        raise ValueError(f'unsafe runtime mount path: {value!r}')
    return Path(value)


def _sha(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('runtime digest must be lowercase SHA256')


def _identity(value):
    version = value['version']
    if not isinstance(version, list) or len(version) != 3 or any(type(n) is not int or n < 0 for n in version):
        raise ValueError('runtime version requires three nonnegative integers')
    executable = value['executable']
    _absolute(executable['path'])
    _absolute(executable['resolved_path'])
    _sha(executable['sha256'])
    if type(executable['bytes']) is not int or executable['bytes'] <= 0:
        raise ValueError('runtime executable byte length must be positive')
    if type(value['elf_machine']) is not int or value['elf_machine'] not in (62, 183):
        raise ValueError('unsupported root-test ELF architecture')


@dataclass(frozen=True)
class RuntimeProfile:
    path: Path
    sha256: str
    image: str
    primary: dict
    installed_reader: dict
    closure: dict


@dataclass(frozen=True)
class RuntimeExpansion:
    path: str
    target: str
    remaining: tuple[str, ...]


@dataclass(frozen=True)
class RuntimeResolved:
    spelling: str
    terminal: str
    chain: tuple[RuntimeExpansion, ...]


@dataclass(frozen=True)
class RuntimeWitnessedAbsent:
    spelling: str
    missing: str
    remaining: tuple[str, ...]
    chain: tuple[RuntimeExpansion, ...]
    witness: str


class RuntimeLookupAbsent(Exception):
    def __init__(self, witness):
        self.witness = witness
        super().__init__(witness)


@dataclass(frozen=True)
class ImageRuntimeAbsence:
    parent: str
    name: str
    before: tuple[int, ...]
    after: tuple[int, ...]
    names: tuple[str, ...]
    operation: str = 'lstat_at_nofollow'
    errno: str = 'ENOENT'
    authority: str = 'image'


@dataclass(frozen=True)
class HostRuntimeAbsence:
    parent: str
    name: str
    before: tuple[int, ...]
    after: tuple[int, ...]
    names: tuple[str, ...]
    checkpoint_sha256: str
    operation: str = 'complete_host_directory_enumeration'
    authority: str = 'host'


@dataclass(frozen=True)
class MountRuntimeAbsence:
    parent: str
    name: str
    names: tuple[str, ...]
    authority: str = 'mount'
    operation: str = 'mount_plan_children'


@dataclass(frozen=True)
class CompletedRuntimeNamespace:
    roots: tuple[dict, ...]
    native_library_roots: tuple[str, ...]
    components: dict
    directory_witnesses: dict
    link_outcomes: dict
    plan: dict
    consumed_budget: dict
    completion: dict
    reader_installation: dict | None = None
    namespace_states: dict = field(default_factory=dict)


def load_profile(path, source_root):
    source_root = source_root.resolve()
    path = Path(path).absolute()
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(source_root):
        raise ValueError('runtime profile must be a regular snapshot-local source input')
    path = path.resolve(strict=True)
    raw = path.read_bytes()
    value = json.loads(raw, object_pairs_hook=_unique)
    if value['schema'] != PROFILE_SCHEMA or value['closure']['schema'] != CLOSURE_SCHEMA:
        raise ValueError('unsupported root runtime profile or closure')
    image = value['image']
    if not isinstance(image, str) or not image.startswith('sha256:'):
        raise ValueError('runtime image must be an existing immutable local image ID')
    _sha(image.removeprefix('sha256:'))
    _identity(value['primary'])
    reader = value['installed_reader']
    _identity(reader)
    if reader['executable']['path'] != '/usr/bin/python3':
        raise ValueError('installed reader must retain its production interpreter path')
    if any(type(reader[n]) is not int or reader[n] <= 0 for n in ('uid', 'gid')):
        raise ValueError('installed reader requires real unprivileged UID/GID')
    if reader.get('link_target') is not None:
        _absolute(reader['link_target'])
    closure = value['closure']
    _sha(closure['sha256'])
    if digest(canonical({k: v for k, v in closure.items() if k != 'sha256'})) != closure['sha256']:
        raise ValueError('runtime closure digest differs')
    if closure['elf_machine'] != value['primary']['elf_machine'] or not closure['elf_roots']:
        raise ValueError('runtime closure requires explicit native architecture and stdlib seeds')
    for seed in closure['elf_roots']:
        _absolute(seed)
    combined = closure['audit']
    if (combined['schema'] != 'prismabuild.proof_runtime_closure_audit.v2'
            or closure['graph_policy'] != 'required-runtime-graph-v1'
            or combined['link_outcomes_sha256'] != digest(canonical(closure['link_outcomes']))
            or combined['completion_sha256'] != digest(canonical(closure['completion']))
            or combined['reader_installation_sha256'] != digest(canonical(closure['reader_installation']))
            or combined['namespace_states_sha256'] != digest(canonical(closure['namespace_states']))
            or set(combined['namespace_states']) != set(NAMESPACE_STATES)):
        raise ValueError('runtime closure requires its retained graph and completion audit')
    validate_runtime_native_audit(combined['native_audit'])
    roots = closure['roots']
    validate_runtime_roots(roots)
    for path_value in value['primary']['executable']['path'], value['primary']['executable']['resolved_path']:
        if not any(root['origin'] == 'host' and Path(path_value).is_relative_to(root['path']) for root in roots):
            raise ValueError('primary interpreter must come from the declared host mounts')
    expected_native = sorted({'/usr/lib/' + closure['completion'][origin + '_multiarch']
                              for origin in ('host', 'image')})
    if closure['native_library_roots'] != expected_native or any(
            not any(root['path'] == native and root['entries']['.']['kind'] == 'directory'
                    for root in roots) for native in expected_native):
        raise ValueError('runtime native library roots differ from selected MULTIARCH')
    if ({name: reader[name] for name in ('version', 'executable', 'elf_machine')}
            != closure['plan']['reader_identity']
            or reader.get('link_target') != closure['plan']['reader_target']):
        raise ValueError('runtime installed reader differs from the derived completion identity')
    if closure['reader_installation'] != runtime_reader_installation(closure):
        raise ValueError('runtime reader installation differs from retained namespace facts')
    for state in NAMESPACE_STATES:
        view = runtime_namespace_state(closure, state)
        if closure['namespace_states'][state] != runtime_namespace_state_record(view):
            raise ValueError('runtime namespace state differs from retained replay: ' + state)
        audit_link_outcomes(view)
        state_audit = combined['namespace_states'][state]
        validate_runtime_native_audit(state_audit['native_audit'])
        if state_audit['state_sha256'] != digest(canonical(closure['namespace_states'][state])):
            raise ValueError('runtime namespace audit belongs to another state')
    if combined['native_audit'] != combined['namespace_states']['before_installation']['native_audit']:
        raise ValueError('runtime primary native audit differs from its before-installation state')
    audit_observed_runtime_links(closure)
    return RuntimeProfile(path, digest(raw), image, value['primary'], reader, closure)


def validate_runtime_native_audit(audit):
    if audit['schema'] != 'prismabuild.proof_native_closure_audit.v3' or any(type(audit[name]) is not int or audit[name] < 0 for name in ('elf_files', 'symlinks')):
        raise ValueError('runtime closure requires its retained native dependency audit')
    _sha(audit['edges_sha256'])
    _sha(audit['symlinks_sha256'])
    for name in ('search_directories_sha256', 'default_directories_sha256', 'cache_sha256'):
        _sha(audit[name])


def validate_runtime_roots(roots):
    if not isinstance(roots, list) or not roots:
        raise ValueError('runtime closure requires complete captured roots')
    paths = []
    for root in roots:
        path_value = _absolute(root['path'])
        if root['origin'] not in ('host', 'image') or str(path_value) in BROAD_ROOTS:
            raise ValueError('runtime roots must be narrow host or image dependencies')
        if any(path_value.is_relative_to(other) or other.is_relative_to(path_value) for other in paths):
            raise ValueError('runtime roots must not overlap')
        paths.append(path_value)
        entries = root['entries']
        if not isinstance(entries, dict) or '.' not in entries:
            raise ValueError('runtime root inventory must include its own entry')
        if root['origin'] == 'host' and entries['.']['kind'] == 'symlink':
            raise ValueError('a host bind root cannot preserve a symlink inode')
        for relative, entry in entries.items():
            member = PurePosixPath(relative)
            if member.is_absolute() or '..' in member.parts or str(member) != relative:
                raise ValueError('runtime inventory members must stay inside their root')
            if entry['kind'] not in ('file', 'directory', 'symlink'):
                raise ValueError('runtime inventory contains an unsupported file kind')
            if type(entry['mode']) is not int or not 0 <= entry['mode'] <= 0o7777:
                raise ValueError('invalid runtime member mode')
            if entry['kind'] == 'file':
                _sha(entry['sha256'])
                if type(entry['bytes']) is not int or entry['bytes'] < 0:
                    raise ValueError('invalid runtime member byte length')
            if entry['kind'] == 'symlink' and not isinstance(entry['target'], str):
                raise ValueError('runtime symlink requires its exact target')


def runtime_stamp(info):
    return tuple(getattr(info, key) for key in (
        'st_dev', 'st_ino', 'st_mode', 'st_size', 'st_mtime_ns', 'st_ctime_ns'))


def runtime_charge(budget, *, members=0, regular_bytes=0):
    if budget is None:
        return
    budget['members'] += members
    budget['regular_bytes'] += regular_bytes
    if (budget['members'] > CAPTURE_BOUNDS['members']
            or budget['regular_bytes'] > CAPTURE_BOUNDS['regular_bytes']):
        raise ValueError(f'capture geometry exceeds its sealed limits: {budget}')
    if time.monotonic() > budget['deadline']:
        raise TimeoutError('capture inventory exceeded its finite deadline')


def runtime_capture_work(captured):
    entries = runtime_namespace_entries(captured)
    work = dict(members=sum(len(root['entries']) for root in captured['roots']),
                regular_bytes=sum(entry['bytes'] for entry in entries.values() if entry['kind'] == 'file'))
    for probe in captured.get('pre_inventory_probes', []):
        if set(probe) != {'path', 'header'}:
            raise ValueError('runtime provider header probe has an unsupported field set')
        path = str(_absolute(probe['path']))
        raw = bytes.fromhex(probe['header'])
        if len(raw) > 4 or raw.hex() != probe['header']:
            raise ValueError('runtime provider header probe exceeds its bounded read')
        entry = entries.get(path)
        if raw == b'\x7fELF' and (entry is None or entry['kind'] != 'file'):
            raise ValueError('runtime ELF header probe is outside its captured providers')
        if entry is not None and (entry['kind'] != 'file' or len(raw) != min(4, entry['bytes'])):
            raise ValueError('runtime provider header probe differs from its captured file size')
        work['members'] += 1
        work['regular_bytes'] += len(raw)
    work['members'] += len(captured.get('customization_probes', []))
    observations = captured.get('cache_observations', [])
    if observations:
        if ([item['boundary'] for item in observations] != ['listing', 'recheck']
                or any(root['origin'] != 'image' for root in captured['roots'])):
            raise ValueError('runtime cache observation boundaries differ from image capture')
        cache = captured['loader_cache']
        entry = entries[cache['path']]
        if entry['kind'] != 'file' or any(entry[name] != cache[name] for name in ('bytes', 'sha256')):
            raise ValueError('runtime cache observations differ from their retained cache file')
        for observation in observations:
            if (set(observation) != {'path', 'entry', 'boundary'}
                    or observation['path'] != cache['path'] or observation['entry'] != entry):
                raise ValueError('runtime cache observation differs from its inventoried file')
            work['members'] += 1
            work['regular_bytes'] += entry['bytes']
    return work


def inventory(path, *, budget=None, directory_observations=None):
    path = Path(path)
    members = {}
    pending = [path]
    directories = []
    while pending:
        current = pending.pop()
        before = current.lstat()
        runtime_charge(budget, members=1,
                       regular_bytes=before.st_size if stat.S_ISREG(before.st_mode) else 0)
        mode = stat.S_IMODE(before.st_mode)
        relative = str(current.relative_to(path))
        if stat.S_ISLNK(before.st_mode):
            entry = dict(kind='symlink', mode=mode, target=os.readlink(current))
        elif stat.S_ISDIR(before.st_mode):
            entry = dict(kind='directory', mode=mode)
            names = []
            directories.append((current, before, names))
            with os.scandir(current) as children:
                for child in children:
                    if budget is not None:
                        if time.monotonic() > budget['deadline']:
                            raise TimeoutError('capture enumeration exceeded its finite deadline')
                        if budget['members'] + len(pending) >= CAPTURE_BOUNDS['members']:
                            raise ValueError('capture geometry exceeds its sealed limits during directory enumeration')
                    pending.append(Path(child.path))
                    names.append(child.name)
        elif stat.S_ISREG(before.st_mode):
            hasher = hashlib.sha256()
            with current.open('rb') as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                    if budget is not None and time.monotonic() > budget['deadline']:
                        raise TimeoutError('capture file exceeded its finite deadline')
                    hasher.update(chunk)
            entry = dict(kind='file', mode=mode, bytes=before.st_size, sha256=hasher.hexdigest())
        else:
            raise ValueError(f'unsupported runtime member: {current}')
        after = current.lstat()
        if runtime_stamp(before) != runtime_stamp(after):
            raise ValueError(f'runtime member changed during capture: {current}')
        members[relative] = entry
    for current,before,names in directories:
        after = current.lstat()
        if runtime_stamp(before) != runtime_stamp(after):
            raise ValueError(f'runtime directory changed during capture: {current}')
        if directory_observations is not None:
            directory_observations[str(current)] = dict(
                before=list(runtime_stamp(before)), after=list(runtime_stamp(after)), names=sorted(names))
    return dict(sorted(members.items()))


def elf_requirements(path):
    with Path(path).open('rb') as handle:
        return read_runtime_elf(handle)


def runtime_elf_observation(path):
    try:
        return elf_requirements(path)
    except (ValueError, KeyError, struct.error, UnicodeError) as error:
        return dict(parse_refusal=str(error))


def read_runtime_elf(handle):
    header = handle.read(64)
    if header[:4] != b'\x7fELF':
        return None
    if len(header) != 64 or header[:6] != b'\x7fELF\x02\x01':
        raise ValueError('closure contains an unsupported ELF format')
    fields = struct.unpack('<16sHHIQQQIHHHHHH', header)
    if fields[1] not in (2, 3):
        return None
    if (header[6] != 1 or header[7] not in (0, 3) or header[8] != 0
            or fields[3] != 1 or fields[7] != 0 or fields[8] != 64):
        raise ValueError('runtime ELF has an unsupported ABI or header')
    offset, size, count = fields[5], fields[9], fields[10]
    if size != 56 or count > 4096:
        raise ValueError('unsupported ELF program header table')
    handle.seek(offset)
    segments = [struct.unpack('<IIQQQQQQ', handle.read(size)) for _ in range(count)]
    interpreters, dynamic, abi_kernels = [], [], []
    for segment in segments:
        if segment[0] not in (2, 3, 4):
            continue
        if segment[5] > 16 * 1024 * 1024:
            raise ValueError('ELF metadata exceeds the capture bound')
        handle.seek(segment[2])
        raw = handle.read(segment[5])
        if len(raw) != segment[5]:
            raise ValueError('truncated ELF metadata')
        if segment[0] == 3:
            interpreters.append(raw.rstrip(b'\0').decode())
        elif segment[0] == 4:
            cursor = 0
            while cursor < len(raw):
                names, description, kind = struct.unpack_from('<III', raw, cursor)
                cursor += 12
                name = raw[cursor:cursor + names]
                cursor += (names + 3) & ~3
                value = raw[cursor:cursor + description]
                cursor += (description + 3) & ~3
                if cursor > len(raw) or len(value) != description:
                    raise ValueError('truncated runtime ELF note')
                if name == b'GNU\0' and kind == 1:
                    if len(value) != 16:
                        raise ValueError('unsupported GNU ABI note')
                    operating_system, *version = struct.unpack('<IIII', value)
                    if operating_system != 0:
                        raise ValueError('runtime dependency does not target Linux')
                    abi_kernels.append(version)
        else:
            if len(raw) % 16:
                raise ValueError('invalid ELF dynamic table')
            for index in range(0, len(raw), 16):
                row = struct.unpack_from('<qQ', raw, index)
                if row[0] == 0:
                    break
                dynamic.append(row)
    if any(tag in (0x6ffffefa, 0x6ffffefb, 0x6ffffefc, 0x7ffffffd, 0x7fffffff)
           for tag,value in dynamic):
        raise ValueError('runtime ELF uses unsupported audit, filter or configuration loading')
    strings = {tag:value for tag,value in dynamic if tag in (5, 10)}
    values = {1:[], 15:[], 29:[]}
    if dynamic:
        address, length = strings[5], strings[10]
        mapped = [segment for segment in segments if segment[0] == 1
                  and segment[3] <= address and address + length <= segment[3] + segment[5]]
        if len(mapped) != 1 or length > 16 * 1024 * 1024:
            raise ValueError('unsupported ELF string table')
        handle.seek(mapped[0][2] + address - mapped[0][3])
        table = handle.read(length)
        for tag,value in dynamic:
            if tag in values:
                values[tag].append(table[value:table.index(b'\0', value)].decode())
    return dict(machine=fields[2], interpreters=interpreters,
                needed=values[1], rpath=values[15], runpath=values[29],
                nodeflib=any(tag == 0x6ffffffb and value & 0x800 for tag,value in dynamic),
                abi_kernels=abi_kernels)


def loader_default_directories(raw):
    rows = {}
    for line in raw.splitlines():
        match = re.fullmatch(r'path\.system_dirs\[(0x[0-9a-f]+|[0-9]+)\]=(.*)', line)
        if match:
            index = int(match[1], 0)
            if index in rows:
                raise ValueError('loader diagnostics repeat a default directory')
            rows[index] = str(_absolute(json.loads(match[2]).rstrip('/')))
    if not rows or sorted(rows) != list(range(len(rows))):
        raise ValueError('loader did not report a complete ordered default search path')
    return [rows[index] for index in range(len(rows))]


def loader_cache_entries(raw):
    lines = raw.splitlines()
    if not lines or not re.match(r'^[0-9]+ libs found in cache ', lines[0]):
        raise ValueError('unsupported loader cache listing')
    entries = []
    for line in lines[1:]:
        match = re.fullmatch(r'\s*(\S+)\s+\(([^)]+)\)\s+=>\s+(/.+)', line)
        if match:
            entries.append(dict(name=match[1], abi=match[2], path=str(_absolute(match[3]))))
    if len(entries) != int(lines[0].split()[0]):
        raise ValueError('loader cache listing is incomplete')
    return entries


def kernel_version(release):
    match = re.match(r'^([0-9]+)\.([0-9]+)(?:\.([0-9]+))?', release)
    if match is None:
        raise ValueError('runtime kernel release is unsupported')
    return tuple(int(value or 0) for value in match.groups())


def loader_kernel(raw):
    releases = [json.loads(line.removeprefix('uname.release='))
                for line in raw.splitlines() if line.startswith('uname.release=')]
    if len(releases) != 1 or not isinstance(releases[0], str):
        raise ValueError('loader diagnostics lack the actual kernel release')
    return releases[0], kernel_version(releases[0])


def require_profile_kernel(profile, observation):
    _, sealed = loader_kernel(profile.closure['loader']['diagnostics']['raw'])
    if kernel_version(observation['kernel']['release']) < sealed:
        raise ValueError('actual runtime kernel predates the sealed loader eligibility check')


def merge_runtime_roots(host_roots, image_roots):
    roots = list(host_roots)
    for captured in image_roots:
        path = Path(captured['path'])
        if any(path.is_relative_to(known['path']) for known in roots):
            continue
        if any(Path(known['path']).is_relative_to(path) for known in roots):
            raise ValueError('image runtime root would replace a primary host root')
        roots.append(captured)
    return roots


def walk_runtime_path(spelling, lookup):
    if (not isinstance(spelling, str) or not spelling.startswith('/')
            or spelling.startswith('//') or any(char in spelling for char in '\0\n\r')):
        raise ValueError('unsupported runtime lookup spelling: ' + repr(spelling))
    remaining = spelling.split('/')[1:]
    current = []
    chain = []
    root = lookup('/')
    if root['kind'] != 'directory':
        raise ValueError('runtime namespace root is not a directory')
    while remaining:
        component, *remaining = remaining
        if component in ('', '.'):
            continue
        if component == '..':
            if current:
                current.pop()
            continue
        path = '/' + '/'.join((*current, component))
        try:
            entry = lookup(path)
        except RuntimeLookupAbsent as absent:
            return RuntimeWitnessedAbsent(spelling, path, tuple(remaining), tuple(chain), absent.witness)
        if entry['kind'] == 'symlink':
            if len(chain) >= 64:
                raise ValueError('runtime closure has a symlink cycle')
            target = entry['target']
            if (not isinstance(target, str) or not target or target.startswith('//')
                    or any(char in target for char in '\0\n\r')):
                raise ValueError('unsupported runtime symlink target: ' + repr(target))
            chain.append(RuntimeExpansion(path, target, tuple(remaining)))
            if target.startswith('/'):
                current = []
            remaining = target.split('/') + remaining
            continue
        if remaining and entry['kind'] != 'directory':
            raise ValueError('runtime lookup traverses a non-directory component: ' + path)
        if entry['kind'] not in ('file', 'directory'):
            raise ValueError('runtime lookup contains an unsupported member: ' + path)
        current.append(component)
    return RuntimeResolved(spelling, '/' + '/'.join(current), tuple(chain))


def runtime_outcome_record(outcome):
    chain = [dict(path=step.path, target=step.target, remaining=list(step.remaining))
             for step in outcome.chain]
    if isinstance(outcome, RuntimeResolved):
        return dict(status='resolved', spelling=outcome.spelling, terminal=outcome.terminal, chain=chain)
    if isinstance(outcome, RuntimeWitnessedAbsent):
        return dict(status='witnessed_absent', spelling=outcome.spelling,
                    missing=outcome.missing, remaining=list(outcome.remaining), chain=chain,
                    witness=outcome.witness)
    raise ValueError('unsupported runtime path outcome')


def resolve_runtime_path(entries, path, components=None):
    def lookup(component):
        entry = entries.get(component)
        if entry is None and components is not None and component in components:
            entry = components[component]['entry']
            if entry['kind'] != 'directory':
                raise ValueError('runtime navigation component is not a directory: ' + component)
        if entry is None:
            raise ValueError('runtime symlink or ELF path escapes the sealed closure: ' + component)
        return entry
    return walk_runtime_path(path, lookup).terminal


def runtime_namespace_entries(closure):
    return {str(Path(root['path']) / name): entry for root in closure['roots']
            for name, entry in root['entries'].items()}


class RuntimeNamespaceIndex:
    """Indexes valid only for one operation over its unchanged namespace facts."""
    def __init__(self, closure, *, entries=None):
        self.closure = closure
        self.directory_listings = {}
        self.entries = runtime_namespace_entries(closure) if entries is None else entries
        self.children = self.immediate_names(self.entries)
        image_roots = [root for root in closure['roots'] if root['origin'] == 'image']
        image_roots += closure.get('completion', {}).get('base_image_roots', [])
        self.image_children = self.immediate_names(runtime_namespace_entries(dict(roots=image_roots)))
        for path, component in closure.get('components', {}).items():
            parent, _, name = path.rpartition('/')
            if not name:
                continue
            self.children.setdefault(parent or '/', set()).add(name)
            if component['origin'] == 'image':
                self.image_children.setdefault(parent or '/', set()).add(name)
        self._before = None
        for witness in closure.get('directory_witnesses', {}).values():
            if witness.get('authority') in ('host', 'image') and witness.get('operation') != 'namespace_directory_expectation':
                self.check_listing(witness['parent'], witness, witness['authority'])
        for parent, observation in closure.get('completion', {}).get('search_directory_observations', {}).items():
            self.check_listing(parent, observation, runtime_path_authority(closure, parent))

    def check_listing(self, parent, observed, authority):
        before, after, names = observed['before'], observed['after'], observed['names']
        if (not isinstance(before, (tuple, list)) or len(before) != 6
                or any(type(value) is not int for value in before) or before != after
                or not stat.S_ISDIR(before[2]) or not isinstance(names, (tuple, list))
                or any(not isinstance(value, str) or value in ('', '.', '..') or '/' in value or '\0' in value for value in names)
                or list(names) != sorted(set(names))):
            raise ValueError('runtime directory listing lacks stable complete observations')
        if authority == 'image' and not self.image_children.get(parent, set()).issubset(names):
            raise ValueError('runtime absence listing omits a retained child')
        key = (parent, tuple(before))
        previous = self.directory_listings.setdefault(key, list(names))
        if previous != list(names):
            raise ValueError('conflicting runtime directory listings for the same parent stamp')
        completion = self.closure.get('completion', {})
        basis = completion.get(authority + '_directory_observations', {}).get(parent)
        stamp = self.closure.get('components', {}).get(parent, {}).get('stamp')
        if ((basis is not None and (list(before) != basis['before'] or list(names) != basis['names']))
                or (stamp is not None and list(before) != list(stamp))):
            if authority == 'host':
                raise ValueError('host runtime absence differs from its checkpoint directory observation')
            raise ValueError('runtime directory listing differs from its retained parent stamp or listing')

    @staticmethod
    def immediate_names(entries):
        children = {}
        for path in entries:
            parent, _, name = path.rpartition('/')
            children.setdefault(parent or '/', set()).add(name)
        return children

    def before_index(self):
        if self._before is None:
            self._before = RuntimeNamespaceIndex(self.closure['_before_namespace'])
        return self._before


def runtime_mount_ancestor(closure, path, entry=None):
    target = Path(path)
    roots = [Path(root['path']) for root in closure['roots'] if root['origin'] == 'host']
    if any(target.is_relative_to(root) for root in roots):
        return None
    children = sorted({root.relative_to(target).parts[0] for root in roots
                       if root.is_relative_to(target)})
    if not children:
        return None
    if entry is not None and entry['kind'] != 'directory':
        raise ValueError('image mount-plan ancestor is not a directory: ' + path)
    return children


def runtime_mount_component(closure, path):
    children = runtime_mount_ancestor(closure, path)
    image_roots = [root for root in closure['roots'] if root['origin'] == 'image']
    image_roots += closure.get('completion', {}).get('base_image_roots', [])
    if children is None or any(Path(root['path']).is_relative_to(path) for root in image_roots):
        raise ValueError('mount-created component conflicts with retained image or mount plan: ' + path)
    return dict(origin='mount', entry=dict(kind='directory'), names=children)


def runtime_path_authority(closure, path):
    if any(root['origin'] == 'host' and Path(path).is_relative_to(root['path']) for root in closure['roots']):
        return 'host'
    components = closure.get('components', {})
    if any(components.get(candidate, {}).get('origin') == 'mount'
           for candidate in (path, str(Path(path).parent))):
        return 'mount'
    return 'image'


def runtime_directory_expectation(closure, key, state):
    witness = closure['directory_witnesses'][key]
    if witness['authority'] != 'image':
        return witness
    names = set(witness['names']) | set(runtime_mount_ancestor(closure, witness['parent']) or ())
    installation = closure.get('reader_installation')
    if (state == 'after_installation' and installation is not None
            and witness['parent'] == str(Path(installation['path']).parent)):
        if key == installation['absent_before']['witness']:
            raise ValueError('reader creation absence is inactive after installation')
        names.add(Path(installation['path']).name)
    if sorted(names) == list(witness['names']):
        return witness
    expected = dict(authority='image', operation='namespace_directory_expectation',
                    parent=witness['parent'], name=witness['name'], basis=key,
                    state=state, names=sorted(names))
    if installation is not None:
        expected['installation_sha256'] = digest(canonical(installation))
    return expected


def runtime_absence_witness(closure, key, *, index=None):
    witness = closure['directory_witnesses'][key]
    if digest(canonical(witness)) != key:
        raise ValueError('runtime absence witness digest differs')
    parent = str(_absolute(witness['parent']))
    name = witness['name']
    if not isinstance(name, str) or name in ('', '.', '..') or '/' in name or '\0' in name:
        raise ValueError('runtime absence witness has an invalid child name')
    if witness.get('operation') == 'namespace_directory_expectation':
        state = closure.get('_namespace_state')
        if state not in NAMESPACE_STATES or witness['state'] != state:
            raise ValueError('derived runtime absence belongs to another namespace state')
        if index is None:
            index = RuntimeNamespaceIndex(closure)
        runtime_absence_witness(closure['_before_namespace'], witness['basis'], index=index.before_index())
        expected = runtime_directory_expectation(closure['_before_namespace'], witness['basis'], state)
        if canonical(witness) != canonical(expected) or name in expected['names']:
            raise ValueError('derived runtime absence differs from its composed namespace directory')
        return witness
    if witness.get('authority') == 'mount':
        expected = runtime_mount_component(closure, parent)
        expected_witness = asdict(MountRuntimeAbsence(parent, name, tuple(expected['names'])))
        if (closure['components'].get(parent) != expected or name in expected['names']
                or canonical(witness) != canonical(expected_witness)
                or runtime_path_authority(closure, str(Path(parent) / name)) != 'mount'):
            raise ValueError('mount absence listing differs from its derived children')
        return witness
    before, after, names = witness['before'], witness['after'], witness['names']
    if (not isinstance(before, (list, tuple)) or len(before) != 6
            or any(type(value) is not int for value in before)
            or list(before) != list(after) or not stat.S_ISDIR(before[2])
            or not isinstance(names, (list, tuple)) or list(names) != sorted(set(names))
            or any(not isinstance(value, str) or value in ('', '.', '..')
                   or '/' in value or '\0' in value for value in names)
            or name in names):
        raise ValueError('runtime absence lacks a stable complete parent enumeration')
    missing = str(Path(parent) / name)
    authority = runtime_path_authority(closure, missing)
    if witness.get('authority') != authority:
        raise ValueError('runtime absence witness has the wrong mount authority')
    if authority == 'image':
        if witness.get('operation') != 'lstat_at_nofollow' or witness.get('errno') != 'ENOENT':
            raise ValueError('image runtime absence lacks its exact no-follow ENOENT observation')
    elif (witness.get('operation') != 'complete_host_directory_enumeration'
            or witness.get('checkpoint_sha256') != closure['completion']['host_checkpoint']['sha256']):
        raise ValueError('host runtime absence differs from its durable checkpoint')
    if authority == 'host':
        observed = closure['completion']['host_directory_observations'].get(parent)
        if observed is None or any(list(witness[field]) != observed[field]
                                   for field in ('before', 'after', 'names')):
            raise ValueError('host runtime absence differs from its checkpoint directory observation')
    if index is None:
        index = RuntimeNamespaceIndex(closure)
    entries = index.entries
    parent_entry = entries.get(parent, closure.get('components', {}).get(parent, {}).get('entry'))
    if parent_entry is None or parent_entry['kind'] != 'directory':
        raise ValueError('runtime absence parent is not a retained directory')
    if parent_entry['mode'] != stat.S_IMODE(before[2]):
        raise ValueError('runtime absence parent mode differs from its retained directory')
    children = index.image_children if authority == 'image' else index.children
    if not children.get(parent, set()).issubset(names):
        raise ValueError('runtime absence listing omits a retained child')
    if authority == 'host' and list(names) != sorted(index.children.get(parent, ())):
        raise ValueError('host runtime absence listing differs from its complete inventory')
    return witness


def runtime_replay_lookup(closure, *, index=None):
    if index is None:
        index = RuntimeNamespaceIndex(closure)
    entries = index.entries
    for path, component in closure['components'].items():
        if component['origin'] == 'mount' and component != runtime_mount_component(closure, path):
            raise ValueError('mount-created component listing differs from its derived children: ' + path)
    witnesses = {}
    for key in closure['directory_witnesses']:
        witness = runtime_absence_witness(closure, key, index=index)
        missing = str(Path(witness['parent']) / witness['name'])
        if missing in witnesses:
            raise ValueError('runtime absence has duplicate witnesses')
        witnesses[missing] = key
    def lookup(component):
        entry = entries.get(component)
        if entry is not None:
            runtime_mount_ancestor(closure, component, entry)
            return entry
        navigation = closure['components'].get(component)
        if navigation is not None:
            if (navigation['origin'] != runtime_path_authority(closure, component)
                    or navigation['entry']['kind'] != 'directory'):
                raise ValueError('runtime navigation component has invalid authority or kind')
            runtime_mount_ancestor(closure, component, navigation['entry'])
            if 'stamp' in navigation:
                stamp = navigation['stamp']
                if (len(stamp) != 6 or any(type(value) is not int for value in stamp)
                        or not stat.S_ISDIR(stamp[2])
                        or stat.S_IMODE(stamp[2]) != navigation['entry']['mode']):
                    raise ValueError('runtime navigation component has an invalid stable stamp')
            return navigation['entry']
        if component in witnesses:
            raise RuntimeLookupAbsent(witnesses[component])
        raise ValueError('runtime symlink or ELF path escapes the sealed closure: ' + component)
    return lookup


def replay_runtime_path(closure, path):
    return walk_runtime_path(path, runtime_replay_lookup(closure))


def runtime_reader_installation(closure):
    path = '/usr/bin/python3'
    outcome = replay_runtime_path(closure, path)
    if isinstance(outcome, RuntimeResolved):
        expected = closure['plan']['reader_identity']['executable']
        entry = runtime_namespace_entries(closure).get(outcome.terminal)
        if (outcome.terminal != expected['resolved_path'] or entry is None or entry['kind'] != 'file'
                or any(entry[name] != expected[name] for name in ('bytes', 'sha256'))):
            raise ValueError('existing runtime reader differs from its selected identity')
        return None
    witness = runtime_absence_witness(closure, outcome.witness)
    if (outcome.missing != path or outcome.remaining or witness['authority'] != 'image'
            or runtime_mount_ancestor(closure, path) is not None
            or witness.get('operation') != 'lstat_at_nofollow' or witness.get('errno') != 'ENOENT'):
        raise ValueError('runtime reader creation requires exact image-owned leaf ENOENT')
    target = closure['plan']['reader_target']
    resolved = replay_runtime_path(closure, target)
    expected = closure['plan']['reader_identity']['executable']
    entry = runtime_namespace_entries(closure).get(resolved.terminal) if isinstance(resolved, RuntimeResolved) else None
    if (not isinstance(resolved, RuntimeResolved) or resolved.terminal != expected['resolved_path']
            or entry is None or entry['kind'] != 'file'
            or any(entry[name] != expected[name] for name in ('bytes', 'sha256'))):
        raise ValueError('runtime reader insertion target differs from its selected identity')
    return dict(path=path, target=target, absent_before=runtime_outcome_record(outcome),
                entry=dict(kind='symlink', mode=0o777, target=target),
                expected_custody=dict(uid=0, gid=0, mode=0o777))


def runtime_namespace_state(closure, state):
    if state not in NAMESPACE_STATES:
        raise ValueError('unsupported runtime namespace state: ' + state)
    view = dict(closure)
    view['_namespace_state'] = state
    view['_before_namespace'] = closure
    view['roots'] = [dict(root, entries={name: dict(entry) for name, entry in root['entries'].items()})
                     for root in closure['roots']]
    view['components'] = {path: dict(component) for path, component in closure['components'].items()}
    view['directory_witnesses'] = {}
    view['plan'] = dict(closure['plan'])
    installation = closure.get('reader_installation')
    required = set(closure['plan']['required_spellings'])
    for key, witness in closure['directory_witnesses'].items():
        if (state == 'after_installation' and installation is not None
                and key == installation['absent_before']['witness']):
            continue
        expected = runtime_directory_expectation(closure, key, state)
        view['directory_witnesses'][digest(canonical(expected))] = expected
    if installation is not None:
        path = installation['path']
        if state == 'before_installation':
            required.remove(path)
        else:
            for root in view['roots']:
                if Path(path).is_relative_to(root['path']):
                    relative = str(Path(path).relative_to(root['path']))
                    if relative in root['entries']:
                        raise ValueError('runtime reader insertion would replace a retained member')
                    root['entries'][relative] = dict(installation['entry'])
                    break
            else:
                view['roots'].append(dict(origin='image', path=path, entries={'.': dict(installation['entry'])}))
    view['plan']['required_spellings'] = sorted(required)
    view['link_outcomes'] = dict(closure.get('namespace_states', {}).get(state, {}).get('link_outcomes', {}))
    return view


def runtime_namespace_state_record(view):
    index = RuntimeNamespaceIndex(view)
    lookup = runtime_replay_lookup(view, index=index)
    outcomes = {path: runtime_outcome_record(walk_runtime_path(path, lookup))
                for path, entry in index.entries.items() if entry['kind'] == 'symlink'}
    return dict(link_outcomes=dict(sorted(outcomes.items())),
                required_spellings=view['plan']['required_spellings'],
                directory_witnesses_sha256=digest(canonical(view['directory_witnesses'])))


def audit_observed_runtime_links(closure):
    observed = runtime_namespace_state(closure, 'before_installation')
    observed['directory_witnesses'] = closure['directory_witnesses']
    observed['link_outcomes'] = closure['link_outcomes']
    return audit_link_outcomes(observed)


def audit_link_outcomes(closure):
    index = RuntimeNamespaceIndex(closure)
    entries = index.entries
    links = {path for path, entry in entries.items() if entry['kind'] == 'symlink'}
    if set(closure['link_outcomes']) != links:
        raise ValueError('runtime link outcome roster differs from its declared symlinks')
    aliases = {}
    lookup = runtime_replay_lookup(closure, index=index)
    for path in sorted(links):
        outcome = walk_runtime_path(path, lookup)
        if runtime_outcome_record(outcome) != closure['link_outcomes'][path]:
            raise ValueError('runtime link outcome differs from retained replay: ' + path)
        if isinstance(outcome, RuntimeResolved):
            aliases[path] = outcome.terminal
        elif not any(Path(path).is_relative_to(root) for root in closure['native_library_roots']):
            raise ValueError('witnessed runtime absence is outside the native library root: ' + path)
    for path in closure['plan']['required_spellings']:
        outcome = walk_runtime_path(path, lookup)
        if not isinstance(outcome, RuntimeResolved):
            raise ValueError('required runtime path is witnessed absent: ' + path)
        aliases[path] = outcome.terminal
    return aliases


def runtime_customization_selection(stdlib, selected_paths, lookup):
    """Select only root-owned regular terminals of the two stdlib links."""
    observed, roots = {}, []
    def observed_lookup(path):
        observed[path] = lookup(path)
        return observed[path]
    for name in ('sitecustomize.py', 'usercustomize.py'):
        link = str(Path(stdlib) / name)
        try:
            if observed_lookup(link)['kind'] != 'symlink':
                continue
            outcome = walk_runtime_path(link, observed_lookup)
        except FileNotFoundError:
            continue
        target = outcome.terminal
        entry = observed[target]
        if (entry['kind'] == 'file' and entry['uid'] == 0
                and not any(Path(target).is_relative_to(path) for path in selected_paths)):
            roots.append(dict(link=link, target=target))
    return roots


def runtime_host_root_selection(captured):
    """Rederive every admitted host root, including exact customization files."""
    entries = runtime_namespace_entries(captured)
    executable = captured['observation']['executable']
    selected = [captured['stdlib'], *captured['native_library_roots'],
                executable['resolved_path'], str(Path(executable['path']).parent),
                str(Path(captured['observation']['prefix']) / 'pyvenv.cfg')]
    headers = {probe['path']: probe['header'] for probe in captured.get('pre_inventory_probes', [])}
    for path, provider in captured['providers'].items():
        entry = entries.get(path)
        if (entry is None or entry['kind'] != 'file' or headers.get(path) != '7f454c46'
                or provider != dict(resolved_path=path, bytes=entry['bytes'], sha256=entry['sha256'])):
            raise ValueError('host root selection provider differs from its observed ELF header: ' + path)
        selected.append(path)
    selected = [str(_absolute(path)) for path in selected]
    observed = {}
    for probe in captured['customization_probes']:
        path = str(_absolute(probe['path']))
        entry = probe['entry']
        if entry is None:
            if set(probe) != {'path', 'entry'}:
                raise ValueError('host customization absence probe has unsupported fields')
        else:
            if (set(probe) != {'path', 'entry', 'uid', 'before', 'after'}
                    or probe['before'] != probe['after'] or len(probe['before']) != 6
                    or type(probe['uid']) is not int or probe['uid'] < 0):
                raise ValueError('host customization selection lacks stable custody observations')
            mode = probe['before'][2]
            kind = ('directory' if stat.S_ISDIR(mode) else 'symlink' if stat.S_ISLNK(mode)
                    else 'file' if stat.S_ISREG(mode) else None)
            fields = {'kind', 'mode'} | ({'target'} if kind == 'symlink' else {'bytes'} if kind == 'file' else set())
            if (kind is None or set(entry) != fields or entry['kind'] != kind
                    or entry['mode'] != stat.S_IMODE(mode)
                    or kind == 'file' and entry['bytes'] != probe['before'][3]):
                raise ValueError('host customization component differs from its stable observation')
        known = entries.get(path)
        covered = any(Path(path).is_relative_to(root['path']) for root in captured['roots'])
        if (known is None and covered and entry is not None
                or known is not None and (entry is None or any(known.get(name) != value for name, value in entry.items()))):
            raise ValueError('host customization observation differs from host inventory: ' + path)
        if path in observed and observed[path] != probe:
            raise ValueError('host customization component changed between observations: ' + path)
        observed[path] = probe
    def lookup(path):
        probe = observed.get(path)
        if probe is None:
            raise ValueError('host customization selection lacks a component observation: ' + path)
        if probe['entry'] is None:
            raise FileNotFoundError(path)
        return dict(probe['entry'], uid=probe['uid'])
    derived = runtime_customization_selection(captured['stdlib'], selected, lookup)
    if any(entries.get(item['target'], {}).get('kind') != 'file' for item in derived):
        raise ValueError('host customization target is outside its retained file inventory')
    if captured['customization_roots'] != derived:
        raise ValueError('host customization roots differ from independently derived selection')
    selected.extend(item['target'] for item in derived)
    expected = []
    for path in sorted(set(selected), key=lambda value: (len(Path(value).parts), value)):
        if not any(Path(path).is_relative_to(parent) for parent in expected):
            expected.append(path)
    if sorted(root['path'] for root in captured['roots']) != sorted(expected):
        raise ValueError('host roots differ from independently derived host root selection')
    return derived


def runtime_completion_plan(host, image):
    primary = host['observation']['executable']
    primary_spellings = {primary['path'], primary['resolved_path']}
    for root in host['roots']:
        if root['path'] == str(Path(primary['path']).parent):
            primary_spellings.update(str(Path(root['path']) / relative)
                                     for relative, entry in root['entries'].items()
                                     if entry['kind'] == 'symlink')
    pt_seeds = set()
    for origin, captured in (('host', host), ('image', image)):
        executable = captured['observation']['executable']
        entry = runtime_namespace_entries(captured).get(executable['resolved_path'])
        if (entry is None or entry['kind'] != 'file'
                or any(entry[name] != executable[name] for name in ('bytes', 'sha256'))):
            raise ValueError('runtime completion executable differs from its original inventory')
        metadata = captured['elf_index'][executable['resolved_path']]
        if (metadata is None or 'parse_refusal' in metadata
                or metadata['machine'] != captured['observation']['elf_machine']):
            raise ValueError('runtime completion executable is not its observed native ELF')
        if 'executable_binary' in captured:
            raw = base64.b64decode(captured['executable_binary'], validate=True)
            if len(raw) != executable['bytes'] or digest(raw) != executable['sha256']:
                raise ValueError('runtime completion executable bytes differ from their observation')
            if read_runtime_elf(io.BytesIO(raw)) != metadata:
                raise ValueError('runtime completion executable metadata differs from its observed bytes')
        pt_seeds.update((origin, path) for path in metadata['interpreters'])
        for path, metadata in captured['elf_index'].items():
            if (metadata is not None and 'parse_refusal' not in metadata
                    and any(Path(path).is_relative_to(root) for root in captured['elf_roots'])):
                pt_seeds.update((origin, path) for path in metadata['interpreters'])
    cache_tool = image['loader_cache']['tool']
    required = {cache_tool['path'], cache_tool['resolved_path'], *primary_spellings, '/usr/bin/python3', image['observation']['executable']['path'],
                image['observation']['executable']['resolved_path'], *(path for origin, path in pt_seeds)}
    reader_target = image['observation']['executable']['resolved_path']
    actual_reader = host['observation'] if reader_target == primary['resolved_path'] else image['observation']
    reader_identity = {name: actual_reader[name] for name in ('version', 'executable', 'elf_machine')}
    reader_identity['executable'] = dict(reader_identity['executable'], path='/usr/bin/python3')
    return dict(primary_spellings=sorted(primary_spellings),
                reader_spelling='/usr/bin/python3', reader_target=reader_target,
                reader_identity=reader_identity,
                pt_seeds=[dict(origin=origin, path=path) for origin, path in sorted(pt_seeds)],
                required_spellings=sorted(required),
                elf_roots=sorted(set(host['elf_roots'] + image['elf_roots'])))


def require_runtime_candidate_absence(closure, directory, needed, *, lookup=None):
    path = str(Path(directory) / needed)
    authority = runtime_path_authority(closure, path)
    if authority == 'host':
        observation = closure['completion']['host_directory_observations'].get(directory)
        entries = runtime_namespace_entries(closure)
        expected = sorted(Path(value).name for value in entries if str(Path(value).parent) == directory)
        if (observation is None or observation['before'] != observation['after']
                or len(observation['before']) != 6 or not stat.S_ISDIR(observation['before'][2])
                or observation['names'] != expected or needed in observation['names']):
            raise ValueError('missing candidate absence evidence: ' + path)
        return
    try:
        outcome = walk_runtime_path(path, lookup or runtime_replay_lookup(closure))
    except (KeyError, ValueError) as error:
        raise ValueError('missing candidate absence evidence: ' + path) from error
    if not isinstance(outcome, RuntimeWitnessedAbsent) or outcome.missing != path or outcome.remaining:
        raise ValueError('missing candidate absence evidence: ' + path)


def runtime_expand_search_paths(values, origin):
    def token(match):
        if (match[1] or match[2]) != 'ORIGIN':
            raise ValueError('runtime ELF uses an unsupported library search token')
        return origin
    result = []
    for value in values:
        for directory in value.split(':'):
            expanded = re.sub(r'\$(?:\{([^}]*)\}|([A-Za-z_][A-Za-z_0-9]*))', token, directory)
            if not expanded.startswith('/') or '$' in expanded:
                raise ValueError('runtime ELF uses an unsupported library search token')
            result.append(expanded)
    return result


def runtime_elf_search_directories(metadata_by_path, resolved):
    directories, own_directories = set(), {}
    origins = {}
    for alias, target in resolved.items():
        origins.setdefault(target, set()).add(str(Path(alias).parent))
    for path, metadata in metadata_by_path.items():
        own_directories[path] = set()
        for origin in {str(Path(path).parent), *origins.get(path, ())}:
            own_directories[path].update(runtime_expand_search_paths(
                metadata['runpath'] + metadata['rpath'], origin))
        directories.update(own_directories[path])
    return directories, own_directories


def runtime_loader_edges(metadata_by_path, resolved, cache, defaults, *,
                         directory_candidates, resolve_candidate, diagnosed_loader, executed_paths):
    """Exhaust the evidence union; provider existence can never skip a query."""
    directories, _ = runtime_elf_search_directories(metadata_by_path, resolved)
    origins = {}
    for alias, target in resolved.items():
        origins.setdefault(target, set()).add(str(Path(alias).parent))
    inherited = set()
    for path, metadata in metadata_by_path.items():
        for origin in {str(Path(path).parent), *origins.get(path, ())}:
            inherited.update(runtime_expand_search_paths(metadata['rpath'], origin))
    cache_by_name = {}
    for row in cache:
        cache_by_name.setdefault(row['name'], []).append(row['path'])
    queries = {}
    def search(directory, needed):
        key = (directory, needed)
        if key not in queries:
            queries[key] = directory_candidates(directory, needed)
        return queries[key]
    edges, unresolved = {}, []
    for path, metadata in list(metadata_by_path.items()):
        if path in executed_paths and len(metadata['interpreters']) != 1:
            raise ValueError('reader loading implementation cannot be bound to the diagnosed loader: ' + path)
        if metadata['nodeflib']:
            raise ValueError('runtime ELF candidate has an unsupported architecture, kernel ABI, visibility or NODEFLIB flag')
        targets = set()
        for interpreter in metadata['interpreters']:
            target = resolve_candidate(interpreter)
            if target != diagnosed_loader:
                raise ValueError('distinct runtime loader is not admitted: ' + interpreter)
            targets.add(target)
        contexts = set(inherited)
        for origin in {str(Path(path).parent), *origins.get(path, ())}:
            contexts.update(runtime_expand_search_paths(metadata['runpath'], origin))
        for needed in metadata['needed']:
            if '$' in needed or ('/' in needed and not needed.startswith('/')):
                raise ValueError('runtime ELF uses an unsupported dependency path')
            if needed.startswith('/'):
                targets.add(resolve_candidate(needed))
                continue
            candidates = set()
            for directory in dict.fromkeys([*sorted(contexts), *defaults]):
                candidates.update(search(directory, needed)[0])
            candidates.update(resolve_candidate(candidate) for candidate in cache_by_name.get(needed, ()))
            targets.update(candidates)
            if not candidates:
                unresolved.append('unresolved runtime ELF dependency: ' + path + ' -> ' + needed)
        edges[path] = sorted(targets)
    return edges, directories, unresolved


def runtime_hwcaps_spellings(directory, needed, directory_names):
    root = str(Path(directory) / 'glibc-hwcaps')
    names = directory_names(root, required=False)
    result = [str(Path(directory) / needed)]
    if names is not None:
        for child in names:
            subdirectory = str(Path(root) / child)
            directory_names(subdirectory, required=True)
            result.append(str(Path(subdirectory) / needed))
    return result


def require_modern_glibc(version):
    match = re.fullmatch(r'(\d+)\.(\d+)(?:\.(\d+))?', version) if isinstance(version, str) else None
    if match is None:
        raise ValueError('runtime requires observed GNU glibc 2.37 or newer')
    major_minor = tuple(map(int, match.group(1, 2)))
    if major_minor < (2, 37):
        raise ValueError('legacy hwcap and platform searches require glibc 2.37 or newer')
    return major_minor


def require_modern_loader_diagnostics(raw):
    fields = {name: [json.loads(line.split('=', 1)[1]) for line in raw.splitlines()
                     if line.startswith('version.' + name + '=')]
              for name in ('version', 'release')}
    if len(fields['version']) != 1:
        raise ValueError('mounted loader lacks its observed glibc version')
    if fields['release'] != ['stable']:
        raise ValueError('mounted loader requires an observed stable release build')
    version, = fields['version']
    require_modern_glibc(version)
    return version


def require_runtime_loader_observations(before, after):
    if (loader_default_directories(before['raw']) != loader_default_directories(after['raw'])
            or loader_kernel(before['raw']) != loader_kernel(after['raw'])
            or require_modern_loader_diagnostics(before['raw']) != require_modern_loader_diagnostics(after['raw'])):
        raise ValueError('pre-completion mounted loader diagnostics changed after completion')


def runtime_search_directory_mode(roots, directory):
    path = Path(directory)
    if any(path.is_relative_to(root['path']) and root['entries']['.']['kind'] == 'directory'
           for root in roots):
        return 'complete'
    if any(Path(root['path']) != path and Path(root['path']).is_relative_to(path) for root in roots):
        return 'direct'
    return None


def runtime_candidate_name_index(entries):
    result = {}
    for path, entry in entries.items():
        if entry['kind'] in ('file', 'symlink'):
            result.setdefault(Path(path).name, []).append(path)
    return result


def runtime_directory_candidate_spellings(entries, directory, needed, *, direct=False, by_name=None):
    if direct:
        path = str(Path(directory) / needed)
        return [path] if entries.get(path, {}).get('kind') in ('file', 'symlink') else []
    candidates = (runtime_candidate_name_index(entries) if by_name is None else by_name).get(needed, ())
    return sorted(path for path in candidates if Path(path).is_relative_to(directory))

def require_runtime_search_visibility(entries, components, directory):
    for prefix in (Path(directory), *Path(directory).parents):
        entry = entries.get(str(prefix), components.get(str(prefix), {}).get('entry'))
        if (entry is not None and entry['kind'] == 'directory'
                and 'mode' in entry and not entry['mode'] & 0o001):
            raise ValueError('loader search directory is not public to the installed reader: ' + str(prefix))


def audit_native_closure(closure, byte_path):
    entries = runtime_namespace_entries(closure)
    resolved = audit_link_outcomes(closure)
    index = RuntimeNamespaceIndex(closure, entries=entries)
    lookup = runtime_replay_lookup(closure, index=index)
    def resolve(path):
        if path not in resolved:
            outcome = walk_runtime_path(path, lookup)
            if not isinstance(outcome, RuntimeResolved):
                raise ValueError('required native candidate is witnessed absent: ' + path)
            resolved[path] = outcome.terminal
        return resolved[path]

    links = closure['link_outcomes']
    pending = [path for path,entry in entries.items() if entry['kind'] == 'file'
               and any(Path(path).is_relative_to(seed) for seed in closure['elf_roots'])]
    loader = closure['loader']
    if loader['schema'] != 'prismabuild.proof_loader_search.v1':
        raise ValueError('unsupported runtime loader search contract')
    pre_diagnostics = closure['completion']['pre_completion_diagnostics']
    require_modern_loader_diagnostics(pre_diagnostics['raw'])
    require_runtime_loader_observations(pre_diagnostics, loader['diagnostics'])
    if (pre_diagnostics['executable'] != loader['diagnostics']['executable']
            or pre_diagnostics['command'] != [loader['diagnostics']['executable']['path'], '--list-diagnostics']):
        raise ValueError('pre-completion loader command differs from the captured host loader')
    defaults = loader_default_directories(loader['diagnostics']['raw'])
    _, kernel = loader_kernel(loader['diagnostics']['raw'])
    cache = loader_cache_entries(loader['cache']['raw_listing'])
    cache_file = entries[loader['cache']['path']]
    if any(cache_file[name] != loader['cache'][name] for name in ('bytes', 'sha256')):
        raise ValueError('runtime cache differs from its sealed file')
    for executable in (loader['cache']['tool'], loader['diagnostics']['executable']):
        path = resolve(executable['path'])
        if path != executable['resolved_path'] or any(entries[path][name] != executable[name] for name in ('bytes', 'sha256')):
            raise ValueError('loader observation command differs from its sealed executable')
    executed_paths = {resolve(closure['completion']['host_observation']['executable']['path']),
                      resolve(closure['plan']['reader_target'])}
    pending = sorted(set(pending) | executed_paths)
    metadata_by_path = {}
    required = set(executed_paths)
    candidates_by_name = runtime_candidate_name_index(entries)

    def directory_names(spelling, *, required):
        parent = resolve(str(Path(spelling).parent))
        expected = str(Path(parent) / Path(spelling).name)
        outcome = walk_runtime_path(spelling, lookup)
        if isinstance(outcome, RuntimeWitnessedAbsent):
            if required or outcome.missing != expected or outcome.remaining:
                raise ValueError('required hwcaps directory is not sealed: ' + spelling)
            return None
        directory = outcome.terminal
        entry = entries.get(directory, closure['components'].get(directory, {}).get('entry'))
        if entry is None or entry['kind'] != 'directory':
            raise ValueError('hwcaps component is not a directory: ' + spelling)
        require_runtime_search_visibility(entries, closure['components'], directory)
        if runtime_search_directory_mode(closure['roots'], directory) == 'complete':
            if entries.get(directory, {}).get('kind') != 'directory':
                raise ValueError('complete hwcaps inventory omits its directory')
            return sorted(index.children.get(directory, ()))
        component = closure['components'].get(directory, {})
        if component.get('origin') == 'mount':
            return component['names']
        observation = closure['completion']['search_directory_observations'].get(directory)
        if observation is None:
            raise ValueError('missing complete hwcaps directory listing: ' + directory)
        index.check_listing(directory, observation, runtime_path_authority(closure, directory))
        return sorted(set(observation['names']) | set(runtime_mount_ancestor(closure, directory) or ()))

    def directory_candidates(directory, needed):
        spelling = directory
        directory = resolve(directory)
        mode = runtime_search_directory_mode(closure['roots'], directory)
        if mode is None:
            raise ValueError('loader search directory is not completely inventoried: ' + directory)
        require_runtime_search_visibility(entries, closure['components'], directory)
        found = {sealed_candidate(str(Path(spelling) / Path(path).relative_to(directory)))
                 for path in runtime_directory_candidate_spellings(entries, directory, needed,
                     direct=mode == 'direct', by_name=candidates_by_name)}
        base = None
        for candidate in runtime_hwcaps_spellings(spelling, needed, directory_names):
            try:
                outcome = walk_runtime_path(candidate, lookup)
            except ValueError as error:
                raise ValueError('missing candidate absence evidence: ' + candidate) from error
            if isinstance(outcome, RuntimeWitnessedAbsent):
                parent = resolve(str(Path(candidate).parent))
                expected = str(Path(parent) / needed)
                if outcome.missing != expected or outcome.remaining:
                    raise ValueError('missing candidate absence evidence: ' + candidate)
                continue
            entry = entries.get(outcome.terminal, closure['components'].get(outcome.terminal, {}).get('entry'))
            if entry is not None and entry['kind'] == 'file':
                target = sealed_candidate(candidate)
                found.add(target)
                if candidate == str(Path(spelling) / needed):
                    base = target
        return found, base

    def sealed_candidate(path):
        target = resolve(path)
        if entries.get(target, {}).get('kind') != 'file':
            raise ValueError('runtime ELF candidate is not a sealed regular file')
        return target

    for _ in range(64):
        for path in pending:
            if path in metadata_by_path:
                continue
            metadata = elf_requirements(byte_path(path))
            if metadata is None:
                if path in required:
                    raise ValueError('runtime dependency is not a loadable ELF: ' + path)
                continue
            if (metadata['machine'] != closure['elf_machine'] or metadata['nodeflib']
                    or any(tuple(version) > kernel for version in metadata['abi_kernels'])
                    or not entries[path]['mode'] & 0o004):
                raise ValueError('runtime ELF candidate has an unsupported architecture, kernel ABI, visibility or NODEFLIB flag')
            metadata_by_path[path] = metadata
        origins_before = len(resolved)
        edges, directories, unresolved = runtime_loader_edges(metadata_by_path, resolved, cache, defaults,
            directory_candidates=directory_candidates, resolve_candidate=sealed_candidate,
            diagnosed_loader=loader['diagnostics']['executable']['resolved_path'], executed_paths=executed_paths)
        required = {target for targets in edges.values() for target in targets}
        pending = sorted(required - metadata_by_path.keys())
        if not pending and len(resolved) == origins_before:
            if unresolved:
                raise ValueError(unresolved[0])
            break
    else:
        raise ValueError('loader candidate graph did not converge inside its finite bound')
    return dict(schema='prismabuild.proof_native_closure_audit.v3',
                elf_files=len(edges), edges_sha256=digest(canonical(edges)),
                symlinks=len(links), symlinks_sha256=digest(canonical(links)),
                search_directories_sha256=digest(canonical(sorted(directories))),
                default_directories_sha256=digest(canonical(defaults)), cache_sha256=cache_file['sha256'])


def audit_runtime_completion(closure, byte_path, content_path=None):
    completion = closure['completion']
    captured = {}
    entries = runtime_namespace_entries(closure)
    index = RuntimeNamespaceIndex(closure, entries=entries)
    content = {}
    def retained_file(path, entry):
        if entry['sha256'] not in content:
            if content_path is not None:
                source = content_path(entry['sha256'])
            elif entries.get(path) == entry:
                source = byte_path(path)
            else:
                raise ValueError('shadowed runtime bytes require the retained content accessor: ' + path)
            actual = inventory(source)['.']
            if actual['kind'] != 'file' or any(actual[name] != entry[name] for name in ('bytes', 'sha256')):
                raise ValueError('retained runtime file bytes differ: ' + path)
            content[entry['sha256']] = (actual['bytes'], runtime_elf_observation(source))
        size, metadata = content[entry['sha256']]
        if size != entry['bytes']:
            raise ValueError('retained runtime file size differs: ' + path)
        return metadata
    validate_runtime_roots(closure['roots'])
    for origin in ('host', 'image'):
        facts = dict(observation=completion[origin + '_observation'],
                     roots=completion['base_' + origin + '_roots'],
                     elf_roots=completion[origin + '_elf_roots'],
                     elf_index=completion[origin + '_elf_index'],
                     pre_inventory_probes=completion.get(origin + '_pre_inventory_probes', []),
                     cache_observations=completion.get(origin + '_cache_observations', []))
        validate_runtime_roots(facts['roots'])
        if origin == 'host':
            facts.update(multiarch=completion['host_multiarch'],
                native_library_roots=['/usr/lib/' + completion['host_multiarch']], stdlib=completion['host_stdlib'],
                providers=completion['host_providers'],
                customization_roots=completion['host_customization_roots'],
                customization_probes=completion['host_customization_probes'])
            runtime_host_root_selection(facts)
        if origin == 'image':
            facts['loader_cache'] = completion['image_loader_cache']
            if closure['loader']['cache'] != facts['loader_cache']:
                raise ValueError('runtime image cache differs from its captured loader observation')
        parsed = {}
        for root in facts['roots']:
            if root['origin'] != origin:
                raise ValueError('runtime completion base inventory has the wrong origin')
            for relative, entry in root['entries'].items():
                path = str(Path(root['path']) / relative)
                if entry['kind'] != 'file':
                    continue
                parsed[path] = retained_file(path, entry)
        if parsed != facts['elf_index']:
            raise ValueError('runtime completion ELF index differs from retained bytes: ' + origin)
        runtime_capture_work(facts)
        base_entries = runtime_namespace_entries(facts)
        for probe in facts['pre_inventory_probes']:
            entry = base_entries.get(probe['path'])
            if entry is None:
                continue
            source = content_path(entry['sha256']) if content_path is not None else byte_path(probe['path'])
            with Path(source).open('rb') as handle:
                if handle.read(min(4, entry['bytes'])).hex() != probe['header']:
                    raise ValueError('runtime provider header probe differs from retained bytes')
        multiarch = completion[origin + '_multiarch']
        if not isinstance(multiarch, str) or not multiarch or '/' in multiarch:
            raise ValueError('runtime completion has an unsupported MULTIARCH')
        native = '/usr/lib/' + multiarch
        if not any(root['path'] == native and root['entries']['.']['kind'] == 'directory'
                   for root in facts['roots']):
            raise ValueError('runtime completion lacks its selected native library inventory')
        captured[origin] = facts
    if ({root['path']: root for root in closure['roots'] if root['origin'] == 'host'}
            != {root['path']: root for root in captured['host']['roots']}):
        raise ValueError('runtime completion changed its durable host inventory')
    base_roots = merge_runtime_roots(captured['host']['roots'], captured['image']['roots'])
    if any(root not in closure['roots'] for root in base_roots):
        raise ValueError('runtime completion changed a selected base inventory')
    for path, entry in entries.items():
        if entry['kind'] == 'file':
            retained_file(path, entry)
    expected_native = sorted({'/usr/lib/' + completion[origin + '_multiarch']
                              for origin in ('host', 'image')})
    if closure['native_library_roots'] != expected_native:
        raise ValueError('runtime native library roots differ from selected MULTIARCH')
    if any(not any(root['path'] == path and root['entries']['.']['kind'] == 'directory'
                       for root in closure['roots']) for path in expected_native):
        raise ValueError('runtime native library root is not completely inventoried')
    expected_plan = runtime_completion_plan(captured['host'], captured['image'])
    if closure['plan'] != expected_plan or closure['elf_roots'] != expected_plan['elf_roots']:
        raise ValueError('runtime completion plan differs from independently derived seeds')
    base = completion['base_consumed_budget']
    charges = completion['charges']
    if (set(base) != {'members', 'regular_bytes'}
            or any(type(base[name]) is not int or base[name] < 0 for name in base)):
        raise ValueError('runtime completion lacks its consumed base budget')
    totals = dict(base)
    witnessed = set()
    observed = {}
    for charge in charges:
        if charge['kind'] not in ('inventory', 'component', 'absence', 'host_absence', 'mount_absence', 'recheck', 'listing'):
            raise ValueError('runtime completion has an unsupported observation charge')
        _absolute(charge['path'])
        if charge['kind'] == 'inventory':
            facts = charge['entries']
            if '.' not in facts:
                raise ValueError('runtime completion inventory charge lacks its root')
            for relative, entry in facts.items():
                path = str(Path(charge['path']) / relative)
                if entries.get(path) != entry:
                    raise ValueError('runtime completion inventory charge differs from retained facts')
                observed[path] = entry
            expected_members = len(facts)
            expected_bytes = sum(entry['bytes'] for entry in facts.values() if entry['kind'] == 'file')
        elif charge['kind'] == 'listing':
            observation = completion['search_directory_observations'][charge['path']]
            if charge['observation'] != observation:
                raise ValueError('runtime search listing charge differs from its observation')
            index.check_listing(charge['path'], observation, runtime_path_authority(closure, charge['path']))
            expected_members, expected_bytes = 1 + len(observation['names']), 0
        elif charge['kind'] in ('component', 'recheck'):
            entry = charge['entry']
            if entry != entries.get(charge['path'], closure['components'].get(charge['path'], {}).get('entry')):
                raise ValueError('runtime completion component charge differs from retained facts')
            observed[charge['path']] = entry
            expected_members = 1
            expected_bytes = entry['bytes'] if entry['kind'] == 'file' else 0
        else:
            witness = runtime_absence_witness(closure, charge['witness'], index=index)
            if (str(Path(witness['parent']) / witness['name']) != charge['path']
                    or charge['kind'] != {'host': 'host_absence', 'image': 'absence',
                                          'mount': 'mount_absence'}[witness['authority']]):
                raise ValueError('runtime completion absence charge differs from its witness')
            witnessed.add(charge['witness'])
            expected_members = 1 + len(witness['names']) if witness['authority'] == 'image' else 1
            expected_bytes = 0
        if charge['members'] != expected_members or charge['regular_bytes'] != expected_bytes:
            raise ValueError('runtime completion charge counters differ from observed work')
        for name in totals:
            if type(charge[name]) is not int or charge[name] < 0:
                raise ValueError('runtime completion has an invalid observation charge')
            totals[name] += charge[name]
    if ({charge['path'] for charge in charges if charge['kind'] == 'listing'}
            != set(completion['search_directory_observations'])):
        raise ValueError('runtime completion budget omits a search directory listing')
    if witnessed != set(closure['directory_witnesses']):
        raise ValueError('runtime completion budget omits an absence observation')
    if any(path not in observed for path in closure['components']):
        raise ValueError('runtime completion budget omits a navigation component')
    base_entries = runtime_namespace_entries(dict(roots=base_roots))
    if any(path not in base_entries and path not in observed for path in entries):
        raise ValueError('runtime completion budget omits a new inventory member')
    if (totals != closure['consumed_budget'] or totals['members'] > CAPTURE_BOUNDS['members']
            or totals['regular_bytes'] > CAPTURE_BOUNDS['regular_bytes']):
        raise ValueError('runtime completion consumed budget differs from its retained work')
    initial_work = [runtime_capture_work(facts) for facts in captured.values()]
    if base != {name: sum(work[name] for work in initial_work) for name in ('members', 'regular_bytes')}:
        raise ValueError('runtime completion base budget omits shadowed observation work')
    return expected_plan


def audit_runtime_closure(closure, byte_path, *, content_path=None):
    if closure['schema'] != CLOSURE_SCHEMA or closure['graph_policy'] != 'required-runtime-graph-v1':
        raise ValueError('unsupported runtime closure graph contract')
    audit_runtime_completion(closure, byte_path, content_path)
    installation = runtime_reader_installation(closure)
    if closure['reader_installation'] != installation or set(closure['namespace_states']) != set(NAMESPACE_STATES):
        raise ValueError('runtime reader transition differs from its derived namespace states')
    views = {state: runtime_namespace_state(closure, state) for state in NAMESPACE_STATES}
    for state, view in views.items():
        if closure['namespace_states'][state] != runtime_namespace_state_record(view):
            raise ValueError('runtime namespace state differs from independently replayed facts: ' + state)
        audit_link_outcomes(view)
    audit_observed_runtime_links(closure)
    executable = closure['completion']['host_observation']['executable']
    primary = closure['completion']['host_elf_index'][executable['resolved_path']]
    if len(primary['interpreters']) != 1:
        raise ValueError('runtime primary requires one independently captured PT interpreter')
    interpreter, = primary['interpreters']
    outcome = replay_runtime_path(closure, interpreter)
    if (not isinstance(outcome, RuntimeResolved)
            or runtime_path_authority(closure, outcome.terminal) != 'host'
            or closure['loader']['diagnostics']['executable']['path'] != interpreter
            or closure['loader']['diagnostics']['executable']['resolved_path'] != outcome.terminal):
        raise ValueError('runtime primary PT chain differs from the captured host loader')
    states = {state: dict(native_audit=audit_native_closure(view, byte_path),
                          state_sha256=digest(canonical(closure['namespace_states'][state])))
              for state, view in views.items()}
    return dict(schema='prismabuild.proof_runtime_closure_audit.v2',
                native_audit=states['before_installation']['native_audit'], namespace_states=states,
                reader_installation_sha256=digest(canonical(installation)),
                namespace_states_sha256=digest(canonical(closure['namespace_states'])),
                link_outcomes_sha256=digest(canonical(closure['link_outcomes'])),
                completion_sha256=digest(canonical(closure['completion'])))


def verify_runtime_components(closure, *, image_root=Path('/'), origins=('host', 'image')):
    origins = set(origins) | ({'mount'} if 'image' in origins else set())
    for spelling, component in closure['components'].items():
        if component['origin'] not in origins:
            continue
        descriptor = os.open(image_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for name in PurePosixPath(spelling).parts[1:]:
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            before = os.fstat(descriptor)
            actual = dict(kind='directory', mode=stat.S_IMODE(before.st_mode))
            if component['origin'] == 'mount':
                if component != runtime_mount_component(closure, spelling):
                    raise ValueError('mount-created component differs from its derived children')
                with os.scandir(descriptor) as children:
                    names = sorted(child.name for child in children)
                if names != component['names']:
                    raise ValueError('mounted mount-created directory children differ: ' + spelling)
                actual = dict(kind='directory')
            if actual != component['entry'] or runtime_stamp(before) != runtime_stamp(os.fstat(descriptor)):
                raise ValueError('mounted runtime navigation component changed: ' + spelling)
        finally:
            os.close(descriptor)


def verify_runtime_negatives(closure, *, image_root=Path('/'), origins=('host', 'image')):
    image_root = Path(image_root)
    origins = set(origins) | ({'mount'} if 'image' in origins else set())
    verify_runtime_components(closure, image_root=image_root, origins=origins)
    entries = runtime_namespace_entries(closure)
    index = RuntimeNamespaceIndex(closure, entries=entries)
    lookup = runtime_replay_lookup(closure, index=index)
    for path, component in sorted(closure['components'].items(), key=lambda item: len(Path(item[0]).parts)):
        if component['origin'] not in origins:
            continue
        location = image_root / path.lstrip('/')
        before = location.lstat()
        if (not stat.S_ISDIR(before.st_mode)
                or ('mode' in component['entry'] and stat.S_IMODE(before.st_mode) != component['entry']['mode'])
                or runtime_stamp(before) != runtime_stamp(location.lstat())):
            raise ValueError('mounted runtime navigation component differs: ' + path)
    for parent, observation in closure.get('completion', {}).get('search_directory_observations', {}).items():
        if runtime_path_authority(closure, parent) not in origins:
            continue
        descriptor = os.open(image_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for name in PurePosixPath(parent).parts[1:]:
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            before = os.fstat(descriptor)
            with os.scandir(descriptor) as children:
                names = sorted(child.name for child in children)
            expected = sorted(set(observation['names']) | set(runtime_mount_ancestor(closure, parent) or ()))
            if (names != expected or stat.S_IMODE(before.st_mode) != stat.S_IMODE(observation['before'][2])
                    or runtime_stamp(before) != runtime_stamp(os.fstat(descriptor))):
                raise ValueError('mounted hwcaps directory listing changed: ' + parent)
        finally:
            os.close(descriptor)
    directory_error = None
    for key in closure['directory_witnesses']:
        witness = runtime_absence_witness(closure, key, index=index)
        if witness['authority'] not in origins:
            continue
        descriptor = os.open(image_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for component in PurePosixPath(witness['parent']).parts[1:]:
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
        except BaseException:
            os.close(descriptor)
            raise
        try:
            before = os.fstat(descriptor)
            recorded = (runtime_absence_witness(closure['_before_namespace'], witness['basis'], index=index.before_index())
                        if witness.get('operation') == 'namespace_directory_expectation' else witness)
            if (witness['authority'] != 'mount'
                    and stat.S_IMODE(before.st_mode) != stat.S_IMODE(recorded['before'][2])):
                raise ValueError('mounted runtime absence parent mode differs: ' + witness['parent'])
            try:
                os.stat(witness['name'], dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError as error:
                if error.errno != 2:
                    raise
            else:
                raise ValueError('mounted runtime absent target appeared: ' + witness['parent'] + '/' + witness['name'])
            with os.scandir(descriptor) as children:
                names = sorted(child.name for child in children)
            try:
                os.stat(witness['name'], dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError as error:
                if error.errno != 2:
                    raise
            else:
                raise ValueError('mounted runtime absent target appeared during verification')
            after = os.fstat(descriptor)
            if runtime_stamp(before) != runtime_stamp(after) or names != list(witness['names']):
                directory_error = directory_error or ('mounted runtime absence directory changed: ' + witness['parent'])
        finally:
            os.close(descriptor)
    if directory_error is not None:
        raise ValueError(directory_error)
    if not {'host', 'image'} <= origins:
        return
    for spelling, record in closure['link_outcomes'].items():
        if record['status'] != 'witnessed_absent':
            continue
        expected = walk_runtime_path(spelling, lookup)
        if runtime_path_authority(closure, expected.missing) not in origins:
            continue
        def mounted_lookup(path):
            location = image_root / path.lstrip('/')
            try:
                info = location.lstat()
            except FileNotFoundError as error:
                if error.errno != 2 or path != expected.missing:
                    raise
                raise RuntimeLookupAbsent(expected.witness) from error
            before = runtime_stamp(info)
            if stat.S_ISLNK(info.st_mode):
                actual = dict(kind='symlink', mode=stat.S_IMODE(info.st_mode), target=os.readlink(location))
            elif stat.S_ISDIR(info.st_mode):
                actual = dict(kind='directory', mode=stat.S_IMODE(info.st_mode))
            elif stat.S_ISREG(info.st_mode):
                actual = dict(kind='file', mode=stat.S_IMODE(info.st_mode))
            else:
                raise ValueError('mounted runtime lookup has an unsupported member: ' + path)
            if runtime_stamp(location.lstat()) != before:
                raise ValueError('mounted runtime lookup changed during verification: ' + path)
            retained = entries.get(path, closure['components'].get(path, {}).get('entry'))
            compared = ('kind',) if closure['components'].get(path, {}).get('origin') == 'mount' else actual
            if retained is None or any(actual[name] != retained.get(name) for name in compared):
                raise ValueError('mounted runtime negative expansion differs: ' + path)
            return actual
        if runtime_outcome_record(walk_runtime_path(spelling, mounted_lookup)) != record:
            raise ValueError('mounted runtime negative lookup differs: ' + spelling)


def runtime_closure_verification(closure, origins=('host', 'image')):
    maps = {root['path']: dict(origin=root['origin'], members=len(root['entries']),
                              entries_sha256=digest(canonical(root['entries'])))
            for root in closure['roots'] if root['origin'] in origins}
    return dict(verified=True, member_maps=maps, member_maps_sha256=digest(canonical(maps)))


def verify_closure(profile, origins=('host', 'image'), *, state='before_installation'):
    closure = runtime_namespace_state(profile.closure, state)
    for root in closure['roots']:
        if root['origin'] in origins and inventory(root['path']) != root['entries']:
            raise ValueError(f'runtime root inventory differs: {root["path"]}')
    verify_runtime_negatives(closure, origins=origins)
    return runtime_closure_verification(closure, origins)


def observe_reader_namespace(closure, state, *, image_root=Path('/')):
    if state not in NAMESPACE_STATES:
        raise ValueError('unsupported mounted reader namespace state')
    path = '/usr/bin/python3'
    parent = str(Path(path).parent)
    descriptor = os.open(image_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for name in PurePosixPath(parent).parts[1:]:
            following = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = following
        before = os.fstat(descriptor)
        name = Path(path).name
        try:
            info = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        except FileNotFoundError as error:
            if error.errno != 2:
                raise
            info = None
        entry = None
        if info is not None:
            if stat.S_ISLNK(info.st_mode):
                entry = dict(kind='symlink', mode=stat.S_IMODE(info.st_mode),
                             target=os.readlink(name, dir_fd=descriptor))
            elif stat.S_ISREG(info.st_mode):
                entry = inventory(Path('/proc/self/fd') / str(descriptor) / name)['.']
            else:
                raise ValueError('mounted reader path has an unsupported inode kind')
        names = []
        with os.scandir(descriptor) as children:
            for child in children:
                if len(names) >= CAPTURE_BOUNDS['members']:
                    raise ValueError('mounted reader parent enumeration exceeds its finite bound')
                names.append(child.name)
        try:
            later = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        except FileNotFoundError as error:
            if error.errno != 2:
                raise
            later = None
        after = os.fstat(descriptor)
        if ((info is None) != (later is None) or (info is not None and runtime_stamp(info) != runtime_stamp(later))
                or runtime_stamp(before) != runtime_stamp(after)):
            raise ValueError('mounted reader namespace changed during observation')
        return dict(state=state, path=path, parent=parent, before=list(runtime_stamp(before)),
                    after=list(runtime_stamp(after)), names=sorted(names), entry=entry,
                    uid=None if info is None else info.st_uid, gid=None if info is None else info.st_gid,
                    parent_uid=before.st_uid, parent_gid=before.st_gid, parent_mode=stat.S_IMODE(before.st_mode))
    finally:
        os.close(descriptor)


def verify_reader_namespace_transition(closure, before, after, custody):
    installation = closure['reader_installation']
    path = '/usr/bin/python3'
    parent = str(Path(path).parent)
    for state, observed in zip(NAMESPACE_STATES, (before, after)):
        stamp = observed['before']
        if (observed['state'] != state or observed['path'] != path or observed['parent'] != parent
                or len(stamp) != 6 or any(type(value) is not int for value in stamp)
                or stamp != observed['after'] or not stat.S_ISDIR(stamp[2])
                or observed['names'] != sorted(set(observed['names']))
                or stat.S_IMODE(stamp[2]) != observed['parent_mode']):
            raise ValueError('mounted reader namespace lacks its exact stable state observation')
    if (before['before'][:3] != after['before'][:3]
            or any(before[name] != after[name] for name in ('parent_uid', 'parent_gid', 'parent_mode'))):
        raise ValueError('mounted reader creation changed or replaced its parent directory')
    if installation is None:
        expected = runtime_namespace_entries(closure).get(path)
        if (before['entry'] != expected or after['entry'] != expected
                or before['names'] != after['names'] or custody['created_symlink'] is not False
                or custody['existed_before'] is not True):
            raise ValueError('existing mounted reader namespace changed unexpectedly')
    else:
        witness = runtime_absence_witness(closure, installation['absent_before']['witness'])
        initial_names = sorted(set(witness['names']) | set(runtime_mount_ancestor(closure, parent) or ()))
        expected_names = sorted([*before['names'], Path(path).name])
        expected_custody = installation['expected_custody']
        if (before['entry'] is not None or Path(path).name in before['names']
                or before['names'] != initial_names or after['names'] != expected_names
                or after['entry'] != installation['entry']
                or any(after[name] != expected_custody[name] for name in ('uid', 'gid'))
                or before['parent_uid'] != 0 or before['parent_mode'] & 0o022
                or custody['created_symlink'] is not True or custody['existed_before'] is not False):
            raise ValueError('mounted reader insertion differs from its authorized one-name delta or custody')
    if (after['uid'] != 0 or custody['path'] != path
            or any(custody[name] != after[name] for name in ('uid', 'gid'))
            or custody['mode'] != after['entry']['mode']
            or custody['target'] != after['entry'].get('target')):
        raise ValueError('mounted reader custody differs from its observed namespace entry')
    return True


def observe_runtime():
    preload = Path('/etc/ld.so.preload')
    try:
        preload_info = preload.lstat()
    except FileNotFoundError:
        preload_info = None
    if preload_info is not None:
        if not stat.S_ISREG(preload_info.st_mode):
            raise ValueError('runtime capture refuses a nonregular system-wide loader preload file')
        with preload.open('rb') as handle:
            raw_preload = handle.read(65537)
        if len(raw_preload) > 65536 or raw_preload.strip():
            raise ValueError('runtime capture refuses system-wide loader preloads')
    running = Path('/proc/self/exe').read_bytes()
    if running[:6] != b'\x7fELF\x02\x01':
        raise ValueError('root runtime must use a native little-endian ELF64 executable')
    executable = Path(sys.executable)
    if executable.read_bytes() != running:
        raise ValueError('runtime executable path differs from the running image')
    auxv = Path('/proc/self/auxv').read_bytes()
    if len(auxv) % 16 or len(auxv) > 65536:
        raise ValueError('runtime auxiliary vector is unsupported')
    secure, = [value for key,value in struct.iter_unpack('<QQ', auxv) if key == 23]
    return dict(version=list(sys.version_info[:3]),
        executable=dict(path=sys.executable, resolved_path=str(executable.resolve()),
                        bytes=len(running), sha256=digest(running)),
        elf_machine=int.from_bytes(running[18:20], 'little'),
        at_secure=secure,
        credentials=dict(uid=os.getuid(), euid=os.geteuid(), gid=os.getgid(), egid=os.getegid(), groups=os.getgroups()),
        affinity=sorted(os.sched_getaffinity(0)), native_threads={name: os.environ.get(name) for name in THREADS},
        environment={name:os.environ.get(name) for name in (
            'TMPDIR', 'PYTHONPATH', 'PYTHONDONTWRITEBYTECODE', 'PYTEST_DISABLE_PLUGIN_AUTOLOAD')},
        loader_environment={name:value for name,value in os.environ.items() if name.startswith('LD_') and value},
        python_flags={name:getattr(sys.flags, name) for name in (
            'no_site', 'isolated', 'dont_write_bytecode', 'ignore_environment')},
        kernel=platform.uname()._asdict(), prefix=sys.prefix, base_prefix=sys.base_prefix, sys_path=list(sys.path))


def require_identity(observed, expected):
    for name in ('version', 'executable', 'elf_machine'):
        if observed[name] != expected[name]:
            raise ValueError(f'actual runtime differs from declared {name}')
    machine = {62: 'x86_64', 183: 'aarch64'}[observed['elf_machine']]
    if observed['kernel']['machine'] != machine:
        raise ValueError('runtime ELF differs from actual kernel architecture')


def native_provider_paths(*, budget=None, probes=None):
    providers, seen = set(), set()
    for line in Path('/proc/self/maps').read_text().splitlines():
        parts = line.split(None, 5)
        if len(parts) != 6 or not parts[5].startswith('/'):
            continue
        path = Path(parts[5]).resolve()
        if str(path) in seen:
            continue
        seen.add(str(path))
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise ValueError('mapped runtime provider is not a regular file: ' + str(path))
        size = min(4, before.st_size)
        runtime_charge(budget, members=1, regular_bytes=size)
        with path.open('rb') as handle:
            header = handle.read(size)
        if len(header) != size or runtime_stamp(before) != runtime_stamp(path.lstat()):
            raise ValueError('mapped runtime provider changed during header observation: ' + str(path))
        if probes is not None:
            probes.append(dict(path=str(path), header=header.hex()))
        if header == b'\x7fELF':
            providers.add(str(path))
    return sorted(providers)


def native_providers():
    providers = {}
    for name in native_provider_paths():
        path = Path(name)
        actual = inventory(path)['.']
        providers[str(path)] = dict(resolved_path=str(path), bytes=actual['bytes'], sha256=actual['sha256'])
    return dict(sorted(providers.items()))


def mapped_native_providers(profile):
    declared = {}
    for root in profile.closure['roots']:
        for relative, entry in root['entries'].items():
            if entry['kind'] == 'file':
                declared[str(Path(root['path']) / relative)] = entry
    providers = native_providers()
    for path, actual in providers.items():
        expected = declared.get(path)
        if expected is None:
            raise ValueError(f'mapped native provider is outside the sealed closure: {path}')
        if actual['bytes'] != expected['bytes'] or actual['sha256'] != expected['sha256']:
            raise ValueError(f'mapped native provider bytes differ: {path}')
    return providers


def observe_reader(profile):
    expected = profile.installed_reader
    path = Path(expected['executable']['path'])
    existed = path.exists() or path.is_symlink()
    created = False
    installation = profile.closure['reader_installation']
    if (not existed) != (installation is not None):
        raise ValueError('mounted reader presence differs from its authorized installation state')
    if not existed:
        target = expected.get('link_target')
        if target is None or not Path(target).is_file():
            raise ValueError('declared image reader path and existing target are absent')
        path.symlink_to(target)
        created = True
    info = path.lstat()
    if info.st_uid != 0 or (not path.is_symlink() and info.st_mode & 0o022):
        raise ValueError('installed reader interpreter path is not in root custody')
    target = path.resolve(strict=True)
    for member in (target, *target.parents):
        custody = member.lstat()
        if custody.st_uid != 0 or custody.st_mode & 0o022 or stat.S_ISLNK(custody.st_mode):
            raise ValueError('installed reader target is outside immutable root custody')
    command = [str(path), '-I', str(Path(__file__).resolve()), '--observe', str(profile.path)]
    result = subprocess.run(command, user=expected['uid'], group=expected['gid'], extra_groups=[],
        env={'PATH': '/usr/bin:/bin', 'TMPDIR':'/home/rob/pbt17', **{name: '1' for name in THREADS}},
        cwd='/', capture_output=True, text=True, timeout=30, check=False)
    if result.returncode:
        raise ValueError('installed reader runtime observation failed: ' + result.stderr[-2000:])
    payload = json.loads(result.stdout, object_pairs_hook=_unique)
    observation = payload['observation']
    require_identity(observation, expected)
    require_profile_kernel(profile, observation)
    credentials = observation['credentials']
    if any(credentials[key] != expected[name] for key, name in [('uid','uid'), ('euid','uid'), ('gid','gid'), ('egid','gid')]) or credentials['groups']:
        raise ValueError('installed reader did not use its real unprivileged credentials')
    observation['link_custody'] = dict(path=str(path), existed_before=existed, created_symlink=created,
        target=os.readlink(path) if path.is_symlink() else None, uid=info.st_uid, gid=info.st_gid, mode=stat.S_IMODE(info.st_mode))
    return observation, payload['mapped_native_providers']


def source_digests(root, tests, profile):
    names = {*tests, 'tests/run_proof_container.py', 'tests/proof_container_runtime.py',
        'tests/capture_proof_runtime.py', 'tests/proof_capture_capacity.py',
        'tests/conftest.py', 'tools/fleet/pbtest_outcomes.py',
        'src/prismabuild/pbtest_collection.py', 'tests/proof_execution.json',
        'tests/receipt_execution.json', 'qualification_dependencies.json',
        'tools/fleet/bootstrap_client.py', 'tests/bootstrap_fixture.py',
        'tools/fleet/upgrade_client.py', 'src/prismabuild/digest_primitives.py',
        'src/prismabuild/generation_receipt.py', 'src/prismabuild/park_proof.py',
        str(profile.path.relative_to(root))}
    return {name: digest((root / name).read_bytes()) for name in sorted(names)}


def observe_container_roles(profile, expected_affinity):
    os.makedirs('/home/rob/pbt17', exist_ok=True)
    closure = verify_closure(profile)
    inner = observe_runtime()
    require_identity(inner, profile.primary)
    require_profile_kernel(profile, inner)
    if any(inner['credentials'][name] != 0 for name in ('uid', 'euid', 'gid', 'egid')):
        raise ValueError('real container UID/GID0 required')
    if inner['affinity'] != expected_affinity or inner['native_threads'] != {name: '1' for name in THREADS}:
        raise ValueError('container changed PB affinity or native thread bounds')
    if inner['python_flags']['no_site'] != 1:
        raise ValueError('primary container must disable unsealed site startup')
    if inner['loader_environment'] or inner['at_secure'] != 0:
        raise ValueError('primary container has an unsupported loader override')
    mapped = mapped_native_providers(profile)
    before = observe_reader_namespace(profile.closure, 'before_installation')
    reader, providers = observe_reader(profile)
    after = observe_reader_namespace(profile.closure, 'after_installation')
    verify_reader_namespace_transition(profile.closure, before, after, reader['link_custody'])
    if reader['loader_environment'] or reader['at_secure'] != 0:
        raise ValueError('installed reader has an unsupported loader override')
    if reader['affinity'] != expected_affinity or reader['native_threads'] != {name: '1' for name in THREADS}:
        raise ValueError('installed reader changed PB affinity or native bounds')
    after_verification = verify_closure(profile, state='after_installation')
    if profile.closure['reader_installation'] is not None:
        closure = dict(closure, namespace_states=dict(before_installation=before, after_installation=after),
                       after_verification=after_verification)
    return dict(inner=inner, installed_reader=reader, closure=closure,
                mapped_native_providers=dict(inner_before=mapped, installed_reader=providers))


def run(profile_path, tests, mode, outer, outer_closure, image, expected_affinity, output, host_output):
    root = Path('/source')
    profile = load_profile(profile_path, root)
    record = dict(schema=ATTESTATION_SCHEMA, mode=mode, complete=False,
        profile_sha256=profile.sha256, image=image, outer=outer, inner=None, installed_reader=None,
        closure=dict(sha256=profile.closure['sha256'], outer=outer_closure, inner=None, mapped_native_providers={}),
        source_sha256=source_digests(root, tests, profile), exitstatus=1, error=None, attestation_path=host_output)
    status = 1
    try:
        roles = observe_container_roles(profile, expected_affinity)
        record['inner'], record['installed_reader'] = roles['inner'], roles['installed_reader']
        record['closure']['inner'] = roles['closure']
        record['closure']['mapped_native_providers'] = roles['mapped_native_providers']
        os.makedirs('/home/rob/pbt17', exist_ok=True)
        owner = runpy.run_path('/source/tools/fleet/pbtest_outcomes.py')
        arguments = ['-q', '-rA', '-o', 'addopts=', '-p', 'no:cacheprovider',
                     '--basetemp=/var/lib/pb-proof-qualification', *tests]
        if mode == 'collection':
            arguments.insert(0, '--collect-only')
        status = int(owner['main'](arguments,
            collection_spec={'files': tests, 'roots': [[path, 'file'] for path in tests]},
            collection_source=(root / 'src/prismabuild/pbtest_collection.py').read_text()))
        verified = verify_closure(profile, state='after_installation')
        if profile.closure['reader_installation'] is not None:
            record['closure']['inner']['after_verification'] = verified
        else:
            record['closure']['inner'] = verified
        record['closure']['mapped_native_providers']['inner_after'] = mapped_native_providers(profile)
        record['complete'] = True
    except BaseException as exc:
        record['error'] = f'{type(exc).__name__}: {exc}'
        status = 1
    record['exitstatus'] = status
    Path(output).write_bytes(canonical(record) + b'\n')
    return status


if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] != '--observe':
        raise SystemExit('runtime helper accepts only the admitted reader observation route')
    profile = load_profile(sys.argv[2], Path(__file__).resolve().parents[1])
    print(json.dumps(dict(observation=observe_runtime(), mapped_native_providers=mapped_native_providers(profile)), sort_keys=True))
