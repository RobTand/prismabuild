"""Interpret a sealed root-test runtime profile inside its admitted container."""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import posixpath
import runpy
import stat
import struct
import subprocess
import sys


PROFILE_SCHEMA = 'prismabuild.proof_runtime_profile.v1'
CLOSURE_SCHEMA = 'prismabuild.proof_runtime_closure.v1'
ATTESTATION_SCHEMA = 'prismabuild.root_test_runtime.v1'
THREADS = ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS')
BROAD_ROOTS = {'/', '/usr', '/usr/bin', '/usr/lib', '/home', '/home/rob', '/etc', '/var', '/mnt'}


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
    audit = closure['native_audit']
    if audit['schema'] != 'prismabuild.proof_native_closure_audit.v1' or any(type(audit[name]) is not int or audit[name] < 0 for name in ('elf_files', 'symlinks')):
        raise ValueError('runtime closure requires its retained native dependency audit')
    _sha(audit['edges_sha256'])
    _sha(audit['symlinks_sha256'])
    roots = closure['roots']
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
    for path_value in value['primary']['executable']['path'], value['primary']['executable']['resolved_path']:
        if not any(root['origin'] == 'host' and Path(path_value).is_relative_to(root['path']) for root in roots):
            raise ValueError('primary interpreter must come from the declared host mounts')
    return RuntimeProfile(path, digest(raw), image, value['primary'], reader, closure)


def inventory(path):
    path = Path(path)
    members = {}
    pending = [path]
    directories = []
    fields = ('st_dev', 'st_ino', 'st_mode', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
    while pending:
        current = pending.pop()
        before = current.lstat()
        mode = stat.S_IMODE(before.st_mode)
        relative = str(current.relative_to(path))
        if stat.S_ISLNK(before.st_mode):
            entry = dict(kind='symlink', mode=mode, target=os.readlink(current))
        elif stat.S_ISDIR(before.st_mode):
            entry = dict(kind='directory', mode=mode)
            directories.append((current, before))
            with os.scandir(current) as children:
                pending.extend(Path(child.path) for child in children)
        elif stat.S_ISREG(before.st_mode):
            hasher = hashlib.sha256()
            with current.open('rb') as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                    hasher.update(chunk)
            entry = dict(kind='file', mode=mode, bytes=before.st_size, sha256=hasher.hexdigest())
        else:
            raise ValueError(f'unsupported runtime member: {current}')
        after = current.lstat()
        if any(getattr(before, key) != getattr(after, key) for key in fields):
            raise ValueError(f'runtime member changed during capture: {current}')
        members[relative] = entry
    for current,before in directories:
        after = current.lstat()
        if any(getattr(before, key) != getattr(after, key) for key in fields):
            raise ValueError(f'runtime directory changed during capture: {current}')
    return dict(sorted(members.items()))


def elf_requirements(path):
    with Path(path).open('rb') as handle:
        header = handle.read(64)
        if header[:4] != b'\x7fELF':
            return None
        if len(header) != 64 or header[:6] != b'\x7fELF\x02\x01':
            raise ValueError('closure contains an unsupported ELF format')
        fields = struct.unpack('<16sHHIQQQIHHHHHH', header)
        if fields[1] not in (2, 3):
            return None
        offset, size, count = fields[5], fields[9], fields[10]
        if size != 56 or count > 4096:
            raise ValueError('unsupported ELF program header table')
        handle.seek(offset)
        segments = [struct.unpack('<IIQQQQQQ', handle.read(size)) for _ in range(count)]
        interpreters, dynamic = [], []
        for segment in segments:
            if segment[0] not in (2, 3):
                continue
            if segment[5] > 16 * 1024 * 1024:
                raise ValueError('ELF metadata exceeds the capture bound')
            handle.seek(segment[2])
            raw = handle.read(segment[5])
            if len(raw) != segment[5]:
                raise ValueError('truncated ELF metadata')
            if segment[0] == 3:
                interpreters.append(raw.rstrip(b'\0').decode())
            else:
                if len(raw) % 16:
                    raise ValueError('invalid ELF dynamic table')
                for index in range(0, len(raw), 16):
                    row = struct.unpack_from('<qQ', raw, index)
                    if row[0] == 0:
                        break
                    dynamic.append(row)
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
                    needed=values[1], rpath=values[15], runpath=values[29])


def audit_native_closure(closure, byte_path):
    entries = {str(Path(root['path']) / name):entry for root in closure['roots']
               for name,entry in root['entries'].items()}

    def resolve(path):
        path = posixpath.normpath(path)
        for _ in range(64):
            parts = PurePosixPath(path).parts
            for index in range(1, len(parts)):
                prefix = str(PurePosixPath(*parts[:index + 1]))
                entry = entries.get(prefix)
                if entry is not None and entry['kind'] == 'symlink':
                    target = posixpath.join(posixpath.dirname(prefix), entry['target'])
                    path = posixpath.normpath(posixpath.join(target, *parts[index + 1:]))
                    break
            else:
                if path not in entries and not any(name.startswith(path + '/') for name in entries):
                    raise ValueError('runtime symlink or ELF path escapes the sealed closure: ' + path)
                return path
        raise ValueError('runtime closure has a symlink cycle')

    links = {path:resolve(path) for path,entry in entries.items() if entry['kind'] == 'symlink'}
    pending = [path for path,entry in entries.items() if entry['kind'] == 'file'
               and any(Path(path).is_relative_to(seed) for seed in closure['elf_roots'])]
    edges = {}
    required = set()
    while pending:
        path = pending.pop()
        if path in edges:
            continue
        metadata = elf_requirements(byte_path(path))
        if metadata is None:
            if path in required:
                raise ValueError('runtime ELF dependency is not a loadable ELF: ' + path)
            continue
        if metadata['machine'] != closure['elf_machine']:
            raise ValueError('runtime closure mixes native ELF architectures')
        directories = []
        for value in metadata['runpath'] or metadata['rpath']:
            for directory in value.split(':'):
                directory = directory.replace('${ORIGIN}', str(Path(path).parent)).replace('$ORIGIN', str(Path(path).parent))
                if not directory.startswith('/') or '$' in directory:
                    raise ValueError('runtime ELF uses an unsupported library search path')
                directories.append(posixpath.normpath(directory))
        multiarch = {62:'x86_64-linux-gnu',183:'aarch64-linux-gnu'}[metadata['machine']]
        directories.extend(['/lib/' + multiarch, '/usr/lib/' + multiarch, '/lib64', '/usr/lib64', '/lib', '/usr/lib'])
        targets = []
        for needed in metadata['needed']:
            if '$' in needed or ('/' in needed and not needed.startswith('/')):
                raise ValueError('runtime ELF uses an unsupported dependency path')
            candidates = [needed] if needed.startswith('/') else [posixpath.join(directory, needed) for directory in directories]
            for candidate in candidates:
                try:
                    target = resolve(candidate)
                except ValueError:
                    continue
                if entries.get(target, {}).get('kind') == 'file':
                    targets.append(target)
                    break
            else:
                raise ValueError('unresolved runtime ELF dependency: ' + path + ' -> ' + needed)
        targets.extend(resolve(interpreter) for interpreter in metadata['interpreters'])
        if any(entries.get(target, {}).get('kind') != 'file' for target in targets):
            raise ValueError('runtime ELF provider is not a sealed regular file')
        edges[path] = sorted(set(targets))
        required.update(targets)
        pending.extend(targets)
    return dict(schema='prismabuild.proof_native_closure_audit.v1',
                elf_files=len(edges), edges_sha256=digest(canonical(edges)),
                symlinks=len(links), symlinks_sha256=digest(canonical(links)))


def verify_closure(profile, origins=('host', 'image')):
    maps = {}
    for root in profile.closure['roots']:
        if root['origin'] not in origins:
            continue
        actual = inventory(root['path'])
        if actual != root['entries']:
            raise ValueError(f'runtime root inventory differs: {root["path"]}')
        maps[root['path']] = dict(origin=root['origin'], members=len(actual),
                                  entries_sha256=digest(canonical(actual)))
    return dict(verified=True, member_maps=maps, member_maps_sha256=digest(canonical(maps)))


def observe_runtime():
    running = Path('/proc/self/exe').read_bytes()
    if running[:6] != b'\x7fELF\x02\x01':
        raise ValueError('root runtime must use a native little-endian ELF64 executable')
    executable = Path(sys.executable)
    if executable.read_bytes() != running:
        raise ValueError('runtime executable path differs from the running image')
    return dict(version=list(sys.version_info[:3]),
        executable=dict(path=sys.executable, resolved_path=str(executable.resolve()),
                        bytes=len(running), sha256=digest(running)),
        elf_machine=int.from_bytes(running[18:20], 'little'),
        credentials=dict(uid=os.getuid(), euid=os.geteuid(), gid=os.getgid(), egid=os.getegid(), groups=os.getgroups()),
        affinity=sorted(os.sched_getaffinity(0)), native_threads={name: os.environ.get(name) for name in THREADS},
        environment={name:os.environ.get(name) for name in (
            'TMPDIR', 'PYTHONPATH', 'PYTHONDONTWRITEBYTECODE', 'PYTEST_DISABLE_PLUGIN_AUTOLOAD')},
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


def native_providers():
    providers = {}
    for line in Path('/proc/self/maps').read_text().splitlines():
        parts = line.split(None, 5)
        if len(parts) != 6 or not parts[5].startswith('/'):
            continue
        path = Path(parts[5]).resolve()
        if str(path) in providers:
            continue
        with path.open('rb') as handle:
            if handle.read(4) != b'\x7fELF':
                continue
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
    credentials = observation['credentials']
    if any(credentials[key] != expected[name] for key, name in [('uid','uid'), ('euid','uid'), ('gid','gid'), ('egid','gid')]) or credentials['groups']:
        raise ValueError('installed reader did not use its real unprivileged credentials')
    observation['link_custody'] = dict(path=str(path), existed_before=existed, created_symlink=created,
        target=os.readlink(path) if path.is_symlink() else None, uid=info.st_uid, gid=info.st_gid, mode=stat.S_IMODE(info.st_mode))
    return observation, payload['mapped_native_providers']


def source_digests(root, tests, profile):
    names = {*tests, 'tests/run_proof_container.py', 'tests/proof_container_runtime.py',
        'tests/capture_proof_runtime.py', 'tests/conftest.py', 'tools/fleet/pbtest_outcomes.py',
        'src/prismabuild/pbtest_collection.py', 'tests/proof_execution.json',
        'tests/receipt_execution.json', 'qualification_dependencies.json',
        str(profile.path.relative_to(root))}
    return {name: digest((root / name).read_bytes()) for name in sorted(names)}


def observe_container_roles(profile, expected_affinity):
    os.makedirs('/home/rob/pbt17', exist_ok=True)
    closure = verify_closure(profile)
    inner = observe_runtime()
    require_identity(inner, profile.primary)
    if any(inner['credentials'][name] != 0 for name in ('uid', 'euid', 'gid', 'egid')):
        raise ValueError('real container UID/GID0 required')
    if inner['affinity'] != expected_affinity or inner['native_threads'] != {name: '1' for name in THREADS}:
        raise ValueError('container changed PB affinity or native thread bounds')
    if inner['python_flags']['no_site'] != 1:
        raise ValueError('primary container must disable unsealed site startup')
    mapped = mapped_native_providers(profile)
    reader, providers = observe_reader(profile)
    if reader['affinity'] != expected_affinity or reader['native_threads'] != {name: '1' for name in THREADS}:
        raise ValueError('installed reader changed PB affinity or native bounds')
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
        record['closure']['inner'] = verify_closure(profile)
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
