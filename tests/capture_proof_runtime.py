"""Capture only the selected CPython runtime and an existing native image."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import site
import subprocess
import sys
import sysconfig
import tarfile

from proof_container_runtime import (
    CLOSURE_SCHEMA, PROFILE_SCHEMA, audit_native_closure, canonical, digest, inventory,
    native_providers, observe_runtime,
)


CAPTURE_SCHEMA = 'prismabuild.proof_runtime_capture.v1'


class HashedReader:
    def __init__(self, handle):
        self.handle = handle
        self.hasher = hashlib.sha256()

    def read(self, size):
        raw = self.handle.read(size)
        self.hasher.update(raw)
        return raw


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
    with tarfile.open(path, 'w:gz', compresslevel=1) as archive:
        for source, entry in sorted(members.items()):
            if entry['sha256'] in retained:
                continue
            member = tarfile.TarInfo('by-sha256/' + entry['sha256'])
            member.size, member.mode = entry['bytes'], 0o444
            with Path(replacements.get(entry['sha256'], source)).open('rb') as handle:
                verified = HashedReader(handle)
                archive.addfile(member, verified)
                if handle.read(1) or verified.hasher.hexdigest() != entry['sha256']:
                    raise ValueError('runtime member changed while archiving: ' + source)
            retained.add(entry['sha256'])
    with path.open('rb') as handle:
        os.fsync(handle.fileno())
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


def captured_roots(origin, paths):
    roots = []
    for path in sorted(set(Path(value).resolve() for value in paths), key=lambda p: (len(p.parts), str(p))):
        if any(path.is_relative_to(root['path']) for root in roots):
            continue
        roots.append(dict(origin=origin, path=str(path), entries=inventory(path)))
    return roots


def runtime_capture(origin):
    observation = observe_runtime()
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
    providers = native_providers()
    paths.extend(Path(path) for path in providers)
    if origin == 'image':
        paths.extend(Path(path) for path in site.getsitepackages() if Path(path).is_dir())
    roots = captured_roots(origin, paths)
    if origin == 'image':
        for name in ('/lib', '/lib64', '/usr/lib64'):
            path = Path(name)
            if (path.is_symlink() or (name == '/usr/lib64' and path.is_dir())) and not any(path.is_relative_to(root['path']) for root in roots):
                roots.append(dict(origin=origin, path=name, entries=inventory(path)))
    return dict(observation=observation, roots=roots, stdlib=str(stdlib), providers=providers,
                executable_binary=base64.b64encode(Path('/proc/self/exe').read_bytes()).decode())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture-image', action='store_true')
    parser.add_argument('--image')
    parser.add_argument('--image-python')
    parser.add_argument('--profile-path')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--cas-root', type=Path)
    parser.add_argument('--reader-uid', type=int, default=65534)
    parser.add_argument('--reader-gid', type=int, default=65534)
    args = parser.parse_args(argv)
    if args.capture_image:
        if args.output is None:
            parser.error('image capture requires a private archive output')
        os.makedirs('/home/rob/pbt17', exist_ok=True)
        captured = runtime_capture('image')
        captured['archive'] = archive_members(args.output, regular_members(captured['roots']))
        print(canonical(captured).decode(), flush=True)
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
    args.output.parent.mkdir(parents=True, exist_ok=True)
    private = args.output.parent / (args.output.name + '.members')
    private.mkdir()
    host = runtime_capture('host')
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
    command = ['docker', 'run', '--rm', '--network', 'none', '--user', '0:0',
        '--entrypoint', args.image_python, '--cpuset-cpus', ','.join(map(str, affinity)),
        '--memory', '1g', '--pids-limit', '128', '--mount', f'type=bind,src={root},dst=/source,readonly',
        '--mount', f'type=bind,src={private},dst=/capture-output',
        '--workdir', '/source', '--env', 'PYTHONDONTWRITEBYTECODE=1',
        '--env', 'TMPDIR=/home/rob/pbt17']
    for name in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS'):
        command.extend(['--env', f'{name}=1'])
    command.extend([args.image, '-B', '/source/tests/capture_proof_runtime.py', '--capture-image',
                    '--output', '/capture-output/image-members.tar.gz'])
    result = subprocess.run(command, capture_output=True, text=True, timeout=600, check=False)
    if result.returncode:
        raise ValueError('existing image capture failed: ' + result.stderr[-2000:])
    image = json.loads(result.stdout)
    image_archive = private / 'image-members.tar.gz'
    if file_descriptor(image_archive) != image['archive']:
        raise ValueError('actual image member archive differs')
    image_members = retain_image_members(image_archive, private / 'image-by-sha256')
    if image['observation']['native_threads'] != threads:
        raise ValueError('image capture changed native thread ceilings')
    if any(image['observation']['credentials'][key] != 0 for key in ('uid', 'euid', 'gid', 'egid')):
        raise ValueError('image capture did not use real UID/GID0')
    if image['observation']['affinity'] != affinity or image['observation']['elf_machine'] != host['observation']['elf_machine']:
        raise ValueError('image capture changed actual architecture or assigned affinity')
    roots = list(host['roots'])
    for captured in image['roots']:
        path = Path(captured['path'])
        if any(path.is_relative_to(known['path']) for known in roots):
            continue
        if any(Path(known['path']).is_relative_to(path) for known in roots):
            raise ValueError('image runtime root would replace a primary host root')
        roots.append(captured)
    target = image['observation']['executable']['resolved_path']
    actual = host['observation'] if target == host['observation']['executable']['resolved_path'] else image['observation']
    reader = {key: actual[key] for key in ('version', 'executable', 'elf_machine')}
    reader['executable'] = dict(reader['executable'], path='/usr/bin/python3')
    reader.update(uid=args.reader_uid, gid=args.reader_gid, link_target=target)
    closure = dict(schema=CLOSURE_SCHEMA, roots=sorted(roots, key=lambda item: (item['origin'], item['path'])),
        elf_machine=host['observation']['elf_machine'],
        elf_roots=sorted({host['stdlib'], image['stdlib'], host['observation']['executable']['resolved_path'], target}))
    closure_members = regular_members(closure['roots'])
    closure['native_audit'] = audit_native_closure(closure,
        lambda path: image_members.get(closure_members[path]['sha256'], path))
    closure['sha256'] = digest(canonical(closure))
    profile = dict(schema=PROFILE_SCHEMA, image=args.image,
        primary={key: host['observation'][key] for key in ('version', 'executable', 'elf_machine')},
        installed_reader=reader, closure=closure)
    raw = canonical(profile) + b'\n'
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
    result = subprocess.run(mounted, capture_output=True, text=True, timeout=600, check=False)
    if result.returncode:
        raise ValueError('mounted runtime capture failed: ' + result.stderr[-2000:])
    roles = json.loads(result.stdout)
    members = dict(closure_members)
    image_executable = image['observation']['executable']
    members['image-executable:' + image_executable['sha256']] = dict(kind='file',
        bytes=image_executable['bytes'], sha256=image_executable['sha256'])
    published = Path('/mnt/shared/prismabuild-fleet/repo').resolve()
    attested = [Path('/bin/bash').resolve(), *(published / name for name in (
        'tools/prismabuild_worker.py', 'src/prismabuild/core.py', 'src/prismabuild/digest_primitives.py'))]
    for path in attested:
        members[str(path)] = inventory(path)['.']
    archive_path = private / 'closure-members.tar.gz'
    archive = archive_members(archive_path, members, image_members)
    owner = runpy.run_path(str(published / 'src/prismabuild/core.py'))
    cas = owner['PrismaBuildCAS'](args.cas_root)
    blob, _ = cas.ingest_input(archive_path, input_id='proof-runtime-closure',
                             expected_sha256=archive['sha256'], expected_bytes=archive['bytes'])
    source = {name: digest((root / name).read_bytes()) for name in (
        'tests/capture_proof_runtime.py', 'tests/proof_container_runtime.py', 'tests/run_proof_container.py')}
    record = dict(schema=CAPTURE_SCHEMA, complete=True, profile_path=args.profile_path,
        profile_bytes=len(raw), profile_sha256=digest(raw), profile_json=raw.decode(),
        closure_sha256=closure['sha256'], source_sha256=source,
        closure_archive=dict(bytes=blob['bytes'], sha256=blob['sha256']),
        closure_members={path:dict(bytes=entry['bytes'], sha256=entry['sha256'],
                                  archive_member='by-sha256/' + entry['sha256']) for path,entry in sorted(closure_members.items())},
        cas_root=str(args.cas_root),
        image=dict(expected=args.image, actual=inspected['id'], architecture=inspected['architecture']),
        outer=host['observation'], image_runtime=image['observation'], inner=roles['inner'],
        installed_reader=roles['installed_reader'], closure_verification=roles['closure'],
        mapped_native_providers=roles['mapped_native_providers'],
        executable_binaries={host['observation']['executable']['sha256']:host['executable_binary'],
                             image['observation']['executable']['sha256']:image['executable_binary']})
    print('prismabuild-root-capture: ' + canonical(record).decode(), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
