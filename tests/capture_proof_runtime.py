"""Capture only the selected CPython runtime and an existing native image."""
import argparse
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import sysconfig

from proof_container_runtime import (
    CLOSURE_SCHEMA, PROFILE_SCHEMA, canonical, digest, inventory,
    native_providers, observe_runtime,
)


CAPTURE_SCHEMA = 'prismabuild.proof_runtime_capture.v1'


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
    return dict(observation=observation, roots=captured_roots(origin, paths), providers=providers,
                executable_binary=base64.b64encode(Path('/proc/self/exe').read_bytes()).decode())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture-image', action='store_true')
    parser.add_argument('--image')
    parser.add_argument('--image-python')
    parser.add_argument('--profile-path')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--reader-uid', type=int, default=65534)
    parser.add_argument('--reader-gid', type=int, default=65534)
    args = parser.parse_args(argv)
    if args.capture_image:
        print(canonical(runtime_capture('image')).decode(), flush=True)
        return 0
    if not args.image or not args.image_python or not args.profile_path or args.output is None:
        parser.error('host capture requires image, image-python, profile-path and output')
    relative = Path(args.profile_path)
    if relative.is_absolute() or '..' in relative.parts:
        raise ValueError('profile output name must be snapshot-local')
    if args.reader_uid <= 0 or args.reader_gid <= 0:
        raise ValueError('reader observation requires real unprivileged UID/GID')
    if not args.image.startswith('sha256:') or len(args.image) != 71:
        raise ValueError('capture requires an existing immutable image ID')
    root = Path(__file__).resolve().parents[1]
    host = runtime_capture('host')
    threads = dict(OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1')
    if host['observation']['native_threads'] != threads:
        raise ValueError('host capture requires actual native thread ceilings1')
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
        '--workdir', '/source', '--env', 'PYTHONDONTWRITEBYTECODE=1']
    for name in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS'):
        command.extend(['--env', f'{name}=1'])
    command.extend([args.image, '-B', '/source/tests/capture_proof_runtime.py', '--capture-image'])
    result = subprocess.run(command, capture_output=True, text=True, timeout=600, check=False)
    if result.returncode:
        raise ValueError('existing image capture failed: ' + result.stderr[-2000:])
    image = json.loads(result.stdout)
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
    closure = dict(schema=CLOSURE_SCHEMA, roots=sorted(roots, key=lambda item: (item['origin'], item['path'])))
    closure['sha256'] = digest(canonical(closure))
    profile = dict(schema=PROFILE_SCHEMA, image=args.image,
        primary={key: host['observation'][key] for key in ('version', 'executable', 'elf_machine')},
        installed_reader=reader, closure=closure)
    raw = canonical(profile) + b'\n'
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    source = {name: digest((root / name).read_bytes()) for name in (
        'tests/capture_proof_runtime.py', 'tests/proof_container_runtime.py', 'tests/run_proof_container.py')}
    record = dict(schema=CAPTURE_SCHEMA, complete=True, profile_path=args.profile_path,
        profile_bytes=len(raw), profile_sha256=digest(raw), profile_json=raw.decode(),
        closure_sha256=closure['sha256'], source_sha256=source,
        image=dict(expected=args.image, actual=inspected['id'], architecture=inspected['architecture']),
        outer=host['observation'], image_runtime=image['observation'],
        executable_binaries={host['observation']['executable']['sha256']:host['executable_binary'],
                             image['observation']['executable']['sha256']:image['executable_binary']})
    print('prismabuild-root-capture: ' + canonical(record).decode(), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
