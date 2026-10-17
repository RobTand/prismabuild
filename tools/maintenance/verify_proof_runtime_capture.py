"""Bind a captured root runtime to retained PB bytes without product imports."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess


SCHEMA = 'prismabuild.proof_runtime_capture_binding.v1'
PREFIX = 'prismabuild-root-capture: '
HELPERS = ('tests/capture_proof_runtime.py', 'tests/proof_container_runtime.py',
           'tests/run_proof_container.py')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value, *, ascii):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=ascii, allow_nan=False).encode()


def pb_hash(value):
    return sha(canonical(value, ascii=False))


def checked(path, descriptor):
    raw = path.read_bytes()
    if len(raw) != descriptor['bytes'] or sha(raw) != descriptor['sha256']:
        raise ValueError(f'retained capture bytes differ: {path}')
    return raw


def descriptor(path):
    raw = path.read_bytes()
    return dict(path=str(path.resolve()), bytes=len(raw), sha256=sha(raw))


def git(directory, *args):
    return subprocess.check_output(['git', '-c', 'core.hooksPath=/dev/null',
        '--git-dir=' + str(directory), *args], stderr=subprocess.PIPE)


def verify(raw_root, output, expected_source):
    paths = {name: raw_root / filename for name, filename in {
        'request':'request.json', 'terminal':'terminal.json', 'receipt':'receipt.json',
        'stdout':'stdout_log.log', 'stderr':'stderr_log.log', 'result':'result.log',
        'source_bundle':'source.bundle'}.items()}
    request, terminal, receipt = (json.loads(paths[name].read_bytes())
                                  for name in ('request', 'terminal', 'receipt'))
    key = request['action_key']
    if key != pb_hash({k:v for k,v in request.items() if k != 'action_key'}):
        raise ValueError('capture action key differs from its sealed request')
    if terminal['action_key'] != key or receipt['action_key'] != key:
        raise ValueError('capture terminal or receipt belongs to a different action')
    if terminal['status'] != 'executed' or terminal['detail']['returncode'] != 0:
        raise ValueError('capture did not execute successfully')
    if receipt['receipt_sha256'] != pb_hash({k:v for k,v in receipt.items() if k != 'receipt_sha256'}):
        raise ValueError('capture receipt digest differs')
    if receipt['action_manifest_sha256'] != pb_hash(request):
        raise ValueError('capture receipt binds a different request')
    producer = receipt['producer']
    if producer['action_key'] != key or producer['inputs'] != request['inputs']:
        raise ValueError('capture producer or inputs differ')
    if producer['attestation_sha256'] != pb_hash({k:v for k,v in producer.items() if k != 'attestation_sha256'}):
        raise ValueError('capture producer attestation differs')
    runtime = producer['runtime']
    if runtime['runtime_sha256'] != pb_hash({k:v for k,v in runtime.items() if k != 'runtime_sha256'}):
        raise ValueError('capture worker runtime attestation differs')
    for name in ('stdout', 'stderr'):
        checked(paths[name], terminal['detail'][name + '_log'])
    result = checked(paths['result'], receipt['result'])
    lines = [line[len(PREFIX):] for line in result.decode().splitlines() if line.startswith(PREFIX)]
    if len(lines) != 1:
        raise ValueError('capture must produce exactly one source-owned record')
    capture = json.loads(lines[0])
    if capture['schema'] != 'prismabuild.proof_runtime_capture.v1' or capture['complete'] is not True:
        raise ValueError('capture record is incomplete or unsupported')
    raw_profile = capture['profile_json'].encode()
    if len(raw_profile) != capture['profile_bytes'] or sha(raw_profile) != capture['profile_sha256']:
        raise ValueError('capture profile bytes differ')
    profile = json.loads(raw_profile)
    if profile['schema'] != 'prismabuild.proof_runtime_profile.v1':
        raise ValueError('unsupported captured profile')
    closure = profile['closure']
    if closure['schema'] != 'prismabuild.proof_runtime_closure.v1':
        raise ValueError('unsupported captured closure')
    if closure['sha256'] != sha(canonical({k:v for k,v in closure.items() if k != 'sha256'}, ascii=True)):
        raise ValueError('captured closure digest differs')
    if capture['closure_sha256'] != closure['sha256']:
        raise ValueError('capture binds a different closure')
    primary = {name:capture['outer'][name] for name in ('version', 'executable', 'elf_machine')}
    if profile['primary'] != primary:
        raise ValueError('capture profile borrowed a different primary runtime')
    if request['params']['command'][:2] != [primary['executable']['path'], 'tests/capture_proof_runtime.py']:
        raise ValueError('capture did not enter through the declared standard interpreter and producer')
    if request['params']['interpreter'] != primary['executable']['path']:
        raise ValueError('capture did not declare its real primary interpreter to PB')
    if request['params'].get('container_images') != [profile['image']]:
        raise ValueError('capture image was not an exact declared placement dependency')
    expected_architecture = {62:'amd64',183:'arm64'}[primary['elf_machine']]
    if capture['image'] != dict(expected=profile['image'], actual=profile['image'], architecture=expected_architecture):
        raise ValueError('capture image identity or architecture differs')
    if capture['outer']['affinity'] != capture['image_runtime']['affinity']:
        raise ValueError('image capture changed PB affinity')
    for name in ('outer', 'image_runtime'):
        observed = capture[name]
        if observed['elf_machine'] != primary['elf_machine']:
            raise ValueError('capture mixes native architectures')
        if observed['native_threads'] != dict(OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1'):
            raise ValueError('capture native thread bounds differ')
    if any(capture['image_runtime']['credentials'][name] != 0 for name in ('uid', 'euid', 'gid', 'egid')):
        raise ValueError('capture did not observe a real root container')
    cleanup = terminal['resource_scope_cleanup']
    physical = cleanup['export']
    if cleanup['complete'] is not True or any(physical[name] is not True for name in ('empty','ok','settled','released','stopped')) or physical['tickets_pending'] is not False:
        raise ValueError('capture physical scope is not completely released')
    snapshot = request['params']['checkout_snapshot']
    if snapshot != terminal['checkout_snapshot']:
        raise ValueError('capture source snapshots differ')
    checked(paths['source_bundle'], snapshot['input'])
    if request['inputs'] != [snapshot['input']]:
        raise ValueError('capture has unaccounted extra inputs')
    output.mkdir(parents=True, exist_ok=True)
    objects = output / 'source-objects.git'
    if not objects.exists():
        subprocess.run(['git','-c','core.hooksPath=/dev/null','init','--bare',str(objects)], check=True, capture_output=True)
    git(objects, 'fetch', str(paths['source_bundle'].resolve()), snapshot['commit'])
    if git(objects,'rev-list','--parents','-n','1',snapshot['commit']).decode().split() != [snapshot['commit'],snapshot['parent']]:
        raise ValueError('capture source snapshot has a different parent')
    for name in HELPERS:
        actual = git(objects, 'show', snapshot['commit'] + ':' + name)
        if sha(actual) != capture['source_sha256'][name] or actual != (expected_source / name).read_bytes():
            raise ValueError(f'capture helper is not the reviewed source: {name}')
    executables = {}
    binary_root = output / 'executables' / 'by-sha256'
    binary_root.mkdir(parents=True, exist_ok=True)
    for name in ('outer','image_runtime'):
        observed = capture[name]
        expected = observed['executable']
        raw = base64.b64decode(capture['executable_binaries'][expected['sha256']], validate=True)
        if len(raw) != expected['bytes'] or sha(raw) != expected['sha256'] or raw[:6] != b'\x7fELF\x02\x01':
            raise ValueError('capture executable bytes or ELF format differ')
        if int.from_bytes(raw[18:20], 'little') != observed['elf_machine']:
            raise ValueError('capture executable ELF architecture differs')
        target = binary_root / expected['sha256']
        target.write_bytes(raw)
        executables[expected['sha256']] = descriptor(target)
    profile_path = Path(capture['profile_path'])
    if profile_path.is_absolute() or '..' in profile_path.parts:
        raise ValueError('captured profile path is not snapshot-local')
    profile_target = output / profile_path
    profile_target.parent.mkdir(parents=True, exist_ok=True)
    profile_target.write_bytes(raw_profile)
    binding = dict(schema=SCHEMA,
        profile=dict(path=str(profile_path), bytes=len(raw_profile), sha256=sha(raw_profile)),
        closure_sha256=closure['sha256'],
        capture=dict(action_key=key, source_pin=snapshot['parent'], snapshot_commit=snapshot['commit'],
                     raw={name:descriptor(path) for name,path in paths.items()},
                     helper_source_sha256=capture['source_sha256']), executables=executables)
    target = output / 'capture-binding.json'
    target.write_bytes(canonical(binding, ascii=True) + b'\n')
    return descriptor(target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw-dir', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--expected-source', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.raw_dir.resolve(), args.output.resolve(), args.expected_source.resolve()), sort_keys=True))


if __name__ == '__main__':
    main()
