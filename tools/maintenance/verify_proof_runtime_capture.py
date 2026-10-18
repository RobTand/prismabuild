"""Audit retained root capture bytes through the published PB entrypoints."""
import argparse
import base64
from pathlib import Path
import subprocess
import tarfile
import runpy
import os
import tempfile
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild.digest_primitives import (
    canonical_sha256, compact_ascii_json_bytes, decode_strict_json, new_sha256,
    raw_sha256, sorted_json,
)
from prismabuild.immutable_files import file_identity_nofollow


SCHEMA = 'prismabuild.proof_runtime_capture_binding.v1'
PREFIX = 'prismabuild-root-capture: '
HELPERS = ('tests/capture_proof_runtime.py', 'tests/proof_container_runtime.py',
           'tests/run_proof_container.py', 'tests/proof_capture_capacity.py')



def read_capture_pin(path, descriptor):
    raw = path.read_bytes()
    if len(raw) != descriptor['bytes'] or raw_sha256(raw) != descriptor['sha256']:
        raise ValueError(f'retained capture bytes differ: {path}')
    return raw


def capture_file_descriptor(path):
    path = path.resolve()
    digest, size, _ = file_identity_nofollow(path, where='retained proof runtime capture')
    return dict(path=str(path), bytes=size, sha256=digest)


def retain_capture_archive(path, expected, expected_members, byte_root):
    observed = capture_file_descriptor(path)
    if {name:observed[name] for name in ('bytes','sha256')} != expected:
        raise ValueError('retained capture closure archive differs')
    byte_root.mkdir(parents=True, exist_ok=True)
    retained = {}
    with tarfile.open(path, 'r|gz') as archive:
        for member in archive:
            name = member.name.removeprefix('by-sha256/')
            if (not member.isfile() or member.name != 'by-sha256/' + name
                    or len(name) != 64 or any(char not in '0123456789abcdef' for char in name)
                    or name in retained or name not in expected_members):
                raise ValueError('capture archive has an unsafe or unexpected member')
            if member.size != expected_members[name]:
                raise ValueError('capture archive member size differs')
            target = byte_root / name
            hasher = new_sha256()
            size = 0
            with tempfile.NamedTemporaryFile(dir=byte_root, prefix='.' + name + '.', delete=False) as output:
                temporary = Path(output.name)
                try:
                    with archive.extractfile(member) as source:
                        for raw in iter(lambda: source.read(1024 * 1024), b''):
                            hasher.update(raw)
                            size += len(raw)
                            output.write(raw)
                    output.flush()
                    os.fsync(output.fileno())
                    if size != member.size or hasher.hexdigest() != name:
                        raise ValueError('capture archive member bytes differ')
                    if target.exists() or target.is_symlink():
                        if target.is_symlink() or capture_file_descriptor(target) != dict(path=str(target.resolve()), bytes=size, sha256=name):
                            raise ValueError('existing retained closure member differs')
                    else:
                        temporary.chmod(0o444)
                        temporary.rename(target)
                finally:
                    temporary.unlink(missing_ok=True)
            retained[name] = capture_file_descriptor(target)
    if set(retained) != set(expected_members):
        raise ValueError('capture archive omits sealed closure or producer bytes')
    return retained


def capture_git_read(directory, *args):
    return subprocess.check_output(['git', '-c', 'core.hooksPath=/dev/null',
        '--git-dir=' + str(directory), *args], stderr=subprocess.PIPE)


def capture_assigned_affinity(request, terminal, producer, bounds):
    allocation = terminal['cpu_allocation']
    if not isinstance(allocation, dict) or set(allocation) != {'preferred', 'fallback'}:
        raise ValueError('capture terminal lacks its exact CPU allocation')
    if any(not isinstance(allocation[name], list) for name in ('preferred', 'fallback')):
        raise ValueError('capture CPU allocation tiers are not lists')
    assigned = allocation['preferred'] + allocation['fallback']
    if any(type(cpu) is not int or cpu < 0 for cpu in assigned) or len(assigned) != len(set(assigned)):
        raise ValueError('capture CPU allocation contains invalid or duplicate IDs')
    resources = terminal['resources']
    if (resources != dict(cpu=1, mem_gb=2, gpu=0) or type(resources['cpu']) is not int
            or type(resources['gpu']) is not int or type(resources['mem_gb']) not in (int, float)
            or len(assigned) != resources['cpu']):
        raise ValueError('capture allocation does not match its one-CPU demand')
    if terminal['priority'] != -10 or request['params']['execution_timeout_s'] != bounds['runtime_seconds']:
        raise ValueError('capture lacks its exact priority or finite hard deadline')
    if request['params']['demand'] != resources or terminal['detail']['cpu_allocation'] != allocation:
        raise ValueError('capture demand or terminal CPU allocation differs')
    if terminal['finished_host'] != terminal['claimed_host'] or terminal['finished_host'] != producer['worker_id'] or producer['evidence']['hostname'] != producer['worker_id']:
        raise ValueError('capture terminal and producer host identities differ')
    return sorted(assigned)


def verify_capture_storage_record(capture, request, terminal, producer, policy, capacity_policy):
    capacity = capture['capacity']
    if set(capacity) != {'schema', 'private', 'publication', 'published_sources'} or capacity['schema'] != capacity_policy['CAPACITY_SCHEMA']:
        raise ValueError('capture storage record has an unsupported schema or field set')
    private = capacity['private']
    expected_private = Path(capture['outer']['sys_path'][0]).parent / (capture['profile_path'] + '.members')
    if (set(private) != {'path', 'minimum_free_bytes', 'statvfs', 'available_bytes', 'observation_only'}
            or private['path'] != str(expected_private) or not expected_private.is_absolute()
            or private['minimum_free_bytes'] != capacity_policy['PRIVATE_FREE_BYTES']
            or private['observation_only'] is not True):
        raise ValueError('capture private capacity is not its declared snapshot-local observation')
    sample = private['statvfs']
    if (set(sample) != set(capacity_policy['STATVFS_FIELDS'])
            or any(type(value) is not int or value < 0 for value in sample.values())
            or sample['f_frsize'] <= 0 or type(private['available_bytes']) is not int
            or private['available_bytes'] != sample['f_bavail'] * sample['f_frsize']
            or private['available_bytes'] < private['minimum_free_bytes']):
        raise ValueError('capture private current-capacity threshold was not observed')
    publication = capacity['publication']
    if set(publication) != {'queue_root', 'action_key', 'cas_root', 'mode_before', 'mode_inside', 'mode_after',
            'growth_gib', 'lease_s', 'verdicts', 'operation_owners', 'owner_observation_error',
            'held_allowance_observed', 'owner_paths_absent_after'}:
        raise ValueError('capture publication capacity field set differs')
    queue = Path(publication['queue_root'])
    environment = request['environment']['variables']
    marker = queue / 'container-owners' / (environment['PRISMABUILD_CONTAINER_OWNER'] + '.used')
    if (not queue.is_absolute() or str(queue) != publication['queue_root'] or '..' in queue.parts
            or environment['PRISMABUILD_CONTAINER_MARKER'] != str(marker)
            or publication['action_key'] != request['action_key']
            or publication['cas_root'] != terminal['cas_root'] or publication['cas_root'] != capture['cas_root']
            or type(publication['growth_gib']) is not int or publication['growth_gib'] != capacity_policy['PUBLICATION_GROWTH_GIB']
            or type(publication['lease_s']) is not int or publication['lease_s'] != capacity_policy['PUBLICATION_LEASE_S']):
        raise ValueError('capture publication is not bound to the actual queue/action/CAS identities')
    mode = publication['mode_inside']
    if mode not in ('off', 'observe', 'enforce') or publication['mode_before'] != mode or publication['mode_after'] != mode:
        raise ValueError('capture floor mode is unsupported or changed during publication')
    verdicts, owners = publication['verdicts'], publication['operation_owners']
    if (not isinstance(verdicts, list) or not isinstance(owners, list) or len(owners) > 1
            or any(not isinstance(value, dict) or type(value.get('allowed')) is not bool for value in verdicts)
            or publication['owner_observation_error'] is not None and not isinstance(publication['owner_observation_error'], str)):
        raise ValueError('capture floor verdict or actual owner observations are malformed')
    if mode == 'off' and (verdicts or owners or publication['owner_observation_error'] is not None):
        raise ValueError('off mode cannot imply floor checks or an operation owner')
    if mode != 'off' and (bool(owners) == (publication['owner_observation_error'] is not None)):
        raise ValueError('capture owner presence and observation error contradict each other')
    for descriptor in owners:
        if set(descriptor) != {'path', 'bytes', 'sha256', 'raw_json'}:
            raise ValueError('capture operation owner lacks exact retained raw bytes')
        raw = descriptor['raw_json'].encode()
        if len(raw) != descriptor['bytes'] or raw_sha256(raw) != descriptor['sha256'] or len(raw) > policy['CAPTURE_BOUNDS']['metadata_bytes']:
            raise ValueError('capture operation owner raw bytes differ')
        value = decode_strict_json(raw, where='actual capture growth operation owner')
        path = Path(descriptor['path'])
        if (set(value) != capacity_policy['OWNER_FIELDS']
                or value['schema'] != 'prismabuild.filesystem_floor.operation_owner.v1'
                or not path.is_absolute() or '..' in path.parts or str(path) != descriptor['path']
                or not path.is_relative_to(queue / 'filesystem-reservations')
                or path.parent.name != 'operations' or path.name != value['holder'] + '.json'
                or not value['holder'].startswith('operation-' + producer['worker_id'] + '-' + str(value['pid']) + '-')
                or value['label'] != request['action_key'] or value['host'] != producer['worker_id']
                or value['boot_id'] != terminal['resource_scope']['boot_id']
                or not isinstance(value['machine_id'], str) or len(value['machine_id']) != 32
                or any(char not in '0123456789abcdef' for char in value['machine_id'])
                or type(value['pid']) is not int or value['pid'] <= 0
                or not isinstance(value['pid_start'], str) or not value['pid_start'].isdigit()
                or type(value['lease_until']) not in (int, float)
                or not terminal['claimed_unix'] < value['lease_until'] <= terminal['finished_unix'] + publication['lease_s']):
            raise ValueError('capture actual operation owner does not bind its host/action/process/lease')
    if publication['owner_paths_absent_after'] != [value['path'] for value in owners]:
        raise ValueError('capture did not observe every retained owner path absent after publication')
    held = mode == 'enforce' and len(owners) == 1 and publication['owner_observation_error'] is None and all(value['allowed'] for value in verdicts)
    if publication['held_allowance_observed'] is not held or mode == 'enforce' and not held:
        raise ValueError('capture held-allowance claim is not supported by actual enforced acquisition')
    sources = capacity['published_sources']
    generation = Path(producer['runtime']['core']['resolved_path']).parents[2]
    if set(sources) != set(policy['CAPTURE_PUBLISHED_SOURCES']):
        raise ValueError('capture published source identity set differs')
    required = {value['path']:value['sha256'] for value in request['params']['requires_files']}
    expected_required = {capture['outer']['executable'][name]:capture['outer']['executable']['sha256']
                         for name in ('path', 'resolved_path')}
    for name, descriptor in sources.items():
        path = str(generation / name)
        if (set(descriptor) != {'path', 'resolved_path', 'bytes', 'sha256'}
                or descriptor['path'] != path or descriptor['resolved_path'] != path
                or type(descriptor['bytes']) is not int or descriptor['bytes'] < 0):
            raise ValueError('capture source is not a named physical published-generation member')
        expected_required[path] = descriptor['sha256']
    if (len(required) != len(request['params']['requires_files']) or required != expected_required
            or request['params']['requires_files'] != terminal['requires_files']):
        raise ValueError('capture did not fence every exact published source and primary executable')
    for field, name in [('core','src/prismabuild/core.py'), ('digest_primitives','src/prismabuild/digest_primitives.py'),
                        ('launcher','tools/prismabuild_worker.py')]:
        if producer['runtime'][field] != sources[name]:
            raise ValueError('capture floor sources and actual admitting runtime generation differ')
    if capture['progress']['helper'] != sources['src/prismabuild/progress.py']:
        raise ValueError('capture progress helper is not aligned with its physical published owners')
    return list(sources.values())


def bind_proof_runtime_capture(raw_root, output, expected_source):
    paths = {name: raw_root / filename for name, filename in {
        'request':'request.json', 'terminal':'terminal.json', 'receipt':'receipt.json',
        'stdout':'stdout_log.log', 'stderr':'stderr_log.log', 'result':'result.log',
        'source_bundle':'source.bundle'}.items()}
    request, terminal, receipt = (decode_strict_json(paths[name].read_bytes(), where=name)
                                  for name in ('request', 'terminal', 'receipt'))
    key = request['action_key']
    if key != canonical_sha256({k:v for k,v in request.items() if k != 'action_key'}):
        raise ValueError('capture action key differs from its sealed request')
    if terminal['action_key'] != key or receipt['action_key'] != key:
        raise ValueError('capture terminal or receipt belongs to a different action')
    if terminal['status'] != 'executed' or terminal['detail']['returncode'] != 0:
        raise ValueError('capture did not execute successfully')
    if receipt['receipt_sha256'] != canonical_sha256({k:v for k,v in receipt.items() if k != 'receipt_sha256'}):
        raise ValueError('capture receipt digest differs')
    if receipt['action_manifest_sha256'] != canonical_sha256(request):
        raise ValueError('capture receipt binds a different request')
    producer = receipt['producer']
    if producer['action_key'] != key or producer['inputs'] != request['inputs']:
        raise ValueError('capture producer or inputs differ')
    if producer['attestation_sha256'] != canonical_sha256({k:v for k,v in producer.items() if k != 'attestation_sha256'}):
        raise ValueError('capture producer attestation differs')
    sys.path.insert(0, str(expected_source / 'tests'))
    owner = runpy.run_path(str(expected_source / 'tests/proof_container_runtime.py'))
    capacity_owner = runpy.run_path(str(expected_source / 'tests/proof_capture_capacity.py'))
    affinity = capture_assigned_affinity(request, terminal, producer, owner['CAPTURE_BOUNDS'])
    runtime = producer['runtime']
    if runtime['runtime_sha256'] != canonical_sha256({k:v for k,v in runtime.items() if k != 'runtime_sha256'}):
        raise ValueError('capture worker runtime attestation differs')
    for name in ('stdout', 'stderr'):
        read_capture_pin(paths[name], terminal['detail'][name + '_log'])
    result = read_capture_pin(paths['result'], receipt['result'])
    lines = [line[len(PREFIX):] for line in result.decode().splitlines() if line.startswith(PREFIX)]
    if len(lines) != 1:
        raise ValueError('capture must produce exactly one source-owned record')
    capture = decode_strict_json(lines[0].encode(), where='root capture record')
    if capture['schema'] != 'prismabuild.proof_runtime_capture.v1' or capture['complete'] is not True:
        raise ValueError('capture record is incomplete or unsupported')
    if set(capture['source_sha256']) != set(HELPERS):
        raise ValueError('capture helper source set differs')
    if (capture['bounds'] != owner['CAPTURE_BOUNDS']
            or capture['closure_archive']['bytes'] > owner['CAPTURE_BOUNDS']['archive_bytes']
            or capture['profile_bytes'] > owner['CAPTURE_BOUNDS']['profile_bytes']
            or len(PREFIX.encode()) + len(lines[0].encode()) + 1 > owner['CAPTURE_BOUNDS']['metadata_bytes']):
        raise ValueError('capture exceeds its reviewed source-owned bounds')
    published_sources = verify_capture_storage_record(capture, request, terminal, producer, owner, capacity_owner)
    raw_profile = capture['profile_json'].encode()
    if len(raw_profile) != capture['profile_bytes'] or raw_sha256(raw_profile) != capture['profile_sha256']:
        raise ValueError('capture profile bytes differ')
    profile = decode_strict_json(raw_profile, where='captured runtime profile')
    if profile['schema'] != 'prismabuild.proof_runtime_profile.v1':
        raise ValueError('unsupported captured profile')
    closure = profile['closure']
    if closure['schema'] != 'prismabuild.proof_runtime_closure.v1':
        raise ValueError('unsupported captured closure')
    if closure['sha256'] != raw_sha256(compact_ascii_json_bytes({k:v for k,v in closure.items() if k != 'sha256'})):
        raise ValueError('captured closure digest differs')
    if capture['closure_sha256'] != closure['sha256']:
        raise ValueError('capture binds a different closure')
    members = {str(Path(root['path']) / relative):dict(bytes=entry['bytes'], sha256=entry['sha256'],
               archive_member='by-sha256/' + entry['sha256'])
               for root in closure['roots'] for relative,entry in root['entries'].items()
               if entry['kind'] == 'file'}
    if capture['closure_members'] != members:
        raise ValueError('capture member map differs from the full sealed profile')
    if (sum(len(root['entries']) for root in closure['roots']) > owner['CAPTURE_BOUNDS']['members']
            or sum(entry['bytes'] for entry in members.values()) > owner['CAPTURE_BOUNDS']['regular_bytes']):
        raise ValueError('capture closure exceeds its full-byte member budget')
    verification = capture['closure_verification']
    maps = {root['path']:dict(origin=root['origin'], members=len(root['entries']),
            entries_sha256=raw_sha256(compact_ascii_json_bytes(root['entries']))) for root in closure['roots']}
    if verification != dict(verified=True, member_maps=maps,
                            member_maps_sha256=raw_sha256(compact_ascii_json_bytes(maps))):
        raise ValueError('capture mounted closure verification differs')
    for providers in capture['mapped_native_providers'].values():
        for path,actual in providers.items():
            expected = members[path]
            if actual != dict(resolved_path=path, bytes=expected['bytes'], sha256=expected['sha256']):
                raise ValueError('capture mapped native provider differs from its sealed bytes')
    primary = {name:capture['outer'][name] for name in ('version', 'executable', 'elf_machine')}
    if profile['primary'] != primary:
        raise ValueError('capture profile borrowed a different primary runtime')
    if request['params']['command'][:2] != [primary['executable']['path'], 'tests/capture_proof_runtime.py']:
        raise ValueError('capture did not enter through the declared standard interpreter and producer')
    if request['params']['interpreter'] != primary['executable']['path']:
        raise ValueError('capture did not declare its real primary interpreter to PB')
    if terminal['interpreter'] != request['params']['interpreter']:
        raise ValueError('capture terminal binds a different interpreter')
    command = request['params']['command']
    if command.count('--cas-root') != 1 or command[command.index('--cas-root') + 1] != terminal['cas_root'] or capture['cas_root'] != terminal['cas_root']:
        raise ValueError('capture archive was not published into the actual action CAS')
    for argument in ('--output', '--profile-path'):
        if command.count(argument) != 1 or command[command.index(argument) + 1] != capture['profile_path']:
            raise ValueError('capture profile was not its explicitly named derived output')
    if request['params'].get('container_images') != [profile['image']]:
        raise ValueError('capture image was not an exact declared placement dependency')
    expected_architecture = {62:'amd64',183:'arm64'}[primary['elf_machine']]
    if capture['image'] != dict(expected=profile['image'], actual=profile['image'], architecture=expected_architecture):
        raise ValueError('capture image identity or architecture differs')
    if (primary['elf_machine'], tuple(primary['version'])) not in {(183, (3, 12, 3)), (62, (3, 14, 4))}:
        raise ValueError('capture is outside the declared standard runtime matrix')
    if {name:capture['inner'][name] for name in ('version', 'executable', 'elf_machine')} != primary:
        raise ValueError('capture mounted primary is a different actual runtime')
    reader = profile['installed_reader']
    if {name:capture['installed_reader'][name] for name in ('version', 'executable', 'elf_machine')} != {name:reader[name] for name in ('version', 'executable', 'elf_machine')}:
        raise ValueError('capture installed reader is a different actual runtime')
    for name in ('outer', 'image_runtime', 'inner', 'installed_reader'):
        observed = capture[name]
        if observed['elf_machine'] != primary['elf_machine']:
            raise ValueError('capture mixes native architectures')
        if observed['native_threads'] != dict(OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1'):
            raise ValueError('capture native thread bounds differ')
        if observed['affinity'] != affinity or any(type(cpu) is not int for cpu in observed['affinity']):
            raise ValueError('capture runtime differs from its terminal-assigned affinity')
        if observed['kernel']['machine'] != {62:'x86_64',183:'aarch64'}[primary['elf_machine']]:
            raise ValueError('capture actual kernel and ELF architecture differ')
        if observed['environment']['TMPDIR'] != '/home/rob/pbt17':
            raise ValueError('capture runtime changed the declared private TMPDIR')
        if observed['loader_environment'] or type(observed['at_secure']) is not int or observed['at_secure'] != 0:
            raise ValueError('capture runtime has unsupported loader overrides or secure mode')
    if capture['inner']['python_flags']['no_site'] != 1 or capture['installed_reader']['python_flags']['isolated'] != 1:
        raise ValueError('capture primary or installed reader startup flags differ')
    for name in ('image_runtime', 'inner'):
        if any(capture[name]['credentials'][field] != 0 for field in ('uid', 'euid', 'gid', 'egid')):
            raise ValueError('capture did not observe real container UID/GID0')
    credentials = capture['installed_reader']['credentials']
    if any(credentials[key] != reader[name] for key,name in [('uid','uid'),('euid','uid'),('gid','gid'),('egid','gid')]) or credentials['groups']:
        raise ValueError('capture installed reader did not drop real credentials')
    cleanup = terminal['resource_scope_cleanup']
    physical = cleanup['export']
    if cleanup['complete'] is not True or any(physical[name] is not True for name in ('empty','ok','settled','released','stopped')) or physical['tickets_pending'] is not False:
        raise ValueError('capture physical scope is not completely released')
    snapshot = request['params']['checkout_snapshot']
    if snapshot != terminal['checkout_snapshot']:
        raise ValueError('capture source snapshots differ')
    read_capture_pin(paths['source_bundle'], snapshot['input'])
    if request['inputs'] != [snapshot['input']]:
        raise ValueError('capture has unaccounted extra inputs')
    output.mkdir(parents=True, exist_ok=True)
    objects = output / 'source-objects.git'
    if not objects.exists():
        subprocess.run(['git','-c','core.hooksPath=/dev/null','init','--bare',str(objects)], check=True, capture_output=True)
    capture_git_read(objects, 'fetch', str(paths['source_bundle'].resolve()), snapshot['commit'])
    if capture_git_read(objects,'rev-list','--parents','-n','1',snapshot['commit']).decode().split() != [snapshot['commit'],snapshot['parent']]:
        raise ValueError('capture source snapshot has a different parent')
    for name in HELPERS:
        actual = capture_git_read(objects, 'show', snapshot['commit'] + ':' + name)
        if raw_sha256(actual) != capture['source_sha256'][name] or actual != (expected_source / name).read_bytes():
            raise ValueError(f'capture helper is not the reviewed source: {name}')
    expected_members = {entry['sha256']:entry['bytes'] for entry in members.values()}
    attested = [producer['executable'], *published_sources]
    helper = capture['progress']['helper']
    if helper['path'] != helper['resolved_path'] or Path(helper['resolved_path']) != Path(runtime['core']['resolved_path']).with_name('progress.py'):
        raise ValueError('capture did not use the actual worker generation progress helper')
    supplemental_bytes = sum(value['bytes'] for value in attested) + capture['image_runtime']['executable']['bytes']
    if supplemental_bytes > owner['CAPTURE_BOUNDS']['supplemental_bytes']:
        raise ValueError('capture named supplemental identities exceed their full-byte budget')
    for expected in attested:
        if expected['sha256'] in expected_members and expected_members[expected['sha256']] != expected['bytes']:
            raise ValueError('capture has conflicting content sizes')
        expected_members[expected['sha256']] = expected['bytes']
    for name in ('outer', 'image_runtime'):
        expected = capture[name]['executable']
        if expected['sha256'] in expected_members and expected_members[expected['sha256']] != expected['bytes']:
            raise ValueError('capture has conflicting executable sizes')
        expected_members[expected['sha256']] = expected['bytes']
    byte_root = output / 'closure-by-sha256'
    archive = raw_root / 'closure-archive.tar.gz'
    executables = retain_capture_archive(archive, capture['closure_archive'], expected_members, byte_root)
    if owner['loader_kernel'](closure['loader']['diagnostics']['raw'])[0] != capture['outer']['kernel']['release']:
        raise ValueError('retained loader diagnostics belong to a different actual kernel')
    if capture['bounds'] != owner['CAPTURE_BOUNDS'] or capture['progress']['phases'] != list(owner['CAPTURE_PHASES']):
        raise ValueError('capture geometry or ordered progress contract differs')
    if request['params']['progress'] != dict(schema='prismabuild.action_progress_policy.v1',
        phases=[dict(name=name, grace_s=120.0) for name in owner['CAPTURE_PHASES']]):
        raise ValueError('capture did not seal its exact finite progress policy')
    progress_observation = terminal['detail']['progress_observation']
    accepted = progress_observation['last_accepted']
    if (progress_observation['source'] != 'action-progress'
            or type(progress_observation['accepted_count']) is not int or progress_observation['accepted_count'] < 1
            or type(progress_observation['rejected_count']) is not int or progress_observation['rejected_count'] < 0
            or progress_observation['phase'] != 'closure_archive'
            or accepted['phase'] != 'closure_archive' or accepted['unit'] != 'closure_archive'
            or type(accepted['units_completed']) is not int or accepted['units_completed'] != 4):
        raise ValueError('capture terminal does not retain actual accepted final progress')
    geometry = dict(members=sum(len(root['entries']) for root in closure['roots']),
                    regular_bytes=sum(entry['bytes'] for entry in members.values()),
                    profile_bytes=len(raw_profile), archive_bytes=capture['closure_archive']['bytes'])
    if capture['geometry'] != geometry or any(geometry[name] > capture['bounds'][name] for name in geometry):
        raise ValueError('actual captured geometry differs or exceeds its sealed refusal caps')
    checkpoints = {}
    for index,phase in enumerate(owner['CAPTURE_PHASES'], 1):
        path = raw_root / 'checkpoints' / (phase + '.json')
        if capture['progress']['checkpoints'][phase]['bytes'] > owner['CAPTURE_BOUNDS']['metadata_bytes']:
            raise ValueError('capture checkpoint exceeds its reviewed metadata bound')
        value = decode_strict_json(read_capture_pin(path, capture['progress']['checkpoints'][phase]), where='capture checkpoint')
        if value['phase'] != phase or type(value['units']) is not int or value['units'] != index:
            raise ValueError('capture durable checkpoint order or cumulative units differ')
        if phase == 'host_inventory' and value['value']['observation'] != capture['outer']:
            raise ValueError('durable host inventory belongs to another runtime')
        if phase == 'image_inventory' and value['value']['observation'] != capture['image_runtime']:
            raise ValueError('durable image inventory belongs to another runtime')
        if phase == 'mounted_runtime' and value['value'] != dict(profile_sha256=capture['profile_sha256'],
            profile_json=capture['profile_json'], roles=dict(inner=capture['inner'], installed_reader=capture['installed_reader'],
                closure=capture['closure_verification'], mapped_native_providers=capture['mapped_native_providers'])):
            raise ValueError('durable mounted-runtime observation differs')
        if phase == 'closure_archive' and value['value'] != dict(archive=dict(id='proof-runtime-closure', **capture['closure_archive']),
            geometry={name:geometry[name] for name in ('members','regular_bytes')}):
            raise ValueError('durable archive commitment differs')
        checkpoints[phase] = capture_file_descriptor(path)
    observed_audit = owner['audit_native_closure'](closure,
        lambda path: byte_root / members[path]['sha256'])
    if observed_audit != closure['native_audit']:
        raise ValueError('retained native dependency graph differs from the capture')
    for name in ('outer','image_runtime'):
        observed = capture[name]
        expected = observed['executable']
        raw = base64.b64decode(capture['executable_binaries'][expected['sha256']], validate=True)
        if len(raw) != expected['bytes'] or raw_sha256(raw) != expected['sha256'] or raw[:6] != b'\x7fELF\x02\x01':
            raise ValueError('capture executable bytes or ELF format differ')
        if int.from_bytes(raw[18:20], 'little') != observed['elf_machine']:
            raise ValueError('capture executable ELF architecture differs')
        target = byte_root / expected['sha256']
        if target.read_bytes() != raw:
            raise ValueError('capture executable is absent from the retained closure archive')
    profile_path = Path(capture['profile_path'])
    if profile_path.is_absolute() or '..' in profile_path.parts:
        raise ValueError('captured profile path is not snapshot-local')
    profile_target = output / profile_path
    profile_target.parent.mkdir(parents=True, exist_ok=True)
    profile_target.write_bytes(raw_profile)
    binding = dict(schema=SCHEMA,
        profile=dict(path=str(profile_path), bytes=len(raw_profile), sha256=raw_sha256(raw_profile)),
        closure_sha256=closure['sha256'],
        capture=dict(action_key=key, source_pin=snapshot['parent'], snapshot_commit=snapshot['commit'],
                     raw={name:capture_file_descriptor(path) for name,path in paths.items()},
                     helper_source_sha256=capture['source_sha256'], assigned_affinity=affinity,
                     progress=dict(helper=executables[helper['sha256']], checkpoints=checkpoints),
                     capacity=capture['capacity']), executables=executables,
        closure_archive=capture_file_descriptor(archive), closure_members=members, closure_byte_root=str(byte_root))
    target = output / 'capture-binding.json'
    target.write_bytes(compact_ascii_json_bytes(binding) + b'\n')
    return capture_file_descriptor(target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw-dir', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--expected-source', required=True, type=Path)
    args = parser.parse_args()
    print(sorted_json(bind_proof_runtime_capture(args.raw_dir.resolve(), args.output.resolve(), args.expected_source.resolve())))


if __name__ == '__main__':
    main()
