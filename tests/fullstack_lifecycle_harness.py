"""Connected portable #2100 acceptance lane. All publications use real movers.

Pipe acknowledgements delimit observer barriers; timeouts only bound failures.
The file-backed RAM fixture makes no native containment or broker claims.
"""
from __future__ import annotations

import hashlib
import multiprocessing as mp
import os
from pathlib import Path
import socket
import subprocess
import sys
import json
import shutil
import tempfile


from prismabuild import core as pb
from prismabuild import client, data_manifest, pool, reader_lease, residency_map
from prismabuild import ram_promote, stage_move, staged_path, storage_tiers
from prismabuild import residency_plan, resource_scope
from fullstack_lifecycle_progress import ClaimedProgressReporter

STAGE = 'prismabuild-stage:lifecycle'
RAM = 'ram:lifecycle'
OWNER = 'lifecycle:1:abcdef01'


def _reader(connection, config):
    """A pin belongs to this reader, not to its controlling parent."""
    q = pool.PoolQueue(config['queue'])
    config['acquire']['holder']['pid'] = os.getpid()
    request = dict(config['acquire'])
    holder = request.pop('holder')
    attempt = request.pop('attempt')
    owner = request.pop('owner_action_key')
    namespace = request.pop('consumer_action_key')
    acquired = client.acquire_for(
        {'queue_root': str(q.root), 'action_key': owner, **attempt,
         'host': holder['host'], 'worker': OWNER},
        material_namespace=namespace, **request)
    connection.send(acquired)
    if not acquired['ok']:
        return
    try:
        command = connection.recv()
        if command == 'read':
            fd, _ = client.open_pinned(q, acquired['pin'], acquired['ref_id'],
                config['key'], residency_root=config['residency'])
            try:
                payload = os.read(fd, 65536)
                assert payload == config['expected']
                connection.send({'bytes': len(payload), 'sha256': hashlib.sha256(payload).hexdigest()})
            finally:
                os.close(fd)
            command = connection.recv()
        assert command == 'release'
    finally:
        assert reader_lease.release(q, acquired['pin_id'], acquired['ref_id'],
            consumer_action_key=config['acquire']['owner_action_key'],
            residency_root=config['residency'])
        connection.close()


def _actor(connection, config):
    q = pool.PoolQueue(config['queue'])
    connection.send('ready')
    command = connection.recv()
    if command == 'copy_evict':
        # Copy actual immutable input bytes to a private candidate, then ask
        # the production publication gate to arbitrate the occupied path.
        # Identical content should be adopted, not replace a pinned inode.
        cas = pb.PrismaBuildCAS(config['cas_root'])
        source = cas.input_path(config['input'])
        destination = Path(config['destination'])
        publisher = staged_path.StagedPublisher(queue=q,
            stage_root=config['stage_root'], residency_root=config['residency'],
            mover_action_key=config['copy_mover'], consumer_action_key=config['consumer'],
            manifest_sha256=config['manifest_sha256'], tier_id=config['tier_id'],
            cas_root=config['cas_root'])
        with tempfile.TemporaryDirectory(dir=destination.parent,
                prefix='.lifecycle-copy-') as private:
            candidate = Path(private) / 'copy.partial'
            shutil.copyfile(source, candidate)
            computed = hashlib.sha256(candidate.read_bytes()).hexdigest()
            assert computed == config['input']['sha256']
            copied = candidate.stat().st_size
            outcome = publisher.publish(config['entry'], destination, candidate, computed)
        connection.send({'copied_bytes': copied, 'publication': outcome})
        assert connection.recv() == 'evict'
        result = staged_path.evict(q, config['mover'],
            consumer_action_key=config['consumer'], stage_root=config['stage_root'],
            residency_root=config['residency'], whole=True)
        connection.send(result)
        assert connection.recv() == 'stop'
    elif command == 'evict':
        result = staged_path.evict(q, config['mover'],
            consumer_action_key=config['consumer'], stage_root=config['stage_root'],
            residency_root=config['residency'], whole=True)
        connection.send(result)
        assert connection.recv() == 'stop'
    elif command == 'promote':
        result = ram_promote.promote(ram_promote.build_parser().parse_args(config['args']))
        connection.send(result)
        assert connection.recv() == 'stop'
    else:
        assert command == 'stop'
    connection.close()


def _receive(connection):
    assert connection.poll(45), 'child failed to acknowledge lifecycle barrier'
    return connection.recv()


def _containment_probe(root):
    """Inspect actual native prerequisites, never infer them from OS name."""
    mountinfo = Path('/proc/self/mountinfo')
    cgroup = Path('/proc/self/cgroup')
    observations = []
    native = []
    if mountinfo.exists():
        for line in mountinfo.read_text().splitlines():
            if ' - cgroup2 ' in line:
                native.append(line)
        observations.append({'cgroup2_mounts': native})
    else:
        observations.append({'missing': str(mountinfo)})
    # A live broker is NOT an isolated fixture authority. ResourceScope accepts
    # a socket override, but validates cgroup identity under the production
    # prismabuild.slice; the shipped broker CLI has no fixture cgroup-root
    # option. Its SystemdBackend.create starts a host slice and sets properties.
    # Do not send create/recover_create to the live service for a fixture.
    # Inspect local delegation separately: a read-only cgroup tree is expected
    # with the broker design, and does not prove native production unavailable.
    controllers = Path('/sys/fs/cgroup/cgroup.controllers')
    observations.append({'controllers': controllers.read_text().strip() if controllers.exists() else None,
                         'delegation_writable': os.access(controllers.parent, os.W_OK),
                         'self_cgroup': cgroup.read_text() if cgroup.exists() else None})
    # Connecting is non-mutating: do not create a privileged scope just to
    # discover that its supervisor/broker is unavailable. Use the production
    # endpoint constant rather than a local deployment pathname.
    missing = []
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        probe.settimeout(2)
        try:
            probe.connect(str(resource_scope.BROKER_SOCKET))
        except OSError as error:
            missing.append(f'resource scope broker connection failed: {type(error).__name__}: {error}')
            observations.append({'broker_socket': str(resource_scope.BROKER_SOCKET),
                                 'connect_errno': error.errno, 'connect_error': str(error)})
        else:
            observations.append({'broker_socket': str(resource_scope.BROKER_SOCKET),
                                 'connected': True})
    if not native:
        missing.append('kernel mountinfo has no cgroup v2 mount')
    if not controllers.exists():
        missing.append('kernel cgroup controller interface is missing')
    # A distinct prerequisite from host native support: authority to create an
    # isolated native fixture. The only endpoint this lane has is the production
    # endpoint, which the task expressly forbids using as a fixture. Changing a
    # socket pathname alone would not isolate the broker's systemd operations.
    observations.append({'fixture_isolation': {
        'broker_endpoint_source': 'resource_scope.BROKER_SOCKET (production)',
        'broker_mutating_requests_sent': False,
        'resource_scope_socket_override_supported': True,
        'resource_scope_custom_cgroup_root_supported': False,
        'broker_cli_custom_cgroup_root_supported': False,
        'production_broker_fixture_use_authorized': False,
        'fixture_root': str(root),
        'fixture_isolation_established': False}})
    host_missing = list(missing)
    # Exercise the necessary kernel isolation operation in a disposable child,
    # not in pytest or the worker's admitted cgroup. No host mount, scope or
    # service is modified. libc avoids requiring an external unshare utility.
    isolation_probe = subprocess.run([sys.executable, '-c', '''
import ctypes, json, os, tempfile
libc = ctypes.CDLL(None, use_errno=True)
flags = 0x10000000 | 0x00020000 | 0x02000000 | 0x20000000
uid, gid = os.getuid(), os.getgid()
operation = "unshare(user,mount,cgroup,pid)"
result = libc.unshare(flags)
if not result:
    operation = "map fixture namespace uid/gid"
    try:
        with open("/proc/self/uid_map", "w") as stream:
            stream.write(f"0 {uid} 1\\n")
        with open("/proc/self/setgroups", "w") as stream:
            stream.write("deny\\n")
        with open("/proc/self/gid_map", "w") as stream:
            stream.write(f"0 {gid} 1\\n")
    except OSError as exc:
        print(json.dumps({"operation": operation, "errno": exc.errno, "reason": str(exc)}))
        raise SystemExit(1)
    # A writable cgroup2 mount/delegation is necessary for an isolated broker.
    # This mount exists only in this child's mount namespace and under TMPDIR.
    operation = "mount isolated fixture cgroup2"
    with tempfile.TemporaryDirectory(prefix="pb2100-cgroup-") as target:
        result = libc.mount(b"none", os.fsencode(target), b"cgroup2", 0, None)
        error = ctypes.get_errno() if result else 0
        if not result:
            try:
                operation = "create isolated delegated fixture cgroup"
                path = os.path.join(target, "pb2100-fixture")
                os.mkdir(path)
                os.rmdir(path)
            except OSError as exc:
                result, error = -1, exc.errno
            finally:
                libc.umount2(os.fsencode(target), 2)
else:
    error = ctypes.get_errno()
print(json.dumps({"operation": operation, "result": result, "errno": error,
                  "reason": os.strerror(error) if error else "isolated delegation succeeded"}))
raise SystemExit(1 if result else 0)
'''], capture_output=True, text=True, timeout=10)
    isolation_evidence = {'returncode': isolation_probe.returncode,
                          'stdout': isolation_probe.stdout.strip(),
                          'stderr': isolation_probe.stderr.strip()}
    observations.append({'native_isolation_probe': isolation_evidence})
    if isolation_probe.returncode:
        missing.append('isolated native fixture namespace entry refused: '
                       + isolation_probe.stdout.strip() + isolation_probe.stderr.strip())
    else:
        # Namespace entry alone is not a broker/supervisor qualification. Fail
        # explicitly rather than turn an unimplemented native lane into a pass.
        raise RuntimeError('native namespace entry available; isolated broker/supervisor '
                           'recovery qualification is not implemented by this portable lane')
    return {'status': 'unqualified', 'evidence': observations,
            'limitation': 'native_fixture_isolation_unavailable',
            'qualification_complete': False,
            'host_missing_prerequisites': host_missing,
            'missing_prerequisites': missing, 'reason': '; '.join(missing),
            # An unavailable fixture must not conceal outstanding harness work.
            # Even on a suitably isolated host this lane is not a recovery test.
            'recovery_implementation': 'unsupported',
            'unimplemented_operations': [
                'native attempt execution and empty-scope terminal retention',
                'queue execute/reap retry with immutable result publication',
                'verified result read and retry adoption byte/accounting checks']}


def run_case(case, *, root: Path, sources: dict[str, Path], observe):
    assert case in {'lifecycle', 'progress', 'recovery'}
    root = Path(root)
    q = pool.PoolQueue(root / 'queue'); q.records.ensure_layout()
    residency = q.root / pool.record_store.RESIDENCY
    stage = root / 'stage'; stage.mkdir()
    ram = root / 'ram'; ram.mkdir()
    staged_path.register_stage_root(q, tier_id=STAGE, stage_root=stage)
    staged_path.register_stage_root(q, tier_id=RAM, stage_root=ram)
    epoch = str(storage_tiers.ensure_ram_epoch(ram, host=socket.gethostname())['epoch'])
    q.tier(STAGE).mint({'stage_gib': 8})
    q.tier(RAM).mint({'ram_gib': 8})
    q.announce_tier({'tier_id': RAM, 'kind': 'ram', 'stage_root': str(ram), 'epoch': epoch})
    # Real immutable fixture entries: a whole file and the SECOND complete
    # manifest entry (middle extent of split.bin), never an intra-entry cut.
    payloads = [sources['whole.bin'].read_bytes(), sources['split.bin'].read_bytes()[4096:8192]]
    entries = [{'path': str(sources[name]), 'offset': offset, 'bytes': len(payload),
                'sha256': hashlib.sha256(payload).hexdigest()}
               for name, offset, payload in zip(('whole.bin', 'split.bin'), (0, 4096), payloads)]
    manifest = {'schema': data_manifest.DATA_MANIFEST_SCHEMA_V1,
        'produced_by': {'tool': 'connected-lifecycle'}, 'mount_prefix': str(root / 'sources'),
        'entries': entries, 'entry_count': 2, 'total_bytes': 8192,
        'annotations': {'phases': [{'name': 'p1', 'cumulative_bytes': 4096},
                                    {'name': 'p2', 'cumulative_bytes': 8192}]}}
    map_path = root / 'map.json'
    manifest_path = root / 'manifest.json'; manifest_path.write_text(json.dumps(manifest))
    cas = pb.PrismaBuildCAS(q.root / 'cas')
    manifest_input, _ = cas.ingest_input(manifest_path,
        input_id=data_manifest.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
    # Use the existing immutable input verifier (#2099), not a harness
    # substitute for its full-content CAS verification contract.
    manifest_path = cas.input_path(manifest_input)
    source_inputs = {}
    for name, path in sources.items():
        source_inputs[name], _ = cas.ingest_input(path, input_id='source-' + name)
        cas.input_path(source_inputs[name])
    digest = str(manifest_input['sha256'])
    checkout = root / 'checkout'; checkout.mkdir()
    (checkout / 'task_code.py').write_text('raise SystemExit(0)\n')
    processes, channels, pins, publications = [], [], [], []
    claims = []
    rows = {}
    ctx = mp.get_context('fork')
    ram_mount_options = storage_tiers.ram_mount_options(str(ram))
    capabilities = {'native_containment': _containment_probe(root),
        'ram_broker': {'status': 'unqualified', 'evidence': {
            'fixture_path': str(ram), 'mode': 'file_backed',
            'observed_tmpfs_mount_options': ram_mount_options,
            'epoch_marker_present': bool(epoch)},
            'reason': ('kernel mount table has no tmpfs mounted at the fixture RAM root'
                       if ram_mount_options is None else
                       'fixture mount observed, but native RAM broker protocol is not exercised')}}
    retained = {}
    recovery = {'status': 'unqualified', 'reason': capabilities['native_containment']['reason'],
                'prerequisite_limitation': capabilities['native_containment']['limitation'],
                'implementation_status': capabilities['native_containment']['recovery_implementation'],
                'unimplemented_operations': capabilities['native_containment']['unimplemented_operations'],
                'blocked_operation': 'containment-authorized crashed-reader retirement/adoption', 'adoptions': 0}

    def seal(command, identity):
        action = pb.seal_action({
            'schema': pb.ACTION_SCHEMA_V2,
            'task': {'definition_id': 'tests/connected-' + identity,
                     'definition_version': 'v1', 'task_class': 'generation',
                     'determinism': 'deterministic', 'artifact_family': 'generic',
                     'artifact_kind': 'generic', 'argv': [sys.executable, '-c', 'pass'],
                     'working_directory': '.', 'result_path': 'result'},
            'inputs': [manifest_input, *source_inputs.values()],
            'code_closure': pb.build_code_closure(checkout, ['task_code.py']),
            'params': {'command': command}, 'environment': {'variables': {}, 'toolchain': {}},
            'execution_scope': {'portability': 'portable', 'platform_key': None, 'host_class': None}})
        cas.publish_action_request(action)
        return str(action['action_key'])

    def publish(key, tier=None, start=0, end=4096):
        resources = {'cpu': 1, 'mem_gb': 1}
        extra = {}
        if tier:
            resources[('ram_gib@' if tier == RAM else 'stage_gib@') + tier] = 1
            extra['residency'] = {'schema': pool.RESIDENCY_SCHEMA_V1,
                'tier_id': tier, 'manifest_sha256': digest, 'manifest_bytes': 8192,
                'range_start_bytes': start, 'range_end_bytes': end}
        row = q.publish(action_key=key, cas_root=str(q.root / 'cas'),
            checkout_root=str(checkout), worker_script=str(checkout / 'task_code.py'),
            resources=resources, max_attempts=1, retry_safe=False, **extra)
        rows[key] = json.loads(row.read_text())
        claim = q.claim(owner=OWNER, capacity={'cpu': 16, 'mem_gb': 16}, tags=[])
        assert claim is not None and claim['action_key'] == key, claim
        claims.append(key)
        return claim

    def finish(key):
        q.finish(key, status='executed'); claims.remove(key)

    def move(key, start, end):
        args = stage_move.build_parser().parse_args([
            '--pool-root', str(q.root), '--cas-root', str(q.root / 'cas'),
            '--action-key', key, '--consumer-action-key', consumer,
            '--tier-id', STAGE, '--stage-root', str(stage),
            '--manifest', str(manifest_path), '--manifest-sha256', digest,
            '--range-start-bytes', str(start), '--range-end-bytes', str(end),
            '--residency-root', str(residency), '--readers', '1', '--max-readers', '1',
            '--block', '4096', '--unpaced'])
        receipt = stage_move.move(args)
        assert receipt['complete'], receipt
        q.record_move(key, receipt)

    def spawn(target, config):
        parent, child = ctx.Pipe()
        process = ctx.Process(target=target, args=(child, config))
        process.start(); child.close()
        processes.append(process); channels.append(parent)
        return process, parent

    def joined(process):
        process.join(45)
        assert not process.is_alive() and process.exitcode == 0, (process.pid, process.exitcode)

    def base(pub):
        key = next(iter(pub['expected']))
        material = reader_lease.read_material(residency, consumer, pub['mover'])
        return {'queue': q, 'residency_root': residency, 'consumer': consumer,
            'mover': pub['mover'], 'tier_id': pub['tier_id'], 'stage_root': pub['stage_root'],
            'protected_path': material['entries'][key]['stage_path'],
            'range_key': key, 'expected_bytes': pub['expected'][key], 'publications': publications,
            'map_path': map_path}

    def acquire_config(pub, token, *, wrong_epoch=False):
        return {'consumer_action_key': consumer,
            'owner_action_key': hashlib.sha256(('reader-' + token).encode()).hexdigest(),
            'attempt': {'nonce': 'nonce-' + token, 'scope_id': 'scope-' + token},
            'tier_id': pub['tier_id'], 'epoch': 'wrong-epoch' if wrong_epoch else pub['epoch'],
            'span': {'start_bytes': 0, 'end_bytes': 4096},
            'holder': {'host': socket.gethostname(), 'pid': os.getpid()},
            'acquire_token': token, 'covers': [{'mover_action_key': pub['mover'],
                                             'manifest_sha256': digest}],
            'residency_root': str(residency)}

    def start_reader(pub, token):
        context = base(pub)
        config = acquire_config(pub, token)
        # Child replaces holder pid with its actual process identity below.
        process, channel = spawn(_reader, {'queue': str(q.root), 'acquire': config,
            'residency': str(residency), 'key': context['range_key'], 'expected': context['expected_bytes']})
        pin = _receive(channel); assert pin['ok'], pin
        pins.append(pin)
        return process, channel, pin

    try:
        consumer = seal(['consumer', '--manifest', str(manifest_path)], 'consumer')
        claim = publish(consumer)
        whole = seal(['stage_move.py', '--range-start-bytes', '0', '--range-end-bytes', '4096'], 'whole')
        split = seal(['stage_move.py', '--range-start-bytes', '4096', '--range-end-bytes', '8192'], 'split')
        ram_key = seal(['ram_promote.py', '--source-stage-root', str(stage),
                        '--range-start-bytes', '0', '--range-end-bytes', '4096'], 'ram')
        for ordinal, key in enumerate((whole, split)):
            publish(key, STAGE, ordinal * 4096, (ordinal + 1) * 4096)
            move(key, ordinal * 4096, (ordinal + 1) * 4096); finish(key)
            material = reader_lease.read_material(residency, consumer, key)
            publications.append({'kind': ('whole', 'split')[ordinal], 'consumer': consumer,
                'mover': key, 'tier_id': STAGE, 'stage_root': stage,
                'manifest_sha256': digest, 'epoch': '',
                'expected': {next(iter(material['entries'])): payloads[ordinal]}})
        source_reader = start_reader(publications[0], 'source-handoff')
        publish(ram_key, RAM)
        promotion, promotion_channel = spawn(_actor, {'queue': str(q.root), 'args': [
            '--pool-root', str(q.root), '--action-key', ram_key,
            '--consumer-action-key', consumer, '--tier-id', RAM, '--ram-root', str(ram),
            '--source-stage-root', str(stage), '--manifest', str(manifest_path),
            '--manifest-sha256', digest, '--range-start-bytes', '0', '--range-end-bytes', '4096',
            '--residency-root', str(residency)]})
        assert _receive(promotion_channel) == 'ready'
        # Actual live-claim handoff, not a fabricated occupancy document. The
        # source reader pins before the promotion process is allowed to copy.
        source_holdings = q.tier(STAGE).holdings(whole)
        deferred = staged_path.evict(q, whole, consumer_action_key=consumer,
            stage_root=stage, residency_root=residency, whole=True)
        assert deferred['tokens_released'] == 0, deferred
        assert q.tier(STAGE).holdings(whole) == source_holdings
        promotion_channel.send('promote')
        receipt = _receive(promotion_channel)
        assert receipt['complete'], receipt
        q.record_move(ram_key, receipt)
        source_reader[1].send('read')
        assert _receive(source_reader[1])['bytes'] == 4096
        source_reader[1].send('release'); joined(source_reader[0])
        promotion_channel.send('stop'); joined(promotion)
        finish(ram_key)
        material = reader_lease.read_material(residency, consumer, ram_key)
        publications.append({'kind': 'ram', 'consumer': consumer, 'mover': ram_key,
            'tier_id': RAM, 'stage_root': ram, 'manifest_sha256': digest, 'epoch': epoch,
            'expected': {next(iter(material['entries'])): payloads[0]}})
        map_path = root / 'map.json'
        fragments = residency_map.read_fragments(residency, consumer)
        mapping = residency_map.compose([f for f in fragments if f['tier_id'] == STAGE])
        mapping = residency_map.overlay_ram(mapping,
            [f for f in fragments if f['tier_id'] == RAM],
            ram_tier_id=RAM, ram_root=str(ram), ram_epoch=epoch)
        residency_map.write_map(map_path, mapping)
        pub = publications[2]  # same incarnation across every held barrier
        first = start_reader(pub, 'first')
        second = start_reader(pub, 'second')
        actor, actor_channel = spawn(_actor, {'queue': str(q.root), 'mover': ram_key,
            'consumer': consumer, 'stage_root': str(ram), 'residency': str(residency),
            'cas_root': str(cas.root), 'input': source_inputs['whole.bin'],
            'entry': entries[0], 'destination': base(pub)['protected_path'],
            'copy_mover': hashlib.sha256(b'lifecycle-competing-copy').hexdigest(),
            'manifest_sha256': digest, 'tier_id': RAM})
        assert _receive(actor_channel) == 'ready'
        context = base(pub)
        context.update(live_processes=[first[0], second[0], actor], pins=[first[2], second[2]],
                       claimed_unix=claim['claimed_unix'])
        if case == 'lifecycle':
            observe('published', context)
            observe('readers_held', context)
            before = reader_lease.read_material(residency, consumer, ram_key)
            protected = Path(context['protected_path'])
            identity = protected.stat()
            supply = q.tier(RAM).free_supply()
            actor_channel.send('copy_evict')
            competing = _receive(actor_channel)
            assert competing['copied_bytes'] == len(payloads[0]), competing
            assert (protected.stat().st_dev, protected.stat().st_ino) == (identity.st_dev, identity.st_ino)
            assert reader_lease.read_material(residency, consumer, ram_key) == before
            assert q.tier(RAM).free_supply() == supply
            context['competing_copy'] = competing
            actor_channel.send('evict')
            attempted = _receive(actor_channel)
            assert q.tier(RAM).free_supply() == supply
            assert attempted['tokens_released'] == 0, attempted
            observe('copy_evict_held', context)
            for reader in (first, second):
                reader[1].send('read'); assert _receive(reader[1])['bytes'] == 4096
            first[1].send('release'); joined(first[0])
            context.update(live_processes=[second[0], actor], pins=[second[2]])
            observe('one_reader_left', context)
            wrong = reader_lease.acquire(q, **acquire_config(pub, 'wrong', wrong_epoch=True))
            assert not wrong['ok'], wrong
            observe('wrong_epoch', {**context, 'acquisition': wrong})
            second[1].send('release'); joined(second[0])
            actor_channel.send('stop'); joined(actor)
            finish(consumer)
            observe('last_release', context)
            # Observers may independently retire here, but harness correctness
            # must not depend on an observer performing its cleanup for it.
            cleanup_args = dict(consumer_action_key=consumer, stage_root=ram,
                                residency_root=residency, whole=True)
            retired = staged_path.evict(q, ram_key, **cleanup_args)
            assert retired['complete'], retired
            supply = q.tier(RAM).free_supply()
            assert staged_path.evict(q, ram_key, **cleanup_args)['complete']
            assert q.tier(RAM).free_supply() == supply
            assert not Path(context['protected_path']).exists()
            # Re-publish by running the real promotion again, not by editing a
            # material generation or copying bytes into a visible path.
            # Promotion requires a live claim; remove no records manually.
            publish(ram_key, RAM)
            receipt = ram_promote.promote(ram_promote.build_parser().parse_args([
                '--pool-root', str(q.root), '--action-key', ram_key,
                '--consumer-action-key', consumer, '--tier-id', RAM, '--ram-root', str(ram),
                '--source-stage-root', str(stage), '--manifest', str(manifest_path),
                '--manifest-sha256', digest, '--range-start-bytes', '0', '--range-end-bytes', '4096',
                '--residency-root', str(residency)]))
            assert receipt['complete'], receipt
            q.record_move(ram_key, receipt); finish(ram_key)
            observe('republished', {**base(pub), 'old_acquisition': first[2]})
        elif case == 'progress':
            phases = [{'name': 'p1', 'start_bytes': 0, 'end_bytes': 4096,
                       'mover_row': rows[whole], 'egress_row': {'action_key': '1' * 64}},
                      {'name': 'p2', 'start_bytes': 4096, 'end_bytes': 8192,
                       'mover_row': rows[split], 'egress_row': {'action_key': '2' * 64}}]
            plan = residency_plan.build_plan(consumer_action_key=consumer, tier_id=STAGE,
                stage_root=str(stage), manifest_sha256=digest, manifest_bytes=8192, phases=phases)
            residency_plan.freeze(q, plan)
            reporter = ClaimedProgressReporter(q, consumer, OWNER, root / 'progress')
            reporter.commit('p1', 1); observe('before_acceptance', context)
            reporter.commit('p2', 2); observe('accepted', context)
            for event in reporter.check_rejections():
                observe('stale_attempt' if event['label'] == 'stale' else event['label'], context)
            for reader in (first, second):
                reader[1].send('release'); joined(reader[0])
            actor_channel.send('stop'); joined(actor)
        else:
            # Bind identifiers to the actual reader ref that cleanup would
            # have to retire. Missing nonce would only test malformed input.
            acquired = first[2]
            pin = reader_lease.validate_pin(acquired['pin'])
            attempt = pin['refs'][acquired['ref_id']]['attempt']
            identifiers = {'action_key': pin['owner_action_key'], 'nonce': attempt['nonce']}
            scope_probes = {}
            for name, certificate, expected_reason in (
                ('missing_scope', dict(identifiers), 'certificate names no attempt'),
                ('wrong_scope', {**identifiers, 'scope_id': attempt['scope_id'] + '-wrong'},
                 'no-broker-attestation-retain')):
                ok, reason = client.containment_certificate_ok(q, certificate)
                assert (ok, reason) == (False, expected_reason), (name, ok, reason)
                probe = {'certificate': certificate, 'reason': reason,
                         'status': 'unqualified' if name == 'wrong_scope' else 'refused',
                         'scope_mismatch_exercised': False}
                if name == 'wrong_scope':
                    # A real authority lookup finds no attestation for this
                    # portable attempt. That is NOT a qualified scope mismatch.
                    probe['blocked_operation'] = 'authoritative scope mismatch verification'
                scope_probes[name] = probe
                observe(name, {**context, 'certificate': certificate, 'scope_probe': probe})
            recovery['scope_probes'] = scope_probes
            # Simulate interruption without inventing an authoritative terminal.
            # The abandoned refs/charged occupancy remain explicitly retained.
            for reader in (first, second):
                reader[0].kill(); reader[0].join(10)
                assert not reader[0].is_alive()
            actor_channel.send('stop'); joined(actor)
            owners, tainted = reader_lease.live_for(q, None, residency_root=residency)
            assert not tainted and owners
            retained = {'reason': recovery['reason'], 'owners': owners,
                'material': reader_lease.read_material(residency, consumer, ram_key),
                'ram_holdings': q.tier(RAM).holdings(ram_key),
                'protected_file_id': reader_lease.stat_identity(context['protected_path'])}
            assert retained['ram_holdings'] and retained['material']

        # Public egress performs deletion before accounting return. Refusals in
        # recovery leave the crashed readers' RAM incarnation and tokens charged.
        for publication in reversed(publications):
            result = staged_path.evict(q, publication['mover'], consumer_action_key=consumer,
                stage_root=publication['stage_root'], residency_root=residency, whole=True)
            if case == 'recovery' and publication['kind'] == 'ram':
                assert not result['complete'] and result['tokens_released'] == 0, result
                assert q.tier(RAM).holdings(ram_key) == retained['ram_holdings']
                assert reader_lease.read_material(residency, consumer, ram_key) == retained['material']
                assert reader_lease.file_id_matches(retained['protected_file_id'],
                    reader_lease.stat_identity(context['protected_path']))
                assert Path(context['protected_path']).read_bytes() == payloads[0]
                retained['refused_cleanup'] = result
            else:
                assert result['complete'], result
        if consumer in claims:
            finish(consumer)
        if retained:
            retained['claims'] = []
            retained['holdings'] = {tier: q.tier(tier).holdings() for tier in (STAGE, RAM)}
        cas.input_path(manifest_input)
        for descriptor in source_inputs.values():
            cas.input_path(descriptor)
        return {'queue': q, 'residency_root': residency, 'processes': processes,
            'competing_copy': context.get('competing_copy'),
            'tier_ids': [STAGE, RAM], 'ram_fixture': 'file_backed',
            'capabilities': capabilities, 'retained': retained, 'recovery': recovery}
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate(); process.join(5)
                if process.is_alive():
                    process.kill(); process.join(5)
        for channel in channels:
            channel.close()
