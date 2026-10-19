"""Sealed pre-execution populations and Git source pins."""
import json
import importlib.util
from pathlib import Path
import subprocess
from .compatibility_window import WindowError, BootstrapQualification
from .digest_primitives import raw_sha256, canonical_sha256

CORPUS_FILES = tuple('tests/test_record_compatibility_' + name + '.py' for name in (
    'queue', 'storage', 'ownership', 'outputs', 'host', 'decomposition', 'core', 'controller'))
ROOT_FILES = ('tests/test_client_bootstrap_root.py', 'tests/test_receipt_reader_root.py')
QUALIFICATION_SOURCES = (*CORPUS_FILES, *ROOT_FILES, 'tests/run_proof_container.py',
    'tests/proof_container_runtime.py', 'tests/proof_execution.json', 'tests/receipt_execution.json',
    'tools/fleet/bootstrap_client.py', 'tools/fleet/upgrade_client.py')


def _qualification_source(checkout, commit, name):
    path = Path(name)
    if path.is_absolute() or '..' in path.parts or not path.parts:
        raise WindowError('invalid qualification source path')
    return subprocess.check_output(['git', '-C', str(checkout), 'show', commit + ':' + name])


def _qualification_index(assessment, checkout, source_reader=None):
    """Required nodes come from the separately sealed release index, never runs."""
    source_reader = source_reader or _qualification_source
    pin = assessment.get('qualification_index', {})
    path = Path(pin.get('path', ''))
    if not path.is_file() or path.stat().st_mode & 0o222:
        raise WindowError('bootstrap requires a sealed qualification index')
    raw = path.read_bytes()
    if (len(raw) > 16 * 1024 * 1024 or raw_sha256(raw) != pin.get('sha256')
            or assessment['plan'].get('qualification_index_sha256') != pin.get('sha256')):
        raise WindowError('bootstrap qualification index pin differs')
    index = json.loads(raw)
    commit = assessment['plan']['target']['commit']
    if (index.get('schema') != 'prismabuild.bootstrap_qualification_index.v1'
            or index.get('target_commit') != commit
            or set(index.get('root_routes', {})) != {'aarch64', 'x86_64'}):
        raise WindowError('bootstrap qualification index source/routes differ')
    for kind, files in (('root', ROOT_FILES), ('corpus', CORPUS_FILES)):
        population = index.get('populations', {}).get(kind, {})
        nodes = population.get('nodes', [])
        if (set(population.get('files', [])) != set(files)
                or len(population['files']) != len(files) or not nodes
                or any(not isinstance(node, str) or '::' not in node for node in nodes)
                or len(nodes) != len(set(nodes))
                or {node.split('::')[0] for node in nodes} != set(files)):
            raise WindowError('bootstrap qualification index must declare the exact required files and nodes')
    sources = index.get('source_sha256', {})
    if not set(QUALIFICATION_SOURCES) <= sources.keys():
        raise WindowError('bootstrap qualification index lacks required source pins')
    for name, expected in sources.items():
        if raw_sha256(source_reader(checkout, commit, name)) != expected:
            raise WindowError('bootstrap qualification index source hash differs: ' + name)
    return index



def _seal_index(index, out):
    import os
    raw = (json.dumps(index, sort_keys=True, indent=2) + '\n').encode()
    path = Path(out)
    try:
        with path.open('xb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
            os.fchmod(stream.fileno(), 0o444)
    except FileExistsError as exc:
        raise WindowError('qualification index destination already exists') from exc
    return index


def _validate_index(path, commit, checkout):
    digest = raw_sha256(Path(path).read_bytes())
    return _qualification_index({'qualification_index': {'path': str(path), 'sha256': digest},
        'plan': {'target': {'commit': commit}, 'qualification_index_sha256': digest}}, checkout)


def generate_qualification_index(*, checkout, target_commit, root_profiles, corpus_routes, collect, out):
    from .release_identity import resolve
    commit = resolve(checkout, target_commit)
    if set(root_profiles) != {'aarch64', 'x86_64'} or not corpus_routes:
        raise WindowError('qualification requires both native profiles and corpus routes')
    index = {'schema': 'prismabuild.bootstrap_qualification_index.v1',
        'target_commit': commit, 'root_routes': {}, 'corpus_routes': corpus_routes,
        'populations': {}, 'source_sha256': {}}
    for name in (*QUALIFICATION_SOURCES, *root_profiles.values()):
        index['source_sha256'][name] = raw_sha256(_qualification_source(checkout, commit, name))
    for machine, name in root_profiles.items():
        profile = json.loads(_qualification_source(checkout, commit, name))
        if profile['primary']['elf_machine'] != {'aarch64': 183, 'x86_64': 62}[machine]:
            raise WindowError('root profile architecture differs')
        index['root_routes'][machine] = {'profile': name,
            'profile_sha256': index['source_sha256'][name]}
    for kind, files, routes in (('root', ROOT_FILES, index['root_routes']),
                                ('corpus', CORPUS_FILES, corpus_routes)):
        populations = []
        for machine, route in routes.items():
            if kind == 'root':
                profile = json.loads(_qualification_source(checkout, commit, route['profile']))
                commands = [[profile['primary']['executable']['path'], 'tests/run_proof_container.py',
                    '--runtime-profile', route['profile'], *files]]
            else:
                commands = route.get('commands', [])
                if not commands or any(not command or not Path(command[0]).is_absolute()
                    or {arg for arg in command if arg.startswith('tests/') and arg.endswith('.py')} != set(files)
                    or any(command.count(name) != 1 for name in files)
                    for command in commands):
                    raise WindowError('corpus route must approve the complete pytest population')
            nodes = list(collect(kind=kind, machine=machine, files=list(files),
                commands=commands, route=route, target_commit=commit))
            if (not nodes or len(nodes) != len(set(nodes))
                or any(not isinstance(node, str) or '::' not in node for node in nodes)
                or {node.split('::')[0] for node in nodes} != set(files)):
                raise WindowError('independent collection lacks exact file/node population')
            populations.append(sorted(nodes))
        if any(nodes != populations[0] for nodes in populations):
            raise WindowError('qualification collections disagree across native routes')
        index['populations'][kind] = {'files': list(files), 'nodes': populations[0]}
    return _seal_index(index, out)


def rebind_qualification_index(*, checkout, requested_commit, generation, index_path, out):
    from .release_identity import release_snapshot_identity
    identity = release_snapshot_identity(checkout=checkout, requested_commit=requested_commit,
                                         generation=generation)
    raw = Path(index_path).read_bytes()
    index = _validate_index(index_path, identity['requested_commit'], checkout)
    index['rebound_from'] = {'target_commit': index['target_commit'], 'sha256': raw_sha256(raw)}
    index['target_commit'] = identity['snapshot_commit']
    index['snapshot_identity'] = identity
    return _seal_index(index, out)


def _require_qualification_execution(action, snapshot, *, kind, machine, index):
    """Bind approved metadata to Core's executed argv and materialized checkout."""
    from .action_result import ActionResultError, bind_standard_capture_command
    try:
        bind_standard_capture_command(action)
    except ActionResultError as exc:
        raise WindowError('bootstrap qualification execution recipe differs: ' + str(exc)) from exc
    if (action['task'].get('definition_id') != 'fleet/pbrun'
            or action['task'].get('working_directory') != '.'
            or action['params'].get('cwd') != '.' or snapshot['subdirectory'] != '.'
            or snapshot['input'] not in action['inputs']):
        raise WindowError('bootstrap qualification checkout/cwd/input route differs')
    if kind == 'corpus':
        command = action['params']['command']
        approved = index.get('corpus_routes', {}).get(machine, {}).get('commands', [])
        if (command not in approved or not Path(command[0]).is_absolute()
                or action['params'].get('interpreter') != command[0]):
            raise WindowError('corpus qualification command is outside the sealed approved routes')


def _require_root_route(action, attestation, files, machine, index, checkout, source_reader):
    route = index['root_routes'].get(machine)
    if route is None:
        raise WindowError('root qualification architecture is outside the sealed index')
    raw = source_reader(checkout, index['target_commit'], route['profile'])
    if (raw_sha256(raw) != route['profile_sha256']
            or index['source_sha256'].get(route['profile']) != route['profile_sha256']):
        raise WindowError('root qualification profile differs from the sealed index')
    profile = json.loads(raw)
    primary = profile['primary']
    command = [primary['executable']['path'], 'tests/run_proof_container.py',
               '--runtime-profile', route['profile'], *files]
    if (action['params'].get('command') != command
            or action['params'].get('interpreter') != command[0]
            or attestation.get('profile_sha256') != route['profile_sha256']
            or primary['elf_machine'] != {'aarch64': 183, 'x86_64': 62}[machine]):
        raise WindowError('root qualification did not use the approved runner/profile route')
    for name, expected in (('outer', primary), ('inner', primary), ('installed_reader', profile['installed_reader'])):
        observed = attestation.get(name, {})
        if any(observed.get(key) != expected[key] for key in ('version', 'executable', 'elf_machine')):
            raise WindowError('root qualification attested runtime differs from its profile')
    if attestation.get('image', {}).get('actual') != profile['image']:
        raise WindowError('root qualification image differs')
    sources = attestation.get('source_sha256', {})
    required = {*files, 'tests/run_proof_container.py', 'tests/proof_container_runtime.py', route['profile']}
    if not required <= sources.keys():
        raise WindowError('root qualification attestation lacks source pins')
    for name, expected in sources.items():
        if raw_sha256(source_reader(checkout, index['target_commit'], name)) != expected:
            raise WindowError('root qualification attested source hash differs: ' + name)


def verify_qualification(assessment, *, checkout, fleet_root, source_reader=None):
    """Verify actual CAS actions/logs, exact source and whole-file populations.

    The operator index supplies keys, never verdicts. Retained snapshots must
    differ from the release commit only by PB's own closure stamp.
    """
    from . import core as pb
    from . import pool
    source_reader = source_reader or _qualification_source
    plan = assessment['plan']
    index = _qualification_index(assessment, checkout, source_reader)
    tools = Path(checkout) / 'tools'
    if (tools / 'fleet').is_dir():
        tools /= 'fleet'
    def load(name):
        spec = importlib.util.spec_from_file_location('_bootstrap_' + name, tools / (name + '.py'))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    gate, outcomes = load('shape_gate'), load('pbtest_outcomes')
    cas = pb.PrismaBuildCAS(Path(fleet_root) / 'cas')
    lanes, keys, runtimes = {}, [], []
    expected_files = set((*CORPUS_FILES, *ROOT_FILES))
    for key in assessment['qualification_actions']:
        full, terminal = gate.resolve_action_key(Path(fleet_root) / 'pb-queue', key)
        if full in keys:
            raise WindowError('duplicate bootstrap qualification action')
        done = pool.record_store.read_document(terminal)
        if done.get('status') != 'executed' or done.get('detail', {}).get('returncode') != 0:
            raise WindowError('bootstrap qualification action did not execute successfully')
        action = cas.read_action_request(full)
        if action is None:
            raise WindowError('missing bootstrap qualification request')
        receipt = cas.lookup(action)
        if receipt is None:
            raise WindowError('missing verified bootstrap qualification CAS receipt')
        snapshot = pb.validate_pbrun_checkout_snapshot(action['params']['checkout_snapshot'])
        bundle = cas.input_path(snapshot['input'])
        gate._git(Path(checkout), 'bundle', 'unbundle', str(bundle))
        changed = gate._git(Path(checkout), 'diff-tree', '-r', '--no-renames', '--name-only',
                            plan['target']['commit'], snapshot['commit']).splitlines()
        if gate.closure_stamp_only(changed, snapshot.get('subdirectory') or '.'):
            raise WindowError('bootstrap qualification source differs from final release')
        log = cas.result_path(receipt, action).read_text(errors='strict')
        record = outcomes.parse(log)
        if not record or record.get('collect_only') or record.get('deselected'):
            raise WindowError('missing complete bootstrap qualification outcomes')
        nodes = record['collected']
        files = sorted({node.split('::')[0] for node in nodes})
        # Other required integration files may accompany a qualification row.
        if not set(files) <= expected_files:
            raise WindowError('qualification rows must name only the corpus and bootstrap root population')
        if set(files) & set(ROOT_FILES):
            records = [json.loads(line.removeprefix('prismabuild-root-runtime: '))
                       for line in log.splitlines() if line.startswith('prismabuild-root-runtime: ')]
            if (len(records) != 1 or records[0].get('complete') is not True
                    or records[0].get('mode') != 'execution' or records[0].get('exitstatus') != 0
                    or not records[0].get('installed_reader')
                    or not set(files) <= set(records[0].get('source_sha256', {}))):
                raise WindowError('root bootstrap qualification needs a complete qualified runtime attestation')
        kind = 'root' if set(files) & set(ROOT_FILES) else 'corpus'
        if kind == 'root' and not set(files) <= set(ROOT_FILES):
            raise WindowError('root and corpus qualification use separate populations')
        machine = receipt['producer']['evidence']['machine']
        _require_qualification_execution(action, snapshot, kind=kind, machine=machine, index=index)
        runtime_key = ''
        if kind == 'root':
            _require_root_route(action, records[0], files, machine, index, checkout, source_reader)
            runtime = {key: records[0]['installed_reader'][key] for key in ('version', 'executable', 'elf_machine')}
            if runtime not in runtimes:
                runtimes.append(runtime)
            runtime_key = canonical_sha256(runtime)
        rows = lanes.setdefault((kind, machine, runtime_key), [])
        rows.append({'shard': len(rows), 'files': files, 'returncode': 0,
                     'timed_out': False, 'output': log})
        keys.append(full)
    if (not {('root', 'aarch64'), ('root', 'x86_64')} <= {(kind, machine) for kind, machine, _ in lanes}
            or not any(kind == 'corpus' for kind, _, _ in lanes)):
        raise WindowError('bootstrap needs native ARM and x86 root qualification plus the exact record corpus')
    for (kind, _, _), rows in lanes.items():
        population = index['populations'][kind]
        verdict = outcomes.judge_shards(rows, requested_files=population['files'], required_nodes=population['nodes'])
        if not verdict['green']:
            raise WindowError('bootstrap qualification is incomplete or contains skips/failures')
    return BootstrapQualification(plan, action_keys=keys, runtimes=runtimes)

