#!/usr/bin/env python3
"""Verify and retain C15 PB primaries using only stdlib and immutable data."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess


QUEUE = Path('/mnt/shared/prismabuild-fleet/pb-queue')
CAS = Path('/mnt/shared/prismabuild-fleet/cas')
RETAINED_BINARIES = {
    'af955ef55333c8fc9c5aa50df91ad1a629d9a79a9afa125cd5e9629585f78015': Path(
        '/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/proof-recovery/arm-nfs-boundary-r2/primary/sparky-bash'),
    'c86f21d0da9290ece2d7d49a3f7ed52aed59cb029abdf0ea25c79634f6cbe960': Path(
        '/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/final-scratch-profile/sparky-python3.12'),
}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':'),
                          ensure_ascii=False, allow_nan=False).encode())


def checked(path, descriptor):
    raw = path.read_bytes()
    assert len(raw) == descriptor['bytes'] and sha(raw) == descriptor['sha256'], str(path)
    return raw


def blob(descriptor):
    return checked(CAS / 'blobs' / descriptor['sha256'][:2] / descriptor['sha256'], descriptor)


def git(root, *words):
    return subprocess.check_output(['git', '-C', str(root), *words], stderr=subprocess.PIPE)


def entries(root, revision):
    return {name.decode(): identity.decode() for identity, name in (
        row.split(b'\t', 1) for row in git(root, 'ls-tree', '-r', '-z', revision).split(b'\0') if row)}


def executable(descriptor):
    candidates = [Path(descriptor['resolved_path'])]
    if descriptor['sha256'] in RETAINED_BINARIES:
        candidates.append(RETAINED_BINARIES[descriptor['sha256']])
    for path in candidates:
        if path.is_file():
            raw = path.read_bytes()
            if len(raw) == descriptor['bytes'] and sha(raw) == descriptor['sha256']:
                return str(path)
    raise AssertionError('unavailable exact executable bytes: ' + repr(descriptor))


def capture(plan, row, destination, objects):
    key = row['action_key']
    terminal_paths = [QUEUE / state / (key + '.json') for state in ('done', 'failed')]
    present = [path for path in terminal_paths if path.exists()]
    assert len(present) == 1, (key, 'not one terminal')
    terminal_raw = present[0].read_bytes()
    terminal = json.loads(terminal_raw)
    assert terminal['action_key'] == key and terminal['priority'] == -10 and not terminal['needs_gpu']
    status = terminal['detail']['returncode']
    assert status == row.get('expected_returncode', 0)
    assert terminal['resources']['cpu'] <= 4 and terminal['resources']['mem_gb'] <= 8
    cleanup = terminal['resource_scope_cleanup']
    export = cleanup['export']
    assert cleanup['complete'] is True and export['tickets_pending'] is False
    assert all(export.get(name) is True for name in ('empty', 'ok', 'settled', 'released', 'stopped'))
    destination.mkdir(parents=True, exist_ok=True)
    (destination / 'terminal.json').write_bytes(terminal_raw)
    stdout = None
    for name in ('stdout_log', 'stderr_log'):
        descriptor = terminal['detail'][name]
        raw = checked(QUEUE / descriptor['path'], descriptor)
        (destination / (name + '.log')).write_bytes(raw)
        if name == 'stdout_log':
            stdout = raw
    request_raw = (CAS / 'requests' / key[:2] / (key + '.json')).read_bytes()
    request = json.loads(request_raw)
    assert canonical({name: value for name, value in request.items() if name != 'action_key'}) == key
    (destination / 'request.json').write_bytes(request_raw)
    environment = request['environment']['variables']
    assert environment['TMPDIR'] == '/home/rob/pbt17'
    assert all(environment[name] == '1' for name in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS'))
    snapshot = terminal['checkout_snapshot']
    assert snapshot == request['params']['checkout_snapshot']
    assert snapshot['parent'] == plan['source']
    bundle = destination / 'source.bundle'
    bundle.write_bytes(blob(snapshot['input']))
    git(objects, 'fetch', str(bundle), snapshot['commit'])
    expected = entries(Path(plan['checkout']), plan['source'])
    actual = entries(objects, snapshot['commit'])
    assert all(actual.get(name) == identity for name, identity in expected.items())
    extra = set(actual) - set(expected)
    stamps = [name for name in extra if name.startswith('.pbrun-closure.') and name.endswith('.json')]
    assert len(stamps) == 1
    allowed = plan.get('extra_inputs', {})
    assert extra - set(stamps) == set(allowed)
    for name, digest in allowed.items():
        assert sha(git(objects, 'show', snapshot['commit'] + ':' + name)) == digest
    assert git(objects, 'rev-list', '--parents', '-n', '1', snapshot['commit']).decode().split() == [
        snapshot['commit'], plan['source']]
    receipt_path = CAS / 'actions/v3' / key[:2] / (key + '.json')
    receipt = None
    input_checks, executable_checks = [], []
    payload = stdout
    if status == 0:
        receipt_raw = receipt_path.read_bytes()
        receipt = json.loads(receipt_raw)
        assert receipt['action_key'] == key and receipt['action_manifest_sha256'] == canonical(request)
        assert receipt['receipt_sha256'] == canonical({name: value for name, value in receipt.items()
                                                       if name != 'receipt_sha256'})
        producer = receipt['producer']
        assert producer['action_key'] == key
        assert producer['attestation_sha256'] == canonical({name: value for name, value in producer.items()
                                                           if name != 'attestation_sha256'})
        runtime = producer['runtime']
        assert runtime['runtime_sha256'] == canonical({name: value for name, value in runtime.items()
                                                      if name != 'runtime_sha256'})
        executable_checks.append(executable(producer['executable']))
        for value in runtime.values():
            if isinstance(value, dict) and 'resolved_path' in value and 'sha256' in value:
                checked(Path(value['resolved_path']), value)
        assert producer['inputs'] == request['inputs']
        for descriptor in producer['inputs']:
            blob(descriptor)
            input_checks.append(descriptor)
        payload = blob(receipt['result'])
        (destination / 'receipt.json').write_bytes(receipt_raw)
        (destination / 'result.log').write_bytes(payload)
    else:
        assert not receipt_path.exists(), 'failed action has a success receipt'
        for descriptor in request['inputs']:
            blob(descriptor)
            input_checks.append(descriptor)
    lines = payload.decode().splitlines()

    def marker(prefix):
        records = [json.loads(line[len(prefix):]) for line in lines if line.startswith(prefix)]
        assert len(records) == 1, (prefix, len(records))
        return records[0]

    completion = marker('pbtest-completion: ')
    outcomes = marker('pbtest-outcomes: ')
    assert completion['collection_complete'] and not completion['collection_errors']
    assert not completion['collect_only'] and not completion['duplicate_teardown']
    for name in ('missing_teardown', 'missing_outcomes', 'unexpected_nodeids', 'problems'):
        assert not completion.get(name), (name, completion.get(name))
    nodes = completion['collected']
    assert len(nodes) == len(set(nodes))
    assert set(nodes) == set(completion['teardown_finished']) == set(completion['outcome_nodeids'])
    assert set(nodes) == set(outcomes['collected'])
    assert not outcomes['uncounted'] and not outcomes['file_selection_error']
    assert not outcomes['file_selection']['ignored']
    assert not outcomes['deselected'] or row.get('negative_control') is True
    files = {node.split('::', 1)[0] for node in nodes}
    assert files == set(outcomes['file_selection']['files']) == set(row['files'])
    native = [report for report in outcomes['reports'] if report[2] in {'passed', 'failed', 'skipped'}]
    assert len(native) == len(nodes) and {report[0] for report in native} == set(nodes)
    counts = dict(Counter(report[2] for report in native))
    assert counts == row['counts']
    runtime = marker('prismabuild-test-runtime: ')
    assert runtime['complete'] is True and runtime['executable_path_matches_running'] is True
    assert runtime['version'][:2] == row['expected_runtime']
    assert set(runtime['files']) == files
    assert set(runtime['native_threads'].values()) == {'1'}
    allocation = terminal['cpu_allocation']
    affinity = set(allocation['preferred'] + allocation['fallback'])
    assert set(runtime['affinity']) == affinity
    assert runtime['conftest_sha256'] == sha(git(objects, 'show', snapshot['commit'] + ':tests/conftest.py'))
    if row.get('container'):
        assert row['image'] in request['params']['container_images']
        outer = marker('c15-action-runtime: ')
        assert outer['complete'] and outer['executable_path_matches_running']
        assert set(outer['affinity']) == affinity and set(outer['native_threads'].values()) == {'1'}
        assert outer['version'][:2] == row['outer_runtime']
        executable_checks.append(executable(outer['executable']))
    else:
        assert request['params']['command'][0] == runtime['executable']['path']
        executable_checks.append(executable(runtime['executable']))
    report = dict(action_key=key, verified=True, source=plan['source'], snapshot=snapshot,
                  request_sha256=sha(request_raw), terminal_sha256=sha(terminal_raw),
                  receipt_sha256=receipt and receipt['receipt_sha256'], returncode=status,
                  failed_has_no_receipt=receipt is None if status else False,
                  counts=counts, files=sorted(files), nodes=nodes, completion=completion,
                  runtime=runtime, executable_checks=executable_checks, inputs=input_checks,
                  scope=export, resources=terminal['resources'], host=terminal['claimed_host'])
    (destination / 'verified.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_bytes())
    args.output.mkdir(parents=True, exist_ok=True)
    objects = args.output / 'source-objects.git'
    if not objects.exists():
        subprocess.run(['git', 'init', '--bare', str(objects)], capture_output=True, check=True)
    reports = [capture(plan, row, args.output / 'raw' / row['action_key'], objects) for row in plan['actions']]
    nodes = [node for report in reports for node in report['nodes']]
    assert len(nodes) == len(set(nodes)), 'duplicate native IDs within the phase'
    result = dict(all_verified=True, source=plan['source'], actions=reports,
                  counts=dict(sum((Counter(report['counts']) for report in reports), Counter())))
    (args.output / 'verified.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(all_verified=True, source=plan['source'], counts=result['counts'])))
