"""Recheck retained D4 evidence and bind its five producer files to a Git pin.

This standard-library audit reads source and retained PB artifacts. It runs no
product code, tests or measurement. A changed producer file requires recapture.
"""
import argparse
import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--checkout', type=Path, default=Path(__file__).resolve().parents[2])
parser.add_argument('--current-pin', default='HEAD')
parser.add_argument('--evidence', type=Path, default=Path(
    '/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/final-scratch-profile'))
parser.add_argument('--output', type=Path)
args = parser.parse_args()
producer = '32577f51fa444d6ebd6456fe4998550f2ebf4bb3cafaaa04d6c23c20d9adcc7a'
consumer = 'e0da5d8c79fbe4d37f1ac5fb8afe28e45048c9cdbdb6abce3fe11d727674c5e0'
packet = '689f3425bd8128184b63e3c622bea510e7a3b2632bcd48971963a254fcea3e75'


def _physical_profile_git(*words):
    return subprocess.check_output(['git', '-C', str(args.checkout), *words])


def _physical_profile_checked(raw, descriptor):
    assert len(raw) == descriptor['bytes']
    assert hashlib.sha256(raw).hexdigest() == descriptor['sha256']
    return raw


phases = {}
for phase, key in [('target-evidence-r1', packet), ('physical-profile-r1', producer),
                   ('current-consumer-r1', consumer)]:
    reports = json.loads((args.evidence / phase / 'primary/verified.json').read_bytes())
    assert len(reports) == 1 and reports[0]['action_key'] == key and reports[0]['returncode'] == 0
    location = args.evidence / phase / 'primary' / key
    raw = (location / 'terminal.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == reports[0]['terminal_sha256']
    terminal = json.loads(raw)
    cleanup, export = terminal['resource_scope_cleanup'], terminal['resource_scope_cleanup']['export']
    assert cleanup['complete'] and export['tickets_pending'] is False
    assert all(export[field] is True for field in ('empty', 'ok', 'settled', 'released', 'stopped'))
    receipt = json.loads((location / 'receipt.json').read_bytes())
    result = _physical_profile_checked((location / 'result.log').read_bytes(), receipt['result'])
    for name in ('stdout_log', 'stderr_log'):
        _physical_profile_checked((location / (name + '.log')).read_bytes(), terminal['detail'][name])
    request = json.loads((location / 'request.json').read_bytes())
    assert terminal['action_key'] == request['action_key'] == receipt['action_key'] == key
    phases[phase] = dict(action=key, terminal=terminal, request=request, receipt=receipt,
                         result=result, primary=reports[0], export=export)
actual = json.loads(phases['physical-profile-r1']['result'])
assert actual['completed'] is True and actual['errors'] == []
assert actual['producer_action_key'] == producer
assert actual['host'] == 'sparky' and actual['root'] == '/home/rob/pbt17'
assert actual['write_bytes'] == actual['read_bytes'] == 134217728
assert actual['envelope'] == dict(root='/home/rob/pbt17', bytes=67108864,
                                  block_bytes=1048576, repetitions=2)
assert actual['start_identity'] == actual['end_identity']
assert actual['write_elapsed_s'] > 0 and actual['read_elapsed_s'] > 0
measured = phases['physical-profile-r1']['request']
assert measured['task']['task_class'] == 'measurement'
assert measured['task']['argv'] == measured['params']['command']
assert measured['task']['argv'][1:3] == ['-I', '-S']
assert measured['task']['result_path'] == 'prismabuild-local-scratch-profile.json'
assert measured['params']['demand'] == dict(cpu=1, mem_gb=2, spool_gb=1)
assert measured['environment']['toolchain'] == phases['physical-profile-r1']['receipt']['producer']['toolchain']['verified']
lines = phases['current-consumer-r1']['result'].decode().splitlines()
rows = [json.loads(line.removeprefix('scratch-source-qualification: '))
        for line in lines if line.startswith('scratch-source-qualification: ')]
assert len(rows) == 1
qualified = rows[0]
assert qualified['qualified'] and qualified['qualification_action'] == consumer
assert qualified['producer_action'] == producer and not qualified['shared_configuration_changed']
assert qualified['observation_count'] == 2 and qualified['immutable_source_verifications'] == 1
profile = qualified['observed']['local_scratch_io_profiles']
assert len(profile) == 1
assert {k: v for k, v in profile[0].items() if k != 'artifact_sha256'} == actual
allocation = phases['current-consumer-r1']['terminal']['cpu_allocation']
assert qualified['affinity'] == sorted(allocation['preferred'] + allocation['fallback'])
current = _physical_profile_git('rev-parse', args.current_pin).decode().strip()
members = qualified['exact_source_members']
assert set(members) == {'tools/fleet/local_scratch_profile.py', 'src/prismabuild/local_scratch.py',
                        'src/prismabuild/core.py', 'src/prismabuild/digest_primitives.py',
                        'src/prismabuild/immutable_files.py'}
for name, descriptor in members.items():
    for pin in (current, '463d0f7b84da0cb961b3c5e74869b9afb39da1c0',
                '404fa84fa83cc8739e119e057ad004de3c5185aa'):
        _physical_profile_checked(_physical_profile_git('show', pin + ':' + name), descriptor)
core = ast.parse(_physical_profile_git('show', current + ':src/prismabuild/core.py'))
binding = next(n.value for n in core.body if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == '_IMMUTABLE_FILES_SOURCE' for t in n.targets))
assert ast.literal_eval(binding) == members['src/prismabuild/immutable_files.py']
report = dict(verified=True, checked_at=datetime.now(timezone.utc).isoformat(), current_source=current,
              producer_action=producer, consumer_action=consumer, packet_action=packet,
              exact_source_members=members, source_binding_preserved=True,
              write_bytes=actual['write_bytes'], read_bytes=actual['read_bytes'],
              current_observations=2, source_verifications=1,
              all_physical_scopes_closed=True, shared_configuration_changed=False,
              primary_paths={name: str(args.evidence / name / 'primary/verified.json') for name in phases},
              exports={name: row['export'] for name, row in phases.items()},
              scope='D4 final producer bytes captured through actual buffered write/fdatasync/warm-read calls and accepted by the current production consumer. No representative bandwidth, spill, pressure, power-loss, GPU workload, rollout or deployment credit.')
if args.output is not None:
    args.output.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({k: report[k] for k in ('verified', 'current_source', 'producer_action',
                                       'consumer_action', 'all_physical_scopes_closed')}))
