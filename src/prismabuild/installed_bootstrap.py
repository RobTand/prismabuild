"""Coordinator boundary for authenticated installed-client observations."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import time
import uuid

from .compatibility_window import (
    BootstrapQualification, RootBootstrapResponses, WindowError,
    require_installed_bootstrap,
)
from .digest_primitives import canonical_sha256, raw_sha256

CORPUS_FILES = tuple('tests/test_record_compatibility_' + name + '.py' for name in (
    'queue', 'storage', 'ownership', 'outputs', 'host', 'decomposition', 'core', 'controller'))
ROOT_FILES = ('tests/test_client_bootstrap_root.py', 'tests/test_receipt_reader_root.py')


def read_assessment(path):
    value = json.loads(Path(path).read_bytes())
    if value.get('schema') != 'prismabuild.installed_bootstrap.assessment.v1':
        raise WindowError('invalid installed bootstrap assessment')
    plan = value['plan']
    if (plan.get('schema') != 'prismabuild.client_bootstrap.plan.v1'
            or re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,95}', plan['id']) is None
            or not value.get('qualification_actions')):
        raise WindowError('invalid installed bootstrap plan/qualification')
    admission = value.get('canary_admission', 'publication-slot')
    if admission not in ('ordinary', 'publication-slot') or (admission == 'ordinary' and not str(value.get('ordinary_canary_reason', '')).strip()):
        raise WindowError('ordinary bootstrap canary needs an explicit operator reason')
    return value


def verify_qualification(assessment, *, checkout, fleet_root):
    """Verify actual CAS actions/logs, exact source and whole-file populations.

    The operator index supplies keys, never verdicts. Retained snapshots must
    differ from the release commit only by PB's own closure stamp.
    """
    from . import core as pb
    from . import pool
    plan = assessment['plan']
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
    lanes, keys = {}, []
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
        rows = lanes.setdefault((kind, machine), [])
        rows.append({'shard': len(rows), 'files': files, 'returncode': 0,
                     'timed_out': False, 'output': log})
        keys.append(full)
    if (not {('root', 'aarch64'), ('root', 'x86_64')} <= set(lanes)
            or not any(kind == 'corpus' for kind, _ in lanes)):
        raise WindowError('bootstrap needs native ARM and x86 root qualification plus the exact record corpus')
    for (kind, _), rows in lanes.items():
        population = ROOT_FILES if kind == 'root' else CORPUS_FILES
        nodes = [node for row in rows for node in outcomes.parse(row['output'])['collected']]
        verdict = outcomes.judge_shards(rows, requested_files=population, required_nodes=nodes)
        if not verdict['green']:
            raise WindowError('bootstrap qualification is incomplete or contains skips/failures')
    return BootstrapQualification(plan, action_keys=keys)


def collect_responses(plan):
    """No supplied observation file can stand in for this authenticated round."""
    nonce = uuid.uuid4().hex + uuid.uuid4().hex
    started = time.time()
    rows = {}
    digest = canonical_sha256(plan)
    for host, aliases in sorted(plan['roster'].items()):
        # Only enrolled literal hostname tokens go into SSH's remote argv.
        if not aliases or host not in aliases or any(
                re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,95}', alias) is None for alias in aliases):
            raise WindowError('invalid enrolled SSH aliases')
        remote = 'sparklina' if 'sparklina' in aliases else host
        if re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,95}', plan['id']) is None:
            raise WindowError('invalid bootstrap id')
        argv = ['/usr/bin/ssh', '-oBatchMode=yes', '-oStrictHostKeyChecking=yes',
                '-oConnectTimeout=10', '--', remote, '/usr/bin/sudo', '-n',
                '/usr/bin/python3', '-I', '/opt/prismabuild-bootstrap/' + plan['id'] + '/bootstrap_client.py',
                'observe', '--id', plan['id'], '--plan-sha256', digest, '--nonce', nonce]
        result = subprocess.run(argv, capture_output=True, timeout=45, check=True)
        if len(result.stdout) > 1024 * 1024:
            raise WindowError('oversized root bootstrap response')
        rows[host] = json.loads(result.stdout)
    return RootBootstrapResponses(rows, nonce=nonce, started_unix=started)


def enrolled_roster(source, target):
    from .fleet_roster import Roster, is_absent
    from .generation_receipt import GenerationReceipt
    captures = []
    for root in (source, target):
        receipt = GenerationReceipt.capture((Path(root) / 'RUNTIME_VERSION.json').read_bytes())
        roster = Roster.qualified(root, receipt.files)
        groups = {}
        for host, aliases, box in roster.barrier_declarations():
            if not is_absent(host, box):
                groups[host] = sorted(aliases)
        captures.append(groups)
    if captures[0] != captures[1]:
        raise WindowError('bootstrap source/target enrollment differs')
    return captures[0]


def assess(assessment, *, checkout, fleet_root, source, target, roster, verify_live=False):
    qualification = verify_qualification(assessment, checkout=checkout, fleet_root=fleet_root)
    live = require_live_window(assessment['plan'], target=target) if verify_live else None
    observations = collect_responses(assessment['plan'])
    result = require_installed_bootstrap(
        source, target, observations, roster=roster,
        qualification=qualification, rollback_designation=assessment['rollback_designation'])
    if live is not None:
        result["live_window"] = live
    return result, observations


def require_live_window(plan, *, target):
    import sys
    result = subprocess.run([sys.executable, str(Path(target) / 'tools/pbstatus.py'),
        '--json', '--transport', 'pool', '--recent', '0', '--timeout-s', '30'],
        capture_output=True, timeout=45, check=True)
    if len(result.stdout) > 4 * 1024 * 1024:
        raise WindowError('oversized live bootstrap census')
    status = json.loads(result.stdout)
    if (status.get('complete') is not True or status.get('timed_out_sections')
            or status.get('unavailable_sections')):
        raise WindowError('bootstrap live window census is incomplete')
    live = [row for row in status['nodes'] if row.get('state') == 'live']
    hosts = {row['node'] for row in live}
    if (len(live) != len(plan['roster']) or len(hosts) != len(live)
            or any(len(hosts & set(aliases)) != 1 for aliases in plan['roster'].values())):
        raise WindowError('bootstrap live census does not cover the enrolled roster')
    commits = {row.get('runtime_commit') for row in live}
    if not commits <= {plan['source']['commit'], plan['target']['commit']}:
        raise WindowError('live window changed; recapture and rerun the exact corpus')
    for commit in commits:
        matches = [path.parent.name for path in Path(target).parent.glob('*/RUNTIME_VERSION.json')
                   if json.loads(path.read_bytes()).get('commit') == commit]
        expected = [plan[key]['generation'] for key in ('source', 'target') if plan[key]['commit'] == commit]
        if sorted(matches) != sorted(expected):
            raise WindowError('bootstrap live generation became ambiguous')
    return {'captured_unix': time.time(), 'status_sha256': raw_sha256(result.stdout),
            'hosts': sorted(hosts), 'commits': sorted(commits)}


def require_convergence(plan, *, fleet_root):
    observations = collect_responses(plan)
    selected = plan['source' if plan.get('direction') == 'restore' else 'target']
    now = time.time()
    for host, row in observations.rows.items():
        if (row.get('nonce') != observations.nonce or row.get('boot_id') != plan['boots'][host]
                or row.get('plan_sha256') != canonical_sha256(plan)
                or not observations.started_unix <= row.get('observed_unix', 0) <= now
                or now - observations.started_unix > 60
                or row.get('convergence', {}).get('complete') is not True):
            raise WindowError('bootstrap awaits actual in-band convergence on ' + host)
        offers = []
        for alias in plan['roster'][host]:
            path = Path(fleet_root) / 'pb-queue/workers' / (alias + '.json')
            if path.exists():
                offer = json.loads(path.read_bytes())
                if 0 <= now - offer.get('announced_unix', 0) <= 60:
                    offers.append(offer)
        if len(offers) != 1 or ('runtime-generation:' + selected['generation']) not in offers[0].get('tags', []):
            raise WindowError('bootstrap needs one fresh selected-generation worker offer for ' + host)
    return observations


def capture_inventory(out):
    """Read root host facts through fixed commands; perform no host mutation."""
    out = Path(out)
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    result = {}
    for host, remote in (('sparky', 'sparky'), ('gx10-6b77', 'sparklina'), ('dl380g10', 'dl380g10')):
        def read(*argv):
            completed = subprocess.run(['/usr/bin/ssh', '-oBatchMode=yes', '-oStrictHostKeyChecking=yes',
                '-oConnectTimeout=10', '--', remote, '/usr/bin/sudo', '-n', *argv],
                capture_output=True, timeout=30, check=True)
            return completed.stdout
        config_raw = read('/usr/bin/cat', '/etc/prismabuild/client-upgrade.json')
        config = json.loads(config_raw)
        if config['state_dir'] != '/var/lib/prismabuild-client-upgrade':
            raise WindowError('nonstandard enrollment requires a separately reviewed bootstrap')
        pid = read('/usr/bin/systemctl', 'show', 'prismabuild-resource-broker.service',
                   '--property=MainPID', '--value').decode().strip()
        if not pid.isdecimal() or int(pid) <= 0:
            raise WindowError('broker is not running')
        proc = read('/usr/bin/cat', '/proc/' + pid + '/stat').decode()
        start = proc.rpartition(')')[2].split()[19]
        result[host] = {'config': config, 'enrollment_sha256': raw_sha256(config_raw),
            'boot_id': read('/usr/bin/cat', '/proc/sys/kernel/random/boot_id').decode().strip(),
            'lock_inode': int(read('/usr/bin/stat', '-c%i', '/var/lib/prismabuild-client-upgrade/upgrade.lock')),
            'broker': {'pid': int(pid), 'starttime': start}}
        (out / (host + '-enrollment.json')).write_bytes(config_raw)
    (out / 'inventory.json').write_text(json.dumps(result, indent=2) + '\n')


def write_plan(*, source, target, inventory, actions, designation, out, operation_id):
    from .compatibility_window import (
        BOOTSTRAP_SOURCE, BOOTSTRAP_MEMBERS, BOOTSTRAP_SERVICE_MEMBERS, require_generation,
    )
    from . import fleet_roster
    from .generation_receipt import GenerationReceipt
    out = Path(out)
    source, target = Path(source).resolve(strict=True), Path(target).resolve(strict=True)
    declared = json.loads(Path(designation).read_bytes())
    require_generation(source, declared['rollback'])
    if any(declared['rollback'][key] != value for key, value in BOOTSTRAP_SOURCE.items()):
        raise WindowError('only the exact designated a174 source is supported')
    def pin(root):
        raw = (root / 'RUNTIME_VERSION.json').read_bytes()
        captured = GenerationReceipt.capture(raw)
        receipt = captured.require_generation(root.name)
        result = {'generation': root.name, 'directory': str(root), 'commit': receipt['commit'],
                  'receipt_sha256': raw_sha256(raw), 'content_sha256': canonical_sha256(receipt['files'])}
        require_generation(root, result)
        return result, receipt
    source_pin, before = pin(source)
    target_pin, after = pin(target)
    members = {name: ('tools/' + name if name in ('upgrade_client.py', 'resource_broker.py', 'resource_payload.py')
                     else 'src/prismabuild/' + name) for name in BOOTSTRAP_MEMBERS}
    roster = fleet_roster.Roster.qualified(target, after['files'])
    source_roster = fleet_roster.Roster.qualified(source, before['files'])
    def enrolled(value):
        return {host: sorted({host, *([box['_alias']] if box.get('_alias') else [])})
                for host, box in value.document['boxes'].items() if box.get('status', 'active') == 'active'}
    hosts = enrolled(roster)
    if hosts != enrolled(source_roster):
        raise WindowError('enrolled source/target roster changed')
    facts = json.loads(Path(inventory).read_bytes())
    if set(facts) != set(hosts):
        raise WindowError('inventory differs from enrolled host roster')
    plan = {'schema': 'prismabuild.client_bootstrap.plan.v1', 'id': operation_id,
        'direction': 'forward', 'source': source_pin, 'target': target_pin, 'members': members,
        'source_files': {name: before['files'][member] for name, member in members.items() if member in before['files']},
        'target_files': {name: after['files'][member] for name, member in members.items()}, 'roster': hosts,
        'boots': {host: row['boot_id'] for host, row in facts.items()},
        'locks': {host: row['lock_inode'] for host, row in facts.items()},
        'enrollments': {host: row['enrollment_sha256'] for host, row in facts.items()},
        'brokers': {host: row['broker'] for host, row in facts.items()},
        'tool_sha256': after['files']['tools/bootstrap_client.py']}
    if any(plan['source_files'][name] != plan['target_files'][name] for name in BOOTSTRAP_SERVICE_MEMBERS):
        raise WindowError('bootstrap cannot change a broker service member')
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    (out / 'plan.json').write_bytes(json.dumps(plan, indent=2).encode())
    assessment = {'schema': 'prismabuild.installed_bootstrap.assessment.v1', 'plan': plan,
        'rollback_designation': declared, 'qualification_actions': json.loads(Path(actions).read_bytes())}
    (out / 'assessment.json').write_text(json.dumps(assessment, indent=2) + '\n')
    for host, row in facts.items():
        directory = out / host
        directory.mkdir()
        for label, root in (('source', source), ('target', target)):
            (directory / (label + '-receipt.json')).write_bytes((root / 'RUNTIME_VERSION.json').read_bytes())
            (directory / (label + '-config.json')).write_text(json.dumps({**row['config'], 'runtime': str(root)}))
    print('plan_sha256=' + canonical_sha256(plan))


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    reverse = commands.add_parser('reverse')
    reverse.add_argument('--assessment', required=True, type=Path)
    reverse.add_argument('--out', required=True, type=Path)
    inventory = commands.add_parser('inventory')
    inventory.add_argument('--out', required=True, type=Path)
    plan = commands.add_parser('plan')
    for name in ('source', 'target', 'inventory', 'actions', 'designation', 'out'):
        plan.add_argument('--' + name, required=True, type=Path)
    plan.add_argument('--id', required=True)
    args = parser.parse_args()
    if args.command == 'reverse':
        value = read_assessment(args.assessment)
        value['plan']['direction'] = 'restore'
        value['canary_admission'] = 'ordinary'
        value['ordinary_canary_reason'] = 'operator restoration uses an ordinary canary and preserves the source generation publication grant'
        args.out.mkdir(mode=0o700, parents=True, exist_ok=False)
        (args.out / 'assessment.json').write_text(json.dumps(value, indent=2) + '\n')
        (args.out / 'plan.json').write_text(json.dumps(value['plan'], indent=2) + '\n')
        print('plan_sha256=' + canonical_sha256(value['plan']))
    elif args.command == 'inventory':
        capture_inventory(args.out)
    else:
        write_plan(source=args.source, target=args.target, inventory=args.inventory, actions=args.actions,
                   designation=args.designation, out=args.out, operation_id=args.id)


if __name__ == '__main__':
    main()
