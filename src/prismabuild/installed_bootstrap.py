"""Coordinator boundary for authenticated installed-client observations."""
from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess
import time
import uuid

from .compatibility_window import (
    BootstrapQualification, RootBootstrapResponses, WindowError,
    require_installed_bootstrap, require_bootstrap_host_identity,
)
from .digest_primitives import canonical_sha256, raw_sha256

from .qualification_evidence import (
    CORPUS_FILES, ROOT_FILES, QUALIFICATION_SOURCES,
    _qualification_source, _qualification_index as _read_qualification_index,
    generate_qualification_index, rebind_qualification_index,
    verify_qualification as _verify_qualification,
)

from .release_identity import (
    release_source_identity, release_snapshot_identity, portable_release_command,
)

def _qualification_index(assessment, checkout):
    return _read_qualification_index(assessment, checkout, _qualification_source)


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
    if admission not in ('ordinary', 'publication-slot') or (admission == 'ordinary' and (not isinstance(value.get('ordinary_canary_reason'), str) or not value['ordinary_canary_reason'].strip())):
        raise WindowError('ordinary bootstrap canary needs an explicit operator reason')
    return value


def verify_qualification(assessment, *, checkout, fleet_root):
    return _verify_qualification(assessment, checkout=checkout, fleet_root=fleet_root,
                                 source_reader=_qualification_source)


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
    live = [row for row in status['nodes'] if row.get('state') in ('live', 'draining')]
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
        require_bootstrap_host_identity(plan, host, row)
        if (row.get('nonce') != observations.nonce
                or row.get('plan_sha256') != canonical_sha256(plan)
                or not observations.started_unix <= row.get('observed_unix', 0) <= now
                or now - observations.started_unix > 60
                or row.get('convergence', {}).get('complete') is not True):
            pending = row.get('convergence', {}).get('pending', [])
            raise WindowError('bootstrap pending actual in-band convergence on ' + host
                + '; process observations=' + json.dumps(pending)
                + '; observation refusal grants no permission to terminate processes')
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


def write_plan(*, source, target, inventory, actions, qualification_index, designation, out, operation_id,
               checkout=None, fleet_root=None):
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
        'source_files': {name: before['files'][members[name]]
                         for name in (*BOOTSTRAP_SERVICE_MEMBERS, 'upgrade_client.py')},
        'target_files': {name: after['files'][member] for name, member in members.items()}, 'roster': hosts,
        'boots': {host: row['boot_id'] for host, row in facts.items()},
        'locks': {host: row['lock_inode'] for host, row in facts.items()},
        'enrollments': {host: row['enrollment_sha256'] for host, row in facts.items()},
        'brokers': {host: row['broker'] for host, row in facts.items()},
        'tool_sha256': after['files']['tools/bootstrap_client.py']}
    if any(plan['source_files'][name] != plan['target_files'][name] for name in BOOTSTRAP_SERVICE_MEMBERS):
        raise WindowError('bootstrap cannot change a broker service member')
    assessment = {'schema': 'prismabuild.installed_bootstrap.assessment.v1', 'plan': plan,
        'rollback_designation': declared, 'qualification_actions': json.loads(Path(actions).read_bytes()),
        'qualification_index': {'path': str(Path(qualification_index).resolve(strict=True)),
                                'sha256': raw_sha256(Path(qualification_index).read_bytes())}}
    plan['qualification_index_sha256'] = assessment['qualification_index']['sha256']
    qualification = verify_qualification(assessment,
        checkout=Path(checkout) if checkout is not None else Path(__file__).resolve().parents[2],
        fleet_root=Path(fleet_root) if fleet_root is not None else target.parent.parent)
    plan['qualified_runtimes'] = list(qualification.runtimes)
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    (out / 'plan.json').write_text(json.dumps(plan, indent=2) + '\n')
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
    for name in ('source', 'target', 'inventory', 'actions', 'qualification-index', 'designation', 'out'):
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
                   qualification_index=args.qualification_index, designation=args.designation, out=args.out, operation_id=args.id)


if __name__ == '__main__':
    main()
