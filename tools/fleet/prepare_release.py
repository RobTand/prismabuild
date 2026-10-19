#!/usr/bin/env python3
"""Prepare independent release populations; finalize into a NEW bootstrap plan.

Coordinator adapter only. Collection always submits through the configured
published pbrun; no local pytest, execution-log population, or pointer mutation.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import sys

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / 'src'))
from prismabuild import installed_bootstrap as release
from prismabuild import action_result, client_channel, core as pb, pool


def load_tool(name):
    spec = importlib.util.spec_from_file_location('release_' + name, _ROOT / 'tools/fleet' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def read_json(path):
    return json.loads(Path(path).read_bytes())


class PublishedCollector:
    def __init__(self, checkout, commit, config, profiles, evidence):
        self.checkout, self.commit = checkout, commit
        self.config, self.profiles = config, profiles
        self.evidence = evidence
        self.evidence.mkdir(parents=True, exist_ok=False)
        self.gate = load_tool('shape_gate')
        self.outcomes = load_tool('pbtest_outcomes')
        self.fleet = Path(config['fleet_root'])
        self.pbrun = Path(config['pbrun']).resolve(strict=True)
        # Select the published entrypoint explicitly, never a worktree client.
        published = (self.fleet / 'repo/tools/pbrun.py').resolve(strict=True)
        if self.pbrun != published or self.pbrun.is_relative_to(checkout.resolve()):
            raise ValueError('collection requires the configured fleet published pbrun')

    def __call__(self, **request):
        kind, machine = request['kind'], request['machine']
        files = sorted(request['files'])
        lane = self.config['collection'][kind][machine]
        if kind == 'root':
            profile_path = self.profiles[machine]
            profile = read_json(self.checkout / profile_path)
            command = [profile['primary']['executable']['path'],
                       'tests/run_proof_container.py', '--runtime-profile',
                       profile_path, '--collect-only', *files]
        else:
            # Prefix must invoke the existing outcome recorder in collection mode.
            command = [*lane['command_prefix'], '--collect-only', *files]
        command = release.portable_release_command(checkout=self.checkout, command=command)
        cpus, memory = lane['cpus'], lane['mem_gb']
        if not isinstance(cpus, int) or cpus < 1 or not isinstance(memory, (int, float)) or memory <= 0:
            raise ValueError('each collection lane needs positive aggregate cpus and mem_gb')
        argv = [self.config['client_python'], str(self.pbrun), '--transport', 'pool',
                '--cwd', str(self.checkout), '--cpus', str(cpus),
                '--demand', 'mem_gb=' + str(memory),
                '--timeout-s', str(lane['timeout_s']), '--wait-s', str(lane['wait_s'])]
        for tag in lane.get('tags', []):
            argv += ['--tag', tag]
        for key, value in {**lane.get('env', {}), 'OMP_NUM_THREADS': '1',
                           'MKL_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1'}.items():
            argv += ['--env', key + '=' + str(value)]
        process = client_channel.ClientProcess.launch([*argv, '--', *command],
            queue_root=self.fleet / 'pb-queue', cas_root=self.fleet / 'cas')
        log_path = self.evidence / (kind + '-' + machine + '-client.log')
        with log_path.open('x') as log:
            for line in process.process.stdout:
                log.write(line)
                log.flush()
                print(line, end='', flush=True)
        log_path.chmod(0o444)
        result = process.wait()
        if result.problem or result.ending is None or result.ending.exit_code != 0 or result.identity is None:
            raise ValueError('collection did not finish successfully; retain ' + str(log_path))
        key, terminal = self.gate.resolve_action_key(self.fleet / 'pb-queue', result.identity.action_key)
        done = pool.record_store.read_document(terminal)
        if done.get('status') != 'executed' or done.get('detail', {}).get('returncode') != 0:
            raise ValueError('collection needs a successful terminal execution')
        cas = pb.PrismaBuildCAS(self.fleet / 'cas')
        action = cas.read_action_request(key)
        receipt = cas.lookup(action) if action else None
        if receipt is None or receipt['producer']['evidence']['machine'] != machine:
            raise ValueError('collection has no verified native CAS receipt for ' + machine)
        action_result.bind_standard_capture_command(action)
        snapshot = pb.validate_pbrun_checkout_snapshot(action['params']['checkout_snapshot'])
        if (action['params']['command'] != command or action['params'].get('cwd') != '.'
                or action['task'].get('working_directory') != '.' or snapshot['subdirectory'] != '.'
                or snapshot['input'] not in action['inputs']):
            raise ValueError('collection executed a different command or checkout route')
        self.gate._git(self.checkout, 'bundle', 'unbundle', str(cas.input_path(snapshot['input'])))
        changed = self.gate._git(self.checkout, 'diff-tree', '-r', '--no-renames', '--name-only',
                                 self.commit, snapshot['commit']).splitlines()
        if self.gate.closure_stamp_only(changed, '.'):
            raise ValueError('collection snapshot differs from requested source')
        output = cas.result_path(receipt, action).read_text(errors='strict')
        record = self.outcomes.parse(output)
        if (not record or not record.get('collect_only') or not record.get('collection_complete')
                or record.get('collection_errors') or record.get('deselected')):
            raise ValueError('collection lacks a complete independent collection record')
        if kind == 'root':
            attestations = [json.loads(line.split(': ', 1)[1]) for line in output.splitlines()
                            if line.startswith('prismabuild-root-runtime: ')]
            if (len(attestations) != 1 or attestations[0].get('mode') != 'collection'
                    or attestations[0].get('complete') is not True or attestations[0].get('exitstatus') != 0
                    or attestations[0].get('profile_sha256') != release.raw_sha256(
                        (self.checkout / profile_path).read_bytes())):
                raise ValueError('root collection lacks a complete native runtime attestation')
        nodes = record['collected']
        if {node.split('::')[0] for node in nodes} != set(files):
            raise ValueError('collection did not cover every assigned whole file')
        evidence = {'action_key': key, 'snapshot': snapshot, 'receipt': receipt,
                    'command': command, 'collection': record}
        path = self.evidence / (kind + '-' + machine + '.json')
        with path.open('x') as handle:
            json.dump(evidence, handle, indent=2)
            handle.write('\n')
        path.chmod(0o444)
        return nodes


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='phase', required=True)
    for phase in ('prepare', 'finalize'):
        command = commands.add_parser(phase)
        command.add_argument('--checkout', required=True, type=Path)
        command.add_argument('--requested-commit', required=True)
        command.add_argument('--main-ref', required=True)
        command.add_argument('--out', required=True, type=Path)
    prepare = commands.choices['prepare']
    prepare.add_argument('--config', required=True, type=Path)
    prepare.add_argument('--collection-evidence', required=True, type=Path)
    finalize = commands.choices['finalize']
    finalize.add_argument('--generation', required=True, type=Path)
    finalize.add_argument('--fleet-root', required=True, type=Path)
    finalize.add_argument('--staging-action', required=True,
                          help='verified staging action whose CAS bundle contains the recorded S')
    finalize.add_argument('--index', required=True, type=Path)
    finalize.add_argument('--rebound-index', required=True, type=Path)
    for name in ('source', 'inventory', 'actions', 'designation'):
        finalize.add_argument('--' + name, required=True, type=Path)
    finalize.add_argument('--id', required=True)
    args = parser.parse_args(argv)
    identity = release.release_source_identity(checkout=args.checkout,
        requested_commit=args.requested_commit, main_ref=args.main_ref)
    print(json.dumps(identity, sort_keys=True))
    if args.phase == 'prepare':
        if os.environ.get('PRISMABUILD_ACTION_KEY'):
            raise ValueError('prepare orchestrates admission; do not run it inside an action')
        config = read_json(args.config)
        profiles = {machine: release.portable_release_command(
            checkout=args.checkout, command=[path])[0]
            for machine, path in config['root_profiles'].items()}
        routes = {machine: {'commands': [release.portable_release_command(
            checkout=args.checkout, command=command) for command in route['commands']]}
            for machine, route in config['corpus_routes'].items()}
        collect = PublishedCollector(args.checkout, args.requested_commit, config,
                                     profiles, args.collection_evidence)
        release.generate_qualification_index(checkout=args.checkout,
            target_commit=args.requested_commit, root_profiles=profiles,
            corpus_routes=routes, collect=collect, out=args.out)
        summary = {'source_identity': identity, 'main_ref': args.main_ref,
                   'qualification_index_sha256': release.raw_sha256(args.out.read_bytes()),
                   'collection_sha256': {path.name: release.raw_sha256(path.read_bytes())
                       for path in sorted(args.collection_evidence.glob('*.json'))}}
        path = args.collection_evidence / 'preparation.json'
        with path.open('x') as handle:
            json.dump(summary, handle, sort_keys=True, indent=2)
            handle.write('\n')
        path.chmod(0o444)
        args.collection_evidence.chmod(0o555)
    else:
        # A remote staging snapshot is retained in CAS, not necessarily in the
        # coordinator's object database. Import the verified action's bundle,
        # then prove the receipt's S; never substitute the inspector's HEAD.
        gate = load_tool('shape_gate')
        key, terminal = gate.resolve_action_key(args.fleet_root / 'pb-queue', args.staging_action)
        done = pool.record_store.read_document(terminal)
        cas = pb.PrismaBuildCAS(args.fleet_root / 'cas')
        action = cas.read_action_request(key)
        receipt = cas.lookup(action) if action else None
        if (receipt is None or done.get('status') != 'executed'
                or done.get('detail', {}).get('returncode') != 0):
            raise ValueError('staging requires successful terminal execution and verified CAS receipt')
        action_result.bind_standard_capture_command(action)
        snapshot = pb.validate_pbrun_checkout_snapshot(action['params']['checkout_snapshot'])
        if snapshot['input'] not in action['inputs'] or snapshot['subdirectory'] != '.':
            raise ValueError('staging action checkout route differs')
        recorded = read_json(args.generation / 'RUNTIME_VERSION.json')['commit']
        if snapshot['commit'] != recorded:
            raise ValueError('staging action snapshot differs from recorded generation')
        gate._git(args.checkout, 'bundle', 'unbundle', str(cas.input_path(snapshot['input'])))
        identity = release.release_snapshot_identity(checkout=args.checkout,
            requested_commit=args.requested_commit, generation=args.generation)
        print(json.dumps(identity, sort_keys=True))
        release.rebind_qualification_index(checkout=args.checkout,
            requested_commit=args.requested_commit, generation=args.generation,
            index_path=args.index, out=args.rebound_index)
        release.write_plan(source=args.source, target=args.generation,
            inventory=args.inventory, actions=args.actions,
            qualification_index=args.rebound_index, designation=args.designation,
            out=args.out, operation_id=args.id, checkout=args.checkout,
            fleet_root=args.fleet_root)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
