"""Operator cutover rehearsal: real gate policy and custody, fixture root steps."""
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shlex
import sys
import types

import pytest

from prismabuild import legacy_custody_host, record_store
from legacy_custody_fixtures_1429 import stopped_legacy_hosts
from test_legacy_custody_r6_1429 import _e9_terminal
from test_resource_broker import module, Backend, create
from test_client_upgrade import setup

ROOT = Path(__file__).resolve().parents[1]


def test_read_only_gate_query_never_retires_pending_or_retired_scopes(tmp_path):
    broker = module()
    backend = Backend()
    authority = broker.Authority(tmp_path / 'broker', os.getuid(), backend, max_memory_bytes=1 << 30)
    request, _ = create(authority)
    authority.handle(0, os.getpid(), {'op': 'maintenance_begin', 'owner': 'cutover'})
    # Create fresh pending state after the administrative drain's own cleanup.
    scope = broker.scope_id(request['action_key'], request['nonce'])
    authority.records[scope].pop('released_unix', None)
    authority.records[scope]['pending'] = True
    backend.groups[scope] = {'budget': 64 << 20, 'populated': False}
    before = json.dumps(authority.records, sort_keys=True)
    backend.ops.clear()
    result = authority.handle(os.getuid(), os.getpid(), {'op': 'maintenance_query'})
    assert result['draining'] and result['active_scopes'] == 1
    assert not backend.ops and json.dumps(authority.records, sort_keys=True) == before
    for op in ('maintenance_begin', 'maintenance_end', 'maintenance_force_end', 'maintenance_takeover', 'maintenance_status'):
        with pytest.raises(PermissionError):
            authority.handle(os.getuid(), os.getpid(), {'op': op, 'owner': 'cutover'})
    with pytest.raises(PermissionError):
        authority.handle(os.getuid() + 1, os.getpid(), {'op': 'maintenance_query'})
    with pytest.raises(ValueError):
        authority.handle(os.getuid(), os.getpid(), {'op': 'maintenance_query', 'proof': {}})


def test_cutover_commands_stop_activate_adopt_reclaim_then_start(tmp_path, monkeypatch, stopped_legacy_hosts):
    guide = (ROOT / 'docs/operating_prismabuild.md').read_text().split(
        '## Deploying the checkout custody contract (#1429)', 1)[1]
    steps = re.split(r'(?m)^\d+\. ', guide)[1:]
    assert len(steps) == 6
    assert '--maintenance-begin' in steps[2] and 'stop prismabuild-supervisor' in steps[2]
    assert '--activate-generation' in steps[3] and '900 s' in guide
    assert '--apply' in steps[4] and '--reclaim-terminal' in steps[4]
    assert 'start prismabuild-supervisor' in steps[5] and '--maintenance-end' in steps[5]
    assert 'no canary' in steps[3] and 'post-start check' in steps[5]
    root_commands = re.findall(r'ssh "\$host" (sudo[^\n]+)\s*\\\n\s*(--maintenance-(?:begin|end) pb1429-adoption)', guide)
    assert len(root_commands) == 2
    for command, operation in root_commands:
        assert shlex.split(command) == ['sudo', '-n', '/usr/bin/python3', '-I',
                                        '/opt/prismabuild-resource-broker/resource_broker.py']
        assert '/mnt/shared' not in command

    broker = module()
    authority = broker.Authority(tmp_path / 'broker', os.getuid(), Backend(), max_memory_bytes=1 << 30)
    events = []
    # Simulate only the root transport. Exercise the installed entrypoint's
    # real argument parser, framing, response handling and root-only RPC policy.
    class RootClient:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def settimeout(self, value): pass
        def connect(self, path): assert path == '/run/prismabuild/resources.sock'
        def sendall(self, raw):
            request = json.loads(raw)
            events.append(request['op'])
            self.response = authority.handle(0, os.getpid(), request)
        def makefile(self, mode): return io.BytesIO(json.dumps(self.response).encode() + b'\n')
    def root_step(operation):
        with monkeypatch.context() as root_fixture:
            root_fixture.setattr(broker.os, 'geteuid', lambda: 0)
            root_fixture.setattr(broker, '_trusted_file', lambda path: path)
            root_fixture.setattr(broker.socket, 'socket', lambda *a, **k: RootClient())
            root_fixture.setattr(sys, 'argv', ['resource_broker.py', operation, 'pb1429-adoption'])
            broker.main()
    root_step('--maintenance-begin')
    assert authority.maintenance['draining']
    events.append('writers-stopped')
    gate = {'draining': True, 'owner': 'pb1429-adoption', 'changed_unix': authority.maintenance['changed_unix']}
    monkeypatch.setattr(legacy_custody_host.park_proof, 'read_gate', lambda path: dict(gate))
    monkeypatch.setattr(legacy_custody_host.park_proof, 'proc_census', lambda: [])
    monkeypatch.setattr(legacy_custody_host, 'subprocess', types.SimpleNamespace(run=lambda *a, **k:
        types.SimpleNamespace(stdout='ActiveState=inactive\nMainPID=0\nExecStart=supervise.py --systemd\n')))
    monkeypatch.setattr(legacy_custody_host.resource_scope, 'broker_request', lambda request:
        authority.handle(os.getuid(), os.getpid(), request))
    monkeypatch.setattr(legacy_custody_host, 'legacy_stopped_host', stopped_legacy_hosts['real_stopped_host'])
    assert legacy_custody_host.legacy_stopped_host()['writer_processes'] == 0
    # Activation itself is covered by the real rolling publisher fixture suite.
    # This root-step rehearsal pins the time/order around that boundary.
    sys.path.insert(0, str(ROOT / 'tools/fleet'))
    import updater_proof
    observed = 1000.
    proof = {'schema': updater_proof.SCHEMA, 'host': 'owner', 'tool_sha256': 'tool',
             'observed_unix': observed, 'updater': {'sha256': 'bytes', 'uid': 0, 'mode': 0o644},
             'status': {'state': 'current', 'host': 'owner', 'checked_unix': observed,
                        'installed_sha256': 'bytes'}}
    assert updater_proof.judge(proof, host='owner', now=observed + 301., tool='tool') == 'bytes'
    with pytest.raises(updater_proof.ProofRefused):
        updater_proof.judge(proof, host='owner', now=observed + 901., tool='tool')
    events.append('activate')

    queue, key, ledger = _e9_terminal(tmp_path / 'world')
    terminal_path = queue.records.item_path('done', key)
    terminal = record_store.read_document(terminal_path)
    terminal['finished_host'] = terminal['claimed_host']
    queue.records.replace_record_at(terminal_path, terminal)
    spec = importlib.util.spec_from_file_location('adopter_r7', ROOT / 'tools/maintenance/adopt_legacy_custody.py')
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    inventory, report = tmp_path / 'inventory.json', tmp_path / 'adoption.json'
    def operator(*args):
        monkeypatch.setattr(sys, 'argv', ['adopt_legacy_custody.py', '--queue-root', str(queue.root),
                                         '--writers-stopped', *map(str, args)])
        assert cli.main() == 0
    operator('--inventory', inventory, '--local-host')
    operator('--apply', inventory, '--report', report)
    assert json.loads(report.read_bytes())[0]['certified_absent']
    assert ledger.holder_tokens(key)
    events.append('adopt')
    operator('--reclaim-terminal', key, '--unpin')
    assert not ledger.holder_tokens(key)
    events.append('reclaim')
    events.append('start-new-writers')
    root_step('--maintenance-end')
    assert not authority.maintenance['draining']
    assert events == ['maintenance_begin', 'writers-stopped', 'activate', 'adopt', 'reclaim',
                      'start-new-writers', 'maintenance_end']


def test_broker_bridge_is_published_before_custody_and_survives_the_gate(setup):
    from test_client_upgrade import upgrade
    updater, backend, generation = setup
    member = upgrade.MEMBERS['resource_broker.py']
    candidate = b"# broker bridge\nmaintenance_query = True\n# --maintenance-begin\n"
    (generation / member).write_bytes(candidate)
    receipt = generation / 'RUNTIME_VERSION.json'
    document = json.loads(receipt.read_text())
    document['files'][member] = upgrade.digest(candidate)
    receipt.write_text(json.dumps(document))
    # Phase A: normal published generation and normal updater convergence.
    assert updater.run()['state'] == 'updated'
    assert updater.run()['state'] == 'current'
    installed = updater.install / 'resource_broker.py'
    assert installed.read_bytes() == candidate
    assert updater.run()['state'] == 'current'  # Next tick never reverts bridge.
    # Phase B changes custody runtime only; managed client members are frozen.
    (generation / 'src/prismabuild/pool.py').parent.mkdir(parents=True, exist_ok=True)
    (generation / 'src/prismabuild/pool.py').write_text('# checkout custody contract')
    document['files']['src/prismabuild/pool.py'] = upgrade.digest(b'# checkout custody contract')
    receipt.write_text(json.dumps(document))
    backend.holder = 'pb1429-adoption'
    backend.operations.clear()
    held = updater.run()
    assert held['state'] == 'held' and held['drain_owner'] == 'pb1429-adoption'
    assert installed.read_bytes() == candidate
    assert not any(op in {'stop', 'start'} for op in backend.operations)
    # Stopped supervisors remain stopped during verification and adoption.
    backend.holder = None
    assert updater.run()['state'] == 'current'
    assert installed.read_bytes() == candidate


# The updater transaction above and the root gate rehearsal are joined by two
# actual private-store publisher activations, each with verified PB proof records.
from test_receipt_activation_direction import private_store
from test_rolling_activation_with_installed_updater_proof import (
    generation, publisher, proof_action, enrolled_host, HOSTS, RECEIPT_LEAVES)


def test_broker_bridge_then_contract_activate_as_two_immutable_generations(private_store, monkeypatch):
    import hashlib
    import time
    for name in ('PRISMABUILD_ACTION_NONCE', 'PRISMABUILD_ACTION_SCOPE', 'PRISMABUILD_READER_HELPER_ROOT'):
        monkeypatch.delenv(name, raising=False)
    generations = [generation(private_store, name, 'final', tuple(RECEIPT_LEAVES))
                   for name in ('previous', 'broker-bridge', 'checkout-contract')]
    broker = (ROOT / 'tools/fleet/resource_broker.py').read_bytes()
    for index, directory in enumerate(generations):
        changed = {'tools/resource_broker.py': b'# old broker' if index == 0 else broker,
                   'src/prismabuild/pool.py': b'# pre-contract runtime' if index < 2 else b'# custody contract runtime'}
        receipt = directory / 'RUNTIME_VERSION.json'
        document = json.loads(receipt.read_text())
        for member, data in changed.items():
            path = directory / member
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            document['files'][member] = hashlib.sha256(data).hexdigest()
        receipt.write_text(json.dumps(document))
        publisher._seal_generation(directory)
    previous, bridge, contract = generations
    publisher.MIRROR.symlink_to(previous)
    fleet = private_store.parent
    offers = fleet / 'pb-queue/workers'
    offers.mkdir(parents=True)
    for host in HOSTS:
        (offers / (host + '.json')).write_text(json.dumps({'schema': 'prismaquant.prismabuild.pool_offer.v1',
                                                        'host': host, 'announced_unix': time.time()}))
    import updater_proof
    monkeypatch.setattr(updater_proof, 'TRUSTED_UID', os.getuid())
    activated = []
    for target in (bridge, contract):
        proofs = []
        for host in HOSTS:
            proof = enrolled_host(fleet / target.name, host, monkeypatch)
            proof["nonce"] = target.name + "-" + host
            proofs.append(proof_action(fleet, host, proof, monkeypatch))
        assert publisher._activate_existing(target.name, dry_run=True, rollout='rolling',
                rollout_reason='two-phase checkout custody rehearsal', proofs=proofs) == 0
        assert publisher.MIRROR.resolve() == (previous if target == bridge else bridge)
        assert publisher._activate_existing(target.name, dry_run=False, rollout='rolling',
                rollout_reason='two-phase checkout custody rehearsal', proofs=proofs) == 0
        assert publisher.MIRROR.resolve() == target
        activated.append(target.name)
    assert activated == ['broker-bridge', 'checkout-contract']
    assert (bridge / 'tools/resource_broker.py').read_bytes() == (contract / 'tools/resource_broker.py').read_bytes()
    guide = (ROOT / 'docs/operating_prismabuild.md').read_text().split('## Deploying the checkout custody contract (#1429)', 1)[1]
    activation = guide.index('--activate-generation STAGED_NAME')
    assert guide.index('After successful activation, wait') > activation
    assert guide.index('Take and verify the full durable queue/reservation backup now') > activation
