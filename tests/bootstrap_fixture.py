"""Private sealed generations and authenticated-channel substitutes for policy tests."""
from copy import deepcopy
from pathlib import Path
import time

import pytest

from prismabuild import compatibility_window as window
from prismabuild.digest_primitives import canonical_sha256, raw_sha256, compact_ascii_json_bytes
from generation_fixture import generation_receipt

ROOT = Path(__file__).resolve().parents[1]


def bootstrap_case(tmp_path, monkeypatch):
    members = {name: ('tools/' + name if name in ('upgrade_client.py', 'resource_broker.py', 'resource_payload.py')
                     else 'src/prismabuild/' + name) for name in window.BOOTSTRAP_MEMBERS}
    identities, file_maps = [], []
    for label, commit in (('old', 'a' * 40), ('final', 'b' * 40)):
        root = tmp_path / 'runtime-generations' / label
        files, named = {}, {}
        for name, member in members.items():
            if label == 'old' and name in window.BOOTSTRAP_LEAVES:
                continue
            source = ROOT / ('tools/fleet/' + name if member.startswith('tools/') else member)
            raw = source.read_bytes()
            if name == 'upgrade_client.py' and label == 'old':
                raw = b'MEMBERS = {}\nOPTIONAL_MEMBERS = {}\nCLIENT_UPGRADE_PROTOCOL = 2\n'
            path = root / member
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            files[member] = named[name] = raw_sha256(raw)
        tool = root / 'tools/bootstrap_client.py'
        tool.parent.mkdir(parents=True, exist_ok=True)
        tool.write_bytes((ROOT / 'tools/fleet/bootstrap_client.py').read_bytes())
        files['tools/bootstrap_client.py'] = raw_sha256(tool.read_bytes())
        publisher = root / 'tools/publish_runtime.py'
        publisher.write_bytes((ROOT / 'tools/fleet/publish_runtime.py').read_bytes())
        files['tools/publish_runtime.py'] = raw_sha256(publisher.read_bytes())
        roster_path = root / 'tools/fleet_boxes.json'
        roster_path.write_bytes(compact_ascii_json_bytes({'boxes': {'one': {}, 'two': {'_alias': 'alias-two'}}}))
        files['tools/fleet_boxes.json'] = raw_sha256(roster_path.read_bytes())
        receipt_raw = compact_ascii_json_bytes(generation_receipt(root.name, commit, files))
        (root / 'RUNTIME_VERSION.json').write_bytes(receipt_raw)
        identities.append({'generation': root.name, 'commit': commit, 'directory': str(root),
                           'receipt_sha256': raw_sha256(receipt_raw), 'content_sha256': canonical_sha256(files)})
        file_maps.append(named)
        for path in root.rglob('*'):
            path.chmod(0o555 if path.is_dir() else 0o444)
        root.chmod(0o555)
    for state in ('ready', 'claimed'):
        (tmp_path / 'pb-queue' / state).mkdir(parents=True, exist_ok=True)
    source, target = identities
    # Synthetic fixtures exercise the predicate; a separate assertion pins the
    # production-only designation. No fixture is usable as a live assessment.
    monkeypatch.setattr(window, 'BOOTSTRAP_SOURCE', {k: v for k, v in source.items() if k != 'directory'})
    plan = {'schema': 'prismabuild.client_bootstrap.plan.v1', 'id': 'test-bootstrap', 'direction': 'forward',
            'source': source, 'target': target, 'source_files': file_maps[0], 'target_files': file_maps[1],
            'members': members, 'roster': {'one': ['one'], 'two': ['alias-two', 'two']},
            'boots': {'one': 'boot-one', 'two': 'boot-two'}, 'enrollments': {'one': 'e' * 64, 'two': 'f' * 64},
            'locks': {'one': 123, 'two': 456}, 'roles': {},
            'brokers': {'one': {'pid': 100, 'starttime': '1'}, 'two': {'pid': 101, 'starttime': '2'}},
            'tool_sha256': raw_sha256((ROOT / 'tools/fleet/bootstrap_client.py').read_bytes())}
    designation = {'rollback': source, 'mode': 'operator-restoration', 'automatic_reachability': False,
                   'retained_leaves': list(window.BOOTSTRAP_LEAVES)}
    runtime = {'version': [3, 12, 0], 'elf_machine': 62,
               'executable': {'path': '/usr/bin/python3', 'resolved_path': '/usr/bin/python3.12',
                              'bytes': 100, 'sha256': '9' * 64}}
    plan['qualified_runtimes'] = [runtime]
    now = time.time()
    rows = {host: {'runtime': runtime, 'schema': 'prismabuild.client_bootstrap.observation.v1', 'host': host,
             'nonce': 'c' * 64, 'observed_unix': now, 'boot_id': plan['boots'][host], 'broker_process': plan['brokers'][host],
             'source': source, 'target': target, 'plan_sha256': canonical_sha256(plan),
             'files': deepcopy(file_maps[1]), 'executing_sha256': file_maps[1]['upgrade_client.py'],
             'captured_sha256': file_maps[1]['upgrade_client.py'], 'reader_loaded': True, 'proof_loaded': True,
             'metadata': {name: {'uid': 0, 'gid': 0, 'mode': 0o644} for name in members},
             'fenced': True, 'oneshot_exited': True, 'automatic_journal': False, 'epoch': None,
             'upgrade_lock': {'held': True, 'inode': plan['locks'][host]},
             'enrollment_sha256': plan['enrollments'][host], 'journal_sha256': 'd' * 64,
             'broker': {'health': True, 'active_scopes': 0, 'maintenance_durable_protocol': 1, 'draining': False,
                        'installed_sha256': {name: file_maps[0][name] for name in window.BOOTSTRAP_SERVICE_MEMBERS}},
             'gate': {'draining': False}} for host in plan['roster']}
    observations = window.RootBootstrapResponses(rows, nonce='c' * 64, started_unix=now - .01)
    qualification = window.BootstrapQualification(plan, action_keys=['1' * 64], runtimes=[runtime])
    return plan, observations, qualification, designation


@pytest.fixture
def bootstrap(tmp_path, monkeypatch):
    return bootstrap_case(tmp_path, monkeypatch)


def assess_case(case):
    plan, observations, qualification, designation = case
    return window.require_installed_bootstrap(Path(plan['source']['directory']), Path(plan['target']['directory']),
        observations, roster=plan['roster'], qualification=qualification, rollback_designation=designation)
