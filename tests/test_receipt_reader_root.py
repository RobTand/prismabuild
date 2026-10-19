"""Actual installed root readers cross the dependency bridge in both directions."""
import inspect
import json
import os
from pathlib import Path
import stat
import runpy
import subprocess
import sys

import pytest

from client_proof_bridge_fixture import _fresh, publication, reader_process
from receipt_reader_fixture import (
    ROOT, RECEIPT_LEAVES, prepared_receipt_bridge, publish_updater,
    publish_receipt_closure, receipt_source_artifact, remove_receipt_closure,
)
from test_client_proof_prior_trust import tmp_path


CURRENT = ROOT / 'tools/fleet/upgrade_client.py'


def installed_bytes(publication):
    return {path.name: path.read_bytes() for path in publication[1].iterdir() if path.is_file()}


def tick(publication, expected):
    result = _fresh(publication)
    assert result['exception'] is None, result
    assert result['result']['state'] == expected, result
    assert not (publication[2] / 'transaction.json').exists()
    return result


def old_to_bridge(publication):
    install = publication[1]
    old = receipt_source_artifact('base/tools/fleet/upgrade_client.py')
    (install / 'upgrade_client.py').write_bytes(old)
    assert not any((install / leaf).exists() for leaf in RECEIPT_LEAVES)
    tick(publication, 'updated')
    assert (install / 'upgrade_client.py').read_bytes() == prepared_receipt_bridge()
    assert not any((install / leaf).exists() for leaf in RECEIPT_LEAVES)


def bridge_with_leaves(publication):
    old_to_bridge(publication)
    publish_receipt_closure(publication)
    tick(publication, 'updated')
    for name, member in RECEIPT_LEAVES.items():
        assert (publication[1] / name).read_bytes() == (ROOT / member).read_bytes()


def current_reader(publication):
    bridge_with_leaves(publication)
    publish_updater(publication, CURRENT.read_bytes())
    tick(publication, 'updated')
    tick(publication, 'current')
    assert (publication[1] / 'upgrade_client.py').read_bytes() == CURRENT.read_bytes()


def test_reader_first_forward_and_reverse_preserve_exact_dependency_closure(publication):
    current_reader(publication)
    generation, install, _, _, backend, _ = publication
    raw = (generation / 'RUNTIME_VERSION.json').read_bytes()
    final = installed_bytes(publication)
    tick(publication, 'current')
    assert installed_bytes(publication) == final
    assert (generation / 'RUNTIME_VERSION.json').read_bytes() == raw
    publish_updater(publication, prepared_receipt_bridge())
    tick(publication, 'updated')
    assert all((install / name).exists() for name in RECEIPT_LEAVES)
    remove_receipt_closure(publication)
    tick(publication, 'updated')
    tick(publication, 'current')
    assert (install / 'upgrade_client.py').read_bytes() == prepared_receipt_bridge()
    assert not any((install / name).exists() for name in RECEIPT_LEAVES)
    status = json.loads(backend.read_bytes())
    assert status['active'] == 0 and status['holder'] is None and status['running'] is True


@pytest.mark.parametrize('direction', ['bridge-without-leaves', 'current-to-old', 'partial-closure'])
def test_incapable_reader_or_partial_closure_refuses_before_transaction(publication, direction):
    if direction == 'bridge-without-leaves':
        old_to_bridge(publication)
        publish_updater(publication, CURRENT.read_bytes())
    else:
        current_reader(publication)
        if direction == 'current-to-old':
            publish_updater(publication, receipt_source_artifact('base/tools/fleet/upgrade_client.py'))
            remove_receipt_closure(publication)
        else:
            generation = publication[0]
            path = generation / 'RUNTIME_VERSION.json'
            value = json.loads(path.read_bytes())
            value['files'].pop(RECEIPT_LEAVES['digest_primitives.py'])
            path.write_text(json.dumps(value))
    before = installed_bytes(publication)
    operations = json.loads(publication[4].read_bytes())['operations']
    result = _fresh(publication)
    assert result['exception'] == 'ValueError', result
    assert any(word in result['error'] for word in ('receipt', 'proof', 'protocol')), result
    assert installed_bytes(publication) == before
    assert not (publication[2] / 'transaction.json').exists()
    after = json.loads(publication[4].read_bytes())['operations'][len(operations):]
    assert not set(after) & {'maintenance_begin', 'stop', 'start', 'maintenance_end'}


@pytest.mark.parametrize('phase', ['install-leaves', 'activate-final', 'restore-bridge', 'remove-leaves'])
@pytest.mark.parametrize('cut', ['journal-durable', 'updater-after-replace', 'install-after-sync'])
def test_interrupted_transactions_restart_the_actual_installed_reader(publication, phase, cut):
    if phase == 'install-leaves':
        old_to_bridge(publication)
        publish_receipt_closure(publication)
    elif phase == 'activate-final':
        bridge_with_leaves(publication)
        publish_updater(publication, CURRENT.read_bytes())
    else:
        current_reader(publication)
        publish_updater(publication, prepared_receipt_bridge())
        if phase == 'remove-leaves':
            tick(publication, 'updated')
            remove_receipt_closure(publication)
    before = installed_bytes(publication)
    proof = publication[1] / 'park_proof.py'
    os.chown(proof, 0, 65533)
    proof.chmod(0o660)
    with reader_process(publication, cut=cut) as child:
        assert child.message() == {'bridge_cut': cut, 'phase': 'forward'}
        journal = json.loads((publication[2] / 'transaction.json').read_bytes())
        assert journal['previous_proof_file'] == {'uid': 0, 'gid': 65533, 'mode': 0o660}
        child.kill()
    tick(publication, 'rolled_back')
    assert installed_bytes(publication) == before
    info = proof.stat()
    assert (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) == (0, 65533, 0o660)


@pytest.mark.parametrize('leaf', ['digest', 'receipt'])
@pytest.mark.parametrize('operation', ['after-replace', 'after-absence'])
def test_dependency_boundaries_keep_a_runnable_recovery_reader(publication, leaf, operation):
    if operation == 'after-replace':
        old_to_bridge(publication)
        publish_receipt_closure(publication)
    else:
        current_reader(publication)
        publish_updater(publication, prepared_receipt_bridge())
        tick(publication, 'updated')
        remove_receipt_closure(publication)
    before = installed_bytes(publication)
    cut = leaf + '-' + operation
    with reader_process(publication, cut=cut) as child:
        assert child.message() == {'bridge_cut': cut, 'phase': 'forward'}
        if operation == 'after-absence':
            assert (publication[1] / 'upgrade_client.py').read_bytes() == prepared_receipt_bridge()
        child.kill()
    tick(publication, 'rolled_back')
    assert installed_bytes(publication) == before


def test_final_recovers_before_importing_even_a_missing_installed_reader(publication):
    current_reader(publication)
    publish_updater(publication, prepared_receipt_bridge())
    with reader_process(publication, cut='journal-durable') as child:
        assert child.message() == {'bridge_cut': 'journal-durable', 'phase': 'forward'}
        child.kill()
    for name in RECEIPT_LEAVES:
        (publication[1] / name).unlink()
    assert (publication[1] / 'upgrade_client.py').read_bytes() == CURRENT.read_bytes()
    tick(publication, 'rolled_back')
    publish_updater(publication, CURRENT.read_bytes())
    tick(publication, 'current')
    assert all((publication[1] / name).exists() for name in RECEIPT_LEAVES)


def test_fixed_old_reader_cannot_transport_the_new_leaf_pair(publication):
    generation, install, _, _, _, _ = publication
    (install / 'upgrade_client.py').write_bytes(receipt_source_artifact('base/tools/fleet/upgrade_client.py'))
    publish_updater(publication, CURRENT.read_bytes())
    publish_receipt_closure(publication)
    tick(publication, 'updated')
    assert (install / 'upgrade_client.py').read_bytes() == CURRENT.read_bytes()
    assert not any((install / name).exists() for name in RECEIPT_LEAVES)
    observed = _fresh(publication)
    assert observed['exception'] == 'ValueError', observed
    assert 'missing installed receipt dependency' in observed['error']


@pytest.mark.parametrize('fault', ['missing', 'untrusted', 'invalid-syntax'])
def test_both_dependency_sources_are_captured_before_either_executes(publication, fault):
    current_reader(publication)
    install = publication[1]
    marker = install / 'partial-execution'
    digest = install / 'digest_primitives.py'
    digest.write_bytes(digest.read_bytes() + f"\nopen({str(marker)!r}, 'w').write('partial')\n".encode())
    receipt = install / 'generation_receipt.py'
    if fault == 'missing':
        receipt.unlink()
    elif fault == 'untrusted':
        os.chown(receipt, 65534, 65533)
    else:
        receipt.write_bytes(b'not valid Python !!!')
    observed = _fresh(publication)
    assert observed['exception'] in ('ValueError', 'SyntaxError'), observed
    assert not marker.exists()
    assert not (publication[2] / 'transaction.json').exists()


def selected_installed_reader():
    # Only the frozen runtime helper holds the selected profile; changing it would void the qualified captures.
    for frame in inspect.stack():
        if frame.filename == str(ROOT / 'tests/proof_container_runtime.py') and frame.function == 'run':
            reader = frame.frame.f_locals['profile'].installed_reader
            return reader['version'], reader['executable']['resolved_path'], reader['executable']['sha256']
    raise AssertionError('no active proof_container_runtime.run frame selects an installed reader')


def test_actual_installed_reader_drops_credentials_before_receipt_intake(publication):
    current_reader(publication)
    generation, install, _, config_path, _, _ = publication
    config = json.loads(config_path.read_bytes())
    config['reader_uid'] = 65534
    config_path.write_text(json.dumps(config))
    interpreter = Path('/usr/bin/python3')
    created = not interpreter.exists()
    if created:
        interpreter.symlink_to(sys.executable)
    try:
        probe = subprocess.check_output([str(interpreter), '-I', '-c',
            "import base64, hashlib, json, os, pathlib, sys; "
            "raw=pathlib.Path('/proc/self/exe').read_bytes(); "
            "print(json.dumps(dict(version=list(sys.version_info[:3]), "
            "executable=sys.executable, sha256=hashlib.sha256(raw).hexdigest(), "
            "matches=pathlib.Path(sys.executable).read_bytes()==raw, "
            "binary=base64.b64encode(raw).decode(), affinity=sorted(os.sched_getaffinity(0)))))"], text=True)
        observed = json.loads(probe)
        identity = observed['version'], str(interpreter.resolve(strict=True)), observed['sha256']
        assert identity == selected_installed_reader() and observed['matches'] is True
        assert observed['affinity'] == sorted(os.sched_getaffinity(0))
        print('c15-root-export-runtime: ' + probe.strip())
        program = runpy.run_path(str(install / 'upgrade_client.py'), run_name='installed_runtime_export')
        expected = program['desired'](config)
        actual = program['desired_as_reader'](config_path, config)
    finally:
        if created:
            interpreter.unlink()
    assert actual == expected
    assert set(actual[1]) == {'upgrade_client.py', 'resource_broker.py', 'resource_payload.py',
                              'gpu_memory.py', 'gpu_capacity.py', 'park_proof.py', *RECEIPT_LEAVES}
    for name, member in RECEIPT_LEAVES.items():
        assert actual[1][name] == (generation / member).read_bytes() == (install / name).read_bytes()


def test_enrollment_copies_the_exact_leaf_pair_before_the_installed_consumer(tmp_path):
    stage = tmp_path / 'enrollment'
    stage.mkdir()
    for name, member in RECEIPT_LEAVES.items():
        (stage / name).write_bytes((ROOT / member).read_bytes())
    (stage / 'upgrade_client.py').write_bytes(CURRENT.read_bytes())
    installer = stage / 'install_client_upgrader.sh'
    installer.write_bytes((ROOT / 'tools/fleet/install_client_upgrader.sh').read_bytes())
    helpers = tmp_path / 'commands'
    helpers.mkdir()
    trace = tmp_path / 'install-order.jsonl'
    real_install = Path('/usr/bin/install')
    wrapper = helpers / 'install'
    wrapper.write_text(f'#!{sys.executable}\nimport json, subprocess, sys\n'
                       f'with open({str(trace)!r}, "a") as stream: stream.write(json.dumps(sys.argv[1:])+"\\n")\n'
                       f'raise SystemExit(subprocess.call([{str(real_install)!r}, *sys.argv[1:]]))\n')
    wrapper.chmod(0o755)
    service = helpers / 'systemctl'
    service.write_text('#!/bin/sh\nexit 0\n')
    service.chmod(0o755)
    done = subprocess.run(['/bin/bash', str(installer)], capture_output=True, text=True,
                          env={**os.environ, 'PATH': str(helpers) + ':' + os.environ['PATH']}, timeout=30)
    assert done.returncode == 0, (done.stdout, done.stderr)
    installed = Path('/opt/prismabuild-resource-broker')
    copies = [json.loads(line) for line in trace.read_text().splitlines()]
    files = [Path(row[-1]).name for row in copies if row[-1].endswith('.py')]
    assert files == ['digest_primitives.py', 'generation_receipt.py', 'upgrade_client.py']
    for name in files:
        assert (installed / name).read_bytes() == (stage / name).read_bytes()
        info = (installed / name).stat()
        assert (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) == (0, 0, 0o644)


@pytest.mark.parametrize('cut', ['journal-durable', 'restore-updater-before-replace', 'restore-updater-after-replace',
                                  'updater-before-replace', 'updater-after-replace',
                                  'install-after-sync', 'journal-before-clear'])
def test_exact_a174_preseeded_five_member_journal_recovers_with_leaves_retained(publication, cut):
    from test_client_bootstrap_root import a174_updater
    from prismabuild.digest_primitives import raw_sha256
    generation, install, state, _, backend, _ = publication
    old = a174_updater()
    # a174's five-member selector cannot journal these independently seeded leaves.
    for name, member in {**RECEIPT_LEAVES, 'park_proof.py': 'src/prismabuild/park_proof.py'}.items():
        (install / name).write_bytes((ROOT / member).read_bytes())
    (install / 'upgrade_client.py').write_bytes(old)
    publish_receipt_closure(publication)
    publish_updater(publication, CURRENT.read_bytes())
    effective = 'updater-after-replace' if cut.startswith('restore-') else cut
    with reader_process(publication, cut=effective) as child:
        assert child.message()['bridge_cut'] == effective
        journal = json.loads((state / 'transaction.json').read_bytes())
        assert set(journal['previous']) == {'resource_broker.py', 'resource_payload.py', 'gpu_memory.py',
                                           'gpu_capacity.py', 'upgrade_client.py'}
        assert 'previous_proof_file' not in journal
        assert 'park_proof.py' not in journal['desired']['files']
        child.kill()
    if cut.startswith('restore-'):
        with reader_process(publication, cut=cut) as child:
            assert child.message()['bridge_cut'] == cut
            child.kill()
    result = _fresh(publication)
    assert result['exception'] is None, result
    assert result['result']['state'] == 'rolled_back'
    assert not (state / 'transaction.json').exists()
    assert (install / 'upgrade_client.py').read_bytes() == old
    for name, member in RECEIPT_LEAVES.items():
        assert (install / name).read_bytes() == (ROOT / member).read_bytes()
    assert (install / 'park_proof.py').read_bytes() == (ROOT / 'src/prismabuild/park_proof.py').read_bytes()
