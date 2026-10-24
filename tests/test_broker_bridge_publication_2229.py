"""The broker bridge survives normal publication and an external maintenance hold."""
import json
import os
from pathlib import Path

from test_client_upgrade import setup
from test_receipt_activation_direction import private_store
from test_rolling_activation_with_installed_updater_proof import (
    generation, publisher, proof_action, enrolled_host, HOSTS, RECEIPT_LEAVES)

ROOT = Path(__file__).resolve().parents[1]

def test_broker_bridge_is_published_before_custody_and_survives_the_gate(setup):
    from test_client_upgrade import upgrade
    updater, backend, generation = setup
    member = upgrade.MEMBERS['resource_broker.py']
    candidate = (ROOT / "tools/fleet/resource_broker.py").read_bytes()
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
