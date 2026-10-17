"""Current commits reconcile funding; older allocated intents still recover."""
import json

import pytest

from prismabuild import batch_ledger as bl, output_contract as oc
from prismabuild import produced_output as po, tier_holdings
from test_produced_output_scope import _queue, _template, _bind, STAGE_TIER, STAGE_BARE


def _world(tmp_path, size=8):
    queue = _queue(tmp_path)
    origin = tmp_path / 'outputs'
    template = _template(str(origin), durable_maxima={
        'payload_max_bytes': 3 << 30, 'checkpoint_max_bytes': 0, 'temp_max_bytes': 0})
    instance = _bind(queue, template)['instance']
    assert bl.BatchLedger.admit(queue, instance, template)['ok']
    path = origin / 'payload.bin'
    assert bl.BatchLedger.reserve(
        queue, instance, template, batch_id='batch', tier=STAGE_TIER,
        class_bytes={'payload': size, 'checkpoint': 0, 'temp': 0},
        paths=[str(path)])['ok']
    origin.mkdir(parents=True)
    with path.open('wb') as stream:
        stream.truncate(size)
    descriptor = oc.validate_descriptor({
        'schema': oc.DESCRIPTOR_SCHEMA_V2, 'slot': 'boundary-0',
        'artifact_class': 'payload', 'path': str(path), 'bytes': size,
        'sha256': '0' * 64, 'producer_generation': '7' * 32,
        'owner_action_key': instance['owner_action_key'],
        'owner_attempt': instance['owner_attempt']}, template, instance)
    manifest = oc.output_manifest_sha256([descriptor])
    namespace = oc.batch_namespace(instance, 'batch', manifest)
    funding = bl.scope_dir(queue.root, instance) / 'funding' / 'batch.funding.json'
    return queue, template, instance, descriptor, manifest, namespace, funding


def _commit(world):
    queue, template, instance, descriptor, _, _, _ = world
    return po.commit_batch(queue, instance, template, [descriptor],
                           batch_id='batch', tier=STAGE_TIER, mover_key='a' * 64)


def test_no_pool_or_legacy_intent_refuses_without_minting(tmp_path, capsys):
    world = _world(tmp_path)
    queue, _, instance, _, _, namespace, funding = world
    ledger = queue.tier(STAGE_TIER)
    free = ledger.free_supply()
    result = _commit(world)
    effects = dict(result=result, free_before=free, free_after=ledger.free_supply(),
                   holder=ledger.holdings(namespace), mover=ledger.holdings('a' * 64),
                   intent_present=funding.exists())
    with capsys.disabled():
        print('funding-new-mint-control: ' + json.dumps(effects, sort_keys=True), flush=True)
    assert result == {'ok': False, 'refusal': 'prepaid-funding-required'}
    assert ledger.free_supply() == free
    assert ledger.holdings(namespace) == {}
    assert ledger.holdings('a' * 64) == {}
    assert not funding.exists()
    assert (bl.prewrites_dir(queue.root, instance) / 'batch.prewrite.json').exists()
    assert bl.BatchLedger.for_instance(queue.root, instance).capture()['batches'] == {}


@pytest.mark.parametrize('moved', [0, 1, 2], ids=['allocated', 'split', 'handed-over'])
def test_existing_positive_legacy_intent_recovers_without_acquire(tmp_path, monkeypatch, moved):
    world = _world(tmp_path, size=(1 << 30) + 8)
    queue, _, instance, _, manifest, namespace, funding = world
    ledger = queue.tier(STAGE_TIER)
    assert ledger.take(namespace, {STAGE_BARE: 2})
    names = set(ledger.holder_state(namespace))
    if moved:
        assert ledger.handover_count(namespace, 'a' * 64, moved) == moved
    # This is an already allocated old-writer intent, not permission to mint.
    funding.parent.mkdir(parents=True)
    funding.write_text(json.dumps({
        'batch_id': 'batch', 'tier': STAGE_TIER, 'mover_key': 'a' * 64,
        'batch_gib': 2, 'manifest_digest': manifest}, sort_keys=True) + '\n')
    free = ledger.free_supply()

    def forbidden(*args, **kwargs):
        raise AssertionError('positive legacy recovery acquired a second time')

    monkeypatch.setattr(tier_holdings.TierLedger, 'take', forbidden)
    result = _commit(world)
    assert result['ok'] is True, result
    assert result['funding'] == 'legacy'
    assert ledger.free_supply() == free
    assert ledger.holdings(namespace) == {}
    assert set(ledger.holder_state('a' * 64)) == names
    assert not funding.exists()
    assert not (bl.prewrites_dir(queue.root, instance) / 'batch.prewrite.json').exists()
    assert 'batch' in bl.BatchLedger.for_instance(queue.root, instance).capture()['batches']
    assert _commit(world)['duplicate'] is True


def test_unreadable_legacy_intent_retains_without_minting(tmp_path):
    world = _world(tmp_path)
    queue, _, _, _, _, namespace, funding = world
    funding.parent.mkdir(parents=True)
    funding.write_text('{')
    ledger = queue.tier(STAGE_TIER)
    free = ledger.free_supply()
    result = _commit(world)
    assert result['ok'] is False
    assert result['refusal'].startswith('unknown-retain: funding intent unreadable:')
    assert ledger.free_supply() == free
    assert ledger.holdings(namespace) == {}
    assert ledger.holdings('a' * 64) == {}
    assert funding.read_text() == '{'
