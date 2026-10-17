"""The narrow mint refusal preserves zero credit and earlier diagnostics."""
import json

import pytest

from prismabuild import produced_output
from test_commit_requires_existing_funding import _world, _commit
from test_produced_output_scope import STAGE_TIER, STAGE_BARE


def test_zero_total_keeps_its_original_outer_refusal(tmp_path):
    world = _world(tmp_path)
    queue, template, instance, descriptor, _, namespace, funding = world
    # Descriptors and the total-stage quantizer both require positive bytes.
    from pathlib import Path
    Path(descriptor['path']).unlink()
    ledger = queue.tier(STAGE_TIER)
    free = ledger.free_supply()
    with pytest.raises(ValueError, match='^a residency range must be a positive number of bytes$'):
        produced_output.commit_batch(
            queue, instance, template, [], batch_id='batch',
            tier=STAGE_TIER, mover_key='a' * 64)
    assert ledger.free_supply() == free
    assert ledger.holdings(namespace) == {}
    assert ledger.holdings('a' * 64) == {}
    assert not funding.exists()


def test_the_owned_zero_credit_branch_does_not_acquire(tmp_path):
    from prismabuild import batch_ledger, output_contract
    world = _world(tmp_path)
    queue, template, instance, _, _, _, funding = world
    ledger = queue.tier(STAGE_TIER)
    free = ledger.free_supply()
    manifest = output_contract.output_manifest_sha256([])
    namespace = output_contract.batch_namespace(instance, 'batch', manifest)
    with batch_ledger.path_locks(queue, instance):
        result = queue.funding.commit_stage(
            instance, template, batch_id='batch', manifest_digest=manifest,
            batch_ns=namespace, tier=STAGE_TIER, mover='a' * 64,
            kind=STAGE_BARE, batch_total=0, batch_gib=0)
    assert result.refusal is None
    assert result.prepaid is None
    assert result.path == funding
    assert ledger.free_supply() == free
    assert ledger.holdings(namespace) == {}
    assert ledger.holdings('a' * 64) == {}
    assert not funding.exists()


@pytest.mark.parametrize('destination', ['batch', 'mover'])
def test_unattributed_holdings_keep_the_original_unknown_diagnostic(tmp_path, destination):
    world = _world(tmp_path)
    queue, _, _, _, _, namespace, funding = world
    ledger = queue.tier(STAGE_TIER)
    holder = namespace if destination == 'batch' else 'a' * 64
    assert ledger.take(holder, {STAGE_BARE: 1})
    names = set(ledger.holder_state(holder))
    free = ledger.free_supply()
    assert _commit(world) == {'ok': False, 'refusal': 'unknown-retain: unfunded holdings'}
    assert ledger.free_supply() == free
    assert set(ledger.holder_state(holder)) == names
    assert not funding.exists()


def test_mismatched_legacy_intent_keeps_the_original_binding_refusal(tmp_path):
    world = _world(tmp_path)
    queue, _, _, _, manifest, _, funding = world
    funding.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(dict(batch_id='batch', tier=STAGE_TIER, mover_key='b' * 64,
                          batch_gib=1, manifest_digest=manifest), sort_keys=True) + '\n'
    funding.write_text(raw)
    free = queue.tier(STAGE_TIER).free_supply()
    assert _commit(world) == {'ok': False, 'refusal': 'batch-id-in-use'}
    assert queue.tier(STAGE_TIER).free_supply() == free
    assert funding.read_text() == raw


def test_matching_legacy_intent_with_lost_tokens_retains(tmp_path):
    world = _world(tmp_path)
    queue, _, _, _, manifest, _, funding = world
    funding.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(dict(batch_id='batch', tier=STAGE_TIER, mover_key='a' * 64,
                          batch_gib=1, manifest_digest=manifest), sort_keys=True) + '\n'
    funding.write_text(raw)
    free = queue.tier(STAGE_TIER).free_supply()
    assert _commit(world) == {'ok': False, 'refusal': 'unknown-retain: funded tokens lost'}
    assert queue.tier(STAGE_TIER).free_supply() == free
    assert funding.read_text() == raw
