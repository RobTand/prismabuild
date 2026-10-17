"""Allocated old-writer intents for tests of retained legacy batch recovery."""
from prismabuild import batch_ledger, output_contract, storage_tiers
from prismabuild.legacy_funding import LegacyFunding


def allocated_legacy_intent(queue, instance, descriptors, *, batch_id, tier, mover_key):
    """Model the prior writer's completed allocation before the tested commit."""
    manifest = output_contract.output_manifest_sha256(descriptors)
    batches = batch_ledger.BatchLedger.for_instance(queue.root, instance).capture()['batches']
    if batch_id in batches:
        assert batches[batch_id]['manifest_digest'] == manifest
        return
    count = storage_tiers.stage_tokens_for_bytes(sum(int(value['bytes']) for value in descriptors))
    if count == 0:
        return
    namespace = output_contract.batch_namespace(instance, batch_id, manifest)
    record = dict(batch_id=batch_id, tier=tier, mover_key=mover_key,
                  batch_gib=count, manifest_digest=manifest)
    funding = LegacyFunding(LegacyFunding.directory(
        batch_ledger.scope_dir(queue.root, instance)) / (batch_id + '.funding.json'))
    existing = funding.read()
    if existing is not None:
        assert existing == record
        return
    ledger = queue.tier(tier)
    assert not ledger.holdings(namespace)
    assert not ledger.holdings(mover_key)
    assert ledger.take(namespace, {storage_tiers.capacity_kind_of(tier): count})
    funding.file(record)
