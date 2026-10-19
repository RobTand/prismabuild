"""The public window declaration still recognizes the delivered funding owner."""
from prismabuild import produced_output, pool, output_contract
from fleet_fixture import broker
from output_producer_fixture import Outputs
from test_prepaid_output_funding import _queue, _template, TIER, KIND


def test_the_declaration_uses_the_concrete_funding_owner(tmp_path, broker):
    queue = _queue(tmp_path)
    template = _template(str(tmp_path / "outputs"))
    instance = Outputs(queue, tmp_path, broker).bind(template, "a" * 64)
    ledger = queue.tier(TIER)
    free = ledger.free_supply()
    held = ledger.holdings(instance["owner_action_key"])
    assert produced_output.admit_funded_window(
        queue, instance, template, need_gib_per_tier={TIER: 1}) == {
            "ok": True, "mode": "prepaid-per-batch",
            "declaration_only": True, "authority": "per-batch-funding",
            "owner_action_key": instance["owner_action_key"],
            "owner_demand_terms": output_contract.owner_demand_terms(template),
            "window_gib": {TIER: 2},
            "batch_ref_schema": pool.PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1,
            "sequence": ["require_prewrite", "publish_prepaid_batch",
                         "fleet-claims-the-mover", "retire_batch",
                         "reclaim_origin", "safe_release_instance"],
        }
    assert ledger.free_supply() == free
    assert ledger.holdings(instance["owner_action_key"]) == held
    assert held[KIND] == 2
