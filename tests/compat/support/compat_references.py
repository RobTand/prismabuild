"""Concrete source locations for the modules selected by the isolated loader."""
import ast
from functools import lru_cache


def module_paths(module):
    relative = module.replace(".", "/")
    if module.startswith("prismabuild."):
        return ("src/" + relative + ".py",)
    paths = ("tools/fleet/" + relative + ".py", "tools/" + relative + ".py")
    if module == "docker":
        paths += ("tools/fleet/docker", "tools/docker")
    return paths


# Exact C13 catalogue relocation, used only for independent pre-C13 closures.
C13_PRIOR_ENTRIES = {'prismabuild.batch_ledger:BatchLedger.capture': 'prismabuild.produced_output:_read_commitments', 'prismabuild.batch_ledger:declared_consumers': 'prismabuild.produced_output:_declared_consumers', 'prismabuild.batch_ledger:read_consumer_release': 'prismabuild.produced_output:_consumer_release', 'prismabuild.batch_ledger:BatchLedger.record': 'prismabuild.produced_output:batch_record', 'prismabuild.output_contract:validate_descriptor': 'prismabuild.produced_output:validate_descriptor', 'prismabuild.output_contract:checked_origin_ref': 'prismabuild.produced_output:_checked_origin_ref', 'prismabuild.output_contract:validate_template': 'prismabuild.produced_output:validate_template', 'prismabuild.output_contract:validate_instance': 'prismabuild.produced_output:validate_instance', 'prismabuild.batch_ledger:BatchLedger.file': 'prismabuild.produced_output:_write_commitments', 'prismabuild.batch_ledger:BatchLedger.commit_origin': 'prismabuild.produced_output:commit_origin_batch', 'prismabuild.output_contract:origin_batch_ref': 'prismabuild.produced_output:origin_batch_ref', 'prismabuild.batch_ledger:declare_instance': 'prismabuild.produced_output:declare_instance', 'prismabuild.batch_ledger:BatchLedger.due_mover_rows': 'prismabuild.produced_output:due_mover_rows'}

# Fixed source relocations: a captured closure must contain the selected definition.
CURRENT_PRIOR_ENTRIES = {'prismabuild.submission:snapshot_checkout': 'pbrun:build_git_checkout_snapshot', 'prismabuild.residency_plan:prepare_window': 'pbrun:residency_stage_rows', 'prismabuild.residency_plan:prepare_window.<locals>.seal_stage_mover': 'pbrun:residency_stage_rows.<locals>.seal_stage_mover', 'prismabuild.residency_plan:prepare_window.<locals>.seal_ram_chunk': 'pbrun:residency_stage_rows.<locals>.seal_ram_chunk', 'prismabuild.stage_move:_LandingReport.write': 'stage_move:_LandingReport.write', 'prismabuild.census:_transport': 'pbstatus:_transport', 'prismabuild.scratch_lifetime:selections': 'prismabuild.local_scratch:_scratch_lifetime_selections', 'prismabuild.scratch_lifetime:build_scratch_lifetime_selection': 'prismabuild.local_scratch:build_scratch_lifetime_selection', 'prismabuild.scratch_lifetime:validate_record': 'prismabuild.local_scratch:_scratch_lifetime_record', 'prismabuild.scratch_lifetime:register_attempt': 'prismabuild.pool:PoolQueue._record_scratch_lifetimes', 'prismabuild.d38_gate:load_plan': 'd38_gate:load_plan', 'prismabuild.d38_gate:_parse_receipt': 'd38_gate:_parse_receipt', 'prismabuild.submission:parse_progress_phases': 'pbrun:parse_progress_phases', 'prismabuild.submission:parse_awaited_batch': 'pbrun:parse_awaited_batch', 'prismabuild.lifecycle:_file_unreadable': 'prismabuild.pool:PoolQueue._file_unreadable', 'prismabuild.stage_move:move': 'stage_move:move', 'prismabuild.ram_promote:promote': 'ram_promote:promote', 'prismabuild.ram_promote:_refused': 'ram_promote:_refused', 'prismabuild.submission:seal': 'pbrun:seal_action_from_template', 'prismabuild.submission:build_stamp_closure': 'pbrun:build_stamp_closure', 'prismabuild.stage_release:_ledger_unreadable_receipt': 'stage_release:_ledger_unreadable_receipt', 'prismabuild.stage_release:_receiptless_refusal': 'stage_release:_receiptless_refusal', 'prismabuild.stage_release:_unresolved_reports': 'stage_release:_unresolved_reports', 'prismabuild.stage_release:_uncharged_owner_report': 'stage_release:_uncharged_owner_report', 'prismabuild.stage_release:sweep_dead_owner_fragments': 'stage_release:sweep_dead_owner_fragments', 'prismabuild.stage_release:_evict_uncharged_owner': 'stage_release:_evict_uncharged_owner', 'prismabuild.stage_release:_retire_produced_orphan': 'stage_release:_retire_produced_orphan', 'prismabuild.staged_path:stage_root_refusal': 'stage_release:stage_root_refusal', 'prismabuild.staged_path:register_stage_root': 'stage_release:register_stage_root', 'prismabuild.staged_path:_refused_receipt': 'stage_release:_refused_receipt', 'prismabuild.staged_path:prune_stale_mentions': 'stage_release:prune_stale_mentions', 'prismabuild.staged_path:recover_orphaned_range': 'stage_release:recover_orphaned_range', 'prismabuild.staged_path:reconcile': 'stage_release:reconcile', 'prismabuild.staged_path:_evict_shared_locked': 'stage_release:_evict_shared_locked', 'prismabuild.staged_path:_evict_owned': 'stage_release:_evict_owned', 'prismabuild.runtime_publication:_publication_receipt': 'prismabuild.runtime_publication:_receipt'}


CURRENT_PRIOR_ENTRIES.update({
    "prismabuild.filesystem_floor:LedgerGate.reserve": "prismabuild.filesystem_floor:LedgerGate.granted",
    'prismabuild.pbtest_capabilities:load_config': 'pbtest_capabilities:load_config',
    'prismabuild.tier_cycle:_cycle': 'tier_loop:_cycle',
    'prismabuild.tier_cycle:publish_landing_expectations': 'tier_loop:publish_landing_expectations',
    'prismabuild.decomposition:DataPlans.rows_for_children': 'pbcampaign:frozen_child_data_plans',
    'prismabuild.decomposition:DataPlans.freeze': 'pbcampaign:frozen_child_data_plans',
})


def absence_module_paths(root, reference):
    """Declare the fixed historical module even when both relocated owners are absent."""
    paths = module_paths(reference.split(":")[0])
    prior = CURRENT_PRIOR_ENTRIES.get(reference)
    if prior is not None and not any((root / path).is_file() for path in paths):
        return module_paths(prior.split(":")[0])
    return paths


@lru_cache(maxsize=128)
def source_definitions(raw):
    """Index exact source bytes; paths are read again before every lookup."""
    names = set()

    def visit(node, prefix=""):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            qualified = prefix + node.name
            names.add(qualified)
            prefix = qualified + ("." if isinstance(node, ast.ClassDef) else ".<locals>.")
        for child in ast.iter_child_nodes(node):
            visit(child, prefix)

    visit(ast.parse(raw))
    return frozenset(names)


def _has_definition(root, reference):
    module, name = reference.split(":")
    path = next((root / item for item in module_paths(module) if (root / item).is_file()), None)
    return path is not None and name in source_definitions(path.read_bytes())


def generation_reference(root, reference):
    if reference in CURRENT_PRIOR_ENTRIES:
        if _has_definition(root, reference):
            return reference
        prior = CURRENT_PRIOR_ENTRIES[reference]
        if not _has_definition(root, prior):
            raise ValueError(("generation has no exact current or retained source owner", reference, prior))
        return prior
    if not (root / "src/prismabuild/funding.py").is_file():
        reference = C12_PRIOR_ENTRIES.get(reference, reference)
    if not (root / "src/prismabuild/batch_ledger.py").is_file():
        return C13_PRIOR_ENTRIES.get(reference, reference)
    return reference

C12_PRIOR_ENTRIES = {'prismabuild.funding:FundingLedger.validate_window': 'prismabuild.pool:PoolQueue.validate_funding', 'prismabuild.funding:FundingLedger.read_window': 'prismabuild.pool:PoolQueue.read_funding', 'prismabuild.funding:FundingLedger.window_evidence': 'prismabuild.pool:PoolQueue.read_funding_evidence', 'prismabuild.funding:FundingLedger.write_window': 'prismabuild.pool:PoolQueue.write_funding', 'prismabuild.funding:FundingLedger._write_locked': 'prismabuild.pool:PoolQueue._write_funding_record_locked', 'prismabuild.funding:FundingLedger.advance_window': 'prismabuild.pool:PoolQueue.advance_funding_state', 'prismabuild.funding:FundingLedger.advance_locked': 'prismabuild.pool:PoolQueue._advance_funding_state_locked', 'prismabuild.funding:FundingLedger.cover_window': 'prismabuild.pool:PoolQueue.funded_cover', 'prismabuild.funding:FundingLedger.reserve_window': 'prismabuild.pool:PoolQueue.reserve_fence', 'prismabuild.funding:FundingLedger._reserve_window_locked': 'prismabuild.pool:PoolQueue._reserve_fence_locked', 'prismabuild.funding:FundingLedger._transfer_window': 'prismabuild.pool:PoolQueue.transfer_fence', 'prismabuild.funding:FundingLedger.validate_output': 'prismabuild.pool:PoolQueue.validate_output_funding', 'prismabuild.funding:FundingLedger.read_output': 'prismabuild.pool:PoolQueue.read_output_funding', 'prismabuild.funding:FundingLedger.output_evidence': 'prismabuild.pool:PoolQueue.output_funding_file_state', 'prismabuild.funding:FundingLedger.write_output': 'prismabuild.pool:PoolQueue.write_output_funding', 'prismabuild.funding:FundingLedger._refuse_retired_output_funding': 'prismabuild.pool:PoolQueue._refuse_retired_output_funding', 'prismabuild.funding:FundingLedger.retire_output': 'prismabuild.pool:PoolQueue.retire_terminal_output_funding', 'prismabuild.funding:FundingLedger._output_funding_terminal': 'prismabuild.pool:PoolQueue._output_funding_terminal', 'prismabuild.funding:FundingLedger._output_live_owner': 'prismabuild.pool:PoolQueue._output_live_owner', 'prismabuild.funding:FundingLedger._output_live_mover': 'prismabuild.pool:PoolQueue._output_live_mover', 'prismabuild.funding:FundingLedger._output_owner_authority': 'prismabuild.pool:PoolQueue._output_owner_authority', 'prismabuild.funding:FundingLedger._output_batch_authority': 'prismabuild.pool:PoolQueue._output_batch_authority', 'prismabuild.funding:FundingLedger._output_precommit_authority': 'prismabuild.pool:PoolQueue._output_precommit_authority', 'prismabuild.funding:FundingLedger.stage_output': 'prismabuild.pool:PoolQueue.stage_output_intent', 'prismabuild.funding:FundingLedger.fund_output': 'prismabuild.pool:PoolQueue.fund_output_batch', 'prismabuild.funding:FundingLedger._fund_output_batch_locked': 'prismabuild.pool:PoolQueue._fund_output_batch_locked', 'prismabuild.funding:FundingLedger.output_funded_cover': 'prismabuild.pool:PoolQueue.output_funded_cover', 'prismabuild.funding:FundingLedger.drive_output': 'prismabuild.pool:PoolQueue.drive_output_funding', 'prismabuild.funding:FundingLedger.release_output': 'prismabuild.pool:PoolQueue.release_output_funding', 'prismabuild.funding:FundingLedger._release_never_started_funding_locked': 'prismabuild.pool:PoolQueue._release_never_started_funding_locked', 'prismabuild.funding:FundingLedger._output_funding_nonexecution_locked': 'prismabuild.pool:PoolQueue._output_funding_nonexecution_locked', 'prismabuild.funding:FundingLedger.output_census': 'prismabuild.pool:PoolQueue.output_census_for_owner', 'prismabuild.funding:FundingLedger._output_spoken_token_names': 'prismabuild.pool:PoolQueue._output_spoken_token_names', 'prismabuild.funding:FundingLedger._output_outstanding_window_tokens': 'prismabuild.pool:PoolQueue._output_outstanding_window_tokens', 'prismabuild.funding:FundingLedger.output_keep_names_for_owner': 'prismabuild.pool:PoolQueue.output_keep_names_for_owner', 'prismabuild.funding:FundingLedger.pending_window': 'prismabuild.pool:PoolQueue._window_fence_state', 'prismabuild.funding:FundingLedger._chunk_leg_mover': 'prismabuild.prelaunch_group:_leg_mover', 'prismabuild.funding:FundingLedger._chunk_leg_size': 'prismabuild.prelaunch_group:_leg_size', 'prismabuild.funding:FundingLedger._chunk_funding_matches': 'prismabuild.prelaunch_group:_funding_matches', 'prismabuild.funding:FundingLedger._chunk_record_tokens': 'prismabuild.prelaunch_group:_record_tokens', 'prismabuild.funding:FundingLedger._chunk_birth_record': 'prismabuild.prelaunch_group:_birth_record', 'prismabuild.funding:FundingLedger._chunk_row_is_ready': 'prismabuild.prelaunch_group:_row_is_ready', 'prismabuild.funding:FundingLedger._chunk_rotate_record': 'prismabuild.prelaunch_group:_rotate_record', 'prismabuild.funding:FundingLedger._chunk_advance_record': 'prismabuild.prelaunch_group:_advance_record', 'prismabuild.funding:FundingLedger._chunk_fresh_binding': 'prismabuild.prelaunch_group:_fresh_binding', 'prismabuild.funding:FundingLedger._chunk_rebind_retained': 'prismabuild.prelaunch_group:_rebind_retained', 'prismabuild.funding:FundingLedger.bind_chunk': 'prismabuild.prelaunch_group:publish_chunk', 'prismabuild.funding:FundingLedger._bind_chunk_locked': 'prismabuild.prelaunch_group:_publish_chunk', 'prismabuild.funding:FundingLedger._chunk_republished': 'prismabuild.prelaunch_group:_republished'}
