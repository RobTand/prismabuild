"""Positive capability authority inventory, including production tools."""
import ast
from collections import Counter
from pathlib import Path
import shutil
import pytest

# Existing business APIs are explicit exceptions; none may grow a new mint.
# The four identity-only signatures are separately pinned by the r7 guard.
PUBLIC_API = {'materialize': ('CheckoutCertifier.certify_checkout_cleanup',
                 'ReleaseCertificate.permits',
                 'ReleaseCertificate.require',
                 'ReleaseCertificate.returning'),
 'pool': ('ExecutionBudget.as_record',
          'ExecutionBudget.clamped',
          'PoolContentionProbe.as_record',
          'PoolContentionProbe.judge',
          'PoolContentionProbe.note_gate_holder',
          'PoolQueue.action_transition_lock',
          'PoolQueue.announce',
          'PoolQueue.announce_tier',
          'PoolQueue.archive_attempt',
          'PoolQueue.archived_generation_outcomes',
          'PoolQueue.archived_preemption_outcomes',
          'PoolQueue.attempt_ending',
          'PoolQueue.attempt_outcomes',
          'PoolQueue.begin_mover_retirement_batch',
          'PoolQueue.build_produced_output_batch_ref',
          'PoolQueue.certify_checkout_cleanup',
          'PoolQueue.claim',
          'PoolQueue.claim_holder_pids',
          'PoolQueue.claim_intent_age',
          'PoolQueue.claim_intent_host',
          'PoolQueue.claim_reservation_hosts',
          'PoolQueue.class_dependency_gap',
          'PoolQueue.cleanup_action_containers',
          'PoolQueue.clear_stage_ownership_holder',
          'PoolQueue.commit_move_retirements',
          'PoolQueue.consumer_events',
          'PoolQueue.container_marker',
          'PoolQueue.copy_filed_complete',
          'PoolQueue.current_ending',
          'PoolQueue.decode_publication_supersession',
          'PoolQueue.demand_of',
          'PoolQueue.denial_transitions',
          'PoolQueue.dependency_placement_verdict',
          'PoolQueue.dependent_rows',
          'PoolQueue.ending_diagnosis',
          'PoolQueue.execute',
          'PoolQueue.export_records',
          'PoolQueue.export_wait_verdict',
          'PoolQueue.fail_dead_input_dependency',
          'PoolQueue.fence',
          'PoolQueue.file_tier_commitment',
          'PoolQueue.find_key',
          'PoolQueue.finish',
          'PoolQueue.holder_bound',
          'PoolQueue.host_events',
          'PoolQueue.image_placement_census',
          'PoolQueue.interpreter_placement_verdict',
          'PoolQueue.latest_denials',
          'PoolQueue.launch_environment',
          'PoolQueue.lease_age',
          'PoolQueue.ledger',
          'PoolQueue.live_withdrawal',
          'PoolQueue.mark_residency_plan_superseded',
          'PoolQueue.move_record',
          'PoolQueue.move_records',
          'PoolQueue.mover_landing',
          'PoolQueue.mover_report',
          'PoolQueue.mover_retirement_lock',
          'PoolQueue.mover_transition_lock',
          'PoolQueue.movers_claimed_on_tier',
          'PoolQueue.note_fill_borrow_end',
          'PoolQueue.offer_clock_skews',
          'PoolQueue.offered_tags',
          'PoolQueue.offers',
          'PoolQueue.output_partial_pin_holds',
          'PoolQueue.ownership_start_gate',
          'PoolQueue.passes',
          'PoolQueue.pin_holds_tier_tokens',
          'PoolQueue.placeable',
          'PoolQueue.placeable_hosts',
          'PoolQueue.placement_census',
          'PoolQueue.placement_progress_contracts',
          'PoolQueue.placement_timeout_ceilings',
          'PoolQueue.plan_requeue',
          'PoolQueue.prepare_move_retirement',
          'PoolQueue.prewarm',
          'PoolQueue.prune_prewarm_receipt',
          'PoolQueue.publication_ending',
          'PoolQueue.publish',
          'PoolQueue.quarantine_orphans',
          'PoolQueue.queued_child_wait_evidence',
          'PoolQueue.queued_child_wait_verdict',
          'PoolQueue.read_mover_retirements',
          'PoolQueue.reader_plan_stand_aside',
          'PoolQueue.reader_plan_verdict',
          'PoolQueue.ready_items',
          'PoolQueue.reap_stale',
          'PoolQueue.reclaim_terminal_reservation',
          'PoolQueue.record_denial',
          'PoolQueue.record_export',
          'PoolQueue.record_move',
          'PoolQueue.record_pass',
          'PoolQueue.record_prewarm',
          'PoolQueue.recorded_stage_ownership',
          'PoolQueue.recover_archived_move',
          'PoolQueue.released_origin_consumer_keys',
          'PoolQueue.residency_map_environment',
          'PoolQueue.residency_pin_holds',
          'PoolQueue.residency_verdict',
          'PoolQueue.resident_copy_hosts',
          'PoolQueue.resolve_claim_holder',
          'PoolQueue.resolve_prewarm_reference',
          'PoolQueue.retire_move_receipt',
          'PoolQueue.retire_residency_plan',
          'PoolQueue.retry_own_pending_finishes',
          'PoolQueue.rows_host_memory_held',
          'PoolQueue.runtime_of',
          'PoolQueue.scope_for_record',
          'PoolQueue.selection_live',
          'PoolQueue.serve_once',
          'PoolQueue.stage_ownership_holder',
          'PoolQueue.stage_ownership_lock',
          'PoolQueue.staged_range_of',
          'PoolQueue.staged_wait_verdict',
          'PoolQueue.sweep_consumer_events',
          'PoolQueue.sweep_denial_transitions',
          'PoolQueue.sweep_finish_tombstones',
          'PoolQueue.sweep_gangs',
          'PoolQueue.sweep_orphan_passes',
          'PoolQueue.sweep_prewarm_receipts',
          'PoolQueue.sweep_ready_transitions',
          'PoolQueue.sweep_stale_acquisitions',
          'PoolQueue.sweep_widowed_leases',
          'PoolQueue.terminal_keys',
          'PoolQueue.terminal_outcome_covers',
          'PoolQueue.tier',
          'PoolQueue.tier_commitment',
          'PoolQueue.tiers',
          'PoolQueue.unlink_retired_move',
          'PoolQueue.validate_produced_output',
          'PoolQueue.validate_produced_output_batch',
          'PoolQueue.validate_residency',
          'PoolQueue.withdraw',
          'PoolQueue.withdrawal_covers',
          'PoolQueue.withdrawn_keys',
          'PoolQueue.withhold_age',
          'PoolQueue.write_lease',
          'PoolQueue.write_stage_ownership_holder',
          'ProgressPhase.as_record',
          'ProgressPhase.clamped',
          'ProgressPhase.effective_grace_s',
          'ProgressPolicy.as_record',
          'ProgressPolicy.clamped',
          'ProgressPolicy.index_of',
          'ProgressPolicy.no_progress_bound_s',
          'ProgressWatch.as_record',
          'ProgressWatch.check_stall',
          'ProgressWatch.delivered_units_per_s',
          'ProgressWatch.exempt_export_wait',
          'ProgressWatch.exempt_pool_contention',
          'ProgressWatch.exempt_queued_child_wait',
          'ProgressWatch.exempt_reader_plan',
          'ProgressWatch.exempt_staged_wait',
          'ProgressWatch.exempt_start_gate',
          'ProgressWatch.grace_s',
          'ProgressWatch.grant_start_gate_edge',
          'ProgressWatch.sample',
          'ProgressWatch.sample_queued_child_wait',
          'ProgressWatch.shift',
          'ProgressWatch.stall_deadline',
          'ProgressWatch.stall_record',
          'ResourceLedger.abandon_acquire',
          'ResourceLedger.acquire',
          'ResourceLedger.acquisition_receipt_path',
          'ResourceLedger.acquisitions_of',
          'ResourceLedger.available',
          'ResourceLedger.base',
          'ResourceLedger.begin_acquire',
          'ResourceLedger.bind_release_claim',
          'ResourceLedger.capacity',
          'ResourceLedger.capacity_census',
          'ResourceLedger.census_report_path',
          'ResourceLedger.certify_release',
          'ResourceLedger.commit_acquire',
          'ResourceLedger.configure_cpu_tiers',
          'ResourceLedger.cpu_allocation',
          'ResourceLedger.cpu_ids',
          'ResourceLedger.ensure_capacity',
          'ResourceLedger.free_cpu_allocation',
          'ResourceLedger.free_cpu_tokens',
          'ResourceLedger.free_dir',
          'ResourceLedger.free_preferred',
          'ResourceLedger.held',
          'ResourceLedger.held_dir',
          'ResourceLedger.held_keys',
          'ResourceLedger.holder_census',
          'ResourceLedger.holder_tokens',
          'ResourceLedger.holds_gpu',
          'ResourceLedger.legacy_owner_path',
          'ResourceLedger.minted_dir',
          'ResourceLedger.release',
          'ResourceLedger.release_count',
          'ResourceLedger.release_except',
          'ResourceLedger.release_kinds',
          'ResourceLedger.release_owner_path',
          'ResourceLedger.release_records',
          'ResourceLedger.report_census',
          'ResourceLedger.retire_free_capacity',
          'ResourceLedger.retire_held',
          'ResourceLedger.sweep_stale_acquisitions',
          'ResourceLedger.transfer',
          'ResourceLedger.transfer_count',
          'ResourceLedger.transfer_tokens',
          'ResourceLedger.unbind_unstarted_claim',
          'ResourceLedger.unparsed_acquisitions_of',
          '_ClaimDiagnostics.add_denial',
          '_ClaimReservation.abandon',
          '_ClaimReservation.commit_host',
          '_ClaimReservation.reset_tier_take',
          '_ClaimReservation.return_unlaunched_probe',
          '_ClaimScanReads.claimed_hint',
          '_ClaimScanReads.movement_authority',
          '_ClaimScanReads.renamed_into_claimed',
          '_ClaimScanReads.verdicts',
          '_DrainAccounting.for_key',
          '_DrainAccounting.stage',
          '_PendingDrainNotes.take_measurement',
          '_PendingDrainNotes.take_ordinary',
          '_TransitionHolds.add',
          '_TransitionHolds.summary',
          'action_process_groups',
          'describe_placement_census',
          'describe_resource_profile',
          'execution_budget',
          'find_launcher_pids',
          'held_names_visible',
          'is_box_local_path',
          'launcher_owns_action',
          'membership_handoff_authorized',
          'move_pricing_projection',
          'normalize_placement_tags',
          'normalize_priority_reason',
          'normalize_tested_repository',
          'offer_timing',
          'profile_from_launcher_stdout',
          'progress_policy',
          'read_export_wait',
          'read_queue_record',
          'read_staged_wait',
          'ready_placement',
          'resource_profile_summary',
          'staged_wait_declared',
          'staged_wait_since',
          'terminate_action',
          'tested_repository_of',
          'tier_ledger_names_from',
          'transition_locks_never_wait',
          'worker_argv'),
 'tier_holdings': ('TierLedger.abandon_take',
                   'TierLedger.begin_take',
                   'TierLedger.cancel_fence',
                   'TierLedger.census_report',
                   'TierLedger.certify_release',
                   'TierLedger.commit_take',
                   'TierLedger.declared',
                   'TierLedger.egress',
                   'TierLedger.egress_best_effort',
                   'TierLedger.entry_paths',
                   'TierLedger.fence_key',
                   'TierLedger.free_supply',
                   'TierLedger.grow_supply',
                   'TierLedger.handover',
                   'TierLedger.handover_count',
                   'TierLedger.handover_named',
                   'TierLedger.held_grants',
                   'TierLedger.holder_absent',
                   'TierLedger.holder_entries',
                   'TierLedger.holder_fresh_present',
                   'TierLedger.holder_has_entries',
                   'TierLedger.holder_names',
                   'TierLedger.holder_observations',
                   'TierLedger.holder_state',
                   'TierLedger.holders',
                   'TierLedger.holdings',
                   'TierLedger.mint',
                   'TierLedger.mint_census',
                   'TierLedger.mint_guarded',
                   'TierLedger.mint_history',
                   'TierLedger.mint_issues',
                   'TierLedger.mint_supply',
                   'TierLedger.mutation_lock',
                   'TierLedger.observation_fences',
                   'TierLedger.paths',
                   'TierLedger.pending_takes',
                   'TierLedger.reap_pending_takes',
                   'TierLedger.reclaim_idle_rates',
                   'TierLedger.reconcile_mirror',
                   'TierLedger.release_rates',
                   'TierLedger.release_unspent',
                   'TierLedger.release_unspent_best_effort',
                   'TierLedger.retire_duplicate',
                   'TierLedger.settle_egress',
                   'TierLedger.shortage',
                   'TierLedger.take',
                   'TierLedger.take_advance',
                   'TierLedger.token_names',
                   'TierLedger.token_paths',
                   'TierLedger.trim_supply',
                   'TierLedger.unparsed_takes',
                   'TierLedger.uses_tier_census',
                   'egress_all',
                   'for_action',
                   'ids',
                   'release_rates_all',
                   'release_unspent_all')}

FACTORIES = {
    ('pool', 'ResourceLedger.certify_release'): Counter({
        'materialize.CheckoutCertifier(self.release_records.root, ledger=self, recover=False)': 1}),
    ('pool', 'PoolQueue.certify_checkout_cleanup'): Counter({'materialize.CheckoutCertifier(self.root)': 1}),
    ('pool', 'PoolQueue._resume_heartbeat_recovery'): Counter({'materialize.CheckoutCertifier(self.root, recover=False)': 1}),
    ('pool', 'PoolQueue._release_reservation'): Counter({'materialize.CheckoutCertifier(self.root, recover=False)': 1}),
    ('pool', 'PoolQueue._complete_lifetime_evidence'): Counter({'materialize.CheckoutCertifier(self.root, recover=False)': 1}),
    ('pool', 'PoolQueue.execute'): Counter({'materialize.CheckoutCertifier(self.root)': 2}),
    ('pool', 'PoolQueue.execute.register_checkout'): Counter({'materialize.CheckoutCertifier(self.root)': 1}),
}


def assert_capability_allowlist(root):
    found_public = {name: [] for name in PUBLIC_API}
    found_factories = {}
    paths = [*root.joinpath('src/prismabuild').glob('*.py'), *root.joinpath('tools').rglob('*.py')]
    for path in paths:
        tree = ast.parse(path.read_text())
        def visit(node, context=(), in_function=False):
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                context += (node.name,)
            if (path.stem in found_public and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and not in_function and not node.name.startswith('_')):
                found_public[path.stem].append('.'.join(context))
            if isinstance(node, ast.Call) and (
                    getattr(node.func, 'attr', None) == 'CheckoutCertifier'
                    or isinstance(node.func, ast.Name) and node.func.id == 'CheckoutCertifier'):
                owner = (path.stem, '.'.join(context))
                assert owner in FACTORIES, ('unapproved authority factory', path, context)
                found_factories.setdefault(owner, Counter()).update([ast.unparse(node)])
            for child in ast.iter_child_nodes(node):
                visit(child, context, in_function or isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)))
        visit(tree)
    assert {name: tuple(sorted(names)) for name, names in found_public.items()} == PUBLIC_API
    assert found_factories == FACTORIES


def test_public_functions_and_authority_factories_are_positive_allowlisted():
    assert_capability_allowlist(Path(__file__).resolve().parents[1])


@pytest.mark.parametrize('body', [
    'def mint_for_queue(self, key, queue_root):\n        return materialize.CheckoutCertifier(queue_root, ledger=self, recover=False).certify_checkout_cleanup(key)',
    'def recover_and_mint(self, key):\n        return materialize.CheckoutCertifier(self.release_records.root, ledger=self, recover=True).certify_checkout_cleanup(key)',
])
def test_reviewers_new_routes_fail_the_guard(tmp_path, body):
    root = Path(__file__).resolve().parents[1]
    (tmp_path / 'src/prismabuild').mkdir(parents=True)
    for name in PUBLIC_API:
        text = (root / 'src/prismabuild' / (name + '.py')).read_text()
        if name == 'pool':
            index = text.index('    def __init__', text.index('class ResourceLedger:'))
            text = text[:index] + '    ' + body + '\n\n' + text[index:]
        (tmp_path / 'src/prismabuild' / (name + '.py')).write_text(text)
    with pytest.raises(AssertionError):
        assert_capability_allowlist(tmp_path)


@pytest.mark.parametrize('component', ['queue-parent', 'reservations', 'ledger-base', 'release-owners', 'manifest', 'custody-owner'])
def test_certificate_refuses_every_symlinked_component(tmp_path, component):
    import os
    from prismabuild import pool, materialize
    queue = pool.PoolQueue(tmp_path / 'queue')
    ledger = queue.ledger()
    ledger.ensure_capacity({'cpu': 2})
    assert ledger.acquire('holder', {'cpu': 1})
    certificate = ledger.certify_release('holder')
    paths = {'queue-parent': queue.root, 'reservations': ledger.root,
             'ledger-base': ledger.base, 'release-owners': ledger.release_owner_path('holder').parent,
             'manifest': ledger.release_owner_path('holder'), 'custody-owner': ledger.release_owner_path('holder')}
    path = paths[component]
    moved = path.with_name(path.name + '.real')
    path.rename(moved)
    path.symlink_to(moved, target_is_directory=moved.is_dir())
    try:
        with pytest.raises((OSError, ValueError, pool.PoolContractError)):
            ledger.release('holder', certificate=certificate)
        assert ledger.holder_tokens('holder') == {'cpu': 1}
        with pytest.raises(pool.PoolContractError):
            ledger.certify_release('holder')
    finally:
        path.unlink()
        moved.rename(path)
