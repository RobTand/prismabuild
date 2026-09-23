"""Retiring the original lead cannot turn an admitted consumer into a priority barrier."""
import test_higher_priority_window_waits_for_existing_promises as priority
import test_the_nonfinal_window_fences_its_advance as progress

base = priority.base


def advanced_consumer(queue, *, key, rank):
    plan = base._plan(queue, key, phases=3, gib_per_phase=22, seed='rolling')
    base.residency_plan.freeze(queue, plan)
    queue.publish(action_key=key, cas_root=queue.root / 'cas',
                  checkout_root=queue.root / 'co', worker_script=queue.root / 'worker.py',
                  resources={'cpu': 1, 'mem_gb': 1}, priority=rank,
                  residency={'schema': base.pool.RESIDENCY_SCHEMA_V1,
                             'tier_id': base.TIER, 'manifest_sha256': base.MANIFEST,
                             'manifest_bytes': 1 << 30,
                             'leads': base.residency_plan.leads_for(plan)})
    # Seed the already-admitted prior execution using the established private
    # claim+authenticated lease fixture. Its original head is no longer held
    # or queued; accepted progress is beyond it. No live fleet data is touched.
    progress._claim_with_progress(queue, key, phase='phase-1')
    head = plan['phases'][0]['mover_row']['action_key']
    assert not queue.item_path(base.pool.READY, head).exists()
    assert not queue.tier_ledger(base.TIER).holder_tokens(head)
    return plan


def test_claimed_advanced_high_priority_does_not_block_independent_small_lead(tmp_path):
    """The stalled claimed window sets no barrier for a lead that fits beside it.

    The 40 GiB holder is an orphan -- its receipt names a consumer that is
    gone -- so the admission commitment (#907) counts it as room eviction
    can make: the claimed window's 44 GiB footprint and the lead's 3 fit the
    60.  The joint-fit gate still counts it held, so the claimed window
    stalls on its 22 GiB current while the lead publishes into the 20 free.
    """
    queue = base._queue(tmp_path, capacity_gib=60)
    assert queue.tier_ledger(base.TIER).acquire('e' * 64, {'stage_gib': 40})
    queue.record_move('e' * 64, {'consumer_action_key': 'd' * 64})
    high = 'f' * 64
    plan = advanced_consumer(queue, key=high, rank=0)
    low = priority.publish_consumer(queue, '1' * 64, gib=3, priority=-10)
    events = priority.cycle(queue, tmp_path)
    assert any(e.get('consumer') == high and e.get('reason') == 'joint-fit-stall'
               for e in events), events
    assert queue.item_path(base.pool.READY, low).exists(), (
        'CLAIMED rolling window with retired original lead established a new-admission barrier', events)
    row = base.pool._read_json(queue.item_path(base.pool.READY, low))
    claimed = queue.claim(ready=[row], tags=['dl380g10'], owner='small-qualifier',
                          capacity={'cpu': 4, 'mem_gb': 4})
    assert claimed and claimed['action_key'] == low
    assert queue.tier_ledger(base.TIER).holder_tokens(low) == {'stage_gib': 3}
    # Existing larger-window credit gate was not bypassed to make this pass.
    assert all(not queue.item_path(base.pool.READY, phase['mover_row']['action_key']).exists()
               for phase in plan['phases'])


def test_a_lead_waits_for_the_footprint_of_a_window_a_static_holder_starves(tmp_path):
    """The same claimed window beside a holder nothing can evict (#907).

    The 40 GiB holder has no receipt, so no eviction returns it, and the
    claimed window's 44 GiB footprint already exceeds the 20 GiB left: the
    tier is over-committed, and every newcomer waits until it is not,
    whatever its priority.  The lead is refused on the commitment -- not on
    a priority barrier, which the claimed window still does not set.
    """
    queue = base._queue(tmp_path, capacity_gib=60)
    assert queue.tier_ledger(base.TIER).acquire('e' * 64, {'stage_gib': 40})
    high = 'f' * 64
    advanced_consumer(queue, key=high, rank=0)
    low_key = '1' * 64
    low = priority.publish_consumer(queue, low_key, gib=3, priority=-10)
    events = priority.cycle(queue, tmp_path)
    assert not queue.item_path(base.pool.READY, low).exists()
    reasons = {e.get('reason') for e in events
               if e.get('event') == 'window-gated' and e.get('consumer') == low_key}
    assert reasons == {'joint-commitment-stall'}, events


def test_claimed_window_cannot_be_deferred_by_ready_newcomer_priority_barrier(tmp_path):
    queue = base._queue(tmp_path, capacity_gib=60)
    assert queue.tier_ledger(base.TIER).acquire('e' * 64, {'stage_gib': 45})
    claimed_key = 'f' * 64
    advanced_consumer(queue, key=claimed_key, rank=-10)
    priority.publish_consumer(queue, '1' * 64, gib=18, priority=-5)
    events = priority.cycle(queue, tmp_path)
    decisions = [e for e in events if e.get('consumer') == claimed_key and e.get('reason')]
    assert any(e['reason'] == 'joint-fit-stall' for e in decisions), decisions
    assert not any(e['reason'] == 'higher-priority-window-waiting' for e in decisions), decisions
