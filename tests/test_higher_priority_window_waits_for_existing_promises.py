"""New low-priority windows cannot indefinitely refill ahead of a larger lead."""
import test_window_multiconsumer_liveness as base


def publish_consumer(queue, key, *, gib, priority):
    plan = base._plan(queue, key, phases=1, gib_per_phase=gib, seed='priority')
    plan['phases'][0]['mover_row']['priority'] = priority
    plan['phases'][0]['mover_row']['resources']['cpu'] = 1
    base.residency_plan.freeze(queue, plan)
    queue.publish(action_key=key, cas_root=queue.root / 'cas', checkout_root=queue.root / 'co',
                  worker_script=queue.root / 'worker.py', priority=priority,
                  resources={'cpu': 1, 'mem_gb': 1},
                  residency={'schema': base.pool.RESIDENCY_SCHEMA_V1, 'tier_id': base.TIER,
                             'manifest_sha256': base.MANIFEST, 'manifest_bytes': 1 << 30,
                             'leads': base.residency_plan.leads_for(plan)})
    return plan['phases'][0]['mover_row']['action_key']


def cycle(queue, tmp_path):
    return base.tier_loop.residency_window(queue, tiers={base.TIER: {
        'tier_id': base.TIER, 'tier': 'stage', 'mountpoint': str(tmp_path / 'stage')}})


def test_higher_priority_lead_claims_after_old_promises_drain_without_refill(tmp_path):
    queue = base._queue(tmp_path, capacity_gib=60)
    ledger = queue.tier_ledger(base.TIER)
    assert ledger.acquire('e' * 64, {'stage_gib': 15})
    low = publish_consumer(queue, '1' * 64, gib=3, priority=-10)
    high = publish_consumer(queue, 'f' * 64, gib=18, priority=-5)
    old = []
    for index in range(13):
        key = base._hexkey(f'old-{index}')
        row = {**base._row(key, {base.STAGE_KIND: 3, 'cpu': 1, 'mem_gb': 1}, queue),
               'priority': -10,
               'residency': {'schema': base.pool.RESIDENCY_SCHEMA_V1, 'tier_id': base.TIER,
                             'manifest_sha256': base.MANIFEST, 'manifest_bytes': 1 << 30,
                             'range_start_bytes': 0, 'range_end_bytes': 3 * base.GIB}}
        queue.publish(**row)
        old.append(key)
    assert ledger.available()['stage_gib'] == 45
    cycle(queue, tmp_path)
    assert not queue.item_path(base.pool.READY, high).exists()
    assert not queue.item_path(base.pool.READY, low).exists(), 'new low-priority credit refilled ahead of waiting high priority'
    assert all(queue.item_path(base.pool.READY, key).exists() for key in old)
    for key in old:
        row = base.pool._read_json(queue.item_path(base.pool.READY, key))
        claimed = queue.claim(ready=[row], tags=['dl380g10'], owner='drain-existing',
                              capacity={'cpu': 4, 'mem_gb': 4})
        assert claimed and claimed['action_key'] == key
        queue.finish(key, status='executed', detail={'returncode': 0})
        cycle(queue, tmp_path)
        if queue.item_path(base.pool.READY, high).exists():
            break
        assert not queue.item_path(base.pool.READY, low).exists()
    assert queue.item_path(base.pool.READY, high).exists()
    row = base.pool._read_json(queue.item_path(base.pool.READY, high))
    claimed = queue.claim(ready=[row], tags=['dl380g10'], owner='native-lead',
                          capacity={'cpu': 4, 'mem_gb': 4})
    assert claimed and claimed['action_key'] == high
    assert ledger.holder_tokens(high)['stage_gib'] == 18
    assert all(not queue.item_path(base.pool.WITHDRAWN, key).exists() for key in old)


def test_permanently_oversized_higher_priority_does_not_block_small_window(tmp_path):
    queue = base._queue(tmp_path, capacity_gib=10)
    low = publish_consumer(queue, '1' * 64, gib=3, priority=-10)
    high = publish_consumer(queue, 'f' * 64, gib=18, priority=-5)
    cycle(queue, tmp_path)
    assert queue.item_path(base.pool.READY, low).exists()
    assert not queue.item_path(base.pool.READY, high).exists()


def test_equal_priority_retains_existing_backfill(tmp_path):
    queue = base._queue(tmp_path, capacity_gib=20)
    assert queue.tier_ledger(base.TIER).acquire('e' * 64, {'stage_gib': 5})
    large = publish_consumer(queue, '1' * 64, gib=18, priority=-5)
    small = publish_consumer(queue, 'f' * 64, gib=3, priority=-5)
    cycle(queue, tmp_path)
    assert not queue.item_path(base.pool.READY, large).exists()
    assert queue.item_path(base.pool.READY, small).exists()
