"""Historical outcomes cannot multiply live-queue scans in the tier cycle (#870)."""
from pathlib import Path

import test_a_failed_consumer_stops_staging as base
from test_a_failed_consumer_stops_staging import queue


def failed_plan(queue):
    plan = base._plan(queue, base.FIRST, label='discovery')
    base._publish_consumer(queue, base.FIRST, plan)
    row = plan['phases'][0]['mover_row']
    queue.publish(**row, recompute=True)
    base._fail_consumer(queue, base.FIRST)
    return row['action_key']


def test_28000_unrelated_terminals_skip_reads_and_real_plan_still_withdraws(queue, monkeypatch):
    mover = failed_plan(queue)
    history = [queue.dir(base.pool.DONE) / (f'{index:064x}.json') for index in range(28000)]
    unrelated = {path.stem for path in history}
    scan, read, live = base.pool._scan, base.pool._read_json, base.residency_plan.live_state
    calls = []

    def listed(path):
        return history if Path(path) == queue.dir(base.pool.DONE) else scan(path)

    def read_only_candidates(path, *args, **kwargs):
        assert Path(path).stem not in unrelated, 'irrelevant historical terminal body was read'
        return read(path, *args, **kwargs)

    def live_only_candidates(q, key):
        calls.append(key)
        assert key not in unrelated, 'irrelevant history rescanned the live queue'
        return live(q, key)

    monkeypatch.setattr(base.pool, '_scan', listed)
    monkeypatch.setattr(base.pool, '_read_json', read_only_candidates)
    monkeypatch.setattr(base.residency_plan, 'live_state', live_only_candidates)
    events = base.tier_loop.withdraw_dead_consumer_movers(queue)
    assert any(event.get('mover') == mover and event.get('withdrawn') for event in events)
    assert queue.item_path(base.pool.WITHDRAWN, mover).exists()
    assert base.FIRST in calls


def test_unreadable_plan_discovery_defers_instead_of_sweeping_history(queue, monkeypatch):
    mover = failed_plan(queue)
    scan = base.pool._scan

    def unreadable(path):
        if Path(path) == queue.root / base.pool.RESIDENCY_PLANS:
            raise OSError('unknown plan registry')
        return scan(path)

    monkeypatch.setattr(base.pool, '_scan', unreadable)
    assert base.tier_loop.withdraw_dead_consumer_movers(queue) == []
    assert queue.item_path(base.pool.READY, mover).exists()


def test_plan_missing_from_discovery_is_deferred_until_the_next_pass(queue, monkeypatch):
    mover = failed_plan(queue)
    scan = base.pool._scan
    missed_once = False

    def first_observation(path):
        nonlocal missed_once
        if Path(path) == queue.root / base.pool.RESIDENCY_PLANS and not missed_once:
            missed_once = True
            return []  # A filing not visible in this discovery has no authority.
        return scan(path)

    monkeypatch.setattr(base.pool, '_scan', first_observation)
    assert base.tier_loop.withdraw_dead_consumer_movers(queue) == []
    assert queue.item_path(base.pool.READY, mover).exists()
    events = base.tier_loop.withdraw_dead_consumer_movers(queue)
    assert any(event.get('mover') == mover and event.get('withdrawn') for event in events)
