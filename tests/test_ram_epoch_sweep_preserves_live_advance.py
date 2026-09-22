"""Epoch cleanup must not turn a live advance promise into free capacity."""
import pytest
import test_the_nonfinal_window_fences_its_advance as base


def blind_window(tmp_path):
    (tmp_path / 'ram').mkdir()
    queue = base._queue(tmp_path, stage_gib=3, ram_gib=3)
    epoch = base.storage_tiers.ensure_ram_epoch(tmp_path / 'ram', host='dl380g10')
    queue.announce_tier({
        'schema': base.storage_tiers.TIER_RECORD_SCHEMA_V1, 'tier': 'ram',
        'tier_id': base.RAM_TIER, 'host': 'dl380g10',
        'mountpoint': str(tmp_path / 'ram'), 'epoch': epoch['epoch'],
        'capacity_bytes': 3 * base.GIB})
    plan = base._plan(queue, 3, ram=True)
    for phase in plan['phases']:
        phase['ram_mover_row']['resources']['cpu'] = 1
    base._publish_consumer(queue, plan, base.CONSUMER)
    land_source(queue, plan, 0)
    base.tier_loop.ram_residency_window(queue, tiers=base._ram_tiers(tmp_path))
    grant = base.window_credit.grant_key(base.CONSUMER, base.RAM_TIER,
                                         'ram_mover_row', 'phase-1')
    return queue, plan, grant


def land_source(queue, plan, ordinal):
    mover = base._movers(plan)[ordinal]
    assert queue.tier_ledger(base.TIER).acquire(mover, {'stage_gib': 1})
    base.residency_publication.vouch_landed(
        queue, consumer_action_key=base.CONSUMER, mover_action_key=mover,
        tier_id=base.TIER, stage_root='/stage/prewarm',
        manifest_sha256=str(plan['manifest_sha256']),
        range_start_bytes=ordinal * base.SPAN,
        range_end_bytes=(ordinal + 1) * base.SPAN)


def test_blind_advance_survives_epoch_sweep_binds_claims_and_retires(tmp_path):
    queue, plan, grant = blind_window(tmp_path)
    ledger = queue.tier_ledger(base.RAM_TIER)
    target = base._movers(plan, 'ram_mover_row')[1]
    assert ledger.holder_tokens(grant) == {'ram_gib': 1}
    assert not queue.item_path(base.pool.READY, target).exists()
    assert queue.read_funding(target, base.RAM_TIER) is None

    base.tier_loop.drop_prior_ram_epochs(queue, base._ram_tiers(tmp_path))
    assert ledger.holder_tokens(grant) == {'ram_gib': 1}, 'epoch sweep erased live blind advance'
    assert ledger.available()['ram_gib'] == 2

    land_source(queue, plan, 1)
    base.tier_loop.ram_residency_window(queue, tiers=base._ram_tiers(tmp_path))
    assert queue.item_path(base.pool.READY, target).exists()
    assert ledger.holder_tokens(grant) == {}
    assert ledger.holder_tokens(target) == {'ram_gib': 1}
    assert queue.read_funding(target, base.RAM_TIER)['state'] == 'transferring'
    base.tier_loop.drop_prior_ram_epochs(queue, base._ram_tiers(tmp_path))
    assert ledger.holder_tokens(target) == {'ram_gib': 1}, 'epoch sweep erased bound advance'
    row = base.pool._read_json(queue.item_path(base.pool.READY, target))
    claim = queue.claim(ready=[row], tags=['dl380g10'], owner='ram-advance-proof',
                        capacity={'cpu': 4, 'mem_gb': 8})
    assert claim and claim['action_key'] == target
    assert queue.read_funding(target, base.RAM_TIER)['state'] == 'consumed'
    assert ledger.holder_tokens(target) == {'ram_gib': 1}
    queue.finish(target, status='failed', detail={'returncode': 1})
    assert ledger.holder_tokens(target) == {}


@pytest.mark.parametrize('ending', ['terminal', 'vanished', 'plan-vanished'])
def test_known_dead_or_missing_plan_reclaims_blind_grant(tmp_path, ending):
    queue, plan, grant = blind_window(tmp_path)
    if ending == 'terminal':
        queue.withdraw(base.CONSUMER, reason='finished private fixture')
    elif ending == 'vanished':
        queue.item_path(base.pool.READY, base.CONSUMER).unlink()
    else:
        queue.residency_plan_path(base.CONSUMER).unlink()
    base.tier_loop.drop_prior_ram_epochs(queue, base._ram_tiers(tmp_path))
    assert queue.tier_ledger(base.RAM_TIER).holder_tokens(grant) == {}


@pytest.mark.parametrize('corruption', ['plan', 'funding'])
def test_unknown_credit_evidence_retains_and_reports(tmp_path, corruption):
    queue, plan, grant = blind_window(tmp_path)
    if corruption == 'plan':
        path = queue.residency_plan_path(base.CONSUMER)
        path.unlink()  # The immutable private fixture file cannot be overwritten.
        path.write_text('{bad')
    else:
        target = base._movers(plan, 'ram_mover_row')[1]
        path = queue.funding_path(target, base.RAM_TIER)
        path.parent.mkdir(exist_ok=True)
        path.write_text('{bad')
    events = base.tier_loop.drop_prior_ram_epochs(queue, base._ram_tiers(tmp_path))
    assert queue.tier_ledger(base.RAM_TIER).holder_tokens(grant) == {'ram_gib': 1}
    assert any(e['event'] == 'ram-credit-cleanup-deferred' for e in events)


def test_forged_phase_and_malformed_grants_are_not_exempt(tmp_path):
    queue, plan, grant = blind_window(tmp_path)
    ledger = queue.tier_ledger(base.RAM_TIER)
    forged = base.window_credit.grant_key(base.CONSUMER, base.RAM_TIER,
                                          'ram_mover_row', 'phase-2')
    for holder in (forged, 'advance-malformed'):
        assert ledger.acquire(holder, {'ram_gib': 1})
    base.tier_loop.drop_prior_ram_epochs(queue, base._ram_tiers(tmp_path))
    assert ledger.holder_tokens(grant) == {'ram_gib': 1}
    assert ledger.holder_tokens(forged) == {}
    assert ledger.holder_tokens('advance-malformed') == {}


def test_new_epoch_keeps_future_credit_but_shrink_cannot_overadmit(tmp_path):
    queue, plan, grant = blind_window(tmp_path)
    tiers = base._ram_tiers(tmp_path)
    tiers[base.RAM_TIER]['epoch'] = '1790000000-0123456789abcdef'
    queue.mint_tier_capacity(base.RAM_TIER, {'ram_gib': 1})
    ledger = queue.tier_ledger(base.RAM_TIER)
    base.tier_loop.drop_prior_ram_epochs(queue, tiers)
    assert ledger.holder_tokens(grant) == {'ram_gib': 1}
    assert ledger.available().get('ram_gib', 0) == 0
    assert not ledger.acquire('d' * 64, {'ram_gib': 1})
    # Credit never asserts RAM residency in the new epoch.
    assert queue.move_record(base._movers(plan, 'ram_mover_row')[1]) is None


def test_disappeared_ram_tier_releases_future_credit(tmp_path):
    queue, plan, grant = blind_window(tmp_path)
    base.tier_loop.drop_prior_ram_epochs(queue, {})
    assert queue.tier_ledger(base.RAM_TIER).holder_tokens(grant) == {}


@pytest.mark.parametrize("held_lock", ["consumer", "mover", "mint"])
def test_transition_contention_retains_credit(tmp_path, held_lock):
    import threading
    queue, plan, grant = blind_window(tmp_path)
    entered, release = threading.Event(), threading.Event()
    def holds():
        lock = (queue.tier_mint_lock(base.RAM_TIER) if held_lock == 'mint' else
                queue.mover_transition_lock(base.CONSUMER if held_lock == 'consumer'
                    else base._movers(plan, 'ram_mover_row')[1]))
        with lock:
            entered.set()
            release.wait(5)
    thread = threading.Thread(target=holds)
    thread.start()
    try:
        assert entered.wait(5)
        events = base.tier_loop.drop_prior_ram_epochs(queue, base._ram_tiers(tmp_path))
        assert queue.tier_ledger(base.RAM_TIER).holder_tokens(grant) == {'ram_gib': 1}
        expected = {'consumer': 'consumer transition busy',
                    'mover': 'mover transition busy', 'mint': 'tier mint busy'}[held_lock]
        assert any(e.get('reason') == expected for e in events)
    finally:
        release.set()
        thread.join(5)
        assert not thread.is_alive()


def test_funding_rotation_during_discovery_is_not_permission_to_release(tmp_path, monkeypatch):
    queue, plan, grant = blind_window(tmp_path)
    land_source(queue, plan, 1)
    base.tier_loop.ram_residency_window(queue, tiers=base._ram_tiers(tmp_path))
    target = base._movers(plan, 'ram_mover_row')[1]
    original = queue.read_funding_evidence
    calls = 0
    def rotates(mover, tier):
        nonlocal calls
        status, record, reason = original(mover, tier)
        if mover == target:
            calls += 1
            if calls == 1:
                record = dict(record, generation='0' * 32)
        return status, record, reason
    monkeypatch.setattr(queue, 'read_funding_evidence', rotates)
    events = base.tier_loop.drop_prior_ram_epochs(queue, base._ram_tiers(tmp_path))
    assert queue.tier_ledger(base.RAM_TIER).holder_tokens(target) == {'ram_gib': 1}
    assert any(e.get('reason') == 'funding rotated during qualification' for e in events)


def test_unreadable_claimed_progress_cannot_rewind_credit_frontier(tmp_path, monkeypatch):
    queue, plan, grant = blind_window(tmp_path)
    base._claim_with_progress(queue, base.CONSUMER, phase='phase-0')
    monkeypatch.setattr(base.tier_loop.prewarm_loop, 'progress_phase', lambda *args: None)
    events = base.tier_loop.drop_prior_ram_epochs(queue, base._ram_tiers(tmp_path))
    assert queue.tier_ledger(base.RAM_TIER).holder_tokens(grant) == {'ram_gib': 1}
    assert any(e.get('reason') == 'claimed progress is not positively available' for e in events)


def test_reclaim_completes_while_another_tier_mint_is_held(tmp_path):
    """The RAM reclaim must not wait on a second tier while holding its own mint.

    The ledger's mutation guard is that tier's mint lock, so an all-tier
    release inside the guarded section blocks on another tier's mint while
    holding this one -- an order nothing else in the tree takes.  The queue
    sorts ``prismabuild-stage:`` before ``ram:``, so under that shape the
    sweep parks on the stage mint before it ever frees the RAM grant.
    """

    import threading
    import time

    queue, plan, grant = blind_window(tmp_path)
    queue.withdraw(base.CONSUMER, reason='finished private fixture')
    ledger = queue.tier_ledger(base.RAM_TIER)
    assert ledger.holder_tokens(grant) == {'ram_gib': 1}

    holding, let_go = threading.Event(), threading.Event()
    failures: list[BaseException] = []

    def hold_other_tier_mint():
        with queue.tier_mint_lock(base.TIER):
            holding.set()
            let_go.wait(20)

    def run_sweep():
        try:
            base.tier_loop.drop_prior_ram_epochs(queue, base._ram_tiers(tmp_path))
        except BaseException as exc:      # surfaced by the assertions below
            failures.append(exc)

    other = threading.Thread(target=hold_other_tier_mint)
    sweep = threading.Thread(target=run_sweep)
    other.start()
    try:
        assert holding.wait(5), 'fixture never took the other tier mint'
        sweep.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if ledger.holder_tokens(grant) == {} or failures:
                break
            time.sleep(0.05)
        assert not failures, failures[0]
        assert ledger.holder_tokens(grant) == {}, (
            'RAM reclaim did not complete while another tier mint was held: '
            'the sweep is waiting on that tier while holding this one')
        # The guarded section is over, so this tier's mint is free again.
        with queue.tier_mint_lock(base.RAM_TIER, blocking=False) as acquired:
            assert acquired, 'sweep still holds the RAM mint after releasing it'
    finally:
        let_go.set()
        other.join(10)
        if sweep.is_alive() or sweep.ident is not None:
            sweep.join(10)
        assert not other.is_alive()
    assert not sweep.is_alive(), 'sweep never finished'
    assert not failures, failures[0]
