"""Real ownership controls for C05 L; run only through published PrismaBuild."""
import errno
import json
import os
from pathlib import Path
import pytest
from prismabuild import pool, prelaunch_group as pg, produced_output as po
from test_prelaunch_group_reconcile_1594 import (
    TIER, _queue, _hexkey, _plan, _group, _drive_to_committed, _publish, _leg,
)
import test_pool_orphan_reservation as orphan
from test_output_window_obligation import owner
import test_a_hold_by_an_unknown_attempt_is_named as holds
import test_incomplete_ram_promotions_release_their_tokens as incomplete
import test_the_tier_loop_drops_a_prior_epoch_and_holds as epochs
import tier_loop


def _unreadable_stat(monkeypatch, target, error):
    real = os.stat
    seen = []
    def stat(path, *args, **kwargs):
        if (isinstance(path, (str, bytes, os.PathLike))
                and Path(os.fsdecode(path)) == target):
            seen.append(Path(os.fsdecode(path)))
            raise OSError(error, 'C05 real underlying metadata failure', str(path))
        return real(path, *args, **kwargs)
    monkeypatch.setattr(os, 'stat', stat)
    return seen


@pytest.mark.parametrize('error', [errno.EIO, errno.ESTALE])
def test_prelaunch_unknown_claim_keeps_exact_reserved_chunk(tmp_path, monkeypatch, error):
    queue = _queue(tmp_path, stage_gib=8)
    consumer, mover = _hexkey('unknown-consumer'), _hexkey('unknown-mover')
    plan = _plan(queue, consumer, mover, _hexkey('other'))
    unit, holder = _group(queue, plan, [mover], demand=4)
    _drive_to_committed(queue, TIER, unit, holder, 4, [mover])
    row = _publish(queue, plan, mover)
    assert pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, mover),
                            float(row['published_unix'])).status == 'published'
    ledger = queue.tier_ledger(TIER)
    before = ledger.holder_tokens(mover)
    assert before == {'stage_gib': 2}
    seen = _unreadable_stat(monkeypatch, queue.records.item_path(pool.record_store.CLAIMED, mover), error)
    events = pg.release_unit(queue, TIER, unit, holder, [mover], terminal=True)
    assert seen, 'the underlying native metadata failure was never exercised'
    assert ledger.holder_tokens(mover) == before, 'unknown claimed evidence released the actual chunk tokens'
    assert queue.read_funding(mover, TIER)['state'] == 'reserved'
    assert 'prelaunch-unknown-evidence' in events


@pytest.mark.parametrize('state', ['ready', 'claimed', 'lease'])
@pytest.mark.parametrize('error', [errno.EIO, errno.ESTALE])
def test_uncertain_reclaim_keeps_actual_reservation(tmp_path, monkeypatch, state, error):
    queue = orphan._queue(tmp_path)
    key = orphan.KEY
    queue.publish(action_key=key, cas_root='/cas', checkout_root='/checkout',
                  worker_script='/worker.py', resources={'cpu': 1, 'mem_gb': 3})
    claim = queue.claim(capacity={'cpu': 1, 'mem_gb': 3})
    terminal = dict(claim, status='executed', finished_host=claim['claimed_host'])
    queue.records.item_path(pool.record_store.DONE, key).write_text(json.dumps(terminal))
    if state != 'claimed':
        queue.records.item_path(pool.record_store.CLAIMED, key).unlink()
    if state != 'lease':
        queue.records.lease_path(key).unlink()
    if state == 'ready':
        queue.records.item_path(pool.record_store.READY, key).write_text(json.dumps(claim))
    target = queue.records.lease_path(key) if state == 'lease' else queue.records.item_path(state, key)
    ledger = queue.ledger()
    before = ledger.holder_tokens(key)
    assert before == {'cpu': 1, 'mem_gb': 3}
    seen = _unreadable_stat(monkeypatch, target, error)
    refused = None
    try:
        queue.reclaim_terminal_reservation(key)
    except pool.PoolContractError as exc:
        refused = exc
    assert seen
    assert ledger.holder_tokens(key) == before, 'unknown queue evidence released a real reservation'
    assert refused is not None and 'reclaim' in str(refused)


@pytest.mark.parametrize('error', [errno.EIO, errno.ESTALE])
def test_unreadable_claim_keeps_the_actual_output_window_obligation(owner, monkeypatch, error):
    queue, key, _bound, _template = owner
    queue.tier_ledger('prismabuild-stage:dl380g10').release(key)
    assert po.unheld_window_gib(queue, 'prismabuild-stage:dl380g10')['gib'] == 2
    seen = _unreadable_stat(monkeypatch, queue.records.item_path(pool.record_store.CLAIMED, key), error)
    owed = po.unheld_window_gib(queue, 'prismabuild-stage:dl380g10')
    assert seen
    assert owed['gib'] is None, 'an unreadable owner erased its real refill obligation'
    assert [row['owner'] for row in owed['unknown']] == [key]


@pytest.mark.parametrize('error', [errno.EIO, errno.ESTALE])
def test_uncertain_claim_keeps_incomplete_ram_promotion(tmp_path, monkeypatch, error):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    ram = incomplete._ram_root(tmp_path, queue)
    incomplete._hold(queue, incomplete.MOVER, 2)
    incomplete._receipt(queue, incomplete.MOVER, staged_gib=0, complete=False, errors=['interrupted'])
    target = queue.records.item_path(pool.record_store.CLAIMED, incomplete.MOVER)
    target.write_text('{}')
    before = queue.tier_ledger(incomplete.RAM_TIER).holder_tokens(incomplete.MOVER)
    seen = _unreadable_stat(monkeypatch, target, error)
    events = tier_loop.release_incomplete_ram_promotions(queue, tiers=incomplete._tiers(ram))
    assert seen
    assert queue.tier_ledger(incomplete.RAM_TIER).holder_tokens(incomplete.MOVER) == before
    assert any(row['event'] == 'ram-mover-incomplete-retained' for row in events)


@pytest.mark.parametrize('error', [errno.EIO, errno.ESTALE])
def test_uncertain_claim_keeps_prior_epoch_tokens(tmp_path, monkeypatch, error):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    epochs._ram_resident(queue, tmp_path, epoch=epochs.OLD_EPOCH)
    target = queue.records.item_path(pool.record_store.CLAIMED, epochs.RAM_MOVER)
    target.write_text('{}')
    before = queue.tier_ledger(epochs.RAM_TIER).holder_tokens(epochs.RAM_MOVER)
    seen = _unreadable_stat(monkeypatch, target, error)
    events = tier_loop.drop_prior_ram_epochs(queue, {epochs.RAM_TIER: epochs._ram_record(tmp_path, epoch=epochs.NEW_EPOCH)})
    assert seen
    assert queue.tier_ledger(epochs.RAM_TIER).holder_tokens(epochs.RAM_MOVER) == before
    assert any('deferred' in str(row['event']) for row in events)


@pytest.mark.parametrize('generation', [10**400, float('nan'), float('inf'), -float('inf'), True])
def test_unorderable_generation_keeps_real_origin_and_foreign_prewrite(tmp_path, monkeypatch, generation):
    for name in ('PRISMABUILD_ACTION_NONCE', 'PRISMABUILD_ACTION_SCOPE',
                 'PRISMABUILD_READER_HELPER_ROOT', 'PRISMABUILD_ACTION_KEY'):
        monkeypatch.delenv(name, raising=False)
    po._UNFILED_REPORTS.clear()
    po._KEPT_PREWRITES.clear()
    template, queue, instance, path = holds._dead_consumed_batch(tmp_path)
    holder = holds._foreign_holder(tmp_path, queue, template, path)
    key = holder['owner_action_key']
    claim_path = queue.records.item_path(pool.record_store.CLAIMED, key)
    claim = json.loads(claim_path.read_bytes())
    claim_path.unlink()
    queue.records.item_path(pool.record_store.FAILED, key).write_text(json.dumps(
        dict(claim, published_unix=generation, status='failed')))
    events = po.origin_retirement_tick(queue)
    assert path.exists(), 'an unorderable terminal generation permitted real origin deletion'
    assert not holds._entry(queue, instance).get('retiring')
    assert not holds._entry(queue, instance).get('origin_reclaimed')
    assert any(event.get('event') == holds.HELD for event in events)
