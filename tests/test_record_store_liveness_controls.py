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
from test_one_publisher_parses_unchanged_publication_metadata_once import (
    forest, _sweep as publication_sweep, CONSUMER as PUBLICATION_CONSUMER,
    MOVER as PUBLICATION_MOVER,
)
from prismabuild import residency_map, kept_reads
from prismabuild import tier_holdings


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
    ledger = queue.tier(TIER)
    before = ledger.holdings(mover)
    assert before == {'stage_gib': 2}
    funding_before = queue.read_funding(mover, TIER)
    assert funding_before['state'] == 'transferring'
    seen = _unreadable_stat(
        monkeypatch,
        queue.records.item_path(pool.record_store.CLAIMED, mover),
        error,
    )
    events = pg.release_unit(queue, TIER, unit, holder, [mover], terminal=True)
    assert seen, 'the underlying native metadata failure was never exercised'
    assert ledger.holdings(mover) == before, 'unknown claimed evidence released the actual chunk tokens'
    assert queue.read_funding(mover, TIER) == funding_before
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
    target = queue.records.lease_path(
        key,
    ) if state == 'lease' else queue.records.item_path(
        state,
        key,
    )
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
    queue.tier('prismabuild-stage:dl380g10').release_unspent(key, authority=tier_holdings.Unspent.NEVER_STARTED)
    assert po.unheld_window_gib(queue, 'prismabuild-stage:dl380g10')['gib'] == 2
    seen = _unreadable_stat(
        monkeypatch,
        queue.records.item_path(pool.record_store.CLAIMED, key),
        error,
    )
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
    incomplete._receipt(
        queue,
        incomplete.MOVER,
        staged_gib=0,
        complete=False,
        errors=['interrupted'],
    )
    target = queue.records.item_path(pool.record_store.CLAIMED, incomplete.MOVER)
    target.write_text('{}')
    before = queue.tier(incomplete.RAM_TIER).holdings(incomplete.MOVER)
    seen = _unreadable_stat(monkeypatch, target, error)
    events = tier_loop.release_incomplete_ram_promotions(queue, tiers=incomplete._tiers(ram))
    assert seen
    assert queue.tier(incomplete.RAM_TIER).holdings(incomplete.MOVER) == before
    assert any(row['event'] == 'ram-mover-incomplete-retained' for row in events)


@pytest.mark.parametrize('error', [errno.EIO, errno.ESTALE])
def test_uncertain_claim_keeps_prior_epoch_tokens(tmp_path, monkeypatch, error):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    epochs._ram_resident(queue, tmp_path, epoch=epochs.OLD_EPOCH)
    target = queue.records.item_path(pool.record_store.CLAIMED, epochs.RAM_MOVER)
    target.write_text('{}')
    before = queue.tier(epochs.RAM_TIER).holdings(epochs.RAM_MOVER)
    seen = _unreadable_stat(monkeypatch, target, error)
    events = tier_loop.drop_prior_ram_epochs(
        queue,
        {epochs.RAM_TIER: epochs._ram_record(tmp_path, epoch=epochs.NEW_EPOCH)},
    )
    assert seen
    assert queue.tier(epochs.RAM_TIER).holdings(epochs.RAM_MOVER) == before
    assert any('deferred' in str(row['event']) for row in events)


@pytest.mark.parametrize('generation', [10**400, float('nan'), float('inf'), -float('inf'), True])
def test_unorderable_generation_keeps_real_origin_and_foreign_prewrite(
    tmp_path,
    monkeypatch,
    generation,
):
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


@pytest.mark.parametrize('error', [errno.EACCES, errno.EIO, errno.ESTALE])
def test_funding_missing_metadata_never_certifies_an_empty_intent_as_absent(
    tmp_path,
    monkeypatch,
    error,
):
    queue = _queue(tmp_path, stage_gib=8)
    mover = _hexkey('unreadable-funding-census')
    path = queue.records.funding_path(mover, TIER)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'')
    seen = _unreadable_stat(monkeypatch, path, error)
    status, record, reason = queue.read_funding_evidence(mover, TIER)
    assert seen, 'the actual metadata query was not reached'
    assert status == 'unknown', 'unavailable metadata certified a present empty funding intent as absent'
    assert record is None
    assert 'census unreadable' in reason


def test_unreadable_publication_metadata_is_not_reused_when_coarse_ctime_repeats(
    forest,
    monkeypatch,
):
    fragment = residency_map.fragment_path(
        forest['residency_root'], PUBLICATION_CONSUMER, PUBLICATION_MOVER)
    assert all(row[1] == 'proof' for row in publication_sweep(forest))
    before = fragment.stat()
    fragment.chmod(0)
    try:
        with pytest.raises(PermissionError):
            fragment.read_bytes()
        current = fragment.stat()
        class CoarsePermissionStat:
            st_ctime_ns = before.st_ctime_ns
            st_ctime = before.st_ctime
            def __getattr__(self, name):
                return getattr(current, name)
        observed = CoarsePermissionStat()
        facts = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        assert tuple(getattr(observed, field) for field in facts) == tuple(
            getattr(before, field) for field in facts)
        assert observed.st_mode & 0o777 == 0
        real = os.stat
        seen = []
        def stat(path, *args, **kwargs):
            if (isinstance(path, (str, bytes, os.PathLike))
                    and Path(os.fsdecode(path)) == fragment):
                seen.append(str(path))
                return observed
            return real(path, *args, **kwargs)
        monkeypatch.setattr(os, 'stat', stat)
        rows = publication_sweep(forest)
        assert seen
        assert rows[0][1] == 'unknown', 'an unreadable file still authorized the real publication proof'
    finally:
        fragment.chmod(before.st_mode)


@pytest.mark.parametrize('error', [errno.EACCES, errno.EIO, errno.ESTALE])
def test_claim_owner_census_never_certifies_idle_from_unknown_claim_metadata(
    tmp_path,
    monkeypatch,
    error,
):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    key = _hexkey('uncertain-claim-owner')
    path = queue.records.item_path(pool.record_store.CLAIMED, key)
    path.write_text('{}')
    seen = _unreadable_stat(monkeypatch, path, error)
    refused = None
    pids = None
    try:
        pids = queue.claim_holder_pids(host='private-c05-host')
    except (OSError, pool.PoolContractError) as exc:
        refused = exc
    assert seen, 'the native claim metadata query was not reached'
    assert pids is None, 'an extant claim with unknown ownership was reported as idle'
    assert refused is not None


@pytest.mark.parametrize('error', [errno.EACCES, errno.EIO, errno.ESTALE])
def test_finish_never_rewrites_an_ending_when_its_presence_is_unknown(tmp_path, monkeypatch, error):
    queue = orphan._queue(tmp_path)
    key = orphan.KEY
    queue.publish(action_key=key, cas_root='/cas', checkout_root='/checkout',
                  worker_script='/worker.py', resources={'cpu': 1, 'mem_gb': 3})
    queue.claim(capacity={'cpu': 1, 'mem_gb': 3})
    queue.records.item_path(pool.record_store.CLAIMED, key).unlink()
    path = queue.records.item_path(pool.record_store.DONE, key)
    path.write_bytes(b'')
    tokens = queue.ledger().holder_tokens(key)
    assert tokens == {'cpu': 1, 'mem_gb': 3}
    seen = _unreadable_stat(monkeypatch, path, error)
    refused = None
    try:
        queue.finish(key, status='executed')
    except (OSError, pool.PoolContractError) as exc:
        refused = exc
    assert seen
    assert path.read_bytes(
    ) == b'', 'unknown presence authorized replacement of the real prior ending'
    assert refused is not None
    # The missing-claim branch releases capacity before reading the ending.
    assert queue.ledger().holder_tokens(key) == {}
