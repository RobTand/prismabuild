"""Bounded advisory publication discovery for metadata-filtered MCP reads."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from prismabuild.record_store import RecordStore


def record(key: int, *, published: float = 100.0, checkout: str = '/task',
           parent: str = 'parent', commit: str = 'commit', host: str = 'publisher'):
    return {'action_key': f'{key:064x}', 'published_unix': published,
            'checkout_root': checkout,
            'checkout_snapshot': {'parent': parent, 'commit': commit},
            'published_by': host}


def file_record(store, item, state='done', *, index=True):
    path = store.item_path(state, item['action_key'])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(item))
    if index:
        assert store.index_publication(item)
    return path


@pytest.fixture(autouse=True)
def settled_local_clock(monkeypatch):
    # The admitted fixture's clock advances independently of filesystem ticks.
    try:
        from prismabuild import publication_index
    except ImportError:
        return
    monkeypatch.setattr(publication_index, '_clock_fence', lambda: 10**30)


def reconcile(store):
    run = store.begin_publication_reconciliation()
    for _ in range(100):
        progress = run.step(max_entries=2)
        if progress['finished']:
            return progress
    pytest.fail('reconciliation did not finish bounded fixture')


@pytest.mark.parametrize(('field','value'), [
    ('checkout_root','/task'), ('snapshot_parent','parent'),
    ('snapshot_commit','commit'), ('published_by','publisher')])
def test_each_facet_addresses_only_its_own_candidates(tmp_path, monkeypatch, field, value):
    store = RecordStore(tmp_path)
    wanted = record(1)
    file_record(store, wanted)
    file_record(store, record(2, checkout='/other', parent='other', commit='other', host='other'))
    native = os.scandir

    def scan(path):
        assert Path(path) not in {store.dir(s) for s in ('ready','claimed','done','failed','withdrawn')}
        return native(path)

    monkeypatch.setattr(os, 'scandir', scan)
    result = store.publication_candidates(facets={field:value})
    assert [(c.action_key,c.published_unix) for c in result.candidates] == [(wanted['action_key'],100.0)]
    assert result.entries_consumed == 1
    assert not result.complete and 'coverage-missing' in result.reasons


def test_facets_and_publication_time_intersect_before_canonical_reads(tmp_path):
    store = RecordStore(tmp_path)
    for item in [record(1,published=100),record(2,published=300),record(3,published=300,host='other')]:
        path=file_record(store,item)
        os.utime(path,(9999,9999))
    result=store.publication_candidates(facets={'checkout_root':'/task','published_by':'publisher'},
                                         published_after=200,published_before=400)
    assert [c.action_key for c in result.candidates] == [f'{2:064x}']
    assert result.entries_consumed == 3


def test_publication_time_query_does_not_consume_old_days(tmp_path):
    store=RecordStore(tmp_path)
    file_record(store,record(1,published=1))
    file_record(store,record(2,published=86401))
    result=store.publication_candidates(facets={},published_after=86400,published_before=86402)
    assert [c.action_key for c in result.candidates] == [f'{2:064x}']
    assert result.entries_consumed == 1 and result.pages_consumed == 1


def test_entry_budget_is_checked_before_consuming_another_leaf(tmp_path):
    store=RecordStore(tmp_path)
    for key in range(1,5):file_record(store,record(key))
    result=store.publication_candidates(facets={'checkout_root':'/task'},max_entries=2)
    assert len(result.candidates) == result.entries_consumed == 2
    assert not result.complete and 'entry-budget-exhausted' in result.reasons


def test_day_page_budget_bounds_even_empty_time_ranges(tmp_path):
    store=RecordStore(tmp_path)
    file_record(store,record(1))
    result=store.publication_candidates(facets={},published_after=0,published_before=864000,max_pages=2)
    assert result.pages_consumed == 2
    assert not result.complete and 'page-budget-exhausted' in result.reasons


def test_reconciliation_indexes_legacy_rows_and_seals_local_coverage(tmp_path):
    store=RecordStore(tmp_path)
    file_record(store,record(1),index=False)
    file_record(store,record(2,checkout='/other'),index=False)
    progress=reconcile(store)
    assert progress['complete']
    result=store.publication_candidates(facets={'checkout_root':'/task'})
    assert result.complete and [c.action_key for c in result.candidates] == [f'{1:064x}']
    missing=store.publication_candidates(facets={'checkout_root':'/missing'})
    assert missing.complete and missing.candidates == ()


def test_legacy_publication_invalidates_coverage_instead_of_answering_complete_empty(tmp_path):
    store=RecordStore(tmp_path)
    file_record(store,record(1))
    reconcile(store)
    file_record(store,record(2,checkout='/legacy'),index=False)
    result=store.publication_candidates(facets={'checkout_root':'/legacy'})
    assert not result.complete and result.candidates == ()
    assert 'coverage-changed' in result.reasons


def test_crash_during_indexing_is_incomplete_and_reconciliation_repairs_it(tmp_path,monkeypatch):
    store=RecordStore(tmp_path)
    file_record(store,record(1));reconcile(store)
    original=store.index_publication
    monkeypatch.setattr(store,'index_publication',lambda _item:False)
    file_record(store,record(2,checkout='/interrupted'),index=False)
    assert not store.publication_candidates(facets={'checkout_root':'/interrupted'}).complete
    monkeypatch.setattr(store,'index_publication',original)
    reconcile(store)
    result=store.publication_candidates(facets={'checkout_root':'/interrupted'})
    assert result.complete and [c.action_key for c in result.candidates] == [f'{2:064x}']


def test_concurrent_publication_during_backfill_does_not_seal_coverage(tmp_path):
    store=RecordStore(tmp_path)
    file_record(store,record(1),index=False)
    run=store.begin_publication_reconciliation()
    run.step(max_entries=1)
    file_record(store,record(2),index=False)
    for _ in range(100):
        progress=run.step(max_entries=2)
        if progress['finished']:break
    assert progress['finished'] and not progress['complete']
    assert not store.publication_candidates(facets={'checkout_root':'/task'}).complete


def test_republish_retains_distinct_generation_candidates(tmp_path):
    store=RecordStore(tmp_path)
    file_record(store,record(1,published=100))
    file_record(store,record(1,published=200))
    result=store.publication_candidates(facets={'checkout_root':'/task'})
    assert sorted(c.published_unix for c in result.candidates) == [100,200]


def test_remote_filesystem_cannot_assert_legacy_publisher_coverage(tmp_path,monkeypatch):
    from prismabuild import publication_index
    store=RecordStore(tmp_path)
    file_record(store,record(1));reconcile(store)
    monkeypatch.setattr(publication_index.kept_reads,'_filesystem_type',lambda *_args,**_kwargs:'nfs')
    result=store.publication_candidates(facets={'checkout_root':'/task'})
    assert result.candidates and not result.complete
    assert 'coverage-untrusted-filesystem' in result.reasons
