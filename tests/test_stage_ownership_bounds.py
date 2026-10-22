"""Supplemental report boundary checks; no live stage inventory."""
from contextlib import contextmanager
import json
import os
import math
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools' / 'fleet'))
from prismabuild import core as pb, data_manifest, pool, residency_map, staged_path  # noqa: E402
from prismabuild import residency_plan, stage_release  # noqa: E402
import stage_reclaim  # noqa: E402
from tools.fleet.stage_ownership import _IO  # noqa: E402


def setup(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    stage = tmp_path / 'stage'
    stage.mkdir()
    staged_path.register_stage_root(queue.root / 'residency',
                                    stage_root=str(stage), tier_id='test-tier')
    return queue, stage


def fragment(queue, stage, path):
    consumer, mover = 'a' * 64, 'b' * 64
    document = dict(schema=residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        consumer_action_key=consumer, mover_action_key=mover,
        tier_id='test-tier', stage_root=str(stage), manifest_sha256='c' * 64,
        entries={residency_map.residency_map_key(str(path), 0): dict(stage_path=str(path), bytes=path.stat().st_size,
                              sha256='d' * 64, offset=0)})
    root = queue.root / 'residency'
    residency_map.write_fragment(root, document)
    return root / consumer / f'{mover}.json'


def test_root_file_retains_fragment_ownership(tmp_path):
    queue, stage = setup(tmp_path)
    path = stage / 'data'
    path.write_bytes(b'abc')
    ref = fragment(queue, stage, path)
    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)
    assert got['complete']
    row = next(row for row in got['directories'] if row['name'] == '.')
    assert row['physical_bytes']['owned'] == 3
    assert row['declarations']['fragment_bytes'] == 3
    assert row['fragments'][0]['record_path'] == str(ref)


@pytest.mark.parametrize('seconds', [math.inf, -math.inf, math.nan, -1])
def test_time_budget_must_be_finite(tmp_path, seconds):
    queue, stage = setup(tmp_path)
    with pytest.raises(ValueError):
        stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage,
                                       max_seconds=seconds)


def test_reference_budget_keeps_prior_readable_record(tmp_path):
    queue, stage = setup(tmp_path)
    path = stage / 'data'
    path.write_bytes(b'abc')
    ref = fragment(queue, stage, path)
    class BudgetIO(_IO):
        def check(self):
            super().check()
            if self.documents:
                raise ValueError('forced stop after first record')
    io = BudgetIO(1 << 20, 30)
    census = stage_reclaim._reference_census(queue, stage=stage,
        tier_id='test-tier', cas_root=None, residency_root=None, io=io)
    assert any(record['record_path'] == str(ref) for record in census.records)
    assert io.errors


def test_real_script_report_entrypoint(tmp_path):
    queue, stage = setup(tmp_path)
    script = Path(__file__).resolve().parents[1] / 'tools/fleet/stage_reclaim.py'
    run = subprocess.run([sys.executable, str(script), '--report',
        '--pool-root', str(queue.root), '--tier-id', 'test-tier',
        '--stage-root', str(stage)], capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout)['deletion_authorized'] is False


def test_unknown_holder_scope_is_not_unattributed(tmp_path):
    queue, stage = setup(tmp_path)
    directory = stage / 'payload'
    directory.mkdir()
    (directory / 'data').write_bytes(b'abc')
    ledger = queue.tier('test-tier')
    ledger.mint({'stage_gib': 1})
    assert ledger.take('f' * 64, {'stage_gib': 1})
    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)
    assert not got['complete']
    assert got['totals']['physical_bytes']['unknown'] == 3


def manifest_move(queue, destination):
    mount = queue.root.parent / 'originals'
    source = mount / 'dataset/data'
    document = dict(schema=data_manifest.DATA_MANIFEST_SCHEMA_V1,
        produced_by={}, annotations={}, mount_prefix=str(mount), entry_count=1,
        total_bytes=11, entries=[dict(path=str(source), offset=0, bytes=11, sha256=None)])
    raw = json.dumps(document, sort_keys=True).encode()
    digest = pb.new_sha256(raw).hexdigest()
    blob = pb.PrismaBuildCAS(queue.root.parent / 'cas').blob_path(digest)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(raw)
    record = queue.records.move_path('b' * 64)
    movement = dict(schema=pool.POOL_MOVE_SCHEMA_V1,
        action_key='b' * 64, consumer_action_key='a' * 64,
        tier_id='test-tier', manifest_sha256=digest,
        stage_root=str(destination), range_start_bytes=0, range_end_bytes=11,
        range_bytes=11, bytes_staged=11, entries_declared=1, entries_staged=1,
        complete=True)
    record.parent.mkdir(parents=True, exist_ok=True)
    record.write_text(json.dumps(movement))
    relative = staged_path.stage_relative(str(source), 0, 11, mount_prefix=str(mount))
    return record, relative


def test_outside_mover_manifest_does_not_own_selected_stage_payload(tmp_path):
    queue, stage = setup(tmp_path)
    outside = tmp_path / 'other-stage'
    outside.mkdir()
    movement, relative = manifest_move(queue, outside)
    target = stage / relative
    target.parent.mkdir(parents=True)
    target.write_bytes(b'x' * 11)
    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)
    assert got['complete']
    assert got['totals']['physical_bytes'] == dict(owned=0, source_only=0,
                                                   unattributed=11, unknown=0)
    assert not any(row['movements'] for row in got['directories'])
    reference = next(ref for ref in got['references']
                     if ref['record_path'] == str(movement) and ref.get('manifest_sha256'))
    assert str(target) not in reference['paths']
    assert str(outside / relative) in reference['paths']
    assert any(entry.get('record_path') == str(movement)
               and entry['path'] == str(outside / relative) for entry in got['excluded'])
    # Strict reclaim retains its conservative, selected-stage protection set.
    strict = stage_reclaim._reference_census(queue, stage=stage, tier_id='test-tier',
        cas_root=None, residency_root=None)
    assert str(target) in strict.protected_paths


def test_nested_mover_destination_is_not_rebased_to_selected_root(tmp_path):
    queue, stage = setup(tmp_path)
    destination = stage / 'nested'
    movement, relative = manifest_move(queue, destination)
    target = destination / relative
    target.parent.mkdir(parents=True)
    target.write_bytes(b'x' * 11)
    decoy = stage / relative
    decoy.parent.mkdir(parents=True)
    decoy.write_bytes(b'x' * 11)
    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)
    assert got['complete']
    rows = {row['name']: row for row in got['directories']}
    assert rows['nested']['physical_bytes']['owned'] == 11
    assert rows['dataset']['physical_bytes']['unattributed'] == 11
    assert not rows['dataset']['movements']
    reference = next(ref for ref in got['references']
                     if ref['record_path'] == str(movement) and ref.get('manifest_sha256'))
    assert reference['paths'] == [str(target)]


@pytest.mark.parametrize('destination', [None, 'relative/stage'])
def test_manifest_without_valid_destination_keeps_attribution_unknown(tmp_path, destination):
    queue, stage = setup(tmp_path)
    record, relative = manifest_move(queue, stage)
    movement = json.loads(record.read_text())
    movement.pop('stage_root')
    movement['source_stage_root'] = str(stage)
    if destination is not None:
        movement['stage_root'] = destination
    record.write_text(json.dumps(movement))
    target = stage / relative
    target.parent.mkdir(parents=True)
    target.write_bytes(b'x' * 11)
    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)
    assert not got['complete']
    assert got['totals']['physical_bytes'] == dict(owned=0, source_only=0,
                                                   unattributed=0, unknown=11)
    assert any('destination' in refusal['reason'] for refusal in got['unresolved_manifests'])


def test_top_level_fifo_is_explicitly_excluded(tmp_path):
    import os
    queue, stage = setup(tmp_path)
    fifo = stage / 'fifo'
    os.mkfifo(fifo)
    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)
    assert got['complete']
    assert got['scan']['files_observed'] == 0
    assert got['totals']['observed']['apparent_bytes'] == 0
    assert any(entry['path'] == str(fifo) and entry['reason'] for entry in got['excluded'])


def test_symlinked_stage_ancestor_is_incomplete(tmp_path):
    queue, stage = setup(tmp_path)
    directory = stage / 'payload'
    directory.mkdir()
    (directory / 'data').write_bytes(b'abc')
    alias = tmp_path / 'alias'
    alias.symlink_to(tmp_path, target_is_directory=True)
    got = stage_reclaim.ownership_report(queue, tier_id='test-tier',
                                        stage_root=alias / stage.name)
    assert not got['complete']
    assert got['scan']['files_observed'] == 0
    assert 'symlink' in str(got['errors'])


def test_missing_configured_residency_root_is_unknown_not_empty(tmp_path):
    queue, stage = setup(tmp_path)
    (stage / 'data').write_bytes(b'abc')
    missing = tmp_path / 'missing-residency'

    got = stage_reclaim.ownership_report(queue, tier_id='test-tier',
        stage_root=stage, residency_root=missing)

    assert not got['complete']
    assert any(error['record_path'] == str(missing) for error in got['errors'])
    assert got['totals']['observed']['apparent_bytes'] == 3
    assert got['totals']['physical_bytes'] == dict(owned=0, source_only=0,
                                                   unattributed=0, unknown=3)


@pytest.mark.parametrize('entries,indices', [([], [0]), (None, [1])],
                         ids=['empty-entries', 'absent-index'])
def test_malformed_manifest_retains_observations_and_diagnostic(tmp_path, entries, indices):
    queue, stage = setup(tmp_path)
    record, relative = manifest_move(queue, stage)
    target = stage / relative
    target.parent.mkdir(parents=True)
    target.write_bytes(b'abc')
    movement = json.loads(record.read_text())
    cas = pb.PrismaBuildCAS(queue.root.parent / 'cas')
    document = json.loads(cas.blob_path(movement['manifest_sha256']).read_bytes())
    document['schema'] = data_manifest.DATA_MANIFEST_SCHEMA_V2
    if entries is not None:
        document.update(entries=entries, entry_count=0, total_bytes=0)
    document['read_plan'] = dict(read_bytes=0, phases=[dict(
        name='phase-0', entry_indices=indices, bytes=0, cumulative_bytes=0)])
    raw = json.dumps(document, sort_keys=True).encode()
    digest = pb.new_sha256(raw).hexdigest()
    blob = cas.blob_path(digest)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(raw)
    movement['manifest_sha256'] = digest
    record.write_text(json.dumps(movement))

    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)

    assert not got['complete']
    assert got['totals']['observed']['apparent_bytes'] == 3
    assert got['totals']['physical_bytes']['unknown'] == 3
    refusal = next(item for item in got['unresolved_manifests']
                   if item['record_path'] == str(record))
    assert refusal['manifest_sha256'] == digest
    assert 'entries' in refusal['reason'] or 'entry' in refusal['reason']


def test_malformed_ready_leads_keeps_partial_report_and_live_owner(tmp_path):
    queue, stage = setup(tmp_path)
    target = stage / 'owned'
    target.write_bytes(b'abc')
    ref = fragment(queue, stage, target)
    (stage / 'uncertain').write_bytes(b'defg')
    ready = queue.records.dir(pool.record_store.READY) / f"{'a' * 64}.json"
    ready.write_text(json.dumps(dict(schema='fixture-ready', action_key='a' * 64,
                                    residency=dict(leads=1))))

    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)

    assert not got['complete']
    assert got['totals']['observed']['apparent_bytes'] == 7
    assert got['totals']['physical_bytes'] == dict(owned=3, source_only=0,
                                                   unattributed=0, unknown=4)
    errors = [error for error in got['errors'] if error['record_path'] == str(ready)]
    assert any('residency.leads' in error['reason'] for error in errors)
    assert any('residency.leads' not in error['reason'] for error in errors)
    known = next(item for item in got['references']
                 if item['record_path'] == str(ref) and item.get('kind') == 'fragment')
    assert known['live_consumer_observed'] is True
    assert known['potentially_stale'] is False
    assert got['read_only'] and not got['deletion_authorized']


@pytest.mark.parametrize('leads', [1, 0, False, 'mover', {}, [1, '']])
def test_evidence_join_rejects_malformed_leads_without_losing_positives(tmp_path, leads):
    queue, _stage = setup(tmp_path)
    good_key, bad_key, mover = 'a' * 64, 'b' * 64, 'c' * 64
    root = queue.records.dir(pool.record_store.READY)
    good = dict(record_path=str(root / f'{good_key}.json'), document=dict(
        action_key=good_key, residency=dict(leads=[mover])))
    bad = dict(record_path=str(root / f'{bad_key}.json'), document=dict(
        action_key=bad_key, residency=dict(leads=leads)))
    errors = []

    wanted, owners = stage_release.live_claims(queue,
        evidence=iter([bad, good]), evidence_errors=errors)

    assert wanted == {mover}
    assert owners == {good_key: good_key, bad_key: bad_key}
    assert errors and all(error['record_path'] == bad['record_path'] for error in errors)
    assert all('residency.leads' in error['reason'] for error in errors)


@pytest.mark.parametrize('residency', [{}, dict(leads=None), dict(leads=[]),
                                    dict(leads=['b' * 64])])
def test_evidence_join_accepts_valid_or_absent_leads(tmp_path, residency):
    queue, _stage = setup(tmp_path)
    key = 'a' * 64
    record = dict(record_path=str(queue.records.dir(pool.record_store.READY) / f'{key}.json'),
                  document=dict(action_key=key, residency=residency))
    errors = []

    wanted, owners = stage_release.live_claims(queue,
        evidence=iter([record]), evidence_errors=errors)

    assert wanted == set(residency.get('leads') or [])
    assert owners == {key: key}
    assert not errors


def test_missing_optional_metadata_namespaces_remain_clean(tmp_path):
    queue, stage = setup(tmp_path)
    (stage / 'data').write_bytes(b'abc')
    optional = (queue.root / pool.record_store.MOVERS,
                queue.root / pool.record_store.MOVERS_RETIRED,
                residency_plan.registry_root(queue), queue.tier('test-tier').paths.held)
    for path in optional:
        if path.exists():
            path.rmdir()

    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)

    assert got['complete'] and not got['errors']
    assert got['totals']['physical_bytes']['unattributed'] == 3


@pytest.mark.parametrize('namespace', ['plans', 'held'])
def test_optional_namespace_disappearance_after_open_is_not_empty(tmp_path, monkeypatch, namespace):
    queue, stage = setup(tmp_path)
    (stage / 'data').write_bytes(b'abc')
    missing = (residency_plan.registry_root(queue) if namespace == 'plans'
               else queue.tier('test-tier').paths.held)
    missing.mkdir(parents=True, exist_ok=True)
    real_scandir = os.scandir

    def vanished():
        raise FileNotFoundError('namespace disappeared during iteration')
        yield  # make this an iterator that fails on next(), not on open

    @contextmanager
    def scandir(path):
        if isinstance(path, (str, os.PathLike)) and Path(path) == missing:
            yield vanished()
        else:
            with real_scandir(path) as entries:
                yield entries

    with monkeypatch.context() as patch:
        patch.setattr(os, 'scandir', scandir)
        got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)

    assert not got['complete']
    assert any(error['record_path'] == str(missing) for error in got['errors'])
    assert got['totals']['physical_bytes']['unknown'] == 3
