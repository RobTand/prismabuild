"""Supplemental report boundary checks; no live stage inventory."""
import json
import math
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools' / 'fleet'))
from prismabuild import core as pb, data_manifest, pool, residency_map, staged_path  # noqa: E402
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
