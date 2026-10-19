"""Shared landed fixtures compose real files and the production pair writer."""
import hashlib
from pathlib import Path

import pytest

import residency_publication
from prismabuild import pool, reader_lease, residency_map, staged_path


def test_the_shared_landed_fixture_dates_actual_private_bytes(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    root = queue.records.residency_fragment_root()
    consumer, mover = '1' * 64, 'd' * 64
    writes = []
    write_fragment = residency_map.write_fragment
    def observe(root, fragment):
        material = reader_lease.read_material(root, consumer, mover)
        assert isinstance(material, dict)
        writes.append('fragment-after-material')
        return write_fragment(root, fragment)
    monkeypatch.setattr(residency_map, 'write_fragment', observe)
    residency_publication.publish_landed_files(queue, consumer_action_key=consumer,
        mover_action_key=mover, tier_id='prismabuild-stage:fixture-control',
        stage_root=tmp_path / 'stage', manifest_sha256='a' * 64,
        range_start_bytes=100, range_end_bytes=607, entries=2)
    fragment = residency_map.read_fragments(root, consumer)[0]
    material = reader_lease.read_material(root, consumer, mover)
    assert writes == ['fragment-after-material']
    assert sum(row['bytes'] for row in fragment['entries'].values()) == 507
    for key, row in fragment['entries'].items():
        path = Path(row['stage_path'])
        assert path.is_relative_to(tmp_path)
        assert row['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
        assert reader_lease.file_id_matches(material['entries'][key]['file_id'],
                                            reader_lease.stat_identity(str(path)))
    receipt = queue.move_record(mover, fresh=True)
    assert receipt['complete'] is True
    assert receipt['entries_staged'] == 2 and receipt['bytes_staged'] == 507


@pytest.mark.parametrize('missing', ['fragment', 'material'])
def test_strict_withdrawal_keeps_benchmark_missing_file_errors(tmp_path, missing):
    root = tmp_path / 'residency'
    consumer, mover = '1' * 64, 'd' * 64
    fragment = residency_map.fragment_path(root, consumer, mover)
    material = reader_lease.material_path(root, consumer, mover)
    for path in (fragment, material):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'private withdrawal boundary')
    (fragment if missing == 'fragment' else material).unlink()
    with pytest.raises(FileNotFoundError):
        staged_path.withdraw_vouch(None, root, consumer, mover, missing_ok=False)
    assert material.exists() is (missing == 'fragment')


def test_starved_fixture_dates_observations_after_landed_files(tmp_path, monkeypatch):
    """Publication cost precedes the fixture's observation clock, without sleeps."""
    import json
    from types import SimpleNamespace
    import pbmcp_fixture as fixture

    clock = [1700000000.0]
    monkeypatch.setattr(fixture, 'time', SimpleNamespace(time=lambda: clock[0]))
    publish = residency_publication.publish_landed_files
    publications = []
    def finish_publication(*args, **kwargs):
        result = publish(*args, **kwargs)
        publications.append(kwargs['tier_id'])
        clock[0] += 60.0
        return result
    monkeypatch.setattr(residency_publication, 'publish_landed_files', finish_publication)
    fleet = fixture.build_starved(tmp_path)
    lease = json.loads(fleet.queue.records.lease_path(fixture.CONSUMER_KEY).read_text())
    assert publications == [fixture.STAGE_TIER, fixture.RAM_TIER]
    assert fleet.consumer_reported_unix == clock[0] - 800
    assert lease['progress_observation']['last_accepted']['reported_unix'] == clock[0] - 800
    assert lease['claimed_unix'] == clock[0] - 100
    assert lease['heartbeat_unix'] == clock[0] - 1
