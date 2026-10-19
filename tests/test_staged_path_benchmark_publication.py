"""Small real benchmark setups must date every published vouch first."""
from pathlib import Path

import pytest

import bench_stage_egress
import bench_tier_cycle
from prismabuild import pool, reader_lease, residency_map


class BenchmarkPairPublished(BaseException):
    pass


@pytest.mark.parametrize('setup', ['cycle', 'shared-cycle', 'dead-owners', 'egress'])
def test_a_benchmark_never_publishes_an_undated_fragment(tmp_path, monkeypatch, setup):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    stage = tmp_path / 'stage'
    stage.mkdir()
    # Exercise the real setup functions with a small byte geometry. Their
    # reservation and actual pair writers still run unchanged.
    monkeypatch.setattr(bench_tier_cycle, 'GIB', 64)
    queue.tier(bench_tier_cycle.TIER).mint({'stage_gib': 64})
    write_fragment = residency_map.write_fragment

    def observe_publication(root, fragment):
        material = reader_lease.read_material(root, str(fragment['consumer_action_key']),
                                              str(fragment['mover_action_key']))
        assert isinstance(material, dict), 'fragment publication preceded its material'
        assert set(fragment['entries']) <= set(material['entries'])
        for key, mention in fragment['entries'].items():
            assert reader_lease.file_id_matches(material['entries'][key]['file_id'],
                reader_lease.stat_identity(str(mention['stage_path'])))
        write_fragment(root, fragment)
        raise BenchmarkPairPublished('after real fragment publication')

    monkeypatch.setattr(residency_map, 'write_fragment', observe_publication)
    with pytest.raises(BenchmarkPairPublished):
        if setup == 'cycle':
            bench_tier_cycle._land(queue, stage, consumer='1' * 64, mover='d' * 64,
                                   ordinal=0, files=1)
        elif setup == 'shared-cycle':
            bench_tier_cycle._land_shared(queue, stage, vouch_as='1' * 64,
                mover='d' * 64, ordinal=0, files=1, phase_gib=1)
        elif setup == 'dead-owners':
            bench_tier_cycle.build_dead_owners(queue, stage, pairs=1, entries=1, charged=False)
        else:
            bench_stage_egress.land_range(queue, stage, 1)
