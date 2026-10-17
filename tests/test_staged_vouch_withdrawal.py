"""A completed reissue retires every part of its donor's vouch."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parents[1] / "src"),
                str(Path(__file__).resolve().parents[1] / "tools/fleet")]

from prismabuild import pool, reader_lease, residency_map, stage_release
import tier_loop
import test_a_resident_range_is_adopted_rather_than_recopied as ranges


def test_adoption_clears_the_donors_retiring_generation(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    queue.tier(ranges.TIER).mint({"stage_gib": ranges.STAGE_GIB})
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=ranges.TIER, stage_root=stage)
    donor, successor = "d" * 64, "e" * 64
    paths = ranges._stage_range(queue, mover=donor, consumer=ranges.FIRST, stage=stage)
    identities = [reader_lease.stat_identity(str(path)) for path in paths]
    root = queue.records.residency_fragment_root()
    material = reader_lease.read_material(root, ranges.FIRST, donor)
    mark = reader_lease.write_retiring(
        reader_lease.leases_root(queue, root), consumer_action_key=ranges.FIRST,
        mover_action_key=donor, generation=material["generation"])

    result = tier_loop.adopt(
        queue, old_key=donor, new_key=successor, consumer_action_key=ranges.SECOND,
        tier_id=ranges.TIER, phase="phase-0", range_start_bytes=0,
        range_end_bytes=ranges.PHASE_GIB * ranges.GIB, residency_root=root)

    assert result["adopted"] is True, result
    assert identities == [reader_lease.stat_identity(str(path)) for path in paths]
    assert queue.tier(ranges.TIER).holdings(donor) == {}
    assert queue.tier(ranges.TIER).holdings(successor) == {"stage_gib": ranges.PHASE_GIB}
    assert residency_map.fragment_path(root, ranges.SECOND, successor).is_file()
    assert not residency_map.fragment_path(root, ranges.FIRST, donor).exists()
    assert reader_lease.read_material(root, ranges.FIRST, donor) is None
    assert not mark.exists(), "the withdrawn donor still closes a material generation"
