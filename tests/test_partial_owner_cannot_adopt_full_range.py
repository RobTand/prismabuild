"""A surviving cache subset is not the original complete movement range."""
from test_stale_material_done_owner_retires import (
    fleet, stale_owner, NAMES, SIZE, TIER, _entries, _write_sidecar, base,
    residency_map, reader_lease,
)
import json
import tier_loop


def test_partial_fragment_cannot_be_adopted_as_the_original_full_range(fleet):
    queue, stage, _ = fleet
    consumer, mover = stale_owner(fleet, coherent_names={NAMES[0]})
    root = queue.residency_fragment_root()
    fragment_path = residency_map.fragment_path(root, consumer, mover)
    fragment = json.loads(fragment_path.read_text())
    keep = next(key for key, value in fragment['entries'].items()
                if value['stage_path'] == str(stage / NAMES[0]))
    fragment['entries'] = {keep: fragment['entries'][keep]}
    residency_map.write_fragment(root, fragment)
    _write_sidecar(queue, stage, consumer, mover, {keep: _entries(stage)[keep]})
    old_tokens = queue.tier_ledger(TIER).holder_tokens(mover)
    old_bytes = (stage / NAMES[0]).read_bytes()
    successor, copier = base._key(), base._key()
    outcome = tier_loop.adopt(
        queue, old_key=mover, new_key=copier,
        consumer_action_key=successor, tier_id=TIER, phase='head',
        range_start_bytes=0, range_end_bytes=len(NAMES) * SIZE,
        residency_root=root)
    assert not outcome['adopted'], (
        'Only one of the two original entries remains, but adoption certified '
        f'the complete two-entry range: {outcome}')
    assert queue.tier_ledger(TIER).holder_tokens(mover) == old_tokens
    assert not residency_map.fragment_path(root, successor, copier).exists()
    assert not reader_lease.material_path(root, successor, copier).exists()
    assert (stage / NAMES[0]).read_bytes() == old_bytes
