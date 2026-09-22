"""Independent cleanup-authority checks for #853."""
import json
import os

from test_stale_material_done_owner_retires import (
    fleet, stale_owner, NAMES, TIER, _sweep, _stage, NEW_PAYLOAD,
    residency_map, reader_lease,
)
import stage_release
import tier_loop
from test_stale_material_done_owner_retires import base, SIZE


def test_equal_entry_counts_do_not_prove_missing_range_bytes(fleet):
    queue, stage, _ = fleet
    consumer, mover = stale_owner(fleet, coherent_names=set(NAMES), replace=False)
    receipt = queue.move_record(mover)
    receipt.update(range_bytes=4 * SIZE, range_end_bytes=4 * SIZE,
                   bytes_staged=4 * SIZE)
    queue.record_move(mover, receipt)
    held = queue.tier_ledger(TIER).holder_tokens(mover)
    successor, copier = base._key(), base._key()
    result = tier_loop.adopt(
        queue, old_key=mover, new_key=copier, consumer_action_key=successor,
        tier_id=TIER, phase='head', range_start_bytes=0,
        range_end_bytes=4 * SIZE, residency_root=queue.residency_fragment_root())
    assert not result['adopted'], (
        'The complete receipt claims 16 KiB but the two current fragment '
        f'entries cover only 8 KiB: {result}')
    assert queue.tier_ledger(TIER).holder_tokens(mover) == held
    assert not residency_map.fragment_path(
        queue.residency_fragment_root(), successor, copier).exists()


def test_matching_nonram_epochs_are_still_invalid_for_cleanup(fleet):
    queue, stage, _ = fleet
    consumer, mover = stale_owner(fleet)
    root = queue.residency_fragment_root()
    fragment_path = residency_map.fragment_path(root, consumer, mover)
    material_path = reader_lease.material_path(root, consumer, mover)
    for path in (fragment_path, material_path):
        doc = json.loads(path.read_text())
        doc['epoch'] = 'invalid-ssd-epoch'
        path.write_text(json.dumps(doc))
    charge = queue.tier_ledger(TIER).holder_tokens(mover)
    before = {name: (stage / name).read_bytes() for name in NAMES}
    result = _sweep(queue, stage)
    assert all((stage / name).exists() for name in NAMES), result
    assert {name: (stage / name).read_bytes() for name in NAMES} == before
    assert queue.tier_ledger(TIER).holder_tokens(mover) == charge
    assert fragment_path.exists() and material_path.exists()


def test_replacement_before_checkpoint_install_does_not_become_clean(fleet, monkeypatch):
    queue, stage, _ = fleet
    consumer, mover = stale_owner(fleet, coherent_names=set(NAMES), replace=False)
    original = stage_release._install_skip_checkpoint
    raced = []

    def before_install(*args, **kwargs):
        if not raced:
            raced.append(True)
            replacement = _stage(stage, NAMES[1] + '.later', NEW_PAYLOAD)
            os.replace(replacement, stage / NAMES[1])
        return original(*args, **kwargs)

    monkeypatch.setattr(stage_release, '_install_skip_checkpoint', before_install)
    stage_release.sweep_dead_owner_fragments(queue, stage_roots={TIER: str(stage)})
    assert raced
    result = stage_release.sweep_dead_owner_fragments(queue, stage_roots={TIER: str(stage)})
    assert any(row.get('entries_pruned') == 1 for row in result), result
    assert not (stage / NAMES[1]).exists()
    assert (stage / NAMES[0]).exists()
