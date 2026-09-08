"""An abandoned claim cannot return a later claimant's borrow."""
from pathlib import Path
import os
import pytest
from test_adaptive_cpu import rig
from prismabuild import pool


def test_exception_after_borrow_return_does_not_return_a_peers_borrow(rig, monkeypatch):
    queue, clock, state, donor, first, publish, claim, telemetry = rig
    original_rename = os.rename
    original_discard = queue._discard_claim_intent
    lost_key = f'{2:064x}'
    peers = []

    def lose(source, target):
        if Path(source) == queue.item_path(pool.READY, lost_key):
            Path(source).unlink()  # another claimant consumed the ready name
            raise FileNotFoundError(source)
        return original_rename(source, target)

    def fail_discard(key, *, owner):
        # The lost-rename branch already returned its tokens and borrow.
        # A sibling now claims another key against the same host sample.
        publish(3)
        second = claim()
        assert second and second['action_key'] == f'{3:064x}'
        peers.append(second)
        queue.finish(second['action_key'], status='executed', detail={})
        # Shared intent cleanup can fail after that successful borrow.
        raise OSError('injected intent cleanup failure')

    monkeypatch.setattr(pool.os, 'rename', lose)
    monkeypatch.setattr(queue, '_discard_claim_intent', fail_discard)
    with pytest.raises(OSError, match='injected intent cleanup failure'):
        claim()
    assert len(peers) == 1
    monkeypatch.setattr(pool.os, 'rename', original_rename)
    monkeypatch.setattr(queue, '_discard_claim_intent', original_discard)
    publish(4)
    assert claim() is None, 'abandoned claim returned a peer borrow and reused its host sample'


@pytest.mark.parametrize('write_then_raise', [False, True])
def test_repeated_return_cannot_clear_a_peer_with_the_same_sample(rig, monkeypatch, write_then_raise):
    from prismabuild import adaptive_cpu
    queue, clock, state, donor, first, publish, claim, telemetry = rig
    controller = adaptive_cpu.Controller(queue.ledger(), {'preferred': [0], 'fallback': [1]})
    old = {'borrowing': True, 'sampled_unix': clock[0]}
    previous = controller.admitted(old)
    # Retain a separate copy to check durable ownership, not just local state.
    delayed = dict(old)
    write = controller.write_state

    def completed_write_then_error(name, record):
        write(name, record)
        raise OSError('write completed before error')

    if write_then_raise:
        monkeypatch.setattr(controller, 'write_state', completed_write_then_error)
        with pytest.raises(OSError, match='write completed'):
            controller.withdrew(old, previous)
        monkeypatch.setattr(controller, 'write_state', write)
    else:
        controller.withdrew(old, previous)
    assert 'borrow_id' not in old
    peer = {'borrowing': True, 'sampled_unix': clock[0]}
    controller.admitted(peer)
    expected = adaptive_cpu.read_json(controller.base / 'last-borrow.json')
    assert peer['borrow_id'] != delayed['borrow_id']
    controller.withdrew(old, previous)
    controller.withdrew(delayed, previous)
    assert adaptive_cpu.read_json(controller.base / 'last-borrow.json') == expected


def test_unowned_legacy_rollback_cannot_return_a_current_borrow(rig):
    from prismabuild import adaptive_cpu
    queue, clock, state, donor, first, publish, claim, telemetry = rig
    controller = adaptive_cpu.Controller(queue.ledger(), {'preferred': [0], 'fallback': [1]})
    current = {'borrowing': True, 'sampled_unix': clock[0]}
    controller.admitted(current)
    expected = adaptive_cpu.read_json(controller.base / 'last-borrow.json')
    controller.withdrew({'borrowing': True, 'sampled_unix': clock[0]}, {})
    assert adaptive_cpu.read_json(controller.base / 'last-borrow.json') == expected
