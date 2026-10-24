"""An abandoned claim cannot return a later claimant's borrow."""
from pathlib import Path
import os
import json
import pytest
from test_adaptive_cpu import rig
from test_cpu_lending import loan_rig
from prismabuild import pool


@pytest.mark.parametrize('field,value', [('priority', 0), ('cpu_qos', 'protected')])
def test_changed_publication_unwinds_before_claim_commit(rig, monkeypatch, field, value):
    from prismabuild import adaptive_cpu
    queue, clock, state, donor, first, publish, claim, telemetry = rig
    key = f'{2:064x}'
    ready = queue.records.item_path(pool.record_store.READY, key)
    original = os.rename
    before = queue.ledger().held()
    marker = adaptive_cpu.local_state_base(queue.ledger().base) / 'last-borrow.json'
    previous = adaptive_cpu.read_json(marker)
    def republish(source, target):
        if Path(source) == ready:
            row = json.loads(ready.read_text())
            row[field] = value
            queue.records.replace_record_at(ready, row)
        return original(source, target)
    monkeypatch.setattr(pool.os, 'rename', republish)
    assert claim() is None
    assert ready.exists()
    assert not queue.records.item_path(pool.record_store.CLAIMED, key).exists()
    assert not queue.records.lease_path(key).exists()
    assert queue.ledger().held() == before
    assert adaptive_cpu.read_json(marker) == previous


def test_exception_after_borrow_return_does_not_return_a_peers_borrow(rig, monkeypatch):
    queue, clock, state, donor, first, publish, claim, telemetry = rig
    original_rename = os.rename
    original_discard = queue._discard_claim_intent
    lost_key = f'{2:064x}'
    peers = []

    def lose(source, target):
        if Path(source) == queue.records.item_path(pool.record_store.READY, lost_key):
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
def test_repeated_return_cannot_clear_a_peer_with_the_same_sample(loan_rig, monkeypatch, write_then_raise):
    from prismabuild import adaptive_cpu
    ledger, tiers, controller, donor, candidate, old = loan_rig
    handle = ledger.begin_acquire(candidate['action_key'], candidate['resources'], adaptive=old, cpu_tiers=tiers)
    assert handle
    previous = controller.admitted(old)
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
    ledger.abandon_acquire(handle)
    peer = controller.decision(candidate, candidate['resources'], identity=('candidate', False), owner=None)
    assert peer and peer['borrowing']
    handle = ledger.begin_acquire(candidate['action_key'], candidate['resources'], adaptive=peer, cpu_tiers=tiers)
    assert handle
    controller.admitted(peer)
    expected = adaptive_cpu.read_json(controller.base / 'last-opportunistic-borrow.json')
    assert peer['borrow_id'] != delayed['borrow_id']
    controller.withdrew(old, previous)
    controller.withdrew(delayed, previous)
    assert adaptive_cpu.read_json(controller.base / 'last-opportunistic-borrow.json') == expected


def test_unissued_opted_rollback_cannot_return_a_current_borrow(loan_rig):
    from prismabuild import adaptive_cpu
    ledger, tiers, controller, donor, item, current = loan_rig
    handle = ledger.begin_acquire(item['action_key'], item['resources'], adaptive=current, cpu_tiers=tiers)
    assert handle
    controller.admitted(current)
    expected = adaptive_cpu.read_json(controller.base / 'last-opportunistic-borrow.json')
    with pytest.raises(ValueError, match='unissued'):
        controller.withdrew({'cpu_qos': 'opportunistic', 'cpu_weight': 10,
                             'borrowing': True, 'sampled_unix': current['sampled_unix']}, {})
    assert adaptive_cpu.read_json(controller.base / 'last-opportunistic-borrow.json') == expected
