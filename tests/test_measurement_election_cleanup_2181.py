"""Concluded measurement elections retire only on positive, quiescent proof (#2181).

Every safety case continues to a positive cleanup assertion.  Election and ending
records come from the sealed fleet/canonical lifecycle, not a mocked verdict.
Only the explicitly named corruption/crash windows are made on disk.
"""
from __future__ import annotations

from contextlib import ExitStack
import json
import threading

import pytest

from test_measurement_drains_gpu_backfill import fleet as fleet_fixture
from test_measurement_reservation_backfill_1419 import _bounded_measurement_wait
from prismabuild import _measurement_reservation as reservation, adaptive_gpu, pool

fleet = fleet_fixture


def _read(path):
    return json.loads(path.read_bytes())


def _elected(fleet):
    queue = fleet[0]
    incumbent, key, _, _ = _bounded_measurement_wait(fleet)
    ready = queue.records.item_path(pool.record_store.READY, key)
    path = queue.records.passes_path(key)
    sidecar = path.read_bytes()
    election = _read(path)[reservation.FIELD]
    assert election["action_key"] == key
    assert election["published_unix"] == _read(ready)["published_unix"]
    assert election["host"] == "sparklina"
    assert reservation.selection(_read(path)) == election
    return incumbent, key, ready.read_bytes(), sidecar


def _claim_measurement(fleet, incumbent, key):
    queue, _, _, _, _, tick, claim, _ = fleet
    queue.finish(incumbent, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim("sparklina") == key
    claimed = queue.records.item_path(pool.record_store.CLAIMED, key).read_bytes()
    lease = queue.records.lease_path(key).read_bytes()
    return claimed, lease


def _authority(queue, key):
    """Byte snapshots include immutable withdrawals, not just their mutable head."""
    paths = [queue.records.item_path(state, key) for state in
             (pool.record_store.DONE, pool.record_store.FAILED, pool.record_store.WITHDRAWN)]
    paths.extend(path for path, _ in queue.records.withdrawal_decisions(key))
    paths.extend(path for path, _ in queue.records.attempt_documents(key))
    return {path: path.read_bytes() for path in paths if path.is_file()}


def _assert_authority(queue, key, before):
    assert _authority(queue, key) == before


def _assert_retired(queue, key):
    assert not queue.records.passes_path(key).exists(), (
        "exact concluded election with no publication, claim, finish, lease or "
        "physical holding must retire its persisted pass sidecar"
    )
    assert queue.passes(key) == 0


def test_canonical_finish_retires_election_only_after_claim_and_holdings_end(fleet):
    queue = fleet[0]
    incumbent, key, ready, sidecar = _elected(fleet)
    _claim_measurement(fleet, incumbent, key)
    path = queue.records.passes_path(key)
    # Recreate a legacy election left behind at claim commit. Consumption of
    # pre-claim authority and deletion of a concluded sidecar are distinct:
    # this sweep must not delete the whole sidecar while holdings remain.
    path.write_bytes(sidecar)
    assert queue.ledger("sparklina").holder_tokens(key)
    queue.sweep_orphan_passes(limit=1)
    assert path.exists(), "live claim holdings still require the pass sidecar"

    queue.finish(key, status="executed")
    ending = _read(queue.records.item_path(pool.record_store.DONE, key))
    assert ending["action_key"] == key
    assert ending["published_unix"] == json.loads(ready)["published_unix"]
    assert ending["status"] == "executed"
    assert not queue.ledger("sparklina").holder_tokens(key)
    assert not queue.records.item_path(pool.record_store.CLAIMED, key).exists()
    assert not queue.records.lease_path(key).exists()
    proof = _authority(queue, key)
    _assert_retired(queue, key)  # Must happen in finish, without waiting for a sweep.
    pool.PoolQueue(queue.root).sweep_orphan_passes(limit=1)
    _assert_authority(queue, key, proof)


def test_canonical_ready_withdrawal_retires_election_without_changing_decision(fleet):
    queue = fleet[0]
    _, key, ready, sidecar = _elected(fleet)
    # A live publication and no ending are not retirement authority.
    queue.sweep_orphan_passes(limit=1)
    assert queue.records.passes_path(key).read_bytes() == sidecar
    assert queue.records.item_path(pool.record_store.READY, key).read_bytes() == ready

    queue.withdraw(key, reason="#2181 acceptance cancellation")
    ending = _read(queue.records.item_path(pool.record_store.WITHDRAWN, key))
    assert ending["action_key"] == key
    assert ending["published_unix"] == json.loads(ready)["published_unix"]
    assert queue.records.withdrawal_decisions(key)
    proof = _authority(queue, key)
    assert not queue.records.item_path(pool.record_store.READY, key).exists()
    _assert_retired(queue, key)  # Must happen in withdraw, not only maintenance.
    pool.PoolQueue(queue.root).sweep_orphan_passes(limit=1)
    _assert_authority(queue, key, proof)


# Ending corruption is tested for both canonical forms.  Liveness guards use a
# real executed claim so claim/lease/finish bytes and physical tokens are real.
_ENDING_GUARDS = ["wrong-key", "stale-ending", "newer-ending", "no-ending",
                  "malformed-ending", "unreadable-ending"]
_CASES = [(ending, guard) for ending in ("terminal", "withdrawal")
          for guard in _ENDING_GUARDS]
_CASES += [("terminal", guard) for guard in
           ("ready", "claimed", "lease", "physical-holding", "finish", "lock")]


@pytest.mark.parametrize("ending_kind,guard", _CASES,
                         ids=[f"{ending}-{guard}" for ending, guard in _CASES])
def test_interrupted_legacy_election_requires_exact_quiescent_ending_then_sweeps(
        fleet, tmp_path, ending_kind, guard):
    queue, _, _, _, publish, _, _, _ = fleet
    incumbent, key, ready, sidecar = _elected(fleet)
    claimed = lease = None
    if ending_kind == "terminal":
        claimed, lease = _claim_measurement(fleet, incumbent, key)
        queue.finish(key, status="executed")
        state = pool.record_store.DONE
    else:
        queue.withdraw(key, reason="#2181 interrupted withdrawal cleanup")
        state = pool.record_store.WITHDRAWN
    ending_path = queue.records.item_path(state, key)
    proof = _authority(queue, key)
    ending_bytes = ending_path.read_bytes()
    path = queue.records.passes_path(key)
    # Recreate the legacy/crash window *after* the canonical ending.  Do not
    # depend on whether the implementation already cleans the synchronous path.
    path.write_bytes(sidecar)

    # An unrelated, genuinely withdrawn counter proves the limit is effective;
    # a retained malformed election tests progress past an unclassifiable row.
    # Neither malformed bytes nor an absent ending are ever positive proof.
    counters = []
    for number in range(2):
        other = publish(f"cleanup-cursor-{number}", cpu=1, gpu=0, mem_gb=1)
        queue.withdraw(other, reason="#2181 cursor fixture")
        record = {"action_key": other, "passes": 2}
        if number:
            record[reservation.FIELD] = {"state": "not-an-election"}
        other_path = queue.records.passes_path(other)
        other_path.write_text(json.dumps(record))
        counters.append(other_path)
    unknown_bytes = counters[1].read_bytes()

    with ExitStack() as restore:
        if guard in _ENDING_GUARDS:
            # Park immutable decisions/history too: no intact exact ending
            # should accidentally supply proof behind the damaged mutable head.
            immutable_records = [*queue.records.withdrawal_decisions(key),
                                 *queue.records.attempt_documents(key)]
            for number, (decision, _) in enumerate(immutable_records):
                parked = tmp_path / f"parked-proof-{number}.json"
                decision.rename(parked)
                restore.callback(parked.rename, decision)
            restore.callback(ending_path.write_bytes, ending_bytes)
            if guard == "no-ending":
                ending_path.unlink()
            elif guard == "unreadable-ending":
                # EISDIR is deterministic even as root; chmod(0) is not.
                ending_path.unlink()
                ending_path.mkdir()
                restore.callback(ending_path.rmdir)
            elif guard == "malformed-ending":
                ending_path.write_bytes(b'{"truncated":')
            else:
                ending = json.loads(ending_bytes)
                if guard == "wrong-key":
                    ending["action_key"] = incumbent
                else:
                    ending["published_unix"] += -1 if guard == "stale-ending" else 1
                ending_path.write_text(json.dumps(ending))
        elif guard in ("ready", "claimed", "lease"):
            blocker = (queue.records.lease_path(key) if guard == "lease" else
                       queue.records.item_path(pool.record_store.READY if guard == "ready"
                                               else pool.record_store.CLAIMED, key))
            # A rename without committed claim stamps is a transition gap,
            # not positive successful-claim evidence consuming the election.
            blocker.write_bytes({"ready": ready, "claimed": ready, "lease": lease}[guard])
            restore.callback(blocker.unlink)
        elif guard == "physical-holding":
            ledger = queue.ledger("sparklina")
            assert ledger.acquire(key, {"cpu": 1})
            restore.callback(ledger.release, key)
        elif guard == "finish":
            # The same canonical entombing operation finish uses, with the
            # genuine claim captured above, represents interrupted settlement.
            queue.records.item_path(pool.record_store.CLAIMED, key).write_bytes(claimed)
            blocker, matched = queue._entomb_claim(key, expect=json.loads(claimed))
            assert matched and blocker is not None
            assert not queue.records.item_path(pool.record_store.CLAIMED, key).exists()
            restore.callback(blocker.unlink)
        else:
            assert guard == "lock"
            # Same-thread locks are reentrant; a different thread must really
            # own this transition for the sweep's nonblocking attempt to fail.
            held, release = threading.Event(), threading.Event()

            def hold_transition():
                with queue.action_transition_lock(key, blocking=False) as acquired:
                    assert acquired
                    held.set()
                    assert release.wait(30)

            thread = threading.Thread(target=hold_transition)
            thread.start()

            def release_transition():
                release.set()
                thread.join(timeout=10)
                assert not thread.is_alive()

            restore.callback(release_transition)
            assert held.wait(10)
            with queue.action_transition_lock(key, blocking=False) as acquired:
                assert not acquired

        # Restart on *every* bounded call: no in-memory cursor or classification
        # may be required.  More calls than rows visit the kept/busy row too.
        for _ in range(4):
            restarted = pool.PoolQueue(queue.root)
            before = set((queue.root / pool.PASSES).glob("*.json"))
            pruned = restarted.sweep_orphan_passes(limit=1)
            after = set((queue.root / pool.PASSES).glob("*.json"))
            assert len(pruned) <= 1
            assert len(before - after) <= 1
            assert key not in pruned
            assert path.exists(), "retained authority forbids deleting the whole pass"
            if guard == "finish":
                # The genuine finish mark can also prove an earlier committed
                # claim consumed this election. That permits field retirement,
                # never deletion of the pass required by pending settlement.
                observed = _read(path)
                prior = json.loads(sidecar)
                assert observed in (prior, {name: value for name, value in prior.items()
                                          if name != reservation.FIELD})
            else:
                assert path.read_bytes() == sidecar
            assert counters[1].read_bytes() == unknown_bytes

    # Repair the interrupted evidence / finish releasing the last authority.
    # Only now does the exact original canonical ending authorize retirement.
    _assert_authority(queue, key, proof)
    for _ in range(4):
        restarted = pool.PoolQueue(queue.root)
        pruned = restarted.sweep_orphan_passes(limit=1)
        assert len(pruned) <= 1
        _assert_authority(queue, key, proof)
        assert counters[1].read_bytes() == unknown_bytes
        if not path.exists():
            assert key in pruned
            break
    _assert_retired(pool.PoolQueue(queue.root), key)
    assert not counters[0].exists()


def test_old_terminal_cannot_retire_a_real_successor_election(fleet, tmp_path):
    queue = fleet[0]
    incumbent, key, _, _ = _elected(fleet)
    _claim_measurement(fleet, incumbent, key)
    queue.finish(key, status="executed")
    old_proof = _authority(queue, key)
    old_generation = _read(queue.records.item_path(pool.record_store.DONE, key))["published_unix"]

    # Isolate the successor race from the separately tested predecessor leak:
    # model successful predecessor retirement before publishing the new work.
    queue.records.passes_path(key).unlink(missing_ok=True)
    # Republish and elect through real admission, never manufacture a successor
    # by relabelling a stored election. The earlier helper already established
    # the bounded drain; its episode need not renew its original deadline.
    publish, claim = fleet[4], fleet[6]
    next_incumbent = publish("successor-incumbent", priority=-10, timeout_s=1200,
                             cpu=2, gpu=1, mem_gb=8)
    assert claim("sparklina") == next_incumbent
    successor = publish("priority-zero-measurement", measurement=True,
                        priority=0, cpu=8, gpu=1, mem_gb=48)
    assert successor == key
    assert claim("sparklina") is None
    path = queue.records.passes_path(key)
    ready_path = queue.records.item_path(pool.record_store.READY, key)
    ready, sidecar = ready_path.read_bytes(), path.read_bytes()
    election = reservation.selection(json.loads(sidecar))
    assert election is not None
    assert election["published_unix"] == json.loads(ready)["published_unix"]
    assert election["published_unix"] > old_generation
    # Restore the old terminal at its original name if publication archived it:
    # this is the stale-ending visibility window the cleanup must withstand.
    for old_path, raw in old_proof.items():
        if old_path.exists():
            assert old_path.read_bytes() == raw
        else:
            old_path.write_bytes(raw)
    parked = tmp_path / "interrupted-successor-publication.json"
    ready_path.rename(parked)
    try:
        # Even with no live publication, the old exact terminal is not an
        # ending for this newer election. Absence alone must not erase it.
        for _ in range(2):
            assert key not in pool.PoolQueue(queue.root).sweep_orphan_passes(limit=1)
            assert path.read_bytes() == sidecar
            _assert_authority(queue, key, old_proof)
    finally:
        parked.rename(ready_path)

    queue.withdraw(key, reason="#2181 successor now actually concluded")
    proof = _authority(queue, key)
    # Reinstatement also exercises restart recovery for this actual successor.
    path.write_bytes(sidecar)
    restarted = pool.PoolQueue(queue.root)
    assert key in restarted.sweep_orphan_passes(limit=1)
    _assert_retired(restarted, key)
    _assert_authority(queue, key, proof)
