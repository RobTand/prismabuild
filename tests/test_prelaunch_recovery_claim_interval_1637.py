"""A claim in the interval of a fence recovery pays no second stage charge (#1637).

Review of b845063a62: a recovery rebinds a ``transferring`` record to a fresh
``reserved`` generation, moves the group's replacement tokens to the mover and
advances the record.  ``funded_cover`` gives a ``reserved`` record no credit, so
a claim that lands between the rotation and the transfer pays its full demand
from free, and the transfer then adds the replacement fence on top: the mover
holds two charges.

Both tests run on a tmp_path queue.  The first stops the recovery after the
rotation (an interrupted transaction) and claims in the gap.  The second runs
the claim on another thread while the recovery's own transfer is in progress.
"""
from __future__ import annotations

from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool, prelaunch_group as pg  # noqa: E402
from test_prelaunch_group_reconcile_1594 import (  # noqa: E402
    TIER, _drive_to_committed, _group, _hexkey, _leg, _plan, _publish, _queue)

KIND = "stage_gib"


def _tokens(queue, key: str) -> int:
    return int(queue.tier_ledger(TIER).holder_tokens(key).get(KIND, 0))


def _free(queue) -> int:
    return int(queue.tier_ledger(TIER).available().get(KIND, 0))


def _recovering(tmp_path: Path):
    """A published mover whose tokens left, and a group topped up to its demand."""
    queue = _queue(tmp_path, stage_gib=12)
    consumer = _hexkey("rci-consumer")
    first, second = _hexkey("rci-m1"), _hexkey("rci-m2")
    plan = _plan(queue, consumer, first, second)
    unit, holder = _group(queue, plan, [first, second], demand=4)
    _drive_to_committed(queue, TIER, unit, holder, 4, [first, second])
    row = _publish(queue, plan, first)
    published = pg.publish_chunk(queue, TIER, unit, holder, plan,
                                 _leg(plan, first), float(row["published_unix"]))
    assert published.status == "published"
    queue.tier_ledger(TIER).release(first)       # the fence leaves, the record stays
    for _ in range(8):
        outcome = pg.reconcile(queue, TIER, unit, holder, 4, [first, second],
                               writer_is_me=True)
        if outcome.authority:
            break
    assert outcome.authority is True, (outcome.state, outcome.events)
    assert _tokens(queue, holder) == 4
    return queue, plan, unit, holder, first, row


def _recover(queue, plan, unit, holder, first, row):
    return pg.publish_chunk(queue, TIER, unit, holder, plan, _leg(plan, first),
                            float(row["published_unix"]))


def test_a_claim_after_an_interrupted_recovery_waits_for_the_fence(
        tmp_path: Path, monkeypatch) -> None:
    queue, plan, unit, holder, first, row = _recovering(tmp_path)
    free_before = _free(queue)

    def interrupted(*_args, **_kwargs):
        raise OSError("the recovery stopped after the rotation")

    real = queue.transfer_tier_reservation_count
    monkeypatch.setattr(queue, "transfer_tier_reservation_count", interrupted)
    stopped = _recover(queue, plan, unit, holder, first, row)
    assert stopped.status == "deferred"
    record = queue.read_funding(first, TIER)
    assert record is not None and record["state"] == "reserved"
    got = queue.claim(tags=["dl380g10"], owner="w-gap")
    state = {"claimed": None if got is None else str(got["action_key"])[-8:],
             "free before": free_before, "free after": _free(queue),
             "mover tokens": _tokens(queue, first)}
    assert got is None or got["action_key"] != first, state
    assert _free(queue) == free_before, state
    assert _tokens(queue, first) == 0, state

    monkeypatch.setattr(queue, "transfer_tier_reservation_count", real)
    assert _recover(queue, plan, unit, holder, first, row).status == "published"
    free_mid = _free(queue)
    got = queue.claim(tags=["dl380g10"], owner="w-after")
    assert got is not None and got["action_key"] == first
    assert _free(queue) == free_mid
    assert _tokens(queue, first) == 2
    assert queue.read_funding(first, TIER)["state"] == "consumed"


def test_a_claim_while_the_recovery_transfers_is_not_charged_twice(
        tmp_path: Path, monkeypatch) -> None:
    queue, plan, unit, holder, first, row = _recovering(tmp_path)
    free_before = _free(queue)
    seen: dict[str, object] = {}
    real = queue.transfer_tier_reservation_count

    def transfer_with_a_claimant(*args, **kwargs):
        def claim() -> None:
            seen["got"] = queue.claim(tags=["dl380g10"], owner="w-thread")

        worker = threading.Thread(target=claim)
        worker.start()
        worker.join(5)
        seen["still waiting"] = worker.is_alive()
        seen["thread"] = worker
        return real(*args, **kwargs)

    monkeypatch.setattr(queue, "transfer_tier_reservation_count",
                        transfer_with_a_claimant)
    assert _recover(queue, plan, unit, holder, first, row).status == "published"
    seen["thread"].join(30)
    state = {"free before": free_before, "free after": _free(queue),
             "mover tokens": _tokens(queue, first),
             "claim returned": None if seen.get("got") is None
             else str(seen["got"]["action_key"])[-8:]}
    assert _tokens(queue, first) in (0, 2), state
    assert _free(queue) == free_before, state
