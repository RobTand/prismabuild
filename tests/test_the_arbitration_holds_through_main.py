"""The #966 arbitration's sibling rule and its busy grace, through ``main`` (#1004 item 5).

Two gaps the #966 review found:

* The sibling rule -- another mover of this copy's own consumer is judged by
  its mover alone, because that consumer is live by construction -- was
  proven only by calling ``_judge_owners`` directly.  Here two passes of one
  live consumer run through the real ``stage_move.main`` over a digest-less
  manifest whose origin changed between them, as a forward and a reverse
  pass, or two phases of one plan, do.  The second pass meets the first's
  record dating other bytes: it must replace them (the first mover has
  ended), not refuse its own consumer as a live conflict and retire its own
  window.
* Nothing contended an owner's transition lock (the ``busy`` state) past
  the publication grace.  Here another thread holds the dead owner's
  consumer lock for the whole run: the copy must wait out the grace and
  then refuse retryably -- no replacement, no terminal conflict, and no
  count toward the unproven-ending bound (#1004 item 2), since a held lock
  settles by itself.

Both pass on the source they test; each is shown to catch its regression by
a mutation recorded in the commit.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_a_divergent_staged_copy_never_loops_its_mover as loop  # noqa: E402
import test_a_restarted_promotion_adopts_its_own_copies as ram  # noqa: E402
import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
from prismabuild import pool, residency_plan  # noqa: E402
import stage_move  # noqa: E402


def _main(queue: pool.PoolQueue, tmp_path: Path, manifest: Path, digest: str,
          consumer: str, mover: str) -> tuple[int, dict[str, object]]:
    """One pass of ``consumer``'s ``mover`` through the real ``main``."""

    rc = stage_move.main([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", ram.STAGE_TIER,
        "--stage-root", str(tmp_path / "stage"),
        "--manifest", str(manifest),
        "--manifest-sha256", digest,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(ram.TOTAL),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", "4096", "--readers", "1", "--max-readers", "1",
        "--unpaced"])
    receipt = queue.move_record(mover)
    assert isinstance(receipt, dict)
    return rc, receipt


def test_a_second_pass_of_one_consumer_replaces_its_siblings_changed_copy(
        tmp_path: Path, monkeypatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", ram.GRACE)
    manifest, digest, body = ram._manifest(tmp_path, null_digest=True)
    paths = [tmp_path / "stage" / stage_move.stage_relative(
                 str(entry["path"]), 0, ram.SIZE,
                 mount_prefix=str(body["mount_prefix"]))
             for entry in body["entries"]]
    # The consumer is live -- claimed -- through both passes, as it is while
    # its plan's movers run.
    consumer, first, second = base._key(), base._key(), base._key()
    base._publish(queue, consumer, max_attempts=1)

    base._publish(queue, first, max_attempts=1)
    rc, receipt = _main(queue, tmp_path, manifest, digest, consumer, first)
    queue.finish(first, status="executed", detail={"returncode": rc})
    assert rc == 0 and receipt["complete"] is True, receipt.get("errors")
    before = ram._identities(paths)

    # The origin changes under the digest-less manifest: same names, same
    # sizes, same manifest digest, other bytes.
    changed = [bytes(reversed(ram._payload(index)))
               for index in range(ram.N)]
    for entry, payload in zip(body["entries"], changed):
        Path(str(entry["path"])).write_bytes(payload)

    base._publish(queue, second, max_attempts=1)
    rc, receipt = _main(queue, tmp_path, manifest, digest, consumer, second)

    assert rc == 0, (
        f"#966 sibling rule: the second pass refused its own consumer's "
        f"sibling copy: refusal={receipt.get('refusal')!r} "
        f"conflict={receipt.get('conflict')} errors={receipt.get('errors')}")
    assert receipt["complete"] is True, receipt.get("errors")
    assert "refusal" not in receipt and "conflict" not in receipt
    assert [path.read_bytes() for path in paths] == changed
    after = ram._identities(paths)
    assert all(new != old for new, old in zip(after, before))
    assert receipt["entries_invalidated"] == ram.N
    for row in receipt["invalidated"]:
        assert row["owners"] == [{"consumer_action_key": consumer,
                                  "mover_action_key": first,
                                  "state": "ended"}]
    # One judgment of the sibling for the whole range.
    timings = receipt["phase_timings"]
    assert timings["thread_seconds"]["owner_judgement"]["calls"] == 1
    assert timings["outcomes"].get("replaced_ended_owner") == ram.N


def test_a_held_owner_lock_past_the_grace_is_a_retryable_refusal(
        fleet, tmp_path, monkeypatch) -> None:
    queue, stage, _ = fleet
    consumer, mover = loop._old_owner(fleet, "failed")
    world = loop._World(fleet, tmp_path, monkeypatch, "last")
    world.claim()
    path = loop._staged(stage, loop.NAMES[1])
    before = os.stat(path)

    taken, release = threading.Event(), threading.Event()
    acquired: list[bool] = []

    def hold() -> None:
        with queue._transition_locked(consumer, blocking=False) as got:
            acquired.append(bool(got))
            taken.set()
            release.wait(timeout=60)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert taken.wait(timeout=10) and acquired == [True]
    try:
        rc, receipt = world.run(claimed=True)
    finally:
        release.set()
        holder.join(timeout=10)

    # Retryable, as a lock that will be let go must be.
    assert rc == 0 and receipt.get("refusal") is None, (
        rc, receipt.get("refusal"), receipt.get("conflict"))
    assert receipt["complete"] is False
    assert "conflict" not in receipt
    assert any("transition lock is held after the grace" in error
               for error in receipt["errors"]), receipt["errors"]
    # Nothing replaced, nothing judged, nothing counted.
    after = os.stat(path)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino,
                                                 before.st_mtime_ns)
    assert path.read_bytes() == loop.OLD
    assert not receipt.get("invalidated")
    assert "unproven" not in receipt
    phases = receipt["phase_timings"]["thread_seconds"]
    assert "owner_judgement" not in phases, phases
    # The grace was waited out at publication, not skipped.
    assert phases["publish_poll_sleep"]["calls"] >= 1, phases
    assert residency_plan.superseded(queue, world.plan) is None
    assert world.copier in world.window()
