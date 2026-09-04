"""The pull-queue transport, exercised on the primitives that can lose work."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time
import threading
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pathlib

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64
KEY_B = "b" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        **kw,
    )


def test_pool_root_must_be_absolute(tmp_path: Path) -> None:
    with pytest.raises(pool.PoolContractError):
        pool.PoolQueue("relative/path")


def test_action_key_must_be_a_digest(queue: pool.PoolQueue) -> None:
    with pytest.raises(pool.PoolContractError):
        _publish(queue, "short")


def test_publish_then_claim_moves_between_directories(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A)
    assert queue.item_path(pool.READY, KEY_A).exists()
    item = queue.claim()
    assert item is not None and item["action_key"] == KEY_A
    assert not queue.item_path(pool.READY, KEY_A).exists()
    assert queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert queue.lease_path(KEY_A).exists()


def test_claim_returns_none_on_empty_queue(queue: pool.PoolQueue) -> None:
    assert queue.claim() is None


def test_exactly_one_of_many_threads_claims_an_item(queue: pool.PoolQueue) -> None:
    """The race the whole design rests on: rename is the arbiter."""

    _publish(queue, KEY_A)
    winners: list[object] = []
    barrier = threading.Barrier(8)

    def contend() -> None:
        barrier.wait()
        got = queue.claim()
        if got is not None:
            winners.append(got)

    threads = [threading.Thread(target=contend) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(winners) == 1, f"{len(winners)} workers claimed the same action"


def test_two_items_two_claimants_no_double_claim(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A)
    _publish(queue, KEY_B)
    first = queue.claim()
    second = queue.claim()
    assert first is not None and second is not None
    assert {first["action_key"], second["action_key"]} == {KEY_A, KEY_B}
    assert queue.claim() is None


def test_gpu_placement_is_respected(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A, needs_gpu=True)
    assert queue.claim(has_gpu=False) is None
    assert queue.claim(has_gpu=True) is not None


def test_tag_placement_requires_every_tag(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A, tags=["gb10", "cuda"])
    assert queue.claim(tags=["gb10"]) is None
    assert queue.claim(tags=["gb10", "cuda", "extra"]) is not None


def test_priority_then_age_orders_the_queue(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A, priority=0)
    _publish(queue, KEY_B, priority=5)
    assert queue.claim()["action_key"] == KEY_B


def test_intent_is_written_before_the_claim(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A)
    queue.claim()
    intent = json.loads(queue.item_path(pool.INTENT, KEY_A).read_bytes())
    assert intent["schema"] == pool.POOL_CLAIM_INTENT_SCHEMA_V1
    assert intent["action_key"] == KEY_A


def test_stale_lease_is_requeued(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A)
    queue.claim()
    assert queue.reap_stale(timeout_s=1e6) == []          # fresh lease: untouched
    assert queue.reap_stale(timeout_s=-1.0) == [KEY_A]    # expired: back to ready
    assert queue.item_path(pool.READY, KEY_A).exists()
    assert not queue.lease_path(KEY_A).exists()


def test_a_claim_with_no_lease_at_all_is_stale(queue: pool.PoolQueue) -> None:
    """The claimant died between the rename and its first heartbeat.

    "No lease" alone is not enough to call it dead -- a claim made microseconds
    ago also has no lease yet, and reaping that one steals live work (see
    ``test_reap_does_not_steal_a_claim_made_moments_ago``).  Death is "no lease
    AND the claim itself is older than the grace period", so this test ages the
    claim record, which is what a real dead claimant's would be.
    """

    _publish(queue, KEY_A)
    queue.claim()
    queue.lease_path(KEY_A).unlink()
    path = queue.item_path(pool.CLAIMED, KEY_A)
    record = json.loads(path.read_bytes())
    record["claimed_unix"] = record["claimed_unix"] - (pool.HEARTBEAT_S + 60.0)
    path.write_text(json.dumps(record))
    assert queue.reap_stale() == [KEY_A]


def test_heartbeat_refresh_keeps_a_claim_alive(queue: pool.PoolQueue) -> None:
    """Assert the stored heartbeat advances, not that a derived age shrank --
    two ages sampled at different instants are not comparable."""

    _publish(queue, KEY_A)
    item = queue.claim()
    first = json.loads(queue.lease_path(KEY_A).read_bytes())["heartbeat_unix"]
    time.sleep(0.01)
    queue.write_lease(KEY_A, owner=str(item["claimed_by"]))
    second = json.loads(queue.lease_path(KEY_A).read_bytes())["heartbeat_unix"]
    assert second > first
    assert queue.reap_stale(timeout_s=1.0) == []


def test_finish_routes_to_done_or_retry(queue: pool.PoolQueue) -> None:
    """Success is terminal; a first failure is a retry, not a verdict."""

    for key, status, state in (
        (KEY_A, "executed", pool.DONE),
        (KEY_B, "failed", pool.READY),
    ):
        _publish(queue, key)
        queue.claim()
        queue.finish(key, status=status)
        assert queue.item_path(state, key).exists()
        assert not queue.item_path(pool.CLAIMED, key).exists()
        assert not queue.lease_path(key).exists()


def test_a_failure_retries_until_its_attempts_are_spent(queue: pool.PoolQueue) -> None:
    """Three strikes, then terminal -- and the count survives the requeue."""

    _publish(queue, KEY_A, max_attempts=3)
    for attempt in (1, 2):
        assert queue.claim() is not None
        queue.finish(KEY_A, status="failed")
        requeued = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
        assert requeued["attempts"] == attempt
        # A requeued item must not carry the dead claimant's identity forward.
        assert "claimed_by" not in requeued
    assert queue.claim() is not None
    queue.finish(KEY_A, status="failed")
    assert queue.item_path(pool.FAILED, KEY_A).exists()
    assert not queue.item_path(pool.READY, KEY_A).exists()


def test_max_attempts_of_one_is_terminal_on_the_first_failure(
    queue: pool.PoolQueue,
) -> None:
    _publish(queue, KEY_A, max_attempts=1)
    queue.claim()
    queue.finish(KEY_A, status="failed")
    assert queue.item_path(pool.FAILED, KEY_A).exists()


def test_cache_hit_counts_as_done_not_failed(queue: pool.PoolQueue) -> None:
    """A CAS hit is a successful outcome -- the work exists, it just already did."""

    _publish(queue, KEY_A)
    queue.claim()
    queue.finish(KEY_A, status="cache_hit")
    assert queue.item_path(pool.DONE, KEY_A).exists()


def test_worker_argv_matches_slurms_canonical_launch_minus_its_gate() -> None:
    argv = pool.worker_argv(
        worker_script="/w.py", action_key=KEY_A, cas_root="/cas", checkout_root="/co"
    )
    assert argv == [
        "/w.py",
        "run-local",
        "--action",
        f"/cas/requests/aa/{KEY_A}.json",
        "--cas-root",
        "/cas",
        "--checkout-root",
        "/co",
    ]
    assert "--require-slurm-initial-start" not in argv


def test_atomic_write_leaves_no_partial_file(queue: pool.PoolQueue, tmp_path: Path) -> None:
    target = tmp_path / "rec.json"
    pool._write_json_atomic(target, {"schema": "x", "n": 1})
    assert json.loads(target.read_bytes())["n"] == 1
    assert not list(tmp_path.glob(".*tmp"))


def test_serve_once_returns_none_on_empty_queue(queue: pool.PoolQueue) -> None:
    assert queue.serve_once() is None


def test_serve_once_executes_and_records(queue: pool.PoolQueue, tmp_path: Path) -> None:
    """End to end against a stub worker: claim -> run -> done."""

    marker = tmp_path / "ran"
    stub = tmp_path / "stub_worker.py"
    stub.write_text(
        "import sys, pathlib\n"
        f"pathlib.Path({str(marker)!r}).write_text(' '.join(sys.argv[1:]))\n"
        "sys.exit(0)\n"
    )
    _publish(queue, KEY_A, worker_script=str(stub))
    outcome = queue.serve_once()
    assert outcome is not None and outcome["status"] == "executed"
    assert outcome["returncode"] == 0
    assert marker.read_text().startswith("run-local --action")
    assert queue.item_path(pool.DONE, KEY_A).exists()


def test_serve_once_records_a_failing_worker_as_failed(
    queue: pool.PoolQueue, tmp_path: Path
) -> None:
    stub = tmp_path / "bad_worker.py"
    stub.write_text("import sys; sys.exit(3)\n")
    _publish(queue, KEY_A, worker_script=str(stub), max_attempts=1)
    outcome = queue.serve_once()
    assert outcome["status"] == "failed" and outcome["returncode"] == 3
    assert queue.item_path(pool.FAILED, KEY_A).exists()


def test_execute_refreshes_the_lease_across_a_slow_action(
    queue: pool.PoolQueue, tmp_path: Path
) -> None:
    """A long action must not be reaped out from under itself."""

    stub = tmp_path / "slow_worker.py"
    stub.write_text("import time; time.sleep(0.6)\n")
    _publish(queue, KEY_A, worker_script=str(stub))
    item = queue.claim()
    before = queue.lease_path(KEY_A).stat().st_mtime_ns
    outcome = queue.execute(item, heartbeat_s=0.15)
    assert outcome["status"] == "executed"
    assert queue.lease_path(KEY_A).stat().st_mtime_ns > before


def test_execute_times_out_and_kills_the_child(
    queue: pool.PoolQueue, tmp_path: Path
) -> None:
    stub = tmp_path / "hang_worker.py"
    stub.write_text("import time; time.sleep(60)\n")
    _publish(queue, KEY_A, worker_script=str(stub))
    item = queue.claim()
    outcome = queue.execute(item, heartbeat_s=0.1, timeout_s=0.3)
    assert outcome["status"] == "timeout"


def test_a_crashing_execute_never_leaves_a_dangling_claim(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    _publish(queue, KEY_A)

    def boom(*a: object, **k: object) -> None:
        raise RuntimeError("worker exploded")

    monkeypatch.setattr(queue, "execute", boom)
    with pytest.raises(RuntimeError):
        queue.serve_once()
    # The invariant is that the claim is gone, not where it went: an unexpected
    # crash still has retries left, so it is requeued rather than condemned.
    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert queue.item_path(pool.READY, KEY_A).exists()
    assert not queue.lease_path(KEY_A).exists()


def test_reap_then_reclaim_is_the_self_healing_path(queue: pool.PoolQueue) -> None:
    """A dead box's work returns to the pool and another worker takes it."""

    _publish(queue, KEY_A)
    first = queue.claim(owner="dead-box:1")
    assert first is not None
    queue.reap_stale(timeout_s=-1.0)
    second = queue.claim(owner="live-box:2")
    assert second is not None
    assert second["claimed_by"] != first["claimed_by"]


def test_schema_strings_keep_the_published_namespace() -> None:
    """Renaming these would orphan every receipt already in the CAS."""

    for schema in (
        pool.POOL_ITEM_SCHEMA_V1,
        pool.POOL_CLAIM_INTENT_SCHEMA_V1,
        pool.POOL_LEASE_SCHEMA_V1,
        pool.POOL_OUTCOME_SCHEMA_V1,
    ):
        assert schema.startswith("prismaquant.prismabuild.")


def test_reap_does_not_steal_a_claim_made_moments_ago(tmp_path):
    """The rename/write_lease window must not look like a dead claimant.

    claim() renames the item and only then writes the lease.  A reaper landing
    in between sees a claimed item with no lease.  Before the grace period it
    requeued that item, so a second worker could claim and run work the first
    worker was still about to start -- duplicated effort, bounded only by the
    CAS.  Simulated here by deleting the lease immediately after a claim.
    """

    queue = pool.PoolQueue(tmp_path / "q")
    queue.publish(
        action_key="a" * 64,
        cas_root=tmp_path / "cas",
        checkout_root=tmp_path / "co",
        worker_script=tmp_path / "w.py",
    )
    item = queue.claim(tags=(), has_gpu=False, owner="worker-1")
    assert item is not None
    key = item["action_key"]

    # Reproduce the window exactly: claimed, lease not yet written.
    queue.lease_path(key).unlink()
    assert queue.lease_age(key) is None

    assert queue.reap_stale() == []
    assert queue.claim(tags=(), has_gpu=False, owner="worker-2") is None

    # A claim old enough to be genuinely dead is still reaped.
    stale = queue.item_path(pool.CLAIMED, key)
    record = json.loads(stale.read_text())
    record["claimed_unix"] = record["claimed_unix"] - (pool.HEARTBEAT_S + 60.0)
    stale.write_text(json.dumps(record))
    assert queue.reap_stale() == [key]
    assert queue.claim(tags=(), has_gpu=False, owner="worker-2") is not None


# --- admission: the ledger, built from REPRO-2026-08-30 ---------------------


def test_capacity_tokens_are_created_idempotently(queue: pool.PoolQueue) -> None:
    ledger = queue.ledger(host="box")
    ledger.ensure_capacity({"gpu": 2, "mem_gb": 4})
    ledger.ensure_capacity({"gpu": 2, "mem_gb": 4})
    assert ledger.capacity() == {"gpu": 2, "mem_gb": 4}
    assert ledger.available() == {"gpu": 2, "mem_gb": 4}


def test_acquire_is_all_or_nothing(queue: pool.PoolQueue) -> None:
    """The repro's third bug: never keep what you got while blocked on the rest."""

    ledger = queue.ledger(host="box")
    ledger.ensure_capacity({"gpu": 1, "mem_gb": 2})
    assert ledger.acquire(KEY_A, {"gpu": 1, "mem_gb": 8}) is False
    # The gpu token it *could* take must not be left held.
    assert ledger.available() == {"gpu": 1, "mem_gb": 2}
    assert ledger.held_keys() == []


def test_capacity_is_not_oversubscribed_under_contention(
    queue: pool.PoolQueue,
) -> None:
    """Eight threads, three tokens: exactly three win."""

    ledger = queue.ledger(host="box")
    ledger.ensure_capacity({"gpu": 3})
    won: list[str] = []
    lock = threading.Lock()

    def grab(n: int) -> None:
        if ledger.acquire(f"{n:064d}", {"gpu": 1}):
            with lock:
                won.append(f"{n:064d}")

    threads = [threading.Thread(target=grab, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(won) == 3
    assert ledger.available().get("gpu", 0) == 0


def test_admission_refuses_when_the_box_is_full(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A, resources={"gpu": 1})
    _publish(queue, KEY_B, resources={"gpu": 1})
    capacity = {"gpu": 1}
    assert queue.claim(capacity=capacity) is not None
    # One token, one running action: the second is denied, and stays ready.
    assert queue.claim(capacity=capacity) is None
    assert queue.item_path(pool.READY, KEY_B).exists()


def test_finishing_an_action_returns_its_capacity(queue: pool.PoolQueue) -> None:
    """A reservation is held only while running -- the repro's first bug."""

    _publish(queue, KEY_A, resources={"gpu": 1})
    _publish(queue, KEY_B, resources={"gpu": 1})
    capacity = {"gpu": 1}
    first = queue.claim(capacity=capacity)
    assert first is not None
    assert queue.claim(capacity=capacity) is None
    queue.finish(KEY_A, status="executed")
    assert queue.ledger().available().get("gpu", 0) == 1
    assert queue.claim(capacity=capacity) is not None


def test_reaping_a_dead_claimant_returns_its_capacity(queue: pool.PoolQueue) -> None:
    """A reservation must not outlive its holder, or the box never recovers."""

    _publish(queue, KEY_A, resources={"gpu": 1})
    assert queue.claim(capacity={"gpu": 1}) is not None
    assert queue.ledger().available().get("gpu", 0) == 0
    queue.reap_stale(timeout_s=-1.0)
    assert queue.ledger().available().get("gpu", 0) == 1


def test_losing_the_claim_race_releases_the_tokens(queue: pool.PoolQueue) -> None:
    """Admission runs before the rename, so a loser must hold nothing."""

    _publish(queue, KEY_A, resources={"gpu": 1})
    ledger = queue.ledger()
    ledger.ensure_capacity({"gpu": 1})
    # Simulate the race: the item vanishes between admission and the rename.
    original = os.rename

    def steal(src: object, dst: object) -> None:
        if str(src).endswith(f"{KEY_A}.json") and pool.READY in str(src):
            raise FileNotFoundError(src)
        original(src, dst)

    with mock.patch.object(pool.os, "rename", steal):
        assert queue.claim(capacity={"gpu": 1}) is None
    assert ledger.available().get("gpu", 0) == 1
    assert ledger.held_keys() == []


def test_denials_age_an_item_to_the_front(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A)
    _publish(queue, KEY_B)
    queue.record_pass(KEY_B)
    assert [r["action_key"] for r in queue.ready_items()][0] == KEY_B
    assert queue.passes(KEY_B) == 1


def test_a_starved_item_withholds_the_host_instead_of_being_overtaken(
    queue: pool.PoolQueue,
) -> None:
    """The repro's second bug: a counter wired to nothing is not a fix.

    A big item that keeps losing admission to small ones must eventually stop
    being overtaken, or it never runs while the queue stays busy.
    """

    _publish(queue, KEY_A, resources={"gpu": 4})      # the big, starved one
    _publish(queue, KEY_B, resources={"gpu": 1})      # the small overtaker
    capacity = {"gpu": 4}
    ledger = queue.ledger()
    ledger.ensure_capacity(capacity)
    # Occupy two tokens so the big item cannot fit but the small one could.
    assert ledger.acquire("0" * 64, {"gpu": 2}) is True

    for _ in range(pool.STARVATION_FLOOR - 1):
        taken = queue.claim(capacity=capacity)
        assert taken is not None and taken["action_key"] == KEY_B
        queue.finish(KEY_B, status="executed")
        queue.item_path(pool.DONE, KEY_B).unlink()
        _publish(queue, KEY_B, resources={"gpu": 1})

    assert queue.passes(KEY_A) >= pool.STARVATION_FLOOR - 1
    queue.record_pass(KEY_A)
    # Now the floor is reached: the host is withheld rather than handed to KEY_B.
    assert queue.claim(capacity=capacity) is None
    assert queue.item_path(pool.READY, KEY_B).exists()


def test_work_that_can_never_fit_here_does_not_deadlock_the_box(
    queue: pool.PoolQueue,
) -> None:
    """Withholding a box for work it could never run is the deadlock, not the fix."""

    _publish(queue, KEY_A, resources={"gpu": 99})     # never fits this host
    _publish(queue, KEY_B, resources={"gpu": 1})
    for _ in range(pool.STARVATION_FLOOR + 2):
        queue.record_pass(KEY_A)
    taken = queue.claim(capacity={"gpu": 2})
    assert taken is not None and taken["action_key"] == KEY_B


def test_a_claim_clears_its_own_denial_history(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_A, resources={"gpu": 1})
    queue.record_pass(KEY_A)
    assert queue.claim(capacity={"gpu": 1}) is not None
    assert queue.passes(KEY_A) == 0


def test_admission_is_skipped_when_no_capacity_is_declared(
    queue: pool.PoolQueue,
) -> None:
    """Pre-ledger behaviour is intact for callers that declare nothing."""

    _publish(queue, KEY_A, resources={"gpu": 99})
    assert queue.claim() is not None


def test_reaping_a_foreign_claimant_returns_capacity_to_that_host(
    queue: pool.PoolQueue,
) -> None:
    """The reaper is usually not the box that died.

    Tokens live under the *claimant's* ledger.  A reaper that released against
    its own hostname would leak the dead box's capacity permanently, and the
    leak is invisible: the box simply stops admitting work, every worker loop
    exits on max-idle, and it looks exactly like an empty queue.
    """

    _publish(queue, KEY_A, resources={"gpu": 2})
    assert queue.claim(capacity={"gpu": 2}) is not None

    # Re-file the claim as though a *different* box had taken it, and move the
    # tokens to that box's ledger -- the real cross-box shape.
    record = json.loads(queue.item_path(pool.CLAIMED, KEY_A).read_text())
    mine = record["claimed_host"]
    record["claimed_host"] = "other-box"
    queue.item_path(pool.CLAIMED, KEY_A).write_text(json.dumps(record))
    queue.ledger(mine).release(KEY_A)
    queue.ledger("other-box").ensure_capacity({"gpu": 2})
    assert queue.ledger("other-box").acquire(KEY_A, {"gpu": 2}) is True

    queue.reap_stale(timeout_s=-1.0)

    assert queue.ledger("other-box").available().get("gpu", 0) == 2
    assert queue.ledger("other-box").held_keys() == []
    assert queue.item_path(pool.READY, KEY_A).exists()


def test_retire_free_capacity_lowers_the_offer_but_never_a_held_token(tmp_path):
    """A box's honest offer falls when work the pool did not schedule arrives.

    ``ensure_capacity`` only ever adds, so without this the ledger keeps
    advertising the high-water mark -- a GB10 offering 96 GB while holding
    10 GB free, which is a promise the box cannot keep rather than admission
    control.
    """

    from prismabuild import pool

    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    ledger = queue.ledger("box")
    ledger.ensure_capacity({"mem_gb": 16, "gpu": 2})
    assert ledger.capacity() == {"mem_gb": 16, "gpu": 2}

    # Something is running under a reservation the pool granted earlier.
    assert ledger.acquire("running-action", {"mem_gb": 6, "gpu": 1}) is True

    retired = ledger.retire_free_capacity({"mem_gb": 8})
    assert retired == {"mem_gb": 8}
    assert ledger.capacity()["mem_gb"] == 8
    # The running action keeps every token it is executing under.
    assert ledger.available()["mem_gb"] == 2
    assert "running-action" in ledger.held_keys()
    # gpu was not named, so it is untouched.
    assert ledger.capacity()["gpu"] == 2

    # Retiring below what is already held cannot delete a held token.
    ledger.retire_free_capacity({"mem_gb": 1})
    assert ledger.capacity()["mem_gb"] >= 6
    assert ledger.release("running-action") == 7


def _vanishing(monkeypatch, victim):
    """Make `victim` pass is_dir() and then raise on iterdir()/glob().

    That is the actual race, and it cannot be staged by deleting the directory
    first: the code checks `is_dir()` and *then* scans, so a test that deletes
    up front takes the guarded early-return path and passes against the bug.
    The window between the check and the scan is the whole defect, so the test
    has to reproduce the window rather than the aftermath.
    """
    real_iterdir = pathlib.Path.iterdir
    real_glob = pathlib.Path.glob

    def iterdir(self):
        if self == victim:
            raise FileNotFoundError(2, "No such file or directory", str(self))
        return real_iterdir(self)

    def glob(self, pattern):
        if self == victim:
            raise FileNotFoundError(2, "No such file or directory", str(self))
        return real_glob(self, pattern)

    monkeypatch.setattr(pathlib.Path, "iterdir", iterdir)
    monkeypatch.setattr(pathlib.Path, "glob", glob)


def test_release_survives_the_holder_vanishing_after_its_is_dir_check(tmp_path, monkeypatch):
    ledger = pool.PoolQueue(tmp_path / "q").ledger("box")
    ledger.ensure_capacity({"mem_gb": 4})
    assert ledger.acquire("act", {"mem_gb": 2})
    _vanishing(monkeypatch, ledger.held_dir / "act")
    assert ledger.release("act") == 0        # not FileNotFoundError


def test_every_ledger_scan_survives_the_held_tree_vanishing(tmp_path, monkeypatch):
    # ensure_capacity, retire_free_capacity, capacity and held_keys all walk
    # held_dir; a concurrent release removing a holder killed the worker.
    ledger = pool.PoolQueue(tmp_path / "q").ledger("box")
    ledger.ensure_capacity({"mem_gb": 4, "gpu": 1})
    assert ledger.acquire("act", {"gpu": 1})
    _vanishing(monkeypatch, ledger.held_dir)
    assert ledger.held_keys() == []
    assert ledger.capacity() == {"mem_gb": 4}
    assert ledger.available() == {"mem_gb": 4}
    ledger.ensure_capacity({"mem_gb": 4})
    ledger.retire_free_capacity({"mem_gb": 2})


def test_a_reaper_that_loses_the_race_writes_no_keyless_stub(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two reapers, one claim: the loser must leave the winner's work alone.

    ``reap_stale`` globs ``claimed`` and then reads each file, and both
    ``finish()`` and the reap itself write the item's next home *before*
    unlinking the claim.  So a second reaper -- on the other box, a second
    apart -- can glob a claim that no longer exists by the time it reads.
    Treating that absence as an empty record and requeueing it publishes a
    record with no ``action_key``, and ``claim()`` skips a keyless item
    forever: the item never runs, never fails, and never leaves ``ready``.
    Four real jobs were lost this way before the read was allowed to mean
    "gone".
    """

    _publish(queue, KEY_A)
    assert queue.claim() is not None
    claimed_path = queue.item_path(pool.CLAIMED, KEY_A)
    real_read = pool._read_json

    def vanishing_read(path: object) -> object:
        if pathlib.Path(str(path)) == claimed_path:
            claimed_path.unlink(missing_ok=True)      # the winner concluded it
            return None
        return real_read(path)

    monkeypatch.setattr(pool, "_read_json", vanishing_read)

    assert queue.reap_stale(timeout_s=-1.0) == []
    assert list(queue.dir(pool.READY).glob("*.json")) == []


def test_a_keyless_ready_record_is_filed_rather_than_left_to_starve(
    queue: pool.PoolQueue,
) -> None:
    """An unaddressable item belongs in ``failed``, where it can be counted.

    ``claim()`` addresses an item by ``action_key`` and skips a record without
    one, so such a record is invisible work: it occupies ``ready``, reports as
    pending, and no worker will ever take it.  The reaper race above is one way
    to make one and a worker still running the old code is another, so the
    sweep stands on its own.  Filing it as ``orphaned_stub`` turns a silent
    permanent resident into a defect somebody can see.
    """

    _publish(queue, KEY_A)
    stub = queue.item_path(pool.READY, KEY_B)
    stub.write_text(json.dumps({"claimed_host": "sparky", "attempts": 1}))

    assert queue.claim() is not None                      # KEY_A still runnable
    assert queue.quarantine_orphans() == [KEY_B]
    assert not stub.exists()

    filed = json.loads(queue.item_path(pool.FAILED, KEY_B).read_text())
    assert filed["status"] == "orphaned_stub"
    assert filed["action_key"] == KEY_B
    assert "reap_stale" in filed["detail"]["reason"]


def test_an_unplaceable_item_is_knowable_before_it_is_published(
    queue: pool.PoolQueue,
) -> None:
    """The queue must be able to say "no box can run this".

    Without an offer registry the queue knows what work was asked for and
    nothing about what the fleet can do, so an item whose required tags no
    worker offers is indistinguishable from an item whose box is merely busy.
    A test suite submitted with tag ``dl380`` sat in ``ready`` for ten minutes
    in front of fifteen idle workers offering ``x86``, and would have sat
    there for a day.
    """

    queue.announce(host="dl380g10", tags=["x86", "dl380g10", "cpu"],
                   has_gpu=False, capacity={"mem_gb": 60})
    queue.announce(host="sparky", tags=["gb10", "sparky"],
                   has_gpu=True, capacity={"gpu": 4, "mem_gb": 100})

    assert queue.placeable({"tags": ["x86"], "resources": {"mem_gb": 4}}) is True
    assert queue.placeable({"tags": ["dl380"], "resources": {"mem_gb": 4}}) is False
    assert queue.offered_tags() == ["cpu", "dl380g10", "gb10", "sparky", "x86"]


def test_a_gpu_demand_is_not_placeable_on_a_cpu_box(queue: pool.PoolQueue) -> None:
    """Tags alone would match; the offer has to carry the GPU fact too."""

    queue.announce(host="dl380g10", tags=["x86", "cpu"], has_gpu=False,
                   capacity={"mem_gb": 60})
    assert queue.placeable({"tags": [], "resources": {"gpu": 1}}) is False
    assert queue.placeable({"tags": ["x86"], "needs_gpu": True}) is False
    assert queue.placeable({"tags": ["x86"], "resources": {"mem_gb": 4}}) is True


def test_a_demand_larger_than_any_box_is_refused_not_queued(
    queue: pool.PoolQueue,
) -> None:
    """An idle box that can never fit the item is not a reason to wait for it."""

    queue.announce(host="sparky", tags=["gb10"], has_gpu=True,
                   capacity={"gpu": 4, "mem_gb": 100})
    assert queue.placeable({"tags": [], "resources": {"mem_gb": 400}}) is False


def test_an_empty_registry_answers_unknown_rather_than_no(
    queue: pool.PoolQueue,
) -> None:
    """Three-valued on purpose: refusing on silence breaks the submit path.

    A fleet whose worker loops predate the registry announces nothing, and a
    queue whose workers are down announces nothing.  Neither is evidence that
    the work is unrunnable, and turning either into a refusal would replace a
    missing diagnostic with a broken submitter.
    """

    assert queue.placeable({"tags": ["x86"]}) is None
    assert queue.offered_tags() == []


def test_a_stale_offer_does_not_vouch_for_a_dead_box(queue: pool.PoolQueue) -> None:
    """An offer is a claim refreshed by its own box; expiry is what makes it one."""

    queue.announce(host="dl380g10", tags=["x86"], has_gpu=False,
                   capacity={"mem_gb": 60})
    assert queue.placeable({"tags": ["x86"]}, max_age_s=1e6) is True
    assert queue.placeable({"tags": ["x86"]}, max_age_s=-1.0) is None
    assert queue.offered_tags(max_age_s=-1.0) == []


def test_finish_does_not_republish_a_payloadless_stub(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The twin of the reaper race, on the other side of the same window.

    A reaper concludes a claim while the work is still running.  The worker
    then calls ``finish()``, reads nothing, and -- before this fix -- built
    ``{"action_key": key}`` and let the requeue branch write it into ``ready``
    *over* the full record the reaper had just filed.  The action could then
    never run again: every later claim died on ``KeyError('worker_script')``,
    taking the worker process with it, and the only record of where the work
    lived was gone.  Six actions in the live queue are unrecoverable this way.
    """

    _publish(queue, KEY_A)
    assert queue.claim() is not None
    queue.item_path(pool.CLAIMED, KEY_A).unlink()          # the reaper won

    landed = queue.finish(KEY_A, status="failed", detail={"exception": "boom"})

    assert landed == queue.item_path(pool.FAILED, KEY_A)
    assert list(queue.dir(pool.READY).glob("*.json")) == []
    filed = json.loads(landed.read_text())
    assert filed["status"] == "finish_lost_race"
    assert filed["detail"]["worker_detail"] == {"exception": "boom"}


def test_finish_losing_the_race_does_not_overwrite_the_winners_record(
    queue: pool.PoolQueue,
) -> None:
    """Whatever the winner filed stands; the loser must not clobber it."""

    _publish(queue, KEY_A)
    assert queue.claim() is not None
    queue.item_path(pool.CLAIMED, KEY_A).unlink()
    winner = queue.item_path(pool.FAILED, KEY_A)
    winner.write_text(json.dumps({"action_key": KEY_A, "status": "the winner"}))

    queue.finish(KEY_A, status="failed")

    assert json.loads(winner.read_text())["status"] == "the winner"


def test_a_ready_record_a_worker_cannot_execute_is_quarantined(
    queue: pool.PoolQueue,
) -> None:
    """Having the key is not the same as being runnable.

    A record with the right ``action_key`` but no ``worker_script`` is claimed,
    kills the worker on ``KeyError``, and does it once per attempt before it is
    finally filed.  Seven of those are in the live queue's ``failed``, each
    having taken a worker process down with it.
    """

    _publish(queue, KEY_A)
    stub = queue.item_path(pool.READY, KEY_B)
    stub.write_text(json.dumps({"action_key": KEY_B, "attempts": 0}))

    assert queue.quarantine_orphans() == [KEY_B]
    filed = json.loads(queue.item_path(pool.FAILED, KEY_B).read_text())
    assert filed["status"] == "orphaned_stub"
    assert "worker_script" in filed["detail"]["reason"]
    # The healthy item is untouched.
    assert queue.item_path(pool.READY, KEY_A).exists()


def test_a_lease_whose_record_is_gone_is_swept(tmp_path) -> None:
    """The mirror of ``quarantine_orphans``, and it had a live instance.

    ``finish`` and ``reap_stale`` each unlink the lease beside the record they
    conclude, so this should not happen -- and ``daf08495c8bb`` sat in the
    live queue for seven and a half hours anyway, pid dead, no ``.json``, read
    by anything counting ``claimed/`` as a running action.
    """

    from prismabuild import pool

    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    queue.ledger("box").ensure_capacity({"gpu": 1})
    assert queue.ledger("box").acquire(KEY_A, {"gpu": 1}) is True
    lease = queue.lease_path(KEY_A)
    lease.parent.mkdir(parents=True, exist_ok=True)
    lease.write_text(json.dumps({
        "action_key": KEY_A, "host": "box", "pid": 1,
        "heartbeat_unix": time.time() - 10_000.0,
    }))

    assert queue.sweep_widowed_leases(timeout_s=60.0) == [KEY_A]
    assert not lease.exists()
    # The tokens it was holding come back with it.
    assert queue.ledger("box").available().get("gpu", 0) == 1


def test_a_lease_written_moments_ago_is_left_alone(tmp_path) -> None:
    """``claim()`` writes the lease after the rename, so young is not widowed."""

    from prismabuild import pool

    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    lease = queue.lease_path(KEY_B)
    lease.parent.mkdir(parents=True, exist_ok=True)
    lease.write_text(json.dumps({"action_key": KEY_B, "heartbeat_unix": time.time()}))

    assert queue.sweep_widowed_leases(timeout_s=60.0) == []
    assert lease.exists()


def test_a_lease_beside_its_record_is_never_swept(tmp_path) -> None:
    """Only the widowed shape; a live claim keeps its lease however old."""

    from prismabuild import pool

    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    _publish(queue, KEY_A)
    assert queue.claim() is not None
    old = json.loads(queue.lease_path(KEY_A).read_text())
    old["heartbeat_unix"] = time.time() - 10_000.0
    queue.lease_path(KEY_A).write_text(json.dumps(old))

    assert queue.sweep_widowed_leases(timeout_s=60.0) == []
    assert queue.lease_path(KEY_A).exists()


def test_the_withhold_expires_so_a_long_block_does_not_become_the_deadlock(
    queue: pool.PoolQueue,
) -> None:
    """The guard must bound itself, or it becomes what it was written to prevent.

    Measured on the live fleet 2026-09-04: a GPU action at the head of sparky's
    ready queue reached **293** denied passes while two multi-hour actions held
    both GPU slots.  It withheld the box for every one of them, and 41 items
    queued behind it -- 24 CPU-only, admissible against five free cores the
    starved item was not competing for.  ``STARVATION_FLOOR`` assumes the block
    is transient; when the blocking resource is held for hours it inverts.

    Past ``WITHHOLD_CEILING_S`` the item keeps every pass, and passes are the
    first term of the ready ordering -- so it loses the veto, not the priority.
    """

    _publish(queue, KEY_A, resources={"gpu": 4})      # the big, starved one
    _publish(queue, KEY_B, resources={"gpu": 1})      # the small overtaker
    capacity = {"gpu": 4}
    ledger = queue.ledger()
    ledger.ensure_capacity(capacity)
    assert ledger.acquire("0" * 64, {"gpu": 2}) is True

    for _ in range(pool.STARVATION_FLOOR):
        queue.record_pass(KEY_A)
    # Inside the ceiling: the host is withheld, exactly as before this change.
    assert queue.claim(capacity=capacity) is None
    assert queue.item_path(pool.READY, KEY_B).exists()

    # Age the block itself past the ceiling -- and only the block.  ``passes``
    # is untouched, which is the property under test.
    record = json.loads(queue.passes_path(KEY_A).read_text())
    before = record["passes"]
    record["first_unix"] -= pool.WITHHOLD_CEILING_S + 1.0
    queue.passes_path(KEY_A).write_text(json.dumps(record))
    assert queue.withhold_age(KEY_A) > pool.WITHHOLD_CEILING_S

    taken = queue.claim(capacity=capacity)
    assert taken is not None and taken["action_key"] == KEY_B, (
        "past the ceiling the box must stop being held shut for work it cannot admit"
    )
    assert queue.passes(KEY_A) >= before, (
        "the starved item must keep its passes, and so its place in the ordering"
    )
    assert queue.item_path(pool.READY, KEY_A).exists(), "it is still queued, not dropped"


def test_the_first_denial_stamp_is_the_age_of_the_block_not_of_the_last_denial(
    queue: pool.PoolQueue,
) -> None:
    """``withhold_age`` must not reset every time the item is denied again.

    If it read ``updated_unix`` the ceiling would never be reached: a queue busy
    enough to starve an item is busy enough to re-deny it every few seconds.
    """

    _publish(queue, KEY_A, resources={"gpu": 4})
    queue.record_pass(KEY_A)
    stamped = json.loads(queue.passes_path(KEY_A).read_text())
    stamped["first_unix"] -= 600.0
    stamped["updated_unix"] -= 600.0
    queue.passes_path(KEY_A).write_text(json.dumps(stamped))

    queue.record_pass(KEY_A)          # denied again, right now
    assert queue.withhold_age(KEY_A) >= 600.0, (
        "a later denial reset the clock, so the ceiling can never be reached"
    )
    assert queue.passes(KEY_A) == 2


def test_an_item_never_denied_has_no_withhold_age(queue: pool.PoolQueue) -> None:
    assert queue.withhold_age(KEY_A) == 0.0
