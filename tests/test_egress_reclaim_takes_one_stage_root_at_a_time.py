"""Egress containment reclamation never holds two stage roots (#780).

The egress takes root A's ownership lock and decides one mover's fate under
it.  Containment reclamation walks *every* pin owner, and ``release_refs``
takes the root each pin names -- root B for a pin filed on another stage.
Called from inside A's exclusion that is an A->B request, and a second egress
holding B walking the same lease tree is a B->A request: a cycle.
``posix_lock.held`` nests on the *same* path only, so same-root reentrancy
does not prevent it.

Two tests, one property:

* the single-process one states the invariant directly -- while this thread
  holds one stage ownership lock, no *other* stage root's lock is requested --
  by watching the acquisitions a real egress makes over a two-root fixture;
* the two-process one demonstrates the schedule with the production lock and
  the production reclamation code, two real egresses on two real roots, and
  fails by not finishing.

Neither mocks the locks or the reclaim.  The only instrumentation is a
rendezvous at the first ``release_refs`` call in each child, which makes the
interleaving deterministic: both children have read the shared pin document
before either mutates it.  Fixture-owned children only, bounded cleanup.

This is a reachable-schedule regression, not the RAM-promotion contention
observed on 2026-09-20; that lock was the ``/ram/prewarm`` destination root
under head promotion adoption and is attributed elsewhere.
"""
from __future__ import annotations

import contextlib
import json
import multiprocessing
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import stage_release  # noqa: E402

CONSUMER_A = "a" * 64
CONSUMER_B = "b" * 64
MOVER_A = "1" * 64
MOVER_B = "2" * 64
TIER_A = "prismabuild-stage:root-a"
TIER_B = "prismabuild-stage:root-b"
CONTAINED = {"nonce": "contained", "scope_id": "s1"}
LIVE = {"nonce": "live", "scope_id": "s1"}
HOLDER = {"host": "test-host", "pid": 4242}
DIGEST = "b" * 64
SIZE = 4096

#: How long a child waits for its peer to reach the same seam.
BARRIER_S = 60.0
#: How long the parent waits for both egresses to finish before calling it a
#: deadlock.  A completed pair takes well under a second.
FINISH_S = 60.0
#: Bounded cleanup of the fixture's own children.
REAP_S = 10.0


def _publish(root: Path, stage: Path, consumer: str, mover: str, tier: str,
             source: str, staged: Path) -> str:
    """One mover's published fragment plus the sidecar that dates it."""

    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": tier, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(source, 0): {
                "stage_path": str(staged), "bytes": SIZE,
                "sha256": DIGEST, "offset": 0,
            },
        },
    })
    generation = reader_lease.mint_generation()
    identity = reader_lease.stat_identity(str(staged))
    assert identity is not None
    reader_lease.write_material(
        root, consumer_action_key=consumer, mover_action_key=mover,
        tier_id=tier, stage_root=str(stage), manifest_sha256="a" * 64,
        generation=generation,
        entries={residency_map.residency_map_key(source, 0): {
            "stage_path": str(staged), "bytes": SIZE, "sha256": DIGEST,
            "file_id": identity}})
    return generation


def _acquire(queue, consumer: str, mover: str, tier: str, attempt: dict,
             token: str) -> dict:
    return reader_lease.acquire(
        queue, consumer_action_key=consumer, attempt=attempt, tier_id=tier,
        epoch="", span={"start_bytes": 0, "end_bytes": SIZE},
        holder=HOLDER, acquire_token=token,
        covers=[{"mover_action_key": mover, "manifest_sha256": "a" * 64}])


def _contain(queue, consumer: str, attempt: dict) -> None:
    """The exact containment evidence one attempt needs: proof + terminal.

    Fabricated the way the reader-lifetime suite fabricates them -- the
    broker attestation the pool writer files, and the terminal record whose
    telemetry names the same attempt.  Nothing here weakens the check that
    reads them; the other attempt on the same pin has neither and stays.
    """

    path = reader_lease.attestation_path(queue, consumer, attempt["nonce"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema": reader_lease.ATTESTATION_SCHEMA_V1,
        "action_key": consumer, "nonce": attempt["nonce"],
        "scope_id": attempt["scope_id"], "host": "test-host",
        "worker": "w1", "incarnation": "i1",
        "scope_empty": True, "released": True, "retired": False,
        "settled": False, "empty": True, "tickets_pending": False,
        "stopped_unix": 1789870000.0, "unix": 1789880000.0}) + "\n")
    done = queue.dir(pool.DONE)
    done.mkdir(parents=True, exist_ok=True)
    (done / f"{consumer}.json").write_text(json.dumps({
        "action_key": consumer, "status": "executed",
        "resource_telemetry": {
            "action_key": consumer, "nonce": attempt["nonce"],
            "scope_unit": attempt["scope_id"], "host": "test-host"}}))


def _root(queue, base: Path, name: str, tier: str, consumer: str,
          mover: str) -> dict[str, object]:
    """One registered stage root carrying one mover and one two-ref pin.

    The pin holds a contained ref (evidence filed) and a live ref (no
    evidence at all).  Reclaiming the first takes this root's ownership lock
    and leaves the pin file in place for the other party to read; the second
    keeps the egress deferring, which is what makes the egress reclaim.
    """

    stage = base / name
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=tier, stage_root=stage) == "registered"
    staged = stage / "model" / "shard.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x19" * SIZE)
    residency = queue.root / pool.RESIDENCY
    _publish(residency, stage, consumer, mover, tier,
             f"/mnt/shared/{name}/shard.bin", staged)
    held = _acquire(queue, consumer, mover, tier, CONTAINED, f"{name}-gone")
    assert held["ok"], held
    live = _acquire(queue, consumer, mover, tier, LIVE, f"{name}-live")
    assert live["ok"], live
    assert live["pin_id"] == held["pin_id"], (held, live)
    _contain(queue, consumer, CONTAINED)
    return {"stage": stage, "staged": staged, "tier": tier,
            "consumer": consumer, "mover": mover,
            "pin_id": held["pin_id"], "contained_ref": held["ref_id"],
            "live_ref": live["ref_id"]}


@pytest.fixture()
def two_roots(tmp_path: Path):
    """One queue, two registered stage roots, each pinned on both attempts."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    first = _root(queue, tmp_path, "stage-a", TIER_A, CONSUMER_A, MOVER_A)
    second = _root(queue, tmp_path, "stage-b", TIER_B, CONSUMER_B, MOVER_B)
    return queue, first, second


def _pin_refs(queue, side: dict[str, object]) -> set[str]:
    path = (queue.root / pool.RESIDENCY / reader_lease.LEASES_SUBDIR
            / str(side["consumer"]) / f"{side['pin_id']}.lease.json")
    pin = json.loads(path.read_text())
    refs = pin["refs"]
    assert isinstance(refs, dict)
    return set(refs)


class WatchingQueue(pool.PoolQueue):
    """A queue that records every nested stage-ownership acquisition.

    The lock itself is the production one: this only notes, per thread,
    which roots are held when another is requested.  ``cross_root`` names
    the pairs that make the cycle in #780.
    """

    def __init__(self, root) -> None:
        super().__init__(root)
        self.cross_root: list[tuple[str, str]] = []
        self._local = threading.local()

    @contextlib.contextmanager
    def stage_ownership_lock(self, stage_root, *, blocking: bool = True):
        key = str(Path(stage_root).absolute())
        held = getattr(self._local, "held", None)
        if held is None:
            held = self._local.held = []
        for other in held:
            if other != key:
                self.cross_root.append((other, key))
        with super().stage_ownership_lock(stage_root,
                                          blocking=blocking) as acquired:
            if not acquired:
                yield acquired
                return
            held.append(key)
            try:
                yield acquired
            finally:
                held.pop()


def test_egress_requests_no_second_stage_root_while_holding_one(
        two_roots) -> None:
    """One egress, two roots: nothing asks for B's lock while holding A's."""

    queue, first, second = two_roots
    watched = WatchingQueue(queue.root)
    receipt = stage_release.evict(
        watched, str(first["mover"]),
        consumer_action_key=str(first["consumer"]),
        stage_root=str(first["stage"]))

    assert watched.cross_root == [], watched.cross_root
    # Reclamation still reaches both roots -- the correction moves it out of
    # exclusion, it does not narrow what it may free -- and takes exactly the
    # contained ref on each.
    assert sorted(receipt["auto_reclaimed"]) == sorted(
        [str(first["contained_ref"]), str(second["contained_ref"])]), receipt
    assert _pin_refs(queue, first) == {first["live_ref"]}
    assert _pin_refs(queue, second) == {second["live_ref"]}
    # ...and the live ref still defers the delete, with the charge retained.
    assert receipt["entries_deferred"] == 1, receipt
    assert receipt["entries_deleted"] == 0, receipt
    assert receipt["live_pins"] == [first["pin_id"]], receipt
    assert receipt["retiring"] is True, receipt
    assert receipt["errors"] == [], receipt
    assert Path(str(first["staged"])).exists()


def _child_egress(pool_root: str, stage_root: str, consumer: str, mover: str,
                  arrived, peer, receipt_path: str) -> None:
    """One real egress, rendezvousing at its first containment release.

    The seam is deliberate: at the first ``release_refs`` the broken egress
    is already inside its own root's exclusion and the corrected one holds no
    stage root at all, so the peer's wait is what turns the difference into a
    pass or a hang.  Both children have read the shared pin document by then,
    so neither outcome depends on who won a read.
    """

    queue = pool.PoolQueue(Path(pool_root))
    original = reader_lease.release_refs
    state = {"first": True}

    def rendezvous(*args, **kwargs):
        if state["first"]:
            state["first"] = False
            arrived.set()
            peer.wait(timeout=BARRIER_S)
        return original(*args, **kwargs)

    reader_lease.release_refs = rendezvous
    receipt = stage_release.evict(queue, mover,
                                  consumer_action_key=consumer,
                                  stage_root=stage_root)
    Path(receipt_path).write_text(json.dumps(receipt) + "\n")


def _reap(children) -> None:
    """Terminate exactly the fixture's own children, bounded."""

    for child in children:
        if child.is_alive():
            child.terminate()
    for child in children:
        child.join(REAP_S)
    for child in children:
        if child.is_alive():
            child.kill()
            child.join(REAP_S)


def test_two_egresses_on_two_stage_roots_both_finish(two_roots,
                                                     tmp_path: Path) -> None:
    """A holds A and reclaims B while B holds B and reclaims A: no cycle."""

    queue, first, second = two_roots
    context = multiprocessing.get_context("fork")
    ready_a, ready_b = context.Event(), context.Event()
    receipt_a = tmp_path / "receipt-a.json"
    receipt_b = tmp_path / "receipt-b.json"
    children = [
        context.Process(
            target=_child_egress,
            args=(str(queue.root), str(first["stage"]),
                  str(first["consumer"]), str(first["mover"]),
                  ready_a, ready_b, str(receipt_a))),
        context.Process(
            target=_child_egress,
            args=(str(queue.root), str(second["stage"]),
                  str(second["consumer"]), str(second["mover"]),
                  ready_b, ready_a, str(receipt_b))),
    ]
    for child in children:
        child.start()
    deadline = time.monotonic() + FINISH_S
    for child in children:
        child.join(max(0.0, deadline - time.monotonic()))
    stuck = [child.pid for child in children if child.is_alive()]
    _reap(children)

    assert ready_a.is_set() and ready_b.is_set(), (
        "an egress never reached containment reclamation")
    assert not stuck, (
        f"egresses {stuck} never finished: each holds one stage root and "
        f"waits for the other's")
    assert [child.exitcode for child in children] == [0, 0], children

    for side, path in ((first, receipt_a), (second, receipt_b)):
        receipt = json.loads(path.read_text())
        assert receipt["errors"] == [], receipt
        assert receipt["entries_deferred"] == 1, receipt
        assert receipt["entries_deleted"] == 0, receipt
        assert receipt["live_pins"] == [side["pin_id"]], receipt
        assert Path(str(side["staged"])).exists()
        assert _pin_refs(queue, side) == {side["live_ref"]}
