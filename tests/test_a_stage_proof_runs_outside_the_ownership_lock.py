"""A staged-publication proof runs outside the stage ownership lock (#981).

The measured defect, 2026-09-23 on dl380g10: every stage mover on the host
proved each staged entry inside ``queue.stage_ownership_lock`` -- one lock for
the whole stage root -- so adoption ran at about 100 ms per entry whatever the
entry's size, a py-spy dump found 16 of a mover's 17 threads parked on the
lock's per-process ``RLock`` and the 17th in ``fcntl.lockf`` behind another
mover, and both GB10 GPUs sat idle waiting for the stage.  The hermetic
profile (``tools/fleet/bench_stage_adopt.py``) put 94% of every adopting
worker's time in that wait, and showed a copy mover running beside two
adopters slowed 45-fold -- its renames queued behind their proofs.

The rule these tests hold: the lock guards the ownership decision and the
rename, never the proof.  A proof is a read of publication metadata and a
live stat, and it acts on nothing; what acts is the commit, and the commit
re-establishes, under the lock, that the destination still carries the exact
incarnation the proof dated.

* ``test_a_proof_holds_no_stage_ownership_lock`` and
  ``test_copy_workers_prove_concurrently`` are the RED cases: on the pre-fix
  tree the lock is held across the whole proof, so another thread cannot take
  it and four workers can never be inside a proof at once.
* ``test_a_fresh_copy_publishes_while_another_proof_is_in_flight`` is the
  copy mover's idle tail: a rename that needs the lock for microseconds must
  not wait out somebody else's proof.
* the race cases hold the safety the narrower lock relies on: two movers
  racing one destination leave exactly one owner, a racer with other bytes is
  refused rather than adopted, and an incarnation that moves between the proof
  and the commit is never adopted.

Every case synchronizes on events and barriers with timeouts that only bound
a failure; none asserts a wall-clock duration.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
SIZE = 4096
#: How long a synchronization may take before the case calls it a failure.
#: Bounds a broken run; a passing run never waits it out.
BOUND_S = 10.0


def _key(seed: str) -> str:
    return hashlib.sha256(f"981:{seed}".encode()).hexdigest()


def _payload(seed: str) -> bytes:
    return (hashlib.sha256(seed.encode()).digest() * (SIZE // 32 + 1))[:SIZE]


class _World:
    """A real queue, a registered stage root and real source files."""

    def __init__(self, tmp_path: Path) -> None:
        self.queue = pool.PoolQueue(tmp_path / "pb-queue")
        self.queue.ensure_layout()
        self.stage = tmp_path / "stage"
        self.stage.mkdir()
        assert stage_release.register_stage_root(
            self.queue, tier_id=TIER, stage_root=self.stage) == "registered"
        self.cas = tmp_path / "cas"
        self.cas.mkdir()
        self.mount = tmp_path / "sources"
        self.mount.mkdir()
        self.root = self.queue.residency_fragment_root()

    def entry(self, name: str, payload: bytes) -> dict[str, object]:
        source = self.mount / name
        source.write_bytes(payload)
        return {"path": str(source), "offset": 0, "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest()}

    def destination(self, entry: dict[str, object]) -> Path:
        path = self.stage / stage_move.stage_relative(
            str(entry["path"]), 0, int(entry["bytes"]),
            mount_prefix=str(self.mount))
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def publisher(self, mover: str, consumer: str) -> stage_move._StagedPublisher:
        return stage_move._StagedPublisher(
            queue=self.queue, stage_root=self.stage, residency_root=self.root,
            mover_action_key=mover, manifest_sha256="a" * 64, tier_id=TIER,
            cas_root=str(self.cas), consumer_action_key=consumer)

    def vouch(self, consumer: str, mover: str,
              entries: list[dict[str, object]]) -> None:
        """File the fragment and the sidecar a finished copy leaves.

        The fragment first, then the sidecar dating it, the order
        ``stage_move.move``'s publication writes them in.
        """

        named, dated = {}, {}
        for entry in entries:
            destination = self.destination(entry)
            key = residency_map.residency_map_key(str(entry["path"]), 0)
            named[key] = {"stage_path": str(destination),
                          "bytes": int(entry["bytes"]), "offset": 0,
                          "sha256": str(entry["sha256"])}
            dated[key] = {"stage_path": str(destination),
                          "bytes": int(entry["bytes"]),
                          "sha256": str(entry["sha256"]),
                          "file_id": reader_lease.stat_identity(str(destination))}
        residency_map.write_fragment(self.root, {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": consumer, "mover_action_key": mover,
            "tier_id": TIER, "stage_root": str(self.stage),
            "manifest_sha256": "a" * 64, "entries": named})
        reader_lease.write_material(
            self.root, consumer_action_key=consumer, mover_action_key=mover,
            tier_id=TIER, stage_root=str(self.stage), manifest_sha256="a" * 64,
            generation="b" * 32, entries=dated)

    def published(self, names: list[str]) -> list[dict[str, object]]:
        """Entries whose bytes a finished predecessor already staged."""

        entries = []
        for name in names:
            payload = _payload(name)
            entry = self.entry(name, payload)
            self.destination(entry).write_bytes(payload)
            entries.append(entry)
        self.vouch(_key("predecessor-consumer"), _key("predecessor-mover"),
                   entries)
        return entries


def _temporary(world: _World, entry, owner: str, payload: bytes) -> Path:
    """A verified private copy beside the destination, as ``_copy_one`` leaves it."""

    destination = world.destination(entry)
    temporary = destination.with_name(f".{destination.name}.{owner[:16]}.partial")
    temporary.write_bytes(payload)
    return temporary


@pytest.fixture()
def world(tmp_path: Path) -> _World:
    return _World(tmp_path)


# ---------------------------------------------------------------------------
# RED on the pre-fix tree
# ---------------------------------------------------------------------------

def test_a_proof_holds_no_stage_ownership_lock(world, monkeypatch) -> None:
    """While an adoption proves, the stage ownership lock is free.

    Another thread asks for the lock without blocking from inside the proof.
    On the pre-fix tree the proving thread holds it, so the ask is refused;
    after the fix the lock is free, and the adoption still lands.
    """

    [entry] = world.published(["one.bin"])
    publisher = world.publisher(_key("successor-mover"),
                                _key("successor-consumer"))
    real = publisher._proof_search
    answers: list[bool] = []

    def observed(*args, **kwargs):
        def ask() -> None:
            with world.queue.stage_ownership_lock(
                    str(world.stage), blocking=False) as acquired:
                answers.append(bool(acquired))

        asker = threading.Thread(target=ask)
        asker.start()
        asker.join(BOUND_S)
        return real(*args, **kwargs)

    monkeypatch.setattr(publisher, "_proof_search", observed)
    adopted = publisher.try_adopt(entry, world.destination(entry),
                                  stage_move._origin_id_of(str(entry["path"])))

    assert answers == [True], (
        f"the stage ownership lock was held while proving: {answers}")
    assert adopted is not None and adopted[1] == entry["sha256"], adopted
    assert reader_lease.file_id_matches(
        adopted[2], reader_lease.stat_identity(str(world.destination(entry))))


def test_copy_workers_prove_concurrently(world, monkeypatch) -> None:
    """Four copy workers can all be inside a proof at the same time.

    The first four proofs wait on one barrier.  Under one lock around the
    proof the barrier can never fill -- one worker proves, three wait on the
    lock -- so it breaks at its timeout; with the proof outside the lock it
    fills at once and every entry is adopted.
    """

    workers = 4
    entries = world.published([f"entry-{n:02d}.bin" for n in range(12)])
    before = [os.stat(world.destination(e)).st_ino for e in entries]
    publisher = world.publisher(_key("successor-mover"),
                                _key("successor-consumer"))
    barrier = threading.Barrier(workers, timeout=BOUND_S)
    met: list[bool] = []
    gate = threading.Lock()
    calls = [0]
    real = publisher._proof_search

    def meeting(*args, **kwargs):
        with gate:
            calls[0] += 1
            first = calls[0] <= workers
        if first:
            try:
                barrier.wait()
                met.append(True)
            except threading.BrokenBarrierError:
                met.append(False)
        return real(*args, **kwargs)

    monkeypatch.setattr(publisher, "_proof_search", meeting)
    copier = stage_move._Copier(
        mounts=prewarm_loop.MountMap([f"{world.mount}={world.mount}"]),
        pacer=None, stage_root=world.stage, mount_prefix=str(world.mount),
        block=1 << 16, workers=workers, owner=_key("successor-mover"),
        publisher=publisher)
    copier.run(entries, stop=threading.Event())

    assert met == [True] * workers, (
        f"proofs were serialized: {met.count(False)} of {workers} workers "
        f"could not be inside a proof together")
    assert copier.errors == [], copier.errors
    assert len(copier.staged) == len(entries)
    assert [os.stat(world.destination(e)).st_ino for e in entries] == before, (
        "adoption must keep the published incarnation, never replace it")


def test_a_fresh_copy_publishes_while_another_proof_is_in_flight(
        world, monkeypatch) -> None:
    """A rename is not queued behind another entry's proof.

    One thread's adoption proof is held open.  Meanwhile a second thread
    publishes a fresh copy of a different entry, which needs the lock only
    for its rename.  Pre-fix, the held proof holds the stage ownership lock
    and the rename cannot land until it ends -- the 45-fold slowdown of the
    ``mixed`` profile; after the fix it lands while the proof is still open.
    """

    [held_entry] = world.published(["held.bin"])
    fresh = world.entry("fresh.bin", _payload("fresh.bin"))
    mover = _key("successor-mover")
    publisher = world.publisher(mover, _key("successor-consumer"))
    inside, release = threading.Event(), threading.Event()
    real = publisher._proof_search

    def held(norm, *args, **kwargs):
        if norm == os.path.normpath(str(world.destination(held_entry))):
            inside.set()
            release.wait(BOUND_S)
        return real(norm, *args, **kwargs)

    monkeypatch.setattr(publisher, "_proof_search", held)
    adopter = threading.Thread(
        target=publisher.try_adopt,
        args=(held_entry, world.destination(held_entry), None))
    adopter.start()
    try:
        assert inside.wait(BOUND_S), "the held proof never started"
        landed: list[object] = []
        temporary = _temporary(world, fresh, mover, _payload("fresh.bin"))

        def publish_fresh() -> None:
            landed.append(publisher.publish(
                fresh, world.destination(fresh), temporary,
                str(fresh["sha256"])))

        publishing = threading.Thread(target=publish_fresh)
        publishing.start()
        publishing.join(BOUND_S)
        still_open = not release.is_set() and adopter.is_alive()
        assert landed, ("a fresh rename waited behind another entry's proof "
                        "for the stage ownership lock")
        assert still_open, "the held proof ended before the rename was judged"
        assert world.destination(fresh).read_bytes() == _payload("fresh.bin")
    finally:
        release.set()
        adopter.join(BOUND_S)


# ---------------------------------------------------------------------------
# The safety the narrower lock relies on
# ---------------------------------------------------------------------------

def _race(world: _World, monkeypatch, payloads: dict[str, bytes]):
    """Two movers publish one staged name at once; report what each did.

    Each mover files its fragment and sidecar right after its own
    publication returns, as ``stage_move.move``'s incremental publication
    does, so the loser can find the winner's proof.  ``os.replace`` onto the
    destination is counted: that is what "one owner" means on disk.
    """

    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.01)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", BOUND_S)
    name = "shared.bin"
    probe = world.entry(name, payloads["a"])
    destination = world.destination(probe)
    real_replace = os.replace
    replaced: list[str] = []
    gate = threading.Lock()

    def counting(src, dst, *args, **kwargs):
        if os.path.normpath(str(dst)) == os.path.normpath(str(destination)):
            with gate:
                replaced.append(str(src))
        return real_replace(src, dst, *args, **kwargs)

    monkeypatch.setattr(stage_move.os, "replace", counting)
    barrier = threading.Barrier(2, timeout=BOUND_S)
    outcomes: dict[str, object] = {}

    def racer(label: str) -> None:
        mover, consumer = _key(f"{label}-mover"), _key(f"{label}-consumer")
        payload = payloads[label]
        entry = {**probe, "sha256": hashlib.sha256(payload).hexdigest()}
        publisher = world.publisher(mover, consumer)
        temporary = _temporary(world, entry, mover, payload)
        barrier.wait()
        try:
            result = publisher.publish(entry, destination, temporary,
                                       str(entry["sha256"]))
        except stage_move._PublicationRefused as exc:
            outcomes[label] = exc
            return
        outcomes[label] = result
        world.vouch(consumer, mover, [entry])
        outcomes[f"{label}-temporary-gone"] = not temporary.exists()

    threads = [threading.Thread(target=racer, args=(label,))
               for label in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(3 * BOUND_S)
    assert not any(thread.is_alive() for thread in threads), "a racer hung"
    return destination, replaced, outcomes


def test_two_movers_racing_one_destination_leave_exactly_one_owner(
        world, monkeypatch) -> None:
    """Same bytes, one name, two movers at once: one rename, one adoption."""

    payload = _payload("shared")
    destination, replaced, outcomes = _race(
        world, monkeypatch, {"a": payload, "b": payload})

    assert len(replaced) == 1, f"{len(replaced)} renames onto one name"
    live = reader_lease.stat_identity(str(destination))
    for label in ("a", "b"):
        result = outcomes[label]
        assert isinstance(result, tuple), f"{label}: {result!r}"
        assert result[1] == hashlib.sha256(payload).hexdigest()
        assert reader_lease.file_id_matches(result[2], live), (
            f"{label} names an incarnation that is not the one on disk")
        assert outcomes[f"{label}-temporary-gone"], (
            f"{label} left its private copy behind")
    assert destination.read_bytes() == payload


def test_a_racing_mover_with_other_bytes_is_refused_not_adopted(
        world, monkeypatch) -> None:
    """Other bytes under the same name: the loser refuses, the winner stands."""

    destination, replaced, outcomes = _race(
        world, monkeypatch, {"a": _payload("first"), "b": _payload("second")})

    assert len(replaced) == 1, f"{len(replaced)} renames onto one name"
    won = [label for label in ("a", "b") if isinstance(outcomes[label], tuple)]
    lost = [label for label in ("a", "b")
            if isinstance(outcomes[label], stage_move._PublicationRefused)]
    assert len(won) == 1 and len(lost) == 1, outcomes
    winner = outcomes[won[0]]
    assert destination.read_bytes() == _payload(
        "first" if won[0] == "a" else "second"), (
        "the refused racer's bytes reached the published name")
    assert reader_lease.file_id_matches(
        winner[2], reader_lease.stat_identity(str(destination))), (  # type: ignore[index]
        "the winner's incarnation was replaced")
    assert "different bytes" in str(outcomes[lost[0]]), outcomes[lost[0]]


def test_an_incarnation_that_moves_after_the_proof_is_not_adopted(
        world, monkeypatch) -> None:
    """The commit re-checks the proven identity; a moved name is re-decided.

    The proof completes for the published file; before the commit takes the
    lock, the name is replaced by a new inode carrying the same bytes, which
    no record dates.  Adopting the proof would vouch for an incarnation that
    is no longer there.  The commit must see the move and decide again --
    here, nothing proves the new incarnation, so the copy proceeds.
    """

    [entry] = world.published(["moved.bin"])
    destination = world.destination(entry)
    publisher = world.publisher(_key("successor-mover"),
                                _key("successor-consumer"))
    real = publisher._proof_search
    moved = [False]

    def then_move(*args, **kwargs):
        answer = real(*args, **kwargs)
        if not moved[0]:
            moved[0] = True
            later = destination.with_name(destination.name + ".later")
            later.write_bytes(destination.read_bytes())
            os.replace(later, destination)
        return answer

    monkeypatch.setattr(publisher, "_proof_search", then_move)
    before = reader_lease.stat_identity(str(destination))
    assert before is not None
    adopted = publisher.try_adopt(entry, destination, None)

    assert moved[0], "the proof never ran"
    assert adopted is None, (
        f"adopted an incarnation that moved after its proof: {adopted}")
