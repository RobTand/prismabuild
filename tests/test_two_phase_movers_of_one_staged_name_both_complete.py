"""Two phase movers of one staged name both complete (#1151).

Stage B's spill phases each declare the same exact-boundary files, because
every probe replays the same boundaries, so the phase movers publish the same
staged names.  On 2026-09-25 two of them overlapped: spill-p0's mover
``574747d717fc`` ended ``complete: false`` at 436 of 512 entries and
spill-p1's ``6444f5c4d8de`` at 511 of 512, each on one name the other had
renamed: "shared staged name still has a live publisher after the grace".

A mover republishes its fragment at most once per ``FRAGMENT_PUBLISH_S``, and
a landing inside that interval was owed to the next landing's publication.
When a copy's landings stopped -- its queue drained behind a straggler, or
its readers held by the pacer (``pace_wait`` peaked at 28.8 s) -- its last
renamed names stayed unvouched until its whole range ended.  The other mover
read each as a live publisher's pending name, waited out
``_PUBLISH_GRACE_S`` and refused.

With the fix the rate limit has a trailing edge: what it declined is
published once it allows, whatever the copy does next, and the waiting mover
adopts the name through the publication gate's proof.

The fixtures drive the real ``stage_move.move`` over tiny real files, with
real sealed claims for the publication gate's live-claim census.  The first
mover is held on its last entry for longer than the grace, the shape of a
pacer hold.  The grace and the rate limit are shortened in proportion: the
rate limit stays well inside the grace, as 5 s is inside 30 s.  Runs under
pbtest at priority -10; never executed locally.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import stage_move  # noqa: E402

TIER = "prismabuild-stage:testbox"
STAGE_KIND = f"stage_gib@{TIER}"
#: One share namespace per phase, as Stage B seals them (#1026).
CONSUMER_P0 = "c" * 64
CONSUMER_P1 = "d" * 64
MOVER_P0 = "1" * 64
MOVER_P1 = "2" * 64
N = 3
SIZE = 16 * 1024
PHASE_BYTES = N * SIZE
#: Shortened for the test; the rate limit stays a fraction of the grace.
GRACE = 4.0
FRAGMENT_S = 1.0


def _payload(index: int) -> bytes:
    return bytes((position * 11 + index * 17) % 251 + 1
                 for position in range(SIZE))


def _manifest(tmp_path: Path) -> tuple[Path, str, list[Path]]:
    """Two phases that read the same ``N`` boundary files, in the same order."""

    origin = tmp_path / "origin"
    origin.mkdir()
    paths = []
    entries = []
    for index in range(N):
        path = origin / f"boundary-{index}.pt"
        path.write_bytes(_payload(index))
        paths.append(path)
        entries.append({"path": str(path), "offset": 0, "bytes": SIZE,
                        "sha256": hashlib.sha256(_payload(index)).hexdigest()})
    indices = list(range(N))
    body = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V2,
        "produced_by": {"tool": "two-phase-movers-fixture"},
        "annotations": {},
        "mount_prefix": str(origin),
        "entries": entries,
        "entry_count": N,
        "total_bytes": PHASE_BYTES,
        "read_plan": {"phases": [
            {"name": "spill-p0", "entry_indices": indices,
             "bytes": PHASE_BYTES, "cumulative_bytes": PHASE_BYTES},
            {"name": "spill-p1", "entry_indices": indices,
             "bytes": PHASE_BYTES, "cumulative_bytes": 2 * PHASE_BYTES},
        ], "read_bytes": 2 * PHASE_BYTES},
    }
    blob = json.dumps(body).encode()
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(blob)
    digest = hashlib.sha256(blob).hexdigest()
    cas_blob = pb.PrismaBuildCAS(tmp_path / "cas").blob_path(digest)
    cas_blob.parent.mkdir(parents=True, exist_ok=True)
    cas_blob.write_bytes(blob)
    return manifest, digest, paths


def _seal_claim(queue: pool.PoolQueue, cas: Path, mover: str, digest: str,
                start: int, end: int) -> None:
    """A claimed phase mover: its sealed request names its manifest and range.

    What the publication gate's claim census reads
    (``stage_release._claimed_paths``) to see an in-flight copy.
    """

    request = {
        "action_key": mover,
        "params": {"command": [
            "python3", "stage_move.py",
            "--range-start-bytes", str(start),
            "--range-end-bytes", str(end)]},
        "inputs": [{"id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                    "sha256": digest}],
    }
    path = cas / "requests" / mover[:2] / f"{mover}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(request))
    queue.item_path(pool.CLAIMED, mover).write_text(json.dumps({
        "action_key": mover, "cas_root": str(cas),
        "resources": {"cpu": 2, "mem_gb": 1, STAGE_KIND: 1}}))


def _conclude(queue: pool.PoolQueue, mover: str) -> None:
    queue.item_path(pool.CLAIMED, mover).unlink()


def _args(tmp_path: Path, queue: pool.PoolQueue, manifest: Path, digest: str,
          consumer: str, mover: str, start: int, end: int):
    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", TIER,
        "--stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", digest,
        "--range-start-bytes", str(start),
        "--range-end-bytes", str(end),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", "4096",
        "--readers", "1",
        "--max-readers", "1",
        "--unpaced",
    ])


def _staged(tmp_path: Path, origin: Path) -> Path:
    return tmp_path / "stage" / stage_move.stage_relative(
        str(origin), 0, SIZE, mount_prefix=str(origin.parent))


def _hold(monkeypatch, mover: str, path: Path):
    """Hold ``mover`` before it copies ``path`` until released.

    The shape of a pacer hold: the copy is alive and claimed, and it lands
    nothing while held.  Returns ``(held, release)`` events.
    """

    held, release = threading.Event(), threading.Event()
    real = stage_move._Copier._copy_one

    def copy_one(self, entry, destination, *args, **kwargs):
        if self.owner == mover and str(entry["path"]) == str(path):
            held.set()
            assert release.wait(60), "the fixture never released the hold"
        return real(self, entry, destination, *args, **kwargs)

    monkeypatch.setattr(stage_move._Copier, "_copy_one", copy_one)
    return held, release


def _fixture(tmp_path: Path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    (tmp_path / "stage").mkdir()
    manifest, digest, paths = _manifest(tmp_path)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)
    monkeypatch.setattr(stage_move, "FRAGMENT_PUBLISH_S", FRAGMENT_S)
    return queue, manifest, digest, paths


def _vouched(queue: pool.PoolQueue, consumer: str, mover: str) -> set[str]:
    path = residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover)
    try:
        fragment = json.loads(path.read_bytes())
    except (OSError, ValueError):
        return set()
    return {os.path.normpath(str(entry["stage_path"]))
            for entry in fragment["entries"].values()}


def test_a_held_first_publisher_does_not_fail_its_sibling(
        tmp_path: Path, monkeypatch) -> None:
    """RED before #1151: the second mover waits out the grace and refuses."""

    queue, manifest, digest, paths = _fixture(tmp_path, monkeypatch)
    cas = tmp_path / "cas"
    _seal_claim(queue, cas, MOVER_P0, digest, 0, PHASE_BYTES)
    _seal_claim(queue, cas, MOVER_P1, digest, PHASE_BYTES, 2 * PHASE_BYTES)
    held, release = _hold(monkeypatch, MOVER_P0, paths[-1])

    first: dict[str, object] = {}

    def run_first() -> None:
        first["receipt"] = stage_move.move(_args(
            tmp_path, queue, manifest, digest, CONSUMER_P0, MOVER_P0,
            0, PHASE_BYTES))

    thread = threading.Thread(target=run_first, daemon=True)
    thread.start()
    try:
        assert held.wait(30), "the first mover never reached its last entry"
        # The first mover renamed every name but the last and is held; the
        # second walks the same names.
        renamed = [_staged(tmp_path, path) for path in paths[:-1]]
        before = [reader_lease.stat_identity(str(path)) for path in renamed]
        assert all(before), before

        started = time.monotonic()
        second = stage_move.move(_args(
            tmp_path, queue, manifest, digest, CONSUMER_P1, MOVER_P1,
            PHASE_BYTES, 2 * PHASE_BYTES))
        elapsed = time.monotonic() - started
        _conclude(queue, MOVER_P1)
    finally:
        release.set()
        thread.join(60)
    assert not thread.is_alive()
    _conclude(queue, MOVER_P0)
    receipt = first["receipt"]
    assert isinstance(receipt, dict)

    assert second["complete"] is True, (
        f"the second phase mover failed on a name the first had renamed and "
        f"not yet vouched, after {elapsed:.1f} s against a {GRACE} s grace: "
        f"{second['errors']}")
    assert second["errors"] == [], second["errors"]
    assert receipt["complete"] is True, receipt["errors"]
    assert receipt["errors"] == [], receipt["errors"]
    assert elapsed < GRACE, (
        f"the second mover waited out the grace: {elapsed:.1f} s")
    # Nothing was replaced: the names the first mover renamed kept their
    # inodes, and the second adopted them rather than copying over them.
    assert [reader_lease.stat_identity(str(path))
            for path in renamed] == before
    # Before the copy or at its publication, depending on whether the vouch
    # had landed when the second mover reached the name; never a rename.
    outcomes = second["phase_timings"]["outcomes"]
    assert outcomes.get("renamed") == 1, second["phase_timings"]
    assert (outcomes.get("adopted", 0)
            + outcomes.get("adopted_at_publication", 0)) == N - 1, (
        second["phase_timings"])
    assert receipt["phase_timings"]["outcomes"] == {
        "renamed": 2, "adopted": 1}, receipt["phase_timings"]
    # The debt was paid by the trailing edge while the first mover was held.
    assert "fragment_trailing_publication" in (
        receipt["phase_timings"]["thread_seconds"]), receipt["phase_timings"]
    names = {os.path.normpath(str(_staged(tmp_path, path))) for path in paths}
    assert _vouched(queue, CONSUMER_P0, MOVER_P0) == names
    assert _vouched(queue, CONSUMER_P1, MOVER_P1) == names


def test_a_landed_name_is_vouched_while_the_copy_lands_nothing(
        tmp_path: Path, monkeypatch) -> None:
    """RED before #1151: the name stayed unvouched until the range ended.

    The invariant under the collision: a renamed name is vouched within
    about ``FRAGMENT_PUBLISH_S`` whatever the copy does next.  And the
    trailing edge adds no writes of its own: while nothing is owed, nothing
    is written.
    """

    queue, manifest, digest, paths = _fixture(tmp_path, monkeypatch)
    _seal_claim(queue, tmp_path / "cas", MOVER_P0, digest, 0, PHASE_BYTES)
    held, release = _hold(monkeypatch, MOVER_P0, paths[-1])
    writes: list[float] = []
    real_write = residency_map.write_fragment

    def counted(*args, **kwargs):
        writes.append(time.monotonic())
        return real_write(*args, **kwargs)

    monkeypatch.setattr(residency_map, "write_fragment", counted)
    first: dict[str, object] = {}

    def run_first() -> None:
        first["receipt"] = stage_move.move(_args(
            tmp_path, queue, manifest, digest, CONSUMER_P0, MOVER_P0,
            0, PHASE_BYTES))

    thread = threading.Thread(target=run_first, daemon=True)
    thread.start()
    landed = {os.path.normpath(str(_staged(tmp_path, path)))
              for path in paths[:-1]}
    try:
        assert held.wait(30), "the mover never reached its last entry"
        held_at = time.monotonic()
        deadline = held_at + 3 * FRAGMENT_S
        vouched: set[str] = set()
        while time.monotonic() < deadline:
            vouched = _vouched(queue, CONSUMER_P0, MOVER_P0)
            if vouched == landed:
                break
            time.sleep(0.05)
        waited = time.monotonic() - held_at
        # Hold on past the vouch for a few more ticks: nothing owed, so
        # nothing more may be written.
        time.sleep(FRAGMENT_S)
        during_hold = len(writes)
    finally:
        release.set()
        thread.join(60)
    assert not thread.is_alive()
    _conclude(queue, MOVER_P0)

    assert vouched == landed, (
        f"{len(landed - vouched)} renamed name(s) still unvouched "
        f"{waited:.1f} s into a hold, against a {FRAGMENT_S} s rate limit")
    assert waited <= 2 * FRAGMENT_S, waited
    # The first landing's publication, then the trailing one; no more.
    assert during_hold == 2, (
        f"{during_hold} fragment writes during the hold; the trailing edge "
        f"must write only what is owed")
    receipt = first["receipt"]
    assert isinstance(receipt, dict)
    assert receipt["complete"] is True, receipt["errors"]
