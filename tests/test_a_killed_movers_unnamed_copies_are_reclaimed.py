"""A killed mover's copies no fragment names are reclaimed and sized (#1088).

Both movers mark every copy with the prewarm loop's ``user.pbstage.source``
before they rename it into place, and ``stage_release.reconcile`` read that
mark as "a prewarm object": counted in ``unowned_left``, never deleted.  A
RAM promotion files its only fragment at the end of its range, so one killed
before then leaves every copy it renamed named by nothing; a stage mover
leaves the copies it renamed after its last fragment publish.  With no retry
and a superseded plan nothing adopts them either -- the dead-owner sweep
deletes by fragment names and the prewarm loop by its own records -- so the
room stayed lost, and the receipt counted the files without their bytes,
mixed in with the prewarm loop's objects.

With the fix each mover also names itself on its copy
(``stage_move.STAGE_MOVER_XATTR``).  The reconciliation deletes such a copy
once no fragment, live pin or live promotion names it and its writer has
ended, and it sizes what it deleted and what it left by kind.  A copy whose
writer is still wanted or queued stays, a genuine prewarm object stays, and
so does a copy marked before the fix, which no attribute tells from one.

The fixtures drive the real ``stage_move.move``, ``ram_promote.promote`` and
``stage_release.sweep``/``reconcile`` over tiny real files.
"""
from __future__ import annotations

import errno
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import prismabuild.core as pb  # noqa: E402
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import ram_promote  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

STAGE_TIER = "prismabuild-stage:testbox"
RAM_TIER = "ram:testbox"
CONSUMER = "a" * 64
STAGE_MOVER = "1" * 64
RAM_MOVER = "e" * 64
#: The mover the superseding plan names instead of the killed promotion.
SUCCESSOR = "5" * 64
N = 4
SIZE = 16 * 1024
TOTAL = N * SIZE


class _Killed(BaseException):
    """The promotion's process ending where a SIGKILL would end it."""


def _payload(index: int) -> bytes:
    return bytes((position * 7 + index * 13) % 251 + 1
                 for position in range(SIZE))


def _manifest(tmp_path: Path) -> tuple[Path, str, dict]:
    origin = tmp_path / "origin"
    origin.mkdir(parents=True, exist_ok=True)
    entries = []
    for index in range(N):
        path = origin / f"shard-{index}.bin"
        path.write_bytes(_payload(index))
        entries.append({"path": str(path), "offset": 0, "bytes": SIZE,
                        "sha256": hashlib.sha256(_payload(index)).hexdigest()})
    body = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "killed-mover-residue-fixture"},
        "mount_prefix": str(origin),
        "entries": entries,
        "entry_count": N,
        "total_bytes": TOTAL,
        "annotations": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(body))
    return path, hashlib.sha256(path.read_bytes()).hexdigest(), body


class _World:
    """A queue, a registered stage and ram root, and one staged range."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.queue = pool.PoolQueue(tmp_path / "pb-queue")
        self.queue.ensure_layout()
        self.residence = self.queue.root / pool.RESIDENCY
        self.stage = tmp_path / "stage"
        self.ram = tmp_path / "ram"
        self.manifest, self.manifest_sha, self.body = _manifest(tmp_path)
        staged = stage_move.move(stage_move.build_parser().parse_args([
            "--pool-root", str(self.queue.root),
            "--cas-root", str(tmp_path / "cas"),
            "--action-key", STAGE_MOVER,
            "--consumer-action-key", CONSUMER,
            "--tier-id", STAGE_TIER,
            "--stage-root", str(self.stage),
            "--manifest-sha256", self.manifest_sha,
            "--range-start-bytes", "0",
            "--range-end-bytes", str(TOTAL),
            "--manifest", str(self.manifest),
            "--residency-root", str(self.residence),
            "--block", "4096",
            "--readers", "1",
            "--max-readers", "1",
            "--unpaced",
        ]))
        assert staged["complete"] is True, staged
        self.ram.mkdir()
        assert storage_tiers.ensure_ram_epoch(self.ram, host="testbox")
        for tier, root in ((STAGE_TIER, self.stage), (RAM_TIER, self.ram)):
            assert stage_release.register_stage_root(
                self.queue, tier_id=tier, stage_root=root) == "registered"

    def paths(self, root: Path) -> list[Path]:
        return [root / stage_move.stage_relative(
                    str(entry["path"]), 0, SIZE,
                    mount_prefix=str(self.body["mount_prefix"]))
                for entry in self.body["entries"]]

    def kill_a_promotion(self, monkeypatch) -> list[Path]:
        """A real promotion of the whole range, killed after every rename.

        ``ram_promote`` writes its material sidecar and then its fragment,
        once, at the end of the range; the kill lands on the first of the
        two, so every copy is renamed into place and no document names any.
        """

        args = ram_promote.build_parser().parse_args([
            "--pool-root", str(self.queue.root),
            "--cas-root", str(self.tmp / "cas"),
            "--action-key", RAM_MOVER,
            "--consumer-action-key", CONSUMER,
            "--tier-id", RAM_TIER,
            "--ram-root", str(self.ram),
            "--source-stage-root", str(self.stage),
            "--manifest-sha256", self.manifest_sha,
            "--range-start-bytes", "0",
            "--range-end-bytes", str(TOTAL),
            "--manifest", str(self.manifest),
            "--residency-root", str(self.residence),
            "--block", "4096",
            "--readers", "1",
            "--max-readers", "1",
        ])

        def kill(*_args, **_kwargs):
            raise _Killed()

        with monkeypatch.context() as patch:
            patch.setattr(ram_promote.reader_lease, "write_material", kill)
            with pytest.raises(_Killed):
                ram_promote.promote(args)
        copies = self.paths(self.ram)
        assert all(path.read_bytes() == _payload(index)
                   for index, path in enumerate(copies))
        assert not residency_map.fragment_path(
            self.residence, CONSUMER, RAM_MOVER).exists()
        return copies

    def forget_stage_fragment(self, *, keep: int = 0) -> None:
        """What a stage mover killed after its last fragment publish leaves.

        The fragment it published names only the first ``keep`` entries;
        every later copy was renamed into place and is named by nothing.
        """

        path = residency_map.fragment_path(self.residence, CONSUMER,
                                           STAGE_MOVER)
        if keep == 0:
            path.unlink()
            return
        fragment = json.loads(path.read_text())
        named = {os.path.normpath(str(one))
                 for one in self.paths(self.stage)[:keep]}
        fragment["entries"] = {
            key: entry for key, entry in fragment["entries"].items()
            if os.path.normpath(str(entry["stage_path"])) in named}
        assert len(fragment["entries"]) == keep
        residency_map.write_fragment(self.residence, fragment)

    def live_consumer(self, *, leads: list[str]) -> None:
        """The consumer, still queued, under a plan that names ``leads``."""

        self.queue.publish(
            action_key=CONSUMER, cas_root=str(self.tmp / "cas"),
            checkout_root=str(self.queue.root), worker_script="w.py",
            tags=["testbox"], resources={"cpu": 1, "mem_gb": 1},
            residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                       "manifest_sha256": self.manifest_sha,
                       "manifest_bytes": TOTAL, "tier_id": STAGE_TIER,
                       "leads": leads})

    def sweep(self) -> list[dict[str, object]]:
        return stage_release.sweep(self.queue, stage_roots={
            STAGE_TIER: str(self.stage), RAM_TIER: str(self.ram)})


def _reconciled(events: list[dict[str, object]], tier: str) -> dict:
    found = [event for event in events
             if event.get("event") == stage_release.UNATTRIBUTED_EVENT
             and event.get("tier_id") == tier]
    assert len(found) == 1, events
    return found[0]


@pytest.fixture()
def world(tmp_path: Path) -> _World:
    probe = tmp_path / "xattr-probe"
    probe.write_bytes(b"")
    try:
        os.setxattr(probe, "user.pbstage.probe", b"1")
    except OSError:
        pytest.skip("this filesystem carries no user extended attributes")
    finally:
        probe.unlink()
    return _World(tmp_path)


# --- the issue's scenario ----------------------------------------------------

def test_a_killed_promotions_copies_are_deleted_and_sized(world, monkeypatch):
    """Killed after k renames, never retried, its plan superseded.

    The consumer lives on under a plan that names its stage mover and a
    successor, never the promotion.  On main every copy stayed, counted in
    ``unowned_left`` beside the prewarm loop's objects with no bytes.
    """

    copies = world.kill_a_promotion(monkeypatch)
    world.live_consumer(leads=[STAGE_MOVER, SUCCESSOR])

    events = world.sweep()

    left = [path.name for path in copies if path.exists()]
    assert left == [], f"a killed promotion's copies were not reclaimed: {left}"
    receipt = _reconciled(events, RAM_TIER)
    assert receipt["entries_deleted"] == N, receipt
    assert receipt["bytes_deleted"] == TOTAL, receipt
    assert receipt["deleted_by_kind"]["mover_residue"] == {
        "entries": N, "bytes": TOTAL}, receipt
    assert receipt["unowned_left"] == 0, receipt
    assert receipt["unowned_left_bytes"] == 0, receipt
    assert receipt["complete"] is True, receipt
    # The ram root's own markers, and the stage copies a wanted fragment
    # names, are nobody's residue.
    assert (world.ram / storage_tiers.RAM_EPOCH_MARKER).exists()
    assert (world.ram / stage_release.STAGE_ROOT_MARKER).exists()
    assert all(path.exists() for path in world.paths(world.stage))


def test_a_killed_stage_movers_unnamed_copies_go_and_its_named_ones_stay(
        world):
    """The stage mover's shape: one fragment interval renamed and unnamed.

    The consumer is gone with no ending the dead-owner sweep can prove, so
    nothing wants the mover and nothing retires its fragment.  The copies
    its fragment names are that fragment's, wanted or not; the one renamed
    after it is residue.
    """

    world.forget_stage_fragment(keep=N - 1)
    stage_copies = world.paths(world.stage)

    events = world.sweep()

    assert not stage_copies[-1].exists(), (
        "the copy renamed after the last fragment publish was not reclaimed")
    assert all(path.exists() for path in stage_copies[:-1]), (
        "a copy some fragment names was deleted from under it")
    receipt = _reconciled(events, STAGE_TIER)
    assert receipt["deleted_by_kind"]["mover_residue"] == {
        "entries": 1, "bytes": SIZE}, receipt
    assert receipt["bytes_deleted"] == SIZE, receipt
    assert receipt["unowned_left"] == 0, receipt


# --- what a live mover still owns --------------------------------------------

def test_a_ready_movers_copies_are_never_judged(world, monkeypatch):
    """A mover ready or claimed on the tier may be writing: no pass at all."""

    copies = world.kill_a_promotion(monkeypatch)
    world.queue.publish(
        action_key=RAM_MOVER, cas_root=str(world.tmp / "cas"),
        checkout_root=str(world.queue.root), worker_script="w.py",
        tags=["testbox"],
        resources={"cpu": 2, "mem_gb": 1,
                   f"{storage_tiers.RAM_CAPACITY_KIND}@{RAM_TIER}": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                   "manifest_sha256": world.manifest_sha,
                   "manifest_bytes": TOTAL, "tier_id": RAM_TIER,
                   "range_start_bytes": 0, "range_end_bytes": TOTAL})

    receipt = stage_release.reconcile(world.queue, tier_id=RAM_TIER,
                                      stage_root=str(world.ram), wanted=set())

    assert receipt["skipped"] == "movers_in_flight", receipt
    assert all(path.exists() for path in copies)


@pytest.mark.parametrize("owner", ["wanted", "lease"])
def test_a_live_writers_residue_is_kept_and_sized(world, monkeypatch, owner):
    """A writer a live plan still names, or one whose lease outlived its row.

    Wanted: a retry under the same key adopts these copies by content
    (#1081), so they are its, not residue.  Leased: an entry under
    ``claimed/`` is no proof the writer has ended.
    """

    copies = world.kill_a_promotion(monkeypatch)
    wanted: set[str] = set()
    if owner == "wanted":
        wanted = {RAM_MOVER}
    else:
        pool._write_json_atomic(world.queue.lease_path(RAM_MOVER),
                                {"action_key": RAM_MOVER})

    receipt = stage_release.reconcile(world.queue, tier_id=RAM_TIER,
                                      stage_root=str(world.ram), wanted=wanted)

    assert all(path.exists() for path in copies), receipt
    assert receipt["entries_deleted"] == 0, receipt
    assert receipt["unowned_left_by_kind"]["mover_residue"] == {
        "entries": N, "bytes": TOTAL}, receipt
    assert receipt["unowned_left"] == N, receipt
    assert receipt["unowned_left_bytes"] == TOTAL, receipt
    reason = "mover_wanted" if owner == "wanted" else "mover_claimed"
    assert receipt["mover_residue_left_reasons"] == {reason: N}, receipt


def test_a_live_promotions_source_leg_is_kept(world):
    """A promotion claimed right now reads its stage leg; no fragment needed."""

    world.forget_stage_fragment()
    copies = world.paths(world.stage)
    cas = world.tmp / "cas"
    blob = pb.PrismaBuildCAS(cas).blob_path(world.manifest_sha)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(world.manifest.read_bytes())
    sealed = cas / "requests" / RAM_MOVER[:2] / f"{RAM_MOVER}.json"
    sealed.parent.mkdir(parents=True, exist_ok=True)
    sealed.write_text(json.dumps({
        "action_key": RAM_MOVER,
        "params": {"command": [
            "python3", "ram_promote.py",
            "--consumer-action-key", CONSUMER, "--tier-id", RAM_TIER,
            "--ram-root", str(world.ram),
            "--source-stage-root", str(world.stage),
            "--range-start-bytes", "0", "--range-end-bytes", str(SIZE)]},
        "inputs": [{"id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                    "sha256": world.manifest_sha,
                    "bytes": blob.stat().st_size}],
    }))
    (world.queue.dir(pool.CLAIMED) / f"{RAM_MOVER}.json").write_text(
        json.dumps({"action_key": RAM_MOVER, "cas_root": str(cas),
                    "resources": {"cpu": 2, "mem_gb": 1,
                                  f"{storage_tiers.RAM_CAPACITY_KIND}"
                                  f"@{RAM_TIER}": 1}}))

    receipt = stage_release.reconcile(world.queue, tier_id=STAGE_TIER,
                                      stage_root=str(world.stage), wanted=set())

    assert copies[0].exists(), "a live promotion's source leg was deleted"
    assert not any(path.exists() for path in copies[1:]), receipt
    assert receipt["mover_residue_left_reasons"] == {"promotion_source": 1}
    assert receipt["deleted_by_kind"]["mover_residue"] == {
        "entries": N - 1, "bytes": (N - 1) * SIZE}, receipt


# --- what is not a mover's to lose -------------------------------------------

def test_a_prewarm_object_and_a_copy_marked_before_the_fix_are_kept(world):
    """The prewarm loop's own mark alone is never permission (#1088).

    A mover's copy from before this change carries that same mark and no
    other, so it is read exactly like a prewarm object: kept, and sized
    under ``source_mark_only``.
    """

    world.forget_stage_fragment()
    legacy, *residue = world.paths(world.stage)
    os.removexattr(legacy, stage_move.STAGE_MOVER_XATTR)
    assert os.getxattr(legacy, prewarm_loop.STAGE_SOURCE_XATTR)
    origin = Path(str(world.body["entries"][0]["path"]))
    tier = prewarm_loop.StageTier(state="present", reason="test",
                                  mountpoint=str(world.stage),
                                  free_bytes=1 << 20)
    sink = tier.open_object("prewarm/shard-0.bin.pbstage@0+16384", SIZE)
    assert sink is not None
    sink.identify(os.stat(origin))
    assert sink.write(memoryview(_payload(0))) is True
    sink.commit()
    prewarmed = world.stage / "prewarm" / "shard-0.bin.pbstage@0+16384"
    assert prewarmed.exists() and tier.identity_unrecorded == 0

    receipt = stage_release.reconcile(world.queue, tier_id=STAGE_TIER,
                                      stage_root=str(world.stage), wanted=set())

    assert prewarmed.exists(), "a genuine prewarm object was deleted"
    assert legacy.exists(), "a copy marked before the fix was deleted"
    assert not any(path.exists() for path in residue), receipt
    assert receipt["unowned_left_by_kind"]["source_mark_only"] == {
        "entries": 2, "bytes": 2 * SIZE}, receipt
    assert receipt["unowned_left"] == 2, receipt
    assert receipt["unowned_left_bytes"] == 2 * SIZE, receipt
    assert receipt["deleted_by_kind"]["mover_residue"] == {
        "entries": N - 1, "bytes": (N - 1) * SIZE}, receipt


def test_an_unanswerable_mark_is_kept_and_sized(world, monkeypatch):
    """No attribute can be read: no kind is known, and nothing is deleted."""

    world.forget_stage_fragment()
    copies = world.paths(world.stage)

    def refuse(*_args, **_kwargs):
        raise OSError(errno.EOPNOTSUPP, "Operation not supported")

    monkeypatch.setattr(stage_release.os, "getxattr", refuse)
    receipt = stage_release.reconcile(world.queue, tier_id=STAGE_TIER,
                                      stage_root=str(world.stage), wanted=set())

    assert all(path.exists() for path in copies), receipt
    assert receipt["unowned_left_by_kind"]["mark_unanswerable"] == {
        "entries": N, "bytes": TOTAL}, receipt
    assert receipt["unowned_left_bytes"] == TOTAL, receipt


def test_every_movers_copy_names_its_writer(world, monkeypatch):
    """The mark is on the file from its first incarnation under its name."""

    for root, mover in ((world.stage, STAGE_MOVER), (world.ram, RAM_MOVER)):
        if root == world.ram:
            world.kill_a_promotion(monkeypatch)
        for path in world.paths(root):
            assert os.getxattr(path, stage_move.STAGE_MOVER_XATTR) == (
                mover.encode()), path
            assert os.getxattr(path, prewarm_loop.STAGE_SOURCE_XATTR), path
