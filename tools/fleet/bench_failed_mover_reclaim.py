#!/usr/bin/env python3
"""What a reclaimed partial and a shared staged name cost a Stage B row (#1151).

On 2026-09-25 Stage B row 013 died in two steps.  Spill-p0's mover
``574747d717fc`` ended ``complete: false`` at 436 of 512 entries, refused on a
staged name spill-p1's mover had renamed and not yet vouched (defect 2).  Then
the failed-mover reclaim (#627) published that mover's egress while the
consumer was reading spill-p0, and the consumer's next lease refused
``unpublished`` (defect 1).

This harness replays each step at incident proportions on a scratch queue,
stage root and origin, with the real ``tier_loop`` window and reclaim, the real
``stage_move`` mover, the real ``stage_release`` egress and the real
``reader_lease`` reader path.  It counts what the steps cost, from the movers'
own receipts and their copy workers' own read counters:

* ``reclaim`` -- phase 0 of one consumer lands ``--landed`` of ``--entries``
  and ends incomplete; the consumer is claimed and reading phase 0; one
  pressured reclaim pass runs, then any egress it published, then the window,
  then whatever mover the window republished.  Counted: egresses published,
  reader leases refused, and origin bytes read per staged byte to bring
  phase 0 to complete (every copy worker's own ``bytes_read()`` across both
  attempts, over the range's bytes).
* ``collision`` -- two phase movers of ``--shared`` staged names run at once;
  the first is held on its last entry for longer than the publication grace,
  the shape of a pacer hold.  Counted: movers that end incomplete, the second
  mover's wall time, the retries needed until both are complete, and origin
  bytes read per staged byte.

The harness names which tree it measured (``fix_present``), so the same file
reads a before tree and an after tree.  Run it through PrismaBuild on a GB10,
once per checkout, never on the tier host and never against the live stage or
queue::

    pbrun.py --cwd <checkout> --tag sparky --cpus 2 --demand mem_gb=2 \\
        --priority -10 -- python3 tools/fleet/bench_failed_mover_reclaim.py \\
        --work <scratch dir>
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import sys
import tempfile
import threading
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))

from prismabuild import adaptive_cpu, pool, reader_lease  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import residency_map, residency_plan  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
#: Real mountpoints a scratch root must never sit under.
FORBIDDEN_ROOTS = ("/stage", "/ram", "/mnt/shared")


def _key(label: str) -> str:
    return hashlib.sha256(f"bench-1151:{label}".encode()).hexdigest()


def _payload(index: int, size: int) -> bytes:
    return bytes((position * 7 + index * 13) % 251 + 1
                 for position in range(size))


@contextlib.contextmanager
def _patched(target, name: str, value):
    """Set one attribute for the duration, and put the original back."""

    original = getattr(target, name)
    setattr(target, name, value)
    try:
        yield
    finally:
        setattr(target, name, original)


class _OriginReads:
    """Every byte the movers' copy workers read from origin, landed or not.

    Summed from each ``_Copier``'s own ``bytes_read()`` (#1090), which counts
    only source reads.  The receipt's ``landing_report.copied_bytes`` is not
    this: it is ``max(landed, read)``, and landed counts adopted and resumed
    entries, which read nothing from origin.
    """

    def __init__(self) -> None:
        self.copiers: list = []

    @contextlib.contextmanager
    def installed(self):
        real = stage_move._Copier.__init__
        copiers = self.copiers

        def init(copier, *args, **kwargs):
            real(copier, *args, **kwargs)
            copiers.append(copier)

        with _patched(stage_move._Copier, "__init__", init):
            yield self

    def total(self) -> int:
        return sum(int(copier.bytes_read()) for copier in self.copiers)


# ---- defect 1: the reclaim under a reader ---------------------------------

class _Reclaim:
    """One consumer of two phases, a real queue, a real stage root."""

    def __init__(self, root: Path, *, entries: int, size: int) -> None:
        self.size = size
        self.entries = entries
        self.consumer = _key("consumer")
        self.movers = [_key("mover0"), _key("mover1")]
        self.egresses = [_key("egress0"), _key("egress1")]
        self.queue = pool.PoolQueue(root / "pb-queue")
        self.queue.ensure_layout()
        self.stage = root / "stage"
        self.stage.mkdir()
        if stage_release.register_stage_root(
                self.queue, tier_id=TIER, stage_root=self.stage) != "registered":
            raise SystemExit("the scratch stage root did not register")
        self.cas = root / "cas"
        self.cas.mkdir()
        origin = root / "origin"
        origin.mkdir()
        self.paths: list[Path] = []
        rows = []
        for index in range(2 * entries):
            path = origin / f"boundary-{index}.pt"
            path.write_bytes(_payload(index, size))
            self.paths.append(path)
            rows.append({"path": str(path), "offset": 0, "bytes": size,
                         "sha256": hashlib.sha256(
                             _payload(index, size)).hexdigest()})
        total = 2 * entries * size
        body = {"schema": "prismaquant.prismabuild.data_manifest.v1",
                "produced_by": {"tool": "bench-1151"},
                "mount_prefix": str(origin), "entries": rows,
                "entry_count": len(rows), "total_bytes": total,
                "annotations": {}}
        self.manifest = root / "manifest.json"
        self.manifest.write_text(json.dumps(body))
        self.digest = hashlib.sha256(self.manifest.read_bytes()).hexdigest()
        self.keys = [residency_map.residency_map_key(str(path), 0)
                     for path in self.paths]
        self.total = total
        phases = []
        for ordinal in range(2):
            start = ordinal * entries * size
            end = start + entries * size
            phases.append({
                "name": f"phase-{ordinal}",
                "start_bytes": start, "end_bytes": end, "stage_gib": 1,
                "mover_row": {
                    **self._row(self.movers[ordinal],
                                {STAGE_KIND: 1, "cpu": 1, "mem_gb": 1}),
                    "max_attempts": 1,
                    "residency": {
                        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": self.digest,
                        "manifest_bytes": total,
                        "range_start_bytes": start, "range_end_bytes": end}},
                "egress_row": self._row(self.egresses[ordinal],
                                        {"cpu": 1, "mem_gb": 1}),
            })
        self.plan = residency_plan.build_plan(
            consumer_action_key=self.consumer, tier_id=TIER,
            stage_root=str(self.stage), manifest_sha256=self.digest,
            manifest_bytes=total, phases=phases)
        residency_plan.freeze(self.queue, self.plan)
        self.queue.publish(
            action_key=self.consumer, cas_root=self.queue.root / "cas",
            checkout_root=self.queue.root / "co",
            worker_script=self.queue.root / "worker.py",
            resources={"cpu": 1, "mem_gb": 1}, max_attempts=1, tags=["x86"],
            residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                       "manifest_sha256": self.digest,
                       "manifest_bytes": total,
                       "leads": residency_plan.leads_for(self.plan)})
        self.queue.mint_tier_capacity(TIER, {"stage_gib": 8})
        self.tiers = {TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(self.stage)}}
        self.refuse: set[str] = set()

    def _row(self, key: str, resources: dict) -> dict:
        return {"action_key": key, "cas_root": str(self.queue.root / "cas"),
                "checkout_root": str(self.queue.root / "co"),
                "worker_script": str(self.queue.root / "worker.py"),
                "tags": ["dl380g10"], "resources": resources}

    def _claim(self, key: str) -> dict:
        ready = [row for row in self.queue.ready_items()
                 if row.get("action_key") == key]
        if not ready:
            raise SystemExit(f"{key[:12]} is not queued")
        claimed = self.queue.claim(
            tags=["dl380g10"], owner=f"{socket.gethostname()}:1:bench",
            capacity={"cpu": 4, "mem_gb": 16}, ready=ready)
        if claimed is None or claimed.get("action_key") != key:
            raise SystemExit(f"{key[:12]} did not claim: {claimed}")
        return claimed

    def published(self) -> list[str]:
        events = tier_loop.residency_window(self.queue, tiers=self.tiers)
        return [str(event.get("action_key")) for event in events
                if event.get("event") == "mover-published"]

    def run_mover(self, ordinal: int) -> dict:
        mover = self.movers[ordinal]
        claimed = self._claim(mover)
        start = ordinal * self.entries * self.size
        began = time.monotonic()
        rc = stage_move.main([
            "--pool-root", str(self.queue.root),
            "--cas-root", str(self.cas),
            "--action-key", mover,
            "--consumer-action-key", self.consumer,
            "--tier-id", TIER, "--stage-root", str(self.stage),
            "--manifest", str(self.manifest),
            "--manifest-sha256", self.digest,
            "--range-start-bytes", str(start),
            "--range-end-bytes", str(start + self.entries * self.size),
            "--residency-root", str(self.queue.residency_fragment_root()),
            "--readers", "1", "--max-readers", "1", "--unpaced"])
        seconds = time.monotonic() - began
        receipt = self.queue.move_record(mover)
        if not isinstance(receipt, dict):
            raise SystemExit(f"mover {ordinal} filed no receipt")
        self.queue.finish(mover, status="executed" if rc == 0 else "failed",
                          detail={"returncode": rc}, claim_snapshot=claimed)
        return {**receipt, "_wall_s": seconds}

    def claim_reader(self, phase: str) -> None:
        source = self.queue.item_path(pool.READY, self.consumer)
        item = json.loads(source.read_text())
        source.unlink()
        now = time.time()
        item.update({"action_key": self.consumer, "claimed_unix": now - 60.0,
                     "claimed_by": "bench-reader",
                     "claimed_host": "sparklina"})
        self.queue.item_path(pool.CLAIMED, self.consumer).write_text(
            json.dumps(item))
        self.queue.write_lease(
            self.consumer, owner="bench-reader", claim_snapshot=item,
            progress_observation={
                "source": "action-progress",
                "last_accepted": {"phase": phase, "units_completed": 1,
                                  "reported_unix": now - 9.0}})

    def read(self, indices, token: str) -> dict:
        residence = self.queue.residency_fragment_root()
        reader_lease.clear_cover_docs_cache()
        found = reader_lease.covers_for_keys(
            residence, self.consumer, [self.keys[index] for index in indices],
            tier_id=TIER, manifest_sha256=self.digest, epoch="", context={})
        if not found.get("ok"):
            return found
        return reader_lease.acquire(
            self.queue, consumer_action_key=self.consumer,
            attempt={"nonce": "e" * 32, "scope_id": "bench-reader"},
            tier_id=TIER, epoch="",
            span={"start_bytes": 0, "end_bytes": self.total},
            holder={"host": socket.gethostname(), "pid": os.getpid()},
            acquire_token=token, covers=found["covers"],
            expected=found["expected"], residency_root=residence,
            file_pin=False)

    def reclaim(self) -> list[dict]:
        """One pressured reclaim pass, then every egress it published runs."""

        consumers = tier_loop._planned_consumers(self.queue, self.tiers)
        events = tier_loop.reclaim_failed_mover_partials(
            self.queue, consumers, pressure={TIER: 1})
        for ordinal, egress in enumerate(self.egresses):
            if not self.queue.item_path(pool.READY, egress).exists():
                continue
            claimed = self._claim(egress)
            stage_release.evict(
                self.queue, self.movers[ordinal],
                consumer_action_key=self.consumer,
                stage_root=str(self.stage),
                residency_root=self.queue.residency_fragment_root())
            self.queue.finish(egress, status="executed",
                              detail={"returncode": 0},
                              claim_snapshot=claimed)
        return events


def reclaim_scenario(root: Path, *, entries: int, landed: int,
                     size: int) -> dict:
    world = _Reclaim(root, entries=entries, size=size)
    real = stage_move._Copier._copy_one

    def copy_one(copier, entry, destination, *args, **kwargs):
        if str(entry["path"]) in world.refuse:
            # What 574747d717fc met on boundary-425 (defect 2's symptom).
            raise stage_move._PublicationRefused(
                "shared staged name still has a live publisher after the "
                f"grace, deferring to retry: {destination}")
        return real(copier, entry, destination, *args, **kwargs)

    reads = _OriginReads()
    with _patched(adaptive_cpu, "action_identity",
                  lambda item: ("shape", False)), \
            _patched(stage_move._Copier, "_copy_one", copy_one), \
            reads.installed():
        if world.movers[0] not in world.published():
            raise SystemExit("the window did not publish phase 0's mover")
        world.refuse = {str(world.paths[landed])}
        first = world.run_mover(0)
        world.refuse = set()
        if first.get("complete") is not False or int(
                first.get("entries_staged") or 0) != landed:
            raise SystemExit(f"phase 0 did not fail at {landed}: {first}")
        world.claim_reader("phase-0")
        control = world.read(range(landed), "bench:control")
        if not control.get("ok"):
            raise SystemExit(f"control read refused: {control}")
        events = world.reclaim()
        after = world.read(range(landed), "bench:after-reclaim")
        republished = False
        retry: dict = {}
        for _cycle in range(3):
            if world.movers[0] in world.published():
                republished = True
                break
        if republished:
            retry = world.run_mover(0)
        origin = reads.total()
        staged = entries * size
        whole = world.read(range(entries), "bench:whole") if (
            retry.get("complete")) else {"ok": False, "refusal": "no retry"}
    return {
        "entries": entries, "landed_before_failure": landed,
        "entry_bytes": size,
        "failed_mover_reclaims": sum(
            1 for event in events
            if event.get("event") == "failed-mover-egress-published"),
        "reclaims_deferred_for_reader": sum(
            1 for event in events if event.get("event")
            == "failed-mover-reclaim-deferred-for-reader"),
        "reader_refusals": 0 if after.get("ok") else 1,
        "reader_refusal": None if after.get("ok") else after.get("refusal"),
        "retry_published": republished,
        "retry_complete": bool(retry.get("complete")),
        "retry_entries_resumed": retry.get("entries_resumed"),
        "retry_entries_copied": (
            ((retry.get("phase_timings") or {}).get("thread_seconds") or {})
            .get("copy_read", {}).get("calls")),
        "retry_wall_s": round(float(retry.get("_wall_s") or 0.0), 3),
        "whole_phase_reads": bool(whole.get("ok")),
        "origin_bytes_read": origin,
        "staged_bytes": staged,
        "origin_read_per_staged_byte": round(origin / staged, 4),
    }


# ---- defect 2: two phase movers of one staged name ------------------------

def collision_scenario(root: Path, *, shared: int, size: int, grace: float,
                       fragment_s: float) -> dict:
    queue = pool.PoolQueue(root / "pb-queue")
    queue.ensure_layout()
    stage = root / "stage"
    stage.mkdir()
    cas = root / "cas"
    origin = root / "origin"
    origin.mkdir()
    paths, rows = [], []
    for index in range(shared):
        path = origin / f"boundary-{index}.pt"
        path.write_bytes(_payload(index, size))
        paths.append(path)
        rows.append({"path": str(path), "offset": 0, "bytes": size,
                     "sha256": hashlib.sha256(
                         _payload(index, size)).hexdigest()})
    phase_bytes = shared * size
    indices = list(range(shared))
    body = {"schema": pb.DATA_MANIFEST_SCHEMA_V2,
            "produced_by": {"tool": "bench-1151"}, "annotations": {},
            "mount_prefix": str(origin), "entries": rows,
            "entry_count": shared, "total_bytes": phase_bytes,
            "read_plan": {"phases": [
                {"name": "spill-p0", "entry_indices": indices,
                 "bytes": phase_bytes, "cumulative_bytes": phase_bytes},
                {"name": "spill-p1", "entry_indices": indices,
                 "bytes": phase_bytes, "cumulative_bytes": 2 * phase_bytes},
            ], "read_bytes": 2 * phase_bytes}}
    blob = json.dumps(body).encode()
    manifest = root / "manifest.json"
    manifest.write_bytes(blob)
    digest = hashlib.sha256(blob).hexdigest()
    cas_blob = pb.PrismaBuildCAS(cas).blob_path(digest)
    cas_blob.parent.mkdir(parents=True, exist_ok=True)
    cas_blob.write_bytes(blob)
    consumers = [_key("consumer-p0"), _key("consumer-p1")]
    movers = [_key("collide-p0"), _key("collide-p1")]
    spans = [(0, phase_bytes), (phase_bytes, 2 * phase_bytes)]

    def seal(ordinal: int) -> None:
        start, end = spans[ordinal]
        request = {"action_key": movers[ordinal],
                   "params": {"command": [
                       "python3", "stage_move.py",
                       "--range-start-bytes", str(start),
                       "--range-end-bytes", str(end)]},
                   "inputs": [{"id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                               "sha256": digest}]}
        path = cas / "requests" / movers[ordinal][:2] / f"{movers[ordinal]}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(request))
        queue.item_path(pool.CLAIMED, movers[ordinal]).write_text(json.dumps({
            "action_key": movers[ordinal], "cas_root": str(cas),
            "resources": {"cpu": 2, "mem_gb": 1, STAGE_KIND: 1}}))

    def conclude(ordinal: int) -> None:
        queue.item_path(pool.CLAIMED, movers[ordinal]).unlink(missing_ok=True)

    def move(ordinal: int) -> dict:
        start, end = spans[ordinal]
        args = stage_move.build_parser().parse_args([
            "--pool-root", str(queue.root), "--cas-root", str(cas),
            "--action-key", movers[ordinal],
            "--consumer-action-key", consumers[ordinal],
            "--tier-id", TIER, "--stage-root", str(stage),
            "--manifest-sha256", digest,
            "--range-start-bytes", str(start),
            "--range-end-bytes", str(end),
            "--manifest", str(manifest),
            "--residency-root", str(queue.root / pool.RESIDENCY),
            "--block", "4096", "--readers", "1", "--max-readers", "1",
            "--unpaced"])
        began = time.monotonic()
        receipt = stage_move.move(args)
        return {**receipt, "_wall_s": time.monotonic() - began}

    held, release = threading.Event(), threading.Event()
    real = stage_move._Copier._copy_one
    last = str(paths[-1])

    def copy_one(copier, entry, destination, *args, **kwargs):
        if copier.owner == movers[0] and str(entry["path"]) == last:
            held.set()
            release.wait(120)
        return real(copier, entry, destination, *args, **kwargs)

    first: dict = {}
    reads = _OriginReads()
    with _patched(stage_move, "_PUBLISH_GRACE_S", grace), \
            _patched(stage_move, "_PUBLISH_POLL_S", 0.02), \
            _patched(stage_move, "FRAGMENT_PUBLISH_S", fragment_s), \
            _patched(stage_move._Copier, "_copy_one", copy_one), \
            reads.installed():
        seal(0)
        seal(1)
        began = time.monotonic()
        thread = threading.Thread(
            target=lambda: first.setdefault("receipt", move(0)), daemon=True)
        thread.start()
        if not held.wait(60):
            raise SystemExit("the first mover never reached its last entry")
        second = move(1)
        release.set()
        thread.join(120)
        if thread.is_alive():
            raise SystemExit("the first mover never finished")
        retries = 0
        latest = [first["receipt"], second]
        # Whatever ended incomplete runs again under its own key, as the
        # window republishes it, until both phases are complete.
        while retries < 4 and not all(r.get("complete") for r in latest):
            for ordinal in range(2):
                if not latest[ordinal].get("complete"):
                    latest[ordinal] = move(ordinal)
                    retries += 1
        all_complete_s = time.monotonic() - began
        conclude(0)
        conclude(1)
        origin_read = reads.total()
    return {
        "shared_names": shared, "entry_bytes": size,
        "grace_s": grace, "fragment_publish_s": fragment_s,
        "movers_incomplete_first_attempt": sum(
            1 for receipt in (first["receipt"], second)
            if not receipt.get("complete")),
        "first_attempt_errors": [str(error)[:160] for receipt in (
            first["receipt"], second) for error in receipt.get("errors", [])],
        "second_mover_wall_s": round(second["_wall_s"], 3),
        "retries_to_complete": retries,
        "both_complete": all(r.get("complete") for r in latest),
        "seconds_until_both_complete": round(all_complete_s, 3),
        "origin_bytes_read": origin_read,
        "staged_bytes": phase_bytes,
        "origin_read_per_staged_byte": round(origin_read / phase_bytes, 4),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--work", required=True,
                        help="scratch directory; a fresh subdirectory is "
                             "made per run and removed after it")
    parser.add_argument("--entries", type=int, default=512,
                        help="entries in the failed phase (574747d7: 512)")
    parser.add_argument("--landed", type=int, default=436,
                        help="entries it landed before failing (574747d7: 436)")
    parser.add_argument("--shared", type=int, default=64,
                        help="staged names the two colliding movers share")
    parser.add_argument("--entry-bytes", type=int, default=4096)
    parser.add_argument("--grace-s", type=float, default=3.0,
                        help="the publication grace, shortened from 30 s")
    parser.add_argument("--fragment-s", type=float, default=0.5,
                        help="the fragment rate limit, shortened from 5 s in "
                             "proportion to the grace")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args(argv)
    work = Path(args.work).resolve()
    if any(str(work) == root or str(work).startswith(root + "/")
           for root in FORBIDDEN_ROOTS):
        raise SystemExit(f"refusing a scratch root under a live mount: {work}")
    if not 0 < args.landed < args.entries:
        raise SystemExit("--landed must be between 0 and --entries")
    work.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="bench-1151-", dir=work))
    try:
        (run / "reclaim").mkdir()
        (run / "collision").mkdir()
        result = {
            "schema": "prismabuild.bench_failed_mover_reclaim.v1",
            "host": socket.gethostname(),
            "fix_present": {
                "reclaim_consults_readers": hasattr(
                    tier_loop, "_legs_a_reader_can_reach"),
                "fragment_trailing_edge": hasattr(
                    stage_move, "_TrailingFragment")},
            "reclaim": reclaim_scenario(
                run / "reclaim", entries=args.entries, landed=args.landed,
                size=args.entry_bytes),
            "collision": collision_scenario(
                run / "collision", shared=args.shared, size=args.entry_bytes,
                grace=args.grace_s, fragment_s=args.fragment_s),
        }
    finally:
        if not args.keep:
            shutil.rmtree(run, ignore_errors=True)
    print("BENCH-1151 " + json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
