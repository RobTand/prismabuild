#!/usr/bin/env python3
"""Make the next actions' declared bytes resident before anyone claims them.

Why this exists
---------------
A campaign whose working set is far larger than the file server's ARC gets
nothing from caching: every byte is read once and never asked for again, so a
cache that fills behind the reader is a cache that never hits.  The only read
that can be made fast is one that was already resident when the action started.

Measured on the GLM-5.3-Flash census (2026-09-11, dl380g10): a row whose
capture files were resident read at 3298.7 MB/s over NFS; the same size of
never-read row read at 306.9 MB/s.  10.75x, 145.8 s saved on a single row, and
the disks were never the limit -- HDD utilization never passed 88% at any
reader count.

What it does, every poll
------------------------
1. Reads ``ready/`` in PrismaBuild's own claim order and takes the first few
   actions that carry a ``pbcampaign.data-manifest`` input.  It does not claim,
   reserve, reorder or write an item: claim order stays the coordinator's, and
   this loop only reads the same list the workers read.
2. Checks ARC headroom against what the manifests ask for, minus what the
   currently claimed actions still need.
3. Reads each manifest's files, in manifest order, through the host's local
   pool path with bounded parallelism.
4. Writes one record per action under ``pb-queue/prewarm/<action_key>.json``.
   That is the only thing it writes, anywhere.

What it does not do
-------------------
It is not a second dispatcher.  It never decides what runs, where, or next; it
predicts nothing the queue does not already say out loud.  If the coordinator
reorders the queue between two polls, the loop simply warms a different row.

It cannot *guarantee* that a claimed action's data is never evicted, and says
so rather than implying otherwise: ZFS exposes no pin, and the ARC target ``c``
is volatile on a shared box -- it fell 99 GB inside one five-minute window on
2026-09-11 without any tenant asking for the memory.  What it does guarantee is
budgetary: it never *asks* for more than ``(c_max - size) * reserve`` minus the
manifest bytes of every action claimed inside the last ``--claim-grace-min``
minutes, which is the window in which a claimed action is still reading.  Both
headroom numbers are logged every cycle so the gap between the budget and the
outcome stays visible.
"""

from __future__ import annotations

import argparse
import json
import os
import queue as queuelib
import socket
import stat as statmod
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

# The publisher writes this program twice, ``tools/fleet/prewarm_loop.py`` and
# a flattened ``tools/prewarm_loop.py``, and ``supervise._spawn_role`` runs the
# flat one.  ``parents[2]`` is the repository root under the first spelling and
# the parent of the generation store under the second, so the generation must
# be resolved by the rule that knows both layouts (the rule ``pbtest.py`` uses).
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

import prismabuild.core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

#: ZFS reads a whole record either way, and the pool this was measured on uses
#: 1 MiB records; a smaller block only costs syscalls.
BLOCK = 1 << 20
ARCSTATS = "/proc/spl/kstat/zfs/arcstats"
#: The fleet's own store, spelled the way ``worker_loop`` spells it.  Not
#: ``pool.DEFAULT_POOL_ROOT``: that default is ``/mnt/shared/pb-queue``, one
#: directory above the queue this fleet actually keeps, and a loop pointed
#: there reports an empty ready list forever rather than an error.
SH = Path("/mnt/shared/prismabuild-fleet")


# ------------------------------------------------------------------ ARC


def arcstats(path: str = ARCSTATS) -> dict[str, int]:
    out: dict[str, int] = {}
    try:
        with open(path) as handle:
            for line in handle:
                fields = line.split()
                if len(fields) == 3:
                    try:
                        out[fields[0]] = int(fields[2])
                    except ValueError:
                        pass
    except OSError:
        pass
    return out


def arc_headroom(reserve_fraction: float, path: str = ARCSTATS) -> dict[str, int]:
    """The ARC this loop may spend, and the counters that explain the number.

    The budget is a share of ``c_max`` -- the cache's *capacity* -- and not of
    the free space inside it.  That is not an optimism: a cache in steady
    state is full, and this one is.  Measured on dl380g10 at 2026-09-11
    01:59 UTC, after the campaign had been reading for hours: ``size``
    254.05 GB against ``c_max`` 257.70 GB, leaving 3.65 GB free.  A loop that
    budgeted on free space would have refused every 64 GB row forever while
    the ARC sat full of bytes nobody would ask for again -- which is the whole
    failure prewarm exists to fix.  Warming evicts, and it is supposed to: the
    question is never "is there room" but "is what I would displace worth less
    than what I would make resident".  That question is answered by
    subtracting the bytes that are still wanted, in ``cycle``.

    ``size``, ``c`` and the two free-space numbers are reported anyway,
    because they are what a reader needs to tell a refusal caused by the
    protected set from one caused by a shrunken cache: ``c`` is volatile here,
    and fell 99 GB inside one five-minute window on 2026-09-11.
    """

    stats = arcstats(path)
    size = stats.get("size", 0)
    current = stats.get("c", 0)
    ceiling = stats.get("c_max", 0)
    return {
        "arc_size": size,
        "arc_c": current,
        "arc_c_max": ceiling,
        "headroom_nominal": max(0, ceiling - size),
        "headroom_effective": max(0, current - size),
        "capacity_budget": max(0, int(ceiling * reserve_fraction)),
    }


# ------------------------------------------------------------ path mapping


class MountMap:
    """Rewrite a manifest's shared-mount path onto this host's local mount.

    The mapping is host configuration, not a constant: dl380g10 serves
    ``/mnt/shared`` out of a local ZFS dataset also mounted at
    ``/storage_pool/shared``, and reading the local path keeps the warm off the
    server's own NFS client.  A host with no local mount has no business
    running this loop, which is what ``usable`` reports.
    """

    def __init__(self, pairs: list[str]) -> None:
        self.pairs: list[tuple[str, str]] = []
        for pair in pairs:
            shared, _, local = pair.partition("=")
            if not shared.startswith("/") or not local.startswith("/"):
                raise SystemExit(f"prewarm: bad --mount-map entry: {pair!r}")
            self.pairs.append((shared.rstrip("/"), local.rstrip("/")))

    def usable(self) -> bool:
        return any(os.path.isdir(local) for _, local in self.pairs)

    def local(self, path: str) -> str:
        for shared, local in self.pairs:
            if path == shared or path.startswith(shared + "/"):
                candidate = local + path[len(shared):]
                if os.path.isdir(local):
                    return candidate
        return path


# ---------------------------------------------------------------- reading


class Reader:
    """Bounded parallel sequential reads, in the order the manifest lists them.

    Order matters twice: it is the order the action itself will consume the
    bytes, so a partial warm is a useful prefix rather than a random subset,
    and it is the order the files were written, so the disks see something
    close to a sequential sweep.
    """

    def __init__(self, readers: int, mounts: MountMap, block: int = BLOCK) -> None:
        self.readers = max(1, readers)
        self.mounts = mounts
        self.block = block

    def read(
        self,
        entries: list[dict[str, object]],
        *,
        budget_bytes: int,
        stop: threading.Event,
        recheck: Callable[[], int] | None = None,
    ) -> dict[str, object]:
        work: "queuelib.Queue[dict[str, object] | None]" = queuelib.Queue()
        for entry in entries:
            work.put(entry)
        for _ in range(self.readers):
            work.put(None)
        lock = threading.Lock()
        state = {"bytes": 0, "entries": 0, "budget": int(budget_bytes)}
        errors: list[str] = []

        def worker() -> None:
            buffer = bytearray(self.block)
            view = memoryview(buffer)
            while not stop.is_set():
                job = work.get()
                if job is None:
                    return
                with lock:
                    if state["bytes"] >= state["budget"]:
                        return
                path = self.mounts.local(str(job["path"]))
                want_total = int(job["bytes"])
                offset = int(job["offset"])
                got = 0
                try:
                    # O_NOFOLLOW at the leaf, and a regular-file check: the
                    # manifest is submitter-supplied, so a symlink swapped in
                    # after validation must fail here rather than send this
                    # loop reading something outside the mount it approved.
                    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                except OSError as exc:
                    with lock:
                        if len(errors) < 20:
                            errors.append(f"{os.path.basename(path)}: {exc}")
                    continue
                try:
                    if not statmod.S_ISREG(os.fstat(fd).st_mode):
                        raise OSError(f"not a regular file: {path}")
                    if offset:
                        os.lseek(fd, offset, os.SEEK_SET)
                    remaining = want_total
                    while remaining > 0 and not stop.is_set():
                        chunk = os.readv(fd, [view[: min(self.block, remaining)]])
                        if not chunk:
                            break
                        got += chunk
                        remaining -= chunk
                        with lock:
                            state["bytes"] += chunk
                            if state["bytes"] >= state["budget"]:
                                break
                except OSError as exc:
                    with lock:
                        if len(errors) < 20:
                            errors.append(f"{os.path.basename(path)}: {exc}")
                finally:
                    os.close(fd)
                with lock:
                    state["entries"] += 1
                    if recheck is not None and state["entries"] % 64 == 0:
                        room = recheck()
                        if room <= 0:
                            state["budget"] = state["bytes"]

        started = time.time()
        threads = [threading.Thread(target=worker, daemon=True)
                   for _ in range(self.readers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = max(1e-9, time.time() - started)
        return {
            "bytes_warmed": state["bytes"],
            "entries_warmed": state["entries"],
            "seconds": round(elapsed, 3),
            "mb_per_s": round(state["bytes"] / 1e6 / elapsed, 1),
            "readers": self.readers,
            "errors": errors,
        }


# ------------------------------------------------------------- queue view


def sealed_request(cas_root: Path, action_key: str) -> dict | None:
    path = Path(cas_root) / "requests" / action_key[:2] / f"{action_key}.json"
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def manifest_input_of(request: dict | None) -> dict | None:
    if not isinstance(request, dict):
        return None
    for entry in request.get("inputs") or ():
        if isinstance(entry, dict) and \
                entry.get("id") == pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID:
            return entry
    return None


def declared_manifest_bytes(request: dict | None) -> int:
    """What the submitter said the manifest totals, without fetching it.

    ``pbrun`` seals this summary into ``params`` precisely so the headroom
    arithmetic -- which runs for every claimed action on every poll -- never
    has to open and hash a blob.
    """

    if not isinstance(request, dict):
        return 0
    summary = (request.get("params") or {}).get("data_manifest")
    if isinstance(summary, dict):
        value = summary.get("total_bytes")
        if isinstance(value, int) and value >= 0:
            return value
    return 0


def load_manifest(cas_root: Path, entry: dict) -> dict | None:
    """Fetch and validate the manifest blob the input row addresses.

    ``input_path`` re-hashes the blob, so a manifest that does not match the
    digest the action key covers is refused here rather than read from.
    """

    try:
        path = pb.PrismaBuildCAS(Path(cas_root)).input_path(entry)
        return pb.load_data_manifest(path)
    except (pb.PrismaBuildError, OSError, ValueError):
        return None


def claimed_reserve(queue: pool.PoolQueue, cas_root: Path, grace_s: float) -> dict:
    """Bytes a claimed action may still be reading, and must not be displaced.

    An action reads its inputs at the start of its run, so a claim older than
    the grace window is past its load phase and its bytes are no longer worth
    reserving.  The window is configuration, not a guess about any one
    campaign: set it to the load phase you measured.
    """

    now = time.time()
    reserved = 0
    keys: list[str] = []
    for path in sorted((queue.root / pool.CLAIMED).glob("*.json")):
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(record, dict):
            continue
        claimed_unix = record.get("claimed_unix")
        if not isinstance(claimed_unix, (int, float)):
            continue
        if now - float(claimed_unix) > grace_s:
            continue
        key = str(record.get("action_key", path.stem))
        size = declared_manifest_bytes(sealed_request(cas_root, key))
        if size:
            reserved += size
            keys.append(key)
    return {"claimed_reserved_bytes": reserved, "claimed_reserved_keys": keys}


def already_warm(queue: pool.PoolQueue, action_key: str, sha256: str) -> bool:
    record = queue.prewarm(action_key)
    return bool(
        record
        and record.get("manifest_sha256") == sha256
        and record.get("status") == "complete"
    )


def warmed_reserve(queue: pool.PoolQueue, ready: list[dict]) -> dict:
    """Bytes this loop already made resident for rows nobody has claimed yet.

    Without this the loop would spend its whole budget on the head of the
    queue, then spend it again on the next row by displacing the first --
    warming two rows and arriving with neither.  A row stays protected until
    it is claimed, at which point ``claimed_reserve`` takes over and the
    grace window retires it.
    """

    reserved = 0
    keys: list[str] = []
    for item in ready:
        record = queue.prewarm(str(item.get("action_key", "")))
        if record and record.get("status") == "complete":
            reserved += int(record.get("bytes_warmed", 0))
            keys.append(str(record.get("action_key")))
    return {"warmed_reserved_bytes": reserved, "warmed_reserved_keys": keys}


# ------------------------------------------------------------------ loop


def cycle(args, queue: pool.PoolQueue, mounts: MountMap, stop: threading.Event) -> dict:
    ready = queue.ready_items()
    room = arc_headroom(args.arc_reserve_fraction, args.arcstats)
    cas_root = Path(args.cas_root) if args.cas_root else None
    reserve = claimed_reserve(
        queue,
        cas_root or Path(pool.DEFAULT_POOL_ROOT).parent / "cas",
        args.claim_grace_min * 60.0,
    )
    reserve.update(warmed_reserve(queue, ready))
    protected = (reserve["claimed_reserved_bytes"]
                 + reserve["warmed_reserved_bytes"])
    budget = max(0, room["capacity_budget"] - protected)
    event = {
        "event": "cycle",
        "unix": round(time.time(), 3),
        "host": socket.gethostname(),
        "ready": len(ready),
        **room,
        **reserve,
        "protected_bytes": protected,
        "budget_after_reserve": budget,
        "warmed": [],
        "skipped": [],
    }

    taken = 0
    for item in ready:
        if taken >= args.lookahead or stop.is_set():
            break
        key = str(item.get("action_key", ""))
        root = Path(item.get("cas_root") or cas_root or "")
        if not key or not root.name:
            continue
        request = sealed_request(root, key)
        entry = manifest_input_of(request)
        if entry is None:
            continue
        manifest = load_manifest(root, entry)
        if manifest is None:
            event["skipped"].append({"action_key": key, "reason": "unreadable manifest"})
            continue
        total = int(manifest["total_bytes"])
        if total < args.min_manifest_bytes:
            # A trivial action is not worth a warm window. On the GLM census
            # only 42 of 132 rows are 864-unit expert rows; the other 90 carry
            # one or two units and load in about 0.3 s, so warming one spends
            # the lookahead on a row that was never going to wait for a disk.
            # Skipping without consuming ``taken`` keeps a run of trivial rows
            # from disabling prewarm for the real row behind them.
            event["skipped"].append({
                "action_key": key, "reason": "below the warm threshold",
                "manifest_bytes": total,
                "min_manifest_bytes": args.min_manifest_bytes,
            })
            continue
        taken += 1
        digest = str(entry["sha256"])
        if already_warm(queue, key, digest):
            event["skipped"].append({"action_key": key, "reason": "already warm"})
            continue
        if total > budget:
            # Refusing is the correct outcome, not a failure: warming a row
            # that does not fit would evict the row that is running to make
            # room for one that is not.
            event["skipped"].append({
                "action_key": key, "reason": "headroom",
                "manifest_bytes": total, "budget": budget,
            })
            continue
        started = time.time()
        if args.dry_run:
            result = {"bytes_warmed": 0, "entries_warmed": 0, "seconds": 0.0,
                      "mb_per_s": 0.0, "readers": args.readers, "errors": []}
        else:
            reader = Reader(args.readers, mounts)
            result = reader.read(
                list(manifest["entries"]),
                budget_bytes=budget,
                stop=stop,
                # Re-read rather than trust the cycle's opening number: the
                # ceiling can be lowered under a running warm, and a row that
                # started inside its budget must stop when it leaves it.
                recheck=lambda: arc_headroom(
                    args.arc_reserve_fraction,
                    args.arcstats)["capacity_budget"] - protected,
            )
        after = arc_headroom(args.arc_reserve_fraction, args.arcstats)
        record = {
            "action_key": key,
            "host": socket.gethostname(),
            "manifest_sha256": digest,
            "manifest_bytes": total,
            "entry_count": int(manifest["entry_count"]),
            "mount_prefix": manifest["mount_prefix"],
            "started_unix": round(started, 3),
            "finished_unix": round(time.time(), 3),
            "status": (
                "dry-run" if args.dry_run
                else "complete" if result["bytes_warmed"] >= total
                else "partial"
            ),
            "arc_before": {k: room[k] for k in
                           ("arc_size", "arc_c", "arc_c_max",
                            "headroom_nominal", "headroom_effective")},
            "arc_after": {k: after[k] for k in
                          ("arc_size", "arc_c", "arc_c_max",
                           "headroom_nominal", "headroom_effective")},
            **result,
        }
        if not args.dry_run:
            queue.record_prewarm(key, record)
        budget = max(0, budget - result["bytes_warmed"])
        event["warmed"].append({k: record[k] for k in
                                ("action_key", "status", "manifest_bytes",
                                 "bytes_warmed", "seconds", "mb_per_s")})
    return event


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pool-root", default=str(SH / "pb-queue"),
                        help="the queue whose ready list to read")
    parser.add_argument("--cas-root", default=str(SH / "cas"),
                        help="fallback CAS root; each ready item names its own")
    parser.add_argument("--mount-map", action="append", required=True,
                        help="SHARED=LOCAL, repeatable: rewrite a manifest's "
                             "shared-mount path onto this host's local mount "
                             "(e.g. /mnt/shared=/storage_pool/shared). "
                             "Required and never defaulted -- which mount a "
                             "box serves is host configuration, and a wrong "
                             "guess reads the network instead of the disks")
    parser.add_argument("--readers", type=int, default=8,
                        help="parallel readers; 8 measured 391.9 MB/s on the "
                             "GLM census pool, 1 measured 298.9")
    parser.add_argument("--lookahead", type=int, default=2,
                        help="how many ready actions ahead to warm")
    parser.add_argument("--min-manifest-bytes", type=int, default=1 << 30,
                        help="a manifest smaller than this is passed over "
                             "without consuming the lookahead: a row that "
                             "loads in well under a second cannot be made "
                             "faster by warming it, and spending the window "
                             "on one costs the row behind it")
    parser.add_argument("--poll-s", type=float, default=10.0,
                        help="seconds between polls of the ready list")
    parser.add_argument("--arc-reserve-fraction", type=float, default=0.8,
                        help="fraction of the ARC ceiling c_max this loop may "
                             "hold at once, the rest left as slack for every "
                             "other reader of the box")
    parser.add_argument("--arcstats", default=ARCSTATS,
                        help="where the ARC counters live; a host without "
                             "this file reports zero headroom and warms "
                             "nothing, which is the right answer there")
    parser.add_argument("--claim-grace-min", type=float, default=20.0,
                        help="a claim younger than this still counts its "
                             "manifest bytes against the prewarm budget")
    parser.add_argument("--once", action="store_true",
                        help="run one cycle and exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="decide and report; read nothing, write nothing")
    parser.add_argument("--log", default=None,
                        help="append one JSON object per cycle here")
    args = parser.parse_args(argv)

    mounts = MountMap(list(args.mount_map))
    if not args.dry_run and not mounts.usable():
        raise SystemExit(
            "prewarm: no local mount from --mount-map exists on this host; "
            "this loop belongs on the storage host only")
    queue = pool.PoolQueue(Path(args.pool_root))
    stop = threading.Event()
    while True:
        event = cycle(args, queue, mounts, stop)
        line = json.dumps(event)
        print(line, flush=True)
        if args.log:
            with open(args.log, "a") as handle:
                handle.write(line + "\n")
        if args.once:
            return 0
        time.sleep(args.poll_s)


if __name__ == "__main__":
    raise SystemExit(main())
