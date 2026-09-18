#!/usr/bin/env python3
"""Copy one byte range of a data manifest's read order onto a stage tier (#583).

This is the movement node: an ordinary PB action, placed by tag on the box
that serves the pool, whose whole job is to make a declared range of a
consumer's read order resident somewhere the consumer can reach faster.  It
publishes three things and nothing else --- the staged files, a residency-map
fragment naming the ones it verified, and a receipt saying what the pool
actually delivered.

**Why a copy and not a warm.**  ``prewarm_loop.py`` reads the bytes and throws
them away, which is the right shape for the ARC: the ARC is keyed by on-pool
block pointer, so reading a block warms the path the consumer will open.  A
stage tier is a different device.  Nothing about reading a file makes a copy of
it, so this tool has a sink where the prewarm loop has none.  Everything else
it shares, by import rather than by copy: the mount map, the pacer that
attributes the read to the pool's own members, the admission gate that makes
the depth mean something, and the manifest window helpers.  ``prewarm_loop`` is
imported and never edited.

**What makes a staged byte range visible.**  The copy is written beside its
final name and renamed into place, so a partial file is never visible under the
name the consumer reads.  The stage dataset runs ``sync=disabled``, which means
a crash can lose whole files that were renamed but not yet on stable storage;
the residency map is what turns that from a correctness problem into a
performance one, because an entry is written only after its rename and the
consumer verifies the digest before it trusts a staged read.  Read order is
preserved for the same reason it is in the prewarm loop: a partial range is
then a useful prefix rather than a random subset.

**Streaming, with a bounded window.**  Entries are copied in manifest read
order by a small pool of workers, so the first bytes land while the last are
still being read, and a finished entry is forgotten as soon as its fragment
entry is written.  Nothing accumulates the range in memory; the only buffer is
one block per worker.

**What it refuses.**  A range that stages more bytes than it declared is
``residency_overran_reservation``: its tokens bound what the tier can hold, so
a mover that exceeds them has broken the accounting that keeps the stage from
overfilling.  It exits non-zero, having published no fragment, so the gate
refuses its consumer rather than admitting it onto bytes nobody reserved.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import queue as queuelib
import socket
import stat as statmod
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

import prewarm_loop  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

#: A staged range that is not a whole file lives under this suffix, so the
#: name of one range can never be the name of another.
RANGE_SUFFIX = ".pbrange"


def stage_relative(path: str, offset: int, size: int, *, mount_prefix: str,
                   whole_file: bool) -> str:
    """Where one manifest entry's bytes live under the stage root.

    A path the manifest names exactly once, from offset zero, keeps its own
    relative name.  Anything else is one range among several of the same file
    and gets a name of its own: two movers holding two ranges of one shard
    cannot both rename-publish into one file, and the staged object's length
    has to be the range's length for the consumer to read it from position 0.
    """

    relative = path[len(mount_prefix.rstrip("/")) + 1:]
    if not relative or relative.startswith("/"):
        raise ValueError(f"{path!r} is not inside {mount_prefix!r}")
    if whole_file and offset == 0:
        return relative
    return f"{relative}{RANGE_SUFFIX}/{offset}-{size}"


def whole_file_paths(entries: list[dict[str, object]]) -> set[str]:
    """Paths the manifest names exactly once; everything else is a split file."""

    counts: dict[str, int] = {}
    for entry in entries:
        path = str(entry["path"])
        counts[path] = counts.get(path, 0) + 1
    return {path for path, count in counts.items() if count == 1}


def proc_io(pid: str = "self") -> dict[str, int]:
    """``/proc/PID/io`` as integers, or ``{}`` where it cannot be read.

    Read at both ends and differenced: a rate without ``read_bytes`` beside it
    cannot say whether the bytes came off the device or out of the page cache,
    and that distinction is the whole reason the pacer exists.
    """

    try:
        with open(f"/proc/{pid}/io") as stream:
            text = stream.read()
    except OSError:
        return {}
    out: dict[str, int] = {}
    for line in text.splitlines():
        name, _, value = line.partition(":")
        try:
            out[name.strip()] = int(value.strip())
        except ValueError:
            continue
    return out


def _delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {name: after[name] - before.get(name, 0) for name in sorted(after)}


class _Copier:
    """One range, copied in read order by a bounded set of workers."""

    def __init__(self, *, mounts: prewarm_loop.MountMap, pacer, stage_root: Path,
                 mount_prefix: str, block: int, workers: int) -> None:
        self.mounts = mounts
        self.pacer = pacer
        self.stage_root = stage_root
        self.mount_prefix = mount_prefix
        self.block = block
        self.workers = max(1, workers)
        self.lock = threading.Lock()
        self.staged: dict[str, dict[str, object]] = {}
        self.bytes_staged = 0
        self.errors: list[str] = []

    def _copy_one(self, entry: dict[str, object], destination: Path,
                  admission, stop: threading.Event) -> tuple[int, str]:
        """Read this entry's bytes, write them, hash them; return size and digest.

        The temporary lives beside the final name so the publish is a rename
        within one dataset, and it is removed on any failure: a stage littered
        with half-copies would charge the tier for bytes no map ever names.
        """

        source = self.mounts.local(str(entry["path"]))
        want = int(entry["bytes"])
        offset = int(entry["offset"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.partial")
        digest = hashlib.sha256()
        written = 0
        # O_NOFOLLOW at the leaf and a regular-file check, for the reason the
        # prewarm loop gives: the manifest is submitter-supplied and a symlink
        # swapped in after validation must fail here rather than send this copy
        # reading something outside the mount the manifest declared.
        fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            if not statmod.S_ISREG(os.fstat(fd).st_mode):
                raise OSError(f"not a regular file: {source}")
            if offset:
                os.lseek(fd, offset, os.SEEK_SET)
            with open(temporary, "wb") as sink:
                buffer = bytearray(self.block)
                view = memoryview(buffer)
                while written < want and not stop.is_set():
                    if not admission.acquire(
                            self.limit, stop,
                            self.pacer.hold_s if self.pacer is not None else 0.25):
                        break
                    try:
                        if self.pacer is not None:
                            self.pacer.wait(stop)
                            if stop.is_set():
                                break
                        chunk = os.readv(fd, [view[:min(self.block, want - written)]])
                    finally:
                        admission.release()
                    if not chunk:
                        break
                    sink.write(view[:chunk])
                    digest.update(view[:chunk])
                    written += chunk
                sink.flush()
                os.fsync(sink.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        finally:
            os.close(fd)
        if written != want:
            temporary.unlink(missing_ok=True)
            raise OSError(f"short read on {source}: {written} of {want} bytes")
        os.replace(temporary, destination)
        return written, digest.hexdigest()

    def run(self, entries: list[dict[str, object]], *, whole: set[str],
            stop: threading.Event, on_entry=None) -> None:
        admission = prewarm_loop.Admission()
        self.limit = (self.pacer.depth if self.pacer is not None
                      else (lambda: self.workers))
        work: queuelib.Queue = queuelib.Queue()
        for entry in entries:
            work.put(entry)
        for _ in range(self.workers):
            work.put(None)

        def worker() -> None:
            while not stop.is_set():
                entry = work.get()
                if entry is None:
                    return
                path, offset = str(entry["path"]), int(entry["offset"])
                try:
                    relative = stage_relative(
                        path, offset, int(entry["bytes"]),
                        mount_prefix=self.mount_prefix, whole_file=path in whole)
                    destination = self.stage_root / relative
                    written, digest = self._copy_one(
                        entry, destination, admission, stop)
                except (OSError, ValueError) as exc:
                    with self.lock:
                        if len(self.errors) < 20:
                            self.errors.append(f"{os.path.basename(path)}: {exc}")
                    continue
                declared = entry.get("sha256")
                if isinstance(declared, str) and declared != digest:
                    # The manifest said what these bytes are and the copy is
                    # not them.  Publishing the entry would make the map a lie
                    # the consumer trusts in preference to the pool.
                    destination.unlink(missing_ok=True)
                    with self.lock:
                        if len(self.errors) < 20:
                            self.errors.append(
                                f"{os.path.basename(path)}: digest mismatch")
                    continue
                record = {
                    "stage_path": str(destination),
                    "bytes": written,
                    "offset": offset,
                    "sha256": digest,
                }
                with self.lock:
                    self.staged[residency_map.residency_map_key(path, offset)] = record
                    self.bytes_staged += written
                    # Finished, so forget it: the window is what is in flight,
                    # never the range.
                    if on_entry is not None:
                        on_entry(dict(self.staged))

        threads = [threading.Thread(target=worker, daemon=True)
                   for _ in range(self.workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        admission.close()


def load_manifest(cas_root: Path, action_key: str,
                  manifest_path: str | None) -> dict[str, object]:
    """The manifest this mover moves, from its own sealed request or a file."""

    if manifest_path:
        with open(manifest_path) as stream:
            return json.load(stream)
    request = prewarm_loop.sealed_request(cas_root, action_key)
    entry = prewarm_loop.manifest_input_of(request)
    if entry is None:
        raise SystemExit(
            f"action {action_key} seals no data manifest and no --manifest was given")
    manifest = prewarm_loop.load_manifest(cas_root, entry)
    if manifest is None:
        raise SystemExit(f"the data manifest of {action_key} could not be read")
    return manifest


def move(args, *, stop: threading.Event | None = None) -> dict[str, object]:
    """Stage the declared range and return the receipt, refusing an overrun."""

    stop = stop or threading.Event()
    cas_root = Path(args.cas_root)
    manifest = load_manifest(cas_root, args.action_key, args.manifest)
    mount_prefix = str(manifest["mount_prefix"])
    entries = prewarm_loop.manifest_read_entries(manifest)
    window = prewarm_loop.entries_between(
        entries, int(args.range_start_bytes), int(args.range_end_bytes))
    declared = int(args.range_end_bytes) - int(args.range_start_bytes)
    if declared <= 0:
        raise SystemExit("--range-end-bytes must be above --range-start-bytes")

    window_bytes = sum(int(entry["bytes"]) for entry in window)
    if window_bytes > declared:
        # Refuse before reading 34 GB rather than after.  ``entries_between``
        # includes an entry straddling the start whole, so a phase table whose
        # boundary cuts an entry would hand this mover more bytes than its
        # tokens reserved -- which is the overrun, found for free.
        raise SystemExit(
            f"residency_overran_reservation: the entries covering "
            f"[{args.range_start_bytes}, {args.range_end_bytes}) are "
            f"{window_bytes} bytes, past the {declared} this range reserved")

    pacer = prewarm_loop.pacer_from_args(args)
    if not getattr(args, "unpaced", False):
        prewarm_loop.require_storage_pacing(pacer)
    mounts = prewarm_loop.MountMap(args.mount or [])
    copier = _Copier(
        mounts=mounts, pacer=pacer, stage_root=Path(args.stage_root),
        mount_prefix=mount_prefix, block=args.block, workers=args.max_readers)
    whole = whole_file_paths(entries)

    residency_root = Path(args.residency_root)
    manifest_sha256 = args.manifest_sha256

    def publish(staged: dict[str, dict[str, object]]) -> None:
        """Republish the fragment as entries land, so a crash leaves a prefix."""

        residency_map.write_fragment(residency_root, {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": args.consumer_action_key,
            "mover_action_key": args.action_key,
            "tier_id": args.tier_id,
            "stage_root": str(args.stage_root),
            "manifest_sha256": manifest_sha256,
            "entries": staged,
        })

    if pacer is not None:
        pacer.begin_row(served_host=socket.gethostname())
    before = proc_io()
    started = time.time()
    copier.run(window, whole=whole, stop=stop,
               on_entry=None if args.no_incremental_fragment else publish)
    elapsed = max(1e-9, time.time() - started)
    after = proc_io()
    pacing = (pacer.report() if pacer is not None
              else prewarm_loop.inactive_pacing("no pacer"))

    overran = copier.bytes_staged > declared
    receipt: dict[str, object] = {
        "schema": pool.POOL_MOVE_SCHEMA_V1,
        "action_key": args.action_key,
        "consumer_action_key": args.consumer_action_key,
        "tier_id": args.tier_id,
        "stage_root": str(args.stage_root),
        "manifest_sha256": manifest_sha256,
        "range_start_bytes": int(args.range_start_bytes),
        "range_end_bytes": int(args.range_end_bytes),
        "range_bytes": declared,
        "bytes_staged": copier.bytes_staged,
        "entries_declared": len(window),
        "entries_staged": len(copier.staged),
        "complete": copier.bytes_staged == declared and not copier.errors,
        "seconds": round(elapsed, 3),
        # File-side, and named so: what the copy saw, which the ARC can answer
        # without the disks moving.  ``disk_pacing.mean_self_read_mb_s`` beside
        # it is the pool-side number, and the only one a tier mints from.
        "mb_per_s_file_side": round(copier.bytes_staged / 1e6 / elapsed, 1),
        "disk_pacing": pacing,
        "proc_io": _delta(before, after),
        "host": socket.gethostname(),
        "errors": copier.errors,
        "unix": time.time(),
    }
    if overran:
        # The tokens bound what the tier can hold.  Staging past them is the
        # one failure that cannot be left to the consumer to notice, so the
        # fragment goes and the receipt says why.
        receipt["refusal"] = "residency_overran_reservation"
        receipt["complete"] = False
        residency_map.fragment_path(
            residency_root, args.consumer_action_key,
            args.action_key).unlink(missing_ok=True)
    elif not args.no_incremental_fragment or copier.staged:
        publish(copier.staged)
    if not copier.staged and not overran:
        receipt["refusal"] = receipt.get("refusal") or "residency_moved_nothing"
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="copy one byte range of a data manifest's read order onto a stage tier")
    parser.add_argument("--pool-root", required=True,
                        help="the pull queue root this mover files its receipt and "
                             "its residency-map fragment under")
    parser.add_argument("--cas-root", required=True,
                        help="the content store this mover reads its own sealed "
                             "request and data manifest from")
    parser.add_argument("--action-key", required=True,
                        help="this mover's own action key; its receipt and its "
                             "residency-map fragment are filed under it")
    parser.add_argument("--consumer-action-key", required=True,
                        help="the action these bytes are staged for")
    parser.add_argument("--tier-id", required=True,
                        help="the storage tier these bytes are staged on, as "
                             "tier_loop announces it")
    parser.add_argument("--stage-root", required=True,
                        help="the staging dataset's mountpoint; every staged file "
                             "lives under it and the map refuses a path that does not")
    parser.add_argument("--manifest-sha256", required=True,
                        help="the manifest these bytes belong to; the consumer's "
                             "residency gate refuses a lead that staged another")
    parser.add_argument("--range-start-bytes", type=int, required=True,
                        help="first byte of the manifest read order this mover "
                             "stages (inclusive)")
    parser.add_argument("--range-end-bytes", type=int, required=True,
                        help="one past the last byte of the range this mover "
                             "stages (exclusive)")
    parser.add_argument("--manifest", default=None,
                        help="read the manifest from this file instead of the "
                             "sealed request (testing and one-off moves)")
    parser.add_argument("--residency-root", default=None,
                        help="where residency-map fragments are filed "
                             "(default <pool-root>/residency)")
    parser.add_argument("--mount", action="append", default=[],
                        help="local=declared, as the prewarm loop takes it")
    parser.add_argument("--block", type=int, default=prewarm_loop.BLOCK,
                        help="bytes per read; the buffer each worker holds")
    parser.add_argument("--readers", type=int, default=4,
                        help="copy depth while another client is reading the pool")
    parser.add_argument("--max-readers", type=int, default=16,
                        help="copy depth while the pool serves nobody but this move")
    parser.add_argument("--no-incremental-fragment", action="store_true",
                        help="write the fragment once at the end rather than as "
                             "entries land; the end state is the same")
    parser.add_argument("--receipt", default=None,
                        help="also write the receipt here (it is always filed "
                             "in the queue's movers directory)")
    # The pacer's own arguments, spelled here rather than imported: the same
    # names and defaults as the prewarm loop's, because both read the same
    # pool and a mover that paced differently would not be measuring the same
    # thing.  ``prewarm_loop.py`` is owned by #589 and is not edited to share
    # them; when that lands, one definition replaces these.
    parser.add_argument("--pace-pool", default="storage_pool",
                        help="the ZFS pool whose members the pacer watches")
    parser.add_argument("--disks", default="",
                        help="name the pacer's members directly when zpool "
                             "discovery cannot see them")
    parser.add_argument("--client-active-mb-s", type=float,
                        default=prewarm_loop.CLIENT_ACTIVE_MB_S,
                        help="a client reading at least this fast counts as "
                             "active, and the copy yields to it")
    parser.add_argument("--export-stats", default=prewarm_loop.EXPORT_STATS,
                        help="per-export NFS statistics the pacer attributes "
                             "client reads with")
    parser.add_argument("--nfsd-io", default=prewarm_loop.NFSD_IO,
                        help="server-side NFS byte counters the pacer samples")
    parser.add_argument("--max-util-pct", type=float, default=25.0,
                        help="hold the copy above this member utilization")
    parser.add_argument("--max-read-await-ms", type=float, default=10.0,
                        help="hold the copy above this member read latency")
    parser.add_argument("--max-backlog-ms", type=float, default=2000.0,
                        help="hold the copy above this member queue backlog")
    parser.add_argument("--pace-sample-s", type=float, default=0.25,
                        help="seconds between the pacer's device samples")
    parser.add_argument("--pace-hold-s", type=float, default=0.25,
                        help="seconds a held copy waits before asking again")
    parser.add_argument("--unpaced", action="store_true",
                        help="run with no pacer at all; for a fixture or a box "
                             "that serves nothing, never for the storage role")
    args = parser.parse_args(argv)
    if args.residency_root is None:
        args.residency_root = str(Path(args.pool_root) / pool.RESIDENCY)

    receipt = move(args)
    queue = pool.PoolQueue(Path(args.pool_root))
    queue.record_move(args.action_key, receipt)
    if args.receipt:
        with open(args.receipt, "w") as stream:
            json.dump(receipt, stream, indent=1, sort_keys=True)
            stream.write("\n")
    print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
    return 1 if receipt.get("refusal") else 0


if __name__ == "__main__":
    raise SystemExit(main())
