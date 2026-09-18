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
import resource
import socket
import stat as statmod
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

import prewarm_loop  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

#: A staged range that is not a whole file lives under this suffix, so the
#: name of one range can never be the name of another.
RANGE_SUFFIX = ".pbrange"

#: Seconds between republications of the residency fragment while a move runs.
#: See :func:`move`; a per-entry publish costs an NFS round trip per file and
#: was measured at 75 KB/s against 466 MB/s.
FRAGMENT_PUBLISH_S = 5.0


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


def cpu_seconds(*, usage=resource.getrusage) -> float:
    """This process and its children's CPU seconds so far, user plus system.

    Read at both ends of the copy and differenced, for the same reason
    ``peak_rss_bytes`` is in the receipt: the next submission's ``cpu`` demand
    should be a measurement of what a mover cost rather than a number someone
    chose (``pb_demand_must_be_measured_not_habitual``).  Children are counted
    because the copy's digest work is where the CPU goes and a future mover may
    spawn it; today it is threads, which ``RUSAGE_SELF`` already covers.
    """

    total = 0.0
    for who in (resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN):
        sample = usage(who)
        total += float(sample.ru_utime) + float(sample.ru_stime)
    return total


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
        #: Bumped under the lock on every landed entry, so a fragment written
        #: outside the lock can say which snapshot it is and an older one can
        #: be discarded rather than written over a newer one.
        self.generation = 0

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
        computed = digest.hexdigest()
        declared = entry.get("sha256")
        if isinstance(declared, str) and declared != computed:
            # Before the rename, not after.  These bytes are not the manifest's
            # bytes, and the rename is the publish: checking afterwards means
            # unlinking a destination another mover may have staged correctly
            # -- two movers cover one entry whenever a consumer is re-dispatched
            # under a new key, or whenever an entry straddles two adjacent
            # windows -- which leaves that mover's fragment naming bytes that
            # are gone.  Nothing outside this temporary is touched.
            temporary.unlink(missing_ok=True)
            raise OSError(f"digest mismatch on {source}: manifest says "
                          f"{declared[:12]}, the copy is {computed[:12]}")
        os.replace(temporary, destination)
        return written, computed

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
                record = {
                    "stage_path": str(destination),
                    "bytes": written,
                    "offset": offset,
                    "sha256": digest,
                }
                with self.lock:
                    self.staged[residency_map.residency_map_key(path, offset)] = record
                    self.bytes_staged += written
                    self.generation += 1
                    # Snapshot under the lock, write outside it.  Holding the
                    # lock across an NFS fsync serializes every other worker
                    # behind one round trip; the generation is what keeps an
                    # older snapshot from landing on top of a newer one once
                    # they are no longer ordered by the lock.
                    snapshot = (dict(self.staged), self.generation) if on_entry else None
                if snapshot is not None:
                    try:
                        on_entry(*snapshot)
                    except (OSError, ValueError) as exc:
                        # A fragment that will not write is a degradation, not
                        # a reason to stop copying, and least of all a reason
                        # to let this thread die inside the lock: that killed
                        # every worker in turn on its next landed entry and
                        # still reported ``errors: []``.  The entry stays in
                        # ``staged`` -- it is on the stage -- so the final
                        # publish names it or fails loudly.
                        with self.lock:
                            if len(self.errors) < 20:
                                self.errors.append(f"fragment publication: {exc}")

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
        # The same reader the sealed path gets, not a bare ``json.load``:
        # everything downstream assumes core already refused these shapes, so
        # a zero-byte entry reaches the map's ``_positive`` check and a
        # repeated (path, offset) pair stages two copies under one key.
        # "Testing and one-off moves" is exactly where a hand-written manifest
        # enters, and it reads gzip for free.
        try:
            return pb.load_data_manifest(manifest_path)
        except (pb.ActionContractError, pb.CASTamperError) as exc:
            raise SystemExit(f"{manifest_path}: {exc}") from None
    request = prewarm_loop.sealed_request(cas_root, action_key)
    entry = prewarm_loop.manifest_input_of(request)
    if entry is None:
        raise SystemExit(
            f"action {action_key} seals no data manifest and no --manifest was given")
    manifest = prewarm_loop.load_manifest(cas_root, entry)
    if manifest is None:
        raise SystemExit(f"the data manifest of {action_key} could not be read")
    return manifest


def served_for(args) -> dict[str, object]:
    """The addresses whose reads are this move's own, and how they were found.

    Derived, never configured: the consumer's claim record names its box, and
    that box's worker offer names the client addresses the server's per-client
    counter is keyed by.  Both are records the fleet already writes.  A broken
    link leaves the set empty and the pacer protecting every client, and the
    reason travels into the receipt so the slow case is never silent.
    """

    if args.served_host or args.served_address:
        return {"served_host": args.served_host or "",
                "served_addresses": tuple(args.served_address or ()),
                "served_reason": "named on the command line"}
    try:
        queue = pool.PoolQueue(Path(args.pool_root))
        claim = pool._read_json(
            queue.item_path(pool.CLAIMED, str(args.consumer_action_key)))
    except (OSError, pool.PoolContractError):
        claim = None
    if not isinstance(claim, dict):
        return {"served_host": "", "served_addresses": (),
                "served_reason": "consumer is not claimed, so every client is "
                                 "a client to protect"}
    host = prewarm_loop.claimed_host_of(claim)
    found = prewarm_loop.served_addresses(queue, host)
    return {"served_host": found["served_host"],
            "served_addresses": found["served_addresses"],
            "served_reason": str(found["served_reason"])}


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

    # Two entries that derive one staged name would let the later copy
    # overwrite the earlier while both fragments vouch for it, and compose
    # cannot see it: the two map keys differ, so the same-key conflict never
    # fires.  Derived range names and declared paths share one namespace under
    # the stage root, so a manifest alone can reach the collision.
    whole = whole_file_paths(entries)
    destinations: dict[str, str] = {}
    for entry in window:
        path, offset = str(entry["path"]), int(entry["offset"])
        relative = stage_relative(path, offset, int(entry["bytes"]),
                                  mount_prefix=mount_prefix,
                                  whole_file=path in whole)
        claimed = destinations.get(relative)
        if claimed is not None:
            raise SystemExit(
                f"residency_destination_collision: {claimed!r} and {path!r} "
                f"both stage as {relative!r}; nothing was copied")
        destinations[relative] = path

    pacer = prewarm_loop.pacer_from_args(args)
    if not getattr(args, "unpaced", False):
        prewarm_loop.require_storage_pacing(pacer)
    mounts = prewarm_loop.MountMap(args.mount or [])
    copier = _Copier(
        mounts=mounts, pacer=pacer, stage_root=Path(args.stage_root),
        mount_prefix=mount_prefix, block=args.block, workers=args.max_readers)

    residency_root = Path(args.residency_root)
    manifest_sha256 = args.manifest_sha256

    last_published = [0.0]
    last_generation = [0]
    publish_lock = threading.Lock()

    def publish(staged: dict[str, dict[str, object]], generation: int = 0,
                *, force: bool = False) -> None:
        """Republish the fragment as entries land, so a crash leaves a prefix.

        Rate-limited, and that limit is the difference between a mover that
        works and one that does not.  The fragment is a single document that
        grows with every entry, it lives on the shared mount, and each publish
        is a write plus an ``fsync`` plus a rename over NFS.  Writing it once
        per entry is quadratic in bytes and pays an NFS round trip per file:
        measured on dl380g10, a range of 9,372 entries ran at **75 KB/s** with
        the pool idle at 2% utilization, while a range of 61 entries of the
        same total size ran at **466 MB/s**.  The cost was never the copy.  A
        crash still leaves a valid prefix; it is at most a couple of seconds
        shorter than the one a per-entry publish would have left, and the
        entries it omits are re-staged by the rerun.
        """

        with publish_lock:
            now = time.monotonic()
            if generation and generation <= last_generation[0]:
                # A snapshot older than the one already on disk.  Writing it
                # would take entries back out of a published fragment, which
                # is the one thing a fragment may never do.
                return
            if not force and now - last_published[0] < FRAGMENT_PUBLISH_S:
                return
            last_published[0] = now
            last_generation[0] = max(last_generation[0], generation)
            residency_map.write_fragment(residency_root, {
                "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                "consumer_action_key": args.consumer_action_key,
                "mover_action_key": args.action_key,
                "tier_id": args.tier_id,
                "stage_root": str(args.stage_root),
                "manifest_sha256": manifest_sha256,
                "entries": staged,
            })

    served = served_for(args)
    if pacer is not None:
        # Whose reads are this move's own, and whose are a client to protect.
        # The self is the *consumer's* box, not this one: the point of the
        # split is not to protect a consumer from its own staging.  Naming
        # nobody is the prewarm loop's documented "no self" case, where every
        # client counts and the row is attributed to none of them -- which is
        # also why such a receipt reports a pool-side rate of zero and mints
        # no fill tokens.
        pacer.begin_row(served_host=str(served["served_host"]),
                        served_addresses=tuple(served["served_addresses"]),
                        served_reason=str(served["served_reason"]))
    before = proc_io()
    cpu_before = cpu_seconds()
    started = time.time()
    copier.run(window, whole=whole, stop=stop,
               on_entry=None if args.no_incremental_fragment else publish)
    elapsed = max(1e-9, time.time() - started)
    after = proc_io()
    cpu_used = max(0.0, cpu_seconds() - cpu_before)
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
        # without the disks moving.  ``disk_pacing.mean_pool_read_mb_s`` beside
        # it is the pool-side number, summed off the members' own sector
        # counters, and the only one a tier mints from.
        "mb_per_s_file_side": round(copier.bytes_staged / 1e6 / elapsed, 1),
        "disk_pacing": pacing,
        "served": {k: list(v) if isinstance(v, tuple) else v
                   for k, v in served.items()},
        "proc_io": _delta(before, after),
        # What this mover actually cost in memory, so the next
        # submission's ``mem_gb`` is a measurement rather than a habit
        # (``pb_demand_must_be_measured_not_habitual``).  The copy is a
        # bounded window -- ``--readers`` buffers of ``--block`` -- so
        # this should stay flat as the range grows, and a receipt that
        # says otherwise is the bug report.
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        # The other half of the same story, and the one #603 is about: a
        # mover's sha256 is CPU-bound, it declared no ``cpu`` at all, and
        # ``adaptive_cpu`` reads an absent ``cpu`` as unknown CPU use and
        # serializes it on an otherwise idle host
        # (``adaptive_cpu.py`` ``unbounded_cpu_not_exclusive``).  Differenced
        # across the copy, so the manifest read and the fragment publishes
        # outside it are not charged to the rate.  ``cpu_seconds / seconds``
        # is mean parallelism -- what this copy actually kept busy -- which is
        # the number a next submission can declare and the controller can
        # learn down from.
        "cpu_seconds": round(cpu_used, 3),
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
    elif copier.staged:
        publish(copier.staged, force=True)
    if not copier.staged and not overran:
        receipt["refusal"] = receipt.get("refusal") or "residency_moved_nothing"
    return receipt


def own_action_key(declared: str | None) -> str:
    """This node's own action key, from the flag or from the launcher.

    A movement node files its receipt and holds its tier tokens under its own
    key, and that key is ``canonical_sha256`` of the action body --- so a
    ``--action-key`` sealed into the argv would be hashed into the very value
    it states, and no fixed point exists.  The launcher sets
    :data:`prismabuild.core.ACTION_KEY_ENV` for every action it starts, from
    the action in hand, which is the one place the answer is already known.
    The flag stays, because a direct run and every test needs to say which key
    it is acting as.
    """

    key = declared or os.environ.get(pb.ACTION_KEY_ENV) or ""
    if len(key) != 64 or any(character not in "0123456789abcdef" for character in key):
        raise SystemExit(
            f"pass --action-key, or run under a launcher that sets "
            f"{pb.ACTION_KEY_ENV}; got {key!r}")
    return key


def build_parser() -> argparse.ArgumentParser:
    """Every flag this tool takes, in one place a test can also ask.

    Extracted from ``main`` because the alternative was a test fixture that
    re-listed the defaults by hand: when the pacer's ``--served-host`` was
    added here and not there, every test that drives ``move`` started failing
    on an attribute the real tool always supplies.  A fixture built from this
    parser cannot drift from it.
    """

    parser = argparse.ArgumentParser(
        description="copy one byte range of a data manifest's read order onto a stage tier")
    parser.add_argument("--pool-root", required=True,
                        help="the pull queue root this mover files its receipt and "
                             "its residency-map fragment under")
    parser.add_argument("--cas-root", default="",
                        help="the content store this mover reads its own sealed "
                             "request and data manifest from; not needed when "
                             "--manifest names the file directly")
    parser.add_argument("--action-key", default=None,
                        help="this mover's own action key; its receipt and its "
                             "residency-map fragment are filed under it. "
                             f"Defaults to {pb.ACTION_KEY_ENV}, which the "
                             "launcher sets, because a key cannot be sealed "
                             "into the argv it is computed from")
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
    parser.add_argument("--served-host", default="",
                        help="the box whose reads are this move's own; derived "
                             "from the consumer's claim when not given")
    parser.add_argument("--served-address", action="append", default=[],
                        help="a client address of the served box; derived from "
                             "its worker offer when not given")
    parser.add_argument("--unpaced", action="store_true",
                        help="run with no pacer at all; for a fixture or a box "
                             "that serves nothing, never for the storage role")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.action_key = own_action_key(args.action_key)
    if args.residency_root is None:
        args.residency_root = str(Path(args.pool_root) / pool.RESIDENCY)

    receipt = move(args)
    queue = pool.PoolQueue(Path(args.pool_root))
    queue.record_move(args.action_key, receipt)
    if args.receipt:
        # The atomic writer every other fleet record uses: a reader of this
        # path must never be able to observe half a document.
        pool._write_json_atomic(Path(args.receipt), receipt)
    print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
    return 1 if receipt.get("refusal") else 0


if __name__ == "__main__":
    raise SystemExit(main())
