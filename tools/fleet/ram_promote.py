#!/usr/bin/env python3
"""Promote one staged range from the SSD stage into the tmpfs (#640).

The ram tier's movement node.  The stage mover copies pool -> stage; this
copies stage -> ram, and never pool -> ram directly: the SSD stage is the
durable tier a verdict gates on, the tmpfs is a performance tier in front of
it, and a promotion whose source was the pool would put bytes in RAM that
the map cannot vouch for on the stage.  The window publishes a promotion
only for a phase whose stage range has landed, for the same reason.

Everything about the destination is the stage's own shape, transferred
intact: the copy lands under the *same content-addressed names* -- the same
relative paths, the same digests -- and is written beside its final name and
renamed into place, so a partial file is never visible under the name a
consumer reads.  The map fragment it files names the ram path for exactly
the entries the stage already vouched for, which is what lets the composed
map carry one entry with two servants: the stage path it always had, and the
``ram_path`` beside it.

Two things are deliberately *not* here, and both are the stage mover's:

* **No pacer, no fill demand, no warm read-back.**  The source is the stage
  on this same box -- NVMe into RAM -- so no pool is drained, no client is
  protected, and nothing is warmed: the bytes land in the tier the warm was
  a substitute for.
* **No incremental fragment.**  A crashed promotion holds no tokens past its
  reap (its receipt never landed), so its files are bytes no record names.
  A retry of the range -- the same action key on a new attempt -- adopts
  each one whose bytes hash to the declared digest, before any copy and
  without the publication grace (#1081); anything else it copies again.
  A prefix is the stage's purchase; the ram tier buys whole ranges or
  nothing.

The epoch is read, never created: the tier loop stamps the mount's marker at
bootstrap, and a promotion that finds none refuses -- a range nobody can
place in time is not one to stage, because after the next reboot it would
read as resident while the tmpfs holds nothing.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import resource
import socket
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

import prewarm_loop  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import reader_lease  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

from stage_move import (  # noqa: E402
    _Copier, _StagedPublisher, _delta, cpu_seconds, load_manifest,
    own_action_key, proc_io, stage_relative,
)


def _refused(args, reason: str, **extra) -> dict[str, object]:
    """A receipt that stages nothing, says why, and never writes a fragment."""

    receipt: dict[str, object] = {
        "schema": pool.POOL_MOVE_SCHEMA_V1,
        "action_key": args.action_key,
        "consumer_action_key": args.consumer_action_key,
        "tier_id": args.tier_id,
        "ram_root": str(args.ram_root),
        "source_stage_root": str(args.source_stage_root),
        "manifest_sha256": args.manifest_sha256,
        "range_start_bytes": int(args.range_start_bytes),
        "range_end_bytes": int(args.range_end_bytes),
        "range_bytes": int(args.range_end_bytes) - int(args.range_start_bytes),
        "bytes_staged": 0,
        "entries_declared": 0,
        "entries_staged": 0,
        "complete": False,
        "refusal": reason,
        "host": socket.gethostname(),
        "errors": [],
        "unix": time.time(),
    }
    receipt.update(extra)
    return receipt


def promote(args, *, stop=None) -> dict[str, object]:
    """Copy the declared range from the stage into the tmpfs; return the receipt."""

    stop = stop or threading.Event()
    manifest = load_manifest(Path(args.cas_root), args.action_key, args.manifest)
    mount_prefix = str(manifest["mount_prefix"])
    entries = prewarm_loop.manifest_read_entries(manifest)
    window = prewarm_loop.entries_between(
        entries, int(args.range_start_bytes), int(args.range_end_bytes))
    declared = int(args.range_end_bytes) - int(args.range_start_bytes)
    if declared <= 0:
        raise SystemExit("--range-end-bytes must be above --range-start-bytes")
    window_bytes = sum(int(entry["bytes"]) for entry in window)
    if window_bytes > declared:
        # The same refusal the stage mover makes: the tokens bound what the
        # tier can hold, and staging past them breaks the accounting that
        # keeps the tmpfs from overfilling.
        raise SystemExit(
            f"residency_overran_reservation: the entries covering "
            f"[{args.range_start_bytes}, {args.range_end_bytes}) are "
            f"{window_bytes} bytes, past the {declared} this range reserved")

    # The epoch first, before a byte is read: a promotion nobody can place in
    # time is not one to stage, and the tmpfs empties on reboot while this
    # receipt survives on the shared mount.
    epoch = storage_tiers.read_ram_epoch(args.ram_root)
    if epoch is None:
        return _refused(args, "ram_epoch_absent", epoch=None)
    source = Path(args.source_stage_root)
    if not source.is_dir():
        # Pool -> ram directly is the one road this refuses: the stage is the
        # durable tier and the promotion's only source.
        return _refused(args, "ram_source_stage_absent",
                        epoch=str(epoch["epoch"]))

    # The source coverage proof, before a byte is read: the window may span
    # several stage movers, so every entry must be shown covered by published
    # stage material with matching length (and digest where the manifest
    # declares one).  Missing coverage refuses boundedly here -- never a
    # faked full file, never the pool path at the manifest offset.  Proof
    # only, no durable pin: this promotion's live claim (parsed by the stage
    # egress from the sealed request) protects the sources through the copy,
    # while a pin would outlive a crashed promotion with no terminal to
    # contain it against.
    residency_root = Path(args.residency_root) if args.residency_root else None
    queue = pool.PoolQueue(Path(args.pool_root))
    residence = (Path(residency_root) if residency_root is not None
                 else queue.root / pool.RESIDENCY)
    covers: list[dict[str, str]] = []
    stage_tier: str | None = None
    for fragment in residency_map.read_fragments(
            residence, args.consumer_action_key):
        if (str(fragment.get("tier_id") or "") == str(args.tier_id)
                or str(fragment.get("manifest_sha256") or "")
                != str(args.manifest_sha256)):
            continue
        if (os.path.normpath(str(fragment.get("stage_root") or ""))
                != os.path.normpath(str(source))):
            continue
        mover = str(fragment.get("mover_action_key") or "")
        if stage_tier is None:
            stage_tier = str(fragment.get("tier_id") or "")
        elif str(fragment.get("tier_id") or "") != stage_tier:
            return _refused(args, "source-spans-tiers",
                            epoch=str(epoch["epoch"]))
        covers.append({"mover_action_key": mover,
                       "manifest_sha256": str(args.manifest_sha256)})
    if not covers:
        return _refused(args, "source-coverage-gap",
                        epoch=str(epoch["epoch"]))
    expected = {residency_map.residency_map_key(
        str(entry["path"]), int(entry["offset"])): {
            "bytes": int(entry["bytes"]), "sha256": entry.get("sha256")}
        for entry in window}
    proof = reader_lease.acquire(
        queue, consumer_action_key=args.consumer_action_key,
        attempt={"nonce": str(args.action_key), "scope_id": "ram-promotion"},
        tier_id=stage_tier or "", epoch="",
        span={"start_bytes": int(args.range_start_bytes),
              "end_bytes": int(args.range_end_bytes)},
        holder={"host": socket.gethostname(), "pid": os.getpid()},
        acquire_token=f"promote:{args.action_key}", covers=covers,
        expected=expected, residency_root=residence, file_pin=False)
    if not proof.get("ok"):
        return _refused(args, str(proof.get("refusal")),
                        epoch=str(epoch["epoch"]))
    proven = {str(item["key"]): item for item in proof["entries"]}  # type: ignore[index]

    # The same destination-collision check the stage mover makes: two entries
    # that derive one staged name would let the later copy overwrite the
    # earlier while both fragments vouch for it.
    destinations: dict[str, str] = {}
    for entry in window:
        path, offset = str(entry["path"]), int(entry["offset"])
        relative = stage_relative(path, offset, int(entry["bytes"]),
                                  mount_prefix=mount_prefix)
        # The staged name is taken from the proof, never recomputed on
        # trust: a replace between the proof and this loop must refuse,
        # not read a new file under an old name.
        key = residency_map.residency_map_key(path, offset)
        pinned = proven.get(key)
        if (pinned is None or os.path.normpath(
                str(source / relative)) != os.path.normpath(
                    str(pinned["stage_path"]))):
            return _refused(args, "source-identity-changed",
                            epoch=str(epoch["epoch"]))
        if entry.get("sha256") is None:
            # A manifest-null digest borrows the stage sidecar's verified
            # binding, so the copy below verifies against content evidence
            # rather than copying unchecked.
            entry["sha256"] = str(pinned["sha256"])
        claimed = destinations.get(relative)
        if claimed is not None:
            raise SystemExit(
                f"residency_destination_collision: {claimed!r} and {path!r} "
                f"both stage as {relative!r}; nothing was copied")
        destinations[relative] = path

    # The stage root stands in for the manifest's shared mount: the same
    # relative names the stage mover wrote are the ones read here and written
    # under the ram root, which is what makes the copy content-addressed
    # rather than merely present.
    mounts = prewarm_loop.MountMap([f"{mount_prefix}={str(source)}"])
    copier = _Copier(
        mounts=mounts, pacer=None, stage_root=Path(args.ram_root),
        mount_prefix=mount_prefix, block=args.block, workers=args.max_readers,
        owner=str(args.action_key),
        # The source is the stage, not the pool: split ranges are read from
        # the staged names the stage mover wrote, from byte zero.
        source_stage_root=source,
        publisher=_StagedPublisher(
            queue=queue, stage_root=Path(args.ram_root),
            residency_root=residence,
            mover_action_key=str(args.action_key),
            manifest_sha256=str(args.manifest_sha256),
            tier_id=str(args.tier_id), cas_root=str(args.cas_root)))

    before = proc_io()
    cpu_before = cpu_seconds()
    started = time.time()
    # The start gate: order this copy's first rename against an egress that
    # may be snapshotting right now.  The claim already exists, so an egress
    # that snapshots after this point attributes the copy through the claim;
    # one that snapshotted before waits out here until its delete completes.
    # Acquired and released -- nothing is held during the copy itself.
    # Timed, because a wait here is an egress's hold paid by this copy (#988).
    gate_started = time.perf_counter()
    pool.PoolQueue(Path(args.pool_root)).ownership_start_gate(args.ram_root)
    start_gate_wait_s = time.perf_counter() - gate_started
    copier.run(window, stop=stop)
    elapsed = max(1e-9, time.time() - started)
    after = proc_io()
    cpu_used = max(0.0, cpu_seconds() - cpu_before)

    overran = copier.bytes_staged > declared
    receipt: dict[str, object] = {
        "schema": pool.POOL_MOVE_SCHEMA_V1,
        "action_key": args.action_key,
        "consumer_action_key": args.consumer_action_key,
        "tier_id": args.tier_id,
        "ram_root": str(args.ram_root),
        "source_stage_root": str(args.source_stage_root),
        "manifest_sha256": args.manifest_sha256,
        "range_start_bytes": int(args.range_start_bytes),
        "range_end_bytes": int(args.range_end_bytes),
        "range_bytes": declared,
        "bytes_staged": copier.bytes_staged,
        "entries_declared": len(window),
        "entries_staged": len(copier.staged),
        "complete": copier.bytes_staged == declared and not copier.errors,
        "epoch": str(epoch["epoch"]),
        "seconds": round(elapsed, 3),
        # The stage ownership lock's queue at the start gate (#988).
        "start_gate_wait_s": round(start_gate_wait_s, 6),
        # File-side and named so: what the copy saw.  No pool-side pacing
        # block rides with it, because no pool was drained -- the fill
        # measurement reads only receipts that carry one.
        "mb_per_s_file_side": round(copier.bytes_staged / 1e6 / elapsed, 1),
        "proc_io": _delta(before, after),
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "cpu_seconds": round(cpu_used, 3),
        "host": socket.gethostname(),
        "errors": copier.errors,
        # Thread-seconds per phase and how each entry ended, the stage
        # mover's own record (#981): a restart that adopts its last
        # attempt's copies shows here as outcomes without copy phases.
        "phase_timings": copier.clock.report(),
        "unix": time.time(),
    }
    material_generation = reader_lease.mint_generation()
    if overran:
        receipt["refusal"] = "residency_overran_reservation"
        receipt["complete"] = False
        residency_map.fragment_path(
            Path(args.residency_root), args.consumer_action_key,
            args.action_key).unlink(missing_ok=True)
        reader_lease.material_path(
            residence, args.consumer_action_key,
            args.action_key).unlink(missing_ok=True)
    elif copier.staged:
        # Once, at the end, on purpose: see the module docstring.  The
        # fragment carries the epoch so every reader of the map can tell a
        # current promotion from one the reboot deleted.
        residency_map.write_fragment(args.residency_root, {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": args.consumer_action_key,
            "mover_action_key": args.action_key,
            "tier_id": args.tier_id,
            "stage_root": str(args.ram_root),
            "epoch": str(epoch["epoch"]),
            "manifest_sha256": args.manifest_sha256,
            "entries": copier.staged,
        })
        # The fragment goes first, then the sidecar that dates it (same
        # crash order as the stage mover: a vouch without a date is safe
        # and healed by rerun).
        if copier.sidecar:
            reader_lease.write_material(
                residence,
                consumer_action_key=args.consumer_action_key,
                mover_action_key=args.action_key,
                tier_id=args.tier_id, stage_root=str(args.ram_root),
                manifest_sha256=args.manifest_sha256,
                generation=material_generation, entries=copier.sidecar,
                epoch=str(epoch["epoch"]))
    receipt["material_generation"] = material_generation
    receipt["source_covers"] = proof.get("covers")
    if not copier.staged and not overran:
        receipt["refusal"] = receipt.get("refusal") or "residency_moved_nothing"
    return receipt


def build_parser() -> argparse.ArgumentParser:
    """Every flag this tool takes, in one place a test can also ask."""

    parser = argparse.ArgumentParser(
        description="copy one staged range from the SSD stage into the ram tier")
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
                        help="the action these bytes are promoted for")
    parser.add_argument("--tier-id", required=True,
                        help="the ram tier these bytes land on, as tier_loop "
                             "announces it (ram:<host>)")
    parser.add_argument("--ram-root", required=True,
                        help="the tmpfs mountpoint this promotion copies into; "
                             "the epoch marker lives at its root and the "
                             "fragment's entries live under it")
    parser.add_argument("--source-stage-root", required=True,
                        help="the stage dataset's mountpoint this promotion "
                             "copies from; the promotion's only source, never "
                             "the pool")
    parser.add_argument("--manifest-sha256", required=True,
                        help="the manifest these bytes belong to; the consumer's "
                             "residency gate refuses a lead that staged another")
    parser.add_argument("--range-start-bytes", type=int, required=True,
                        help="first byte of the manifest read order this mover "
                             "promotes (inclusive)")
    parser.add_argument("--range-end-bytes", type=int, required=True,
                        help="one past the last byte of the range this mover "
                             "promotes (exclusive)")
    parser.add_argument("--manifest", default=None,
                        help="read the manifest from this file instead of the "
                             "sealed request (testing and one-off moves)")
    parser.add_argument("--residency-root", default=None,
                        help="where residency-map fragments are filed "
                             "(default <pool-root>/residency)")
    parser.add_argument("--block", type=int, default=prewarm_loop.BLOCK,
                        help="bytes per read; the buffer each worker holds")
    parser.add_argument("--readers", type=int, default=4,
                        help="copy width; accepted for parity with the stage "
                             "mover, which paces a pool this mover never touches")
    parser.add_argument("--max-readers", type=int, default=16,
                        help="copy width; the stage serves nobody but this move")
    parser.add_argument("--receipt", default=None,
                        help="also write the receipt here (it is always filed "
                             "in the queue's movers directory)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.action_key = own_action_key(args.action_key)
    if args.residency_root is None:
        args.residency_root = str(Path(args.pool_root) / pool.RESIDENCY)

    receipt = promote(args)
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
