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
from collections.abc import Mapping
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
from prismabuild import reader_lease  # noqa: E402
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


#: Grace for a late fragment or an in-flight publisher before an unproven
#: staged name settles to refuse-or-heal.  Every wait sleeps outside the
#: ownership lock in short polls; nothing about the timeout grants
#: permission -- expiry re-verifies, and only re-verified positive absence
#: heals.  Covers the incremental fragment rate limit (FRAGMENT_PUBLISH_S)
#: with wide margin; a publisher slower than this still converges, through
#: the stall policy's retry, never through replacement.
_PUBLISH_GRACE_S = 30.0
_PUBLISH_POLL_S = 0.25

#: Residency subdirectories that never hold consumer fragments.
_NON_FRAGMENT_DIRS = frozenset({"leases", "material"})


def _origin_id_of(path_str: str) -> str | None:
    """``st_size:st_mtime_ns:st_ino`` of a copy source, or ``None``.

    The same three numbers :mod:`prewarm_loop` files as
    ``STAGE_SOURCE_XATTR`` at copy time: the existing origin
    change-detection evidence, read here rather than reinvented.
    """

    try:
        sample = os.stat(path_str)
    except OSError:
        return None
    if not statmod.S_ISREG(sample.st_mode):
        return None
    return f"{sample.st_size}:{sample.st_mtime_ns}:{sample.st_ino}"


class _StagedPublisher:
    """Linearize staged-path publication under the stage ownership lock.

    Overlapping consumers stage one content-addressed name for the same
    bytes; an unconditional rename per copy mints a fresh inode/mtime
    and invalidates every other consumer's published material identity
    (and any live cover proof or pin fenced on it). The payload copy
    into the private owner-keyed temp stays outside every lock; this
    gate runs immediately before destination publication, under the
    existing stage-ownership exclusion (the same lock egress holds
    across snapshot-to-release), and decides metadata-only:

    * absent destination: this copy publishes (concurrent absentees
      serialize here, so exactly one first publication wins; a loser
      that already passed finds a present path below and converges);
    * present with an adoption proof -- some consumer's fragment entry
      naming this path with equal bytes, a material sidecar digest that
      matches (the declared digest; the copier's own computed digest
      for digest-less manifests, which pin no content; or the origin
      fast path below), and a live stat equal to the sidecar's file
      identity: adopt the published incarnation, discard the temp.
      Unchanged bytes keep every live pin valid, so no pin census is
      needed on this path;
    * present and provably different -- a sidecar digest that matches
      neither the declared nor the computed digest, or a live stat the
      sidecar no longer names: refuse at once, without replacing.
      A shared name with divergent content is a conflict for an owner
      to resolve, never a blind overwrite;
    * present but unproven, live-pinned, or unreadable: refuse at once
      for pins and unreadable proof state; otherwise wait out the grace
      for a late fragment or an in-flight publisher, then refuse --
      deferring to the action stall policy's retry, which adopts once
      the proof lands.  No timeout ever permits replacement.

    The single exception is re-verified positive absence: no fragment
    names the path, no pin refs it, no live mover claim (own excluded)
    covers it, no sibling copy is in flight, and every census read
    clean -- re-checked after the grace, not merely elapsed.  Only then
    does the copy heal the name as orphan/crash residue.  A live or
    unknown publisher always means wait/refuse, never replace.

    Digest-less (dev/null) manifests converge like the rest: the origin
    fast path adopts without copying where the published file still
    carries the source identity this copy's source stats today
    (``user.pbstage.source``, the prewarm loop's existing evidence);
    otherwise the necessary private copy's own computed digest is
    compared against the stored material proof at final publication --
    the existing file is never rehashed.  No material schema change:
    origin provenance rides the existing xattr where the filesystem
    carries it, and the digest comparison where it does not.

    Never replaces an occupied path except into proven absence (first
    publication) or re-verified orphan residue.  Returns ``(written,
    digest, identity)`` like a copy; raises ``OSError`` on refuse/stop,
    which the worker files as an entry error exactly like a digest
    mismatch.
    """

    def __init__(self, *, queue, stage_root, residency_root,
                 consumer_action_key: str, mover_action_key: str,
                 manifest_sha256: str, tier_id: str, cas_root) -> None:
        self.queue = queue
        self.stage_root = Path(stage_root)
        self.residency_root = Path(residency_root)
        self.consumer = consumer_action_key
        self.mover = mover_action_key
        self.manifest_sha256 = manifest_sha256
        self.tier_id = tier_id
        self.cas_root = cas_root

    def try_adopt(self, entry: dict[str, object], destination: Path,
                  source_id: str | None = None,
                  ) -> tuple[int, str, dict[str, int]] | None:
        """Adopt an already-published incarnation without copying, if proven.

        Metadata-only, under one ownership-lock hold: the same proof the
        publish gate requires, minus the copy-compare path (no computed
        digest exists yet).  Returns ``(want, digest, file_id)`` or
        ``None`` to proceed with the copy.  Raises ``OSError`` for
        provably divergent bytes, failing fast before any copy.
        """

        want = int(entry["bytes"])
        declared = entry.get("sha256")
        norm = os.path.normpath(str(destination))
        try:
            present = os.lstat(destination)
        except OSError:
            return None
        if not statmod.S_ISREG(present.st_mode):
            return None
        with self.queue.stage_ownership_lock(str(self.stage_root)):
            proof, standing, detail = self._proof_search(
                norm, want, declared, source_id=source_id)
            if standing == "unknown":
                return None
            if standing == "divergent":
                raise OSError(detail)
            if proof is None:
                return None
            return want, proof[0], proof[1]

    def publish(self, entry: dict[str, object], destination: Path,
                temp_path: Path, computed: str,
                stop: threading.Event | None = None,
                source_id: str | None = None,
                ) -> tuple[int, str, dict[str, int] | None]:
        """Decide one entry's publication; copy already verified in temp."""

        want = int(entry["bytes"])
        declared = entry.get("sha256")
        deadline = time.monotonic() + _PUBLISH_GRACE_S
        while True:
            now = time.monotonic()
            with self.queue.stage_ownership_lock(str(self.stage_root)):
                verdict = self._decide(
                    destination, want, declared, computed, source_id,
                    heal=now >= deadline)
                if verdict[0] == "replace":
                    os.replace(temp_path, destination)
                    return want, computed, self._identity(destination)
                if verdict[0] == "adopt":
                    temp_path.unlink(missing_ok=True)
                    return want, verdict[1], verdict[2]
                if verdict[0] == "refuse":
                    temp_path.unlink(missing_ok=True)
                    raise OSError(verdict[1])
            if stop is not None and stop.is_set():
                temp_path.unlink(missing_ok=True)
                raise OSError(f"stopping before {destination} publishes")
            time.sleep(_PUBLISH_POLL_S)

    @staticmethod
    def _identity(path: Path) -> dict[str, int] | None:
        try:
            info = os.stat(path)
            return {"ino": info.st_ino, "size": info.st_size,
                    "mtime_ns": info.st_mtime_ns,
                    "ctime_ns": int(getattr(info, "st_ctime_ns", 0))}
        except OSError:
            return None

    def _decide(self, destination: Path, want: int, declared: object,
                computed: str, source_id: str | None, heal: bool,
                ) -> tuple:
        norm = os.path.normpath(str(destination))
        try:
            present = os.lstat(destination)
        except FileNotFoundError:
            return ("replace",)
        except OSError as exc:
            return ("refuse",
                    f"staged destination unstatable, not replacing "
                    f"{destination}: {exc}")
        if not statmod.S_ISREG(present.st_mode):
            return ("refuse",
                    f"staged destination is not a regular file, not "
                    f"replacing: {destination}")
        proof, standing, detail = self._proof_search(
            norm, want, declared, computed=computed, source_id=source_id)
        if proof is not None:
            return ("adopt", proof[0], proof[1])
        if standing == "unknown":
            return ("refuse",
                    f"staged publication proof unreadable for "
                    f"{destination}: {detail}; not replacing")
        if standing == "divergent":
            return ("refuse", detail)
        pins = self._live_pins(norm)
        if pins is None:
            return ("refuse",
                    f"staged pin census unreadable for {destination}; "
                    f"not replacing")
        if pins:
            return ("refuse",
                    f"shared staged name is live-pinned by "
                    f"{pins}, not replacing: {destination}")
        if standing == "owned":
            # A fragment vouches but the date is missing: a crash between
            # the fragment and its sidecar, or a publisher still running.
            # Defer to the stall policy's retry; never replace what
            # another publication names.
            return ("wait",
                    f"shared staged name is published elsewhere, "
                    f"deferring: {destination}")
        cover, cover_detail = self._live_claim_cover(norm)
        if cover is None:
            return ("refuse",
                    f"staged claim census unreadable for {destination}: "
                    f"{cover_detail}; not replacing")
        if cover:
            # A live mover claim covers this name: its fragment may still
            # land.  Defer; the retry adopts once it does.
            return ("wait",
                    f"shared staged name has a live publisher, "
                    f"deferring: {destination}")
        partials = self._inflight_partials(destination)
        if partials is None:
            return ("refuse",
                    f"staged copy census unreadable for {destination}; "
                    f"not replacing")
        if partials:
            return ("wait",
                    f"shared staged name has a copy in flight "
                    f"({partials}), deferring: {destination}")
        # Positive absence, re-verified: no fragment, no pin, no live
        # claim, no copy in flight, every census clean.  Before the grace
        # expires this still waits for a late fragment; only a grace that
        # expires with the absence intact heals the name as orphan/crash
        # residue -- elapsed time alone never permits it.
        if heal:
            return ("replace",)
        return ("wait",
                f"shared staged name unattributed, deferring: {destination}")

    def _live_pins(self, norm: str) -> list[str] | None:
        """Pin ids live on one staged path, or None when unknowable."""

        try:
            owners, tainted = reader_lease.live_for(
                self.queue, {norm}, residency_root=self.residency_root)
        except Exception:
            return None
        if tainted:
            return None
        return sorted(owners.get(norm, []))[:5]

    def _live_claim_cover(self, norm: str) -> tuple[bool | None, str]:
        """Whether another live mover claim covers one staged name.

        The existing containment authority: ``stage_release`` attributes
        in-flight copies from their sealed claims, which exist before any
        fragment does.  This mover's own claim is excluded, so a mover
        never defers to itself.  ``None`` means unknowable (fail closed);
        the claim paths are stage-root-relative there, joined here.
        """

        try:
            from stage_release import _claimed_paths
        except ImportError as exc:
            return None, f"claim authority unavailable: {exc}"
        try:
            paths, tainted = _claimed_paths(
                self.queue, str(self.tier_id), self.cas_root,
                exclude={str(self.mover)})
        except Exception as exc:
            return None, str(exc)
        if tainted:
            return None, "; ".join(tainted[:3])
        root = str(self.stage_root)
        for relative in paths:
            if os.path.normpath(os.path.join(root, str(relative))) == norm:
                return True, "live mover claim"
        return False, ""

    def _inflight_partials(self, destination: Path) -> list[str] | None:
        """Sibling copy temporaries for one destination, sans this mover's.

        The owner-keyed ``.<name>.<owner>.partial`` convention (plus the
        legacy shared ``.<name>.partial``) is this copier's own namespace:
        a sibling means another copy is in flight -- or crashed, in which
        case the sweep reaps it and a later retry proceeds.  ``None``
        means the directory could not be read (fail closed).
        """

        own = f".{destination.name}.{str(self.mover)[:16]}.partial"
        try:
            names = [entry.name for entry in os.scandir(destination.parent)
                     if entry.is_file(follow_symlinks=False)]
        except OSError:
            return None
        out = []
        for name in names:
            if name == own:
                continue
            if name == f".{destination.name}.partial":
                out.append(name)
            elif (name.startswith(f".{destination.name}.")
                    and name.endswith(".partial")):
                out.append(name)
        return sorted(out)[:5]

    @staticmethod
    def _published_source_id(norm: str) -> str | None:
        """The origin identity a published file still carries, if any."""

        try:
            raw = os.getxattr(norm, prewarm_loop.STAGE_SOURCE_XATTR)
        except OSError:
            return None
        try:
            return raw.decode()
        except ValueError:
            return None

    def _proof_search(self, norm: str, want: int, declared: object,
                      computed: str | None = None,
                      source_id: str | None = None,
                      ) -> tuple[tuple[str, dict[str, int]] | None, str,
                                 str | None]:
        """An adoptable publication of these bytes, if one is proven.

        Returns ``(proof, standing, detail)`` where ``proof`` is
        ``(record_digest, file_id)`` and ``standing`` is one of:

        * ``"proof"`` -- some consumer's fragment entry names this path
          with equal bytes, a sidecar digest that matches, and a live
          stat equal to the sidecar's file identity.  The match is the
          declared digest; the copier's computed digest for digest-less
          manifests (no repeat hash of the existing file); or the origin
          fast path (the published file still carries this copy's source
          identity) with the sidecar digest riding along;
        * ``"divergent"`` -- a fragment names the path but the bytes are
          provably not these (digest or stat mismatch): immediate
          refuse, never wait;
        * ``"owned"`` -- a fragment names the path without proving it
          (sidecar missing): defer, a rerun may date it;
        * ``"clean"`` -- no fragment names the path at all;
        * ``"unknown"`` -- unreadable proof state: fail closed even over
          an otherwise valid proof, since an unreadable fragment might
          name this very path.

        No payload is hashed here: the sidecar digest is the copy-time
        content proof, and stat stability is the change detection.
        """

        try:
            children = sorted(
                e.name for e in os.scandir(self.residency_root)
                if e.is_dir() and not e.name.startswith(".")
                and e.name not in _NON_FRAGMENT_DIRS)
        except FileNotFoundError:
            return None, "clean", None
        except OSError as exc:
            return None, "unknown", f"{self.residency_root}: {exc}"
        standing = "clean"
        unknown: str | None = None
        found: tuple[str, dict[str, int]] | None = None
        for child in children:
            cdir = self.residency_root / child
            try:
                names = sorted(
                    e.name for e in os.scandir(cdir)
                    if e.is_file() and e.name.endswith(".json")
                    and not e.name.endswith(".retiring.json")
                    and not e.name.endswith(".tmp"))
            except FileNotFoundError:
                continue
            except OSError as exc:
                unknown = f"{child}: {exc}"
                continue
            for name in names:
                candidate = self._proof_candidate(
                    cdir / name, child, norm, want, declared,
                    computed=computed, source_id=source_id)
                if candidate == "tainted":
                    unknown = f"{child}/{name}: unreadable"
                elif candidate == "divergent":
                    return None, "divergent", (
                        f"staged destination holds different bytes than "
                        f"manifest digest for {norm}; refusing to "
                        f"invalidate its owner")
                elif candidate == "owned":
                    standing = "owned"
                elif candidate is not None and found is None:
                    found = candidate
        if unknown is not None:
            return None, "unknown", unknown
        if found is not None:
            return found, "proof", None
        return None, standing, None

    def _proof_candidate(self, fragment_path: Path, consumer: str,
                         norm: str, want: int, declared: object,
                         computed: str | None = None,
                         source_id: str | None = None,
                         ) -> tuple[str, dict[str, int]] | str | None:
        """One fragment file's verdict: proof, "divergent", "owned",
        "tainted" or None (names nothing here)."""

        try:
            with open(fragment_path) as stream:
                fragment = json.load(stream)
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            return "tainted"
        if (not isinstance(fragment, dict)
                or fragment.get("schema")
                != residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1):
            return None
        matched = False
        for record in (fragment.get("entries") or {}).values():
            if (isinstance(record, dict)
                    and os.path.normpath(str(record.get("stage_path") or ""))
                    == norm):
                matched = True
                break
        if not matched:
            return None
        mover = str(fragment.get("mover_action_key") or "")
        if not mover:
            return "owned"
        sidecar = reader_lease.read_material(
            self.residency_root, consumer, mover)
        if sidecar is None:
            # Published vouch without a date: unprovable either way.
            return "owned"
        if isinstance(sidecar, Exception):
            return "tainted"
        digest: str | None = None
        for mention in (sidecar.get("entries") or {}).values():
            if (not isinstance(mention, dict)
                    or os.path.normpath(
                        str(mention.get("stage_path") or "")) != norm):
                continue
            try:
                if int(mention.get("bytes")) != want:
                    continue
            except (TypeError, ValueError):
                continue
            digest = mention.get("sha256")
            if not isinstance(digest, str) or not digest:
                continue
            break
        else:
            return "owned"
        if isinstance(declared, str) and declared:
            if digest != declared:
                return "divergent"
            record_digest: str = declared
        elif consumer == self.consumer:
            # Digest-less manifests pin no content across consumers:
            # only this consumer's own verified copy adopts blind, and
            # its recorded digest rides along for the new sidecar.
            record_digest = digest
        elif (source_id is not None
                and self._published_source_id(norm) == source_id):
            # Origin fast path: the published file still carries this
            # copy's source identity, so the source has not changed since
            # the vouched copy -- adopt without copying.
            record_digest = digest
        elif computed is not None:
            # The necessary private copy's digest against the stored
            # material proof: same content adopts, a changed source
            # refuses -- without rehashing the existing file.
            if computed != digest:
                return "divergent"
            record_digest = digest
        else:
            return "owned"
        file_id = reader_lease.stat_identity(norm)
        published = None
        for mention in (sidecar.get("entries") or {}).values():
            if (isinstance(mention, dict)
                    and os.path.normpath(
                        str(mention.get("stage_path") or "")) == norm
                    and isinstance(mention.get("file_id"), dict)):
                published = mention["file_id"]
                break
        if (not isinstance(published, dict) or file_id is None
                or file_id != {key: published.get(key)
                               for key in ("ino", "size", "mtime_ns",
                                           "ctime_ns")}):
            return "divergent"
        return record_digest, file_id


class _Copier:
    """One range, copied in read order by a bounded set of workers."""

    def __init__(self, *, mounts: prewarm_loop.MountMap, pacer, stage_root: Path,
                 mount_prefix: str, block: int, workers: int,
                 owner: str = "",
                 source_stage_root: Path | str | None = None,
                 publisher: _StagedPublisher | None = None) -> None:
        self.mounts = mounts
        self.pacer = pacer
        self.stage_root = stage_root
        self.mount_prefix = mount_prefix
        self.block = block
        self.workers = max(1, workers)
        self.owner = str(owner or "")
        self.publisher = publisher
        #: Read the copy's bytes from an already-staged tree instead of the
        #: pool: a promotion's source is the stage, where split ranges live
        #: under their staged names from byte zero rather than under the
        #: manifest's pool path at the manifest offset.  ``None`` keeps the
        #: pool behavior (``mounts`` plus the entry offset); set, the caller
        #: passes the staged source per entry (see ``run``) and the offset is
        #: always zero.  ``stage_move`` leaves this unset.
        self.source_stage_root = (
            None if source_stage_root is None else Path(source_stage_root))
        self.lock = threading.Lock()
        self.staged: dict[str, dict[str, object]] = {}
        #: Publish-time identity per staged key, filed beside the fragment
        #: as the material sidecar: the map entry stays schema-v1, and this
        #: dates the vouching (see ``reader_lease.write_material``).
        self.sidecar: dict[str, dict[str, object]] = {}
        self.bytes_staged = 0
        self.errors: list[str] = []
        #: Bumped under the lock on every landed entry, so a fragment written
        #: outside the lock can say which snapshot it is and an older one can
        #: be discarded rather than written over a newer one.
        self.generation = 0

    def _temporary(self, destination: Path) -> Path:
        """This mover's own temporary beside ``destination`` (#620).

        Stage paths are content-addressed per manifest entry and shared
        between consumers, so two movers -- a dead consumer's unstarted one
        and its successor's -- copy one entry to one destination.  A shared
        ``.<name>.partial`` is then truncated by both and renamed away by the
        winner, and the loser's rename fails with ENOENT (or worse, lands a
        torn prefix).  Keying the temporary by the mover keeps each copy's
        rename its own: both verify the same digest and both land the same
        bytes.  The first 16 hex characters name the mover uniquely enough --
        a collision only reverts to the old sharing -- and keep long staged
        names inside the filename limit.  An empty owner keeps the legacy
        name, so a direct run and every old test stages exactly as before.
        The sweep still recognises both spellings: each starts with ``.``
        and ends with ``.partial``.
        """

        if not self.owner:
            return destination.with_name(f".{destination.name}.partial")
        return destination.with_name(
            f".{destination.name}.{self.owner[:16]}.partial")

    def _copy_one(self, entry: dict[str, object], destination: Path,
                  admission, stop: threading.Event,
                  source: Path | str | None = None,
                  source_offset: int | None = None
                  ) -> tuple[int, str, dict[str, int] | None]:
        """Read this entry's bytes, write them, hash them; return size and digest.

        The temporary lives beside the final name so the publish is a rename
        within one dataset, and it is removed on any failure: a stage littered
        with half-copies would charge the tier for bytes no map ever names.

        ``source``/``source_offset`` override the pool default (the mounts map
        plus the entry's own offset) for copies whose source is an already-
        staged tree, where split ranges live under their staged names from
        byte zero.  Omitted, the pool behavior is byte-identical.

        With a ``publisher`` (both production movers pass one), an
        already-published incarnation is adopted without copying, and the
        final rename goes through the publication gate under the stage
        ownership exclusion; without one (unit-constructed copiers on
        isolated scratch) the legacy direct rename runs.
        """

        want = int(entry["bytes"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._temporary(destination)
        if source is None:
            source = self.mounts.local(str(entry["path"]))
            source_offset = int(entry["offset"])
        source = str(source)
        offset = int(source_offset
                     if source_offset is not None else entry["offset"])
        # One stat of the source, for the origin fast path: compared
        # against the published file's existing ``user.pbstage.source``
        # evidence, never a new binding.
        source_id = _origin_id_of(source)
        if self.publisher is not None and want > 0:
            try:
                adopted = self.publisher.try_adopt(
                    entry, destination, source_id)
            except OSError:
                temporary.unlink(missing_ok=True)
                raise
            if adopted is not None:
                # Adopting skips the copy, but a crashed predecessor's
                # owner-keyed temp for this destination must still go:
                # it is never the published bytes.
                temporary.unlink(missing_ok=True)
                return adopted
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
        if source_id is not None:
            # File the origin change-detection the prewarm loop defined
            # (same name, same format), best-effort: a filesystem that
            # will not carry it still converges through the digest
            # comparison at publication.
            try:
                os.setxattr(temporary, prewarm_loop.STAGE_SOURCE_XATTR,
                            source_id.encode())
            except OSError:
                pass
        if self.publisher is not None:
            return self.publisher.publish(entry, destination, temporary,
                                          computed, stop, source_id)
        os.replace(temporary, destination)
        try:
            info = os.stat(destination)
            identity = {"ino": info.st_ino, "size": info.st_size,
                        "mtime_ns": info.st_mtime_ns,
                        "ctime_ns": int(getattr(info, "st_ctime_ns", 0))}
        except OSError:
            identity = None
        return written, computed, identity

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
                    if self.source_stage_root is not None:
                        # A promotion reads the staged tree, where this same
                        # relative name is what the stage mover wrote -- split
                        # ranges from byte zero, never the manifest's pool
                        # path at the manifest offset.
                        staged_source = self.source_stage_root / relative
                        staged_offset = 0
                    else:
                        staged_source, staged_offset = None, offset
                    written, digest, identity = self._copy_one(
                        entry, destination, admission, stop,
                        source=staged_source, source_offset=staged_offset)
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
                key = residency_map.residency_map_key(path, offset)
                with self.lock:
                    self.staged[key] = record
                    if identity is not None:
                        self.sidecar[key] = {
                            "stage_path": str(destination),
                            "bytes": written,
                            "sha256": digest,
                            "file_id": identity,
                        }
                    self.bytes_staged += written
                    self.generation += 1
                    # Snapshot under the lock, write outside it.  Holding the
                    # lock across an NFS fsync serializes every other worker
                    # behind one round trip; the generation is what keeps an
                    # older snapshot from landing on top of a newer one once
                    # they are no longer ordered by the lock.
                    snapshot = ((dict(self.staged), dict(self.sidecar),
                                 self.generation)
                                if on_entry else None)
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


def warm_staged(paths: "list[str]", *, workers: int = 4,
                block: int = prewarm_loop.BLOCK,
                stop: "threading.Event | None" = None) -> dict[str, object]:
    """Read staged files back, on the box that owns the stage.

    This is the whole of cache layer 2, and it is one sentence long because
    that is all ZFS needs: the ARC is filled by reads, the copy that wrote
    these files is a *write*, and nothing else in PrismaBuild ever opens them
    on this box.  Without this the staged blocks reached the ARC only
    incidentally and left it as other shards landed -- a repeat read on
    dl380g10 fell from 9580 to 7423 MiB/s while the range sat untouched --
    and the consumer paid the SSD price for every one of them: 2402 MB/s
    against 10045 MB/s on the same file at the same concurrency
    (sparky -> dl380g10, ``dd iflag=direct``, 16 x 256 MiB, 2026-09-18).

    Buffered on purpose -- no ``O_DIRECT`` -- because the point is to leave
    the bytes in the server's cache rather than to measure the device.  A file
    that cannot be read is an error on the receipt and never a raise: the copy
    is published and its map is written before this runs, so a failed warm
    costs speed and nothing else.
    """

    stop = stop or threading.Event()
    work: queuelib.Queue = queuelib.Queue()
    for path in paths:
        work.put(path)
    workers = max(1, min(int(workers), len(paths) or 1))
    for _ in range(workers):
        work.put(None)
    lock = threading.Lock()
    state = {"bytes": 0, "entries": 0}
    errors: list[str] = []

    def worker() -> None:
        buffer = bytearray(block)
        view = memoryview(buffer)
        while not stop.is_set():
            path = work.get()
            if path is None:
                return
            read = 0
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            except OSError as exc:
                with lock:
                    if len(errors) < 20:
                        errors.append(f"{os.path.basename(path)}: {exc}")
                continue
            try:
                while not stop.is_set():
                    chunk = os.readv(fd, [view])
                    if not chunk:
                        break
                    read += chunk
            except OSError as exc:
                with lock:
                    if len(errors) < 20:
                        errors.append(f"{os.path.basename(path)}: {exc}")
            finally:
                os.close(fd)
            with lock:
                state["bytes"] += read
                state["entries"] += 1

    started = time.time()
    threads = [threading.Thread(target=worker, daemon=True)
               for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = max(1e-9, time.time() - started)
    return {"bytes": state["bytes"], "entries": state["entries"],
            "seconds": round(elapsed, 3),
            "mb_per_s": round(state["bytes"] / 1e6 / elapsed, 1),
            "errors": errors}


def arc_warm_verdict(args) -> dict[str, object]:
    """Whether this mover warms its range, decided by the box that owns the tier.

    Read off the live tier record rather than sealed into the argv, for the
    reason ``movers_claimed_on_tier`` is read there: ``primarycache`` is a
    fact about the dataset *now*, and a mover sealed a week ago must not carry
    last week's answer.  Three outcomes, all of them named on the receipt --
    the warm is refused when the ARC may not hold this dataset's data at all,
    when the operator asked for no warm, and when the tier cannot be read,
    because a warm spends reads and an unattested setting does not license
    them.
    """

    mode = str(getattr(args, "warm_after_copy", "auto") or "auto")
    if mode == "never":
        return {"warm": False, "state": "disabled", "primarycache": None,
                "reason": "--warm-after-copy never"}
    if mode == "always":
        return {"warm": True, "state": "warmed", "primarycache": None,
                "reason": "--warm-after-copy always"}
    try:
        queue = pool.PoolQueue(Path(args.pool_root))
        records = queue.tiers()
    except (OSError, pool.PoolContractError) as exc:
        return {"warm": False, "state": "refused", "primarycache": None,
                "reason": f"the tier record could not be read: {exc}"}
    for record in records:
        if str(record.get("tier_id")) != str(args.tier_id):
            continue
        verdict = storage_tiers.stage_arc_eligibility(record)
        if not verdict["eligible"]:
            return {"warm": False, "state": "refused",
                    "primarycache": verdict["primarycache"],
                    "reason": str(verdict["reason"])}
        # No budget question here, and that is a decision rather than an
        # omission: a mover co-demanding the ARC's GiB would bound the
        # published SSD window by min(stage, ARC) and throttle layer 1's
        # read-ahead once the ARC shrinks for an explicit RAM tier (#638's
        # part 3, withdrawn).  The warm is therefore bounded by this mover's
        # own range and by nothing across movers -- successive phases may
        # evict each other's warm, which costs the *pre*-warm and never the
        # residency the verdict gates on.  The RAM tier's movement nodes
        # bring the occupancy budget back, pointed at ``ram_gib``.
        return {"warm": True, "state": "warmed",
                "primarycache": verdict["primarycache"],
                "reason": str(verdict["reason"])}
    return {"warm": False, "state": "refused", "primarycache": None,
            "reason": f"no box announces the tier {args.tier_id!r}"}


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


def announced_pool_identity(pool_root: str | Path,
                            tier_id: str) -> dict[str, object] | None:
    """The pool identity the tier loop announced for ``tier_id``, or ``None``.

    Copied into the mover's receipt so the receipt folds can key it to the
    pool it measured (#611).  ``None`` -- no record announced, or one stamped
    by an older generation -- files a receipt exactly as before, which a
    gated fold drops and an ungated one reads.
    """

    try:
        records = pool.PoolQueue(Path(pool_root)).tiers()
    except (OSError, pool.PoolContractError):
        return None
    for record in records:
        if str(record.get("tier_id")) != str(tier_id):
            continue
        identity = record.get("pool_identity")
        return dict(identity) if isinstance(identity, Mapping) else None
    return None


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
    queue = pool.PoolQueue(Path(args.pool_root))
    residency_root = Path(args.residency_root)
    copier = _Copier(
        mounts=mounts, pacer=pacer, stage_root=Path(args.stage_root),
        mount_prefix=mount_prefix, block=args.block, workers=args.max_readers,
        owner=str(args.action_key),
        publisher=_StagedPublisher(
            queue=queue, stage_root=Path(args.stage_root),
            residency_root=residency_root,
            consumer_action_key=str(args.consumer_action_key),
            mover_action_key=str(args.action_key),
            manifest_sha256=str(args.manifest_sha256),
            tier_id=str(args.tier_id), cas_root=str(args.cas_root)))

    manifest_sha256 = args.manifest_sha256

    last_published = [0.0]
    last_generation = [0]
    publish_lock = threading.Lock()
    # One publish run, one materialization generation: a retry republishes
    # under the same mover key with a new generation, so the key alone never
    # identifies the bytes (see ``reader_lease``).
    material_generation = reader_lease.mint_generation()

    def publish(staged: dict[str, dict[str, object]],
                identities: dict[str, dict[str, object]] | None = None,
                generation: int = 0,
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

        The fragment goes first, then the material sidecar that dates it: a
        crash between them leaves a vouch without a date, which strict
        readers refuse (safe) and legacy readers use (today's behavior),
        and the rerun overwrites both.
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
            if identities:
                reader_lease.write_material(
                    residency_root,
                    consumer_action_key=args.consumer_action_key,
                    mover_action_key=args.action_key,
                    tier_id=args.tier_id, stage_root=str(args.stage_root),
                    manifest_sha256=manifest_sha256,
                    generation=material_generation, entries=identities)

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
    # How many movers were reading this tier when the copy began, this one
    # included.  Without it ``mean_pool_read_mb_s`` cannot price a single
    # mover: it is what the whole pool delivered, so a window shared by three
    # copies reports three copies' worth, and a next submission that read it as
    # one mover's rate would reserve the entire pool for each of them -- which
    # re-serializes movers through the fill token, the same failure this whole
    # change is about, arriving by another resource kind.
    try:
        concurrent = len(pool.PoolQueue(Path(args.pool_root)).movers_claimed_on_tier(
            str(args.tier_id))) or 1
    except (OSError, pool.PoolContractError):
        concurrent = 0          # unknown, and unknown prices nothing
    before = proc_io()
    cpu_before = cpu_seconds()
    started = time.time()
    # The start gate, same as the promotion's: order this copy's first rename
    # against an egress that may be snapshotting right now.  Acquired and
    # released -- nothing is held during the copy itself.
    pool.PoolQueue(Path(args.pool_root)).ownership_start_gate(args.stage_root)
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
        # 0 means "could not be read", which prices nothing, rather than 1.
        storage_tiers.MOVER_CONCURRENCY_FIELD: int(concurrent),
        # What the ledger promised this copy of the pool, beside what the pool
        # actually delivered (``disk_pacing.mean_pool_read_mb_s``) and what
        # this copy achieved.  The three together are what says whether the
        # supply can grow or has found its ceiling.
        storage_tiers.MOVER_FILL_DEMAND_FIELD: int(
            getattr(args, "fill_mb_s_pool_side", 0) or 0),
        "host": socket.gethostname(),
        "errors": copier.errors,
        "unix": time.time(),
    }
    # Which pools this copy read off and wrote onto, for the receipt folds
    # that price the next mover (#611).  Read off the live tier record rather
    # than sealed into the argv, for the same reason ``movers_claimed_on_tier``
    # is read there: the pool a mover measured is a fact about the tier now,
    # and a mover sealed before a rebuild must not carry the old pool's name.
    identity = announced_pool_identity(args.pool_root, str(args.tier_id))
    if identity is not None:
        receipt["pool_identity"] = identity
    if overran:
        # The tokens bound what the tier can hold.  Staging past them is the
        # one failure that cannot be left to the consumer to notice, so the
        # fragment goes and the receipt says why.
        receipt["refusal"] = "residency_overran_reservation"
        receipt["complete"] = False
        residency_map.fragment_path(
            residency_root, args.consumer_action_key,
            args.action_key).unlink(missing_ok=True)
        reader_lease.material_path(
            residency_root, args.consumer_action_key,
            args.action_key).unlink(missing_ok=True)
    elif copier.staged:
        publish(copier.staged, copier.sidecar, force=True)
    # Layer 2, after every measurement of layer 1 has been taken.  The warm
    # reads the stage, not the pool, so folding it into ``seconds`` or the
    # pacer's window would price a copy by a read the pool never served --
    # and ``fill_supply_from_records`` would then read a healthy mover as one
    # that fell short of the bandwidth it reserved (#636).  Its own block,
    # its own rate.
    warm = arc_warm_verdict(args)
    outcome = {"bytes": 0, "entries": 0, "seconds": 0.0, "mb_per_s": 0.0,
               "errors": []}
    if overran or not copier.staged:
        warm = {**warm, "warm": False, "state": "skipped",
                "reason": "nothing was published to warm"}
    elif warm["warm"]:
        outcome = warm_staged(
            [str(record["stage_path"]) for record in copier.staged.values()],
            workers=args.max_readers, block=args.block, stop=stop)
    receipt["arc_warm"] = {
        "state": str(warm["state"]),
        "reason": str(warm["reason"]),
        "primarycache": warm["primarycache"],
        "tier_id": str(args.tier_id),
        **outcome,
    }
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
    parser.add_argument("--fill-mb-s-pool-side", type=int, default=0,
                        help="the pool bandwidth this mover's claim reserved, "
                             "recorded in the receipt so a later cycle can ask "
                             "whether the pool delivered what the ledger "
                             "promised.  0 means the claim reserved none")
    parser.add_argument("--readers", type=int, default=4,
                        help="copy depth while another client is reading the pool")
    parser.add_argument("--max-readers", type=int, default=16,
                        help="copy depth while the pool serves nobody but this move")
    parser.add_argument("--warm-after-copy", choices=("auto", "always", "never"),
                        default="auto",
                        help="read the staged range back on this box once it is "
                             "copied and verified, so the file server's ARC holds "
                             "it for the consumer (#638).  'auto' asks the tier's "
                             "own record whether its dataset may cache file data "
                             "at all; 'never' spends no reads on it")
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
