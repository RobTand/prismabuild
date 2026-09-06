#!/usr/bin/env python3
"""Measure the shared filesystem every claim, offer and receipt crosses.

The fleet had no measurement of its own medium.  On 2026-09-06 one Spark's
NFS client entered a state-recovery retry loop: 4,784 RPC/s of which 97.7%
were ``TEST_STATEID``, saturating the session slot table so that every real
operation waited 100-180 ms *to be transmitted* while the server answered in
0.3 ms.  A ``RENAME`` -- the claim operation -- cost 165 ms.  Nothing in
PrismaBuild noticed.  The box was admitted, offered and claimed exactly like
a healthy one, because a box 400x slower than its peers and a box that is
merely busy are the same observation when nobody samples the medium.

Establishing that took a from-scratch investigation, and the reason it had to
be from scratch is that no series existed: not for metadata latency, not for
where an RPC's time went.  Netdata does collect ``nfs.rpc`` on these boxes,
but that is a client-wide *count* out of ``/proc/net/rpc/nfs``.  It would have
shown the storm as a tall line and said nothing about what it cost, because
counts do not carry latency and neither is per-mount.

So, two instruments, because they answer different questions and only one of
them can be trusted when the mount is sick.

**Where the time went** comes from ``/proc/self/mountstats``, which the kernel
keeps per mount and per NFS operation.  Each op carries cumulative queue time
and round-trip time separately, and that split *is* the diagnosis: time in
``rtt`` is the server being slow, and time in ``queue`` is this client not
managing to send.  The morning's storm was 142 ms of queue against 0.32 ms of
rtt on ``GETATTR`` -- a local fault wearing a remote fault's symptoms.  This
leg reads one procfs file, costs no network operation at all, and therefore
keeps working precisely when the mount does not.

**What it actually cost** comes from timing a handful of real syscalls against
the mount: a ``stat``, an ``open``, a bounded ``listdir``, and a
create/rename/unlink in a box-private directory.  The rename is there because
``RENAME`` is the claim path, and a read-only probe would have measured
everything except the operation the queue depends on.

Reading them together is what answers "was the mount the constraint?" rather
than inferring it.  A probe that took 200 ms while the server answered its
RPCs in 0.3 ms locates the fault on this client; a probe that took 200 ms with
an rtt to match locates it at the server; a fast probe says the box was busy,
not blocked, whatever its load average claimed.

Two properties this must have, because it runs on the sick box too:

*Bounded.*  A hard NFS mount does not fail, it waits, and a ``stat`` on a
wedged one blocks uninterruptibly -- no signal, no thread timeout, no
``SIGKILL`` reaches it.  The syscall leg therefore runs in a forked child the
parent abandons at a deadline, and the parent records ``timed_out`` instead of
joining it in D state.  At most one child is ever outstanding: while one is
still unreaped the next sample skips the syscall leg entirely and reports
``wedged``, which is not a degraded reading but the strongest one this module
produces.  The procfs leg keeps reporting throughout.

*Lock-free.*  Nothing here takes a lock, on the mount or off it.  A probe that
serialises against a wedged peer is a probe that wedges.

Nothing in this module decides anything.  It measures and it records.  Refusing
or deprioritising admission on a box whose latency is out of line with the
fleet is a policy decision that belongs to the queue, and it needs the
fleet-relative view that only the recorded series can give it.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import select
import signal
import socket
import sys
import time

SCHEMA = "prismaquant.prismabuild.mount_latency.v1"

#: The mount this fleet is built on.
DEFAULT_MOUNT = Path("/mnt/shared/prismabuild-fleet")

#: Box-private scratch for the write half of the probe.  Deliberately outside
#: ``pb-queue`` and ``reservations``: a census of either must not have to know
#: about a measurement directory, and a measurement must not have to know when
#: a census is walking it.
PROBE_DIRNAME = "mount-probe"

#: Box-local, never on the mount.  Recording a measurement of a filesystem onto
#: that filesystem loses exactly the samples worth having.
DEFAULT_RECORD_DIR = Path("/home/rob/tmp")

#: The local log is a backstop, not the archive.  A sample is about 1.7 KB, so
#: two generations of this hold roughly a day and a half at a 15 s scrape --
#: long enough that somebody who notices an incident can still read it here,
#: and bounded, because a measurement left on forever must not become the
#: thing that fills the disk.  The long history is netdata's, which already
#: tiers it.
RECORD_MAX_BYTES = 16 * 1024 * 1024

#: How long the syscall leg may take before the parent stops waiting for it.
#: Well inside a 15 s scrape so a slow sample cannot stack up, and far enough
#: above a healthy mount's sub-millisecond answers that only a real stall
#: reaches it.
PROBE_DEADLINE_S = 2.0

#: How long to wait for a ``SIGKILL`` to land on an abandoned probe child.
#: A runnable process dies at its next scheduling point, well inside this; a
#: process blocked in the kernel on a hard NFS mount never does, and that is
#: the whole distinction being drawn.
KILL_GRACE_S = 0.25
KILL_POLL_S = 0.005

#: Repetitions per timed operation.  Enough for a median to mean something,
#: small enough that the whole leg is a fixed and trivial number of syscalls.
PROBE_REPEATS = 5

#: ``/proc/self/mountstats`` per-op columns, ``statvers=1.1``.
_OP_FIELDS = ("ops", "trans", "timeouts", "bytes_sent", "bytes_recv",
              "queue_ms", "rtt_ms", "exec_ms", "errors")

#: The operations a claim actually rides on, reported individually.  Everything
#: else is summed into the totals; naming these keeps the interesting rows
#: readable without turning one sample into forty numbers.
CLAIM_PATH_OPS = ("GETATTR", "LOOKUP", "ACCESS", "RENAME", "CREATE",
                  "REMOVE", "OPEN", "READ", "WRITE", "TEST_STATEID")

#: PrismaBuild's admission gate is a local ``flock`` (``adaptive_cpu.py``
#: ``locked()``), taken on a file in this box-private directory and held across
#: the whole of ``pool.py`` ``_claim`` -- a ``ready/`` scan, a record rename, a
#: lease write and the token renames, all of them on NFS.  That *was* the
#: conversion point: a mount that was merely slow became a local queue, and one
#: process waiting on a remote peer starved every other loop on the box.  #267
#: made the acquisition non-blocking, so a loop that finds admission held now
#: refuses and returns to its poll instead of queueing.  The critical section
#: is unchanged and still on the mount, so this leg still measures something
#: real -- but the signal moved: a long hold is the mount doing something to
#: this box, and a *waiter* is now a regression.
#:
#: Found by globbing rather than by recomputing the identity hash, because the
#: hash is the lock's business and duplicating it here would make this file
#: wrong the day that changes.  Whatever admission locks exist on this box are
#: what we measure.
ADMISSION_LOCK_DIR = Path("/tmp") / f"prismabuild-admission-{os.getuid()}"

#: Reading ``/proc/<pid>/wchan`` for an unbounded waiter list would make the
#: cost of this leg a function of how bad the incident is.  Beyond this many we
#: still report the exact count and stop attributing.
MAX_ATTRIBUTED_WAITERS = 32


@dataclass
class _Mount:
    """One mount's identity, as ``/proc/self/mountstats`` spells it."""

    device: str = ""
    mount_point: str = ""
    fstype: str = ""
    transport: str = ""
    age_s: int = 0
    options: dict[str, str] = field(default_factory=dict)
    ops: dict[str, dict[str, int]] = field(default_factory=dict)

    @property
    def is_nfs(self) -> bool:
        return self.fstype.startswith("nfs")


def _parse_options(raw: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for token in raw.split(","):
        name, sep, value = token.partition("=")
        parsed[name.strip()] = value.strip() if sep else "yes"
    return parsed


def read_mountstats(target: Path, text: str | None = None) -> _Mount | None:
    """The mountstats record for the filesystem ``target`` lives on.

    The longest matching mount point wins, so a path under a submount is
    attributed to the submount rather than to whatever it is nested in.  An
    ``autofs`` trigger line names the same mount point as the NFS mount behind
    it and carries no statistics; the record with per-op data is preferred.

    Returns ``None`` when the path is on no listed mount -- which is the
    correct answer on the box that *is* the server and reaches the same bytes
    as local ZFS, and is why this fleet's three boxes must never be given one
    symmetric latency number.
    """

    if text is None:
        try:
            text = Path("/proc/self/mountstats").read_text(encoding="utf-8")
        except OSError:
            return None
    wanted = str(target)
    best: _Mount | None = None
    current: _Mount | None = None
    in_per_op = False

    def consider(candidate: _Mount | None) -> None:
        nonlocal best
        if candidate is None or not candidate.mount_point:
            return
        point = candidate.mount_point
        if not (wanted == point or wanted.startswith(point.rstrip("/") + "/")):
            return
        if best is None:
            best = candidate
            return
        # Longer mount point first; among equals, the one carrying statistics.
        if (len(point), bool(candidate.ops)) > (len(best.mount_point),
                                                bool(best.ops)):
            best = candidate

    for line in text.splitlines():
        if line.startswith("device "):
            consider(current)
            in_per_op = False
            parts = line.split()
            current = _Mount()
            try:
                current.device = parts[1]
                current.mount_point = parts[parts.index("on") + 1]
                current.fstype = parts[parts.index("with") + 2]
            except (IndexError, ValueError):
                current = None
            continue
        if current is None:
            continue
        stripped = line.strip()
        if stripped.startswith("opts:"):
            current.options = _parse_options(stripped.split(None, 1)[-1])
            current.transport = current.options.get("proto", "")
        elif stripped.startswith("age:"):
            try:
                current.age_s = int(stripped.split()[1])
            except (IndexError, ValueError):
                pass
        elif stripped.startswith("xprt:"):
            words = stripped.split()
            if len(words) > 1 and not current.transport:
                current.transport = words[1]
        elif stripped == "per-op statistics":
            # The only reliable boundary.  Several header lines carry a
            # name, a colon and a long row of integers -- ``events:`` has
            # twenty-seven of them -- and a shape test admits them as
            # operations.  Read live, ``events`` was counted into the totals
            # and put mean round-trip time at 266 *seconds*, which is the kind
            # of number that discredits an instrument rather than a mount.
            in_per_op = True
        elif in_per_op and ":" in stripped:
            name, _, rest = stripped.partition(":")
            name = name.strip()
            if not name.replace("_", "").isalpha():
                continue
            values = rest.split()
            if len(values) < len(_OP_FIELDS):
                continue
            try:
                numbers = [int(v) for v in values[:len(_OP_FIELDS)]]
            except ValueError:
                continue
            current.ops[name] = dict(zip(_OP_FIELDS, numbers))
    consider(current)
    return best


def _op_delta(before: dict[str, dict[str, int]],
              after: dict[str, dict[str, int]]) -> dict[str, dict[str, int]]:
    """Per-op counter differences, dropping any that went backwards.

    A remount resets every counter, and a negative difference read as a rate is
    a fabricated number.  Dropping the op is the honest reading of a counter
    whose origin moved.
    """

    delta: dict[str, dict[str, int]] = {}
    for name, now in after.items():
        was = before.get(name, {})
        row = {field: now.get(field, 0) - was.get(field, 0)
               for field in _OP_FIELDS}
        if any(value < 0 for value in row.values()):
            continue
        if row["ops"]:
            delta[name] = row
    return delta


def _rpc_view(delta: dict[str, dict[str, int]],
              window_s: float) -> dict[str, object]:
    """Rates and mean per-op times, plus the queue/server split.

    ``queue_share`` is the whole point.  NFS records the time an RPC spent
    waiting to be transmitted separately from the time the server took to
    answer it, so the ratio says which side of the wire the latency is on
    without anyone having to guess.  It is arithmetic on two counters, not a
    threshold, and it carries no judgement about whether the number is bad --
    that comparison is fleet-relative and belongs to whatever reads the series.

    The aggregate means are composition-sensitive and ``by_op`` is not, which
    is why both are here and why a reader should reach for the second.
    Measured on sparky on 2026-09-06: driving 65,000 negative ``LOOKUP``/s at
    the mount moved the all-ops mean round-trip time *down*, from 0.52 ms to
    0.03 ms, because the added operations are cheaper than the baseline mix --
    the mount got busier and the headline number improved.  ``by_op`` showed
    the same window honestly: ``LOOKUP`` at 0.031 ms and ``RENAME``, the claim
    path, unchanged at 0.6-1.8 ms.
    """

    total_ops = sum(row["ops"] for row in delta.values())
    total_queue = sum(row["queue_ms"] for row in delta.values())
    total_rtt = sum(row["rtt_ms"] for row in delta.values())
    total_exec = sum(row["exec_ms"] for row in delta.values())
    by_op: dict[str, dict[str, float]] = {}
    for name in CLAIM_PATH_OPS:
        row = delta.get(name)
        if not row:
            continue
        ops = row["ops"]
        by_op[name] = {
            "ops_per_s": round(ops / window_s, 3) if window_s > 0 else 0.0,
            "queue_ms": round(row["queue_ms"] / ops, 3),
            "rtt_ms": round(row["rtt_ms"] / ops, 3),
            "exec_ms": round(row["exec_ms"] / ops, 3),
            "errors": row["errors"],
            "major_timeouts": row["timeouts"],
        }
    spent = total_queue + total_rtt
    return {
        "ops_per_s": round(total_ops / window_s, 3) if window_s > 0 else 0.0,
        "ops": total_ops,
        "queue_ms": round(total_queue / total_ops, 3) if total_ops else 0.0,
        "rtt_ms": round(total_rtt / total_ops, 3) if total_ops else 0.0,
        "exec_ms": round(total_exec / total_ops, 3) if total_ops else 0.0,
        "queue_share": round(total_queue / spent, 4) if spent else 0.0,
        "errors": sum(row["errors"] for row in delta.values()),
        "major_timeouts": sum(row["timeouts"] for row in delta.values()),
        "by_op": by_op,
    }


def _getattr_count(target: Path) -> int | None:
    """This mount's cumulative ``GETATTR`` count, or ``None`` if unreadable.

    A procfs read, so it costs the mount nothing and cannot itself block on it.
    """

    mount = read_mountstats(target)
    if mount is None or not mount.is_nfs:
        return None
    return mount.ops.get("GETATTR", {}).get("ops")


def timed_probe(probe_dir: Path, repeats: int = PROBE_REPEATS,
                setup: bool = True) -> dict[str, object]:
    """Time the metadata operations the queue is built out of.

    Runs in the forked child, so it may block forever without consequence for
    the caller.  It creates its own directory on first use and leaves it in
    place: a probe that rebuilt its scratch each cycle would be measuring
    directory creation rather than the operations a claim performs, and would
    pay for it in RPCs every sample.  ``setup`` is false once the parent has
    seen one cycle succeed, so the steady-state footprint is the timed
    operations and nothing else.

    The rename is the reason this leg exists at all.  ``RENAME`` is how the
    pool takes a claim, it was the operation at 165 ms during the storm, and no
    amount of read-side timing would have shown it.
    """

    def median(values: list[float]) -> float:
        ordered = sorted(values)
        middle = len(ordered) // 2
        if not ordered:
            return 0.0
        if len(ordered) % 2:
            return ordered[middle]
        return (ordered[middle - 1] + ordered[middle]) / 2.0

    def timed(action) -> tuple[float, float]:
        samples = []
        for _ in range(repeats):
            started = time.perf_counter()
            action()
            samples.append((time.perf_counter() - started) * 1000.0)
        return round(median(samples), 4), round(max(samples), 4)

    anchor = probe_dir / "anchor"
    if setup:
        probe_dir.mkdir(parents=True, exist_ok=True)
        if not anchor.exists():
            anchor.write_text("prismabuild mount probe\n", encoding="utf-8")

    result: dict[str, object] = {"status": "ok"}
    # Bracket the stat loop alone with the kernel's own GETATTR counter.  Five
    # stats that produced no GETATTR were answered out of the attribute cache
    # and five that produced five were not, and during the storm that ratio was
    # 8 of 60.  Counting every GETATTR in the whole sample instead would
    # attribute the setup's lookups to the stats and read as a permanent miss.
    getattr_before = _getattr_count(probe_dir)
    result["stat_ms"], result["stat_max_ms"] = timed(
        lambda: os.stat(anchor))
    getattr_after = _getattr_count(probe_dir)
    if getattr_before is not None and getattr_after is not None:
        result["stat_getattr_rpcs"] = max(0, getattr_after - getattr_before)
    result["open_ms"], result["open_max_ms"] = timed(
        lambda: os.close(os.open(anchor, os.O_RDONLY)))
    result["listdir_ms"], result["listdir_max_ms"] = timed(
        lambda: os.listdir(probe_dir))

    # The claim path: create, rename, unlink.  One name per pid so two probes
    # on one box -- an exporter and somebody's one-shot reading -- cannot
    # collide, and no lock is needed to say so.
    source = probe_dir / f"claim-{os.getpid()}.tmp"
    target = probe_dir / f"claim-{os.getpid()}.done"

    def claim_cycle() -> None:
        with source.open("w", encoding="utf-8") as handle:
            handle.write("probe\n")
        os.rename(source, target)
        os.unlink(target)

    result["claim_ms"], result["claim_max_ms"] = timed(claim_cycle)
    result["worst_ms"] = max(
        float(result["stat_max_ms"]), float(result["open_max_ms"]),
        float(result["listdir_max_ms"]), float(result["claim_max_ms"]))
    return result


# --------------------------------------------------------------------------
# The third leg: local lock contention.
#
# This one exists because the other two would have called a wedged box healthy.
# On 2026-09-06 dl380g10 had 15 of 16 worker loops blocked on the admission
# flock and one holder stuck in ``__break_lease`` waiting for a remote client
# to return an NFS delegation.  Nothing served.  Its load average read 1.13.
#
# That is not bad luck.  A blocking ``flock`` sleeps *interruptibly*
# (``locks_lock_inode_wait``), and load average counts only running and
# uninterruptible tasks, so a total admission stall is invisible to load by
# construction -- measured on sparky through PrismaBuild, action key
# ``463aacd60ceb``: 15 processes fully blocked moved load1 from 0.24 to 0.30,
# with zero waiters in D state.  Every load-based health check on this fleet is
# blind to this failure, and a mount instrument that reported only RPC latency
# would have been blind to it too: the mount was answering, one peer was not
# returning a delegation, and the damage was entirely local and entirely in a
# lock queue.
# --------------------------------------------------------------------------


def _load1() -> float | None:
    """Recorded next to the lock counts so the blind spot is in the data.

    Not a health signal.  It is here to be contradicted: a reading of "load
    0.3, fifteen waiters" is the incident, and the pair says more than either
    number does alone.
    """

    try:
        return float(Path("/proc/loadavg").read_text().split()[0])
    except (OSError, ValueError, IndexError):
        return None


def lock_key(path: Path) -> str | None:
    """``/proc/locks`` spells a file as ``major:minor:inode``.

    Both numbers are hex, and both are zero-padded to two digits -- the kernel
    prints ``%02x:%02x:%ld``.  The padding is not cosmetic: ``/tmp`` on tmpfs
    is major 0, which the kernel writes ``00`` and an unpadded ``{:x}`` writes
    ``0``.  The key would then match nothing, and this leg would report a gate
    with no holders and no waiters -- *healthy* -- on precisely the box that
    was stalled.  Verified against a live ``/proc/locks`` entry on a major-0
    device (``00:1e:104``) rather than read off the format string.

    Widths are minimums, so a major above 255 still prints in full: sparky's
    ``/tmp`` is major 259 and appears as ``103``.
    """

    try:
        info = os.stat(path)
    except OSError:
        return None
    return (f"{os.major(info.st_dev):02x}:{os.minor(info.st_dev):02x}"
            f":{info.st_ino}")


def read_proc_locks(keys: set[str], text: str | None = None) -> dict[str, dict]:
    """Split holders from waiters for each key of interest.

    A waiting record is marked by ``->`` after its id.  That single character
    is the whole diagnosis: holders are working, waiters are starved, and a
    lock with one holder and fifteen waiters is a stall no throughput number
    will show you.
    """

    found: dict[str, dict[str, list[int]]] = {
        key: {"holders": [], "waiters": []} for key in keys}
    if text is None:
        try:
            text = Path("/proc/locks").read_text()
        except OSError:
            return found
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 2:
            continue
        waiting = fields[1] == "->"
        rest = fields[2:] if waiting else fields[1:]
        # kind, mandatory-ness, access, pid, maj:min:inode, start, end
        if len(rest) < 5 or rest[4] not in found:
            continue
        try:
            pid = int(rest[3])
        except ValueError:
            continue
        found[rest[4]]["waiters" if waiting else "holders"].append(pid)
    return found


def process_status(pid: int, want_wchan: bool = True) -> tuple[str, str | None]:
    """``(state, wchan)`` for one pid, or ``("?", ...)`` if it has gone.

    The state answers "would load average have seen this?" and the wchan
    answers "waiting on what?".  Together they separate a holder stuck in the
    kernel on NFS from a holder that is simply doing slow work, which is the
    difference between blaming the mount and blaming the code holding the lock.

    The two are not equally available.  ``/proc/<pid>/stat`` is world-readable;
    ``wchan`` is gated by ``ptrace_may_access``, so a reader running as another
    uid without ``CAP_SYS_PTRACE`` -- the packaged netdata plugin, for one --
    gets ``"0"`` for every process it does not own.  The state, and therefore
    the headline, survives that; the attribution does not.  ``want_wchan``
    exists so the caller can skip the more expensive half.
    """

    try:
        stat_line = Path(f"/proc/{pid}/stat").read_text()
        state = stat_line[stat_line.rindex(")") + 2]
    except (OSError, ValueError, IndexError):
        return "?", ("?" if want_wchan else None)
    if not want_wchan:
        return state, None
    try:
        wchan = Path(f"/proc/{pid}/wchan").read_text().strip() or "?"
    except OSError:
        wchan = "?"
    return state, wchan


def lock_contention(lock_dir: Path = ADMISSION_LOCK_DIR,
                    locks_text: str | None = None,
                    held_since: dict[str, float] | None = None,
                    now: float | None = None) -> dict[str, object]:
    """Who holds the admission gate, who is queued behind it, and for how long.

    ``held_since`` is the caller's memory across samples, so the hold age is a
    *lower* bound quantised by the sample interval: a hold that began and ended
    between two samples is not seen at all.  That is honest and it is enough --
    the incident this catches lasted hours, and a bound that says "at least
    four minutes" already names a defect, since the gate is meant to be held
    for the length of a few renames.
    """

    now = time.time() if now is None else now
    held_since = {} if held_since is None else held_since
    try:
        paths = sorted(lock_dir.glob("*.lock"))
    except OSError:
        paths = []

    keys = {}
    for path in paths:
        key = lock_key(path)
        if key:
            keys[key] = path.name
    if not keys:
        # No admission lock on this box is a fact, not a failure: a box that
        # has never run a loop has no gate to contend for.
        return {"present": False, "waiters": 0, "holders": 0}

    locks = read_proc_locks(set(keys), text=locks_text)
    holders, waiters = [], []
    for key, entry in locks.items():
        for pid in entry["holders"]:
            state, wchan = process_status(pid)
            token = f"{key}:{pid}"
            since = held_since.setdefault(token, now)
            holders.append({"pid": pid, "state": state, "wchan": wchan,
                            "held_at_least_s": round(max(0.0, now - since), 3)})
        for pid in entry["waiters"]:
            waiters.append((key, pid))

    live = {f"{key}:{pid}" for key, entry in locks.items()
            for pid in entry["holders"]}
    for token in [token for token in held_since if token not in live]:
        del held_since[token]

    # State is read for every waiter and the wchan only for the first few.
    # The headline number is a count of waiters, so capping it would make a
    # chart of 200 waiters and 32 invisible ones read as "168 of them were in
    # the load average" -- the opposite of the finding.  ``/proc/PID/stat`` is
    # one small read; ``wchan`` is the one worth bounding.
    states: dict[str, int] = {}
    wchans: dict[str, int] = {}
    for index, (_key, pid) in enumerate(waiters):
        state, wchan = process_status(pid, want_wchan=index < MAX_ATTRIBUTED_WAITERS)
        states[state] = states.get(state, 0) + 1
        if wchan is not None:
            wchans[wchan] = wchans.get(wchan, 0) + 1

    return {
        "present": True,
        "files": len(keys),
        "holders": len(holders),
        "waiters": len(waiters),
        "holder_detail": holders,
        "waiter_states": states,
        "waiter_wchans": wchans,
        # Every waiter is counted and stated; only the wchan attribution is
        # capped, so this names what the wchan histogram covers.
        "attributed_waiters": min(len(waiters), MAX_ATTRIBUTED_WAITERS),
        # The headline.  Waiters that load average cannot see, which on this
        # fleet is nearly all of them, and the reason this leg exists.
        "waiters_invisible_to_load": sum(
            count for state, count in states.items() if state not in ("R", "D")),
        "max_hold_s": max((h["held_at_least_s"] for h in holders), default=0.0),
    }


class MountSampler:
    """One box's view of one mount, sampled repeatedly.

    Holds the previous counter snapshot so every reading is a delta over a
    stated window rather than an average since the mount came up -- a
    distinction that decides whether a four-hour-old storm is still visible in
    today's number.  Holds, too, the pid of an abandoned probe child, which is
    what bounds this to one outstanding blocked process no matter how long the
    mount stays wedged.
    """

    def __init__(self, mount: Path = DEFAULT_MOUNT, *,
                 host: str | None = None,
                 deadline_s: float = PROBE_DEADLINE_S,
                 repeats: int = PROBE_REPEATS,
                 probe: object = None,
                 lock_dir: Path = ADMISSION_LOCK_DIR) -> None:
        self.mount = Path(mount)
        self.host = host or socket.gethostname()
        self.deadline_s = deadline_s
        self.repeats = repeats
        self._probe = timed_probe if probe is None else probe
        self._setup_done = False
        self._previous: _Mount | None = None
        self._previous_at: float | None = None
        self._outstanding_pid: int | None = None
        self._outstanding_since: float | None = None
        self._lock_dir = Path(lock_dir)
        self._held_since: dict[str, float] = {}

    @property
    def probe_dir(self) -> Path:
        return self.mount / PROBE_DIRNAME / self.host

    @staticmethod
    def _reap_within(pid: int, grace_s: float) -> int:
        """Reap ``pid`` if it goes within ``grace_s``, without ever blocking.

        A blocking ``waitpid`` is the one call in this module that could
        outlast its own deadline: a child stuck in the kernel on a hard mount
        never returns, and waiting on it would put the caller in exactly the
        state the fork was there to avoid.  Polling costs a few syscalls and
        keeps the bound over the whole call rather than over most of it.
        """

        deadline = time.time() + grace_s
        while True:
            try:
                gone, _status = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                return pid                    # already reaped; treat as gone
            except OSError:
                return 0
            if gone or time.time() >= deadline:
                return gone
            time.sleep(KILL_POLL_S)

    def _reap(self) -> None:
        """Collect an abandoned child if it has finally come back."""

        if self._outstanding_pid is None:
            return
        try:
            pid, _status = os.waitpid(self._outstanding_pid, os.WNOHANG)
        except ChildProcessError:
            pid = self._outstanding_pid          # already gone; treat as reaped
        except OSError:
            return
        if pid:
            self._outstanding_pid = None
            self._outstanding_since = None

    def _run_probe(self) -> dict[str, object]:
        """The syscall leg, in a child this process is willing to abandon.

        A hard NFS mount answers a blocked ``stat`` with neither an error nor a
        signal, so there is no in-process way to stop waiting for one.  Forking
        is what makes the deadline real: the parent stops reading and returns a
        recorded failure, and the child stays in D state until the mount comes
        back, which it does not get to do inside this call.
        """

        self._reap()
        if self._outstanding_pid is not None:
            since = self._outstanding_since or time.time()
            return {"status": "wedged",
                    "wedged_for_s": round(time.time() - since, 1),
                    "outstanding_pid": self._outstanding_pid,
                    "detail": "a previous probe has not returned; the syscall "
                              "leg is skipped rather than adding a second "
                              "blocked process"}

        read_fd, write_fd = os.pipe()
        started = time.time()
        pid = os.fork()
        if pid == 0:                              # child
            code = 1
            try:
                os.close(read_fd)
                payload = json.dumps(self._probe(
                    self.probe_dir, self.repeats, not self._setup_done))
                os.write(write_fd, payload.encode("utf-8"))
                code = 0
            except BaseException as exc:          # noqa: BLE001
                try:
                    os.write(write_fd, json.dumps({
                        "status": "error",
                        "error": f"{type(exc).__name__}: {exc}",
                    }).encode("utf-8"))
                except OSError:
                    pass
            finally:
                try:
                    os.close(write_fd)
                except OSError:
                    pass
                os._exit(code)

        os.close(write_fd)
        chunks: list[bytes] = []
        try:
            while True:
                remaining = self.deadline_s - (time.time() - started)
                if remaining <= 0:
                    break
                ready, _, _ = select.select([read_fd], [], [], remaining)
                if not ready:
                    break
                chunk = os.read(read_fd, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
        except OSError as exc:
            chunks = []
            payload_error = f"{type(exc).__name__}: {exc}"
        else:
            payload_error = ""
        finally:
            try:
                os.close(read_fd)
            except OSError:
                pass

        elapsed = time.time() - started
        if chunks:
            try:
                result = json.loads(b"".join(chunks).decode("utf-8"))
            except ValueError as exc:
                result = {"status": "error",
                          "error": f"unreadable probe payload: {exc}"}
            self._reap_within(pid, KILL_GRACE_S)
            result["elapsed_s"] = round(elapsed, 4)
            if result.get("status") == "ok":
                self._setup_done = True
            return result

        # Nothing came back inside the deadline.  SIGKILL is sent because a
        # child merely slow is reaped by it; a child in D state is not, and
        # that is exactly the case this is here to survive.
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
        # SIGKILL is not instantaneous even when it works: the signal is
        # delivered at the child's next scheduling point, so an immediate
        # WNOHANG says "still running" about a child that is already dying.
        # Believing it would mark a merely slow probe as outstanding and
        # suppress the next sample on a healthy mount.  Poll for a short
        # grace instead -- long enough for the scheduler to deliver a signal
        # to a runnable process, far too short to be confused with a mount
        # timeout, and never blocking, because a D-state child never comes.
        if not self._reap_within(pid, KILL_GRACE_S):
            self._outstanding_pid = pid
            self._outstanding_since = started
        return {"status": "timed_out",
                "elapsed_s": round(elapsed, 4),
                "deadline_s": self.deadline_s,
                "error": payload_error or "",
                "detail": "the mount did not answer a bounded metadata probe "
                          "within the deadline"}

    def sample(self) -> dict[str, object]:
        """One bounded reading: procfs attribution, then the syscall leg.

        Order matters.  The counters are read before and after the probe so the
        probe's own RPCs can be told apart from the box's, which is what makes
        the attribute-cache reading possible: five ``stat`` calls that produced
        no ``GETATTR`` were served from cache, and five that produced five were
        not.  During the storm that ratio was 8 of 60.
        """

        now = time.time()
        before = read_mountstats(self.mount)
        probe = self._run_probe()
        after = read_mountstats(self.mount)

        record: dict[str, object] = {
            "schema": SCHEMA,
            "host": self.host,
            "unix": round(now, 3),
            "mount_point": str(self.mount),
            "probe": probe,
            "locks": lock_contention(self._lock_dir,
                                     held_since=self._held_since, now=now),
            "load1": _load1(),
        }

        if after is None or not after.is_nfs:
            # The box that exports this filesystem reaches it as a local pool.
            # Its probe numbers are real and its RPC numbers do not exist; a
            # zero here would read as "perfectly healthy network mount".
            record["mount"] = {"transport": "local", "is_nfs": False,
                               "device": after.device if after else "",
                               "fstype": after.fstype if after else ""}
            record["rpc"] = None
            record["attribution"] = None
            self._previous, self._previous_at = after, now
            return record

        record["mount"] = {
            "transport": after.transport or "unknown",
            "is_nfs": True,
            "device": after.device,
            "fstype": after.fstype,
            "age_s": after.age_s,
            "vers": after.options.get("vers", ""),
            "nconnect": after.options.get("nconnect", ""),
            "timeo": after.options.get("timeo", ""),
        }

        window_s = 0.0 if self._previous_at is None else now - self._previous_at
        remounted = (self._previous is not None
                     and after.age_s < self._previous.age_s)
        if self._previous is None or window_s <= 0 or remounted:
            # The first sample of a process, and the first after a remount,
            # have no window to state.  Reporting a rate over "since the mount
            # came up" would silently mix a four-hour-old storm into a reading
            # of the last fifteen seconds.
            record["rpc"] = None
            record["window_s"] = 0.0
            record["note"] = ("remount detected; counters restarted"
                              if remounted else "first sample; no window yet")
        else:
            record["window_s"] = round(window_s, 3)
            record["rpc"] = _rpc_view(
                _op_delta(self._previous.ops, after.ops), window_s)

        record["attribution"] = _attribution(before, after, probe, self.repeats)
        self._previous, self._previous_at = after, now
        return record


def _attribution(before: _Mount | None, after: _Mount | None,
                 probe: dict[str, object], repeats: int) -> dict[str, object]:
    """What the probe's own operations cost, separated from the box's.

    The counters are read either side of the probe, so this is the probe's
    footprint measured rather than assumed: how many RPCs five ``stat`` calls
    actually generated, and what the client paid for them.  It is also the
    honest answer to "is this cheap enough to leave on" -- the number is in
    every sample.
    """

    if before is None or after is None or not after.is_nfs:
        return {}
    delta = _op_delta(before.ops, after.ops)
    attribution: dict[str, object] = {
        # An upper bound, not an exact cost: mountstats counts the whole
        # client, so anything else on this box that touched the mount while
        # the probe ran is counted here too.  Stated as a bound because a
        # number that pretends to be exact is worse than one that does not.
        "probe_rpcs_upper_bound": sum(row["ops"] for row in delta.values()),
        "probe_rpc_ms_upper_bound": round(
            sum(row["exec_ms"] for row in delta.values()), 3),
    }
    stat_getattrs = probe.get("stat_getattr_rpcs")
    if isinstance(stat_getattrs, int) and repeats:
        attribution["attribute_cache_hits"] = (
            f"{max(0, repeats - stat_getattrs)}/{repeats}")
        attribution["stat_getattr_rpcs"] = stat_getattrs
    return attribution


def one_line(record: dict[str, object]) -> str:
    """A single human-readable line, for a shell and for a log."""

    probe = record.get("probe") or {}
    mount = record.get("mount") or {}
    rpc = record.get("rpc")
    status = probe.get("status", "?")
    parts = [f"[{record.get('host')}] {mount.get('transport', '?')}",
             f"probe={status}"]
    if status == "ok":
        parts.append(
            f"stat={probe.get('stat_ms')}ms open={probe.get('open_ms')}ms "
            f"listdir={probe.get('listdir_ms')}ms "
            f"claim={probe.get('claim_ms')}ms worst={probe.get('worst_ms')}ms")
    elif status == "wedged":
        parts.append(f"for {probe.get('wedged_for_s')}s")
    elif status == "timed_out":
        parts.append(f"after {probe.get('deadline_s')}s")
    if rpc:
        parts.append(
            f"rpc={rpc['ops_per_s']}/s queue={rpc['queue_ms']}ms "
            f"rtt={rpc['rtt_ms']}ms queue_share={rpc['queue_share']}")
    else:
        parts.append("rpc=(no window)")
    attribution = record.get("attribution") or {}
    if attribution:
        parts.append(f"cost<={attribution.get('probe_rpcs_upper_bound')}rpc "
                     f"cache={attribution.get('attribute_cache_hits', '?')}")
    locks = record.get("locks") or {}
    if locks.get("present"):
        parts.append(f"gate={locks.get('holders')}held/"
                     f"{locks.get('waiters')}wait")
        if locks.get("max_hold_s"):
            parts.append(f"held>={locks['max_hold_s']}s")
        for holder in (locks.get("holder_detail") or [])[:1]:
            parts.append(f"holder={holder['pid']}:{holder['state']}:"
                         f"{holder['wchan']}")
        # Printed together on purpose.  This pair is the whole finding.
        if locks.get("waiters"):
            parts.append(f"load1={record.get('load1')} "
                         f"(invisible={locks.get('waiters_invisible_to_load')})")
    return " ".join(parts)


def _record_path(directory: Path, host: str) -> Path:
    return Path(directory) / f"pb-mount-latency-{host}.jsonl"


def append_record(record: dict[str, object], directory: Path) -> Path | None:
    """Append one sample to this box's local log.

    Box-local on purpose.  Writing a measurement of a filesystem onto that
    filesystem drops exactly the samples that describe it failing, and takes a
    lock on the way.

    The size check runs before the append, not after, so a file may exceed
    ``RECORD_MAX_BYTES`` by one record.  Stated rather than rounded away: the
    guarantee is two generations of the cap plus two records, which is a bound,
    and pretending to an exact ceiling would be a claim the code does not make.
    """

    path = _record_path(directory, str(record.get("host") or "unknown"))
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if path.stat().st_size >= RECORD_MAX_BYTES:
                path.replace(path.with_suffix(path.suffix + ".1"))
        except FileNotFoundError:
            pass
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    except OSError:
        return None
    return path


# --- netdata external plugin -------------------------------------------------
#
# Netdata already runs on every box in this fleet and already keeps history, so
# the cheapest way to turn a sample into a series is to speak its plugin
# protocol rather than to build a store.  Charts are declared once and values
# are integers, so latencies are emitted in microseconds.

_CHARTS = (
    ("prismabuild.mount_latency", "Shared mount metadata latency",
     "microseconds", "latency", "line", 90000,
     (("stat",), ("open",), ("listdir",), ("claim",), ("worst",))),
    ("prismabuild.mount_rpc", "Shared mount NFS RPC rate",
     "calls/s", "rpc", "line", 90001, (("ops",),)),
    ("prismabuild.mount_rpc_time", "Shared mount NFS time per RPC",
     "microseconds", "rpc", "line", 90002, (("queue",), ("rtt",))),
    ("prismabuild.mount_queue_share", "Shared mount RPC time spent queued",
     "percentage", "rpc", "line", 90003, (("queue_share",),)),
    ("prismabuild.mount_probe_state", "Shared mount probe outcome",
     "state", "latency", "line", 90004,
     (("ok",), ("timed_out",), ("wedged",), ("error",))),
    # The admission gate.  Separate family: this is a local lock, not the
    # mount, and the entire point of measuring it is that it fails while the
    # mount looks fine.
    ("prismabuild.admission_gate", "PrismaBuild admission lock contention",
     "processes", "gate", "line", 90005,
     (("holders",), ("waiters",), ("invisible_to_load",))),
    ("prismabuild.admission_hold", "PrismaBuild admission lock hold age",
     "seconds", "gate", "line", 90006, (("max_hold",),)),
)


def _declare_charts(update_every: int, out) -> None:
    for chart, title, units, family, kind, priority, dimensions in _CHARTS:
        out.write(f"CHART {chart} '' '{title}' '{units}' {family} "
                  f"{chart} {kind} {priority} {update_every}\n")
        for (dimension,) in dimensions:
            out.write(f"DIMENSION {dimension} '' absolute 1 1\n")


def _emit(record: dict[str, object], out) -> None:
    probe = record.get("probe") or {}
    rpc = record.get("rpc")
    status = str(probe.get("status", "error"))

    def micros(value: object) -> int:
        try:
            return int(round(float(value) * 1000.0))
        except (TypeError, ValueError):
            return 0

    if status == "ok":
        out.write("BEGIN prismabuild.mount_latency\n")
        for name, key in (("stat", "stat_ms"), ("open", "open_ms"),
                          ("listdir", "listdir_ms"), ("claim", "claim_ms"),
                          ("worst", "worst_ms")):
            out.write(f"SET {name} = {micros(probe.get(key))}\n")
        out.write("END\n")

    out.write("BEGIN prismabuild.mount_probe_state\n")
    for name in ("ok", "timed_out", "wedged", "error"):
        out.write(f"SET {name} = {1 if status == name else 0}\n")
    out.write("END\n")

    locks = record.get("locks") or {}
    if locks.get("present"):
        out.write("BEGIN prismabuild.admission_gate\n")
        out.write(f"SET holders = {int(locks.get('holders') or 0)}\n")
        out.write(f"SET waiters = {int(locks.get('waiters') or 0)}\n")
        out.write("SET invisible_to_load = "
                  f"{int(locks.get('waiters_invisible_to_load') or 0)}\n")
        out.write("END\n")
        out.write("BEGIN prismabuild.admission_hold\n")
        out.write(f"SET max_hold = {int(float(locks.get('max_hold_s') or 0))}\n")
        out.write("END\n")

    if rpc:
        out.write("BEGIN prismabuild.mount_rpc\n")
        out.write(f"SET ops = {int(round(float(rpc['ops_per_s'])))}\n")
        out.write("END\n")
        out.write("BEGIN prismabuild.mount_rpc_time\n")
        out.write(f"SET queue = {micros(rpc['queue_ms'])}\n")
        out.write(f"SET rtt = {micros(rpc['rtt_ms'])}\n")
        out.write("END\n")
        out.write("BEGIN prismabuild.mount_queue_share\n")
        out.write(
            f"SET queue_share = {int(round(float(rpc['queue_share']) * 100))}\n")
        out.write("END\n")
    out.flush()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mount", type=Path, default=DEFAULT_MOUNT,
                    help="shared mount to measure")
    ap.add_argument("--once", action="store_true",
                    help="take one reading and exit")
    ap.add_argument("--json", action="store_true",
                    help="print the reading as JSON rather than one line")
    ap.add_argument("--netdata", action="store_true",
                    help="run as a netdata external plugin, emitting charts "
                         "on stdout until stopped")
    ap.add_argument("--interval-s", type=float, default=15.0,
                    help="seconds between readings")
    ap.add_argument("--record-dir", type=Path, default=DEFAULT_RECORD_DIR,
                    help="box-local directory for the JSONL log; empty to "
                         "record nothing")
    ap.add_argument("--lock-dir", type=Path, default=ADMISSION_LOCK_DIR,
                    help="directory holding the admission lock files to watch")
    ap.add_argument("--deadline-s", type=float, default=PROBE_DEADLINE_S,
                    help="how long the syscall probe may take before it is "
                         "abandoned and recorded as timed out")
    # netdata passes the update interval as the first positional argument.
    ap.add_argument("update_every", nargs="?", type=float, default=None,
                    help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    interval = args.update_every or args.interval_s
    sampler = MountSampler(args.mount, deadline_s=args.deadline_s,
                           lock_dir=args.lock_dir)

    if args.netdata:
        _declare_charts(max(1, int(interval)), sys.stdout)

    while True:
        started = time.time()
        record = sampler.sample()
        if args.record_dir and str(args.record_dir):
            append_record(record, args.record_dir)
        try:
            if args.netdata:
                _emit(record, sys.stdout)
            elif args.json:
                print(json.dumps(record, indent=2, sort_keys=True), flush=True)
            else:
                print(one_line(record), flush=True)
        except BrokenPipeError:
            # netdata closes the pipe to stop a plugin.  That is the stop
            # signal, not a fault, and a traceback in the agent's error log is
            # a worse way to report it than exiting.
            return 0
        if args.once:
            return 0
        time.sleep(max(0.0, interval - (time.time() - started)))


if __name__ == "__main__":
    raise SystemExit(main())
