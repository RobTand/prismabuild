# Why an idle 80-CPU box went invisible to placement (issue #205)

Measured on 2026-09-06 between 16:03 and 17:05 UTC, read-only, on the live
fleet. Nothing in the pool was mutated to produce any figure below.

## The symptom

`dl380g10` offered `{'cpu': 80, 'gpu': 0, 'mem_gb': 192}`, was executing
claimed actions, and was excluded from `live`. Nine READY items sat in the
queue; every one of them reported `MATCHING: sparky`, and one had been passed
over 48 times across 898 s. `sparky` — 20 CPUs — was carrying the whole fleet.
The status view called `dl380g10`'s CPU admission `stale`.

## What the ADMISSION column is, and is not

It is not a placement gate. Nothing in `offers()`, `_matching_offers()`,
`placeable()` or `placeable_hosts()` opens `adaptive/cpu-sample.json`.
`pbstatus._admission_sample` computes the column after the fact, comparing the
record's own `sampled_unix` against `adaptive_cpu.MAX_SAMPLE_AGE_S` (5 s) —
not the directory's mtime, and not the 120 s clock that governs offers.

The record only advances when the box runs an admission decision, and a box
runs one only for an item that already matched its tags. So the column reports
the *shadow* of an exclusion, never its cause. A test pins this:
`tests/test_pool_admission_staleness_is_not_placement.py` asserts that a box
with a sample ten times past `MAX_SAMPLE_AGE_S` is still returned by
`placeable_hosts`, in the same breath as the status view calls it stale.

Read as a cause, the column sends an investigation at the admission
controller. That is where issue #205's went.

## The one clock that does gate placement

`offers()` filters on the offer's `announced_unix` against
`OFFER_TIMEOUT_S = 120`. `announce` sits at the top of the worker's poll cycle
(`tools/fleet/worker_loop.py`), so the offer lands only as often as the cycle
turns.

Measured over six minutes on `dl380g10`, watching the mtime of
`workers/dl380g10.json`: three rewrites, at intervals of **180.0 s, 4.4 s and
129.0 s**. Two of three exceeded the timeout. Concurrently `/proc/loadavg` read
`0.41` across 80 CPUs and 17 of the box's 18 worker loops were in
`__break_lease`.

So the box was invisible because its poll cycle could not get back to
`announce` inside 120 s — while being almost entirely idle.

## Why the cycle would not turn

`dl380g10` exports the pool. `/mnt/shared` is its own ZFS dataset
(`storage_pool/shared`) and it runs `nfsd`. When a local process on that box
opens a file another box has been writing over NFSv4, the kernel must recall
that box's delegation and waits for the remote client to answer.

`serve_once` called `reap_stale()` on every poll, and `reap_stale` opens every
`claimed/<key>.json` **and its lease**. The leases are exactly the hot,
remotely-written files. The box runs one loop per class and `supervise` grows
more of them — eighteen were alive — and each polls about once a second while
the queue is non-empty. The box was therefore opening every lease in the pool
tens of times a second, and paying a delegation recall for each one.

Reading twenty `pb-queue` records from the box itself, on an idle box:

| reads | latency |
| --- | --- |
| 17 | under 1 ms |
| 1 | 11.9 s |
| 1 | 34.2 s |
| 1 | 40.3 s |

Total 86.4 s for twenty small files. `/proc/locks` at the same time showed the
per-box admission `flock` with twelve waiters behind one holder, and the holder
listed as `DELEG BREAKER WRITE` on `claimed/<key>.lease` — a delegation recall
inside the lock, with the rest of the box's loops queued behind it.

This is not throughput and not contention for the disk or the wire. The server
was idle (ZFS ~3 MB/s, 200 GB free, zero processes in `D`) and `nfsstat -c` on
`sparky` showed 16,673,766 calls with **0** retransmits.

## The change

`serve_once` now sweeps at most once per `HEARTBEAT_S` per box, through a
host-local marker keyed by the same identity the admission lock uses
(`adaptive_cpu.box_state`).

The interval is derived, not chosen. Everything the reaper concludes is a
statement about a lease; a lease's own writer refreshes it every
`HEARTBEAT_S`, and the grace `reap_stale` applies to a claim with no lease at
all is also `HEARTBEAT_S`. No input to the sweep can move faster than that, so
a second sweep inside one heartbeat re-reads bytes that cannot have changed. A
lease still expires at `LEASE_TIMEOUT_S` and is still noticed within one
heartbeat of expiring, by this box or by any other box polling the same pool.

The marker is unlocked on purpose: deciding whether to sweep must not sit
behind the NFS waits it exists to prevent, and losing the race costs one extra
sweep, which is what the code did before.

Pinned by `tests/test_pool_sweep_is_throttled_to_the_heartbeat.py`: the
throttle is per box rather than per process (the storm is loops times polls,
so a per-process throttle would divide by the poll rate and multiply back by
the loop count), a second box serving the same pool sweeps on its own clock,
and an expired lease is still requeued.

## What was considered and is not the cause

**Load is not the cause, and `busy_cpus` is not `load1`.**
`adaptive_cpu.counters()` reads `/proc/stat` per-CPU jiffies and computes busy
as `total - idle - iowait`, so a process blocked on I/O contributes nothing to
it. Neither `adaptive_cpu` nor `pool` reads `loadavg` at all. `load1` appears
once, in `box_capacity.py`, where the surrounding comment records it as
"Recorded, never clamped" and gives the measurement behind that decision:
clamping capacity on `load1` would charge the pool for its own work and take a
busy-with-our-own-work box from ten CPU slots to zero. Confirmed live —
`sparky` offered `observed_capacity {'cpu': 20}` at `load1 29.63`. And in the
`dl380g10` case there was no load to misread: 0.41 across 80 CPUs.

**Tag pins are correct and are a separate observation.** At 16:12 UTC every
queued item carried `sparky` or `gb10`; one names
`/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python`, an aarch64 CUDA
interpreter that does not exist on x86, and `pbrun.placement_tags` kept the
source-host pin as designed. That is a work-supply fact. It is not why the box
took nothing once x86-tagged work existed.

## Still open

**The admission sample can be stale *within* a single scan.** `decision()`
caches its host sample once per `Controller`, and a `Controller` is built once
per `claim()`, so every item in one ready scan is judged against the sample
taken at the top of it. A live `.adaptive.json` recorded `admitted_unix` 30.78 s
after its own `sampled_unix` — six times `MAX_SAMPLE_AGE_S` — which means that
scan ran for at least that long. The holder walk inside `decision()`
(the `held/` listing, each holder's metadata, `cpu-*` glob and telemetry read,
and a read plus atomic rewrite of `profiles.json` and `jobs.json`) repeats for
every item although only `profiles.get(shape)` depends on the item.

**`not fresh` is more permissive, not less.** Both the PSI refusal and the
headroom check in `decision()` are guarded by `fresh`, so a box whose sampler
has gone quiet skips them. A live holder was admitted with `sampled_unix 0`,
and another recorded `pending_cpu_cost 24.0` on a 20-CPU box — which is how
`sparky` came to hold more reserved CPUs than it has.

**A borrow-gate refusal does not age the item.** `decision() is None` records a
pass and continues; only `handle is None` reaches `STARVATION_FLOOR`. Work that
could only be admitted by borrowing is therefore overtaken indefinitely by work
that fits the free tokens.

**The admission lock spans queue ownership.** `adaptive_cpu`'s module docstring
states that "the local lock serializes budget decisions, not queue ownership
(which still uses NFS rename)", but `claim()` holds it across the whole of
`_claim` — the ready scan, the withdrawn listing, the rename, and the lease
write that was measured breaking a delegation inside it. Narrowing it to the
decision-and-acquire section it describes is a concurrency change to the hot
path and is not part of this change.

**The lock lives under `/tmp`,** which has been cleared on these hosts. A clear
while loops are live changes the lock's inode underneath them and lets mutual
exclusion lapse. Pre-existing; recorded here because the sweep marker now
shares that directory and treats a missing file as "no information", which is
the safe reading for it but not for the lock.
