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

A second sample, taken per directory to find out which records stall, says the
lease is not the only one:

| directory | reads | slowest | over 1 s |
| --- | --- | --- | --- |
| `workers/` | 3 | 0.000 s | 0 |
| `claimed/*.lease` | 8 | 9.95 s | 2 |
| `passes/` | 12 | 45.001 s | 1 |
| `ready/` | 12 | 45.001 s | 3 |

Every stalling directory holds records another box rewrites: a lease from its
heartbeat, a `passes` sidecar from `record_pass` on every refused poll, and a
`ready` record from every requeue. `workers/` is written by the reading box
itself and never stalls. The repeated **45.001 s** is a ceiling, not a
coincidence — a delegation recall that ran out its timeout rather than being
answered.

So the poll has two readers of remotely-written records, not one.
`reap_stale` reads `claimed/` and its leases. `ready_items` reads every
`ready/*.json` **and** calls `passes()` on each of them
(`pool.py`, `ready_items`), because the ready ordering sorts on the denial
count. Only the first is throttled below.

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

The derivation covers the reaper's lease conclusions and not its
`finish_pending` branch, whose input is cgroup state on its own clock. That
branch's capacity return is delayed by up to `HEARTBEAT_S`, accepted rather
than derived, and recorded as such in the code.

**What this does not fix.** The ready scan still reads every `ready/*.json`
and every `passes/*.json` on every poll, and the table above measures those at
the same 45 s ceiling as the lease. This change removes one of the two
readers; it is a measured reduction, not a restored offer. Do not read it as
"the box is visible again" until an after-measurement of the offer cadence
says so.

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

## The same stall loses work, not only capacity

`sparklina` claimed action `0a44f2e0f62c…` and never filed an outcome:
`lease_lost_max_attempts`, empty `stdout` and `stderr`, `lease_age_s: null`,
attempt `00000001`. Its sibling shard ran on `sparky` and passed.

`lease_age_s: null` is not "the lease was old". `reap_stale` sets it from
`lease_age`, which returns `None` when there is **no lease file**, and it takes
that branch only once `_now() - claimed_unix` is past `grace_s`. So the record
carried `claimed_unix` — `_write_json_atomic(dst, claimed)` had landed — and
the next statement in `_claim`, `write_lease`, had still produced nothing
thirty seconds later.

That window is the one `reap_stale`'s own docstring reasons about: *"the
claimed file … on NFS that stretch spans two directory scans and is hundreds
of milliseconds wide"*, and the grace is set to `HEARTBEAT_S` as *"longer than
the window actually spans"*. The measurement above falsifies that premise on
this fleet: a single pool-record operation reached 45.001 s, and the flock
holder was caught as `DELEG BREAKER WRITE` on exactly a `claimed/<key>.lease`.
The grace is shorter than the operation it waits for, so a claimant that is
slow is filed as dead, its work is taken back, and its attempt is burned.

**Does anything gate claiming on a freshness that admission does not have?**
No, and the arrow runs the other way. Admission is not a separate check next
to the claim: `claim()` builds the `Controller`, takes the lock, and calls
`_claim`, which runs `controller.decision(item, demand)` *before* the rename
and skips the item when it returns `None`. There is no path that renames an
item without an admission decision. And `decision()`'s freshness gate is
reached only when the box is borrowing; when free tokens exist it is not
consulted at all, and a box whose sampler has gone quiet **skips** the PSI and
headroom refusals rather than failing them. A stale box is admitted more
easily, not less.

So this is not a claim-versus-admission disagreement. It is the same NFS stall
as the offer half, met at a different place in the same poll — and it needs its
own change, because widening a grace is not the fix: the window it guards has
no upper bound on this filesystem, and there is no liveness signal between the
rename and the first lease write for a longer grace to consult.

## Still open

**The ready scan is the other half of the storm.** `ready_items` reads every
`ready/*.json` and its `passes/*.json` on every poll of every loop, both
measured at the same 45 s ceiling as the lease. It cannot be throttled the way
the reaper was — claiming is what the poll is for — but the `passes` read is
only there to order the scan, and it is paid for items this box has already
been shown it cannot place: the placement filter runs *after* `ready_items`
returns. Deferring the sidecar read until after placement would have cost
`dl380g10` nothing at all in the observed pool, where every ready item was
tagged for another box.

**A claimant blocked between the rename and its lease is filed as dead.**
Measured above; needs its own change and its own issue, since a wider grace
guards a window with no upper bound on this filesystem.

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
