# Declaring an action's data, and warming it before the claim

Issue #487.  Two pieces: a second content-addressed input that says which
bytes an action will read off the shared mount, and a single loop on the
storage host that makes those bytes resident before anybody claims the action.

The [fleet storage host record](fleet_storage.md) describes the clients'
readahead proposal and DL380's existing sync/L2ARC/no-SLOG configuration
(#523). These host settings do not replace the prewarmer's disk pacing.

## Why

A campaign whose working set is far larger than the file server's ARC gets
nothing from caching.  Every byte is read once and never asked for again, so a
cache that fills behind the reader never hits.  The only read that can be made
fast is one that was already resident when the action started.

Measured on dl380g10 against the GLM-5.3-Flash census, 2026-09-11:

| arm | bytes | rate | wall |
|---|---|---|---|
| row already in ARC, read over NFS by 8 readers | 49.3 GB | 3298.7 MB/s | 14.94 s |
| never-read row, same shape, same readers | 49.3 GB | 306.9 MB/s | 160.77 s |

10.75x, 145.8 s saved on one row.  During the warm arm the four HDDs summed
8.6 MiB/s against 2.26 GB/s served -- 0.4% -- and ARC demand-data hits ran
2,318,508 against 3,721 misses.  The disks were never the constraint at any
reader count; HDD utilization never passed 88%.

## The input

`pbcampaign.data-manifest` is an ordinary `action.inputs` row
(`{id, sha256, bytes}`), so nothing in the action schema changes and the
existing CAS ingestion carries it.  The blob it addresses is a JSON manifest:

```json
{
  "schema": "prismaquant.prismabuild.data_manifest.v1",
  "produced_by": {"tool": "...", "commit": "...", "unix": 1789090615},
  "annotations": {"row_id": "row-0079"},
  "mount_prefix": "/mnt/shared",
  "entries": [
    {"path": "/mnt/shared/.../inputs/x.pt", "offset": 0,
     "bytes": 20973477, "sha256": null},
    {"path": "/mnt/shared/models/M/model-00096-of-00120.safetensors",
     "offset": 0, "bytes": 5369757696, "sha256": null}
  ],
  "entry_count": 868,
  "total_bytes": 63786036448
}
```

For a large read set, the blob may instead be one standard gzip member
containing this same UTF-8 JSON document (#543). The loader detects the gzip
header, including when the CAS pathname has no extension. Submit the compressed
path through the same `--data-manifest` flag or campaign `data_manifest` field.
The sealed `params.data_manifest.content_encoding` is `"gzip"`; plain inputs
keep their existing summary and action identity. The input digest and byte
length address the **compressed file**, so changing its encoding changes the
action key even when the expanded read list agrees.

The stored-file ceiling remains 64 MiB. A gzip member may expand to at most
512 MiB, and the existing 1,000,000-entry ceiling still applies. The loader
bounds both reads before parsing JSON and rejects a corrupt CRC, incomplete
stream, concatenated members or trailing bytes. Compression does not relax
path validation, duplicate checks, totals or consumption order. These are byte
bounds, not a process-memory ceiling: decoded JSON objects and validation
indexes require additional memory. Budget producer/validation work accordingly.

A producer can use `gzip.compress(json_bytes, mtime=0)` or `gzip -n -c` to
write a separate compressed manifest; retain those exact bytes for resealing.
Do not replace a manifest inside an existing sealed request. The storage loop
must load a published generation supporting gzip before it can warm these
inputs; an older loop refuses the compressed hint. Submission still carries
the ordinary CAS input and requires no worker-side decompression for execution.

`validate_data_manifest` refuses anything that is not exact identity: a path
outside `mount_prefix`, a path that is not already normalized, a relative path,
a repeated `(path, offset)`, a zero-length entry, and totals that disagree with
the list.  `offset` is there because the consumer reads tensor byte ranges out
of safetensors shards rather than whole files, and a whole-file manifest would
price the warm at several times the bytes the action touches.

`annotations.phases` is optional and is what lets a manifest larger than the
cache be warmed at all.  It is a running byte sum over `entries`, in the order
the action reads them:

```json
"annotations": {
  "row_id": "joint-3c",
  "phases": [
    {"name": "head", "bytes": 14829322240, "cumulative_bytes": 14829322240},
    {"name": "layer-0", "bytes": 118380625920,
     "cumulative_bytes": 133209948160}
  ]
}
```

Names are unique and non-empty, each cumulative boundary falls between manifest
entries, `cumulative_bytes` never decreases, and the last boundary is the
manifest's `total_bytes`. A table that breaks any of
those rules is treated as absent rather than repaired: windowing on the wrong
boundaries reads the wrong bytes and then records them as resident.  Every
other annotation stays uninterpreted -- PrismaBuild reads `row_id` for the
receipt's label and `phases` for the window, and nothing else.

`entries` are in *consumption* order and are never re-sorted, so a warm cut
short by the budget leaves a useful prefix rather than a random subset.

`sha256` may be null on every entry, and is for the GLM census.  Hashing a
terabyte of calibration captures costs far more than the residency it buys,
and integrity is not what this contract carries.  What binds the byte list to
the action is that the *manifest file* is content-addressed like any other
input -- so the action key covers exactly these bytes, and a different list is
a different action.

**Attaching a manifest changes the action key.**  `inputs` is inside the
hashed action body (`core.py` `_normalize_action_body`), so the same command
with a manifest is a different action from the same command without one.
That is the property that makes the input safe -- no receipt can be answered
by a run that read different bytes -- but it does mean a campaign that gains
manifests gains new keys, and any `passes/` sidecars under the old keys are
orphaned.

Submitters:

- `pbrun.py --data-manifest PATH`
- `pbcampaign.py`: a per-row `data_manifest` field, which is just that flag.

`pbrun` also seals a summary into `params.data_manifest`
(`{input, mount_prefix, entry_count, total_bytes}`) so the prewarm loop's
headroom arithmetic can read a byte count per claimed action without fetching
and parsing a blob on every poll.

## The loop

`tools/fleet/prewarm_loop.py`, run by the supervisor on a box that declares
the `storage` role in `fleet_boxes.json`.  Every poll:

1. `ready_items()` -- the queue's own order, `(-priority, -passes, age)`.  The
   loop has no opinion about what runs next; it reads the same list the
   workers read.  **It is not a second dispatcher:** it never claims,
   reserves, reorders or writes an item.
2. The window of any claimed action that is reading a manifest larger than the
   budget moves forward first (below).  It does not consume the lookahead: a
   claimed action is not a row the lookahead is counting.
3. The first `--lookahead` actions the loop can still act on.  An action
   without a manifest, one below `--min-manifest-bytes`, one this loop has
   already warmed as far as the budget allows, and one that does not fit
   today's budget are each passed over and do not consume the lookahead: the
   window counts rows a warm could still make faster, and one oversized
   submission must not turn prewarm off for the rows behind it.  How much the
   loop holds resident ahead of the claim frontier is bounded by the budget,
   not by `--lookahead` -- step 4 subtracts every warmed, unclaimed row's
   bytes.
4. ARC headroom: `c_max * --arc-reserve-fraction`, minus what claimed actions
   may still read, and minus the bytes of every row this loop has already
   warmed that nobody has claimed yet (`warmed_reserve`). A phased claim reserves
   its warmed window ahead of the accepted read frontier. A declared progress
   policy keeps that reservation past `--claim-grace-min`; missing observations
   release no bytes. Claims without a progress policy use the grace fallback.
   Unphased manifests retain whole-manifest accounting. A manifest that fits
   no window is skipped without a new prewarm record.
5. Reads the window's entries, in manifest order, through the host's local
   pool path (`--mount-map SHARED=LOCAL`), `--readers` at a time,
   `O_NOFOLLOW`, regular files only, paced by the pool's disks and by whether
   any client is reading (below).
6. Writes `pb-queue/prewarm/<action_key>.json`.

### Windows, when the manifest is larger than the ARC

The jobs that need prewarm most were the ones it refused.  The GLM joint pass
(chain step 3b `prepare`, 3c `run`) reads about 4.75 TB layer-major: 45 hidden
layers, layers 3-44 about 111-124 GB each, against an ARC `c_max` of 257.7 GB
and a 206 GB budget.  A whole-manifest `total > budget` check refuses that by
construction, so the pass read every byte off the spindles.

What has to fit in the cache was never the manifest.  It is the distance
between what the action has read and what the loop has made resident.  So when
`annotations.phases` is present the loop warms through the last phase boundary
whose `cumulative_bytes` fits the budget and records how far it got:

```json
{"status": "partial", "warmed_through_phase": "layer-1",
 "warmed_bytes": 133209948160, "window_start_bytes": 0,
 "bytes_warmed": 133209948160, "phased": true, "trigger": "claim"}
```

`warmed_bytes` is the absolute manifest offset reached by a contiguous sequence
of fully read entries. `bytes_warmed` counts the actual I/O in the latest pass;
`contiguous_bytes` counts only its successfully read prefix. An error or partial
entry does not advance the frontier past a gap, even when parallel readers
successfully read later entries. `warmed_through_phase` names the selected target;
check the byte frontier and errors to determine whether that target was reached.

On later polls, the window starts at the later of the previous frontier and
the bytes the consumer has finished. It does not reread a consumed gap when
progress jumps ahead. A phased action claimed before the storage poll can start
its first window from the claimed queue. The sidecar is rewritten in place.
The loop budgets the bytes ahead of consumption together with all other
protected rows, and reader threads share one byte allowance. These records
describe reads and budgeting, not proof that ZFS still retains every page.

For a windowed row, "already warm" means *everything the budget allows is
resident*, which is the honest claim about a manifest that will never fit
whole.  A manifest without phases keeps the whole-manifest rule, unchanged.

### Following the running action, instead of its claim

#523 measured what the old trigger cost.  The loop warmed the next ready row
when the lookahead slot freed -- which is the instant the previous row was
claimed and started its own cold reads.  On 2026-09-12 the warm ran 4.3 s
ahead of the claim and overlapped that row's own reads at 256.7 MB/s: the
client saw no speedup at all, because the bytes arrived exactly as late as if
nobody had warmed them.

An action declares `params.progress` and reports through
`PRISMABUILD_ACTION_PROGRESS_PATH`. Its worker validates the launch token,
sealed phase names and cumulative counts, then publishes its accepted
`ProgressWatch` observation in the claim's lease. The storage loop reads that
observation through the bounded, strict, no-follow regular-file reader and
checks that the lease belongs to the current claim. It never treats the raw
action-written channel or its wall-clock timestamp as proof of advancement.
The worker's heartbeat cadence and the storage poll cadence bound how quickly
a newly accepted phase becomes visible to this loop.

Manifest phase names must describe the consumer's read order: entering a phase
means preceding phases are no longer needed. Those preceding bytes leave the
claimed reserve, allowing the next row to warm while the current action works.
Cyclic progress can revisit earlier input phases, so it does not release an
irreversible manifest frontier. A declared policy with no accepted observation
also releases no bytes; its reservation remains while the claim is active.

The cycle event says which signal fired, with the key, the phase and the bytes
it released:

```json
{"claimed_released_bytes": 14829322240,
 "progress_triggers": [{"action_key": "...", "phase": "layer-1",
                        "released_bytes": 14829322240}],
 "warmed": [{"action_key": "...", "trigger": "progress"}]}
```

`trigger` is an observation, not a label: a row is `"progress"` only when it
fits the budget with the release and would not have fit without it. An action
that declares no progress policy keeps claim-plus-`--claim-grace-min` accounting.

### Pacing, and why the reader count is 1

The first deployment read with `--readers 8`, and the read rate was the only
thing it measured.  Measured against the rest of the fleet on 2026-09-11
(#499), that warm cost more than it saved: eight concurrent 1 MiB readers took
the four-spindle raidz1 to 73-83% utilization, 38-54 ms read await and
11 000-14 400 ms of backlog; the clients' sync writes queued behind that
backlog, their replies passed the NFS-over-RDMA timeout, the server's late
completions failed (`WC error: 10, remote access error`), and every in-flight
RPC on *every* client was retried after a reconnect.  Both Sparks' GPUs idled
100-130 s per row, against the ~3.5 min of prefetch the warm saved on one.
The same bytes read at 8-12% utilization and ~0-2 ms await cost nobody
anything.

So the loop is paced by what the disks are doing, not by a thread count.
Before every block it consults a sample of `/sys/block/<dev>/stat` for the
pool's own data vdev members -- discovered from `zpool status -P`
(`--pace-pool`), or named outright with `--disks` -- and holds while the worst
disk is over a cap:

| argument | default | harmless (measured) | stalling (measured) | holds |
|---|---|---|---|---|
| `--client-active-mb-s` | 2.6 | 0 MB/s (nobody reading) | 26 MB/s and up | required |
| `--max-read-await-ms` | 10 | 0-2 ms | 38-54 ms | yes |
| `--max-backlog-ms` | 2000 | 300-450 ms | 11 000-14 400 ms | yes |
| `--max-util-pct` | 25 | 8-12 % | 73-83 % | no, recorded only |

### A hold needs a client to protect

The pacer exists to protect NFS clients, so the first question a hold answers
is whether anybody is reading.  Measured on dl380g10 at 2026-09-13 04:05Z: a
`zpool scrub` (53.4 % done, issuing 736 MB/s) drove sdb to 88 % utilization at
153.8 MB/s with 9.4 ms read await; the storage role's pacer had held for
5709 s cumulative and was holding at every sample, `util_pct` 100 and backlog
up to 26 s; and no NFS client read a byte for the whole window.  The loop
warmed nothing for hours and protected nobody.  Utilization answered "is the
disk busy"; nobody had asked that question.

So the verdict is an AND: hold while clients are reading **and** the pool is
over its service-time or backlog cap.  Client activity is read from this
host's own NFS server counters (`--nfsd-io`, the `io` line of
`/proc/net/rpc/nfsd`), which count bytes `nfsd` served; the loop reads the pool
through the host's local path, so its own reads never appear there and the
measurement cannot chase itself.  The threshold has to sit *below* the slowest
read worth protecting -- a client already slowed by the warm would otherwise
read as idle and release the hold that would give it the disks back -- so the
default is a tenth of the 26 MB/s cold-start client read #523 measured, an
order of magnitude above an idle mount's attribute traffic.

A host that cannot read the counter paces as though clients were always
reading: blind is not idle, and one missing file must not turn the pacer off
on the box that needs it most. Missing disk telemetry holds reads regardless
of client activity until complete samples return. A hold is released at half
the cap that started it, so a
disk sitting exactly on a threshold does not flap the reader once per sample.

`--max-util-pct` is still accepted and still recorded beside the rate, because
it is the number an operator compares against Netdata; it no longer holds on
its own.  Removing it would have broken every command line in
`fleet_boxes.json` and thrown away a measurement, for a flag whose only defect
was being consulted.

The pacing report separates the two questions an operator actually asks --
what pacing cost, and what it cost for nothing:

```json
{"held_seconds": 41.2, "held_seconds_total": 5709.0,
 "held_while_clients_active_s": 0.0, "held_while_clients_idle_s": 5709.0,
 "samples": 88, "samples_total": 21714,
 "clients_active": false, "client_read_mb_s": 0.0}
```

`held_seconds` prices one row and resets with it; the totals belong to the
role and outlive the pacer each cycle rebuilds.

The defaults are the shape that passed on the live fleet, not the midpoint
between the two measured states.  One reader at 40 % / 15 ms / 4 000 ms held
the disks to 31 % peak yet still dropped both Sparks' NFS clients to ~50
RPC/s for a minute (their own metrics collectors missed samples while it ran);
one reader at 25 % / 10 ms / 2 000 ms warmed a 63.8 GB row at 146.5 MB/s with
the clients untouched.  One reader is already enough to keep the pool busy:
98 % of that run's ARC misses were *prefetch* misses, so the size of each read
burst is set by ZFS's prefetcher (`zfetch_max_distance`), not by the reader,
and a second reader only adds queue depth the pacer then has to take back.

The three numbers are computed the way Netdata computes `disk_util`,
`disk_await` and `disk_backlog`, so the record and the chart an operator reads
afterwards are the same quantities.  `--pace-sample-s` bounds how often sysfs
is read (0.25 s); every block's check reads the cached verdict.  Because the
burst height belongs to the prefetcher, the sample interval is what bounds a
burst's *length*.

On the storage role, those samples are required evidence, not a best-effort
optimization.  Every data-vdev member discovered from the pool must provide a
readable stat row.  A per-block loss of one member or all rows holds before the
next read; a topology-discovery failure before a later cycle exits the role for
its supervisor to retry instead of creating an unpaced cycle.  The hold-start
event reports `telemetry_state` and `missing_devices`; the pacing report also
records `telemetry_gaps` beside the ordinary disk numbers.  After recovery the
first complete sample is only a new baseline: a second complete sample is
required before a read resumes.  The hold is interruptible through the
reader's stop event.  A direct fixture or non-storage caller that configures
no disks remains explicitly inactive (`disk_pacing.active: false`); that mode
is not a storage-role fallback.

The disk verdict and its prior stat baseline remain shared across sequential
rows in a poll, but receipt accounting is row-scoped: samples, means, maxima,
holds, held seconds, and telemetry gaps are reset at each row boundary.  An
existing hold is never reset by that accounting boundary.

Setting any cap to 0 disables that cap.  Setting the await and backlog caps
off is how you reproduce the pre-#499 behaviour, and it is not a supported
production shape. Setting `--client-active-mb-s 0` treats any positive measured
client read rate as active; a measured zero rate remains idle.

### The ARC arithmetic behind `--lookahead 1`

The lookahead is bounded by what the ARC can hold *besides* the rows that are
running, not by how far ahead the loop can see.  On dl380g10: `c_max` is
245 760 MiB (257.7 GB, and both spellings appear in the record: `arcstats`
counts bytes, Netdata's `zfs.arc_size` charts MiB), `--arc-reserve-fraction 0.8` leaves the loop 206.2 GB, and a GLM
census row is ~64 GB.  Two rows being read plus one warmed ahead is 191.4 GB,
14.8 GB under the budget.  A second lookahead row is 255.2 GB and does not
fit: the only way to warm it is to displace what a running row is still
reading, which is what the `arc_size` swing (205 -> 237 -> 220 GB) recorded on
2026-09-11.  The headroom check already refuses that row; `--lookahead 1`
means the loop does not spend a poll discovering it while the head of the
queue is still cold.  Once the head is warm the loop advances past it in the
same poll, and it is then the headroom check that decides whether a second row
fits -- a refusal that reads no data and writes no record.

### What "never evicts a claimed row's data" means here

It is a budget rule, not a guarantee, and the code says so.  ZFS exposes no
way to pin a page, and the ARC target `c` is volatile on a shared box: it fell
99 GB inside one five-minute window on dl380g10 on 2026-09-11 without any
tenant asking for the memory.  What the loop guarantees is that it never
*asks* for more than the ceiling leaves after claimed actions are subtracted,
and it logs both headroom numbers (`c_max - size` and `c - size`) every cycle
so the gap between the budget and the outcome stays visible.

### The one write

`pb-queue/prewarm/<action_key>.json` is the only thing the loop writes,
anywhere.  It is a sidecar for the same reason `passes/` is one: the action is
may move from `ready` to `claimed` while the warm runs. Rewriting the ready item
could resurrect a claimed action and hand it to a second worker. Nothing under
the shared data mount is written.

### The receipt

`claim` copies the sidecar onto the claimed record as `prewarm`, in the write
it was already making, and `finish` carries it into the terminal record's
`detail.prewarm`.  For a windowed manifest that copy is the claim-time
snapshot: the window advances after the claim, so the sidecar under
`pb-queue/prewarm/` is where the final window state lives.  A worker that measured its own residency wins: `finish`
uses `setdefault`.  An action nobody warmed carries no `prewarm` key at all,
because "nobody looked" and "warmed nothing" are different facts.

## Deploying

### Restarting the role after a publication

Follow the operating guide's publication prerequisites first. Publication
changes the desired generation; it does not replace code inside a running
process. Current storage loops detect the new generation between cycles and
exit for the supervisor to replace them. An active read or pacing hold can
delay that boundary, and an older loop may lack the rotation check.

If the role needs a targeted restart, use its supervisor log to identify the
storage child. Record its PID, parent PID, `/proc/<pid>/stat` start time,
`/proc/<pid>/cmdline`, and service cgroup. Confirm that its parent is this
host's active PrismaBuild supervisor and its script belongs to the old published
generation. Recheck the same identity immediately before sending `SIGTERM` to
that exact PID. A name search alone is insufficient. Keep the supervisor
running so it can replace the role without interrupting other work.

After the old child exits, inspect the supervisor's new storage-child log and
the replacement's PID/start time and command line. Its resolved script path
must belong to the live generation and its SHA-256 must match that generation's
`RUNTIME_VERSION.json`. Confirm its configured reader/pacing arguments and a
new cycle event. A sent signal or moved `repo` link alone is not adoption.

Measured example (2026-09-13, dl380g10): `kill -TERM 2486293` at 04:12:24Z
ended the role running generation `953c95e5fb52`; at 04:12:29Z the supervisor,
already re-exec'd to `176021ec3efb-1789272169-6533ecef8514`, spawned pid
2201912 with the same arguments from
`runtime-generations/176021ec3efb-1789272169-6533ecef8514/tools/prewarm_loop.py`.
Its first cycles logged `ready=1`, `pacing_active=true`, `arc_size` 251.2 GB
of a 257.7 GB `c_max`.

The `storage` role only exists once a runtime generation carrying
`prewarm_loop.py` is published.  A supervisor running an older generation
ignores the `roles` key; a supervisor running a newer one with no script logs
`role storage not startable` and keeps supervising its worker loops.  With no
manifest-carrying action in the queue the loop does nothing at all.
