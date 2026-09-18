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

### Opt-in v2 read timeline for later source revisits

A v2 manifest keeps `entries`, `entry_count`, and `total_bytes` as a unique
registry of byte ranges. It adds a required, versioned read plan, whose ordered
references name those entries by zero-based index:

```json
{
  "schema": "prismaquant.prismabuild.data_manifest.v2",
  "entries": ["... unique entry objects, unchanged from v1 ..."],
  "entry_count": 2,
  "total_bytes": 8,
  "read_plan": {
    "phases": [
      {"name": "forward", "entry_indices": [0, 1],
       "bytes": 8, "cumulative_bytes": 8},
      {"name": "compute", "entry_indices": [],
       "bytes": 0, "cumulative_bytes": 8},
      {"name": "reverse", "entry_indices": [1, 0],
       "bytes": 8, "cumulative_bytes": 16}
    ],
    "read_bytes": 16
  }
}
```

The example omits the otherwise required v1 `produced_by`, `annotations` and
`mount_prefix` fields. Each unique entry must appear at least once; a phase
can hold zero read references when compute progress needs a named frontier.
Repeating one index inside a phase is refused, but a later phase may read the
same range again. The validator checks each reference, unique phase name, per
phase byte count, cumulative boundary and final read total; its read-reference
ceiling is 4,000,000. `total_bytes` still names the unique entry bytes, while
`read_bytes` names the complete read timeline, including revisits. The stored
manifest's CAS digest seals both. V1 has neither the plan nor a changed
summary, so its bytes and behavior remain unchanged.

The submitter seals `schema` and `read_bytes` in the v2 summary and requires
linear progress reporting with the read phases in the same order; progress
phases may also include startup or publication. The storage role budgets and
warms the timeline, and an accepted `ProgressWatch` report for the *current*
phase releases only preceding read operations. It never converts arbitrary
unit counts into byte offsets. A cold start can stop at an entry boundary
inside a phase. Failed reads do not advance its contiguous warm frontier.
Old storage generations reject v2 manifests rather than silently treating
the unique entry registry as the read schedule. Publish the compatible storage
generation and verify its running role before submitting v2 campaign rows;
source changes or a producer capable of sealing v2 are not deployment proof.

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

A campaign row that names a shared-mount path in its declared `argv` or `env`
and carries no `data_manifest` is warned about when it is published, naming
the field that matched: the bytes it reads cannot be warmed.  The scan sees
only the row's declaration.  A path built inside the command, read out of a
configuration file, reached through a symlink or resolved from a relative name
is not detected; `cwd` is not scanned because the checkout is materialized
box-local; and a path the command only writes is a false positive.  The
warning therefore says the row *may* read cold rather than that it will.

`--require-data-manifest` is the strict form and does not depend on that scan.
`pbcampaign` then refuses the whole manifest before its first row is sealed
unless every row -- and a logical request's common half -- carries a nonblank
`data_manifest`.  A producer whose reads this tool cannot see, such as a
script that assembles paths at run time, can therefore still require every row
to declare its bytes.

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
   An unphased claim whose prefix is resident reserves that prefix, on the same
   rule; a claim with nothing warmed, or one warmed whole, reserves its
   manifest. A manifest that fits no window is skipped without a new prewarm
   record.
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
`annotations.phases` is present the loop warms through the last entry boundary
that fits the budget, including a boundary inside one oversized phase, and
records how far it got:

```json
{"status": "partial", "warmed_through_phase": "layer-1",
 "warmed_bytes": 133209948160, "window_start_bytes": 0,
 "bytes_warmed": 133209948160, "phased": true, "trigger": "claim"}
```

`warmed_bytes` is the absolute manifest offset reached by a contiguous sequence
of fully read entries. `bytes_warmed` counts the actual I/O in the latest pass;
`contiguous_bytes` counts only its successfully read prefix. An error or partial
entry does not advance the frontier past a gap, even when parallel readers
successfully read later entries. `warmed_through_phase` names the selected target
only when the entry boundary is also a phase boundary; it is empty for an
in-phase target. Check the byte frontier and errors to determine whether that
target was reached. Entry boundaries make safe residency cuts, but do not move
the accepted read frontier: only a matching `ProgressWatch` phase observation
releases claimed reserve, and arbitrary progress units are never converted to
consumed bytes.

On later polls, the window starts at the later of the previous frontier and
the bytes the consumer has finished. It does not reread a consumed gap when
progress jumps ahead. A phased action claimed before the storage poll can start
its first window from the claimed queue. The sidecar is rewritten in place.
The loop budgets the bytes ahead of consumption together with all other
protected rows, and reader threads share one byte allowance. These records
describe reads and budgeting, not proof that ZFS still retains every page.

For a windowed row, "already warm" means *everything the budget allows is
resident*, which is the honest claim about a manifest that will never fit
whole.

A manifest with no phase table is windowed the same way (#499).  `entries` is
already the consumption order -- the phase table is a running sum over it --
so the loop warms the longest entry-aligned prefix the budget allows, in
manifest order, and records it with `"phased": false` and an empty
`warmed_through_phase`.  What the phases buy is the *advance*, not the cut:
without them no worker report maps to a byte count, so an unphased window
stops following the reader the moment its row is claimed.  While the row is
still ready a later poll extends its prefix as the budget grows; after the
claim it stays where the budget left it.  Declare `annotations.phases` when
you want the window to follow the reader.

Before this, an unphased manifest larger than the budget was refused on every
poll for as long as it was queued and its action read every byte off the
spindles.  One refusal remains, and it is narrower: a single entry larger than
the whole budget has no entry-aligned prefix inside it, because a warm reads
files and not byte ranges.

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

### Pacing, and the two read depths

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
| `--client-active-mb-s` | 2.6 | 0 MB/s (nobody else reading) | 26 MB/s and up | required |
| `--max-read-await-ms` | 10 | 0-2 ms | 38-54 ms | yes |
| `--max-backlog-ms` | 2000 | 300-450 ms | 11 000-14 400 ms | yes |
| `--max-util-pct` | 25 | 8-12 % | 73-83 % | no, recorded only |

The depth follows the same verdict (#580).  While a client other than the
action being warmed for is reading, the reader is admitted `--readers` blocks
at a time -- 1, the depth #499 measured to pass on the live fleet.  While
nobody else is, it is admitted `--max-readers` at a time.  The ceiling is a
property of the pool, and its default is the deepest point of the measured
cold-read curve on dl380g10's raidz1 (2026-09-17): 1 stream 28.6 MB/s,
2 -> 40.5, 4 -> 86.9, 8 -> 134.2, 16 -> 204.7 MB/s -- the same bytes, seven
times the rate, from queue depth alone.  Depth is the only lever that gets
the warm ahead of the reader it feeds: a consumer reading cold at 160 MB/s is
getting that rate from single-depth demand reads, so a warm that matches it
cannot gain on it, and once the warm is ahead the window budget bounds the
lead, so depth costs nothing.  The reader spawns the ceiling once and the
pacer admits the tier that applies before every block, so a third party
arriving drops the depth without a thread being spawned or joined, and
leaving raises it back with the hold's own hysteresis: a client counts as
reading above `--client-active-mb-s` and stops counting below half of it.
The receipt carries `readers_peak`, `per_reader_mb_s` (bytes per admitted
reader-second, the per-stream rate the row actually got) and
`keep_pace_depth`, the depth that rate would need to keep pace with the
served action's own mean read rate -- a keep-pace depth above the ceiling
says the pool could not feed this consumer at the depth it was allowed.

`--lookahead` is unchanged: it counts *ready rows* warmed ahead of the claim
frontier, and the running action's window advances outside it.

### A hold needs a client to protect -- and it is never the one being served

The pacer exists to protect NFS clients, so the first question a hold answers
is whether anybody is reading.  Measured on dl380g10 at 2026-09-13 04:05Z: a
`zpool scrub` (53.4 % done, issuing 736 MB/s) drove sdb to 88 % utilization at
153.8 MB/s with 9.4 ms read await; the storage role's pacer had held for
5709 s cumulative and was holding at every sample, `util_pct` 100 and backlog
up to 26 s; and no NFS client read a byte for the whole window.  The loop
warmed nothing for hours and protected nobody.  Utilization answered "is the
disk busy"; nobody had asked that question.

The second question is *which* client (#580).  Measured on the GLM-5.3-Flash
joint-AURA `prepare` (PB action `8b53c37c`, 2026-09-17): the prepare was the
only NFS client on the box, reading cold at 160-280 MB/s through the very
bytes the storage role was fetching for it.  The pacer counted that as a
client to protect, held 39 714.9 s of 20.6 h -- 3 516 s of the last hour --
with `in_flight` 0 on every hold record, and the warm advanced at 22.8 MB/s
behind a reader it could never get ahead of: the prefetcher backed off from
load that existed because it backed off.  Prefetching bytes the served action
is about to demand-read is not extra I/O.  It is the same one-pass bytes read
earlier and at depth, so those reads are *self*, not a client.

So the verdict is an AND: hold while clients **other than the served action**
are reading **and** the pool is over its service-time or backlog cap.  Client
activity is read per client from this host's own NFS server
(`--export-stats`, `/proc/fs/nfsd/export_stats`, one `io_read` counter per
export and client address; the aggregate `io` line of `--nfsd-io` is the
fallback where that file does not exist).  Measured 2026-09-17T21:57Z: over
10 s the two client rows moved 1 630 408 918 B and 5 744 B and the aggregate
line moved 1 630 414 662 B -- one counter, kept per client.  The loop reads
the pool through the host's local path, so its own reads never appear there
and the measurement cannot chase itself.  The server recreates a client's
block when its export cache entry expires -- every 901 s on the live box --
and the counter restarts from zero; the loop carries that client's last rate
across the one interval rather than reading the restart as an idle client or
as a phantom third party.

Whose reads are self is derived, not guessed, from records the fleet already
writes.  The claim record names the box running the action (`claimed_host`);
that box's worker offer (`workers/<host>.json`) names the IPv4 addresses its
kernel holds (`addresses`, read from `ip -4 -o addr show scope global` and
announced by every loop on the box); the per-client counter is keyed by those
addresses.  The set is scoped to the window being read: a claimed row's
window is warmed with its claimant's addresses as self, and a ready row --
warmed for a claimant that does not exist yet -- has no self at all, so every
client reading during that warm is one to protect, which is #499's case
exactly.  A missing link -- a claim naming no host, a box with no offer, an
offer from a runtime that announced no addresses, a host without
`export_stats` -- leaves the set empty and the pacer protecting every client
as it did before, and `served_attribution` in the receipt says which link was
missing, so the old behaviour is never silent.  The offer's age is reported,
not enforced: a box whose every loop is busy stops refreshing its offer, and
its addresses are the same addresses.

The threshold has to sit *below* the slowest read worth protecting -- a
client already slowed by the warm would otherwise read as idle and release
the hold that would give it the disks back -- so the default is a tenth of
the 26 MB/s cold-start client read #523 measured, an order of magnitude above
an idle mount's attribute traffic.

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
 "clients_active": false, "client_read_mb_s": 260.0,
 "self_read_mb_s": 260.0, "other_read_mb_s": 0.0,
 "served_host": "sparky", "served_client_addresses": ["10.100.98.1"],
 "served_attribution": "attributed", "depth": 16}
```

`client_read_mb_s` is still every client's bytes; `other_read_mb_s` is the
number the verdict read.

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

## The stage tier (#582) -- opt-in, and off

`--stage` gives the window read a second destination: a copy on a ZFS pool
whose name starts with `prismabuild-stage`. Everything else is the loop you
have just read -- the same `Reader`, the same `DiskPacer`, the same window
arithmetic and the same ARC budget.

Without the flag nothing in this section runs. No `zpool` is asked, no
directory is created, and a receipt carries no stage field. The default flips
only on a measured campaign result.

### What it does not buy

No consumer reads the stage. PrismaBuild publishes no residency map, the
export is served from the pool path, and the ARC is keyed by the on-pool block
pointer, so a staged copy does not warm the path a consumer reads. Staging
today writes bytes that nobody reads back.

Every record says so. The `consumer` block on each stage record carries
`reads_stage: false`, `verified_reads: null` and `effect: "copy_only"`, and
the receipt reports `staged_bytes` apart from `bytes_warmed` so the two claims
stay separate. Turning the tier on without a consumer costs SSD writes at pool
read rate and returns nothing.

Two more facts to weigh before the default could change:

* The stage pool's writes go through the same ARC this loop is filling, so a
  staged window holds two copies unless the stage dataset is created with
  `primarycache=metadata`.
* An entry is a byte range, so a stage object is a byte range. Each object is
  named `<relative path>.pbstage@<offset>+<bytes>`. The tree is a source for a
  residency map, not an overlay lower layer: a range written under a mirrored
  file name would be a short file that an overlay would serve as the whole
  thing.

### Discovery

The tier is rediscovered every cycle, never remembered. A pool created,
exported, filled or lost between two polls changes the answer.

* The pool is any imported pool whose name starts with `--stage-pool-prefix`.
  The name is the declaration. The rule is deliberately not "any unused SSD":
  `nvme0n1p1` on dl380g10 still carries a stale `zfs_member` signature, and a
  rule that took idle devices would seize it.
* Bytes are written to the pool's `prewarm` dataset when it has one, and to
  the pool's own mountpoint when it does not.
* Capacity is the pool's own `free` less `--stage-free-floor-bytes`, read from
  `zpool list -Hp`.
* Members reach a record only as `/dev/disk/by-id` names. Device numbering on
  dl380g10 is the reverse of what the model names suggest: `nvme0n1` is the
  stage device and `nvme1n1` is the root disk. A record naming a device number
  would hand an operator the wrong disk.

### The four states

Every cycle records one of them, with its reason, whether or not anything was
staged -- including a cycle with an empty queue. A correct non-action that
nobody wrote down is what cost the diagnosis in pb#585.

| State | Meaning |
|---|---|
| `present` | Discovered, writable, with budget. It does not mean anything was staged. |
| `absent` | No imported pool carries the prefix. The state on every box but the file server. |
| `full` | The pool's `free` leaves nothing above the floor, or the budget ran out during the cycle. |
| `unreadable` | No `zpool`, no mounted directory, an unwritable one, or a health that is neither ONLINE nor DEGRADED. |

A tier that fills or faults stops being written to and the warm carries on.
Losing the second destination never costs the first.

### Release

Release is delete-behind-the-accepted-phase: the frontier a running action's
progress record moves is what frees the bytes behind it, which is the signal
that already releases ARC reserve. Two guards make it safe:

* A staged object is deleted only when no other queued row still wants it.
  Three measured GLM-5.3-Flash prepare manifests share 469,007 of 469,008
  entries, so a release that asked only the row in front of it would delete
  the bytes the row behind it has not reached.
* A row that has left the queue has its whole band swept, read back from the
  receipt the warm wrote. A last phase leaves no frontier behind it, and the
  loop keeps no memory across a restart.

A cycle with several queued rows over the same bytes can therefore release
nothing and report `full`. That is the measured shape of total overlap, not a
leak, and it is the retained-prefix behaviour the #583 design argues for --
arrived at rather than chosen. A row whose sealed manifest can no longer be
read cannot have its objects named, so the sweep reports it blocked instead of
marking it swept.

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

## The manifest as a residency demand (#583, off by default)

The same phase table this role windows on is what a storage-tier reservation is
derived from. `storage_tiers.manifest_phase_ranges` reads it — v1
`annotations.phases`, v2 `read_plan.phases` — and returns the half-open byte
ranges in read order; `storage_tiers.residency_demand` turns one range into the
whole GiB a movement node must hold on a tier. A table that does not describe
its manifest yields no ranges, the same refusal `manifest_phases` makes here,
because reserving on the wrong boundaries reserves for bytes nobody reads.

An item may then carry a `residency` block:

```json
{"schema": "prismabuild.residency.v1",
 "tier_id": "prismabuild-stage:dl380g10",
 "manifest_sha256": "...", "manifest_bytes": 4096,
 "range_start_bytes": 0, "range_end_bytes": 11496376320,
 "leads": ["<mover action key>"]}
```

A block with a range is a movement node: `publish` refuses it unless its
`stage_gib@<tier_id>` demand is at least that range's ceiling in GiB. A block
with `leads` is a consumer: it is admitted only once every lead's `done/` record
says `executed`. Both halves are optional and a block with neither refuses.

This prewarm role is unaffected. It does not read the `residency` block, no
fleet submission writes one, and no tier is minted unless a box runs the
separate `tiers` role. See the design document's cluster-scoped storage tiers
section.
