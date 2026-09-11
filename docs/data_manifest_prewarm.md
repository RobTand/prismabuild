# Declaring an action's data, and warming it before the claim

Issue #487.  Two pieces: a second content-addressed input that says which
bytes an action will read off the shared mount, and a single loop on the
storage host that makes those bytes resident before anybody claims the action.

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

`validate_data_manifest` refuses anything that is not exact identity: a path
outside `mount_prefix`, a path that is not already normalized, a relative path,
a repeated `(path, offset)`, a zero-length entry, and totals that disagree with
the list.  `offset` is there because the consumer reads tensor byte ranges out
of safetensors shards rather than whole files, and a whole-file manifest would
price the warm at several times the bytes the action touches.

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
2. The first `--lookahead` actions that carry a manifest.  Actions without one
   are passed over and do not consume the lookahead.
3. ARC headroom: `(c_max - size) * --arc-reserve-fraction`, minus the manifest
   bytes of every action claimed inside the last `--claim-grace-min` minutes.
   A manifest that does not fit is refused, and refusing writes nothing.
4. Reads each entry, in manifest order, through the host's local pool path
   (`--mount-map SHARED=LOCAL`), `--readers` at a time, `O_NOFOLLOW`, regular
   files only.
5. Writes `pb-queue/prewarm/<action_key>.json`.

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
still in `ready` when the warm runs, and rewriting a ready item races the claim
that may already have moved it -- which would resurrect a claimed action and
hand it to a second worker.  Nothing under the shared data mount is written.

### The receipt

`claim` copies the sidecar onto the claimed record as `prewarm`, in the write
it was already making, and `finish` carries it into the terminal record's
`detail.prewarm`.  A worker that measured its own residency wins: `finish`
uses `setdefault`.  An action nobody warmed carries no `prewarm` key at all,
because "nobody looked" and "warmed nothing" are different facts.

## Deploying

The `storage` role only exists once a runtime generation carrying
`prewarm_loop.py` is published.  A supervisor running an older generation
ignores the `roles` key; a supervisor running a newer one with no script logs
`role storage not startable` and keeps supervising its worker loops.  With no
manifest-carrying action in the queue the loop does nothing at all.
