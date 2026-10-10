# Declaring residency for GPU jobs: guide and Spark staged-reader contract (v1 DRAFT)

Author pb-integrator, 2026-10-05 14:30Z. For kernels and campaign. Source: the live runtime generation `04f6f00e0410`
(`/mnt/shared/prismabuild-fleet/repo`), PrismaBuild `docs/data_manifest_prewarm.md` and `docs/design.md`, the canary leg 3 code
(`tools/fleet/pbcanary_legs/leg3.py`) and its receipt. Nothing in this file moves, mounts or changes any path: **a running job such as
confirming1cb keeps exactly the paths it was started with.** Residency never touches the origin files.

Every claim carries a tag. **VERIFIED** = I read the source or a receipt and say which. **UNQUALIFIED** = no run has shown it; treat it
as unknown, not as working. **PROPOSAL** = my recommendation, to be confirmed by the owner.

## 1. What you get, in one paragraph

You declare the bytes a job will read, in the order it reads them (a data manifest). PrismaBuild copies those byte ranges, in that order,
from the HDD pool onto a stage tier (the NVMe dataset `/stage/prewarm`) and, if the RAM tier admits, onto a RAM tier (`/ram/prewarm`).
It admits your job only when its first phase is on the tier, and it evicts what the job has passed or what nobody wants. Your job is
told where the copies are by a map file and reads them under a lease (a pin) that stops eviction while it reads. Anything the map does not
name is read from the declared path as before. VERIFIED: `docs/data_manifest_prewarm.md`, `src/prismabuild/residency_map.py`.

## 2. Qualification table (the Spark staged-reader contract, as of now)

| Row | Status | Evidence |
|---|---|---|
| A Spark action receives `PRISMABUILD_RESIDENCY_MAP` | VERIFIED | `pool.py residency_map_environment`; canary leg 3, action `faccd6b4`, claimed on sparky |
| A Spark action reads a staged range from `/stage/prewarm` under a pin and the sha256 matches | VERIFIED, 24 MiB only | same receipt: 3 chunks, `serving.tier_id prismabuild-stage:dl380g10`, paths `/stage/prewarm/<run>/leg-3/<name>.pbrange/0-<n>`, pin ids recorded |
| `/stage/prewarm` and `/ram/prewarm` are the same absolute paths on both Sparks, read-only NFS-RDMA | VERIFIED | Sparks' `findmnt` (nfs4, `proto=rdma`, `ro`) |
| Read from the RAM tier (`ram_path`, epoch) on a Spark | UNQUALIFIED | leg 3 had `serving.epoch ""` (stage), RAM never exercised |
| A file larger than 10 MiB staged and read on a Spark | UNQUALIFIED | |
| A GPU action, or any process inside a Docker container, reading via the map | UNQUALIFIED | the Docker shim forwards no residency env and mounts nothing (`tools/docker`, read in full); a container job must be given the env and mounts by its own argv |
| A server (vLLM) that opens model files by directory path using staged copies | UNQUALIFIED | needs a path mapping plus a lease holder, see section 6 |
| Gang members with a manifest and residency | NOT YET PUBLISHED; branch `pb/gang-residency-members` is independently approved and a few pre-merge fixes are in progress | Today `pbgang` still refuses these options (live generation 1ac00386). Reviewed behaviour, from the pool review: a member whose staged bytes are not resident is held back before the gang code, so it does not elect or fence; its siblings DO elect and fence their hosts' lower-priority work, and that wait is unbounded if a lead ends terminally or a plan is refused (follow-up issue filed). **Rules for a gang manifest:** every member that declares `data_manifest` must also declare `--residency stage` (a manifest-only member is planned one row per tier-loop cycle, and the gang can commit with it unplanned, reading the pool at full cost without saying so), the manifest path must be absolute (pbgang's cwd, not the member's, resolves a relative one), and all members of one A8S gang should declare the same manifest with `--residency-share auto` (one shared mover, so they become resident together; a sibling can elect alone for at most one map-composition cycle, about 60 s). Submit the movers before the gang if the window is slow. |
| The stage tier evicts orphans under real demand | NOT OBSERVED LIVE | tier loop log: 0 `stage-orphan-evicted` events since 10:37Z; code path is the normal loop; see section 7 |
| The RAM tier admits its window | **ADMITTING since 14:21Z, but it flaps** | 1,808 `ram-admission-refused` 10:37-14:21Z, none since; ledger 160 GiB, 125 available; it refuses whenever dl380g10 rows hold more than about 96 GiB of memory tokens (it did at 132 and 116 GiB). A decision (`dec-1005-140823-4934`) chose stage-only for GPU inputs; no RAM read on a Spark is qualified either way |

So today, **plan for the stage tier. Use `--residency-ram off` until the RAM tier admits and a Spark read from it is qualified.**

## 3. The manifest (read-order data manifest)

`pbcampaign.data-manifest` is an ordinary input (`{id, sha256, bytes}`) addressing a JSON blob. VERIFIED: `docs/data_manifest_prewarm.md`.

```json
{
  "schema": "prismaquant.prismabuild.data_manifest.v1",
  "produced_by": {"tool": "...", "commit": "...", "unix": 1789090615},
  "annotations": {"row_id": "a8s-load",
    "phases": [
      {"name": "p0", "bytes": 43980465111, "cumulative_bytes": 43980465111},
      {"name": "p1", "bytes": 43980465111, "cumulative_bytes": 87960930222}]},
  "mount_prefix": "/mnt/shared",
  "entries": [
    {"path": "/mnt/shared/tessera-runs/.../exported/model-00001.safetensors", "offset": 0, "bytes": 1465868832, "sha256": null}
  ],
  "entry_count": 1, "total_bytes": 1465868832
}
```

- **`entries` are in consumption order and are never re-sorted.** A warm cut short leaves a useful prefix.
- Paths are absolute, normalized, under `mount_prefix`. No repeated `(path, offset)`, no zero-length entry, totals must agree.
- `offset` + `bytes` is a byte range of the file. A whole file is `offset 0, bytes = file size`. Tensor extents out of a safetensors shard are `offset > 0`.
- `sha256` may be null in the manifest (the GLM census manifests are). The map requires a digest per entry, so the mover hashes what it copies; but with a null manifest digest that is only the mover's own hash of what it read, not a check against known content. **Put the real sha256 in every entry you can** (the A8S content manifest has all 128), so the mover verifies the copy against known content. The manifest itself is content-addressed, so the action key covers exactly these bytes.
- **Phases** (v1 `annotations.phases`, running byte sums; v2 `read_plan.phases` with `entry_indices`) are what let a manifest larger than the tier be staged as a rolling window. Boundaries must fall between entries; a table that breaks a rule is treated as absent. A v2 plan can revisit entries (a later phase may read the same range again).
- Limits: 64 MiB stored, gzip allowed (expands to at most 512 MiB, plain `gzip -n`), 1,000,000 entries, 4,000,000 read references (v2).
- **Attaching a manifest changes the action key.** A job with a manifest is a different action from the same job without one.
- **Phase names and progress.** The tier publishes the rest of the staging window, and releases bytes behind the reader, as the job's *accepted progress* advances (`docs/data_manifest_prewarm.md`, leg 3 header). So the job declares `--progress-phase NAME=SECONDS` for each phase, in order, with the same names as the manifest's phases (leg 3 uses `leg3-c0`, `leg3-c1`, `leg3-c2` for both). **This is not free:** declaring any progress phase admits the job under the *progress contract* (`pbrun --help`): it is then bounded by how long it goes without committing work in a phase (SECONDS), not by how long it runs, and it must report with `prismabuild.progress.commit` (or by running `$PRISMABUILD_ACTION_PROGRESS_HELPER` when it cannot import PrismaBuild). A server that loads for minutes and then serves for hours has to be given a quiet allowance that fits, or report from a wrapper. What the tier does for a job that declares a manifest but reports no progress, I did not verify (UNQUALIFIED); the ARC prewarm keeps claim-plus-grace accounting for that case, the stage tier is not documented. `--progress-cycle` lets phases repeat (each phase gets one allowance between increases in cumulative committed units) and needs cyclic-capable workers; it is the flag for a reload per leg, and I have not seen it used with residency.

## 4. The flags (`pbrun`; in `pbcampaign` the row field `data_manifest` is the first flag)

| Flag | What it does | Guidance |
|---|---|---|
| `--data-manifest PATH` | attach the manifest | required for everything below |
| `--residency stage` | seal one mover and one egress per phase; admit the job once phase 1 is staged and pinned | the explicit opt-in. A row that merely declares a manifest is also planned by the tier loop (#1247, `manifest_promotion.py`, one row per cycle in claim order) |
| `--residency-ram auto\|off` | add a RAM leg when a RAM tier is live | **use `off` today** (no RAM read on a Spark is qualified, and the tier flaps with row load); `auto` is the default |
| `--residency-share auto\|off` | stage a range once for every consumer that reads it | `auto` is right for repeated loads of one artifact; the second reader names the first's mover |
| `--residency-tier ID` | which stage tier | only needed if more than one is announced |
| `--residency-prefetch-depth-gib N` | GiB the job holds ahead of the phase it reads | **declare it.** Undeclared, the window is priced at the job's `mem_gb` plus GPU budget, which for a 100 GiB GPU job is far too large |
| `--residency-read-mb-s N` | how fast the job reads staged bytes | **declare it.** Undeclared, the window is priced at its run-ahead bound until the job reports progress |
| `--residency-mover-mem-gb`, `--residency-mover-readers`, `--residency-mover-max-attempts` | mover sizing (defaults 1 GiB, 4 readers, 3 attempts) | leave unless a receipt says otherwise |

## 5. How the job finds its staged paths (VERIFIED by leg 3; this is the only qualified reader)

1. The launcher sets `PRISMABUILD_RESIDENCY_MAP` only if the composed map file exists and the row has leads or declares the manifest input. If it is unset, read the declared paths.
2. The map (`prismaquant.prismabuild.residency_map.v1`) has `tier_id`, `stage_root`, `manifest_sha256`, `leads`, `entries` keyed `"<offset>:<path>"` with `stage_path`, `bytes`, `offset`, `sha256` and optional `ram_path`, and for RAM `ram_tier_id`, `ram_root`, `ram_epoch`. A key the map does not name falls back to the declared path.
3. The reader must come from the admitted helper tree (`PRISMABUILD_READER_HELPER_ROOT`, the immutable generation root; append `/src`), not from an installed copy. The launch identity (`PRISMABUILD_ACTION_KEY`, `PRISMABUILD_ACTION_NONCE`, `PRISMABUILD_ACTION_SCOPE`, `PRISMABUILD_QUEUE_ROOT`) comes from the launcher's proxy; a partial set is refused.
4. Per range, in order (the whole sequence is in `leg3.py` `_StagedWindow`):

```python
from prismabuild import reader_lease, residency_map          # from <helper_root>/src
ctx = reader_lease.injected_context(env=os.environ)["ctx"]    # {"ok": False, "refusal": ...} if not launch-bound
mapping = residency_map.read_map(ctx["map_path"])
key = residency_map.residency_map_key(path, offset)           # "<offset>:<path>"
covers = reader_lease.covers_for_keys(root, consumer, [key], tier_id=..., manifest_sha256=..., epoch=...)
acq = reader_lease.acquire_for(ctx, tier_id=..., epoch=..., covers=covers["covers"],
        expected={key: {"bytes": n, "sha256": h}}, span={"start_bytes": 0, "end_bytes": n},
        acquire_token="<unique>", residency_root=root)        # wait and retry on the retryable refusals: the mover may not have landed yet
fd, serving = reader_lease.open_pinned(queue, acq["pin"], acq["ref_id"], key, residency_root=root)
... read fd, hash it against the manifest ...
os.close(fd); reader_lease.release(queue, acq["pin_id"], acq["ref_id"], consumer_action_key=consumer, residency_root=root)
```

   The pin must outlive every descriptor and every mmap of that file (release-before-close is forbidden). `open_pinned` refuses a stale ref and a file whose inode/mtime/ctime no longer matches the pin.
5. Hazards for a GPU job: the Docker shim passes no residency env and mounts nothing, so the job's own `docker run` must carry `-e PRISMABUILD_RESIDENCY_MAP -e PRISMABUILD_ACTION_KEY -e PRISMABUILD_ACTION_NONCE -e PRISMABUILD_ACTION_SCOPE -e PRISMABUILD_QUEUE_ROOT -e PRISMABUILD_READER_HELPER_ROOT` and mounts for `/stage/prewarm` (read-only), the queue root (the pin is a write to the queue) and the helper root, and the image must run Python able to import it. **UNQUALIFIED:** nobody has run this in a container.

> **Container consumers: the full helper/mount/env/lease contract is `/home/rob/fleet/inventory/pb-container-reader-contract-20261005.md`** (six env names to forward, the five mounts and their modes, the per-range call sequence, the pin-lifetime rule, and what a harness must show). Section 5's hazard list below is superseded by it.

## 6. Serving a model (vLLM) from staged copies: what is missing (PROPOSAL, UNQUALIFIED)

Kernels' source inspection (15:1xZ) settles how Window 4 opens A8S: `vllm serve <A8S root>` inside each rank's Docker container, stock `DefaultModelLoader` reading the shards by path, **no** PB reader call anywhere in the serve path, and `/stage/prewarm` not mounted (only `/mnt/shared` read-only). So staged bytes reach a server only through a lease holder plus a path mapping. The design (host-side holder in the PB action, a directory of symlinks to `stage_path` files, one added read-only `/stage/prewarm` bind, one single-phase manifest, stage-only) is in section 9 of `pb-container-reader-contract-20261005.md`. UNQUALIFIED: nobody has run it. Window 4 itself stays on warm L2ARC and is untouched.

## 7. Eviction and the RAM tier, as they behave

- Stage tier now: capacity 645 GiB, held 638, **636 evictable orphans**, 2 committed, 10.7 GB available (starvation report 14:05Z). Orphans are evicted only when a live consumer's window cannot be placed without the room (`tier_loop.window_pressure` then `sweep_orphans(pressure)`), oldest receipt first. The first real staged job larger than the free space is therefore the proof; none has asked yet.
- RAM tier: `window + rows_held + max(ARC c_max, floor) + reserve <= MemTotal` with window 160 GiB, ARC 22 GiB, reserve 16 GiB, MemTotal 294.5 GiB: admissible only while dl380g10 rows hold at most 96.5 GiB of memory tokens. Today about 132 GiB. Admission flaps with row load (refusing at 132 and 116 GiB of rows, admitting at about 37-44 GiB), so a job that needs the RAM leg can wait on it; stage-only (`--residency-ram off`) does not.

## 8. Identity under D32 (sealing is off): there is nothing to do

A staged copy is a different file: new inode, new ctime, a different directory (`<name>.pbrange/<start>-<end>`, so a different leaf). Two bindings in the fleet are keyed on that physical identity (campaign, `rep-1005-123423-b839`): the old PACT source proof stores device, inode, size, mtime and ctime of each BF16 shard and its checkpoint owner compares all five (`tessera_calibration_cache.py:295-301`); `SourceDigestCache` keys on leaf, inode, size, mtime_ns and ctime_ns (`source_digest_cache.py:109-130`, device recorded not keyed). A copy will not match either.

Under **D32** (Rob, 2026-09-24, now in `STANDING.md`: all sealing is disabled; dev mode is on unless `PRISMAQUANT_DEV_MODE` is exactly `0`; I have not read the PrismaQuant `seal_check` code myself) such a mismatch is **stamped `[DEV-MODE]` and the run continues using stored data**: no recompute, no re-pin, no archive. So this guide adds none of the following, and nobody should: a fingerprint transplant onto a staged file, a cache adoption step, a quiescence wait, a new cache, a re-seal or a proof packet. (An earlier draft of this section proposed `SourceDigestCache.adopt`; that is withdrawn.)

What still holds, because D32 leaves it in force:
- **Byte integrity.** The mover verifies the copy against its digest (the manifest entry's sha256 where present, otherwise its own hash of what it read; section 3) and the reader checks what it reads against the map entry's sha256. A digest of a byte range is not the digest of the shard it came from; do not record one as the other.
- PrismaBuild action keys (a manifest changes the key), exact-head code review, and the safety gates (OOM guard, disk, GPU exclusivity).
- Content-only identities need nothing: the A8S shipping manifest (`/home/rob/fleet/inventory/t8-v1-a8s-content-manifest-20261005.json`, 128 rows of name, bytes, sha256; identical to the Window 4 one) and the EXL3 inventory bind name, size and sha256, not inode.

One judgment call I made and did not change: `reader_lease.open_pinned` refuses a file whose inode, mtime or ctime no longer match its pin (it is how a lease guarantees the descriptor you read is the one that was pinned, and stops a republished file's different bytes being served as the pinned range). I treat that as byte integrity, not a seal. If D32 should cover it, say so and I will bring it to review as a change.

## 9. Example, the qualified one and an illustrative one

Qualified (canary leg 3, run `20261005T104448Zc`): a 3-chunk v2 manifest (6, 8 and 10 MiB), phases `leg3-c0..c2`, submitted as

```
pbrun --cwd <checkout> --data-manifest <manifest> --residency stage \
  --progress-phase leg3-c0=600 --progress-phase leg3-c1=600 --progress-phase leg3-c2=600 \
  --timeout-s 600 --wait-s 900 --deterministic -- python3 tools/fleet/pbcanary_legs/leg3.py --run-action
```

Illustrative only (not run): for a job that reads A8S in 4 phases of about 41 GiB: `--data-manifest a8s.manifest.json --residency stage --residency-ram off --residency-share auto --residency-prefetch-depth-gib 41 --residency-read-mb-s 800 --progress-phase p0=1800 --progress-phase p1=1800 --progress-phase p2=1800 --progress-phase p3=1800 --progress-cycle`. It would need the section 6 tooling.

## 10. Not verified by me

The RAM tier on a Spark; any file over 10 MiB; a container; the gang interaction (under test); eviction under live demand; the Window 4 load path (asked of kernels).
