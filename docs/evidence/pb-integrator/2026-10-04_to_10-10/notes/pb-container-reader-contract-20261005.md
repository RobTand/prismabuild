# Staged-reader contract for a Docker consumer on a Spark (v1 DRAFT, for G3ReadsetRepair and kernels)

Author pb-integrator, 2026-10-05 15:10Z. Companion to `pb-residency-guide-20261005.md`. Read-only analysis of the live runtime
generation `04f6f00e0410` (`/mnt/shared/prismabuild-fleet/repo`); nothing here was run in a container. **No PrismaBuild core edit is
needed or proposed.** Tags: VERIFIED (I read the source or a receipt), UNQUALIFIED (nobody has run it), YOUR TEST (what the harness must show).
D32 applies: no source-stat adoption, re-seal or proof wall. What stays: own-range digest checks and the pinned-descriptor lifetime.

## 0. Why a container is a different case

The one qualified staged read (canary leg 3, sparky, 24 MiB, stage tier) ran as a plain host process. A `docker run` child gets none of the
action's environment and mounts nothing: the shim `tools/docker` only binds the container to the action (owner label, cgroup, CPUs) and passes
through whatever `-e`/`-v` the caller wrote (VERIFIED, read in full: its option tables list `-e/--env/-v/--volume` as plain pass-through). So
the consumer's own `docker run` line carries the whole contract below.

## 1. Environment: forward these six names, by name, from the action process (VERIFIED names)

`docker run -e NAME ...` (no `=value`) copies the value from the calling process, so nothing is hard-coded and nothing leaks.

| name | set by | meaning |
|---|---|---|
| `PRISMABUILD_RESIDENCY_MAP` | the launcher (`pool.residency_map_environment`) | path of the composed map; **unset means no map: read declared paths** |
| `PRISMABUILD_ACTION_KEY` | `core._residency_environment` | this action's key (64 hex) |
| `PRISMABUILD_ACTION_NONCE` | `resource_exec.payload_identity_env` | this attempt's nonce (32 hex) |
| `PRISMABUILD_ACTION_SCOPE` | same | `prismabuild-job<32hex>.slice`, derived from key+nonce; a mismatch is refused |
| `PRISMABUILD_QUEUE_ROOT` | the pool launcher (every action) | queue root; `injected_context` reads the live claim from it |
| `PRISMABUILD_READER_HELPER_ROOT` | `resource_exec` | the immutable generation ROOT (consumers append `/src` themselves) |

Rules the reader enforces (VERIFIED, `core._reader_identity_environment`, `reader_lease.injected_context`): the last three of the identity
set (nonce, scope, helper root) are all-or-none; a partial set refuses. The broker token and socket never cross into the reader. Do not seal any
of these names in the action's own `--env`: sealing one is a refusal (`conflicting = ... ACTION_RESIDENCY_ENV`).

## 2. Mounts (VERIFIED paths; the read/write modes are my reading, YOUR TEST confirms them)

| mount | mode | why |
|---|---|---|
| the helper root, same absolute path (`/mnt/shared/prismabuild-fleet/runtime-generations/<generation>`) | `ro` | the reader modules must load from it; `leg3._load_reader_modules` refuses any module not loaded from `<root>/src/prismabuild/`. Mount the generation, not the moving `repo` link |
| the queue root `/mnt/shared/prismabuild-fleet/pb-queue` | **`rw`** | `acquire_for` writes the pin and its ref under the residency root and `release` removes them; `injected_context` reads `claimed/<key>.json`. A read-only queue mount makes acquire fail |
| `/stage/prewarm` | `ro` | the map's `stage_path` values are absolute `/stage/prewarm/...`; mount at the **same path** so the map works unmodified |
| `/ram/prewarm` | `ro` | only if a RAM leg is used (`ram_path`); same-path rule |
| `/mnt/shared` (the declared origin paths) | `ro` | fallback for any key the map does not name, and the manifest file itself |

The map file lives at `<residency root>/<consumer key>.map.json`, which is under the queue root, so the queue mount also carries it.

## 3. Python in the image (UNQUALIFIED)

The reader chain (`reader_lease`, `residency_map`, `storage_tiers`) imports only the standard library, but it loads `prismabuild.pool`, a very large module whose
minimum Python version I did not establish (VERIFIED only that no third-party import appears in those three modules; the Sparks' host python is 3.12-class,
the image's is unknown). YOUR TEST: `python -c "import prismabuild.pool"` inside the exact image with `PYTHONPATH=<helper root>/src`). Pass `-e PYTHONDONTWRITEBYTECODE=1`:
the helper root is read-only.

## 4. Per-range call sequence (VERIFIED, `leg3.py` `_StagedWindow`; the only qualified reader)

1. `ctx = reader_lease.injected_context(env=os.environ)`; refuse on `{"ok": False, "refusal": ...}`.
2. `mapping = residency_map.read_map(ctx["map_path"])`. Entries are keyed `"<offset>:<path>"` (`residency_map.residency_map_key(path, offset)`).
   Tier is `mapping["tier_id"]`; the epoch is `""` for a stage tier and `mapping["ram_epoch"]` for a RAM tier (a RAM header must never lend its epoch to a stage pin).
3. `covers = reader_lease.covers_for_keys(root, consumer, [key], tier_id=..., manifest_sha256=..., epoch=...)`.
4. `acq = reader_lease.acquire_for(ctx, tier_id=..., epoch=..., covers=covers["covers"], expected={key: {"bytes": n, "sha256": h}}, span={"start_bytes": 0, "end_bytes": n}, acquire_token=<unique per range>, residency_root=root)`.
   Retry with a deadline on the retryable refusals (the mover may not have landed yet); leg 3 uses `LEG3_RETRYABLE_REFUSALS` and a poll interval.
5. `fd, serving = reader_lease.open_pinned(queue, acq["pin"], acq["ref_id"], key, residency_root=root)`. **Record `serving`** (tier, epoch, pin id): it is the proof of where bytes came from.
6. Read and hash `fd` against the manifest/map sha256 (own-range digest; this is byte integrity and stays under D32).
7. `os.close(fd)` **then** `reader_lease.release(queue, acq["pin_id"], acq["ref_id"], consumer_action_key=consumer, residency_root=root)`. Release-before-close is forbidden.

## 5. Pinned-descriptor lifetime (VERIFIED rule; the part a model server cannot satisfy by itself)

The pin must outlive every descriptor and every `mmap` of that file (`open_pinned` docstring: "mmap holds it; async prefetch holds it"; fork inherits
only via a registered ref). `open_pinned` also refuses a stale ref and a file whose inode, mtime or ctime no longer matches the pin; the CEO ruled
that stays (ABA protection inside one job, not a cross-run seal).

Consequence for a `>10 MiB` and for a whole-model consumer: a process that opens files itself (vLLM, `safetensors.safe_open`, `mmap`) is **not**
covered by the sequence above. Two shapes work in principle (both UNQUALIFIED, both are harness code, no PB edit):
- **A. In-process reader**: the consumer calls the sequence per range and keeps the pin until it has closed the descriptor/mmap (the Tessera `pb_staged_store` SDK mode).
- **B. Lease holder + path mapping** (for a server that insists on a model directory): a small holder process in the same container (or its parent) takes a pin for every
  whole-file entry before the server starts, keeps all refs until the server exits, and exposes a directory of symlinks `<name> -> <stage_path>` as the server's model dir;
  `/stage/prewarm` mounted at the same path. Only whole-file entries (offset 0, full size) can be symlinked; extents (offset > 0) need shape A.
Tessera's direct-vLLM mode "acquires no PB context, residency state or lease" and therefore gives the tier nothing to pin: do not use it for staged reads.

## 6. What the harness must show (YOUR TEST, each with evidence a reviewer can check)

1. Image: `import prismabuild.pool` works with the mounts above and `PYTHONDONTWRITEBYTECODE=1`.
2. `injected_context` returns `ok` inside the container (all six names forwarded; queue mount `rw`).
3. A range **larger than 10 MiB** (at least one whole 1 GiB-class shard and one extent with offset > 0) is read through `open_pinned`, digest equal, `serving.tier_id` recorded, on **both** Sparks.
4. The pin is released after close, and the queue shows no leftover ref (list the residency leases for the consumer before and after).
5. The negative: a key the map does not name falls back to the declared path and the receipt says so; the same job with `PRISMABUILD_RESIDENCY_MAP` unset reads declared paths only.
6. A killed container (SIGKILL) leaves a pin that is freed only by the PB reaper after proven scope stop, not by the next job (design says so; I did not verify it, so state what you observe).
7. No measured speed claim until a same-input A/B (staged vs declared path, cold and warm) exists; none exists now.

## 7. Stage tier alone vs the RAM tier: correction to the premise "while RAM refuses"

**The RAM tier is not refusing right now.** Read-only at 15:05Z: the tier loop's last `ram-admission-refused` was 14:21:34Z (1,808 total since 10:37Z, none in the 44 minutes since);
the RAM ledger shows capacity 160 GiB with 125 GiB available; host memory held by rows and fills fell to 44 of 235 GiB. It refuses whenever dl380g10 rows hold more than
about 96 GiB of memory tokens (window 160 + rows + ARC 22 + reserve 16 must fit in 294.5 GiB), and it did at 132 and 116 GiB. So it can flap.
Guidance stays: **use `--residency stage --residency-ram off` for the harness and for any first GPU job**, because (a) no read from `/ram/prewarm` on a Spark has ever been qualified, and
(b) a flapping tier must not decide whether a job places. This is an exception to what Rob asked for (`--residency-ram auto`), and the reason is qualification, not refusal. Qualify a RAM read on one
Spark with the same harness as a separate step (a RAM leg adds `ram_path`, `ram_epoch` and a RAM pin), then switch the default to `auto`.

## 8. Not verified

Anything in a container; the `rw` requirement on the queue mount (read from the write path, not exercised); the Python version floor; shape B; the killed-container behaviour; RAM reads on a Spark.

## 9. The Window 4 / ship-window A8S serve: what it is, and the integration it needs (added 15:25Z)

Facts from kernels' source inspection (CEO message 15:1xZ; I did not re-read their files, and nobody has observed a live model PID):
- Both arms (eager2048, eager4096) run `vllm serve <A8S exported root> --node-rank <r>` **inside each rank's Docker container**; `docker run -d` per Spark (`tp2_recipe.py:215-228`). The model-folder argument is the same in both arms.
- The serve path has **no** `injected_context`, `acquire_for`, `open_pinned`, `PRISMABUILD_RESIDENCY_MAP` or `PBStagedStore` reference. Tessera's stock `DefaultModelLoader` reads the safetensors shards by path. `pb_staged_store`'s SDK and direct-vLLM modes are benchmark transports and are not used here.
- Container binds today: `/mnt/shared` **read-only** at the same path, plus Tessera src, ext, out and digest dirs. **Neither `/stage/prewarm` nor `/ram/prewarm` is mounted**, and the queue root is only visible read-only (as part of `/mnt/shared`). Window 4 itself stays on warm L2ARC and is not to be touched.

What follows (my design, PROPOSAL, UNQUALIFIED): shape B of section 5, with the holder **outside** the container, which is simpler than I first wrote it.
1. **The lease holder is the PB action's own host-side process** (the rank-control process), not anything in the container. It already carries the launch identity (`ACTION_KEY`, `NONCE`, `SCOPE`, helper root, queue root, map), so no env forwarding into the container is needed for it, and the queue is writable there. Before it starts the container it takes a pin per whole-file entry (128 shards and metadata), and it releases them only after the container has exited (the pin therefore outlives every mmap, as the rule requires).
2. **A model directory of 128 symlinks** `<name> -> <stage_path>` (absolute `/stage/prewarm/...pbrange/0-<size>` paths) built on the host under a path the container can see (`/ext` or `/out`, already bind-mounted). Whole-file entries only (offset 0, full size), which A8S is.
3. **One new bind: `-v /stage/prewarm:/stage/prewarm:ro`**, at the same path, so the symlink targets resolve inside the container. The vllm argv's model argument becomes the link directory. No other mount or env change.
4. **One data manifest for the A8S set**, taken from the content manifest (128 rows, real sha256, so the mover verifies the copy against known content), as a single phase containing every entry (a server maps all shards at start; the pin has to cover them all at once, so a rolling window cannot work here).
5. **Declare** `--residency stage --residency-ram off --residency-share auto`, `--residency-prefetch-depth-gib` large enough for the one phase, and progress phases only if the wrapper can report them (guide section 3: declaring phases brings the progress contract).

Consequences, derived and not measured:
- **RAM is structurally unfit for a whole-model load at window 160:** the A8S set is 163.47 GiB and the RAM window is 160 GiB. That is a second reason, besides non-qualification and flapping, for stage-only on A8S.
- **What staging buys:** the HDD read (about 26 min cold at the 111 MB/s measured) moves to before the claim, so the GPU box is not held while the bytes land, and with `share auto` a later leg that reloads the same set finds it resident if it has not been evicted. It does **not** make the first cold load faster, and nothing is measured; the earlier derived figure of about 3.3 min is a NVMe-read estimate for a staged copy that is already resident.
- **It needs pbgang to carry `--data-manifest` and the residency flags** (worker PbgangResidencyMembers; the gang-with-pending-residency interaction is not established yet), or the A8S members to be submitted outside a gang.
- **Eviction safety:** while a pin is held the tier cannot evict those ranges; 163 GiB pinned for the life of a serve is 25% of the 645 GiB stage tier, which matters if two serves of different artifacts overlap.

