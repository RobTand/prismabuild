# Stage NVMe study and hot-read proposal (pb-integrator, 2026-10-05, read only)

Nothing was changed, moved or deleted. Every number is marked MEASURED, DERIVED (arithmetic on measured
numbers), or NOT CONFIRMED. Measured on dl380g10 and both Sparks between 11:30Z and 11:50Z.

## 1. What is on the stage NVMe, live versus stale (MEASURED)

- `prismabuild-stage` is ONE 745 GB NVMe (LT0800KEXVA), ZFS, 89 percent full, compression off, recordsize 1M,
  atime OFF (so access time cannot say what is cold). `/stage/prewarm` holds 647 GiB; only 10 GiB is available to
  it: `prismabuild-stage/pbqueue` has a 64 GiB reservation and quota (16.9 GiB used, 47.1 GiB reserved but empty)
  and ZFS keeps a slop of 1/32 of the pool (about 23 GiB). Verified with `zfs get`.
- By directory: tessera-measurements 456 GiB (glm-canonical-census-20260908 324 GiB in 48,809 files with files as
  new as today; glm-campaign-takeover-20260913 69 GiB; pq-rows-qualification-20260930 47 GiB;
  pact-e4m3-accuracy-20260928 12 GiB), models/GLM-5.3-Flash-BF16 184 GiB, tessera-runs 4 GiB, everything else
  under 6 GiB.
- PrismaBuild's own stage ledger: capacity 642 GiB, held 634, **evictable 632, committed 2 GiB**, growth admitted 0.
  All 684 recorded residency plans are state absent and 682 of their consumers are finished. So PrismaBuild itself
  counts 2 GiB as live.
- The stage keeps staged byte ranges (`model-NNNNN.safetensors.pbrange/<offset>-<size>`), not whole models. The
  shared copy of GLM-5.3-Flash-BF16 is 598.5 GiB; the stage holds 184 GiB of ranges. The census directory is 313.8 GiB
  on the stage against 3,045 GiB (926,935 files) on the shared pool.
- Is it a copy? A random sample of 600 of the 59,496 stage files: 584 range files whose shared file exists and
  covers the range, 16 whole files with the same size on shared, 0 missing. This bounds a missing rate at roughly 0.5
  percent at 95 percent confidence. It compares SIZES, not content hashes. The other stage directories were not
  compared in full (the full comparison timed out).
- Conclusion (DERIVED): about 632 GiB of the stage is a re-creatable cache that PrismaBuild's own tier loop may
  evict. Nothing needs deleting by hand; shrinking or reusing it goes through PrismaBuild's eviction.

## 2. Why Spark GPU reads are slow (MEASURED)

- The network is not the limit. Both Sparks mount /mnt/shared as NFS 4.2 over RDMA (nconnect 16, rsize 1 MiB) on
  100 to 200 Gb/s links; dl380g10 has two 100 Gb/s ports.
- The source is four HDDs in raidz1: 111 MB/s pool side at 11:3xZ (about 27 MB/s per disk, 30 to 47 ms waits), nfsd
  serving 513 MB/s at that moment mostly from cache.
- Volume: sparklina has read 33,401 GiB (32.6 TiB) from the server since its mount 8.8 days ago (about 43 MB/s
  average, bursts to 193 MB/s); sparky 2,644 GiB in 2.3 days.
- **The other SSD is already in use.** `nvme1n1p5` (759 GiB) is the L2ARC cache vdev of storage_pool, 96.8 percent
  full, already tuned (noprefetch 0, write_max 128 MiB/s, headroom 8, rebuild on). Lifetime: 40.8 TB read from it,
  20.2 TB written to it (heavy churn), 187.6 million L2 hits against 154.3 million L2 misses (55 percent). ARC is
  29.6 GiB in use of the new 64 GiB maximum. Settings are not the problem; capacity and pollution are: one shared
  cache serves every reader, and trees of 3 TiB and full rehashes evict the hot sets.
- Spark local NVMe is too small to cache the A8S artifact: sparky 130 GB free, sparklina 152 GB free.

## 3. Hot read sets (sizes)

| set | size | source |
|---|---|---|
| A8S artifact for Window 4 (128 files, largest 3.7 GiB) | 163.47 GiB | MEASURED, content manifest |
| GLM-5.3-Flash-BF16, whole model on shared pool | 598.5 GiB | MEASURED |
| GLM render chains (KL and stage-B renders), declared per plan | median 162.1, max 214.6 GiB (60 plans) | MEASURED, PrismaBuild plans |
| Stage chains | median 125.1, max 175.7 GiB (47 plans) | MEASURED |
| PACT-style row batch | median 2.66 GiB (266 plans) | MEASURED declared range |
| EXL3 artifact for Window 4 and ship window | NOT CONFIRMED. The only figures in records are 175.7 GB for the T-8 artifact and a 186.75 GB whole-artifact cap; not shown to be the EXL3 baseline | |
| What a PACT quanta job really reads from the BF16 model | NOT CONFIRMED. The 2.66 GiB figure is only the declared staged range; process read counters were unusable (they count pipes and sockets) | |

## 4. Options

1. **PrismaBuild staged reader from /stage.** Built for CPU consumers on dl380g10 (reader lease from the stage and
   RAM tiers). It does NOT serve Spark GPU jobs, which read /mnt/shared over NFS. Good for dl380g10 renders, no
   help for the Window 4 or PACT GPU reads.
2. **Spark-local cache.** Does not fit for A8S (see section 2) unless Spark disk space is freed first; each Spark
   would need its own copy. Not recommended now.
3. **Serve the hot sets from the stage NVMe over the existing NFS-RDMA path, at the SAME path.** Create a dataset on
   the stage pool, copy each hot set in, verify every file against its sha256 manifest, then mount it at the exact
   artifact path inside the exported tree so the Sparks see unchanged paths (action identity includes the env path,
   so a different path would break the reviewed identity). The HDD copy stays underneath as the fallback; nothing is
   deleted. NEEDS: root on dl380g10, a test first with a throwaway dataset to prove that NFSv4 clients traverse a
   child dataset mounted over an existing directory (I have NOT tested this), and stage space (about 340 GiB for
   A8S plus EXL3 if EXL3 is about 176 GiB), released through PrismaBuild's own eviction of its 632 GiB evictable cache.
4. **Reduce L2ARC pollution** (properties only, no vdev change): `secondarycache=metadata` on datasets that hold
   cold bulk captures. Works only if hot and cold data sit in different datasets; storage_pool/shared is one
   dataset, so this needs child datasets first. Not tested.
5. Excluded by your rule: adding or changing storage_pool vdevs (the textbook fix would be a bigger L2ARC).

## 5. Expected gain (DERIVED: bytes divided by measured rates)

| set | HDD pool 111 MB/s | stage NVMe 735 MB/s (measured floor) | ARC-warm 3.5 GB/s |
|---|---|---|---|
| A8S 163.47 GiB | 26.4 min | 4.0 min | 0.8 min |
| render chain median 162.1 GiB | 26.1 min | 3.9 min | 0.8 min |
| render chain max 214.6 GiB | 34.6 min | 5.2 min | 1.1 min |
| stage chain median 125.1 GiB | 20.2 min | 3.0 min | 0.6 min |
| whole BF16 model 598.5 GiB | 96.5 min | 14.6 min | 3.1 min |

The 735 MB/s is ONE dd stream measured while dl380g10 was at load average 124, so it is a floor, not the NVMe's
peak; the 3.5 GB/s is a repeat read served from ARC. A multi-stream NFS read of the stage will be faster than the
floor and is not measured. Expected gain over today's loaded HDD pool: at least 6.6 times, likely more.
(The HDD number is an under-load number; an idle pool reads faster, so the gain against a quiet pool is smaller.)

## 6. Recommendation and what I need

1. Do option 3 for the A8S artifact first (163.47 GiB, content-verified by its manifest), after a throwaway
   same-path test; it is the only option that keeps identity and serves the Spark GPU jobs.
2. Ask Rob which SSD he means: `nvme1n1p5` is ALREADY the pool's L2ARC and is nearly full; the stage NVMe is the
   other one. If he means freeing the L2ARC SSD for hot data, that changes a vdev and is excluded by your rule.
3. I need: root on dl380g10 for the dataset, mount and export steps (I do not have it), a go for the throwaway test,
   and the EXL3 artifact path and the real PACT quanta read set from the owners (campaign and kernels).
4. Measure before and after with the Sparks' own NFS counters (/proc/self/mountstats serverreadbytes) and the
   arcstats l2_hits counters, not process read counters.

## 7. Identity answer from kernels (10-05 12:25Z; I spot-checked the two cache keys in source, the rest is kernels' audit)

Hot set confirmed: `/mnt/shared/tessera-runs/moe/glm53-a8-bf16menu-20260930/release/exported`, 175,527,382,717 bytes (163.47 GiB), 128 files,
content hash 45407d43, config 3f5c2c73. Window 4 does not read the EXL3 weights or the BF16 teacher. Other reads are small immutable trees
(the pinned Tessera worktree, the stock client source, the rdv inputs, the A8S content manifest) plus live control trees that must never sit under an overlay
(the D1 feed, PrismaBuild runtime/CAS/queue/group records, the window's own output directory).

- **Artifact identity is content only.** Window 4 binds the path, file names, sizes and SHA-256 manifest. It stores no inode, device or NFS handle. A copy with the same paths, names, sizes and bytes keeps that identity.
- **Two persistent caches DO key on the inode, so a copy causes a miss and a full rehash (content SHA unchanged).**
  Tessera `source_digest_cache.py` keys on leaf, inode, size, mtime_ns and ctime_ns (I confirmed this at lines 24-26 and 98-100). PrismaQuant `cost_streaming.py` keeps the source-checkpoint digest cache on path, device, inode, size, mtime and ctime (inode field confirmed at line 1175).
  Plan: invalidate and repopulate these off the GPU, through the existing authorized cache path, and never reuse an old stat fingerprint as if it still matched. Rehashing 163 GiB from NVMe is cheap compared with the HDD read it replaces.
- **Per-job safety checks hold a file's identity for the life of the job** (PACT staged store, native identity check on the loaded ELF, Hessian FD-versus-path signatures). They are not cross-job IDs, but they make any mount change under a running job a hazard. Some banks also pin nanosecond mtimes (`piece_major_protocol.py`), so the copy must preserve mtime_ns where a bank says so (rsync -a plus a nanosecond check; I have not tested that).
- **Repeated opens are real.** Both server arms (eager2048, eager4096) reopen the same shards, and preflight, config and manifest rereads happen between arms. So the no-mount interval is the whole job or window including held FDs and mmaps, not just active I/O. Mount before the first open, keep it through terminal cleanup, and only then may new jobs establish new inode-based provenance.
- **Writer constraint, not a read-set one:** PrismaSnap hardlink transactions check device and inode. Hot-set copies must stay read-only.

Effect on the plan: the hot-set overlay is workable for the A8S release, but the order is (1) throwaway test, (2) copy and verify with sha256 and nanosecond mtimes, (3) invalidate the two digest caches off the GPU, (4) mount between windows only, never while any job holds a file open. Campaign's answer for Tessera-side read sets beyond Window 4 and PrismaQuant's real PACT read set is still outstanding.

