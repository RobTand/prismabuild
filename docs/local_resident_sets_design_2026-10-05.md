# Local resident sets: a GPU-host NVMe cache tier with leases (design note, DRAFT for review)

Author: pb-integrator, 2026-10-05. Status: **design only, nothing built.** Direction from Rob via the CEO: "Things that will be needed a lot
should probably not be streamed from dl380 and instead delivered for campaign duration and cleaned up afterwards." The CEO's sketch has five parts
(resident sets with leases, transparent paths, locality-aware placement, capacity, migration of the manual A8S copies). This note keeps the goal,
**changes two of the five parts**, and puts a measurement phase in front of the build. Every claim is tagged VERIFIED (I read the source or ran a
read-only command, and say which) or UNVERIFIED.

**Prior (D33).** Hindsight was unreachable when I wrote this (health `crit`, "API unreachable: connection reset", 16:39Z to at least 16:55Z; the API process on dl380g10 had just restarted),
so I could not check history. I searched the fleet's own records instead (`records/*.json`, `inventory/*.md`, `ceo/STATE.md`): the only prior art is my own hot-A8S same-path NVMe
overlay plan of 10-05 (cancelled by the CEO in favour of PrismaBuild's own tiering) and the manual local copies the Opus agent `spark-local-models` is making now. Treat "no earlier ruling found" as
unproven. **Reviewers: please rerun the history query when Hindsight is back and attach hits.**

## 1. What exists today (VERIFIED, source and live reads)

- **All tiers are on dl380g10.** Tier kinds are `arc`, `stage` (the `prismabuild-stage` NVMe dataset, 645 GiB, 636 evictable) and `ram` (tmpfs, 160 GiB window). `tier_loop` is a single writer
  on the file server; every other box has "absent" for the stage tier. The Sparks read the stage and RAM tiers read-only over NFS-RDMA at the same absolute paths.
- **Residency machinery** (`--data-manifest`, `--residency stage`, #583, #909, #1026): movers copy byte ranges onto a tier, a ledger (`stage_gib@<tier>` tokens) accounts capacity, a composed map tells a
  consumer where its bytes are (`PRISMABUILD_RESIDENCY_MAP`), reader pins stop eviction while a job reads, and an orphan sweep evicts what nobody names. It is a **rolling window for one consumer**, and
  residency is an **admission gate**: the row is not claimed until its first phase is resident (`residency_verdict`).
- **No PB code mounts anything into a container.** The Docker shim (`tools/docker`) binds a container to an action (owner label, cgroup, CPUs) and passes `-e/-v` through. Window 4's own `docker run`
  mounts `/mnt/shared` read-only and nothing else (kernels' source inspection).
- **Placement** is `_matching_offers` (tags, GPU, images, interpreters, files). Residency of bytes plays no part in it.
- **Sparks' disks** (read-only `df`, 10-05 ~16:50Z): sparky `/` 1,834 GiB, **125 GiB free (93% used)**; sparklina `/` 916 GiB, **147 GiB free (84%)**. One ext4 partition each (`errors=remount-ro`, no quota options),
  and `/var/lib/docker` is on the same filesystem (images 186 GB on sparky, 219 GB on sparklina; reclaimable 29 and 42 GB). `/home/rob/models-local` does not exist yet on either.
- **D1 (`fleet-diskcheck`):** at least 5% free, and a write of N GiB needs at least 1.5N + 20 GiB available. **Copying the 163.47 GiB A8S set therefore needs about 265 GiB free on the target**, plus the 5% floor
  (92 GiB on sparky, 46 GiB on sparklina). Neither Spark can do it today.
- **Private mount namespaces are not available to our jobs.** On both Sparks `kernel.apparmor_restrict_unprivileged_userns=1`, and `unshare -Urm` fails with `write failed /proc/self/uid_map: Operation not permitted`
  (ran as rob, nothing mounted). Only a root component could do it; the root-owned resource broker is the only one we have.

## 2. Where I disagree with the sketch

**2.1 A local copy must be an accelerator with fallback, not an admission gate.** In the sketch, locality is "a strong preference, not a pin". But the existing residency machinery is a **gate**: a row whose bytes are not resident is
not claimed. If a host-local tier reused it unchanged, residency on `local:sparky` would make the row sparky-only, which is the pin the sketch says it does not want. So the local tier needs different semantics:
the canonical path stays authoritative and always readable; local residency never blocks a claim; at launch, if (and only if) the set is complete, verified and leased on the claiming host, PB injects the mount, and otherwise
the job reads the canonical path over NFS exactly as today. Cost: **the fallback is silent to the job**, which is the failure PB's residency map already warns about ("silently, at full cost"). So every attempt must record what it
got (`served_from: local|canonical`, set digest, host) in its record, and `pbstatus` must show it.

**2.2 "Private mount namespace" for non-container actions: drop it from the plan.** It does not work unprivileged (section 1). The options are a root helper (the resource broker creating a mount namespace for the
payload) or nothing. I recommend **containers only** in the first release: the gap the CEO cares about (vLLM serve, the G3 instrument) is all in containers, and a broker change that mounts into job namespaces is a
security-sensitive design of its own. Non-container consumers keep reading the canonical path, or read the local path explicitly if their author wants to.

**2.3 The copy tier is not the existing window tier.** Reuse the parts that fit (movement-action receipts with byte verification, `ResourceLedger` for capacity, pins that protect in-use bytes, the data manifest as the
declaration). Do **not** reuse the phase windows, the map composition or the orphan sweep: a resident set is static, whole and long-lived. A separate small subsystem ("resident sets") sharing those primitives is
smaller and safer than generalising `tier_loop`, which is dl380-centric (`stage_roots`, ARC and RAM arithmetic, a host-local singleton lock).

## 3. Design

### 3.1 Objects
- **Resident set record** (queue root, one JSON per set, immutable body plus an append-only lease log): `set_id` = sha256 of the canonical manifest; `manifest` (entries: path, bytes, sha256, **real digests required**); `canonical_root`
  (the directory the container sees, for example `/mnt/shared/tessera-runs/moe/glm53-a8-bf16menu-20260930/release/exported`); `hosts` (the GPU hosts that should hold it); `lease` (`until` a date, or `campaign` id plus a hard maximum);
  `immutable: true` (see 4.1); `created_by`.
- **Per-host copy record** (written by the mover on that host): `state` (`absent|copying|resident|evicting`), `local_root`, verification receipt (every file's sha256 of the bytes it wrote, equal to the manifest's; this is byte
  integrity and stays under D32), `bytes`, `completed_unix`.
- **Host capacity** via the existing ledger: a new capacity kind `local_gib` on tier id `local:<host>`, minted by a small per-host policy file (`local_tier_policy.json`, published with the runtime like `ram_tier_policy.json`):
  root directory, a maximum GiB, and the D1 floor.

### 3.2 Lifecycle
1. `pbresident publish --manifest M --canonical-root DIR --hosts sparky,sparklina --lease-until DATE|--campaign ID`: validates the manifest (shape, digests present, **the set covers the whole directory**, see 4.2), takes ledger tokens `local_gib@local:<host>` on each host, files the record.
2. A **mover action per host** (existing movement-action pattern, pinned to that host, ordinary x86 CPU row): copies from the nearest source, hashes while writing, verifies, renames into place, files the copy record. Source preference: the dl380g10 stage tier
   if resident (measured 852 MB/s single stream over NFS-RDMA), else the canonical HDD pool (about 111 MB/s cold, 26 min for A8S).
3. **Lease:** a set stays while `now < until` **or** any queued or claimed row references it (a row references a set when its manifest entries are a subset of the set's), refreshed by the lease pass. A campaign lease ends when the operator closes the campaign
   (`pbresident release`) or the hard maximum passes, whichever is first. **There is no "forever" lease**: a lease without a maximum is how a 163 GiB copy outlives its campaign.
4. **Eviction:** when the lease ends and no live attempt holds a pin on the copy, a per-host pass moves the copy to a `.evicting` name, releases the tokens, then deletes. A pinned copy is never deleted: a container with the directory bind-mounted
   makes `rmdir` fail with EBUSY, and deleting files a running vLLM has mapped only moves the space cost to when it exits. The pin is a record held by the attempt, freed on proven scope stop (the reader-pin rule already in `reader_lease`).
5. **Injection:** the Docker shim, on a `docker run` by an action whose declaration names the set, resolves the claiming host's copy record and, if `resident`, adds `-v <local_root>:<canonical_root>:ro` (a nested bind inside the existing `/mnt/shared:ro` bind; Docker orders mounts by depth, UNVERIFIED on this Engine, Phase 0 tests it),
   takes the pin, and records `served_from` in the attempt. If the copy is not resident it adds nothing.

### 3.3 Declaration
A row opts in with one flag, for example `pbrun --resident-set SET_ID` (or the shim derives it from the row's manifest). The set id is part of the sealed action, so the same command with and without a set is a different action, as `--data-manifest` already is.
**The mounts the shim adds are environment, not identity**: the bytes are content-verified equal to the canonical ones, so the action's result does not depend on which path served them (see 4.1 for the one way it could).

### 3.4 Placement
Add a **preference** to `_matching_offers` ordering and gang election: among hosts that already pass every gate, prefer one whose copy record is `resident`. Never a filter (D26). For a two-member gang that must use both Sparks the preference is moot; the value there
is that both copies exist before the gang starts, so a publication step should wait for `resident` on every listed host, with a bounded timeout, and then proceed with fallback rather than hold the gang.

### 3.5 Capacity (the part that will bite first)
- Budget per host = (free bytes) minus (5% floor) minus (docker image growth allowance), counted **before** the copy: a copy of N GiB needs 1.5N + 20 GiB free on the target under D1 (the copy writes a temporary tree and verifies it before the rename).
- Today: sparky has 125 GiB free with a 92 GiB floor, so about **33 GiB** is available; sparklina has 147 GiB with a 46 GiB floor, so about **101 GiB**. **A8S (163.47 GiB) fits neither**, and with the 1.5N + 20 rule needs 265 GiB free. Whether it fits after the Opus cleanup is unknown until that
  inventory exists. Because `/var/lib/docker` shares the filesystem, the image store competes for the same bytes; the policy should reserve an explicit docker allowance rather than assume images stay flat.
- ext4 has no quota here, so capacity is **ledger accounting plus a pre-copy `df` check**, not enforcement. A runaway process can still fill the disk; the 5% floor is checked again by the mover before each file.

### 3.6 Migration of the manual copies
The Opus agent is making `/home/rob/models-local/...` copies with systemd bind-mount units at the same path (a **global** mount per host). Adopting them is possible without recopying: verify every file's sha256 against the manifest (about minutes for 163 GiB), `mv` the
directory into the tier root (same filesystem, so inodes and bytes stay), and file the copy record as `resident`. Two cautions: (a) the global bind mounts conflict with the "no global mounts" goal and with running jobs (the CEO's own same-path NFS overlay test showed server-side mount
changes are not deterministic for a cached dentry; per-container mounts avoid that), so the units should be removed when the set is adopted; (b) A8S is the first set only if it fits (3.5).

## 4. Risks the sketch does not mention

**4.1 A stale local copy is worse than a slow read.** If the canonical directory changes after the copy, a container that gets the local mount silently reads old bytes. The sets must be declared `immutable`, the mover hashes the canonical source as it copies and records source size and mtime as a **stamp**
(D32: a diagnostic, never a refusal), and the lease log should name the manifest digest so a reader can see which version a result used.

**4.2 A nested bind mount shadows the whole canonical directory.** Mounting a local directory over `canonical_root` hides every file under it that the local copy lacks, including files added later. A set must therefore cover the **entire** directory (the publish step lists the canonical directory and compares to the manifest; a mismatch
is a validity refusal, not a seal), or the mount must be per file (more `-v` arguments, and a directory listing inside the container would no longer match).

**4.3 Does it beat staged-over-NFS at all? Measure before building.** The staged tier already serves A8S to the Sparks over NFS-RDMA at 852 MB/s for one stream (measured today); `nconnect=16` over 100 to 200 Gb/s may give several GB/s aggregate,
which I did **not measure**. There is a prior claim that it is large: the issue #523 reporter measured **259 MB/s with one serial reader and 9,348 MB/s with 32 readers** on these mounts (`docs/fleet_storage.md`; the record says these are the reporter's numbers, not a maintenance before/after, and that the 1 MiB RPC window was reproduced but the concurrency limit was not established). At 9 GB/s, 163 GiB is about 19 s of wire time, so the file server's HDD pool and ARC, not the network, would be the limit (the stage NVMe tier removes that). If a multi-stream NFS read already loads A8S in a few minutes, a local copy buys independence from dl380g10's load (ARC pressure, HDD contention, NFS server threads) and reload reliability, not raw speed. That may still be worth it for a ship window that reloads A8S on every leg, but
the note should not promise a speedup without a measurement.

**4.4 Spark disks are nearly full for a reason, and the docker store is on them.** Putting a 163 GiB resident set on a 93%-full system disk makes every later `docker pull` and every temp file compete with it. The tier needs an explicit maximum and the ability to refuse a set that would push the host under the floor, with the refusal visible in `pbstatus`.

**4.5 Gangs.** A gang member that pays for a copy (a mover on the Spark, reading the file server over NFS) uses the same NFS path and Spark CPU as the gang's own loading. Copies should be scheduled before the window, not during it.

**4.6 Locking and failure.** A mover killed mid-copy leaves a partial tree: the copy record stays `copying`, the tree is under a `.partial` name, and the next mover run resumes or restarts it; nothing is ever mounted from a `.partial` tree.

## 5. Phases

- **Phase 0, measure (no PB code; about a day).** (a) Multi-stream NFS-RDMA read rate (**repeat the #523 reporter's 32-reader figure on today's mounts, from the stage tier and from the HDD pool, cold and warm**) of a staged A8S file set from sparky and sparklina (1, 4, 16 streams) against a local NVMe read of the same bytes; (b) Docker nested bind: mount a local directory over a subdirectory of an existing `-v /mnt/shared:/mnt/shared:ro`, confirm
  ordering and that a `ls` inside shows only the local files; (c) write throughput of each Spark's NVMe and its interaction with a running GPU job; (d) the post-cleanup free space and the docker allowance; (e) the Opus agent's manual copies' completeness. **Gate: continue only if local beats staged-NFS by enough to matter for the ship window, or if the reliability argument is accepted.**
- **Phase 1, resident sets without placement.** Set and copy records, policy file, ledger kind, mover, verification, lease pass, eviction with pin protection, adoption of the manual copies, `pbstatus` view, `served_from` record. No shim injection yet (consumers read `local_root` explicitly, which Window-style harnesses can do).
- **Phase 2, container injection** in the Docker shim for declared sets, with the fallback record.
- **Phase 3, locality preference** in offer ordering and gang election.
- **Phase 4, campaign lease integration** (close hooks, renewal by live rows) and operator tooling.
- **Not planned:** private mount namespaces (needs a root helper; revisit only if a non-container consumer needs it), a global host mount of any set, and any seal over the copy beyond its own content digests.

## 6. Open questions for the reviewer

1. Is "accelerator with fallback" (2.1) the right default, or does the campaign want a **required** mode (a row that must read local and fails if it cannot) for measurements where NFS jitter would pollute a timing? I propose both flags, required being opt-in.
2. Should the shim inject mounts, or should PB expose the local path through an environment variable and let the launcher decide? Injection keeps tools unchanged (the CEO's stated goal) but couples the shim to the data contract.
3. Is a per-host mover on the Spark acceptable under D29 (the Sparks run GPU jobs and aarch64/GB10 work only)? A copy is I/O, not CPU analysis, but it is an action on the box.
4. Lease ownership: who may release a campaign lease (any lead, or only the one that published it)?
5. What is the right hard maximum lease (I suggest 14 days, renewable by an explicit act)?
