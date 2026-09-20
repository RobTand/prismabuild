# Worker expansion + join/resign design (task root)

Source commit: `e91a55d` (PB main, PR #724 staged-read target contract).
Worktree: `/home/rob/tmp/pb-worker-expansion-20260920`,
branch `feat/worker-expansion-join-resign-20260920`.
Dirty `/home/rob/prismabuild` checkout untouched (review/simplify-20260919).

This doc is the early contract the integration worker consumes. It cites
exact code at `e91a55d`; line numbers are that commit.

User authority:

- Original brief (`worker-expansion-contract-brief.md`): heterogeneous +
  homogeneous expansion, capability matrix first, then formal contract with
  stable FLEET IDs, qualification before first admission, no invented
  backends, no parallel dispatcher/cache/scheduler.
- Latest user instruction (2026-09-20, this lane): a worker (host fleet
  member = all its loops) can RESIGN what it claimed; new workers can JOIN.
  First-class deterministic join/resign API/CLI on the existing fleet
  lifecycle, exact instance identity/epoch, atomic stop of new claims,
  resumable committed work, exact-scope containment before release,
  explicit interrupted terminal for unknown/non-retry-safe, preserve
  attempt budget, never release on heartbeat loss, voluntary resign distinct
  from crash/expiry, no auto-rejoin without explicit desired membership,
  join validates before eligibility, frozen child scope untouched, named
  races. Owner: fleet membership CLI/supervision/new module; pool/tier
  runtime owned by lease worker; pool claim hook via root.
- Root coordination (2026-09-20): reuse `worker_loop._drain_gate` (~:207),
  broker `maintenance_begin/end` and supervise declaration draining
  (~:1539-1575); no parallel membership/scheduler if the declaration
  already provides authority; `changed_unix` identity + exact-loop acks
  matter; tiny pool claim hook via root only.

## 1. Actual capability matrix (measured, not tags)

Built from `tools/fleet/fleet_boxes.json` (boxes + `_why` provenance),
`src/prismabuild/box_capacity.py` (offer/observe, `GPU_MEMORY_DOMAINS`,
`GPU_CAPACITY_SCHEMA`, 5 s freshness, 8 GiB margin),
`tools/fleet/worker_loop.py` (offer publication, class/tags, image inventory),
`tools/fleet/resource_broker.py` (systemd containment, maintenance gate),
`docs/amd_gpu_capacity_2026-09-12.md`, `docs/adaptive_gpu_admission_2026-09-05.md`,
`docs/agent_execution_policy.md` ("Adding workers"), and
`docs/heterogeneous_cohort_design_2026-09-19.md` (one scope per action stays).

| Box (roster key) | Arch / OS / ABI | Class tag | GPU backend | Memory domain | Telemetry class | Offered tags | Python (loop launcher) | Roles |
|---|---|---|---|---|---|---|---|---|
| `sparky` | linux aarch64, sm_121 CUDA | `gb10` | 1x physical NVIDIA GB10, CUDA sm_121, unified | `shared_system` (mem_gb is shared budget; GPU subset cap) | full (power/host/residency/attributed broker) | `gb10` + hostname | fleet python (gb10 env) | worker loops 5 |
| `gx10-6b77` (alias `sparklina`, OS hostname `sparklina`) | linux aarch64, sm_121 CUDA | `gb10` | same as sparky | `shared_system` | full | `gb10`,`sparklina`,`gx10-6b77` + hostname | `/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python` | worker loops 3 |
| `dl380g10` | linux x86_64 | `x86` | none (`has_gpu=false`) | host RAM only | host mem/load only | `x86` + hostname | `/home/rob/venvs/pb-cpu/bin/python` | worker loops 16 + `storage` + `tiers` roles (file server, ARC, stage mint) |
| `wsl-gpu` (alias `DESKTOP-P5UOGNJ`) | linux x86_64 under WSL2 | `x86` (ABI is x86; AMD placement by tags, not class) | 1x AMD RX 9070 XT gfx1201 RDNA4 via ROCm/HIP 7.14 through `/dev/dxg`; **no** amdgpu, no power/clock/throttle, no per-process VRAM | `discrete` 15.81 GiB (`hipDeviceAttributeIntegrated==0`) | `memory_only` (one attributed job, no concurrency probe, no measurement, no enforceable per-scope GPU allowance) | `x86`,`wsl-gpu`,`gfx1201`,`rocm`,`rdna4` + hostname | `/usr/bin/python3` (3.14.4) | worker loops 3 |

What this forbids:

- No AMD/Windows/Mac backend beyond the one measured row. `wsl-gpu` is the
  only non-CUDA GPU in the tree and its limits are structural (no amdgpu,
  WDDM paravirtualisation; Windows-side GPU users invisible to Linux
  census; free VRAM still measured). Native-Linux AMD, Windows native, macOS
  (M5 mini is below the value line, `docs/design.md` live inventory) are
  explicit **unsupported**: unknown backend refuses, never advertises
  fake memory/GPU.
- No `EXPORT_CONTAINER`/scope-invented ports. New hardware needs a measured
  reader (rocminfo+HIP style agreement), a broker snapshot shape, a
  `memory_domain` in `{"shared_system","discrete"}`, and a fleet_boxes entry
  with measured capacity — tags alone implement nothing.
- More GB10 workers need no host listing: placement is class-tag conjunction
  (`gb10`); hostname pins are per-action (`--here`), not fleet config.
- Portable code vs platform-bound measurements: portable (`generic`) actions
  may run on any matching offer; measurement/nonportable actions seal
  `platform_keyed` (pool: submitter platform/toolchain + implicit hostname
  pin) or `host_class_keyed` (SLURM: class + controller attestation).
  A gb10 KL never answers an x86 query. Per-worker results carry producer
  provenance (receipt `producer`, worker core/launcher identity, toolchain,
  platform key); cache hits retain producer revision.
- Shared storage capacity/fill + RAM budgets aggregate; no linear-scaling
  claim without measurement. Stage capacity is minted from the dataset's
  own `available`, ARC-bound prewarm is prefix-fit in entry order, and
  rolling arrival/removal never mutates frozen children (decomposer rule:
  frozen child scope immutable; availability changes placement only).

## 2. Stable FLEET IDs → actual fields (minimal amendment sketch)

The staged-read ledger owns SC/ID/SM/INV/TIER IDs; this lane adds only
`FLEET-*` rows in a **new** ledger file
(`docs/fleet_expansion_requirements_2026-09-20.json`, this lane owns) and
does not edit `staged_read_requirements_2026-09-20.json`.

| FLEET ID | Meaning | Maps to today |
|---|---|---|
| `FLEET-01 registered` | Roster names the box with a loop shape | `fleet_boxes.json boxes[<host>]` + `supervise._box_entry`; startup `_config` refuses unknown host |
| `FLEET-02 qualifying` | Joined but not yet eligible; validation running | broker gate `draining:true` with this join's `changed_unix` + no fresh offer (`workers/<host>.json` absent/stale) |
| `FLEET-03 eligible` | May be placed and may claim | gate `draining:false` + fresh `workers/<host>.json` (age ≤ `OFFER_TIMEOUT_S=120`) + `placeable`/`claim` match |
| `FLEET-04 draining` | Declared absent or resigning; finish-only, no new claims/loops | roster `status in {retired,offline}` (`fleet_roster.ABSENT`) **or** broker gate `draining:true`; supervise `target=0,reserve=0`, worker_loop skips offer+admission after parking |
| `FLEET-05 unavailable` | Not placeable, reason named | expired/missing offer, stale broker snapshot, failed qualification, or `UNREADABLE` census row; never a silent zero |

Qualification before first admission (reuses existing broker qualification;
no new gate without root review): broker containment envelope
(`SystemdBackend.create` exact scope), exact attempt ownership
(pidfd + cgroup identity, inherited claim handle), telemetry freshness
(GPU snapshot ≤5 s, `complete+attributed`, offer ≤120 s), runtime features
(published generation commit, `progress_contracts`, `timeout_ceiling_s`),
toolchain/image (`argv0` digest for nonportable, `container_images`
positive inventory for image-pinned), storage access + read-only staged
mounts (NFS mount, `/stage/prewarm` RO), tier+epoch/reservations (tier loop
mints, mover receipts), topology (physical P-cores preferred, SMT/e-cores
overflow; preserve PB CPU affinity, including in containers).

## 3. Join/resign on the existing lifecycle (explicit, not advisory)

> Superseded in details by §6 (root-reviewed revision, all applied) and §7
> (lifecycle nature). This section remains the original sketch; where it
> disagrees with §6/§7 (e.g. per-call owner nonces, withdraw-before-ack
> ordering, `pending_requeue` reporting), §6/§7 govern.

Worker = host fleet member = all loops on that host. Instance identity =
`(host, supervisor_incarnation)` where incarnation binds
`(supervisor pid, /proc starttime, fresh nonce, changed_unix of its drain)`.
Stale processes never clear a replacement's gate: broker `maintenance_end`
already refuses foreign owners; the CLI mints a distinct owner per
incarnation and records the `changed_unix` it created so only that epoch
may close/open it.

No parallel membership store, no second dispatcher. Desired membership
authority stays the declaration (`fleet_boxes.json status` via
`fleet_roster`); execution fence stays the broker gate
(`MAINTENANCE_GATE` + `changed_unix` + `PARKED_ROOT` exact-loop markers).
The new module is a deterministic driver of those two authorities plus the
existing withdrawal ladder.

### 3.1 State

- Desired (declaration): roster `status: active | retired | offline` with
  `status_reason/by/unix` provenance. `retired` = gone for good, `offline` =
  expected back. Supervisor startup refuses absent; a running supervisor
  drains to zero when the declaration publishes absent. Undeclare + publish
  to resume. Restart never auto-rejoins an absent box.
- Fence (execution): broker gate JSON `{draining, changed_unix, reason?,
  owner?}` at `MAINTENANCE_GATE` (default `/run/prismabuild/maintenance.json`),
  mirrored durably when configured. `changed_unix` names the drain epoch;
  `reason` is descriptive only.
- Acknowledgment: one `PARKED_ROOT/<pid>-<starttime>-<gatekey>` marker per
  loop parked on that `changed_unix`. A loop that cannot write parks anyway
  and the host reads as not-drained: failure keeps the drain closed.
- Claims: existing `ready/claimed/done/failed/withdrawn` + immutable
  withdrawal decisions. Resign drives per-key `withdraw` (operator decision)
  and lets the ladder signal, contain, and file the terminal record; it
  never `rm`s a claim or releases tokens on heartbeat loss.

### 3.2 JOIN (explicit)

```
fleet_membership.py join --reason "..." --owner <incarnation> [--expect-active]
```

1. Refuse if roster declares this host absent (must un-declare + publish
   first; this CLI never edits the roster).
2. Validate, fail-closed, before touching the gate: published generation
   readable, broker `healthy()` + `maintenance_status` readable, one fresh
   `box_capacity.observe` (mem + GPU evidence justified, unknown ≠ zero),
   staged/storage mount readable where declared, loop shape readable via
   `declared_shape`, container inventory available if image-pinned work is
   to be taken. Any failure leaves the gate as-is.
3. `maintenance_end` (owner must match holder or holder unowned; foreign
   holder refuses unless `maintenance_force_end` with evidence — operator
   only, recorded as `forced_end_of/by`).
4. Eligibility follows: loops resume offer publication (`workers/<host>.json`
   fresh) and `claim`/`serve_once` admission. Joining never mutates a frozen
   child scope or another dispatcher: it only opens this host's gate.

### 3.3 RESIGN (explicit, voluntary — distinct from crash/expiry)

```
fleet_membership.py resign --reason "..." --owner <incarnation> [--wait-s N]
fleet_membership.py status [--json]
```

1. `maintenance_begin(reason, owner=incarnation)` mints a new `changed_unix`.
   From the next poll each loop posts its park marker and skips offer +
   admission. New claims stop atomically at the gate; in-flight `serve_once`
   is atomic to its loop and finishes under the generation that claimed it.
2. Wait for exact-loop acks: census live loops, require one marker per loop
   for this `changed_unix` (bounded `--wait-s`; timeout leaves the host
   `draining` with reason, never half-open).
3. Capture resumable committed work: read durable progress counters /
   published units (cumulative, never reset; replays idempotent). Nothing new
   is committed after the gate.
4. Contain/stop owned scopes **before** any release: broker exact-scope
   stop → verify identity → reclaim → verify → `empty` → `release` (the
   `_reap_retired_scope` order). For claimed actions drive the withdrawal
   ladder per key (`withdraw` + signal ladder + terminal record): retry-safe
   rows requeue as new attempts (request immutable, new nonce, budget
   preserved); unknown/non-retry-safe rows file explicit
   `interrupted`/`withdrawn` terminal, never a silent duplicate or a second
   launch. Attempt budget (`max_attempts`, `retry_safe`, withdrawal links)
   is preserved; any policy that refunds/consumes budget is a versioned
   contract change for root, not this CLI.
5. Only then release capacity/cache leases: ledger tokens via the normal
   `finish`/reaper path, tier pins via the tier loop, offers left to expire
   (resigning host unlinks nothing; a stale offer expires at 120 s and
   `placeable` stops considering it). Heartbeat disappearance alone releases
   nothing: crash/expiry keeps claims + reservations until the reaper proves
   owned processes/scopes stopped. Failed containment keeps the host
   `resigning` with held resources + reason; it never reports clean.
6. Stale offer/reservations retired after clear: next fresh `observe` +
   `retire_free_capacity` converges; replacement incarnation joins under a
   new owner/`changed_unix` and stale markers (old gate key) do not count
   toward its acks.

### 3.4 Races (acceptance must cover)

- join-vs-claim: claim boundary re-reads generation + gate; a claim that
  loses the race never executes (generation handshake at poll top and claim
  boundary; gate check before `serve_once`).
- resign-vs-claim: gate closes first; `serve_once` after the gate parks
  instead of claiming; an intervening claim wins at the rename and is then
  withdrawn through the ladder, not killed out-of-band.
- resign with executing loops: in-flight actions finish or are withdrawn per
  key; loops park between polls; supervisor never SIGKILLs mid-action for a
  drain (idle-only stop, later tick).
- replacement incarnation: old pid/starttime/owner cannot park for, end, or
  force-end the new `changed_unix` without an explicit operator force record;
  markers are keyed by `(pid,starttime,gatekey)`.
- retry takeover: new attempt mints new nonce, links immutable withdrawal
  decision, verifies restartability; no double-adopt (`INV-08`).
- failed scope containment: broker verify/reclaim mismatch retains the tombstone
  charge and the resigning state with reason; tokens not freed.
- stale offer/reservations: expired offers read as unavailable; ledger
  `retire_free_capacity` only retires free tokens, never an active action's
  reservation.

### 3.5 Ownership + hook request

- This lane owns: this design doc, `tools/fleet/fleet_membership.py` (new),
  `tests/test_fleet_membership_join_resign.py` (new), and the new
  `docs/fleet_expansion_requirements_2026-09-20.json` ledger (new FLEET IDs).
- It does **not** edit: `src/prismabuild/pool.py`, tier/storage loops,
  residency paths, PQ readers, or the ACC harness.
- Minimal hook needed from the lease worker (via root, not taken here):
  a `PoolQueue.claim(..., gate=None)`-style admission precondition so the
  claim itself refuses when the caller's current gate is draining, closing
  the poll-check→rename window for resign-vs-claim without duplicating the
  claim state machine. Exact hunk through root; until then the gate check
  lives in the loop/CLI layer and the rename race is owned by withdrawal.

## 4. Acceptance plan (thin, real logic)

New test file exercises the actual `PoolQueue` claim/offer/capability path
with fresh controlled offers (tmp queue root, real `announce`/`offers`/
`placeable`/`claim`), plus the gate/park/withdrawal authorities it drives:

- 2 matching workers, same class: both placeable; either may claim.
- CPU-only x86 vs ARM+CUDA: x86 item not placeable on CUDA-only matching
  confusion; CUDA item needs `has_gpu` + class conjunction.
- discrete vs unified domains: `wsl-gpu`-shaped discrete offer vs
  GB10-shaped unified offer; budgets separate (`mem_gb` vs VRAM).
- unsupported backend: unknown arch/backend offer refuses; no fake GPU/mem.
- missing capability / stale offer / drain: missing field ≠ zero; expired
  offer (>120 s) unavailable; draining gate parks admission and offers expire.
- join/resign races: resign-vs-claim (gate closes, claim refused or
  withdrawn), replacement incarnation (old epoch cannot ack/close new gate),
  retry-takeover preserves budget, failed containment stays resigning.

Execution: all candidate tests/compile checks via published
`/mnt/shared/prismabuild-fleet/repo/tools/pbrun.py|pbtest.py` at priority -10,
aggregate reservations, bounded native threads, JSON saved outside the
checkout. No local candidate execution, no new nodes, no capacity inflation.

## 5. Remaining gaps (honest)

- The in-claim gate hook (§3.5) is proposed, not implemented here.
- Cohort (multi-host measurement barrier, #567) stays design-only.
- `wsl-gpu` limits stay: memory-only telemetry, no concurrency/measurement,
  no per-scope GPU enforcement; Windows-side load invisible.
- No Windows-native/macOS/ROCm-native qualification claimed.
- Stage/lease proofs (SM-02/SM-03 gaps in the staged-read ledger) are untouched
  by this lane.

## 6. Revision after root critical review (2026-09-20)

Root reviewed the §3 design and the first `fleet_membership.py` draft and did
**not** accept the API for integration yet. The seven findings below are the
corrective direction; this section records the revised concrete workflow, the
durable authority mapping, and the exact shared-pool hook. No broad new
implementation lands before root approves the hook.

### 6.1 Resign order: withdraw while busy, prove terminals before complete

The first draft waited for all parked acks *before* withdrawing claims. That
deadlocks a busy resign: executing loops never park until their jobs finish,
and their jobs never finish because resign never withdraws them. The revised
order is:

1. `maintenance_begin` under the broker mutex (existing authority). From that
   instant the broker refuses new scope `create`; this is the linearization
   point for "no new work". The loop-gate poll check stays advisory.
2. Census owned claims (bounded; unknown is unknown, never empty).
3. `withdraw` every owned key **while loops are still busy**. The ladder
   signals holders; holders conclude through their own `finish`/reaper path.
4. Wait for, per owned key, an actual terminal record (`done/` or `failed/`,
   attempt identity + terminal reason preserved) **and** broker
   `active_scopes == []` (exact scopes stopped, reclaimed, released; a failed
   containment keeps the host `resigning` with held resources + reason).
5. Only then collect exact-loop park acks for this `changed_unix` and report
   `resigned` with the evidence. Any new claim that won the poll-check race
   in step 1's window must be identified (post-begin census, not one
   pre-census) and contained before SUCCESS — which is what the §6.5 claim
   hook closes deterministically. Until that hook lands, the CLI re-censuses
   after begin and refuses SUCCESS while the race window is unobservable.

`resigned` is never returned after only a withdraw request. Unknown or
non-retry-safe rows file explicit interrupted terminal, never a silent
duplicate. Source/tier lease cleanup on the PB-owned path finishes before
capacity is reported released.

### 6.2 Retry handoff is explicit and budget-preserving (applied)

`queue.withdraw` publishes the immutable decision and drives withdrawal; it
does **not** requeue. The resign driver hands retry-safe rows to successors
through the applied plan-then-publish handoff (§6.5): same budget rule
as admission (attempts increment, never refund), linked by exact
`withdrawn_by` equality. Unknown/non-retry-safe rows file explicit
interrupted terminal, never a silent duplicate. Source/tier lease cleanup
on the PB-owned path finishes before capacity is reported released.

### 6.3 Durable authority mapping

| Fact | Authority | Durability | Reader |
|---|---|---|---|
| Registered hardware shape | `fleet_boxes.json` entry + `fleet_roster.box_status` | versioned file + published generation | supervise `_config` (refuses unknown/absent at startup) |
| Desired membership (transient resign/rejoin) | broker durable maintenance state (`maintenance_state_path`, schema `prismabuild.resource-maintenance.v1`) | root-owned persistent JSON; restart replays it; boot hold (`owner=client-upgrade`) keeps the fence closed until client verification | broker `_restore_maintenance`; CLI never invents a parallel store |
| Execution fence (this epoch) | volatile gate mirror (`MAINTENANCE_GATE`, `changed_unix` names the epoch) | tmpfs; missing/unparsable reads as draining (fail closed) | `worker_loop.read_maintenance_gate` |
| Exact-loop acks | `PARKED_ROOT/<pid>-<starttime>-<gatekey>` markers | tmpfs; missing marker is never a stale absence | resign collector (exact name-set match, not PID-prefix) |
| Worker incarnation | live supervisor pid in `supervise.CLAIM` + `/proc` starttime | host-local; reboot clears it (boot hold applies) | CLI derives owner, never mints random per-call nonces |
| Claim/attempt truth | `claimed/` + leases + immutable withdrawal decisions + terminal records | shared queue | ladder, reaper, `finish` |

A restart therefore never auto-rejoins: the durable maintenance state still
says draining (or the boot hold does), the supervisor re-reads the roster,
and a join is an explicit authorized transition (§6.4). An unregistered host
still refuses join; first-time provision (roster entry + publish + broker +
interpreter + storage) is a documented prerequisite, not something join
performs.

### 6.4 Incarnation, fencing, and the authorized join transition

- Owner = `{host}:supervisor-{pid}:{starttime}` from the live `CLAIM` file.
  Unreadable/absent CLAIM, pid-reuse mismatch on starttime, or
  `--host != socket.gethostname()` all refuse: the CLI operates only on its
  local worker, never on an arbitrary `--host`.
- Every mutating call carries `expected_changed_unix` read immediately
  before: the broker op is compare-and-set against the current epoch, and a
  stale process holding an old expectation refuses before touching the
  broker. `maintenance_begin` already refuses a foreign holder; `end` refuses
  one too — the CLI does not `force_end`, and does not ask operators to copy
  opaque owner strings. A supervisor replacement (new pid/starttime) cannot
  end the old epoch: operator `maintenance_force_end` (recorded as
  `forced_end_of/by`) is the only path, by existing broker authority.
- Join transition: roster active + full §6.6 qualification + expected epoch
  presented + caller incarnation live → `maintenance_end`. A supervisor
  restart takes over an old durable drain by presenting the explicit old
  epoch (broker-enforced CAS) plus a live-supervisor owner — never
  `force_end`. The broker permits this ONLY for its own kind: the held
  owner must be membership-shaped (`supervisor-`) AND verifiably
  dead/replaced (pid gone or starttime mismatch). A live old supervisor, an
  upgrade/operator/unshaped hold, or a stale epoch stays refused even from a
  live root supervisor — liveness grants no license to steal another
  maintenance, and root-owned `force_end` (recorded) remains the only path
  for those.

### 6.5 Shared-pool hook (APPLIED in this lane — pool.py owned here by root grant)

The resign linearization point is the broker mutex for scope creation, but
the queue rename (`_claim`: `_write_claim_intent` + `os.rename(ready,
claimed)`) did not consult any fence. The applied hook re-checks a
caller-supplied `admission_open` fence under the same per-key transition
exclusion that guards the rename. This narrows the poll-check→rename race;
it does NOT lock the broker's gate — a drain can still begin after the
check. A claim that wins then cannot execute (broker `create` refuses under
its mutex; the loop's cleanup path releases it), and the resign proof
accounts for it: repeated census, bracketed park acks around a stable
census/epoch/incarnation, and empty broker scopes before SUCCESS.

Against `e91a55d`, `src/prismabuild/pool.py` (applied):

- `claim(...)` / `_claim(...)` / `serve_once(...)` take keyword-only
  `admission_open: Callable[[], bool] | None = None` (default preserves
  behavior). The `_claim` check sits immediately before the intent write,
  unwinding like the tier-shortage path (ledger abandon, borrow/probe
  return, tier abandon) with a new advisory-only `resign_fenced` denial.
- `worker_loop` (and the `worker.py` one-shot) pass a gate reader, so the
  last check sits milliseconds — not one poll — before the rename.

- Retry handoff (applied): public `PoolQueue.plan_requeue(record)` (pure
  successor constructor + `_preemption_eligible` guard, no side effects).
  The resign driver builds the plan before withdrawing, withdraws, waits
  for the original attempt's exact terminal, then publishes via
  `publish(preempted_claim=..., handoff_by=...)` — publishing after the
  terminal is what keeps a live holder's finish from overwriting the
  successor. The `publish` guard accepts, besides the admission
  `preempted_by` linkage, a resign linkage: the withdrawal decision's
  `withdrawn_by` exactly equal to `handoff_by`. Only the party that
  cancelled can revive; operator cancellations stay unrevivable through it.
  Successors carry `resigned_by` lineage, attempts increment, budget never
  refunds.

The resign linearization point is the broker mutex for scope creation. The
queue rename does not lock that gate: the `_claim` fence check narrows the
poll-check→rename race but a drain can still begin after it. A claim that
wins then cannot execute (broker `create` refuses; loop cleanup releases
it), and the resign proof — repeated census, bracketed park acks around a
stable census/epoch/incarnation, empty scopes — accounts for it.

Against `e91a55d`, `src/prismabuild/pool.py` (all applied):

- `claim(...)` gains keyword-only `admission_open: Callable[[], bool] | None
  = None`, forwarded to `_claim(...)` as `admission_open`.
- In `_claim`, immediately before the `# Intent precedes the claim` comment
  (the `_write_claim_intent(key, owner=owner)` + `os.rename(src, dst`
  sequence, still inside `with self._transition_locked(key,
  blocking=False)`), insert:

```python
                    # Resign fence, re-checked under per-key exclusion: a
                    # maintenance drain that began before the intent write
                    # refuses the claim here rather than racing it. ``None``
                    # preserves current behavior for fenceless callers.
                    if admission_open is not None and not admission_open():
                        if ledger is not None and handle is not None:
                            ledger.abandon_acquire(handle)
                        self._return_borrow(controller, borrow)
                        self._return_gpu_probe(controller, gpu_controller,
                                               gpu_probe)
                        self._abandon_tier_acquire(tier_handles)
                        tier_handles.clear()
                        self.record_denial(item, "resign_fenced", {
                            "action_key": key,
                        })
                        continue
```

  The unwind mirrors the tier-shortage `continue` path directly above it
  (ledger abandon, borrow/probe return, tier abandon); the denial label is
  new and advisory-only (denials never decide placement).

- `serve_once(...)` gains the same keyword and forwards it to `claim`.
  `worker_loop` passes `admission_open` reading the current gate
  (`read_maintenance_gate() is None`) at call time, so the last check sits
  milliseconds — not one poll — before the rename.

- Resign-side broker CAS (applied): `maintenance_begin/end` accept
  `expected_changed_unix`, enforced under the broker mutex — the CLI reads
  the gate only to send its expectation. A supervisor restart takes over an
  old durable drain by presenting the explicit old epoch plus a
  live-supervisor owner the broker verifies itself (`takeover_of/by`
  evidence); stale epochs refuse, dead owners stay refused, never
  `force_end`.

### 6.6 Join qualification (more than broker health)

Join records structured per-check results and refuses on any failure:

1. Roster active with provenance (`fleet_roster.box_status`).
2. Shared-namespace read/write proof: create + fsync + verify + remove a
   probe under the queue root (a mount that cannot be written cannot be
   claimed from).
3. Committed runtime/features: `RUNTIME_VERSION.json` receipt readable;
   worker loop advertises `timeout_ceiling_s` + `progress_contracts`
   (recorded, not re-derived).
4. Resource enforcement + domain: real `box_capacity.observe` against the
   declared shape; GPU-declaring boxes require a fresh trusted broker
   snapshot (`complete + attributed`, ≤5 s), else refuse — tags never stand
   in for evidence, and unknown backends refuse (no fake memory/GPU).
5. Foreign load: `observe` foreign/attributed accounting reviewed; an
   unattributed holder refuses.
6. Images where declared: container inventory positively shows required
   references when the box will take image-pinned work.
7. Eligibility needs BOTH fresh roster-active AND a fresh qualified offer
   (`workers/<host>.json` age ≤120 s). Gate-open without a fresh offer is
   `qualifying`, never `eligible`. `status` reports the five wire states
   `eligible | qualifying | draining | unavailable | unknown` — the earlier
   `qualifying?` placeholder is withdrawn.

### 6.7 Fixtures and hunk tests (this lane, PB-mandatory)

`tests/test_fleet_membership_busy_resign.py` (new, mine) drives real paths
only — real `PoolQueue`, real broker `Authority` with the established stub
backend, real gate files, real `worker_loop` gate readers, real parked
markers:

- busy resign: claimed attempt concludes through the real holder
  `finish`; resign waits for the exact-attempt terminal and empty scopes
  before `resigned` (was meaningfully RED against withdraw-only completion).
- retry takeover: withdrawn retry-safe row requeues automatically with
  preserved budget, exact terminal, `resigned_by` lineage, no duplicate.
- scope containment: `maintenance_begin` refuses new `create` under the
  broker mutex; stop → release empties `active_scopes` (the order resign
  observes).
- fence: closed `admission_open` refuses at the rename, open admits; stale
  broker epochs refuse; live-supervisor takeover of a dead owner's drain
  succeeds with evidence while foreign non-supervisors stay refused;
  local directories fail shared-mount proof; exact-attempt terminals do not
  leak across generations of one key.

Validation runs at `--priority -10` (self-validation behind campaign priority 0)
saved outside the checkout; time fields are tool-derived UTC.

### 6.8 Reader-refs gate (consumer side; writer is the lease worker)


Resign reports `resigned` only with reader refs drained **as well as**
scopes/claims: the completion order is exact terminals → empty broker
scopes → refs drained → bracketed park acks around a stable census/epoch/
incarnation. Attestation writing and ref reclaim are the lease worker's
automatic production path (PR #730); this lane only consumes the proof via
`refs_for_holder` (preferred, when its lane has merged) with a
documented absence rule otherwise: a missing leases namespace is provably
drained, a present-but-unreadable one retains the fence. Refs remaining
their automatic path hasn't reclaimed keep the host `resigning` with the
reason — never reported clean on heartbeat loss, missing reads, or another
host's refs. No membership authority, no global lock, no duplicated
release/telemetry writers: `Authority.handle` scope export and
`ResourceScope` telemetry/release stay lease-worker owned.


- `tools/fleet/fleet_boxes.json`, `tools/fleet/fleet_roster.py`,
  `tools/fleet/supervise.py:377-500,1539-1680`,
  `tools/fleet/worker_loop.py:140-300,1230-1470`,
  `tools/fleet/resource_broker.py:267-380,635-780,766-...`,
  `src/prismabuild/pool.py:announce/offers/placeable/claim/serve_once/withdraw`,
  `src/prismabuild/box_capacity.py:observe/GPU_MEMORY_DOMAINS/freshness`,
  `docs/design.md` (live inventory, sealed env, attestation),
  `docs/operating_prismabuild.md` (placement, demand, measurement),
  `docs/agent_execution_policy.md` (adding workers, telemetry rules),
  `docs/amd_gpu_capacity_2026-09-12.md`,
  `docs/heterogeneous_cohort_design_2026-09-19.md`,
  `docs/staged_read_contract_2026-09-20.md` + ledger + acceptance template.

## 7. What JOIN/RESIGN are (lifecycle command, not a roster edit)


JOIN and RESIGN are supervisor-lifecycle commands executed on the worker
itself (operator or authorized agent, same box only — `--host` for another
box refuses). They are NOT roster edits and NOT a second membership
authority:

- The roster (`fleet_boxes.json` presence) stays the hardware-registration
  and desired-membership declaration. A box the roster declares absent
  refuses JOIN until un-declared and published; first-time provision (roster
  entry + publish + broker + interpreter + storage) is a prerequisite JOIN
  never performs.
- The broker durable maintenance state is what survives restart: a resigned
  (draining) gate replays closed after reboot (plus the boot hold until
  client verification), and a supervisor replacement takes over only its
  own abandoned membership drain — explicit old epoch enforced in the
  broker mutex, old incarnation verified dead/replaced, new incarnation
  verified live. Upgrade/operator drains stay `force_end`-only.
- No always-on mystery loop: the supervisor enforces the declaration every
  tick (absent → drain to zero, reselection of loop shape from the file),
  loops obey the gate every poll (park + skip offer/admission, exact-loop
  markers), and the CLI is the explicit verb that moves the gate after
  qualification (JOIN) or proves completion (RESIGN). Steady state needs no
  manual process.

Proofs already reviewed and kept: exact gate epoch through the broker
mutex (never a client-side comparison), same/current supervisor
incarnation re-derived every resign round, takeover restricted to the
lane's own abandoned drain. Retry handoff preserves the original budget
verbatim — successor `attempts` is exactly prior + 1 with
`attempt_history_missing_before` set, `retry_safe`/`max_attempts` carried,
`resigned_by` lineage beside the immutable withdrawal decision, and the
original attempt's exact terminal (generation + claim identity) proven
before the publish. No READY row is ever claimed while live reader refs
for this host remain (FLEET-13).

Exactness rules applied after review (all in this lane):

- Terminal proof is strictly typed: `published_unix` (non-bool number),
  `claimed_by` (non-empty string), `claimed_unix` (non-bool number) must
  match, and `done/`/`failed/` terminals must carry the actual transition
  counter — exactly snapshot attempts + 1 as integers, never bool. A
  same-number, skipped, missing, or counter-less record never matches.
  Withdrawn terminals require the same typed generation/claim identity.
- Graceful resign retains uninterruptible work: retry-unsafe or
  budget-exhausted rows are never withdrawn by resign — they drain to
  their natural exact terminal with an explicit reason. Destructive
  cancellation is a separate operator `withdraw`, never implicit resign;
  budgets are never reset to enable departure.
- Crash-resume is queue-state, not process memory: handoff intent is
  re-derived every round from authoritative `withdrawn/` decisions
  (membership-shaped `withdrawn_by`, same host, retry budget, no
  successor, generation unconcluded). A new supervisor adopts only rows
  whose prior owner is provably gone. Successor adoption requires exact
  lineage (`supersedes_withdrawal.withdrawn_unix` + `resigned_by`), and a
  newer unrelated publication is preserved with the fence retained.
- Proof census is owned claims UNION handled-but-unclaimed attempts, so a
  concluded attempt never leaves the proof by its claim file moving.
- Broker dead-proof is strict: same-host membership owner, pid absent
  (dead) or starttime mismatch (replaced); permission/read errors and
  foreign hosts answer not-dead and any takeover stays refused.

## References

- `tools/fleet/fleet_boxes.json`, `tools/fleet/fleet_roster.py`,
  `tools/fleet/supervise.py:377-500,1539-1680`,
  `tools/fleet/worker_loop.py:140-300,1230-1470`,
  `tools/fleet/resource_broker.py:267-380,635-780,766-...`,
  `src/prismabuild/pool.py:announce/offers/placeable/claim/serve_once/withdraw`,
  `src/prismabuild/box_capacity.py:observe/GPU_MEMORY_DOMAINS/freshness`,
  `docs/design.md` (live inventory, sealed env, attestation),
  `docs/operating_prismabuild.md` (placement, demand, measurement),
  `docs/agent_execution_policy.md` (adding workers, telemetry rules),
  `docs/amd_gpu_capacity_2026-09-12.md`,
  `docs/heterogeneous_cohort_design_2026-09-19.md`,
  `docs/staged_read_contract_2026-09-20.md` + ledger + acceptance template.

