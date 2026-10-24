# The used-filesystem floor (#1483), v2

Status: source contract, **enforcement default-off**. Historical CEO decision
D1 described an interim Netdata disk check; this document does not establish
the current deployed runtime or population. The existing v2 guard supersedes
PR #1490's proposed architecture; its review
(`pull/1490#issuecomment-5982325705`) is historical intake, not current closure.

## #2045 prerequisite review packet

The [existing-owner identity and allowance contract](filesystem_identity_allowance_contract_2045.json)
is a parent-review packet for #1483, not a runtime registry, ledger or second
validator. It reconciles existing source capabilities and destination variants,
conservative accounting obligations, and exact existing suppliers/unblock
conditions. Current authenticated mappings, closed allowance populations,
complete deployment growth bounds and explicit parent acceptance of whole
reservation bounds were not supplied. Dependent repair waits; no deployment,
activation or physical Rob action is authorized. Structure tests do not prove
these facts, accounting semantics or review approval.

The coverage and audit notes below describe source capabilities and historical
limits, not an all-writers certificate. Binding members and tolerant
`held()`/`held_keys()` observations do not close the population; `floor_status`
discovery is diagnostic. NFS server-address association is conservative routing,
not authenticated export-to-backing identity. Descriptor-based identification
is a snapshot, not a retained pin for later pathname writes. Unknown mappings
or allowances require HOLD in the parent contract, without changing current
production behavior here. Historical bootstrap/proof examples below are not
current deployment instructions or sufficient #1483 acceptance evidence.

## The rule

For every physical filesystem PrismaBuild writes:

    free_bytes >= ceil(size_bytes / 20) + charge_bytes (+ demand_bytes)
    free_inodes >= ceil(size_inodes / 20)

`charge_bytes` is every byte allowance PrismaBuild has granted on that
filesystem and not yet taken back: the held tokens of the byte kinds
`spool_gb`, `stage_gib` and `filesystem_gib` (all GiB) in every ledger bound
to the filesystem.  A held token is charged whole even after its bytes are
written (no materialization credit), so the charge is an upper bound on the
bytes still to come.  The module is `src/prismabuild/filesystem_floor.py`.

Inodes are sampled from `statvfs` `f_files` and `f_favail` (#1535), including
on ZFS where byte capacity still comes from the pool dataset. A zero total
reports no fixed inode limit. The shared disk-room helper in
`filesystem_capacity.py` owns the five percent inode predicate; published
samples carry its counters and refusal. A byte-only sample is
`inode_sample_missing` until the next owner refresh, never evidence of
inode headroom. No inode demand estimate or reservation is implied: this
is a sampled floor, not protection against later exhaustion.

ZFS refresh keeps byte accounting on the pool and samples inode availability
without comparing a stored dataset device number. A device-number change
within that pool does not add a capacity refusal; the pre-existing device
comparison remains limited to non-ZFS sampling.

## Bindings and stable identity

A ledger kind is *bound* to the filesystem its bytes land on by
`python -m prismabuild.filesystem_floor register ROOT --member <ledger-root>/<name>:<kind>`,
run on the host where `ROOT` is local.  The binding is keyed by a stable
identity, never by boot or mount id:

| filesystem | key | sample |
|---|---|---|
| block device (ext4, xfs, btrfs) | `uuid:<filesystem UUID>` from `/dev/disk/by-uuid`, matched on the mount's source device (btrfs's `st_dev` is anonymous) | `fstatvfs`: `f_blocks`, `f_bavail` |
| ZFS | `zfs:<pool guid>` | the pool root dataset's `used + available` and `available` (usable space; `zpool list` free includes raidz parity and slop) |
| NFS | none (a client cannot bind) | the client's fresh `fstatvfs` (a server round trip) |
| anything else (tmpfs, overlay) | `volatile:<machine>:<boot>:...` | `fstatvfs`; cannot be registered |

A reboot, remount or new mount namespace of the same disk keeps its binding.
A bound root that now names a different stable key marks the binding
`moved`, refusing its acquisitions until an operator registers again; no
drain is ever required.

Records live under `<queue>/filesystem-floor/`:

    mode                        off | observe | enforce (absent = off)
    fs/<digest(key)>/binding.json   key, root, owner host and machine, size, server addresses, members
    fs/<digest(key)>/sample.json    census, then sample, and the grant counter at that moment
    fs/<digest(key)>/granted.json   monotone count of bytes granted under the floor lock
    fs/<digest(key)>/.floor.lock    the floor lock
    members/<digest(member)>.json   reverse map ledger kind -> key

## Refresh: census, then sample

The owning host's worker loop and tier loop call `loop_tick` every poll
(throttled to 30 s).  For each binding this machine owns it takes the floor
lock, reads the grant counter, counts every member's held tokens (two
passes, names united, so a token moving between holders is not lost), then
samples the filesystem, and publishes all three.  Census-before-sample under
the lock is what makes the published triple sound: a byte written after the
sample belongs to a token held at the census or to a grant made after it,
and every later grant is in the counter.  Refresh runs in every mode, so
`status` shows real samples before anyone enforces.

## Admission of byte acquisitions

Every byte-kind acquisition already funnels through
`ResourceLedger.begin_acquire`, so that is where the gate sits; no caller is
converted.  The four sites #1490 missed (`take_tier_advance`, the
`_reserve_fence_locked` top-up, `produced_output.refill_window` and
`commit_batch`), the claim path and movers are all covered by construction.

1. Before `begin_acquire` takes the ledger's mutation lock: take the mode
   and the member bindings from this process's cache (120 s), which the
   loop tick refreshes outside every lock each poll.  A caller that already
   holds the ledger lock -- the claim path does -- therefore reads no shared
   record here either, except once on a cold cache.  `off` reads only the
   cached mode and then runs exactly the old locked acquisition.
2. Inside the ledger's mutation lock: take the floor lock of each bound
   filesystem, sorted, each waited for up to 5 s.  Read `sample.json` and
   `granted.json`.  Charge `census + (granted_now - granted_at_sample) +
   demand`.  The sample must be at most 300 s old (30 s of future skew is
   accepted).
3. Refused: return `None` with `last_token_shortage = {"resource":
   "filesystem_floor", "reason": ..., "filesystem": ...}` -- the ledger's
   ordinary shortage, which every caller already handles (a claim records
   its denial and moves on; a tier take reports `tier-short`; produced
   output keeps its typed result).  Allowed: take the tokens, then advance
   the grant counter, still under the floor lock.

Under `observe` the same verdicts are computed and refusals logged as
`[filesystem-floor] would refuse ...`; nothing is refused.  Under `enforce` an
unbound byte ledger, a missing or stale sample, a moved binding, a busy floor
lock past its wait, or an unreadable record refuses.  A down owner therefore
refuses only acquisitions on its own filesystems.

### Lock order

`[AdmissionGate] -> ledger mutation lock (host .mutation.lock or tier mint lock) -> floor locks (sorted)`.
A floor section acquires nothing else, and the refresh holds only the floor
lock, so floor locks are innermost everywhere and cannot close a cycle.
Main's #1486 contract holds: no token mutator takes another ledger's lock,
no name is resolved and no network record is read under the ledger lock
except the two small floor files, and no `statvfs` or subprocess runs under
it.  Release, commit, abandon, transfer and sweep are unchanged: they only
lower the charge and take no floor lock.  The review suggested taking the
filesystem locks *before* the ledger locks; that order is impossible to keep
because main's callers legitimately hold a ledger lock across
`begin_acquire`, and innermost is the order every path can honour.

## Used paths

`check_paths` checks filesystems that are written without an allowance of
their own: the queue, the CAS, checkouts, logs, the submitter's tree.

- Local, bound: the published verdict, and the same charge against a fresh
  local sample.
- Local, unbound: a fresh sample, zero charge.
- NFS: matched by server address to **every** binding that server
  registered (no `/proc/net/rpc/nfsd.fh` read, no file-handle parsing).  Each
  binding must pass its published check, and the client's fresh sample must
  clear the largest of their floors plus the sum of their charges.
  Over-charging replaces export deduplication.  A server with no binding is
  `unattributed_nfs` and refuses under `enforce`.

The worker loop checks `[queue, cas, local checkout root]` each poll; under
`enforce` a refusal skips that poll's admission (nothing claimed, nothing
failed), exactly like an unpublished offer.

## Coordinator growth: `filesystem_gib`

`register ROOT --filesystem-gib N --filesystem-ledger NAME` mints a growth
ledger `<queue>/filesystem-reservations/NAME` bound to the filesystem.
`filesystem_floor.operation()` (used by `pbrun`) writes an owner record
(host, machine, boot, pid, pid start time, lease) *before* acquiring, holds
the tokens for the body, and releases in `finally` -- `SystemExit`, ^C and
refusals included.  Any error in the operation itself (an unreadable
binding, an owner record that will not write) is a verdict: logged under
`observe`, refused under `enforce`, never a traceback.  `reap_operations` (every loop tick, and `reap`) releases
a holder whose owner on this machine is gone (boot, pid or start time
differs), and any holder whose lease (1 h) has expired.  `pbrun` holds
`--filesystem-growth-gib` (default 1) on the shared store's ledger while it
snapshots, writes CAS objects and the queue record, and releases before an
attached pool or `--after` wait.  The SLURM lane submits and waits in one
call, so it holds no growth allowance (its used paths are still checked).

## Bootstrap order

1. Publish a runtime containing this change.  Mode is `off`; nothing changes.
2. Register every byte ledger on its owning host (through PB actions pinned
   to that host; the CLI is `python -m prismabuild.filesystem_floor`):
   - dl380g10: `register /storage_pool/shared --filesystem-gib <N> --filesystem-ledger storage-pool`
   - dl380g10: `register <prismabuild-stage mountpoint> --member tier-reservations/prismabuild-stage:dl380g10:stage_gib`
   - sparklina: `register <spool root> --member reservations/sparklina:spool_gb`
   - any other box that later offers `--spool-gb`.
3. `status`: every binding `active` with an allowed verdict, and
   `unbound_byte_ledgers` empty.  The loops refresh from now on; until they
   run this generation, `refresh` on the owner does it by hand.
4. `mode observe` for at least a day; read the loop logs for `would refuse`.
5. The end-to-end CPU proof below.
6. `mode enforce`.  Rollback is `mode off` (or `PRISMABUILD_FILESYSTEM_FLOOR=off`
   for one process); no restart is needed either way.

**Upgrade every owner host before step 6 (#1542, item 5).** A sample carries
the inode counters (`size_inodes`, `free_inodes`, `floor_inodes`,
`inode_refusal`) only when its owner host's loop runs a generation containing
the inode floor (#1540).  `published_verdict` refuses a sample without them as
`inode_sample_missing`, so under `enforce`, while any binding's owner host
(`owner_host` in `fs/<digest(key)>/binding.json`, shown as `owner` by
`status`) still runs an older generation, every byte acquisition on that
filesystem is refused until that host upgrades; under `observe` it only prints
`would refuse`.  Before `mode enforce`, run `status` and confirm that no
binding's `verdict` reads `inode_sample_missing`.  If one does, upgrade that
owner host (or stay in `observe`); do not switch `mode` for the whole fleet to
`off` to get past it.

## End-to-end CPU proof (gate for `enforce`)

All through PrismaBuild CPU actions on the real fleet:

1. **Private-registry proof, already runnable on any generation containing
   this source:** `tests/test_filesystem_floor_1483.py` on dl380g10 (btrfs
   home, ZFS shared store) and sparky (ext4, NFS client): register, refresh,
   admit, refuse at the exact boundary, release, reap a SIGKILLed owner, NFS
   attribution by server address.  Receipts in the PR.
2. **Live registry, after steps 1-4 above:** from celestia, submit one CPU
   action with `PRISMABUILD_FILESYSTEM_FLOOR=enforce pbrun --cwd <tree>
   --filesystem-growth-gib 1 -- /home/rob/venvs/pb-cpu/bin/python -m
   prismabuild.filesystem_floor --queue /mnt/shared/prismabuild-fleet/pb-queue
   check /mnt/shared/prismabuild-fleet/cas <checkout root>`.  This exercises,
   in one run: the dl380g10 registration and its loop's refresh (published
   sample fresh), the coordinator's used-path check over NFS by server
   address, a real `filesystem_gib` admission against the live shared-pool
   sample, release before the wait, and a worker-side check that prints its
   verdicts.  Pass = rc 0, every verdict `allowed`, `storage-pool` holding no
   `operation-*` tokens afterwards, and no `would refuse` lines in the
   observe-mode loop logs for the preceding day.
3. Then `mode enforce`, and watch one claim cycle per box.

## Known limits (stated, not hidden)

- **Residency pins are charged twice.**  A held `stage_gib` pin for bytes
  already on the stage pool lowers `free` *and* counts in the charge.  Before
  enforcing, compare `status`'s stage charge with the pool's free space; if
  pins are large the stage floor will refuse new grants early.  A
  materialization credit is the follow-up, not a precondition.
- Worker CAS writes do not have a demonstrated complete per-action growth
  bound here. A per-poll used-path check is not a reservation for intervening
  writes; CAS/result staging remains a separate #2093 interface.
- `submission.preparation` already enters `submission.write_window` around
  package preparation and child sealing, including campaign preparation.
  The window checks cwd, CAS, queue and temporary storage, but pool growth
  is reserved against the CAS filesystem only. Do not describe campaign
  preparation as wholly unguarded or infer coverage of later publications,
  every destination's growth, or normal action claim custody from that window.
- Supervisor worker logs, role logs and queue attempt logs have distinct
  owners and destinations. Supervisor `LOG_DIR` is not explicitly in the
  worker floor-path list. Inactive worker-log cleanup and role-log trimming
  are not complete active-log bounds; #2094/#2095 retain attempt-log and
  SLURM diagnostic custody work.
- `prewarm_loop`'s reader is read-only; its Admission/HoldLedger/DiskPacer
  bound speculative reads, memory and pacing, not persistent storage.
  Queue/event/control writes, stdout and optional `--log` still need coverage.
- Tier descriptors, registered stage-root deletion authority and returned
  tokens do not prove physical mappings or removal of partial/retry copies.
  Stage residence can outlive movers. Produced-spool copy completion alone
  does not authorize unlink or charge return: preserve ACK, origin commit,
  source release and retirement authority and source/destination coexistence.
- The two-pass census can miss a token that moves twice during it (a commit
  immediately followed by a transfer); the next refresh counts it.
- A lost grant-counter write (logged) under-charges until the next refresh.
- A grant stays charged after its tokens are released, until the owner's
  next census (at most 30 s): released bytes are counted twice briefly, never
  zero times.
- Observed 2026-10-04: dl380g10's btrfs `/home` (where PB tests and local
  checkouts land) had about 11.8 GB free of 137 GB, roughly 5 GB above its
  floor.  Any `enforce` there will refuse growth early; free space before
  enforcing.
- The census lists ledger directories without their locks.  For a ledger on
  the NFS store that *another* host mutates, the owner's directory cache can
  briefly hide that host's newest tokens while the grant counter already
  includes them.  The fleet's bindings avoid this: dl380g10 lists its own
  ledgers on local ZFS, and sparklina's spool ledger is mutated by sparklina.
  A new binding of a ledger mutated from other NFS clients should be
  reviewed for it.
- A process holds its cached mode for up to 120 s (a looping one refreshes
  it every poll), so for that long after `mode enforce` a process that last
  read `off` grants without counting.  The next refresh's census counts
  those tokens.
- `local_scratch.scratch_terms` sums separately rounded positive ROOT/MAX
  declarations into host `spool_gb`. These are reservations, not quotas or
  per-filesystem routing. A measured offer does not prove that scratch,
  spool, checkout and logs share its filesystem or establish freshness just
  by recording a timestamp/PID. Optional `scratch_lifetime` evidence owns
  exact-attempt cleanup, not producer-byte enforcement. Complete growth and
  mapping obligations remain unresolved; preserve existing positive bounds.
- `observe` takes the floor locks exactly as `enforce` does (that is what it
  measures), so it can add up to the 5 s lock wait to a byte acquisition
  under contention; it never refuses.

## #2045 packet verification

Command (through the admitted test runner on both arms):

```sh
python -m pytest tests/test_filesystem_identity_allowance_contract_2045.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Final results: x86/Python 3.14 **112 passed**; ARM/Python 3.12 **112 passed**.
An initial x86 run reported 1 failed, 111 passed because the packet cited a
nonexistent `disk_pacing.py`; corrected the reference to the actual
`prewarm_loop.py` owner before these final runs. These checks validate minimum
packet structure and repository guards, not deployment facts, closed populations,
growth bounds, parent acceptance or operational qualification. No live mapping,
population or deployment qualification was performed or claimed.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d7d2118b847b3b6c47bee83377ed5fee70c311d9c8c4ca84cfdeb6f6681d9708","size":2039}},"operation":"5895fd0dcd001b838dbfbb2608560935c7524814f5435cfe6c6722a7656af191","receipt":"cffb419cab409ff882aa1b457a8570222a59041d553803f8d51508211e966ff8"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"18b83e5c547f26b212b8cd889f99ec057c34e5bb695c3aab857e222aff1e489d","size":2043}},"operation":"f62772b900164a912348dd6cb1d92bb6b87ab0556979e24d44d4764ff51b6a65","receipt":"68188e0d897661186f228f1445bc490fb0061bd52c802434a1de6c8f42ab18d9"}]

## Review findings (#1490) and where each is answered

| # | finding | v2 |
|---|---|---|
| 1 | cross-ledger locks / NFS reads in token mutators | gate reads records before the ledger lock; floor locks innermost; mutators untouched |
| 2 | unconditional enforcement | `off` by default, fleet mode file, per-process env override |
| 3 | lifecycle unwired; 120 s fleet-wide freshness; boot-id identity; circular capture | `register`/`refresh`/`status` CLI; loop ticks; stable keys; freshness only for the filesystems an acquisition touches; registration is a CLI, not a guarded PB capture |
| 4 | P allowances leak | release in `finally`; owner record before acquire; reaper by pid/boot/lease |
| 5 | non-blocking locks become failures | bounded waits; refusals are shortages, never exceptions |
| 6 | storage role admitted with worker terms | no role terms: movers' bytes are their `stage_gib` grants, gated at acquisition |
| 7 | four unconverted acquisition sites | the gate is in `begin_acquire`; tests drive `take_tier_advance` and `refill_window` |
| 8 | `/usr/bin/python3` symlink ELOOP | paths resolved; any identity error is a verdict, never an exception |
| 9 | ZFS raw pool free | root dataset `used + available` |
| 10 | tests fake the guard | real filesystems, ledgers, locks, SIGKILL, ZFS and NFS |
| 11 | Closes vs Refs | Refs #1483 |
