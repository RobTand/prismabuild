# Physical filesystem floor

The shared guard is `src/prismabuild/filesystem_floor.py`. It extends the
existing `ResourceLedger` and `TierLedger` owners. Enforcement defaults off.
The [accepted #2045 packet](filesystem_identity_allowance_contract_2045.json)
releases source implementation and keeps activation gated. This change supplies
no actual fleet mappings, population certificate, deployment, maintenance,
parking or live native capture authority.

## Admission rule

For each physical backing key and every subordinate capacity constraint:

```text
free_bytes >= ceil(size_bytes / 20) + outstanding_upper_bound_bytes + demand_bytes
free_inodes >= ceil(size_inodes / 20)
```

A zero inode total reports no fixed inode limit. Byte arithmetic uses exact
nonnegative integers. Token kinds `spool_gb`, `stage_gib`, `local_gib` and
`filesystem_gib` use GiB. Memory and bandwidth kinds do not reserve disk bytes.
All whole reservations are charged, including private claims and retained
residence. File size, copy completion and later materialization evidence grant
zero credit. This can charge resident bytes both through measured free space
and through their outstanding allowance. That conservative pressure is accepted;
it is not exact materialization accounting.

## Coverage in the existing binding

Stable filesystem UUIDs and ZFS pool GUIDs remain the physical keys. Registered
destinations retain source, device and descriptor mount incarnation. ZFS
subordinate datasets impose additional capacity checks using `used + available`
and `available`; pool free space cannot override a dataset constraint. Native
mount identification uses `statx(AT_EMPTY_PATH, STATX_MNT_ID)` and mountinfo.
It reads neither `fdinfo` nor the kernel lock table.

Registration still writes the existing binding/member records. It does not
prove all writers are enrolled. An optional `coverage` object in that binding
must come from the designated release authority and contain:

| Field | Meaning |
| --- | --- |
| `version` | Immutable coverage version. |
| `scope_sha256` | `coverage_identity(binding)`, binding the physical key, destinations, owner incarnation and complete members array. |
| `mapping_version`, `population_version`, `bounds_version` | Effective evidence versions. |
| `configuration_version`, `writer_fence` | Configuration and existing-owner fencing evidence. |
| `authority`, `evidence` | Attributed authority and nonempty references to reviewed evidence. |
| `valid_from_unix`, `valid_until_unix` | Finite effective interval. Expiry HOLDs. |
| `bytes_per_token` | Complete map from member identity to an approved whole-growth upper bound per token, at least its native byte unit. |
| `additional_bytes` | Finite upper bound for all other simultaneous growth and retained residue in that population. Missing bounds HOLD. |
| `nfs_aliases` | Optional exact client export mappings, described below. |

The evidence must establish writer closure, enrollment/removal authority,
obsolete-writer fencing and complete concurrent live/private/retry/residue bounds.
Strings in this object are references to that proof, not observations that prove
it. There is no automatic approval or inferred certificate. `floor_status` is
diagnostic, including its unbound-ledger inventory.

`register --coverage-json PATH` consumes this authority-supplied object through
the existing registration command. A registration without it remains useful for
diagnostics and returns HOLD in enforce mode. Changing members, destinations,
configuration, boot or mount invalidates the scope and requires new evidence and
a coherent sample. The source ships no coverage values for the fleet.

An NFS alias must match `server`, exact export `source`, client `machine_id`,
`boot_id`, `mount_namespace`, `device` and `mount_id`. Its containing coverage
binds the authenticated backing filesystem and effective evidence versions.
A server address alone cannot authenticate an export. Ambiguous, absent or
remounted aliases HOLD. Rob's reserved server export-mapping decision remains
unresolved for activation.

## Strict capture and acquisition

`ResourceLedger.floor_snapshot` takes its existing nonblocking mutation lock and
reads fresh, required directories. `TierLedger` delegates under its existing
mint lock. The census includes private handles, committed actions, transfers,
coordinator operation holders and unknown/orphan holders. It checks canonical
regular token names and disjoint minted/free/held/dead custody. Missing,
unreadable, duplicate or malformed custody HOLDs; liveness never erases charge.

Refresh uses two floor sections:

1. Capture binding/coverage identity and the grant counter. Publish an
   incomplete-census HOLD before releasing the floor lock.
2. Snapshot each distinct owner separately under its own lock.
3. Retake the floor lock. Require the same pending refresh ID, so an older
   refresh cannot replace newer HOLD evidence. Recheck identity and counter monotonicity, sample
   capacity, then publish the snapshots plus all grants since step 1 and the
   additional growth bound.

A concurrent grant may be counted twice. It cannot disappear between a transfer
and the sample. Refresh never acquires an owner lock while holding a floor lock.
The order for acquisition stays
`AdmissionGate -> ledger mutation lock -> sorted floor locks`.

`begin_acquire` reads the current binding/sample/counter under floor exclusion.
It checks aggregate charge and proposed demand, then persists counters before
taking tokens. A file or parent-directory fsync failure admits no tokens.
Successful partial precharges remain conservative debt until strict refresh.
Commit and transfer preserve the existing reservation lineage. Cross-filesystem
copies retain their source owner and require destination allowances. Only the
existing release/recovery authority retires custody; token return proves no
physical deletion. Storage power-loss guarantees remain limited by the actual
storage policy, separately from a process-visible fsync result.

## Used paths and callers

Growth reservations use the same authenticated export aliases, and every growth
destination is also a used path.

`check_paths` checks the actual resolved destinations and uses the same aggregate
charge. An unregistered local filesystem HOLDs instead of assuming zero charge.
Bound local paths require both the published verdict and fresh local headroom.
NFS checks require the exact alias and both backing and client constraints.

Supervisor spool offers include configured local disk and log destinations.
The host's own outstanding spool window still prevents an unsupported
materialization inference. Enforce mode also limits a fresh offer by aggregate
headroom already promised through other ledgers on the same backing capacity.

Prewarm checks resolved canonical sources even for a read-only warm with no byte
demand, plus control/log destinations. A refusal defers the row or role before
warming or publishing a prewarm receipt. Movement checks actual sources and
created destinations before copy or adoption, including existing final names and
resumed partials. It propagates worker-thread floor refusals to the caller;
command entrypoints return 75 before publishing a successful move receipt.
Local-resident copy checks stage sources while their reader pin is held.

Coordinator `operation()` and submission `write_window` keep their existing
ownership and release ordering. Package preparation already uses that window.
CAS-only operation custody does not automatically cover every action destination;
coverage must supply complete bounds or HOLD. The separate #2092-#2095 interfaces
remain scoped dependencies, not implemented outcomes claimed by this guard.

## Modes and acceptance

- `off` preserves existing acquisition and caller behavior.
- `observe` calculates and reports refusals while permitting work.
- `enforce` turns unknown evidence or insufficient aggregate headroom into the
  existing shortage/defer paths.

Mode caches remain bounded and are not proof of obsolete-writer fencing. Every
participating owner must share the admitted mutation/grant protocol before
activation. Missing strict-population or mapping evidence is still owed work.

The #1483 merge record names actual fleet actions, logs and receipts. A source
merge is distinct from runtime publication, activation and workload acceptance.
No scoped unit test, documentation packet or issue condensation closes those
remaining gates. The live native diagnostic needs Rob's separate authority.
