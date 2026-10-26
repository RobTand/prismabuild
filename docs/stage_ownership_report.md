# Read-only stage ownership evidence (#1973)

Use the existing command with configured pool, tier and stage inputs:

```sh
python tools/fleet/stage_reclaim.py --report \
  --pool-root "$POOL_ROOT" --tier-id "$TIER_ID" --stage-root "$STAGE_ROOT" \
  --cas-root "$CAS_ROOT" --max-files 100000 \
  --max-metadata-bytes 67108864 --max-seconds 30
```

`--cas-root` and `--residency-root` are optional overrides. Without a CAS
root, a record's own CAS root can still resolve its manifests; otherwise missing
layouts remain unresolved. Unlike the default digest-proof reclaim dry run,
reporting needs no originals, mount prefix, quarantine or action key. An action
key inherited from the launcher is ignored. `--report --apply` and
`--report --restore` are errors. JSON goes to stdout, not to a queue record or
`--receipt` file. This mode never files receipts, retires declarations, changes
holders or leases, opens payload contents, or hashes payloads. Metadata/CAS
manifest validation is separate from payload hashing.

The Python interface is
`ownership_report(queue, *, tier_id, stage_root, cas_root=None,
residency_root=None, max_files=100000, max_metadata_bytes=64 << 20,
max_seconds=30)` in `tools/fleet/stage_reclaim.py`.

## Reading the result

- `directories`: every discovered top-level directory, including empty and
  mixed-owner directories. A synthetic `.` row accounts for root-level regular
  files. The stage-root control marker is explicitly excluded, not payload.
  Each row includes `name`, `path`, `observed`, `physical_bytes`, `declarations`,
  `fragments`, `movements` (active/retired), `plans`, `holders`, `readers`, exact
  `references`, `potentially_stale` and `complete`.
- `totals`: the same observed and accounting quantities summed over rows.
- `references`: exact record paths/documents and W1 resolved manifest
  digests/paths. Direct residency-tree path provenance is retained too.
  A directory can have several consumers and movers; no single-owner guess is
  made. In report mode, manifest paths use their declaration's explicit
  `stage_root` (inherited by nested declarations unless overridden), not the
  selected stage root. Outside destinations stay outside and their paths are
  explicitly excluded from directory ownership joins. A missing or invalid
  destination leaves attribution unknown; neither source-stage/RAM hints nor a
  matching tier proves destination ownership. Strict reclaim retains W1's
  conservative selected-stage protection set, which is not affirmative report
  ownership. Holder entries contain token paths and per-resource `holdings`.
- `unresolved_manifests`: originating record, digest, uncertainty `scope` and
  diagnostic reason. Only an input mover's own destination contract bounds
  its missing-layout uncertainty. Source-stage/RAM hints cannot narrow it.
- `errors`, `excluded`: unreadable/incomplete evidence and explicit symlink,
  outside-declaration, control-metadata and non-regular exclusions.
- `scan`: configured bounds, observed regular-file count, actual metadata
  content bytes read, elapsed time and exhausted budgets. Unknown unscanned
  size is `null`, **not measured zero**.
- `complete`, `read_only: true`, `deletion_authorized: false`: neither a complete
  result nor any individual classification is deletion authority.

### Exclusive observations versus overlapping declarations

`observed.apparent_bytes` sums regular-file **lstat** sizes; its basis is
`regular_file_lstat`. `observed.allocated_bytes` sums `st_blocks * 512` when
available. Neither is filesystem-wide unique storage: hardlinks can be counted
more than once, reflinks/compression/snapshots have filesystem-specific sharing,
and allocated blocks are not a prediction of space recoverable by deleting a
path. Symlink payloads, directories and cycles are not followed.

The mutually exclusive **apparent-byte** `physical_bytes` partitions reconcile
exactly to observed apparent bytes:

- `owned`: positive fragment, direct/manifest reference or reader protection
  evidence. Known positives survive other incomplete attribution.
- `source_only`: a source mark, no established ownership, and sufficiently
  complete attribution. This **does not mean reclaimable**.
- `unattributed`: no established ownership or source mark after sufficiently
  complete attribution. Still not deletion permission.
- `unknown`: attribution cannot establish absence because of unreadable records,
  unresolved manifest coverage, or metadata/time exhaustion. Unknown never
  silently becomes source-only or positively unattributed.

`declarations.fragment_bytes`, `source_mark_bytes` and
`source_mark_fragment_overlap_bytes` are separate, **overlapping** quantities.
Fragment bytes sum declarations, including multiple declarations of the same
path and declarations whose payload has disappeared. Source-mark and overlap
bytes count observed files, with fragment overlap counted once per observed
path. For example 31 observed bytes can include 24 marked bytes, 11 bytes both
marked and fragment-owned, and two declarations of that same 11-byte file
(22 declared fragment bytes). Adding these populations would double count.

A fragment with no observed live queue consumer remains owned and can be
`potentially_stale`. This is a review lead, not lifecycle proof. An incomplete
queue census also cannot prove a consumer finished. Reader entries are durable
protection evidence, including orphan pins surviving fragment withdrawal;
their holder host/PID is **not proof of process liveness**.

## Bounded, partial and concurrent evidence

The defaults are 100,000 payload-file observations, 64 MiB of metadata content
and 30 seconds. Budgets must be finite/nonnegative; count/byte budgets are
integers. Bounds are enforced during metadata reads and streaming traversal,
not by truncating an already unbounded census. Metadata documents are cached
only for this invocation. Time checks are cooperative between filesystem
operations: a blocked syscall cannot be preempted by this report. Final
accounting/JSON serialization of evidence already collected also costs time.
A zero-second budget performs no payload observations or metadata-content reads.

Top-level rows are discovered before the payload-file budget is spent. File
exhaustion preserves those rows and partial measured bytes. Unreadable payload
directories, metadata exhaustion and missing layouts return useful incomplete
evidence with diagnostics rather than pretending to have an empty read set.

This is a **non-atomic concurrent scan**, without ownership locks or lifecycle
mutations. Observations, tokens and declarations can change between reads;
path/component checks do not constitute a transaction against concurrent
renames or filesystem replacement. Never use this report as a security boundary
or a reclaim authorization. Do not add its numbers to an incident measurement
as though both were measured together. The issue's 271.4-GiB figure remains
attributed incident evidence, not a measurement made by implementing this tool.

The implementation factors W1's provenance census and injects bounded I/O into
the existing strict fragment and reader censuses. A pure evidence join reuses
`stage_release.live_claims` without another queue scanner; tier holder paths
come from `queue.tier(tier_id)`. Each census is reused across all directory rows
within one invocation. Default strict reclaim/restore readers keep their
fail-closed behavior. There is no recurring scanner, scheduler, declaration
retirement, reclaim execution, quota mechanism or broad GC investigation here.
