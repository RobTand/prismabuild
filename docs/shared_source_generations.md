# Shared source generations: current operational contract

This describes the implemented host-local source lifecycle, not a proposal or
an acceptance result. It is grounded in `shared_sources.py`, `materialize.py`,
`_pbtest_layout.py`, `core.py`, `pool.py`, and `tools/fleet/pbtest.py`.
No test execution or performance result is asserted here.

The broader staged-read obligations remain in the
[contract](staged_read_contract_2026-09-20.md),
[requirement ledger](staged_read_requirements_2026-09-20.json), and
[acceptance record template](staged_read_acceptance_record_template_2026-09-20.md).
This implementation does not by itself establish every staged-read requirement.

## Admission and fallback

Shared source is **opt-in**, through `pbtest --source-readonly`. It is currently
for compatible, unprofiled, host-executed pbtest shards on the **pool** transport:

- The generated action is a sealed `fleet/pbrun` shard with a checkout snapshot,
  repository-root working directory (`.`), and the supported pbtest program and
  result-capture recipe.
- The resealed action names `params.execution_layout =
  "pbtest-source-readonly-v1"`. Its program and capture recipe change, so its
  action key differs from the private-layout action. The snapshot, code closure,
  and logical result declaration are retained.
- The pool checks the sealed request, snapshot agreement, standard worker script,
  layout, and recipe markers before choosing shared materialization. A queue
  field or launch-environment variable alone is not authority to opt in.
- Worker-supplied result/helper/cache/surface path variables are reserved; callers
  must not seal those worker paths into the action environment.

Without the flag, execution keeps its existing private-checkout path. SLURM,
profiling (including plans selecting a profile), containers, explicit scratch or
basetemp selections, absolute/parent-traversing surface destinations, and
incompatible prepared actions do not get the new recipe: pbtest retains the
private recipe and reports the unsupported fallback. Tags, replay, a matching snapshot, or an
existing warm directory do not independently enable sharing. Legacy/custom
worker and existing-root execution are not general shared-source interfaces.

This **admission fallback is not a retry policy**. Once an explicit shared action
has been admitted, a source-proof, source-census, worker-layout, or result-path
refusal is not permission to rerun it privately. In particular, a worker that
cannot adopt exactly the pool-selected generation refuses rather than silently
changing execution semantics.

## Location and source identity

`PRISMABUILD_LOCAL_CHECKOUT_ROOT` is the existing deployment configuration for
host-local materialization. Configure the root on the worker; it must be an
absolute, non-root path without `..`. The pool forwards its configured root to
the worker interpreter. The physical root is not a portable action parameter.
It must be a trusted host store, on a filesystem providing the inode, timestamp,
atomic-publication, and `flock` semantics this implementation uses.

The layout below that root is separate from private execution checkouts:

```text
<local checkout root>/
  shared-sources-v1/
    <source key>/
      mutex
      current.json
      g-<unique generation>/checkout/    # repository, including .git
      g-<unique generation>.readers
      g-<unique generation>.idle.json    # present only when idle is recorded
  .execution/
    pbtest-<unique attempt>/             # writable state, not source
```

A source key hashes the materializer version, the **whole normalized snapshot**,
and the CAS trust context: resolved CAS root plus its device and inode. Thus
snapshot input/bundle identity, commit, subdirectory, and, for v2 snapshots,
parent and named refs all participate. Equal commits alone do not justify
reuse. Another CAS root or replacement CAS-root inode is a different context.
Even warm acquisitions verify the CAS snapshot input; a missing or tampered
bundle is not excused by a cached checkout.

### Build, prove, publish

Under the source-key mutex, a cold acquisition creates a unique, initially
unadvertised generation. The shared materializer uses the same population code
as private materialization: initialize a repository, verify bundle/ref agreement,
fetch the sealed history, check out the sealed commit detached, validate the
subdirectory, and refuse materialized symlinks resolving outside the repository.
Contained dangling links remain acceptable; unresolvable/escaping links do not.

The complete repository is then frozen: regular files lose write bits while
retaining executable bits, and directories become read-only. Symlinks are not
followed by freezing or the later census; special inode types are refused.
Core proves the clean checkout/commit and the snapshot's required ancestry and
refs **once for that generation**, before publication. A single atomic
`current.json` publication names the stable generation path and a digest of its
inode census. The generation is not moved after the proof, preserving its root
binding. Failed or killed builders do not advertise a partial tree; abandoned,
unadvertised generations can subsequently be reaped.

### Warm adoption and continuing checks

The census walks the **entire repository, including `.git` and ignored paths**.
For every entry it records relative name, device/inode, mode, link count,
owner/group, size, `mtime_ns`, and `ctime_ns`. Atime is deliberately excluded.
Write bits on nonsymlinks, a new/missing/replaced entry, changed metadata, or an
unreadable census refuses reuse. In particular, restoring bytes and mtime after
a rewrite does not restore the old ctime identity.

The full census and private core proof are cached in-process. Across worker
interpreters, `current.json` persists the **digest of that census**, not a
serialized proof or a caller-supplied `verified` flag. A new interpreter
recomputes the census, compares the published digest, and internally reconstructs
the snapshot/root-bound capability. It does not repeat the generation's full
Git identity/ancestry proof. Trust in this adoption rests on the materializer's
own host-store publication after the initial core proof.

Each live lease checks its census before and after generation-aware preflight.
Core still binds the proof to the action snapshot and root, verifies the action's
code closure, and performs the other ordinary preflight checks. Worker execution
checks source after execution, at CAS precommit, and on layout exit; cache hits
also pass live-source/action validation. Warm reuse is therefore not a claim of
zero I/O: census walks, CAS input verification, and action-specific validation
still occur.

An uncertain generation is never accepted because its bytes later look right.
A detected census failure invalidates that process's state. Failed adoption
unpublishes the current generation and refuses that acquisition; a later
acquisition can build a new one. Existing leases keep their own reader holds
and fail their own gates if the old source changed. Malformed publication or
other store failures may require operator investigation, not automatic repair.

## Writable state belongs to an attempt

Sharing is of sealed source, **not an execution sandbox or shared writable
workspace**. The execution cwd and code closure remain rooted in the selected
source checkout. Each admitted shard receives a fresh private attempt below
`.execution`, with the following paths:

| State | Attempt-local destination |
| --- | --- |
| Declared result | `pbtest-layout/results/<logical result path>` |
| Roster/helper files | `pbtest-layout/helpers/` |
| pytest cache | `pbtest-layout/pytest-cache/` |
| Surface JSON outputs | `pbtest-layout/surfaces/` |
| Bytecode prefix | `pbtest-layout/bytecode/` |
| Temporary files (`TMPDIR`) | the attempt directory |

The worker validates private, same-owner, nonsymlinked destinations outside
source, plus the expected environment/path relationships. The result must be
absent before launch, with normal result containment and symlink-prefix checks.
Core's output locking, recovery claims, result reading, and publication use the
attempt's result root, not the source checkout. The capture program consumes a
worker-supplied physical result path while the sealed logical declaration stays
unchanged.

The generated pbtest program uses the private helper/roster directory, redirects
pytest `cache_dir` and supported `--surface-json` arguments, and disables normal
Python bytecode writes while supplying a private bytecode prefix. This does not
redirect arbitrary application writes or arbitrary pytest/plugin output options.
A workload needing to write into source is not made compatible by the flag.
Only source-relative `--surface-json` destinations without a `..` component are
supported, including the generated default and nested relative report names.
For this opt-in layout those reports are **attempt-local intermediate artifacts**:
redirects retain the requested basename, not its original directory. They follow
attempt retention and are removed on acknowledged, drained success; the layout
does not separately publish them or promise a report at the original pathname.

Absolute surface destinations (even inside the submitter's checkout) and any
parent-traversing destination select the unchanged **private recipe before shared
creation**. Both split and `--surface-json=...` spellings are checked, in the
original request and after sealing; preparation rebinding does not opt an
external request into sharing. The private `_source_readonly_action` seam refuses such destinations
rather than silently flattening them. This preserves existing private submission
and output-path semantics, **not a guarantee that an external path is portable**:
normal path binding/refusal still applies, and a worker may not have the caller's
external filesystem. No external report is silently redirected into shared
attempt scratch to make it appear supported.

## Kernel ownership and descendant lifetime

The source-key mutex serializes publication, acquisition, and retirement. A
lease additionally holds a **shared `flock` on that exact generation's reader
file** throughout its source-reader lifetime. The pool retains its lease across
worker execution and termination/drain handling. The worker borrows its own
lease and verifies that its checkout is exactly the pool-selected source.

Writable attempts have a separate `.readers` file. Core passes both a duplicate
source-reader descriptor and the attempt-reader descriptor to the action,
alongside its ordinary output-lock descriptor. Inherited descriptors keep the
kernel holds alive through fork/exec and inheriting descendants even if the
worker dies. Releasing a parent lease closes only that parent's descriptors:
it must **not issue `LOCK_UN`**, which would unlock the shared open-file
description while a child still uses it. PID disappearance, a timestamp, an
owner string, or a proof marker is not a substitute for these holds.

This is a cooperative descriptor-inheritance contract. A descendant that
explicitly closes the descriptors or launches another process while discarding
them does not magically remain pinned. This is not hostile-same-UID containment.

### Attempt cleanup and retention

The attempt context covers execution, termination/drain handling, source
revalidation, and the pool's final resource-scope verdict. Cleanup requires an
explicit final outcome acknowledging successful execution: status `executed`,
launcher return code zero, action return code absent or zero, no termination
reason, and no action reported surviving kill. Before deletion, the pool must
also obtain a nonblocking exclusive lock on the original attempt reader inode
and verify the attempt directory identity.

The generated shared shard allocates pytest's inner basetemp below this owned
attempt without a second owner lock, standalone scratch-root registration or
independent expiry/sweep. Returning zero from pytest **does not remove that inner
tree**. It stays available through result/CAS publication, post-execution source
validation and the final resource verdict, then follows the outer attempt's
acknowledgement/reader gate. Successful outer deletion uses the existing
inode-checked owned-tree remover, including for read-only directories pytest
leaves behind. Ordinary private shards keep their existing standalone cleanup
and expiry behavior.

Failed, interrupted, unacknowledged, still-read, or uncleanable attempts remain
on disk, including their inner pytest scratch that pytest itself has retained.
Pytest's own `tmp_path_retention_policy` and test/plugin cleanup remain independent:
PB cannot restore data pytest has already deleted. This boundary is the final
execution/resource verdict and publication of the result, not durable completion
of the subsequent queue-status `finish()` call.
The implementation intentionally uses no temporary-directory finalizer
that would erase failure evidence, and has **no new background attempt reaper**.
When a final outcome is available, its `attempt_scratch` entry records the path,
retention decision, and reason. Abrupt process death need not produce that entry.
Cleanup unavailability does not reinterpret a published result as action failure.
Retained scratch needs operator-managed inspection/retention; do not remove a
live attempt or manually bypass its reader holds.

## Source retirement

Shared acquisitions invoke bounded, best-effort shared-source housekeeping;
it is not a daemon or worker-shutdown-only sweep. The current implementation
scans at most 32 entries per directory scan and retains resumable scan cursors
(up to 64). Reaping tries source-key and generation-reader locks without waiting
on active builders/readers. The bounds are scan bounds, not a hard time budget
for recursively deleting one generation.

- An unadvertised/noncurrent generation can be removed when its reader lock is
  exclusively available; it does not receive the warm-current retention window.
- A published current generation is retained until its recorded idle interval
  reaches **24 hours**. Acquisition clears the idle clock. Normal last-reader
  release records it; if a killed reader/descendant cannot record release, a
  later reaper that observes no readers starts the clock.
- Retirement unpublishes the current pointer before deleting its tree and holds
  exclusion through removal. No live reader is retired merely because it is old.
- Housekeeping failures are reported to stderr and do not change completed work.

The clock uses wall time and cleanup is acquisition-driven. Twenty-four hours is
an idle eligibility threshold, **not a deletion deadline or storage quota**;
without later shared acquisitions, eligible generations can remain indefinitely.
Small bucket/lock metadata can remain after a generation is removed.

Shared generations deliberately do not match legacy private-checkout owner
names. Legacy/private checkout reaping and SLURM per-job/Epilog deletion are
**not owners of shared generations** and must not delete them. Private checkout
behavior remains separate; the common low-level Git population helper does not
merge those lifecycles.

## Trust boundary and evidence

The runtime, configured host-local store, and its publication/lock metadata are
trusted. Read-only mode bits and census gates detect violations by cooperative
workloads; they are not a sandbox against code running as the same UID that can
chmod files, rewrite PB metadata, replace lock paths, or discard inherited file
descriptors. Do not place this store in an untrusted writable namespace or treat
its publication as an action-provided assertion.

Sharing is host-local and limited to compatible snapshots and CAS trust
contexts. It does not promise universal sharing across transports, profiles,
containers, deployments, filesystems, or arbitrary recipes. It also does not
establish an end-to-end performance improvement: materialization reuse and
avoided repeated Git proof are specific mechanisms, while measured benefits
and acceptance verdicts require their own recorded evidence.
