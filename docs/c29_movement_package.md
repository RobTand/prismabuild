# Packaged movement ownership

C29's initial movement slice moved code from fleet scripts into
`src/prismabuild`, from C05 `82f28e4ce36c3de222cc7818a5fb1c8837bb4f7d`.
The remaining completion below moves the tier owner and ten libraries from
`fe416690b87aba99ad30c7df4f95ac8da47055d5`. Execution qualification is recorded
separately from this source ownership contract.

## Callers and boundaries

A produced-output owner imports `prismabuild.produced_output` from a runtime's
`src`. On the tier host, `retire_batch` lazily imports
`prismabuild.stage_release`, releases its output-prefix lock, and calls
`stage_release.evict`. A remote owner keeps publishing the same tier-host
egress action. Package availability no longer changes that routing decision.
`retire_staged_batches` imports the same owner after selection and unlocking.
The diagnostic `_dead_staged_batch` no longer probes a tool import to decide
whether local retirement exists.

`stage_release.evict` takes the mover transition lock, reclaims containment,
then takes the stage ownership lock. Registration and refusal still read the
same `.prismabuild-stage.json` marker. Claimed-path censuses keep current and
pre-range spellings, exact claim identity, and all their existing retention
rules. C05's explicit `RecordStore` and `FilesystemTrust` remain authoritative.
No new cache or filesystem-trust policy is introduced.

`stage_release` uses `stage_move` for range names and `prewarm_loop` for progress
and manifest questions. `stage_move` also uses prewarm's mount map, admission,
pacer and reader. Its reverse imports of `stage_release` stay lazy.
`ram_promote` consumes the same copier and publisher, and belongs beside them.
`stage_rounding` owns only byte/token reporting and has no script entry point.

The intended caller usage is unchanged apart from module ownership:

```python
from prismabuild import stage_release

stage_release.register_stage_root(queue, tier_id=tier, stage_root=stage)
receipt = stage_release.evict(
    queue, mover, consumer_action_key=consumer, stage_root=str(stage))
```

All original function signatures and storage owners remain. The four executable
scripts only locate their immutable generation and call the packaged `main`.
Role singleton identity uses the unchanged `prewarm_loop` stem. No moved
module adds `tools` to `sys.path` or performs bootstrap filesystem I/O.

## Alternatives considered inline

One boundary extracts only claimed paths, registration and eviction. It leaves
those functions dependent on stage-release state, the staged publisher, and
prewarm helpers. Making that graph importable would require script re-exports,
duplicated state, or a callback adapter for the existing policy. That boundary
moves names without removing the production dependency.

The chosen boundary relocates the existing coherent modules and migrates all
callers directly. It preserves function bodies and shared state, removes the
three package/tool import forks, and needs no compatibility module. Five
package modules retain their existing domain divisions rather than merging
pacing, copying, retention and accounting into one module.

The initial movement slice deferred the tier loop and submission libraries
until C14 supplied package submission ownership. The required completion from
`fe416690b87aba99ad30c7df4f95ac8da47055d5` moves the tier implementation into
`prismabuild.tier_cycle` and ten remaining libraries into package owners.
`stage_reclaim` keeps its operator entry and restore launch behavior.

## Source migration and review

`refactor/c29_move.py` replays the source move and caller migration.
`refactor/c29_verify.py` compares function ASTs after explicit import
owner normalization, inventories imports, checks assertions, and records the
remaining manifest-call owners for Root. Run verification through PrismaBuild.
The move script edits source only; it never imports product code or runs tests.

Entry filenames, argv, marker bytes, paths, locks, flags and ordering remain.
`stage_rounding.py` is removed from the hand-kept shipping list because package
sources ship automatically. The four real entry wrappers remain in that list.
The duplication inventory only renames existing owner paths; it adds no budget
or exception. Existing native tests keep their names and assertion conditions.
The two embedded prewarm provenance assertions name the new package file.
Historical manifest-comparison readers retain their historical tool locator.

Root will compose C20's new manifest owner and C31's reader corpus separately.
Moved modules retain `from prismabuild import core as pb` and existing `pb`
manifest calls. The verifier exports exact old/new module and symbol maps,
including these calls, for that integration. Historical C31 source locators
must remain historical; current locators must follow the new package owner.

Execution evidence, frozen cohorts and limits live under
`/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/movement-package-completion`.
Existing C31 physical/hardware provenance is not transferred to this source.

## Remaining script libraries and tier entry

The package now owns `submission_frontend`, `pbtest_collection`,
`pbtest_capabilities`, `runtime_paths`, `role_log_identity`, `deferred_release`,
`manifest_promotion`, `fleet_submit`, `pbmergeq_runtime`, and `prelaunch_tier`.
Their former tool sources and `FLEET_SCRIPTS` rows are removed. Current test
imports, patch targets, embedded programs, and collection-source reads name
the package graph. The two optional package-import fallbacks in
`admission_shared_io` and `fleet_membership` are removed.
Membership also removes the missing-reader module census, its unused fallback
constants and its two private helpers. The required reader owner's enumeration
and existing unreadable-census refusal remain the production path.

`pbtest` reads collection source from the imported package owner's file and
carries those bytes under the existing embedded `pbtest_collection` identity.
The shipped helpers' import hook binds the recorder's package import to that
captured module, as it already does for the digest owner. Ordinary imports in
the tested project retain their on-disk identity. The default capability data
path remains `tests/pbtest_capabilities.json`; the migration rewrites module
literals only in import and patch calls, leaving data filenames intact.

`tools/fleet/tier_loop.py` remains the physical tier-role entry. It passes its
resolved entry path to `tier_cycle.main`. That binding retains the original
tool directory, RAM policy search, mover paths, singleton stem, argv, exits,
and park role. Direct package callers use the checkout's tier entry by default.
The existing funding, staged-path, tier-holdings and submission owners stay
below the relocated implementation. A narrower extraction would leave the
remaining functions dependent on script globals or require a callback layer.

The packaged `fleet_submit` defaults to `tools/fleet/slurm_job.py` under its
generation. Its sole physical producer, `seal_and_publish`, explicitly passes
the sibling of its own script: the short published caller retains
`tools/slurm_job.py`, and the nested caller retains `tools/fleet/slurm_job.py`.
Both entries carry identical bytes. The two-layout qualification drives real
copied producers into the existing fake scheduler and checks the exact job
script address and recorded script digest.

The stdlib-only `runtime_paths` has an existing retained proxy transport.
`publish_runtime.STANDALONE_PACKAGE_SOURCES` maps its two original published
leaf paths to `src/prismabuild/runtime_paths.py`; all three carry identical
bytes. This is one source owner with the existing transport names.
`resource_scope` proves the package owner when the captured receipt lists it,
and the original sibling leaf for retained receipts without the package path.
The installed updater's member sets, C15 receipt pair, copy order, protocols,
custody and prepared bridge do not change.

`refactor/c29_complete.py` replays this completion. Its verifier retains every
definition occurrence and decorator, compares whole normalized source and
function bodies, checks native names and assertions, and exports the exact
current owner and shipping map for C31. The duplication inventory only moves
existing approved owner paths. C31's current drivers and historical artifacts
remain byte-exact for their owner to reconcile.
