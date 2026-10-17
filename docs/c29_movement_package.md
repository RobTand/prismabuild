# Packaged movement ownership

C29's movement slice moves code from fleet scripts into `src/prismabuild`.
The source base is exact C05 `82f28e4ce36c3de222cc7818a5fb1c8837bb4f7d`.
It discharges the movement part of c29.r02-r04, subject to recorded execution
qualification. It does not claim completion of the entire C29 card.

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

Moving the complete tier loop would also pull manifest promotion into this
change. That module imports `pbrun`, which C14 owns on another branch. Tier
census, status/metrics/MCP census and the remaining fleet libraries are broader
C29/C27 work. `stage_reclaim` remains an operator entry command using the new
owners; its restore launch path and operator behavior stay byte-for-byte.

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
