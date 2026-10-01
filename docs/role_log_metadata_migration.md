# Exact legacy role-log metadata migration (#1396)

`tools/fleet/migrate_role_logs.py` is a separate, bounded operator, not a
supervisor maintenance path. The publisher inventory carries it and
`role_log_identity.py` in its existing flat `tools/` and nested `tools/fleet/`
layouts. Source inventory is not publication, installation or live qualification.
This is a **new feature**: no old operator existed and no artificial baseline
no-op, missing import or missing API is an old-behavior RED.

## Closed interface and proof

`main(argv=None)` accepts only `--apply` (plus ordinary help). The default is a
read-only plan; `run(*, apply=False)` consumes a freshly observed ephemeral plan.
There is no saved-plan replay, path, mode, UID, directory or host-policy option.
The only leaves are `/home/rob/tmp/pb-role-storage.log`, `pb-role-tiers.log` and
`pb-role-metrics.log`. Acting real/effective UID and file owner must be **1000**;
the target is **0600**. Only exact **0664** legacy or already-**0600** leaves are
eligible. A fixed leaf absent at its initial descriptor open means
`absent-no-effect`, never creation or adoption. Missing boot, runtime, process or
other qualification evidence is `refused`, not qualified leaf absence.

The complete three-leaf plan is qualified before any effect. It holds an owned,
non-symlink directory (owned **0775 is valid**) and original read-only,
no-follow/nonblocking/CLOEXEC descriptors. Each held/current named leaf must be
regular, single-linked, owner-matching and identical in device/inode/UID/GID/mode.
A current unambiguous declared writer per role must have matching UID,
PID/start incarnation, host ownership marker, exact declared argv, resolved
script member, session/group leadership, and **both FD1 and FD2** naming that
inode with writable append flags. Unknown, stopped, exited, duplicate or malformed
proof refuses. An absent/ambiguous/unsupported supervisor invocation also refuses;
valid cron/manual supervisors need not have the role-only environment or leadership.

The publisher owns shared `_sealed_generation(name)` receipt/full-member integrity.
Decoded receipts must be JSON objects before field access; arrays, null and
strings refuse through the existing qualification error boundary, including
post-effect requalification. Its `_barrier_generation(name)` still additionally requires the existing
rollout-aware updater. Migration does not borrow or waive that rollout policy.
Published and observed supervisor/role generation roster bytes must agree.
The JSON keeps **observed published**, **resolved supervisor**, **resolved role**
and **candidate tool/helper source** identities separate. These are sealed
filesystem source observations bound to PID/start, **not loaded Python import
memory attestation**, claim-lock ownership, adoption or convergence. Those facts
remain `UNKNOWN`. Full argv comparison stays internal; output exposes necessary
kernel/source fields and an `argv_sha256` through existing Core
`canonical_sha256`: UTF-8 compact JSON, sorted keys, `ensure_ascii=False`,
`allow_nan=False`, separators `,` and `:`. No argv or environment dump is emitted.
Main stdout uses existing Core `_sorted_lf_bytes`, preserving the original default
JSON spacing/ASCII escaping, sorted keys and one LF; no parallel serializer/hash
primitive was added.

## Effect and limitations

Immediately per leaf, full current source/census/incarnation/FD/name proof is
repeated before and after the effect. The only effect is
`os.fchmod(original_qualified_fd, 0o600)`. There is no create, content write,
truncate, path chmod, proc-FD mutation, signal, restart, chown, rename, fake
flock or directory-permission change. Qualified 0600 is a no-op. Neither
`_spawn_role`, `_run_supervisor`, `ensure_roles` nor retention maintenance is
called. Existing strict retention refusals (including 0664, 0620 and 0602),
append launch behavior and role-cycle behavior are unchanged.

JSON reports metadata/FD observations and each leaf's disposition. Any uncertainty
stops remaining leaves, without automatic retry or rollback. An attempted or
completed effect followed by failure is `partial-uncertain`, not success. Name
checks **do not atomically exclude rename** in an owned 0775 directory: a late
rename cannot redirect `fchmod` onto a replacement, but the formerly-qualified
held inode may already have changed mode when postcheck refuses. Earlier changes
remain reported. Sizes/mtime may change through natural append; live point-size
metadata is not full-byte equality. Private controls assert full payload digests
and later append, separately from the syscall-limited production contract.

## Acceptance remains with the parent

Private controls in `tests/test_exact_role_log_metadata_migration.py` exercise
the actual CLI/consumer and finite real Popen append writers for all three roles,
0600 no-op, owner/mode/inode/content preservation, both append FDs, namespace and
process/generation/FD refusals, partial stop and late-rename uncertainty.
`tests/test_role_log_migration_generation_integrity.py` covers real publisher
receipt/member integrity, unchanged updater policy and actual manifest/layout
source resolution/dependency checks. Only raw kernel/filesystem/runtime facts are
substituted, not verification verdicts. The first 18-file qualification action
`3de7e485d87d6a3975d88809ff704d9fb8981111ecfc1fd4aad060ee971689cf`
ran on dl380g10: **290 passed, 1 name-contract guard failure, 0 skipped**,
291 reconciled cases. This was not GREEN. Independent ASCII/nonASCII argv digest
and original JSON+LF stdout controls ran in that census. UID1000 positives
explicitly skip on another acting UID; skips are not proof.

The subsequent actual review-boundary action
`5ccf96652bf15c14535c8ace8dc7205fc5b7e6a1c17e0cac6af3cfa34be0c1a6`
ran **9 failed, 1 passed, 0 skipped**, ten reconciled cases. Six failures reached
real non-object receipt reads, including after the first genuine mode change;
three reached missing-proof misclassification. The real missing-leaf control
passed. `tests/test_role_log_migration_review_boundaries.py` preserves those
physical readbacks and desired JSON contracts. Both actions' terminal records,
immutable attempt/log digests and released cleanup were verified.

The corrected nineteen-file action
`a47e32c75476eb95067da3d4c53ed0604a347800a422c4620c9483367362d273`
ran on dl380g10: **301 passed, 0 failed, 0 skipped**, with 301 reconciled cases
in 248.79 seconds. It includes the ten reached-boundary controls, unchanged original
private controls, publisher/supervisor regressions and duplication guards.
Terminal and immutable attempt/log digests, canonical CAS receipt/result and
released cleanup were verified. Retained independent source review found no
issues in the receipt-object guard, typed initial-leaf absence and three exact
name-contract exceptions; source review is not runtime acceptance.

Qualification used the published PrismaBuild client: portable CPU2, memory4GiB,
one native thread, priority-10, an explicit 600-second timeout and one shard. Syntax and isolated nested cold-CLI validation executed through action
`5ad3f0b3c2e2e7b54391e9e2cd8a081ad422f4681caa960fb491c7756500146f`:
`COMPILE_OK 7 files` and `COLD_HELP_OK`, with verified terminal, immutable logs,
canonical CAS result and released cleanup. The refreshed committed-candidate
gate remains pending. The failed results
above are the repair witnesses; this corrected GREEN is private qualification,
not live apply, runtime publication or whole-issue closure. Any later exact live apply/readback is a separate parent decision
under scoped maintenance authority, requiring an admitted host-dependent action
and proven UID/filesystem/proc visibility. If containment cannot expose the required
proof, refuse: no local/SSH chmod fallback, automatic retry or generic permission
loop. A timeout with uncertain effects must not be treated as safely retryable.

Normal task9 publication/full generation-bound canary/image/timeout prerequisites,
task10 profile decisions, runtime adoption and natural retention readback remain
separate. This feature establishes no live installation, role convergence,
retention observation, disk recovery, staged-ledger promotion or full #1396 closure.
