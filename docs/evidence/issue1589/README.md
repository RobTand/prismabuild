# Class CPU admission beside READY GPU work

The READY-GPU opening guard treated class-only CPU prerequisites as portable.
It deferred a higher-priority `gb10` prerequisite even when both reservations
fit. The repair proves complete class-tag exclusion from CPU-only hosts before
allowing the row to reach existing physical admission. Under the host lock it
preserves CPU, memory and host spool room for the GPU row. Shared adaptive holder
pricing charges the candidate's complete future CPU demand with the 1.25 margin,
including accepted export allowances before funding deductions.

Rob approved these limits through PRI-31 interaction
`7c2a2fb9-0d6a-444b-b9f0-9963002ab7ab` at 2026-10-25T23:43:12.170Z, bound to
policy-decision revision `3d0d1914-ce34-4afa-887e-6a65efd5db21`.
`docs/design.md` records the limits. This approval does not authorize merge,
runtime publication, restart or campaign acceptance.

The task branch ports the relevant source and checks from draft PR #2180,
retaining main's gang and idle-guest policy. The original red commit
`e965fe4ef3000b081e0ee90d2546223122448ce0` remains preserved in Git history.
The rebased history keeps the shared pricing refactor before the policy change.
An independent read-only preflight found an additional unknown-presence case;
that regression also precedes its fix.

## Verified focused evidence

Original red action on sparklina:
`b3f4b57ebe37124bf01fc1b46aa15dd7d55d1688d2ce6ee2364c016e536b0b20`.

```text
2 failed, 30 deselected in 0.30s
```

Sealed-request version of the same regression before the policy fix:
`6d7ac1528cadb5a7070a0713614f462517661db20197408689493e12240f43d9`.

Mixed unknown-GPU-presence red action on dl380g10:
`ee0d93488de996816b5bc11cd466e9efd915adf92dd1701505543dab9737ad47`.
The fixture retains known CPU and GPU offers and adds a matching host with
missing, null, integer or string GPU presence. It must retain GPU protection.

```text
4 failed, 67 deselected in 0.43s
```

Current-source green action on dl380g10:
`674767c913eb15cbe5d01c9e88a018cd301b7af1248e7b5849ca289ba3fb6304`.
It covers `test_gpu_host_ready_work_admission.py`,
`test_class_scoped_cpu_admission.py` and `test_class_scoped_cpu_headroom.py`.
Demand was 1 CPU, 2 GiB, no GPU, native thread counts 1, priority -10 and a
180-second hard deadline. Tests use private queues and simulated GPU evidence.

```text
131 passed, 88 warnings in 18.33s
```

Verified execution receipt:
`06cc18c79d31f027b61a89bba61d9d58055219da68d6aa2211446321784a6348`.
Sealed snapshot: `872124db0811d59633166799183a9cadcb516c4d`.
The scheduler bytes match source commit
`8d7a74ebcfdecd75672be085e2c7ff8e3909e21a`.

## Preservation and integration evidence

The frozen source `19511051f7`, based on main `128c8cc958`, passed 985 tests
plus four 145-test repetitions on each architecture. Runs two and four used
two pytest workers. All ten clients exited zero. Every collected test has
an outcome and final teardown, with no skips. All 72 executed actions have
verified cleanup, exact attempt log hashes, sealed scheduler and test bytes,
runtime and conftest identities, and exact CAS execution receipts.
`fence-19511051f7-reconciliation.json` records the checks. The xdist controller
records no selected-file runtime hashes. Those test bytes are proved from
the sealed input bundle, with the worker count in the sealed command.
No per-worker observed test-source hash is claimed for xdist.

The same frozen source completed all 302 archival cases. Controller action
`4fcb3ba986ce9e0b523cff59a5011e31e79bf76ad1d0fc014506b030141e54a7`
reported 236 passed and one failure, only `config-fleet/source`. Host action
`2a58881107e04c8b643c6c4c09289c771373ba9bc7850696bca15d110e4cbb7a`
passed all 65 checks. All outcomes and teardowns reconcile, with no skips.
The archival client exited 1. Both actions have verified log hashes, source
and cleanup; the host action has an exact success receipt. The controller
failure has no success receipt.

The historical config golden lacked `action_tmpdir` and `_action_tmpdir_why` from #1914.
Unchanged-main action
`ae172f72e5f85f70bea778d321e447d14a4c49f01d2a7f5748e3f61a525344b2`
reproduced it: one failure, two passes, 234 deselected. Branch and the then-current
main retained identical production config blob
`fc953a49d95a2b3a874dbd133e417ad277c117ba`. Existing #2253 records this case.
This was a baseline compatibility failure, retained as historical evidence.
The baseline reproduction changed no golden, assertion, skip or timeout.

The subsequent rebase onto main `e2be346b0c1` retains #2272's exact cleanup
obligations. The pricing and candidate-policy modules and all four touched
test files are byte-identical to the frozen matrix. The field inventory
retains main's changes and adds exactly the two reviewed class-admission
entries. Its source-only maintenance report is `field-policy-e2be346b0c1.json`.
The earlier maintenance-inventory entries now belong to main and are no longer
a delta in `field-policy-review.json`.

Source `89a4e1dc25` passed the integrated eight-file claim/cleanup scope:
232 tests per architecture, 16 actions, both clients exited zero. No skips
or missing outcomes or teardown. Every action's cleanup, exact attempt logs,
sealed source, runtime identity and exact CAS receipt are verified in
`cleanup-89a4e1dc25-verified/verified.json`. The source is unchanged after
that run; subsequent edits update evidence and generated field metadata.

Each action reserved 2 CPUs, 4 GiB and zero GPUs, with one native thread per
worker and priority -10. The matrix used 900-second deadlines, archival
3600 seconds, and the cleanup integration 900 seconds. Default max-clients
8 remained unchanged. Aggregate bounds were 38 CPUs and 76 GiB for the matrix
plus archival, then 32 CPUs and 64 GiB for cleanup integration. PB owned
placement. Architecture tags express the required matrix. Every client ran
in a persistent transient user unit. The qualified TMPDIR was `/home/rob/pbt17`.

The earlier `a70f06917f` x86 client failed because HEAD moved during its run,
despite 901 passing shard tests. Its four repetitions never launched. That
failed client verdict is retained; the frozen-head matrix corrected it.
Earlier preservation and main-baseline proof remain available. The published
pbtest client lacks action progress-phase submission; it records durable
completed shards with explicit deadlines.

Reports, launch manifests, verification and reconciliation live under
`/mnt/shared/paperclip-runs/PRI-31`. Independent Principal review and merge
remain pending. No runtime was published. The staged-read ledger axes are
unchanged. [The candidate merge record](merge-acceptance.json) keeps targets,
component test evidence, deployment and workload proof separate.

## Merge-readiness refresh after #2329

Principal review at `cbb83702fba` found zero Standards and Spec findings.
Main `19bdca60f63` then supplied #2329's accepted capture repair. The clean
rebase is frozen source `27f8b437d313cbb872ece0610a3de78e4fffdb7b`.
All 11 commits retain their order and patch identity. Exactly five compatibility
capture files came from main; the other 1,736 `src` and `tests` entries remain
identical to the reviewed head. Scheduler and test logic are unchanged.
The existing matrix, cleanup integration and unaffected archival proof are reused.

Action `2c45483225335363dc21676347a93c48e3485933add8a9a89d963097c35f2c85`
on dl380g10 passed the repaired `config-fleet/source` case and both
`config-fleet` controls: **3 passed, 234 deselected**, no skips. The pbtest
client reported `1/1 shards green` and the unit ended. Its process exit
record was not retained before systemd collected the unit. Every selected
test has an outcome and final teardown.
Exact logs, sealed source, runtime identity and scope cleanup are verified.
Its exact execution receipt is
`d859fa61c02c9ec38a80b9eea8b93e11ac31a5db626f8cc5a1affe6c644f2464`.
The run reserved 2 CPUs, 4 GiB and zero GPUs with one native thread, priority
-10, a 900-second action deadline, and an 1800-second client wait.
It ran in persistent unit `pb-pri31-config-refresh-27f8b437d3`.

The historical failed archival action and failed client remain visible.
This targeted refresh does not claim a new all-green full archival replay.
Exact launch, report, ending, verification and reconciliation artifacts use
prefix `/mnt/shared/paperclip-runs/PRI-31/config-refresh-27f8b437d3`.
`rebase-27f8b437d3-verification.json` records the unchanged-byte proof and
`rebase-27f8b437d3-range-diff.txt` records ordered history preservation.

`verify_rebase.py` repeats the read-only byte check:

```bash
python3 docs/evidence/issue1589/verify_rebase.py \
  --checkout "$PWD" --reviewed cbb83702fba \
  --old-base e2be346b0c1 --new-base 19bdca60f63 \
  --candidate 27f8b437d3 --output /tmp/pri31-rebase.json
```

The final evidence commit and published-head mapping live in the task's
merge-readiness document. Exact-head Principal acceptance and merge remain
pending. No runtime publication, restart or campaign completion occurred.

## Recheck recorded fleet artifacts

Read terminal evidence with the published client, then run the read-only
verifier. Store outputs outside the source checkout:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/fleet/pbwait.py \
  --endings --wait-s 0 FULL_ACTION_KEY > /tmp/pri31-ending.json
python3 docs/evidence/issue1589/verify.py /tmp/pri31-ending.json \
  --checkout "$PWD" --output /tmp/pri31-verification
```

The verifier checks scope cleanup, exact attempt log hashes, the sealed bundle,
scheduler source bytes and the execution receipt. It does not execute tests.
Use `--historical` for red or superseded evidence. Passing pytest summaries
alone do not establish final collection and teardown coverage; inspect each
pbtest report's `test_completion` alongside the verified endings.
