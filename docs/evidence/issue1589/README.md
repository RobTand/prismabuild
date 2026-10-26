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

## Preservation status

The inherited source at `d3b5585846` passed 897 ARM preservation tests across
20 shards. Those results predate the unknown-presence fix and do not qualify
the final source. The x86 run had three failing shards. The import-boundary
file passed all eight checks when run with one pytest worker, avoiding its
shared wheel-build fixture race. The two archival files initially refused
historical environment identity because TMPDIR differed from its qualified
value. With `/home/rob/pbt17`, the host-record file passed all 65 checks.
The controller-record file reached its 900-second hard deadline. It printed
one earlier failure without a final trace, so its verdict remains unresolved.

Final architecture preservation and the controller replay must finish before
this unit goes to the Principal for review. The replay uses the recorded
historical TMPDIR, one pytest worker and an explicit 3600-second deadline.
Stop-on-first-failure preserves the first traceback if the earlier failure
recurs. No test expectation or production timeout changes to make this pass.
The published pbtest client has no action progress-phase submission option;
its durable report records completed shards, with explicit action deadlines.

Reports and launch manifests live under
`/mnt/shared/paperclip-runs/PRI-31`. The task records exact final unit names.
No runtime is published by this task. No staged-read deployment or workload
axis is promoted. [The candidate merge record](merge-acceptance.json) states
target and scoped implementation separately.

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
