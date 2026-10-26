# Slice 2b serialization ownership repair

Vehicle 3 excluded PR #2299 at `3acaeacb2d547518fa0fb7c2d0c26c213cf9cea2` because the mandatory digest guard found two new sorted-JSON sites outside the owner. [Causal report](https://github.com/RobTand/prismabuild/pull/2299#issuecomment-6411195019).

`adaptive_snapshot._previous_snapshot` now uses `digest_primitives._canonical_bytes` for its compact UTF-8 size bound. Its import remains inside the function because the publisher also executes as a standalone script. `InputChanges.observe` uses `digest_primitives.sorted_json` with `allow_nan=False` for the default sorted text spelling. Both profiles match the prior recipes. Python object equality would change numeric spelling comparisons; importing Core would load unrelated attestation code.

The guard and duplication baseline are unchanged. Six new characterization cases preserve key ordering, numeric spelling, nonfinite rejection without state mutation, and both UTF-8 bounds. Existing mutable-observation and recovery assertions remain unchanged. Test commit `b0a9c33c03e5c6d7d9e89ae1006202c1c2d82d7b` precedes refactor `d35af5876dc26cc63f0ffc936599471a8e75f051`. This ownership refactor changes no policy or default.

## Fleet evidence

Before the refactor, exact guard action `75872be9fe9590c56443aef50b4ae6e685ad72a235a147db82b68f8211c27d21` on dl380g10 reported:

```text
1 failed in 19.94s
```

Pre-refactor action `ce907fc9329701c43577c78281718391ffaab3880a426b10efacb03404601ca3` on Sparklina reported the same failure with all characterization cases passing:

```text
1 failed, 16 passed in 10.11s
```

After the refactor, five PB actions reconcile 60 unique collected, executed and passed tests, zero skips and warnings. Selection includes the full duplication guard file, observations, mirror publication and recovery, and local adaptive state. Each action declares one CPU, two GiB, zero GPUs and one native thread. PB chose five shards on dl380g10. Aggregate demand is five CPUs and ten GiB; client concurrency retains the default eight. Action deadlines are 180 seconds and waits are 600 seconds.

Transient unit `pb-pri64-serialization-d35af5876d.service` has finished. The report and submission are in `/mnt/shared/paperclip-runs/PRI-64/batch-repair/`. The audit verifies terminals, both immutable logs, canonical CAS receipts, result payloads, source snapshots and exact node IDs. A supplemental runtime read checks affinity, native threads, declared resources and completed cleanup. The snapshot differs only by PB's generated closure record.

The [scoped acceptance JSON](demand3_slice2b_1941_batch_repair_acceptance.json) records every action and separates target, source implementation and deployment. It supplements the historical 664-test audit. That entire population did not rerun for this repair.

Rerun the read-only verifier:

```sh
python3 docs/results/demand3_slice2b_1941_verify.py \
  --run-dir /mnt/shared/paperclip-runs/PRI-64/batch-repair \
  --checkout /home/rob/paperclip-worktrees/prismabuild/PRI-64-batch-repair
```

## Review and release gates

Earlier source approval is historical. The repaired published exact head requires fresh independent Standards and Spec review from Code Reviewer 01. Principal Engineer 2 owns batch composition, validation and merge on PRI-128. No freshness rebase was requested or performed. The original approved head remains an ancestor.

Keep #1941, #2102 and #2103 open, and #1942 unstarted until merge is verified. No runtime publication, new generation, service restart, deployment acceptance or campaign completion is claimed. Broader staged-read ledger axes and later event-driven slices remain owed.
