# Slice 3a tier bell adapter evidence

Tested source `6cef1e12efbb2610c8186fd2ab304bcfc1f55726` extends the names-only
bell to four existing tier namespaces. Claim order stays in commitments.
The shared census and visibility refactor precedes behavior changes.

The pre-fix action `c88879726fe5756e444ca4739ebfc1c6b44b6ebddb6a2c9c39ec27730b84b48d`
failed 15 deterministic mutation cases. Its terminal and immutable log digests
are in `red-audit.json`. The initial integrated result retained two v2 fixture
failures because those fixtures assumed tier topics would remain uncovered.
`6cef1e12ef` makes incomplete coverage explicit and retains the refusal assertions.
Seven unchanged-base cases passed in action
`30fcad99b607eef4104aaa18a3e1e4dcadc1b4ac45d43dda5a5f87ae190857a0`.

| Selection | Unique tests per run | Runs | Executed actions | Outcome |
|---|---:|---:|---:|---|
| Final integrated, 15 files | 386 | 1 | 15 | 386 passed |
| Five touched files, x86/Python 3.14 | 158 | 5 | 25 | 790 passed |
| Five touched files, GB10/Python 3.12 | 158 | 5 | 25 | 790 passed |
| Unchanged-base v2 fixture baseline | 7 | 1 | 1 | 7 passed |

There are zero skips. The integrated run has two existing fork deprecation
warnings in `test_queue_bell.py::test_real_requeue_supersession_wakes_exact_publication_waiter`.
Qualification runs have no warnings. Each touched file ran three times serially
and twice with two xdist workers per architecture. All 66 successful actions
executed once with distinct sealed keys; none was a cache hit or retry.
These are repeated executions, not 1,973 unique tests.

`evidence.json` retains each action key, terminal and log digest, canonical
receipt and result digest, runtime, affinity, native thread bound, resources,
cleanup, collection/outcome reconciliation and sealed request worker count.
The source audits verify the source bundle bytes and equality of complete
`src`, `tools` and `tests` trees with the named tested source.

`validation-plan.json` retains exact published-client commands. PB owns file
partitioning and placement. Architecture tags declare qualification cohorts.
Serial shards request 1 CPU and 2 GiB; xdist shards request 2 CPU and 3 GiB.
Native thread limits are 1 per worker, GPU demand is zero, priority is -10,
action timeout is 180 seconds and client wait is 600 seconds. The sequential
controller keeps the default eight clients and bounds outstanding resources
at 10 CPU and 16 GiB. No disk-semantics tests are selected.

Run the read-only auditors from a coordinator with retained fleet evidence:

```bash
python3 docs/results/demand3_slice3a_1942/verify.py \
  --evidence-dir /mnt/shared/paperclip-runs/PRI-64/slice3a
```

The command reads retained records and CAS bytes and writes local audit JSON;
it submits no work and executes no tests. It requires the retained worktrees
and reports named by the frozen plan. Missing retained evidence fails the audit.

`merge-acceptance.json` is scoped source-merge evidence pending independent
external review and batch landing. Global ledger axes remain unchanged.
Consumer arming, live latency, outage recovery, runtime publication, role
convergence and whole staged-read or campaign acceptance remain unqualified.
`standards-review.md` and `spec-review.md` are internal read-only reports;
they grant no external approval. No deployment authority follows from this record.
