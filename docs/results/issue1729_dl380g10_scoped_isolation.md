# dl380g10 scoped-isolation evidence preparation

Work: PRISMABUILD-1729, contract revision 2, writer attempt 6. Source baseline:
`dd82782c5fce79648bc2981e1f4ee4fc327a99d7`.

`document_status: prepared` · `live_verdict: not_run`

This revision delivers an offline procedure and an unpopulated
[evidence template](../evidence/issue1729_dl380g10_scoped_isolation.json).
No worker was contacted, runtime published, measurement submitted, or host
capacity observed. The parent's **three live acceptance tests remain
unexecuted**; preparing these documents does not complete those tests or the
[parent issue](https://github.com/RobTand/prismabuild/issues/1729).
Their individual authoritative definitions were not supplied in this brief;
the evidence criteria below must not be substituted for those definitions.

[PRISMABUILD-1727](https://github.com/RobTand/prismabuild/issues/1727) (k2) and
[PRISMABUILD-1728](https://github.com/RobTand/prismabuild/issues/1728) (k3) are
**unverified deployment prerequisites**. Their merged, published, and loaded
states are unknown here. Source presence, a source test, a roster entry, or a
runtime pointer is not loaded-worker support. k3's deployed guard-load schema
has not been supplied; this package does not invent it.

## Source grounding

These references describe checkout behavior, not a deployed observation:

- [Publisher layout](../../tools/fleet/publish_runtime.py#L14): immutable
  generations carry `RUNTIME_VERSION.json`; the runtime name is
  `/mnt/shared/prismabuild-fleet/repo`. The
  [publication manifest](../../tools/fleet/publish_runtime.py#L615) maps member
  paths to SHA-256 digests, with fleet scripts/data published at both
  `tools/<member>` and `tools/fleet/<member>`. The
  [generation construction](../../tools/fleet/publish_runtime.py#L2298) uses
  `runtime-generations/<commit-prefix>-<unix>-<nonce>`.
- [Live roster reader](../../tools/fleet/publish_runtime.py#L1020), specifically
  line 1039, accepts `tools/fleet/fleet_boxes.json` and
  `tools/fleet_boxes.json` in the resolved generation. This does not establish
  which member layout or generation dl380g10 workers loaded.
- [Loaded generation boundary](../../tools/fleet/worker_loop.py#L135) and
  [drift comparison](../../tools/fleet/worker_loop.py#L382) distinguish loaded
  bytes from the moving runtime pointer.
- [Verified result reader](../../src/prismabuild/action_result.py#L485):
  `read_verified_action_result(queue, action_key, published_unix=...,
  attempt=..., max_result_bytes=..., max_evidence_bytes=...)` returns verified,
  owned bytes. [Lines 662–673](../../src/prismabuild/action_result.py#L662)
  expose action key, publication timestamp, attempt number, generation,
  worker/host, request, receipt, and payload. This package does not assume
  additional terminal-detail fields. Attempt nonce and scope evidence must
  come from retained attempt/claim artifacts using their actual schema.
  The returned `generation` is the queue publication digest from
  [PoolQueue.attempt_generation](../../src/prismabuild/pool.py#L6058),
  not the loaded runtime generation; the JSON calls it `publication_generation`.
- [Terminal generation selection](../../tools/fleet/pbrun.py#L2838) explains
  why one action key can have several runs. Publication timestamp, attempt
  number and nonce are distinct from the runtime generation and are required
  for evidence binding.
- [Diagnostic coverage](../operating_prismabuild.md#L2165): bounded denial
  history, asynchronous publication, dropped/coalesced observations and partial
  census reports limit what a status snapshot proves. The
  [execution policy](../agent_execution_policy.md) and
  [operating guide](../operating_prismabuild.md) govern the follow-up. Existing
  whole-host measurement isolation is background behavior; k2/k3 support must
  be established on loaded workers before this scoped proof is attempted.

## Planned CPU proof and evidence model

Let `A` be the observed available CPU-ID set on dl380g10 for the proof.
The target requires `|A| = 80`. It does not assume that CPU IDs are `0..79`,
that a roster's declared count equals availability, or that coordinator
affinity describes this host.

Let `M` be the measurement's assigned CPU-ID set, with `|M| = 4` and
`M ⊆ A`. Let `G` be its measured guard-band CPU-ID set, calculated by the
deployed isolation policy from retained topology/allocation evidence.
Define `P = M ∪ G`, `P ⊆ A`, and `S = |P|`. The guard calculation retains
its inputs, deployed policy/schema, output IDs and evidence bytes; `S` is
observed, never guessed from four CPUs or hard-coded as a guard width.

For peer allocations `Q₁, …, Qₙ`, there must be **one common nonempty
interval** `I = [t_start, t_end)`, with `t_start < t_end`, inside the
measurement payload's execution interval and every counted peer payload's
execution interval. During all of `I`:

- Each peer has an affirmative claim for its exact publication/attempt and
  retained payload evidence of execution on its assigned allocation.
- `Qᵢ ⊆ A`, `Qᵢ ∩ P = ∅`, and `Qᵢ ∩ Qⱼ = ∅` for all `i ≠ j`.
- `⋃ Qᵢ = A \ P`, so `∑ |Qᵢ| = 80 - S`.
- Each peer declares CPU demand equal to its useful worker count/allocation;
  aggregate peer CPU demand is exactly `80 - S`. Memory demand is measured
  aggregate peak use per action, with the measurement and observation overhead
  included in the total resource plan. All actions declare zero GPUs and one
  native thread per worker, preserving PB-assigned affinity.

Claims without payload execution, cache hits, dependent exports, and claims
after measurement termination cannot count. Pairwise overlaps at different
times do not establish `I`. Allocations and timestamps need one host clock
domain or a retained conversion with uncertainty small enough to establish a
nonempty intersection. Allocation changes within `I` require evidence of the
invariants throughout; a single sampled mask is insufficient.

The JSON separates deployment, CPU inventory, measurement identity, peer
overlap, diagnostic coverage, and terminal/receipt evidence. `record_template`
objects describe future records; they are not observations. All unknown
observation values are `null` or empty arrays, with enclosing reasons. The
guard-load entry holds schema and raw-artifact references only until k3
supplies its deployed schema. Its sample must bind the measurement's action
key, publication, attempt/nonce, protected set, host, and the same interval
`I`, using that schema's actual identity/time fields.

## Live verdict rules

Each live criterion starts at `not_run`. A criterion is `pass` only when
retained evidence supports its entire predicate, `fail` only when retained
evidence contradicts it, `not_run` when no relevant execution occurred, and
`unknown` when execution occurred but observation is incomplete. A skipped
tool, unreadable record, or missing denial cannot supply affirmative admission
proof. Incomplete observation without contradiction remains `unknown`.

| JSON criterion | Evidence needed for pass |
| --- | --- |
| `deployment` | Reviewed k2/k3 manifest digests match fresh loaded-worker attestations for all participating dl380g10 workers; deployed policy and k3 schema retained. |
| `cpu_inventory` | Host inventory/topology and availability evidence identify `A` with 80 CPU IDs for the proof interval. |
| `measurement_identity` | Sealed four-CPU measurement request, exact publication/attempt/nonce, claim, scope, assigned `M`, guard calculation and payload boundaries agree. |
| `peer_overlap` | Affirmative claims and payload evidence establish all set equalities and declared demand throughout the same nonempty `I`. |
| `diagnostic_coverage` | Required diagnostic sources and relevant admission history are accounted for with complete interval coverage and no unresolved drops, stale records or missing sections. |
| `guard_load` | k3's deployed-schema evidence binds this measurement attempt, protected set and `I`, and satisfies the deployed guard-load predicate. |
| `terminal_receipt_cleanup` | Measurement and every counted peer have successful exact-attempt terminals, immutable logs, content-verified CAS request/receipt/payload bytes and verified scope cleanup. |

Overall `live_verdict` is `pass` only if every criterion passes, `fail` if any
criterion fails, `not_run` if no criterion ran, and `unknown` otherwise.
Source review does not promote a live criterion. This document's `prepared`
status is independent of that verdict.

## Operations follow-up

**All steps and templates in this section are unexecuted.** This is the only
section prescribing host operations. It supplies an operations plan, not
runtime publication authority or an exception to staged-only policy.

1. **Deployment checks.** Obtain the parent's three authoritative live test
   definitions and k2/k3 deployment acceptance records. Resolve the runtime
   pointer and retain its immutable `RUNTIME_VERSION.json`, manifest digest,
   reviewed commits, per-member digests and actual roster layout. Compare
   these with fresh attestations for each participating loaded worker (worker
   identity, host, process identity/start, loaded generation/digests, observation
   time and freshness basis). Establish matching k2/k3 support in every
   admission path used by this proof, and retain k3's actual guard-load schema
   and pass predicate. If unavailable, stop before submission and record the
   missing prerequisite; no publication or restart is authorized by this unit.
2. **Inventory and resource plan.** On dl380g10, retain host-side available,
   online and allowed CPU IDs, topology, reservations/external load, and the
   deployed definition of availability. Establish `A` for the intended interval.
   Qualify bounded payloads through PrismaBuild, including measured peak
   memory, overhead and useful worker counts. Preflight the deployed guard
   calculation using retained qualification evidence. Freeze the complete
   measurement and independent peer plan before publishing its first unit,
   with peer CPU demand totaling `80 - S`. Then confirm actual `M`, `G`, `P`,
   `S` on the admitted measurement attempt. If the guard differs from the plan,
   end that proof attempt and prepare a new immutable plan; never edit sealed
   requests or resize queued units.
   The aggregate payload CPU demand is `4 + (80 - S)`; `S - 4` guard CPUs
   are protected rather than peer capacity. Reserve measured memory for the
   measurement, every peer and any admitted observer; account for external load.
3. **Bounded payload/deadline templates.** Substitute reviewed payloads,
   interpreters, measured memory and finite positive deadlines before use.
   The following shell templates are **unexecuted** and intentionally require
   operator-supplied values. Run submission on dl380g10 so `--here` means that
   host. Host restriction has a real dependency: this proof concerns
   dl380g10's CPU IDs. No `--exclusive`, manual affinity widening, GPU demand,
   or dependency on the measurement's completion is used for the peers.

   ```bash
   python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
     --transport pool --measurement --here --cpus 4 \
     --demand "gpu=0,mem_gb=${MEASUREMENT_MEM_GB:?measured peak required}" \
     --timeout-s "${MEASUREMENT_DEADLINE_S:?finite payload deadline required}" \
     --env OMP_NUM_THREADS=1 --env MKL_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1 \
     --detach -- "${PAYLOAD_INTERPRETER:?qualified interpreter required}" \
     "${MEASUREMENT_PAYLOAD:?reviewed bounded payload required}" \
     "${PROOF_PLAN:?immutable event and deadline plan required}"

   python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
     --transport pool --tag dl380g10 --cpus "${PEER_CPUS:?planned useful workers required}" \
     --demand "gpu=0,mem_gb=${PEER_MEM_GB:?measured peak required}" \
     --timeout-s "${PEER_DEADLINE_S:?finite payload deadline required}" \
     --env OMP_NUM_THREADS=1 --env MKL_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1 \
     --detach -- "${PAYLOAD_INTERPRETER:?qualified interpreter required}" \
     "${PEER_PAYLOAD:?reviewed bounded payload required}" \
     "${PROOF_PLAN:?immutable event and deadline plan required}" \
     "${PEER_ID:?unique planned peer required}"
   ```

   Repeat the peer template for each frozen independent unit, not as nested
   submissions inside admitted children. Each payload has its own finite
   deadline and event-based readiness/release protocol: record payload entry
   and inherited affinity; start useful bounded CPU work; signal readiness;
   await the common observation/release event with a deadline; retain payload
   exit and durable observations; join all workers and clean up in a finalizer.
   Native threads remain one per worker. No fixed sleep proves overlap.
   A run lasting more than a few minutes needs workload-based progress phases
   and durable committed units through the published helper, only after fresh
   workers advertise both `progress-v1` and `progress-helper-v1`; heartbeats
   and observation traffic do not establish advancement.

   Retain each submission response. A cache hit or attachment to a prior run
   does not create this planned proof; count only independently verified native
   execution in `I`. Record absolute queue/observation/cleanup deadlines in the
   proof plan. `--timeout-s` bounds payload execution subject to the worker's
   ceiling and credited waits; it is not a resource-release deadline.
   A detached submission does not wait. Use bounded waits for each retained
   full key, and withdraw this proof's remaining work at the planned cutoff.
   A wait expiry does not cancel work or prove cleanup.

   Unexecuted wait template:

   ```bash
   python3 /mnt/shared/prismabuild-fleet/repo/tools/pbwait.py \
     --wait-s "${REMAINING_WAIT_S:?finite remaining budget required}" \
     "${ACTION_KEY:?retained full key required}"
   ```

4. **Overlap capture.** Retain original sealed requests, affirmative claim and
   attempt records for the measurement and each peer, full keys,
   `published_unix`, generation digests, attempt numbers/nonces, worker/host,
   scope and allocation evidence. Capture payload entry/readiness/exit events,
   masks and time bases. Once all counted payloads are executing, retain a
   common observation interval within every payload lifetime. Verify `|A|`,
   `M`, the deployed guard calculation, `P`, `S`, pairwise disjointness,
   exclusion of `P` and exact complement coverage. Obtain the guard-load sample
   using k3's deployed schema during the same interval and bind it to the same
   attempt. End/release events delimit the interval; post-termination claims
   and later exports cannot fill gaps. Stop the proof if actual allocations
   cannot satisfy the frozen plan; do not steer PB's CPU placement.
5. **Diagnostic completeness.** Retain bounded status snapshots and original
   admission diagnostic artifacts before eviction. For example, this status
   template is **unexecuted**:

   ```bash
   python3 /mnt/shared/prismabuild-fleet/repo/tools/pbstatus.py \
     --transport pool --json --timeout-s "${CENSUS_BUDGET_S:?finite budget required}"
   ```

   Record `complete`, `timed_out_sections`, `unavailable_sections`,
   `abandoned_children`, host/worker freshness, diagnostic time bounds, dropped
   or coalesced records, unreadable sources and skips/missing tools. Ensure
   publication and attempt identity match, and cover `I` using the actual
   deployed diagnostics. A complete census is not an atomic overlap proof or
   necessarily complete admission history. Missing denials never prove
   admission; claims and payload execution supply affirmative evidence.
6. **Terminals and content addresses.** Retain successful terminal and original
   attempt records for every counted action and verify their publication and
   attempt match the claims. Use the published `read_verified_action_result`
   with the exact full key, `published_unix`, positive attempt number and
   explicit byte caps; retain its returned mapping and owned request, receipt
   and payload bytes. Record reader refusals instead of bypassing them. Retain
   original CAS receipt bytes and their location, digest and byte length, plus
   result and log bytes/digests. Hash the actual bytes and compare with their
   declared content addresses; do not hash reserialized JSON or infer validity
   from a pathname. Bind log digests and cleanup evidence from the actual
   attempt schema, not assumed result-reader fields. Resolve all evidence
   references before changing a criterion to `pass`.
7. **Cleanup and final assessment.** Release event barriers in normal and
   failure paths, join children, and use PB's supported withdrawal for queued
   or running proof actions at the recorded cutoff. The following template is
   **unexecuted**; use only this proof's retained full keys after confirming
   they still identify its publication, never unrelated work:

   ```bash
   python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
     --transport pool --withdraw "${ACTION_KEY:?retained full key required}" \
     --reason 'PRISMABUILD-1729 bounded proof cutoff'
   ```

   Verify exact-attempt process/scope termination, cleanup settlement and
   reservation release using retained PB evidence. A terminal, timeout or
   withdrawal acknowledgement alone does not establish resource release.
   Preserve evidence bytes before removing only this proof's temporary files;
   retain failed cleanup and unresolved scopes. Update the JSON with actual
   artifacts, derive each criterion and the overall verdict by the stated
   rules, and separately assess the parent's three live tests. An incomplete
   attempted proof remains `unknown` unless retained evidence contradicts a
   criterion; an unattempted proof remains `not_run`.

## Open questions

- Which reviewed k2/k3 commits, manifest members, loaded worker attestations and
  deployed guard-load schema/predicate will the operator supply?
- What are the parent's three authoritative live acceptance test definitions?
- What observed CPU IDs, guard calculation, memory demands and bounded payload
  plan will establish the common interval? No host observations exist here.
