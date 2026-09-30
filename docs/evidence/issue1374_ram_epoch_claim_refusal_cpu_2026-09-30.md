# Prior-epoch RAM map refusal at claim — CPU validation (PB #1374)

CPU slice only. No payload, GPU, fleet state, deployment or workload proof.
This record separates source validation (below) from deployment (owed) and
workload proof (owed); it claims neither.

## Finding

`residency_verdict` returned `ram_epoch_stale` when a composed map's
`ram_epoch` was not the epoch the ram tier announced now (#640), but
`RESIDENCY_REFUSAL_STATES` in `src/prismabuild/pool.py` omitted that state.
Both callers of the shared list — the claim pass (`PoolQueue._claim`) and the
ready-GPU row's kept room (`PoolQueue._ready_gpu_row_room`) — therefore
admitted a synthetic prior-epoch or unannounced-tier row instead of refusing
it: the RED `claim()` call returned a claimed record and no residency denial
was filed. Existing coverage called
`residency_verdict` only, never `claim`, so the gate could not catch its own
missing member. The reboot case is the hazard the epoch gate exists for; what
this lane observes is the incorrect admission of synthetic stale and
unannounced-tier maps, not a data read. No payload read, pool fallback,
wrong-byte read or corruption was executed or measured, and no real reboot or
tmpfs epoch move occurred.

## Repair

One production change: add `ram_epoch_stale` to `RESIDENCY_REFUSAL_STATES`,
so claim and room both refuse it like `map_not_composed` — no token, no
aged pass, row stays `READY`, denial `residency_ram_epoch_stale`. Comments on
the constant and the claim pass name the reboot case; `docs/design.md`'s
mount-epoch section states the admission refusal and that an unannounced ram
tier is refused the same way. No scheduler change, no priority/token change.

## RED witness

PB action `3e34b49e69784e827da71d1f35aa7755784dda06dbfc8355d0abb72e7755d0ba`:
one attempt, terminal `failed` rc 1 on dl380g10, snapshot parent
`03374e98c267bda33904be9c329fd0c5da51b683`, **3 failed / 5 passed**,
8 collected/ran, 0 skipped, no reconciliation gaps. CPU-only, shard of one
file, 2 cpu / 4 GiB, one native thread, priority -10, deadline 600 s. Failed
actions publish no successful CAS receipt; the terminal failure and immutable
attempt log are the evidence.

Failing tests (the extended fixture publishes a real consumer and calls the
real `PoolQueue.claim`; the verdict is not monkeypatched):

```text
FAILED test_a_stale_epoch_map_is_refused_at_claim_before_tokens
       assert claim(...) is None  ->  a claimed record was returned
FAILED test_a_map_naming_a_ram_tier_nobody_announced_is_refused
       assert claim(...) is None  ->  a claimed record was returned
FAILED test_the_ready_gpu_row_room_reads_the_ram_epoch
       assert room() is None  ->  {'action_key': ..., 'room': {'cpu': 1}}
```

Positive controls passed in the same run: the same map at the announced epoch
claims with `residency_verdict.state == resident`; a stage-only map claims;
the three pre-existing verdict-only tests pass.

Immutable logs (`.../attempts/3e34b49e.../c3700e.../00000001.*.log`):

- stdout 6741 bytes sha256 `b258b170d32e6266c3359df1b1008fbe66c2263b768f160ff25018bca95b1257`
- stderr 888 bytes sha256 `54f7706ebec4b61f3be4aa1cf98306e4bdbd7d7198231915e5e664368e7c4144`

## GREEN witness

Four CPU shards, one attempt each, all `executed`/exit 0 on dl380g10,
2 cpu / 4 GiB, one native thread per shard, priority -10, deadline 900 s;
13 targeted files (the extended epoch file, residency gate, claim denials,
filed-plan refusal, reserved-range readiness, prior-epoch tier loop, epoch
sweep, ready-GPU room/withhold suite, drain boundary, denial snapshot,
shared ram source):

| shard | action key (prefix) | result | outcome |
| --- | --- | --- | --- |
| 0 | `b2b3fd28a33a` | executed/0 | 42 passed |
| 1 | `59e1ea86f127` | executed/0 | 26 passed |
| 2 | `052ae0054fc1` | executed/0 | 36 passed |
| 3 | `26ce0a295c69` | executed/0 | 64 passed |

**168 passed / 0 failed / 0 skipped**, 168 collected/ran/outcomes, no
reconciliation gaps. All four local-result claims pass `pb_verify_claim`'s
claim/payload/receipt binding and self-consistency checks
(`checks_passed: true`); no full worker-attestation claim is made.
The extended file's 3 previously failing assertions now pass, including the
`residency_ram_epoch_stale` denial, the empty ledger, and `passes == 0`.

Tested source: equality was checked by fetching each of the four sealed CAS
checkout bundles into its own repository and diffing against final HEAD
`6ae67755dfc7`. All four carry `src/prismabuild/pool.py` blob
`3dfe19506299d846c8dae01ea3910dafab29d99b`,
`tests/test_a_prior_epoch_ram_range_is_not_resident.py` blob
`40f9682f47114422dda4e4df8c4295c89ca4f9cb` and `docs/design.md` blob
`05b3359472c5fd4fcaf7170efeecc6a27d1a3194`, byte-identical to HEAD. Each
sealed tree differs from HEAD only by its per-shard
`.pbrun-closure.<stamp>.json` (sealed-tree-only) and by the two evidence
documents added after GREEN.

## Owed

- Deployment: this is a prepared source-merge record — PR #1375 is open for
  parent review and no runtime generation carries the change.
  `deploy: pending`.
- Workload proof: no real reboot/tmpfs epoch move and no live consumer
  admission against a real prior-epoch map; only in-process queue/map
  fixtures. Issue #1374 stays open until root accepts actual resolution.
- Review: parent (Astra) owns merge and final acceptance.
