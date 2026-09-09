# Cross-host claim recovery qualification

`tools/fleet/qualify_claim_recovery.py` checks the #234 queue protocol with two
admitted actors on different hosts. Each pair owns a fresh queue beneath
`/mnt/shared/pb-qualification`; neither actor touches production queue records.
The inner claims are test data and never launch payloads or create broker
scopes. The outer PrismaBuild actions provide actual admission and containment.

The original actor publishes once, claims, starts the normal `pbrun` waiter,
and deliberately stops refreshing its lease. The peer first injects a second
holder in the isolated ledger and verifies that recovery retains the claim,
lease and both reservations. It removes exactly its injected holder, expires
the lease with a one-second test threshold, and requeues through `reap_stale`.
The original reports late while the successor is READY and again after it is
CLAIMED, also attempting an old heartbeat. The peer checks its exact claim,
lease and immutable attempt bytes and its reservation before completing the
successor. The original waiter must return that successor's successful result,
with two verified attempt records and both isolated ledgers empty.

An `AmbiguousClaimHolder` refusal from the late caller is recorded explicitly,
not retried or treated as successful archival. It passes the safety check only
when the peer independently verifies unchanged successor state and the original
waiter subsequently completes. Any other exception fails the actor. A marker
becoming visible in one directory does not establish immediate visibility of
another directory's entries on an NFS client. Therefore the peer supplies the
pre-mutation hashes and verifies the post-mutation state at its source; the
original verifies the eventual terminal after its normal waiter completes.

## Running

Use an isolated Git checkout and a fresh root for each pair. Submit both roles
through the **published** `pbcampaign.py`; do not launch them with SSH or start
another dispatcher. The x86/GB10 constraints ensure different hosts in this
fleet, with PB choosing the eligible GB10 client. Swap the two tags for the
reverse direction. These tags express the cross-host dependency, not a speed
comparison. Both actions are CPU-only.

Example manifest for one late-success pair (replace the checkout and fresh ID):

```json
[
  {
    "cwd": "/path/to/isolated-checkout",
    "argv": ["/usr/bin/python3", "tools/fleet/qualify_claim_recovery.py", "original",
             "--root", "/mnt/shared/pb-qualification/FRESH-ID",
             "--late-status", "executed"],
    "tags": ["x86"], "demand": {"cpu": 1, "mem_gb": 2},
    "priority": -10, "timeout_s": 300,
    "env": {"OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}
  },
  {
    "cwd": "/path/to/isolated-checkout",
    "argv": ["/usr/bin/python3", "tools/fleet/qualify_claim_recovery.py", "peer",
             "--root", "/mnt/shared/pb-qualification/FRESH-ID",
             "--late-status", "executed"],
    "tags": ["gb10"], "demand": {"cpu": 1, "mem_gb": 2},
    "priority": -10, "timeout_s": 300,
    "env": {"OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}
  }
]
```

```sh
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbcampaign.py manifest.json --wait-s 600
```

For complete case coverage, submit all four independent pairs in one manifest:
late success and late failure (`--late-status failed`), in both directions.
Use a distinct fresh root per pair. Aggregate offered demand is CPU8/mem16 GiB,
with one native thread per actor. Do not mark these shared-state actors
retry-safe or reuse their roots. Failed namespaces are bounded evidence;
they hold only isolated bookkeeping, not production resource reservations.

Both actors must have successful terminal records and canonical CAS receipts.
Verify exit status, immutable stdout/stderr hashes and lengths, actual JSON
result payloads, scope cleanup, distinct host attribution and source snapshots.
A successful peer alone does not prove that the original waiter completed.
Retain failed actors and their paired timeouts with the successful evidence.

## Limits

This is queue-method qualification on the real shared mount, with a delayed
caller and an injected ownership contradiction. It does not induce an NFS
kernel stall, stop a production worker, exercise an inner broker scope or
Docker cleanup, test host loss, or qualify execution-budget accounting during
a stall. It makes no performance or saturation claim. Those remaining #234
requirements must not be inferred from a passing campaign.

## Recorded run — 2026-09-09 UTC

Runtime generation `650893b19fd0-1788912745-bc21f351ad74`, source parent
`650893b19fd03bc583de84fef1a04deb32709434`. PB placed the eight CPU-only actors
on DL380 and Sparky. All eight exited zero with verified CAS payloads and
completed outer scope release. All four original waiters returned zero, each
with two intact attempts; every final late finish returned its archived path.
Sparklina was eligible for the GB10 roles but was not selected, so this run
does not qualify that client's mount. No pytest collection or skips apply.

| Actor key | Role / host | CAS receipt |
| --- | --- | --- |
| `3ecd7d008ae2` | original / dl380g10 | `168d824abb9d8838ee3d8177db3c2e50b1af7affdc817ed0be87c288d5b9c113` |
| `e5e4fd2294aa` | peer / sparky | `4e2fc73a407645fbdc57d89161280731aa68c0f5b1253cf487851c940985b9a3` |
| `089e4d48c9e9` | original / dl380g10 | `ae47f4d07d7b46f5eb3c011745215a3a05131a6ccacdaca48ae58fbd2d183680` |
| `1a48cb766f3e` | peer / sparky | `02e2715806582fadb3bbd25cfab9487349704bf6e5460ea6b1f359647fa01365` |
| `b49c2d27633e` | original / sparky | `17c1563a252e3940e35d1cfa3b7e2208898ae8a176a6a90206deb17e30e2ba0f` |
| `f227e0525919` | peer / dl380g10 | `17931a0fa1b0c710f2392b12befb59ae08d0440f65614410b0d3e04d963f7d03` |
| `7264e4e1a418` | original / sparky | `8ad4543a0953ce3128337bdfe155aa70b9bd6223783644dec3b0937d2dcad7b7` |
| `c6fc0d5dba65` | peer / dl380g10 | `49fff40e2406a4ffd8e6a0afc88401972af26bfe432ba00d4a1145dd182ff992` |

Verification independently read every terminal exit, immutable log length and
SHA-256, canonical CAS receipt and actual result payload, completed scope
release, and the source bundle bytes. Every actor ran the checked-in harness
bytes against unchanged `pool.py` and `pbrun.py` from the source parent above.
The final inner terminal hashes, attempt log hashes, distinct original and
successor hosts, and empty ledgers were also independently read back.

Two earlier iterations are retained as negative evidence:

- `cebecae2dce3` exited 1 when the original client read an archived attempt
  immediately after seeing a marker in another directory: `ENOENT`. The
  server already held the file. Paired `4cb77b25d61a` then timed out. The
  harness now gets the pre-mutation digest from its producer and lets that
  producer verify unchanged bytes after the late call. It does not interpret
  marker visibility as a cross-directory consistency barrier.
- `766296ce9a31` exited 1 when a late CLAIMED finish raised
  `AmbiguousClaimHolder`: the old client's claim view and successor ledger
  disagreed. Paired `fe6cf821580f` timed out because the earlier harness aborted
  on the refusal. Production's worker loop catches this exception. The final
  harness records this exact refusal as a distinct disposition and requires
  the peer's preservation checks and the waiter's eventual completion; it
  neither retries the finish nor converts refusal into archival. All late
  finishes in the final run archived, so the final receipts do not qualify
  waiter completion following that specific refusal.

Both earlier server-original pairs completed. These observations do not
establish a lost reservation, overwritten successor or root cause for the
client's inconsistent view. They constrain the qualification claim and remain
available alongside the passing campaign. No production recovery behavior was
changed to obtain these results.

Full keys, manifests, source verification, payloads and retained failures:
`/home/rob/tmp/pb-234-followthrough/` (`final-campaign-verified.json`,
`results-verified.json`, `first-campaign-verified.json`,
`second-campaign-verified.json`). The three isolated shared namespaces are
retained as bounded evidence, including the interrupted inner test queues.
