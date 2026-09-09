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

To require that refusal path deterministically, add `--stale-claim-read` to
both actors. During the late CLAIMED finish only, the original actor replaces
its own thread's claim-file read with the first claim. The successor's ledger
is still read from the shared filesystem, so production ownership resolution
must refuse the contradiction. The waiter and all other paths keep their real
reads, and the replacement is restored even if the finish raises. The result
records the injected-read count and the production refusal reason; the peer
requires the refusal marker before checking preservation and finishing. An
archived late result fails this mode. This models an inconsistent client view;
it does not induce or explain NFS cache incoherence or a kernel stall.

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
with one native thread per actor. Do not mark the outer shared-state PB actors
retry-safe or reuse their roots. The inner fictional queue item is deliberately
retry-safe with two attempts so the harness can exercise its recovery contract.
Failed namespaces are bounded evidence;
they hold only isolated bookkeeping, not production resource reservations.

Both actors must have successful terminal records and canonical CAS receipts.
Verify exit status, immutable stdout/stderr hashes and lengths, actual JSON
result payloads, scope cleanup, distinct host attribution and source snapshots.
A successful peer alone does not prove that the original waiter completed.
The immediate `waiter.is_alive()` checks are only early diagnostics: a poller
might not yet have observed a wrongly published terminal. The peer’s exact
pre/post state comparisons establish successor preservation; the final waiter
result establishes continuity.
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

## Sparklina path — 2026-09-09 UTC

The unchanged harness from main `10253c42ccc5d8ee8974fcaf5e951f3d2881f5f6`
passed all eight actors on DL380 and Sparklina: late success and late failure
in both directions. This covers the client mount that the preceding run did
not select. The manifest uses `sparklina` instead of `gb10` specifically for
that missing client-path dependency, with `x86` for the other actor. PB owns
placement; every independent actor is a campaign row.

Published runtime remained `650893b19fd0-1788912745-bc21f351ad74`. Aggregate
offered demand was CPU8/mem16 GiB, CPU1/mem2 GiB per actor, priority -10, native
threads 1. All work was CPU-only. PB expanded Sparklina from three to six
announced loops while admitting the campaign. The actors wait on each other;
this is protocol qualification, with no throughput or saturation claim.

| Actor key | Role / host | CAS receipt |
| --- | --- | --- |
| `225d14ec46e0` | original / dl380g10 | `e5d0ddfb6d0b0414433439fc230bf09f838e40adffac7c890877f43fcc0482cd` |
| `6389d181bc75` | peer / sparklina | `49a6e2649c8a3406ba2a18ea69fb3a90e7547f5f9ac50348274ae4aa3344bf52` |
| `89f5b3241172` | original / dl380g10 | `a74f249c0e5d7f8d2e8155669d58e6c30bd0bf91ea6b5eb9cf20a5dda44ee4f3` |
| `bd6cd89fecab` | peer / sparklina | `c75c37f05b3092ade4ca14622f856a34ff107af4f4dd4f1eb03d61af0a19603e` |
| `acc857f7af5b` | original / sparklina | `76e858bed34b7ccd718261e3a89427bc537b5903a9f9ca1ee0cf7d4aa2bf0f79` |
| `9f33e09b5fec` | peer / dl380g10 | `8791d296785505e7620f71625b91f9996849071de3195127feb6254bbb33947f` |
| `d30bcf751e56` | original / sparklina | `a799621a527b651dc06579c8a8068035532393d0a5afde6183e4eebdc81db70f` |
| `d9e4c10e9162` | peer / dl380g10 | `e5f3a84e24cab04f82bffd4f96d8223d06e70eee2fa6c47a36f27bcbd49ab66a` |

Independent verification checked all eight terminal return codes (zero),
immutable stdout/stderr lengths and SHA-256 digests, canonical CAS receipts
and actual JSON payloads, completed outer scope release, and sealed source
bundles. Each bundle contains the unchanged harness, pool and waiter bytes
from the source parent above. All four inner terminal hashes, both immutable
attempts and their logs, distinct host identities and empty final ledgers
were read back independently. Each original waiter returned zero with the
successor’s output. All late calls archived; there were no failed actors in
this campaign and no pytest collection or skips. Earlier negative runs remain
retained in the preceding section.

The coordinator’s first glob of the new namespace returned no entries while
actors were running. A direct server read subsequently found the completed
server-original case, and later explicit client reads and complete verification
succeeded. The empty glob was not taken as evidence of absent work; this
observation does not establish a kernel stall or its cause.

This closes the Sparklina queue-method coverage gap only. Induced kernel NFS
stalls, real broker/Docker cleanup, host-loss uncertainty, execution-budget
behavior during stalls and waiter completion after a late-call
`AmbiguousClaimHolder` refusal remain unqualified. No production runtime code
changed, so no fleet publication is required for this evidence update.

Full keys, campaign manifest, verified receipts and source/result readbacks:
`/home/rob/tmp/pb-234-sparklina/` (`campaign.json`,
`final-campaign-verified.json`, `results-verified.json`). The fresh isolated
namespace is retained as bounded evidence; all four inner queues completed
and their ledgers are empty.

## Forced late-caller refusal — 2026-09-09 UTC

The `--stale-claim-read` mode passed **16 admitted actors**, eight pairs:
late success and failure in both directions between DL380 and each of Sparky
and Sparklina. All eight CLAIMED late calls raised the production
`AmbiguousClaimHolder` contradiction refusal; each recorded one injected claim
read. All eight original waiters subsequently returned the successor result.
Each peer independently verified unchanged successor claim/lease bytes,
reservation and first attempt before completing. All READY late calls archived.
Final readback found two immutable attempts per pair and empty inner ledgers.

Source parent was `da36ec2bb5c6598f0f86de837b0dd305cce885d0`; every sealed
harness matched the code added here, and every `pool.py` and `pbrun.py` matched
that parent. The published runtime remained
`650893b19fd0-1788912745-bc21f351ad74`. Published `pbcampaign.py` supplied all
16 independent CPU-only actors, aggregate CPU16/mem32 GiB, CPU1/mem2 GiB each,
priority -10, native threads 1. The two client tags specifically qualify their
different mount paths. All actors received one preferred CPU and no fallback
CPU. These actors chiefly wait on shared state; this is not a utilization or
performance experiment.

| Actor key | Role / host | CAS receipt |
| --- | --- | --- |
| `002f5a89f8e2` | original / dl380g10 | `08143ea8697bf757e58878f4ce877b9047a35a43355a87a425b1e950e24790c5` |
| `130ea9a6ce9e` | peer / sparky | `8c4df9cdd1c3561be131f86040061da1a86c243143bc98f374e698d180df63c7` |
| `1984bdd259d0` | original / sparklina | `2b2f6d17214a64f33cb7f866dafec0d2955f51c73fd42d75cafb0cd9ca04ea16` |
| `1c812ecc24e8` | peer / dl380g10 | `0c080140ea591384585700c8ae96be93ef5941b049111d5f5724d0da40506075` |
| `2fbd08d9152e` | peer / dl380g10 | `b92862b7c86058663c445377bd0f0bd9aacdf5a9f3bda7fcf7d09b2d5c257a80` |
| `3263604cfefd` | peer / sparky | `c67528338ca815b82d831ea20bf8cd0e0e805457348e6a60163ee11ca2778d8a` |
| `62db0835ed82` | peer / sparklina | `046cede8e33bbf6707650020fba0d2bc2fb4ab7752055f576cde9fb4eddb08a0` |
| `863415d80de1` | peer / dl380g10 | `6878ae48d049c4d41acd64e2dfc931b62b7ff13ae8cb194f3abf4529bfe8b478` |
| `8a41a1938482` | original / dl380g10 | `681ca8f94416110ecb085bc8218f3b019339c5be1d0e5d4c9638eea2626d1ffd` |
| `953db54316c5` | original / dl380g10 | `ff083fd47026aa06f128d2d6c466ca73ace16828d3e6ee0e8917dfb153eb361e` |
| `b29f59b5d130` | original / sparky | `8e3817f8372c39c0fd019b0f7af1d227e09c5c13382f52721d3767e6d511d5f0` |
| `b4c7e8e1b1b3` | original / sparklina | `102032db6335adb7e9abd86827e4afa57af678ba304c26cb090b176e0df634dc` |
| `d5c238e05f44` | original / dl380g10 | `c6bb9437dbda3619870e7fdf0100d019168e957c636a980fe808d729866e8608` |
| `e3f878edf918` | peer / sparklina | `79b903cb484dd8ce947fa47276a2b48e9ac8bf6007bad81c0ad51a0e1985f7a1` |
| `e945f2a9e270` | original / sparky | `48828f6d720c35e86bd19aaf0ad9993720ac340f52b47df5007ab10a056256cf` |
| `f891c9ef8899` | peer / dl380g10 | `e0bc8151a06760457ca6cdf89a10a7e927d378565c32536b0f8808da09bff996` |

Independent verification checked every terminal rc0, immutable stdout/stderr
length and SHA-256, canonical CAS receipt and actual JSON payload, completed
outer scope release, sealed source bundles and command/environment. It also
read back every inner terminal, both immutable attempts and logs, distinct host
identities, empty ledgers and original waiter result. No actors failed in this
campaign; no pytest collection or skips apply. Earlier negative campaigns above
remain retained.

This qualifies refusal-to-completion under a caller-only injected stale read.
It does not reproduce the earlier spontaneous NFS inconsistency or establish
its cause. Induced kernel NFS stalls, real inner broker/Docker cleanup,
host-loss uncertainty and execution-budget behavior during stalls remain
unqualified for #234. No production behavior changed, so fleet publication is
unnecessary: the opt-in tool runs from admitted source snapshots.

Full keys, manifest, source/result verification and resource evidence:
`/home/rob/tmp/pb-234-refusal/` (`campaign.json`,
`final-campaign-verified.json`, `results-verified.json`,
`resources-verified.json`). The isolated namespace is retained as bounded
evidence; all eight inner queues completed and their ledgers are empty.
