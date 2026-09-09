# Cross-host claim recovery qualification

`tools/fleet/qualify_claim_recovery.py` checks the #234 queue protocol with two
admitted actors on different hosts by default, or on one host with
`--same-host`. Each pair owns a fresh queue beneath
`/mnt/shared/pb-qualification`; neither actor touches production queue records.
By default the inner claims are test data with no payload or broker scope.
The outer PrismaBuild actions provide actual admission and containment.
The optional `--real-scope` mode below adds bounded direct payloads to the
isolated claims; adding `--docker-image` also exercises a sleeping container
in each scope. Teardown stops only scopes that still exist, preserving the
production termination audit after successful recovery. It still reaps its
payload proxies and asks the broker to confirm release.

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
kernel stall, stop a production worker, test host loss, or qualify execution
budgets during a stall. Inner broker cleanup is exercised only when
`--real-scope` is requested, and Docker cleanup only with `--docker-image`.
These modes make no performance or saturation claim. The remaining #234 requirements must not
be inferred from a passing campaign.

## Real direct scopes

Add `--real-scope` to both roles to exercise cleanup through the installed
broker. Each actor creates a disposable 128 MiB scope using the established
resource-scope qualification API, attaches its exact control record to the
isolated claim, and launches a sleeping Python parent and descendant. The
descendant ignores SIGTERM; the broker must stop the whole scope. Startup
checks both PIDs' scope membership and the inherited PB CPU affinity.
This is not a test of production scope creation/preflight or normal launcher
result collection. The successor's successful queue result is supplied by
the harness, as in the data-only mode.

The peer first verifies the existing contradictory-holder refusal. After
removing its injected ledger entry, it tries the expired claim again: the
real scope belongs to another host, so recovery must retain the claim, lease
and original reservation, without publishing READY or an attempt outcome.
The original actor then injects one failure of its exact scope's local
`terminate_owned` call. It verifies the same retained state and that the real
payload is still alive; the peer independently checks the retained state.
This is caller-local fault injection, not an outage of the installed broker.

With that injection removed, the owning host reaps through unchanged queue
code. Before allowing the peer to claim, it checks the launcher's nonzero
exit, disappearance of the kernel group, the broker's released status, and
the first immutable attempt's matching telemetry nonce. The peer creates a
different real scope for the successor. That payload must survive the late
old-owner calls; normal successor finish must then retire only its own scope.
The original waiter still has to return the successor result, with two
immutable attempts and empty inner ledgers. `--stale-claim-read` can be
combined with this mode to require the late CLAIMED ownership refusal too.

Reserve CPU1/mem2 GiB per actor, with one native thread. The additional inner
scope is a broker-created sibling of the outer scope, as in
`qualify_resource_scope.py`; its bounded demand is included in the outer
reservation, not a second admission. Payloads inherit the assigned CPU mask
and disabled GPU visibility. Both are bounded to 600 seconds if the actor
dies; normal and exceptional exits terminate and release their exact scopes.
No loose process-name matching or production queue mutation is used. Use an outer execution budget of 600 seconds; the original waiter has a
480-second test budget in this mode (120 seconds in data-only mode). These
test bounds allow the extra real cleanup and shared-mount handshakes; they
do not change production execution budgets. The same fresh-root, no-retry
and receipt-verification requirements apply.

A foreign host's refusal is the expected safe result while it cannot prove
cleanup. Completion here depends on the original host remaining available
to clean its scope. It does not qualify permanent host loss, reboot recovery,
an induced kernel NFS stall, Docker cleanup, or execution-budget accounting
during a stall.

### Docker scopes

Add `--real-scope --docker-image ubuntu:24.04` to both actors to put a sleeping
container beside each direct parent/descendant pair. The image must already
be cached on every eligible host and provide `/bin/sh` and `sleep`. The shim
uses `--pull never`, disabled networking and no privileged or GPU access.
The actor records the actual image ID, exact container ID and scope label,
checks its 128 MiB memory/swap caps and verifies the container PID belongs to
the scope with the actor's inherited PB CPU affinity. The container and direct
payloads share that 128 MiB parent; CPU1/mem2 GiB per actor remains sufficient.
The single inner launcher invokes the ordinary shim from within its scope.

The original container must remain running after the injected cleanup refusal,
then disappear through production cleanup before READY is published. The
successor container must survive the late original calls and disappear on
normal successor finish. These checks occur before harness teardown, so its
fallback removal cannot make production cleanup pass. Exceptional teardown
verifies the exact scope, action label, cgroup parent and stopped state before
removing any remaining container object. A daemon error never proves absence.
All container and direct payload lifetimes remain bounded to 600 seconds.

The mode still depends on both hosts being available and injects no delayed
Docker daemon RPC, host loss or kernel stall. Cross-host retries do not qualify
same-host overlapping attempts. The first Docker campaign below landed only on
DL380 and Sparky; the later Sparklina campaign records that client's path separately.

### Same-host delayed callers

Add `--same-host` to both actors and use the same hostname tag for the pair.
PB still submits, admits and places each actor. The tag is a qualification
requirement: a pair landing on different hosts fails this mode. Use fresh
namespaces for each host, late status and stale-read setting.

The peer injects contradictory bookkeeping into a separate fictional host
ledger so it never releases the original's real inner ledger by mistake.
It skips the inapplicable foreign-host cleanup refusal, and still checks the
injected local cleanup refusal and exact original cleanup before retry. The
old caller remains alive while the successor's scope and container run on the
same host. Both ordinary reads and `--stale-claim-read` must preserve that
successor and complete the original waiter. The stale mode requires an explicit
ownership refusal. Available claim/lease identity fields are compared before
ordinary finish cleanup; a shared host name alone cannot distinguish attempts.

This does not run two payload attempts simultaneously: the original scope is
retired before the successor launches. It does not qualify delayed Docker
creation RPCs or jointly stale claim and lease observations.

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

## Real direct-scope recovery — 2026-09-09 UTC

The `--real-scope --stale-claim-read` campaign passed **eight actors**, four
pairs covering late success and failure in both directions between DL380 and
Sparky. All four foreign cleanup attempts retained the original claim and
reservation; all four caller-local injected broker failures did the same while
the payload remained alive. The owning host then released its exact scope
before the peer claimed. All four successor scopes survived the late calls,
and all four original waiters returned the successor result. Every case has
two immutable attempts and empty final inner ledgers. All READY late calls
archived; all CLAIMED late calls raised the required ownership refusal.

Source parent was `ebfbe7de98ae80b151eeb09223eb93246eed5903`. Independently
verified each terminal rc0, immutable log lengths/hashes, canonical CAS result
payload, completed outer scope release, and sealed harness bytes. The pool,
waiter, resource-scope client and proxy bytes match that parent. Inner
terminal/history readback verified the matching cleanup nonce and broker
release evidence. Each payload inherited its actor's one assigned preferred
CPU; none used fallback CPUs. Each inner scope was capped at 128 MiB.

Published pbcampaign offered CPU8/mem16 GiB aggregate, CPU1/mem2 GiB per actor,
priority -10, native threads 1, outer timeout 600 seconds. No GPU work, pytest
collection or skips apply. This is protocol qualification; its predominantly
waiting actors provide no performance or saturation claim. The installed
broker file on both hosts had SHA-256
`62eeeb05c274c979b49c20b9a4c54adfb89796fe3d537847d201c51227db70df`;
PID/start-time and entrypoint reads identify the serving processes.

| Actor key | Role / host | CAS receipt |
| --- | --- | --- |
| `4454400d7229` | original / dl380g10 | `81d4735d6f0b41a824b4777b6a3cc28aec596246bf9417bd5232d239fc0602df` |
| `f5129c4e0961` | peer / sparky | `ead33024e11f2de368d606b2b93f3841e5bdc435c1037ae5c766925c792a2572` |
| `c4766268d187` | original / dl380g10 | `6b97e4c0391d4cddb62f36f9ce52a619942a2dec7613c7b52760a17ee1724dd0` |
| `82b62d064f9f` | peer / sparky | `975f850388800d90c3ae1710cc8173c8c1978c29a6c525846963265a1633a423` |
| `77326a2969b6` | original / sparky | `f14bedb36a4652d5dceed71c542e9dbcf92708bc1b1da465dd1157ad5f1f0ef5` |
| `d2355230baba` | peer / dl380g10 | `932fa9380c178b1b233181212ee0f75fabcbfb7d8931a1f483da9a0dd94ecf09` |
| `0c74afee65ba` | original / sparky | `b3565953279d68df9fc35b839bcf41140df5b685431eabcb1ad8651050ce21ed` |
| `7b98dbbb4fd1` | peer / dl380g10 | `4ae0146a4b79dd87a7f1d070a8b06b20a9e7b74be759d7e0d0adbd8a8e08265d` |

The default data-only mode also passed an admitted two-actor pair on DL380
and Sparky, with the same final harness bytes: `1172e39bc3ff` /
`4a2f063b4466`, receipts
`d71d12e9434fc84ca97a7730db2a73bb265f002e9ff980b8a0ae4091b538a08e` /
`8cf283ac0ed04a4f01a540b557eb761e65bdcb4d84e0c184c02dd092f1565b49`.
Both logs, CAS payloads, source bundles, scope releases, waiter result, inner
history and empty ledgers were independently checked.

The first real-scope campaign did not pass: twelve actors failed and four
unstarted actors were withdrawn after their partners failed. These failures
are retained, not replaced by the final results:

- `80a27aed3c9f` and `0d3bcdf3c332` read READY immediately after a marker in
  another NFS directory and received ENOENT. The corrected harness waits for
  the exact READY and attempt records before reading their bytes.
- `247e81fe0d78` and `9649cf192fd4` exhausted the old 120-second waiter while
  the additional real-scope/shared-mount protocol was still running. The
  real-scope waiter is now 480 seconds; data-only mode keeps 120 seconds.
  Other paired actors timed out waiting for their failed partners.
- Sparklina's four actors never started. Its logs and offers showed external
  GPU/memory load reducing the PB memory budget to zero, alongside admission
  lock-busy diagnostics. No per-candidate denial was recorded. Their partners
  timed out; maintenance withdrew only the four exact unstarted keys and did
  not interrupt the external workload or retry that unchanged condition.
- `aee04e52fc04` also received `scope still populated` during exceptional
  cleanup. Teardown now reaps its proxy and retries that exact broker refusal
  for up to five seconds. All eight scopes created by the failed campaign
  were subsequently proved released and absent by admitted cleanup actions
  `88415d213ef7` and `d99545655a7a`, with verified receipts
  `3a48364386763b5252b369db9e017d4c6ac598fd3c57f7a2e250a3e87fe14e2e` and
  `29dc0f7e2755d564395f48331fa777491084637d1786d1a6e67cde324c37386a`.
  The failed inner queue records remain isolated diagnostic evidence; they
  are not live fleet reservations.

That campaign qualified real direct-scope cleanup on DL380 and Sparky, with
the owning host available. The follow-up below qualifies Sparklina after its
external load cleared. Docker cleanup, permanent host loss/reboot, induced
NFS kernel stalls and execution budgets during stalls remain unqualified. Production recovery behavior is unchanged; no
fleet publication is needed for this opt-in source-snapshot harness. Runtime
`650893b19fd0-1788912745-bc21f351ad74` remained deployed with all 417 manifest
hashes verified.

Full keys, manifests, failed results, cleanup receipts, source verification
and inner-state readback: `/home/rob/tmp/pb-234-real-scope/`
(`final-campaign-verified.json`, `results-verified.json`,
`resources-verified.json`, `first-campaign-verified.json`,
`cleanup-verified.json`, `default-results-verified.json`). Final and failed
namespaces are retained as bounded evidence; all created real scopes retired.

### Sparklina real-scope follow-up

The complete 01:11:52 UTC census showed that Sparklina's external load had
cleared: no foreign GPU process, 116 GiB host memory available and its full
104 GiB PB memory budget. This changed the preceding admission blocker.
An additional eight actors then passed the same real-scope/refusal cases in
both directions against DL380, using the unchanged harness from PR head
`d3ffd3d8d0cb6b1e661cacef628b3cf8bfe3a20e`. No external process was interrupted.

All four original waiters returned zero with two immutable attempts. Every
foreign cleanup refusal and caller-local broker failure retained the claim
and reservation; real cleanup released the old scope before retry; each
successor survived late calls and subsequently released its own scope. READY
late calls archived, CLAIMED late calls produced the forced ownership refusal,
and all inner ledgers ended empty. Each terminal, immutable log, CAS payload,
source snapshot, cleanup nonce, demand, native-thread bound and payload CPU
affinity was independently verified. All actors used one preferred CPU,
CPU1/mem2 GiB per actor, aggregate CPU8/mem16 GiB, priority -10. No actor failed
in this follow-up and no pytest collection or skips apply.

| Actor key | Role / host | CAS receipt |
| --- | --- | --- |
| `4367b14404ca` | original / dl380g10 | `1d9f049e581fb2db6c1f2d8a7d1c1242375042f663f7f93708092a0c8495ff7d` |
| `1d58ca3792b4` | peer / sparklina | `bf7d64c00d37ac91e4026e5ab116df4f5c5e75a6d434987ac33732ab73f5e4c2` |
| `cfc9c7dcfc49` | original / dl380g10 | `a245a7855ae01a5c2341de4a18966b8fdcb73b53a5b1b843edf6dc0c7bec9057` |
| `128fe454fb71` | peer / sparklina | `412243878ff958c949abba2517f80285b764e0d5775c9309eef7f1a7d34ae6e6` |
| `48278bc45ccd` | original / sparklina | `5c2c40c5d76e9a5e867fc0f9ffb6de81f76eb3777b7ae5d41773d114c5d3120c` |
| `3a9c646f6fba` | peer / dl380g10 | `4e31f5b4dfa16c151c948d63f4218435cd83bba3544ddf81b5cffdcb7d65a38b` |
| `26cd3f5b991e` | original / sparklina | `eab0840606b32919add40423322c8db94b198b74efb53f440054d508ec19a06f` |
| `8403fac08c2d` | peer / dl380g10 | `6341d8d3b8497a31cf4ae40b64cdce128817adffd70475f3455b408bff0e482a` |

This brings the final qualification to **16 real-scope actors and two default
actors passed**, plus two verified cleanup actions for the failed first run.
Sparklina's temporary admission blocker is cleared; the first run's failures
and four withdrawals remain retained. Docker cleanup, host loss/reboot,
induced kernel NFS stalls and execution-budget behavior during stalls remain
outside the qualified scope, so #234 stays open. No production runtime change
or publication is required.

Additional evidence: `sparklina-campaign-verified.json`,
`sparklina-results-verified.json`, `sparklina-resources-verified.json`,
`sparklina-campaign.json` and `sparklina-submission.json` in the same evidence
directory. The additional completed namespace is retained with empty ledgers.

## Termination audit preservation — 2026-09-09 UTC

Review of #410 found that harness teardown stopped an already released scope,
rewriting its production termination audit with `disposable claim qualification
cleanup`. Retained original files confirmed the mismatch: their outer `reason`
was the harness reason while the broker's `stop_reason` still named `lease_lost`
or `executed`. These older records remain unchanged as bounded evidence.

Teardown now skips that stop when the exact local cgroup is absent, while
retaining proxy reaping and the broker release check. Regression against
unchanged main `6d9eb471de80` produced **two expected failures and one pass**
(action `cab1e05dc51d`, rc1, no success receipt). It exercises the actual
callback and audit writer with a simulated broker; interrupted-setup cleanup
remains covered. Fixed-code targeted validation passed **26 tests**, no skips,
with two pytest workers (action `83548150c21d`, receipt
`0cd9dac1958b939aa6d56de351f1018f2695b66d347d78a41e0268afdbfbefb1`).
Two earlier regression submissions failed collection due to a missing tool
import path (`06a98d8e8d45`) and missing parametrized function argument
(`1c9c4b5fb262`); neither is counted as reproducing the production defect.

Four real-scope actors then passed two DL380-original/Sparky-peer cases, one
late success and one late failure, using the fixed harness. Both original
waiters returned the successor result, each with two immutable attempts and
empty inner ledgers. After all actors exited and teardown completed, direct
readback verified all four final termination audits retained the production
reason, matching the broker's stop reason: `lease_lost` for the originals and
`executed` for the successors. Exact host cgroup readbacks found all four
scopes absent. No actor failed in this campaign.

| Actor key | Role / host | CAS receipt |
| --- | --- | --- |
| `7254ed067732` | original / dl380g10 | `fb51d7b02a89ed680f7a5c89d2f0c76e697bc8c72901d7e135bd70141785ef74` |
| `07618c67afdc` | peer / sparky | `d992a4ae78e2bdae07976097b901a8c584b16d3d0d04a6593bed048dbc3618ad` |
| `70362710375f` | original / dl380g10 | `61dc8fe3caf043bd82de4eb49116079312792d1306f30edabc897035c71a8f73` |
| `e4c76a22f6bc` | peer / sparky | `e9d1a35a4645762cc0e75fe4c6955922d1bf82e32f4fc3652e56ae7859b8c2be` |

Every terminal exit, immutable log size/hash, canonical CAS receipt/result
payload, completed outer scope release and sealed harness/test/production
source bytes was independently checked. Published PB admitted every action,
priority -10, native threads 1. The portable targeted suite requested CPU2 /
mem3 GiB; the campaign offered CPU4 / mem8 GiB total, CPU1 / mem2 GiB per
actor. Class constraints express the distinct-host protocol dependency.
Each real payload inherited its actor's single preferred CPU and 128 MiB
inner memory cap. These waiting actors provide no performance claim.

No production recovery contract changed and no fleet publication is needed
for this opt-in source-snapshot harness. Docker cleanup, permanent host loss,
induced kernel NFS stalls and execution budgets during stalls remain outside
this qualification; #234 stays open. All 417 deployed runtime manifest hashes
were verified for generation `650893b19fd0-1788912745-bc21f351ad74`.

Evidence is retained at `/home/rob/tmp/pb-234-cleanup-evidence/`:
`verified-receipts.json`, `campaign-verified.json`, `results-verified.json`,
`scope-absence.json`, `prior-audit-readback.json`, `campaign.json` and
`runtime-verified.json`. Both new isolated queues are complete with empty
ledgers; the prior corrupted audit files have not been rewritten.

## Docker cleanup qualification — 2026-09-09 UTC

Eight CPU-only actors passed four pairs: late success and failure, in both
directions between DL380 and Sparky. All four original waiters returned the
successor result with two immutable attempts and empty inner ledgers. Foreign
cleanup and the caller-local injected broker failure retained the claim and
reservation. Original containers remained alive through the injected failure,
then production cleanup removed them before retry. Successor containers
survived every forced late-caller refusal and were removed on normal finish.

All eight exact cgroups and container IDs were independently read back on
their owning hosts after teardown: absent. All eight production termination
audits still retain `lease_lost` for originals and `executed` for successors,
matching their broker stop reasons. No actor failed or was skipped.

| Actor key | Role / host | CAS receipt |
| --- | --- | --- |
| `7c2dc514fa9a` | original / dl380g10 | `4ff70f42a80cb0951b1c3bf519870cc5406d9e70bf6bd0e7049adcbef792831f` |
| `1dc96877c37e` | peer / sparky | `b1f6884e8e092f548af58cc44e925a70beca74f642605b3d6c4b96b3ccd50a63` |
| `d36ba988909f` | original / dl380g10 | `d38461bbfb8138410c7453b8a157af286ba8b1b03d213a31acc28295d394636a` |
| `bbf93769b4e9` | peer / sparky | `8148eb43bf68f99919aa01f170ac63dd9932c5f41e5a0c53bdc8b8cb55e59b13` |
| `ccc0dd043e14` | original / sparky | `46da7d3535bb87160fd9c0cde221615bcd0824686f01d758bd4d9c0b4146777a` |
| `81be99f5b4de` | peer / dl380g10 | `0bf2ee9cd7ed84a262b55bcd4167a88b065d55bc2314c8032b1411607214227e` |
| `e71a50d8500b` | original / sparky | `34e2498fb12da0de3492b23a0702df3f97f22742b20a9844e792e0a0aee7a97b` |
| `b9fceec55a94` | peer / dl380g10 | `aac246687f613f3f005bde629b6dcf1eb24fa7e4419004b590e4be6a02102312` |

Published PB admitted the campaign with CPU8/mem16 GiB aggregate, CPU1/mem2 GiB
per actor, priority -10 and native threads 1. PB selected the hosts using
`x86`/`gb10` constraints; Sparklina was eligible but not selected. Each actor
received one preferred CPU, inherited by its container. The checked local
`ubuntu:24.04` images were
`sha256:a6f81fb630d51837271b89f8193810a5fc493fa4f30a55d7ebcdb3a66f3cc63a`
on DL380 and
`sha256:33ceb71981b602c1a7443a53469e4dba065f7503eab3078a2d7a57a2ab987517`
on Sparky. No image was pulled, no GPU executed and no performance claim made.

Final targeted validation passed **17 tests**, no skips, on Sparky with
CPU2/mem3 GiB, two pytest workers and native threads 1: action
`37ead80c87b485c251b46ff9db3392e6618cb5ddaaecb80a4064b01611114eb2`, receipt
`22d18a6b580f220cd5d05ce72b7f62100a55e0fe3cbb427855485eeae9f61c9e`.
This includes refusal to remove containers with mismatched ownership or a
running state, and refusal to treat a Docker daemon error as absence. An
earlier 12-test run passed before those five additional safety checks.
This extends qualification coverage; no production defect was found or fixed.

Verification read terminal exits, immutable log lengths/SHA-256, canonical
CAS receipts and actual JSON payloads, completed outer scope releases, sealed
harness/test/production source bytes, environments and affinity. Each campaign
snapshot uses unchanged production recovery code from main `033f007667b4`.
This opt-in source-snapshot harness needs no runtime publication. The fleet
remained on `650893b19fd0-1788912745-bc21f351ad74`; all 417 hashes were verified.

The qualification does not cover Sparklina Docker recovery, overlapping
same-host attempts, delayed Docker creation RPCs, permanent host loss/reboot,
induced kernel NFS stalls, stall-budget accounting, production scope
creation/preflight or normal execution-result collection. #234 stays open.

Evidence: `/home/rob/tmp/pb-due-234-266-413/` (`campaign.json`,
`campaign-verified.json`, `actor-payloads-verified.json`, `source-verified.json`,
`inner-state-verified.json`, `scope-absence.json`, `targeted-verified.json`).
The four completed queues under
`/mnt/shared/pb-qualification/docker-recovery-975ea7aa9092` are retained as
bounded evidence; their ledgers are empty and every created scope is retired.

## Sparklina Docker recovery — 2026-09-09 UTC

The unchanged merged harness at source parent `9bccad522ac44e5c02a191f7ad43fcc317b53a1d`
passed all eight CPU-only actors between DL380 and Sparklina: late success and
late failure in both directions, with `--real-scope --docker-image ubuntu:24.04`
and `--stale-claim-read`. The Sparklina tag selected the previously unqualified
Docker/host path; the server actors used `x86`. PB admitted all placements.
Runtime `9bccad522ac4-1788920576-399e6fcd5272` had all 419 manifest hashes verified
and 24 directly observed worker loops across the three fleet hosts.

Foreign-host and injected local cleanup refusals retained the original claim
and reservation. The original container stayed alive through refusal, then
production cleanup removed it before retry. Each successor container survived
late original-owner calls and was removed by normal finish. All four original
waiters returned the successor result; the four inner queues retained two
immutable attempts and ended with empty ledgers. Owner-host readback found all
eight exact scopes and containers absent. All eight termination audits retained
production's `lease_lost`/`executed` reason and matching broker stop reason.

Verification covered terminal exits, immutable stdout/stderr sizes and SHA-256,
canonical CAS receipts and result payloads, source snapshot bytes, outer scope
release, image IDs, memory caps, native threads and inherited preferred CPU
affinity. No actor failed or skipped; pytest collection does not apply. The
campaign reserved CPU8/mem16 GiB aggregate, CPU1/mem2 GiB per actor, priority -10
and native threads 1. Each inner scope shared its 128 MiB cap with its container.

| Actor key | Role / host | CAS receipt |
| --- | --- | --- |
| `29bb2f486c36` | original / dl380g10 | `838e02b1e9bae0c845dd7975f2dc97d39932ee87c0ef0b5075d57433ba53d92a` |
| `55556765a060` | peer / sparklina | `296137ab1bf8231956342feb2101dd50b49243d6a18be6e8c7072bc67b0df415` |
| `74b5fa5c1f19` | original / dl380g10 | `10eb6494fbeeb3e7eded890cd3033c406325cdc80f396de63825fd95005a37b0` |
| `c6d9b73befeb` | peer / sparklina | `b4a2b61f248820cbf001de09459d2400e8bc7727500b683a6cd0c59435741d09` |
| `a7a4c15b477e` | original / sparklina | `6cfb95b4b6b0c1b6ad3b065efd4709cefbc239a9c7252e109f0c600b3553092f` |
| `7c4b9f8b7924` | peer / dl380g10 | `44a9fb7e225699962063c6f7e49937d09739bb02e72d5d23ad6bdbc7dcd67c7e` |
| `052d76539ccf` | original / sparklina | `d698ccfb88d0bb082482df84db7b95afd7be5d8054bdcf95faa12325238381b3` |
| `9221496a906f` | peer / dl380g10 | `956edd5bb7d342bec14468872c7dff0d169355f6feeda4a60680b56ba2d4cb36` |

Evidence is retained under `/home/rob/tmp/pb-due-maintenance-20260909/`
(`campaign.json`, `campaign-verified.json`, `actor-payloads-verified.json`,
`source-verified.json`, `inner-state-verified.json`, `scope-absence.json`).
The four completed isolated namespaces under
`/mnt/shared/pb-qualification/sparklina-docker-8e30726fa6b9/` are retained as
bounded evidence with empty ledgers. This closes the recorded Sparklina Docker
qualification gap; it found no production defect. No runtime publication is
needed for these evidence-only documentation changes.

The owning host remained available and the harness supplied the successor's
queue result. Same-host overlap, delayed Docker RPCs, permanent host loss/reboot,
induced kernel NFS stalls, stall-budget accounting and normal production
startup/result collection remain unqualified. This is not a GPU performance
measurement or a resolution of the remaining #234 requirements.

## Same-host stale-claim recovery — 2026-09-09 UTC

Qualification on baseline main `3489b2ca8` exposed a production defect: an
injected old claim could agree with the caller and the host-level ledger while
the actual lease named the same-host successor. Ordinary finish reached
action-wide Docker cleanup, removed the successor container, overwrote its
claim and returned it to READY. All six stale-read pairs failed across DL380,
Sparky and Sparklina; the six ordinary-read pairs passed. The paired failures
include handshake timeouts after the original assertion failed. Independent
owner-host readback confirmed both exact scopes and all action-labelled
containers absent after exceptional teardown. Failed inner READY records remain
isolated evidence; they are never submitted to the production queue.

Four separate PB unit regressions failed at the action-wide cleanup trap on
unchanged production source, including same-owner attempts distinguished by
claim time. Finish now refuses contradictory available claim/lease owner, host,
claim-time or publication evidence before any cleanup or mutation. Missing
legacy fields add no proof. This is a caller-local inconsistent-read injection,
not an induced kernel NFS stall or a guarantee for jointly stale evidence.

Fixed validation: **56 targeted and 178 integration tests passed, no skips**.
Integration covered finish/reaper races, scope creation and cleanup, unstarted
claims, immutable history and waiter behavior; CPU12/mem16 GiB aggregate, twelve
pytest workers, portable placement, DL380 execution. Receipt:
`8ead05c3d8e563b1299cef8c1205c101d324dd494796b77193bae04f457d9dd5`.

**28 fixed actors passed**: late success and failure with ordinary and forced
stale reads on each of the three hosts (24 actors), plus cross-host forced-read
compatibility between DL380 and a PB-selected GB10 (four actors, Sparky selected).
All 14 original waiters returned successor results with two immutable attempts
and empty ledgers. Local cleanup refusals retained live original containers;
production cleanup removed them before retry. Successor containers survived
late calls and disappeared on normal finish. All 28 exact scopes and container
IDs were independently absent on their owning hosts. Same-host audit sidecars
name the final successor; original recovery telemetry remains in immutable
attempt 1 and late-cleanup evidence is separated by nonce.

CPU1/mem2 GiB per actor, aggregate offered CPU28/mem56 GiB, native threads 1,
priority -10, inherited PB preferred CPU affinity, no GPU payload. Same-host
pairs require matching hostname tags; cross-host pairs require x86/GB10. PB owns
placement and admission. Each inner scope shares its 128 MiB cap with its Docker
container. Checked terminal exits, immutable logs and lengths, canonical CAS
receipts/payloads, sealed source bytes, scope releases, image IDs and CPU masks.

| Actor key | Role / host | CAS receipt |
| --- | --- | --- |
| `e345c571a80e` | original / dl380g10 | `97d3a1447b2c470abb660779625dbd1dca4dcf094d1b0def76ddcca4d7c28058` |
| `f7a47264fb7a` | peer / dl380g10 | `9050763270d7e0b44abbe9cdf3769b02d7c32b418db754e2fc7d8646bea74b87` |
| `ed895b6ed32f` | original / dl380g10 | `6989b58c29fdc539d8288072281f1ce4796c225030c03a6fcff394b7d24f2d85` |
| `f5abea62ab52` | peer / dl380g10 | `1262fff65321bd3016721e0c16c5b5d69cb3532f3519050542a1ccad5675e79e` |
| `10d9016591a6` | original / dl380g10 | `3ae429458ddd1eaba0137120c3047d0f824041da0e31b4b59de369a29f3ad8af` |
| `36c698df5d0d` | peer / dl380g10 | `661ca78a87bf4d5e4fc7c03302331907154d8cc0cd2134780d7bdb72c7c437ac` |
| `60e78276cc4a` | original / dl380g10 | `6f8644d2a6e0479b5f7e227077bc289ad021fbf34033c0dc2bc9a97d353ffa89` |
| `f9f0cc6158f8` | peer / dl380g10 | `f31fc8ce7668b8c5d4e41ee625d1c99599e1e023f28a7834f6f4190448a158f4` |
| `39d0677d6e29` | original / sparky | `f294b5a35f415d7c8fc2f5c4776436c56607f12e3f3c94097b50a13ba1591d68` |
| `cec1a3f3af69` | peer / sparky | `f7b64f7b15fe3434e547cf670c57d25740d8df130d6cf6c48292e60ac30413a7` |
| `068ba45241ae` | original / sparky | `6192af3487e9d1cd86f08ff61d7f2e2934599f20587d5635590ff52e0c8103f5` |
| `f269fc30a427` | peer / sparky | `c7776edeb0faf7a2ccfe97403adee1ed007c68dde6de0ca26ce094df026332bd` |
| `9ef78aa17c5e` | original / sparky | `151e9f8a57b444159e34492bb7a26dae8cb045917977fcfa341d95dfd4109d2d` |
| `9d9954619e28` | peer / sparky | `b64b139fc5108edb5d9d68a231aa19bfd6478ca0a980ab7386fe8e2170847b1b` |
| `8323571a4cb8` | original / sparky | `d32d2be9411387ef4d138ad95607d1d337ef93e27fd641799b4f03e1ac02786e` |
| `ac1529a6a2d4` | peer / sparky | `02072404b5197cca325bf55da092dab990ff721ed2dc24e465f5dc366e1bab2d` |
| `911a6f526485` | original / sparklina | `9930efb1faccce2a859a6e8e9ced2a057233ca46f59804cc58dbf103d6f1acc1` |
| `ffde038c4fbe` | peer / sparklina | `f8956db51bc7d4e8a271afd136d6f4832ee09bc1dce778183b375e36562bd06b` |
| `d1e9f4662b24` | original / sparklina | `40f558fae8062b0ee0a014d9cb83436541579401dca36945a0bda85326792120` |
| `ab7d95c215e2` | peer / sparklina | `5be78ccc11f2b508126ead31e3622c2438cd3406560c3e3faeea67eb63317384` |
| `ac008ad37fa5` | original / sparklina | `160e79fcf566fe294f73f5312600fce631d308806c9a3bef00cc80c53b2aa8b0` |
| `c1ffd072039f` | peer / sparklina | `64e91b05a5d1c4e3f0699336abf478fc651656a6e700ebda7cbb6970a6d930d4` |
| `7f0ca9810996` | original / sparklina | `52447bf4eb7abb96f5a3b874777dec6659d9179ab07334f192bbaaa10485a6d5` |
| `cccd003cbe0b` | peer / sparklina | `7d3c80c46fad3e727e6754f2d32f3e5ad99f60efcd0f227e75bbe01036d0fd7e` |
| `d4240772322a` | original / dl380g10 | `7dba8fe5e6b9c02f2beb5f6af7266fb6783e6c7717512904845fe37935255d1d` |
| `4dbf22c6b338` | peer / sparky | `359c896139b6c5e46ffb2ab6f14976cffe2794b27d4c8921907f6ff234d9d595` |
| `005cdbed7dd7` | original / dl380g10 | `a0e1fb111e1e74f033abff1e03b0d62ce08400de75f28385e1639cc9d1de5757` |
| `1a0006edfe95` | peer / sparky | `7aa8469ff25ddef08e0b88713e33e3c7e78cd29dd4c82ebdab8fc8894d34b5a5` |

Evidence: `/home/rob/tmp/pb-maintenance-samehost/`, including original/fixed
campaign manifests, receipt indexes, actor/source verification, inner-state
and owner-host scope/container checks. Fixed namespaces under
`/mnt/shared/pb-qualification/samehost-fixed-e42d1ac7a802/` retain empty ledgers;
baseline namespaces under `samehost-b35e7badbdc5/` retain bounded failure evidence.

The old caller overlaps the successor, but the original payload is already
retired. Simultaneous payload attempts, delayed Docker RPCs, jointly stale
claim/lease evidence, permanent host loss/reboot, kernel NFS stalls, stall-budget
accounting and normal production startup/result collection remain unqualified.
#234 remains open. Runtime publication and deployed adoption are tracked in the
PR delivery comment, separately from these source-snapshot results.
