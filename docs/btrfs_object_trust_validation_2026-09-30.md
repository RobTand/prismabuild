# Btrfs object-trust validation

Status: the fresh-main uninstrumented final-candidate gate and ten-file
compilation passed at `26fa215e9def17a33b045ffc0d4c9f779fba313a`. The
historical uninstrumented arbitration failure remains unexplained. The
matched affected-filesystem profile remains a mandatory merge gate, not
waived by these correctness results or by source-only review. This record
establishes no speedup, deployment, or full closure of #1358 or #1027.

## Contract

The existing shared filesystem memo retains its direct-device lookup. If a
stat device is absent from the mount table, the fallback binds the exact object
or borrowed descriptor to its live descriptor mount ID, stat version,
namespace, and mount-table generation. It does not infer a filesystem from a
path prefix or add a device alias cache. Unknown, unreadable, NFS, and NFS4
evidence still refuses reuse. Coarse-clock and file-version fences remain.

Checkpoint hits now require current trusted file and directory versions.
Exact-object I/O occurs outside the global memo lock, followed by locked
identity and generation validation. A literal `-` source field is data, not a
second mountinfo separator.

A valid NSFS mount root is not necessarily an absolute path. The parser accepts
only the closed kernel namespace-name vocabulary and a whole ASCII-decimal
inode token; zero and leading zeros are grammar-valid. NSFS remains nonlocal.
Ordinary roots and all mountpoints retain the existing absolute escaped-path
rules. This does not relax the local-filesystem allowlist.

## Observed regression evidence

All listed tests ran through the published PrismaBuild clients with priority
-10, explicit timeouts, at most two shards, bounded native threads, and declared
CPU and memory. No tests ran on the coordinator.

| Boundary | Action | Observed result |
| --- | --- | --- |
| Current trust, unlocked I/O, source separator | `b94fa8e5200eef374f52ab5a2aaf41041b0a1379740c5ace865255c8cfe1dbfd` | 15 failed; 15 reconciled; no skips. Warm-state and real dead-owner consumer premises passed before assertions failed. |
| NSFS kernel table | `c769e706c7951902a1824891c3d3e6e1896392d363baaf7dc4a7f5289b6f5925` | 16 failed, 10 passed; 26 reconciled; no skips. |
| Earlier integrated candidate | `f48042932e8b1d7fc4156e26a1eb7f1e525328de927f365ed01c778b034b1f9d` | 3 failed, 217 passed, 6 subtests passed; 220 cases and 226 outcomes reconciled; no skips. Not GREEN. |
| Clean-main golden baseline | `e7c6e8b762d1244fb7916bf962a6d9bf3d4b9cb576035095c911eb7ecf24454f` | 1 failed, 2 deselected; one reconciled case; no skips. Missing worker-retired output predates this change. |
| Corrected integrated candidate | `85c3e00b6c3d92a66ec2eea90d4f82dc9a60f458b378df34a11c9d61269aa1c9` | 1 failed, 219 passed, 6 subtests passed; 220 cases and 226 outcomes reconciled; no skips. Not GREEN. |
| One lower-observer control | `82fe75c26247b4ca5a074c2a0c2524e586c483263cd82aecfaf7bc7d04de5ce3` | Sparky: 220 passed, 6 subtests passed; 220 cases and 226 outcomes reconciled; no skips. Instrumented control only. Cause UNKNOWN; control ends. |
| Tiny affected-host object proof | `aacb0fdd1ee7184abee863fbd0b6011bdc57d9fb5a507a8562fcbe7582ce8c6c` | dl380g10 device 0:31: direct lookup unknown, exact lookup Btrfs. Real owned record reuse: listed 2, parsed 1, kept 2. Not a performance or live-queue claim. |

The final-candidate GREEN action
`db22dfe17f7b8ec87690f6d402a4241159ebfd7bd925449e4d22552fa2f5626d` ran on
sparklina: **220 passed, 6 subtests passed, zero failed/skipped/deselected**;
220 unique cases and 226 outcomes reconciled across 16 files in 37.03 seconds.
Compilation action
`13f0b31f724fc298775ac8ac4f156c6683936c237c6ae84dfdf4eabae720a70e` ran on
sparky: exit 0, `COMPILE_OK 10 files`. Both checkout snapshots name parent
`26fa215e9def` and differ from its tree only by their generated closure files.
The earlier affected-host proof has snapshot parent `98082a7546c3`, but its
two executed production modules are byte-identical to `26fa215e9def`:
`stage_move.py` SHA-256
`0928a9db0c2f19d6c1560fe44ccbb10c5fad3329b1ae9f5e046a0d945a07e7e1` and
`stage_release.py` SHA-256
`7599d2eacd0ea55985b20a439b52db2d5b3067632b2fc8fd5d3b7370eb874367`.

The three immutable attempts ended successfully; their actual stdout/stderr
hashes and CAS payload/claim bindings were independently checked on 2026-10-03.
Result SHA-256 values are respectively
`d52d454b9197aa79c45f0d390e3a909db2320bee9cd14774f08e3f5c11e44aff`
(58,684-byte GREEN),
`55a339bb181770ab452f2a1719af18a1b07de2be371f01b761e202382dc8ac45`
(20-byte compile), and
`86c796b14ba32e10036945c766c630c82e5facb7aa33d6c5f4fb645f7d3fbe71`
(361-byte affected-host proof). This readback did not rerun any action.

The earlier corrected integrated result resolves the previous three fixture failures.
Its sole failure is the arbitration lock bound: four namespace directories
were listed under the ownership lock. The forest had 404 directories and 177
fragments; 414 total listings included one root listing. Two displayed entries
are empty fixture namespaces, so fragment taint does not explain those entries.
The cause remains unproven; diagnosis must not weaken trust or ignore the bound. Earlier setup errors are not
counted as behavioral RED, and the original 32-failure result is not attributed
wholesale to the NSFS parser.

## Fixture corrections

The manually installed checkpoint fixture now settles its owned files, reads
the coarse fence before versions, and asserts genuine eligibility before
expecting hits. Its capacity and invalidation assertions are unchanged.

The metrics probe counts only `/proc/self/ns/mnt` stats separately from queue
record stats. Queue-read bounds remain, and the test asserts that namespace
checks occurred.

The golden comparison remains complete, excluding only the exporter's existing
self-observation families. A clean source checkout at
`443f96d352f5025ec97f67eb956391d58048692d` generated the exact private-fixture
output through action
`2569f0cd869a7e0b4a4bdc2b1b60b10246525e4cb0fded33ff2390c474623089`.
Terminal state, immutable attempt, log hashes, canonical CAS receipt, result,
and released cleanup were verified. The 23,753-byte result has SHA-256
`3c25e5358677c40f82028dc1baa5eff8dd64f6498517381b63e4be818acd98d3`.
The candidate fixture is byte-identical to that result; its only output delta
is ten lines for the already-shipped worker-retired family.

## Evidence locations

Verification records, exact client commands, kernel evidence, and logs are in
`/home/rob/tmp/claude-campaign-20260926/tmp/p2p3/prismabuild/`:

- `1358-review-corrected-verification.json`
- `1358-nsfs-red-verification.json`
- `1358-green-nsfs-verification.json`
- `1358-golden-baseline-verification.json`
- `1358-golden-generate-verification.json`
- `1358-nsfs-kernel-evidence.md`
- `1358-green-integrated.sh`, result JSON, and verification JSON
- `1358-green-refreshed.sh`, result JSON, and verification JSON
- `1358-compile.sh` and `1358-compile-verification.json`
- `1358-arbitration-failure-selected.txt` (bounded pytest excerpt)

Independent source review found no remaining issue in the current NSFS/trust
implementation or the three fixture corrections. Source review is not runtime
acceptance.

## Remaining gates

The permitted fresh-main rebased, uninstrumented final-candidate gate passed
with unchanged assertions, delays, lock bounds, and trust policy. It validates
only that candidate; it does not retrospectively explain the historical four
listings. The separate compilation gate also passed. Changed production or
test source requires its own applicable qualification, not reuse by action
name alone.

The lower-observer trace contained only the two original acquisitions, no
refusal or mismatch, unchanged retained stamps, and actual zero lock depth at
emission. The original failure therefore remains UNKNOWN. Source proves that a
legitimate synchronized mount/watch-generation invalidation can refuse a
current directory proof and cause a locked relist, but no observed event
attributes this failure to that possibility.

The tiny affected-host proof and its terminal, immutable attempt, log hashes,
CAS result, and cleanup are verified. #1358 still requires an in-process
before/after profile with py-spy and Netdata on dl380g10; #1027 has its own
matched ledger-cache qualification. The PR acceptance audit on 2026-10-03
explicitly requires this pair before merge, or an explicit coordinator ruling
accepting an alternative. Both arms must record live filesystem/device/mount
evidence proving the missing-device Btrfs condition. The prepared campaign
path now resolves to ZFS device 0:92, so the unchanged prepared script cannot
meet this gate. A read-only 2026-10-03 inventory found `/home/rob/tmp` and
`/home/rob/tmp/prismabuild-checkouts` on Btrfs with stat device 0:31; that is a
candidate location, not an executed matched profile. Neither profile nor
runtime publication is established here. Keep the PR draft and #1358 open
until their respective gates are satisfied.

The existing allowed-local full-stat/mount ABA and non-atomic observation limits
remain. This change does not solve wall-clock backsteps or make file snapshots
atomic. A blocked syscall can still block its own caller, but no longer holds
the shared memo mutex across exact-object I/O.
