# Experimental checkout-copy acquisition identity

## Scope

PB #811's arm-B experiment banked an entry identity, checked the path before
initializing a private repository, then opened the pack and index without
checking those descriptors. A same-byte, same-length inode replacement between
those operations passed the private destination digests. This was an identity
acquisition gap, not evidence that wrong bytes were served.

Arm B now supplies each banked object identity to `copy_or_reflink`. The helper
checks `fstat` on the opened regular-file descriptor before ioctl or byte copy.
Path and descriptor records use one local constructor around the existing
`reader_lease.portable_identity` rule; the local device fence is not a
cross-host identity. A mismatch raises `E1Error` and private-checkout cleanup
runs. The retained-descriptor fallback and private destination digests remain.

## CPU evidence

| Arm | PB action | Worker ending | Population |
|---|---|---|---|
| Unfixed RED | `fafb99b09f377c07677af4f1212201ac229d47dee760fb5ac903c6f5a4124c0a` | sparky, failed/exit 1, one attempt | 2 failed, 1 unchanged control passed, 0 skipped; 3 collected/ran/outcomes |
| Fixed GREEN | `5290d3e39e27c9f53a87f21425efb7f2a09790c086893640df71b8c042da3876` | sparky, executed/exit 0, one attempt | 32 passed, 0 failed/skipped/uncollected; 32 collected/ran/outcomes across six files |

Both RED cases failed with `Failed: DID NOT RAISE E1Error`: pack and index
replacement after the path check. Filesystem, descriptor and copy operations
are real; these three acquisition cases scaffold Git, CAS, preflight and
control plumbing. Neighboring real-Git witnesses remain in the GREEN set.
No missing files, duplicate collection or reconciliation gaps were reported.
The expected RED has no successful execution receipt.

GREEN receipt: `ea9cdcc939560f563328066abfbd11de4748cd427d7b30d31607d383b175a6bd`.
Its 9,251-byte payload hashes to
`08bb7fc4d7ead77cf7ad01809873a64843326dd992c6384726f5fa76755190ac`.
Local claim `70f03419a8caed5cdd5b0c50a960057a1b95915dfc21cc47dacfb61897b0b484`
passes all nine integrity checks, including payload hashing. MCP independently
reports a complete, unambiguous terminal record. Full worker attestation was
not independently verified.

Published pbtest used the coordinator's explicit GB10/pb-cpu self-test route:
one shard, two worksteal workers, native threads 1, CPU 2, memory 2 GiB,
priority -10 and timeout/wait 600 seconds. WINDOW_ACTIVE was absent before
each client started. No GPU demand or measurement mode was requested.
Primary active LSP reported no findings; 28 auxiliary existing IO/numeric-parse
findings remain, so this is not a blanket lint-clean statement. Code/test bytes
were tested before these evidence documents were added.

## Remaining acceptance

This is a per-object experimental acquisition fence. It does not implement an
atomic pack/index/manifest transaction, a live production ref or epoch check,
async/mmap ownership, retirement/charge ordering, crash recovery, deployment,
source certification or power-loss durability. Global staged-read ledger axes
remain unchanged and RNG-02 remains partial. PB #811 still owes production
integration and a matched deployed claim-path before/after profile. Historical
prototype timings are not measurements of this new fence. No speedup, actual
reflink-kernel, production-cache or GPU qualification is claimed.
