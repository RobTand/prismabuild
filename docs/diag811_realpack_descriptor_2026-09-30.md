# Experimental checkout fallback: real Git object witnesses

## Scope

This record extends the small-file descriptor tests in PR #1388 with five
real Git cases for PrismaBuild #811. It adds tests, not a production cache,
lease implementation, default, deployment, or performance claim.

Git creates a SHA-1 bundle, pack, and index from a deterministic single-commit
fixture. The test verifies the original pack/index and records their digests.
An injected unsupported-reflink error replaces or unlinks the selected pack or
index after its source descriptor opens. An unchanged-source control covers the
same fallback without mutation. The private copies must retain the original
digests, pass `git index-pack --verify`, and check out the original commit,
payload, and clean worktree. Both source and destination descriptors must close.

The ioctl result is injected. No actual reflink kernel, larger Git history,
SHA-256 Git object format, full repository-state parity, source-seal admission,
production reader lease, or asynchronous lifetime is qualified by these cases.

## RED: historical helper behavior

The branch starts at `c6bbf18e2dcc2da8705bb74323b3d1f100953979`.
For RED only, `copy_or_reflink` was restored verbatim from pre-fix commit
`6d17a00e121f08b5fe71294b7ec3ed4ae8f9cd03`; all other current source stayed
unchanged. This is a function-level historical witness, not a pristine old-tree
baseline. The historical whole-module SHA256 is
`c3253a5f4e4dc43162458ecea307435a7a6997ff0c85c397f58ad7e60cf20d36`
(reference provenance only).

- Action: `c14c491880006c48c5c72bf8b7ef9d310017a62085288139f33394095cc03e79`.
- Terminal: failed, exit 1, one attempt on sparky; MCP complete and
  terminal-unambiguous.
- Population: **4 failed, 1 passed, 0 skipped**, 5 collected/ran/outcomes;
  no missing files, duplicate collection, or reconciliation gaps.
- Replacement failure lines: `AssertionError: copied pack differs from the
  originally verified Git object` and the corresponding `idx` assertion.
- Unlink failure line: `FileNotFoundError: [Errno 2] No such file or directory`
  for each object type.
- The failed action has no successful CAS receipt or local-result claim.

## GREEN: current helper

The helper was restored byte-for-byte to the branch's HEAD before GREEN.
There is no production-code diff. All five experimental test files ran.

- Action: `3ea0a7c7122e6c4563bcac306d9375d63e0431aa691d9553fe2f5e46a686330f`.
- Terminal: done/executed, exit 0, one attempt on sparky; MCP complete and
  terminal-unambiguous.
- Population: **29 passed, 0 failed, 0 skipped, 0 uncollected**,
  29 collected/ran/outcomes; no missing files, duplicate collection, or gaps.
- Canonical receipt SHA256:
  `da65000b296c857ec60d89a450a92b1a3514196db9ba985ec1000752074c9f7b`.
- Result: 8,644 bytes, SHA256
  `ceaf28ca99f07f02ae7c7ddfdc19564fc307a19cee98fb334d4a4c2c98365d7f`.
  Payload length/hash and its terminal pytest summary were independently read.
- Local-result claim:
  `74b7401823993ad516013f74bc4e526cdc9cfe3061afdf59133dfb246b4da8be`.
  `pb_verify_claim(hash_payload=true)` passed all nine executed integrity
  checks. Full worker attestation was not independently checked.
- Worker stdout: 11,209 bytes, SHA256
  `9a31e7faabddc8138884686018a5998e91516d600ef82b28eaeb764c7cbf00e0`;
  the bounded log reader reported matching recorded length.
- Tested module SHA256:
  `ea57691318566b73ba3d3fdb5992665e10de33b1b2ed00c009b2e8933d2e6f59`.
- New test SHA256:
  `9f08a8694100d016d7e934c6e16105d2acc7b7334b02d3a7fe4e787f41d519ba`.

Both actions used published `pbtest.py`, the coordinator's explicit PB-self-test
GB10/pb-cpu route, priority -10, one shard, two pytest workers, one native
thread per worker, CPU 2, memory 2 GiB, and 600-second execution/wait bounds.
`WINDOW_ACTIVE` was absent before each client started. GPU demand was absent and
`CUDA_VISIBLE_DEVICES` was empty. Git uses `pack.threads=1` and `gc.auto=0`;
the fixture has fixed author/committer dates and disables global/system config.
These are CPU-only populations, not CUDA-surface evidence. Pytest durations are
recorded; comparing the failing RED and broader GREEN runtimes is not meaningful.

The tested code/test bytes are unchanged in the PR. These acceptance documents
were written afterward. Primary active LSP found no findings in the new Python
test. The staged-read ledger axes and contract/defaults remain unchanged.

## Remaining acceptance

RNG-02 remains partial: this proves selected-object retention within one
experimental copy, not acquisition against an earlier verified cache identity,
production refs, epoch/ABA fencing, fork/mmap/async holders, or crash recovery.
SM-03/INV-07 lease, retirement, and charge-ordering requirements remain open.
PrismaBuild #811 still needs production integration and matched deployed
claim-path before/after evidence. Earlier prototype timing results are historical
and are not remeasured or promoted by this test-only change.
