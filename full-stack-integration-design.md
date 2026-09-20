# Staged-read integration tests: implemented components and remaining gates

The complete target is the [staged-read contract](docs/staged_read_contract_2026-09-20.md),
including ACC-01 through ACC-06. This change adds PB component integration
fixtures. Full campaign conformance remains open in
[issue #725](https://github.com/RobTand/prismabuild/issues/725).

## Implemented coverage

These tests run inside admitted PB CPU actions. Each uses an isolated queue
and temporary directories; no live fleet queue or storage contents are mutated.
The generated corpus contains a 1 MiB whole file and an 8 MiB file with a
3 MiB declared range starting at a nonzero source offset.

| Test file | Production behavior checked |
|---|---|
| `test_fullstack_producer_rows.py` | Phase ranges cover the declared read order; inconsistent entry lengths refuse; a frozen plan rejects a different binding. |
| `test_fullstack_stage_ram_chain.py` | Real SSD movers and RAM promotion preserve whole-file and nonzero-offset range bytes; composed and overlaid maps resolve both copies. |
| `test_fullstack_reader_boundaries.py` | RAM and SSD digest agreement, unknown lookup, old-epoch overlay refusal, and charged egress returning its tokens exactly once. |
| `test_fullstack_progress_retry.py` | Accepted phase accounting, frozen-plan conflict, and repeated cleanup of staged files. |
| `test_fullstack_claim_retry_primitives.py` | Capability and image matching, unsafe retry declarations, terminal failure, spent acquisition handles, and idempotent ledger release. |

Names containing `fullstack` identify the intended integration suite. These
component fixtures do not establish an application read path, a live RAM
filesystem, broker containment, a worker restart, or numerical GPU results.
The phase-accounting tests do not execute a progress reporter. The ledger
handle tests do not establish worker-incarnation fencing or JOIN/RESIGN.

## Remaining integration gates

| Contract gate | Required evidence beyond these fixtures |
|---|---|
| ACC-01 | Actual PQ producer output, wire/canonical bindings and CLI declarations accepted by PB without remapping fixtures. |
| ACC-02 | Acquire/open/release races and copy/eviction interleavings with actual reader leases and authoritative containment. |
| ACC-03 | Actual PQ source, rendered-weight, wire and activation readers consuming the real mover outputs, including forbidden-pool-open negatives. |
| ACC-04 | Restart, retry and withdrawal with exact-attempt containment, retained charges and single durable-result adoption. |
| ACC-05 | Concurrent useful results on both Sparks, with actual worker placement recorded. Two portable rows alone do not establish this. |
| ACC-06 | Completed staged-only campaign output with read-path evidence and the final artifact gates. |

PQ integration tests live with PQ so its source is sealed into the test action.
Their PB dependency must name an immutable generation or reviewed installed
commit and verify the modules actually imported. Mutable per-host checkouts
are unsuitable because the hosts can carry different source versions.
Production readers, tier placement and recovery remain owned by their existing
implementations; the harness introduces no dispatcher or cache.

JOIN/RESIGN coverage must use the real membership workflow after its candidate
is integrated. Capability matching in this patch is only a prerequisite.
Likewise, generated boundary outputs need their real producer publication,
staging and reader-lifetime contracts before the same-action path can qualify.
A receipt's identity binding is not a storage lease.

## Validation and interpretation

Run these files through published `pbtest.py` at priority -10 with bounded
aggregate CPU/memory and native threads. Keep command/result JSON outside the
checkout and verify terminal status, logs and CAS receipts. PB owns sharding
and placement. No test here requires model payloads or GPU computation.

Early failures while constructing these fixtures were harness debugging,
not demonstrations of production regressions. Each accepted test claims only
its actual assertions. Missing full-chain gates remain incomplete; they are
not replaced by placeholder tests that always fail or catch any exception.
