# Stage coverage consumer handoff (#2197)

## Status and authority

This is **component support only**, alongside `CensusIndex.stage_walk` in
`src/prismabuild/kept_reads.py`. It does not establish deployment, campaign
completion, complete INV-07 conformance, or complete live coverage. Those remain
unverified. No production caller currently supplies this handoff's attestations;
it remains unavailable without explicit inputs from the existing owners.

`stage_walk` retains directory membership and classifications, not an authoritative
inventory of file materialization, marks, ownership/pins, or relevant receipts.
A retained directory stamp, material generation, warm receipt cache, or successful
reconcile cannot establish coverage. Existing per-file metadata fallbacks remain
unchanged. A positive handoff is not permission to skip them or to delete a file.

The stage inventory/materialization and mark owner is #1880. Ownership/pin
coverage belongs to #2104 (remaining completeness work: #2204 and #2198), and
relevant receipt coverage belongs to #2106 (#2206). This component consumes their
normalized in-process inputs; it does not implement missing producers, watchers,
a scanner, a second inventory, or a bell topic/wire schema. Complete live
discovery, O2 metadata-skip performance, and O3 candidate-specific dependency
narrowing remain outside this package. Existing dependency issues are unchanged.

## Contract

`index.stage_coverage(root)` returns the same handle for a lexically normalized
absolute root within that index. It does not resolve symlinks or perform discovery
I/O. Distinct roots and new index instances start independently incomplete.
Callers must use a consistent canonical stage path for retained captures and the
handoff; different symlink/relative spellings of capture keys are not an alias
inventory. `complete` is a synchronized eligibility observation, not a lease or
a replacement for fresh destructive-operation proofs.

A boundary vector maps each of `materialization`, `ownership`, and `receipts` to
`(epoch, consumed_cursor, positive_coverage)`. Epochs are opaque comparable source
identities, cursors are nonnegative integers ordered within that epoch, and
coverage must be explicitly `True` for every dependency. These attestations are
provided by the owner, not inferred or authenticated by directory observations.

1. `begin_recovery(vector)` makes the handle incomplete and supersedes any older
   recovery token. With all positive inputs and matching known source epochs it
   returns an opaque identity token; otherwise it returns `None`. The initial
   boundary establishes unknown epochs. Subsequent producer epoch transitions
   must be delivered through `invalidate`, not silently replaced by a recovery.
2. The owner reconstructs its inventory and replays changes **outside** the
   handoff and transition/ownership locks. No callback or discovery is initiated
   by this interface.
3. `finish_recovery(token, vector)` attests that reconstruction/replay reached
   that boundary. It succeeds only for the current token, all positive coverage,
   matching epochs, and consumed positions at least as new as every observed
   head (and the starting boundary). A covered event during reconstruction
   advances the required head; an unreplayed completion fails, but replay through
   the newer head may finish using the same token. Failed completion does not
   consume a still-current token. Success consumes it and makes `complete` true.
4. `invalidate(source, epoch=..., cursor=..., paths=..., gap=False)` makes coverage
   incomplete and evicts the actual retained stage subtree on **every** input,
   even repeated same-path or same-cursor inputs. Cursor heads never regress
   within an epoch. A gap or epoch transition revokes the token, requiring a new
   reconstruction/replay attestation. Unknown paths also evict the whole subtree;
   ordinary covered events with unknown paths can still be replayed using the
   current token. Known paths conservatively evict the whole root too: this is
   not candidate-specific narrowing. Unknown sources or malformed positions fail
   closed and raise `ValueError` after revoking reuse and the recovery token.
5. `forget_stage` evicts captures and revokes completion and pending tokens for
   overlapping coverage roots. A late obsolete token cannot certify an old epoch
   or disturb a newer completed recovery.

Eviction uses `DirectoryCaptures.forget_tree`, including its existing in-flight
acquisition revocation, so an old capture cannot republish after invalidation.
The registry lock precedes a handle's bookkeeping lock, which precedes the
capture lock; no reverse edge is introduced. No operation takes an ownership or
transition lock or initiates discovery I/O. Reconcile's discovery placement,
fresh locked identity/ownership/pin checks, capacity accounting, and deletion
proofs are untouched. Recovery orchestration stays with the existing owner.

## Component verification

The full acceptance file and the O4 selected retained-read, lock, resolution and
mandatory repository guards passed: **163 passed on x86; 163 passed on ARM**.
Command (plain pytest inside admitted test actions):

```sh
python -m pytest tests/test_stage_coverage_handoff_2197.py tests/test_kept_reads_package.py tests/test_the_reconcile_walk_reuses_only_what_moved.py tests/test_an_egress_census_before_the_lock_decides_nothing.py tests/test_a_stage_proof_runs_outside_the_ownership_lock.py tests/test_the_reconcile_resolves_only_what_it_may_delete.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"cc1c3349145253f14a56bdc7582fa99c319fcece56d2986e99d87940db40e52c","size":3275}},"operation":"a0d6d43ffd66eaeaed1626f380d632f0be4395a08448d9c7476ed27539902cf1","receipt":"d31fea2d754344185bf9a5b468ae3af9ad012ea7f9cfa5d6d6117960bae6a127"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"52da23ffe77cd5498c049ad7da384b3d9cd1dd494871b0932fd7bbe96d3a6b27","size":3274}},"operation":"6c291c86696e89d5e4f03c4a924e5a7a1942a77514d654bcf6678650521fa7f9","receipt":"c4f75aea26052f07a6dfaaa32d059f7ce0588eb6030692cae3f90008b89c6274"}]

These finite component checks do not establish producer completeness, live
qualification, deployment, campaign acceptance, or whole-requirement conformance.
No record format or compatibility golden changes are required.
