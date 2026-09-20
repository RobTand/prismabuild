# Issue #755 validation: stale-donor adoption and proof-search refusal — 2026-09-20

Executor: GLM 5.3 (zai-coding-plan) through OpenCode, worktree
`/home/rob/tmp/pb-stale-adoption-20260920`, branch `fix/stale-adoption-755`,
from accepted main `b8aa472b7257fb43ed179800d77dc9441bbbb107` with
PR746's main `ab3531d6b4114eaa81cbe240ff47f00466b907f4` merged before final
qualification. Live deployment remains b8aa runtime
`00ff716f4151-1789915735-d8182a4286a9`; nothing here is deployed.

## The live failure (read-only evidence, no payload reads)

- Consumer `5a12fdadcffdf11bc20197abfb08fff67a0f578b4d363ffdd711dd925d3a899c`
  (Sparky): strict calibration read refused `file-identity-changed`; its
  material sidecar (under its mover `7b420c953d1a...`) dates
  `ino 92690 / mtime_ns 1789904689441287837` (the superseded incarnation).
- That sidecar was written by `tier_loop.adopt` from donor
  `10743a644a69...` (consumer `4c1811b1...`, then FAILED, valid when it
  staged): the adoption receipt says `adopted_from: 10743a64...`,
  `complete: true, bytes_copied: 0`, no `seconds` — residency claimed after
  the consumer's own mover `7b420c95...` had failed three attempts at zero
  bytes with `staged destination holds different bytes...`.
- The current file `/stage/prewarm/pq-live-reader-20260920/calib-8x16.safetensors`
  is `ino 143497, size 1296, mtime_ns 1789905858950807132,
  ctime_ns 1789905858951807153` on both DL and Sparky (cross-host stat
  agrees), and mover `7378ecc3...` (consumer `43e21bbc...`) holds the valid
  material for exactly that incarnation.

Root cause (two coupled defects, fixed here):

1. `tier_loop.adoptable_ranges` selected the first same-descriptor donor
   from move metadata only, and `adopt` published the successor fragment and
   copied `old_material.entries` unchanged before taking the stage
   ownership lock — publishing superseded identity into a live consumer.
2. `stage_move._StagedPublisher._proof_search` returned `divergent` on the
   first stale `file_id`, so the consumer's own zero-copy mover path
   refused a destination another record proved was the current incarnation.

## The fix (bounded; no cryptographic hardening)

- `tier_loop.adopt` verifies every dated donor entry's `file_id` against a
  live stat under the stage ownership lock **before** publishing any
  successor or transferring credit; successor publication moved inside the
  lock. Declines: `donor_file_changed` (superseded identity) /
  `donor_file_missing` (bytes gone), both with zero publication.
- `adoptable_ranges` returns candidate donors per descriptor (sorted,
  deterministic); `adopt_resident_ranges` falls through to the next
  candidate on those two declines only. Valid zero-copy/token-transfer
  reuse and legacy no-sidecar adoption are unchanged.
- `_proof_candidate` compares identity before content: a sidecar naming a
  **different inode** dates a superseded incarnation (deferred like an
  undated vouch; the search keeps looking); the **same inode changed in
  place**, or a digest mismatch on the current incarnation, remains
  immediate divergence. `_proof_search` no longer aborts on the first
  stale record. Missing proof, unknown state and live pins never permit
  replacement.
- `reader_lease.file_id_matches` is the one shared field-by-field
  comparison (adoption verification and proof search); the strict reader's
  own check is untouched.

## Measured results (all through PB `pbtest`, priority -10, x86,
`/home/rob/venvs/pb-cpu/bin/python`, 1 worker / 1 thread / 2 GiB per shard)

RED on the pre-fix tree (tests-only commits `3cc5f5ab3e0` then helper fix,
atop merged main `ab3531d`): **5 failed in 7.40s**, each reproducing the
live failure shape — the tier loop's captured event
`range-adopted ... adopted_from 61646f6e6f72...` (`adonor0`, the stale
donor), and the publisher's verbatim live refusal
`staged destination holds different bytes than manifest digest ...`.
Action `388e4a5dea0a2d9d15d4fad99f7b6c2f7ff36a888b82bb45b184ce98948994c4`
(state `failed`, rc 1 — the intended RED; first RED sweep
`0404243433a13c00dcfeb5e2e4f6534c8a0759729446d978aa6c8da9c3961b6999`).

GREEN after fix commits `5a02666392b` + ino-split: new file
**5 passed**; intermediate full run
(`pb755-green`/`pb755-green2`, keys `5eb6f1d5b3a2...` etc.) surfaced one
real regression — `test_ram_shared_source.py::test_different_bytes_under_
shared_name_refuse` — resolved by the inode split (same-inode in-place
change stays immediate divergence), after which that file passes 11/11.

Final integrated qualification (`pb755-final`, 12 shards, keys
`b7677f4e2734`, `3b383006c746`, `83f4f08395ac`, `8eca9b5269ff`,
`d75f51b68a09`, `bd1a713ad6fc`, `d003ad1bf423`, `a501ff2e88c6`,
`38f462d21ec5`, `f9336a31d948`, `19fe602ff072`, `f101b3a66f67`;
shard 0 receipt in CAS, verdict `green`):

- `tests/test_a_stale_donor_declines_and_current_material_is_reused.py` —
  **5/5**: the positive path (C adopts B's current incarnation, credits
  transfer through the existing operations, inode preserved, real pinned
  reader reads the bytes through `reader_lease.acquire` + `open_pinned`),
  the mover's zero-copy adoption despite the stale record (real
  `_Copier`+`_StagedPublisher`, `bytes_staged == 1296`, `errors == []`,
  no replacement, fresh dated cover readable), zero-successor refusal,
  grace-refusal without replacement, and live-pin refusal without
  replacement.
- Adjacent suites: adoption 14/15, reader-lease lifetime 57/57, shared
  staged path 15/15, ram shared source 11/11, ram own-consumer change 4/4,
  mover-declares-range 14/14, failed-consumer-stops-staging 7/7, ram egress
  ordering 1/1, stage/ram chunk tiling 4/4, ram promotion token hold 4/4,
  fullstack stage→ram chain 2/2. **Total 138 passed, 1 failed.**

The one failure, `test_an_orphan_is_evicted_when_a_window_cannot_be_placed_
without_it`, is **pre-existing on pristine main `ab3531d`**: reproduced
without any commit of this branch (action
`44b15d3458041920d851304ce71a4a35663a7789ad087f1bd6c38ca5b08e383d`,
`1 failed, 14 deselected`), same captured `window-gated
reason=joint-fit-stall ... output_note=output-scope-unenforced` from
PR746's window/funding blocks. It is PR746's seam, reported to root; this
branch does not touch those blocks.

## Merge-step acceptance record

```json
{
  "schema": "pb.staged_read_acceptance.v1",
  "step": "merge",
  "contract_commit": "ab3531d6b4114eaa81cbe240ff47f00466b907f4",
  "ledger_commit": "ab3531d6b4114eaa81cbe240ff47f00466b907f4",
  "scope": ["SM-02 ownership transfer", "INV-07 copy/publish/lease/release serialization"],
  "prelaunch": {"status": "not-this-step"},
  "merge": {
    "branch": "fix/stale-adoption-755",
    "issue": "https://github.com/RobTand/prismabuild/issues/755",
    "validated_repairs": [
      {"id": "SM-02-ownership-transfer-current-identity",
       "actions": ["b7677f4e27346955b5c983a6c37cd00268fa6db33974ea13e07e160b85515d14"],
       "outcome": "done/green; 5/5 new regression file on the fixed tree; RED recorded at 388e4a5dea0a (failed by design)"},
      {"id": "SM-02-proof-search-current-incarnation",
       "actions": ["b7677f4e27346955b5c983a6c37cd00268fa6db33974ea13e07e160b85515d14"],
       "outcome": "done/green; adjacent suites 138 passed (1 pre-existing main failure, attributed)"}],
    "remaining_gaps": [
      "deploy: live fleet still runs 00ff runtime; root deploys and retries the real dev reader",
      "PR746 seam: test_an_orphan_is_evicted_when_a_window_cannot_be_placed_without_it fails on pristine main ab3531d (44b15d345804)"],
    "note": "Implementation-level repair; no staged-read contract row or default changed. No cryptographic hardening added."
  },
  "deploy": {"status": "not-this-step"},
  "complete": {"status": "not-this-step"},
  "exceptions": []
}
```

## Live retry handoff (root)

After merge + publish + role convergence on a new runtime generation.

**Live donor census (read-only, 2026-09-20, post-fix review).** The stage
file is still `ino 143497 / size 1296 / mtime_ns 1789905858950807132 /
ctime_ns 1789905858951807153`. Nine held fragments name
`/stage/prewarm/pq-live-reader-20260920/calib-8x16.safetensors`; their
materials against that stat:

| mover | consumer | state | material vs live |
|---|---|---|---|
| `7378ecc39a19` | `43e21bbc1b3d` | done, held | **CURRENT** (ino 143497) |
| `7b420c953d1a` | `5a12fdadcffd` | **failed**, held | stale (ino 92690) |
| `227426b8578a` | `552c5bd6b93f` | failed, held | stale (ino 143407) |
| `ebce561f1a03` | `2c3534901da9` | failed, held | stale (ino 92691) |
| `2db88a9ee2c0` | `67076f7ef328` | done, held | stale (ino 143404) |
| `45b01a067c7a` | `01c3e3110281` | done, held | stale (ino 143405) |
| `6168d1fca577` | `8aff19ff7203` | done, held | stale (ino 143406) |
| `b6cd41b5e79d` | `36f795f71e51` | done, held | stale (ino 143408) |
| `4ef35de63adf` | `2ec358c9e0cd` | done, held | stale (ino 143496) |

The wedged consumer `5a12fdad…` has since gone terminal through the
existing stall policy (`failed`), and `ready` is empty — nothing live
names any of these ranges.

- **A surviving current donor exists**: `7378ecc3…` (consumer `43e21bbc…`,
  terminal, unreserved, still holding tokens, material dating the live
  inode). A new consumer key retrying manifest
  `48a88eb75cf57598a24b7fba2c809105a88f6e824238ab9d5c649ccbbc0e8b98`,
  range 0–1296, tier `prismabuild-stage:dl380g10`, therefore heals **by
  adoption through the fixed path alone**: the eight stale candidates
  decline `donor_file_changed` and fall through, `7378ecc3…` is adopted,
  the successor's material dates inode 143497, and the strict pinned read
  passes (`file-identity-changed` gone). No cleanup is required for this
  shape.
- **Had no current donor survived**, the supported existing path is:
  the stalled consumer goes terminal through the action stall policy
  (already the mechanism that failed `5a12fdad…`), its and the other
  stale holders' egress/orphan sweep releases the tokens and deletes the
  unshared name once no holder or live pin remains, and the resubmitted
  consumer's mover republishes the name as a first publication
  (`absent → copying → published`) with fresh dated material — stale-donor
  refusal alone would not heal that shape, and nothing in this branch
  claims it would.
- A same-key retry of the failed consumer is no longer the live shape
  (`5a12fdad…` is terminal); resubmission under a new key is.
- No manual claim/pin/receipt edits or shared-cache deletions were made or
  are needed; cleanup, if any, belongs to the existing sweep/egress
  mechanisms.

Commands (coordinator-side, read-only except PB submission):

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout /home/rob/tmp/pb-stale-adoption-20260920 \
  --python /home/rob/venvs/pb-cpu/bin/python --priority -10 \
  --shards 12 --workers-per-shard 1 --threads-per-shard 1 --mem-gb 2 \
  --timeout-s 1200 --wait-s 7200 --json <out>.json \
  tests/test_a_stale_donor_declines_and_current_material_is_reused.py \
  tests/test_a_resident_range_is_adopted_rather_than_recopied.py \
  tests/test_reader_lease_lifetime.py tests/test_a_shared_staged_path_has_two_owners.py \
  tests/test_ram_shared_source.py tests/test_ram_own_consumer_change.py \
  tests/test_a_mover_stages_the_range_it_declared_and_no_more.py \
  tests/test_a_failed_consumer_stops_staging.py \
  tests/test_the_ram_egress_frees_the_ram_range_before_the_stage_range.py \
  tests/test_stage_and_ram_chunks_tile_one_phase_independently.py \
  tests/test_a_ram_promotion_holds_its_tokens_until_the_egress_frees_them.py \
  tests/test_fullstack_stage_ram_chain.py
```
