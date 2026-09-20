# Candidate pin provenance (fixture-only, PB730-owned)

- File: `reader_lease_candidate_2637a9d0f7.py`
- Source: `RobTand/prismabuild` reader-lifetime lane, commit
  `2637a9d0f7d31afbce7ad2e5735e8334fe37a40d`
  ("Reader env round: generation-root helper form, stale-key clearing,
  launch RPC proof"), blob `e66018a17f47a67da7ea1b2bec0f719ab6a9eef1`
  (verified: `git hash-object` of this file equals the pin blob).
- Worktree: `/home/rob/tmp/pb-reader-lifetime-20260920` (PB730 owner lane).
- Status: CANDIDATE. Root returned R7 on this pin (automatic cleanup
  namespace/incarnation/tombstone defects). Fixtures use ONLY the sound
  public subset: `write_material`, `covers_for_keys`, `acquire`,
  `open_pinned`, `release`, `injected_context`, `stat_identity`,
  `mint_generation`, `material_path`, `leases_root`, `READER_LEASE_TAG`.
  NEVER `auto_reclaim`, `release_refs`, `register_inherited_ref`
  (containment/cleanup paths under R7 review).
- No capability is announced by any test (`reader-lease-v1` stays off).
- Ownership: PB730. This copy is read-only fixture support with candidate
  labeling (a provenance test asserts the blob sha). Delete on PB730 merge
  and import the merged module instead. No production import path reads
  this directory.
