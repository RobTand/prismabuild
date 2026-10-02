# PB #1459: profiler diagnostics outside the source checkout

Source qualification only. Tests were admitted through published generation
`d028dfee920b-1790960385-1d815cff72d1`; this document establishes no deployment.

Corrected baseline action
`cbca55bd6ef230267377744d85c373739a00032ce1f93b8f380ec9ea9d4de562`
ran the owned regression on unchanged core source at
`344bb3ca1863329d3d3fd5fb32a0988ce6a9d46c`: 3 failures / 1 pass, plus the
new collision case deliberately deselected. A root task and a nested task
both exited 91 after actual Git status reported PB's profile/exit-status/route
files as untracked. The genuine untracked-input control also exited 91, then
failed because old cleanup deleted its `real-input` file. A truly changed
sealed closure refused before launch as intended. Earlier attempts
`165d27ce`, `4bbcfb5b` and `c78802b7` had fixture exception/shell-quoting
defects and are confounded; they are not the causal baseline.

The private-directory fix passed all five new cases in
`7ea100ab629a2583429a040ea63e4077d81906dbe3eb7437bee01d2e7f4096a7`.
It preserves the same real Git check in the child, handles nested cwd and
TMPDIR inside source, leaves genuine untracked inputs visible and intact,
retains changed-closure refusal, allocates unique mode-0700 directories,
preserves bytes on a collision, and cleans the session-owned directory.

The six-file profile/front-end neighbor fanout passed 107 tests / 0 skips:
`tests/test_sample_profile.py` (43, including the real py-spy probe),
`tests/test_gpu_profile_modes.py` (34), primary-checkpoint (10),
summary-timeout (3), new source-cleanliness (5) and PB front-end (12).
The GPU-mode tests use CPU fixtures/fake backends; no CUPTI/GPU profiler or
container mounting qualification is claimed. Existing CAS profile ingestion,
partial/error records and action endings remain tested.

These checks used PB-owned placement, CPU-only resources, one native/pytest
thread, two CPU cores and 2 GiB per shard, priority -10, 300 s deadline.
`profile-neighbors.json`, `profile-final-red-submit.log`, immutable attempt
logs, `receipt-metadata.json`, `claim-verification.json` and
`source-and-log-audit.json` live under
`/home/rob/tmp/astra-resume-20261002/pb_collection_1458/`.
Terminal records, full logs and named CAS payloads were checked; independent
full worker-attestation revalidation is outside the read-only claim API.

The source parent must allow creation of the private sibling; an unavailable
parent fails before launch. Explicit in-container torch instrumentation must
mount/forward the path supplied in `PRISMABUILD_PROFILE_TORCH_OUT`, rather than
assuming it lies under cwd. This is a correctness change, with no performance
delta or staged-read/campaign-completion claim. Public scratch lifetimes and
cache/residency owners were not changed.
