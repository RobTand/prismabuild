# PB #1462: carry the named interpreter through the producer boundary

Source-qualified repair, not a runtime publication or live missing-venv
experiment. Admission used generation
`d028dfee920b-1790960385-1d815cff72d1`; the implementation extends #1263.

The child-command binding was isolated first in
`7153d763b166` with unchanged argv behavior. Four admitted PB shards passed
50 tests before changing the producer. The corrected causal RED action
`97a8995a164e3f62bd8fa5d193cb2c107a9cc169bc69c08aeb0faf85d3fc141b`
failed all five new cases. Each generated command was parsed and sealed by
the actual pbrun owner and published into the existing private test-queue
fixture: argv started with `env`, the sealed request/row omitted the named
interpreter, native environment values were outside the sealed environment,
portable CPU requests still carried x86, and preflight priced cpu=1 when the
request reserved four. The private fixture is inside the admitted test and
publishes no recursive job to the real fleet.

The fix uses the existing `pbrun --env` flags, puts the absolute `python*`
entry first, and uses existing `--anywhere` for untagged shared-runtime CPU
work. An intermediate candidate without that flag failed three tests in
`b34242db7754fcef9f32391b8693e9d9e43d42c920e70c8aa41728eabde4e2c9`:
pbrun correctly retained its source-host pin for a box-local executable. The
flag resolves the already-authorized portable contract without wrapper
guessing or a new placement API. Explicit class/GPU dependencies and a local
runtime's source pin retain their constraints.

Final action
`3a67dd662caa0086887ff62bec3e53467f111a941420ffb17c3b8c2edb64e9a0`
passed all 13 new cases: actual request/row/capability agreement, literal
TMPDIR with spaces, empty addopts, PYTHONPATH, native ceilings, alarm and CPU
demand; matching/absent/unknown offers; eligibility across capable
architectures; explicit architecture constraints; legacy-worker refusal;
interpreter removal after publication; and unsupported-entry refusal.
The removed-path case copies a real native Python binary, publishes its
requirement, removes it, and verifies the existing `interpreter_not_present`
claim denial without launch. The earlier shell-script stand-in correctly hit
the outside-closure script gate first; its failed broad action `9c0b9f64` is
retained as a fixture failure rather than accepted as interpreter evidence.

The accepted population is 321 passes in 32 unique files, zero skips. Seven
unchanged broad shards contribute 288 passes; the final new fence file adds
13, documentation references add one, and three neighboring files from the
failed bucket were independently rerun to successful canonical receipts
(19). Existing #1458 native collection (13), outcome reconciliation (35),
thread/timeout/tmpdir bounds, xdist, dependency guards, traces, GPU-option
fixtures, pbrun environment/ordinary-action goldens and fleet submission pass.
Test adapters now apply the supported `--env` values instead of executing
only the argv after `--` and silently omitting the environment boundary.

All test/compile work used published PB clients, CPU-only admission and bounded
native threads. Fanout reserved one/two CPU cores and 1/2 GiB per shard at
priority -10; most tests used a 300 s action deadline. The final compile action
`4e9db85d9e1fd294108ef534d93c015b36bc9646bd82f81c2f2e05a41501b3e5`
compiled all 15 changed Python files successfully. Its sealed source bytes
match the delivered Python files. No GPU workload or performance delta is
claimed; GPU-option cases use CPU fixtures.

Evidence is in `/home/rob/tmp/astra-resume-20261002/pb_interpreter_1462/`:
`accepted-tests.json`, `refactor-tests.json`, `full-neighbors.json`,
`final-fence-tests.json`, `failed-bucket-neighbors.json`,
`receipt-metadata.json`, `claim-verification.json`, and
`source-and-log-audit.json`. Twenty terminal records and full stdout/stderr
byte counts/digests were inspected; 17 positive named claims, canonical
receipts and full payload hashes verified. The claim API does not separately
revalidate the full worker attestation, and no such check is claimed here.
An attempted three-file resubmission overlapped a coordinator prose edit:
the snapshot guard refused all three before publication. Its zero-row
`snapshot-refusal.json` is retained; the stable-checkout retry passed. No
refusal was bypassed and no request was altered after sealing.

Publication remains root acceptance work. Actual source-produced fleet-row
inspection is recorded below without publishing the candidate runtime.
Path availability is an admission requirement, not dependency qualification
for arbitrary project packages or a guarantee of future filesystem access.

## Resume: actual producer and selected-result smoke

Existing PR1464 repair `324778bc23ff8ded80c1e1dabf14678fe70b1b38` was reused
in an isolated descendant worktree; no second interpreter mechanism was added.
The source `pbtest` producer was loaded with `runpy`; only its `PBRUN` global
was rebound to `/mnt/shared/prismabuild-fleet/repo/tools/pbrun.py`. It generated
the repaired command and submitted through the published client. Local-runtime
placement was retained: host-pinned producer evidence, not deployed portable
fanout qualification. No completed qualification was reconstructed.

Arguments: `--python /home/rob/venvs/pb-cpu/bin/python --shards 1
--workers-per-shard 1 --threads-per-shard 1 --cpus-per-shard 1 --mem-gb 2
--priority -10 --timeout-s 300 --wait-s 900`, selecting the existing
`tests/test_pbtest_seals_interpreter_1462.py`.
Actual row `3b4b27694b6092b1d344c07a458fcc00011ad880d2eb3240f6ee26cfa1028aef`
executed/0 on sparky, attempt 1, unchanged generation
`d028dfee920b-1790960385-1d815cff72d1`, 13 collected/ran/passed, zero skips,
1.50 s pytest time. Row tags include `interpreter-path-v1` and `sparky`;
the real launcher uses the requested Python. Snapshot
`badb40643fdde07f868bec5ed729428fa2b76b38` has the exact repair as parent.
Receipt: `7e19e41a00780c0b7e14da21d2bf58674b8bfc435a3ec13e4b6f831646d961b7`.
Payload (3980 bytes):
`80eba4838e373d0e9b66486eeba54a50ce84d76fad21a13e45c4a8325f74ed49`.
Claim: `d8400633ae7a44ae66b76a584267d6cc200cbe969365a17deabc0ef85bffc5d0`.

Portable CPU action
`039389b06e0d85031ed478af44e352ff5a8e10fe0e79a92d9eaeacd05cf2117d`
executed/0 on dl380g10, CPU 1, memory 1 GiB, native threads 1, priority -10,
120 s execution bound. Source SDK4 authenticated the first action at
publication `1790996739.3538232`, attempt 1, result cap 65536, evidence cap
1048576. It returned the exact receipt/payload and asserted
`params.command[0] == params.interpreter` at the requested path and all four
OMP/MKL/OpenBLAS/Torch ceilings 1 in the sealed environment. This is SDK4
source against a real d028 execution, not SDK4 deployment or a consumer pin.
Receipt: `ff4f694b6f0acc5a333e1c28d21f5353325e4f873d4c8d91cf193b467353df00`.
Result (444 bytes):
`0d8bf84459aff891daaa84818cb7d9e14fb5c1ef289d1fcb6a05d45d2e483969`.
Claim: `70e3492c44af3dc60d4a33677e3b37c793a7cbeb810ea85d4ff54911f42d18e2`.

PB MCP checked complete unambiguous terminals, full immutable stdout/stderr,
and claims/payloads. Shared-fleet log directories follow
`pb-queue/attempts/<action-key>/<generation-digest>/`:

| Action | Generation digest | Stdout SHA256 | Stderr SHA256 |
|---|---|---|---|
| `3b4b27694b60` | `05a6eaee96846ccbe4b61079dcc89c58d34373c97c9989fab35c6a86a4554c5b` | `01776254b1feac888682e58e18636e0d77020e91b581d002a5203f9dc51d2f09` | `6ae22e28d8e7d3c9541ecc30e0edf8b3931838f003d6d63a9b15a9f79f0f4b0f` |
| `039389b06e0d` | `29d1fbc10e1deb458d8f610ca3c496a73a279d418a0a9b4eb98f2a56fbea7540` | `aa4095caffa627af77e895bd5c7d81144795b56f7abcd5f6067495351fc5c3a5` | `e53e6c1a643c3ce9d00407937e7616ea827d5ad6f1f283a8b0e8c7f634c59a7c` |

Producer peak scope memory: 137515008 bytes. Retained Netdata/pqteld profile
has zero memory-pressure average; an independent Netdata CPU query returned
samples for the execution interval. No energy or speed comparison is claimed.
No deployment/admin/clock state or historical worktree was changed.
#1419 still needs a genuine waiting measurement within a holder lifetime;
#1403 needs natural closed-gate saved-finish recovery. #1399's historical
executed/0 row does not prove a new cycle bound. No artificial holder or
duplicate GPU work was submitted.
