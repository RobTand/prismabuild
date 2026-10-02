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

Publication and inspection of actual fleet rows remain root acceptance work.
Path availability is an admission requirement, not dependency qualification
for arbitrary project packages or a guarantee of future filesystem access.
