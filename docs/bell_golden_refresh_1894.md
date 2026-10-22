# Bell current-golden refresh for #1894

The namespace lane exposed two current-byte mismatches after the bell change.
The decoded `upgrade-intent/armed` wire changes only `/coordinator_sha256`,
from `576e2df0196dcd257f35b117219b7058b6311cc7e264cc15223857d583c8c1c1`
to `d02d7a2474241a318a00bd4a3b5e6d26df0c3dcb9c7328f1f223b891d6bb6c33`.
This is the SHA-256 of `tools/fleet/publish_runtime.py`, which now includes
`pbbell.py` and excludes `prove_queue_bell.py`.

The decoded `config-fleet/source` wire adds exactly
`/boxes/dl380g10/roles/bell` and `/boxes/dl380g10/_bell_why`.
The role listens on `127.0.0.1`, `10.100.98.3`, `10.100.99.3`, and
`192.168.1.107`, port `9470`, for `/mnt/shared/prismabuild-fleet/pb-queue`.
The lead explicitly accepted both additions. The captured wire equals the
complete immutable `tools/fleet/fleet_boxes.json` bytes.

## Capture and adoption

Evidence is retained under
`/mnt/shared/pbtl-probe-tmp/evidence/1894-goldens-dl380/`.
Both captures freeze production and support sources at parent
`bad8a085ccd5092ac31589b9772c863e687c9d95`. Their only uncommitted snapshot
path is the existing `PR_BODY.md`, which the source audits record explicitly.
Published `pbrun.py` admitted each capture and the adopter with
`--anywhere --cpus 1 --demand mem_gb=1 --priority -10`, native threads one,
and worker `TMPDIR=/home/rob/pbt17`.

- Intent capture `00e9c9212746bf469f5125b18118c1af24221d0b38658303d018504777f18597`.
- Config capture `9079615425b52c461f1d2846d2054b888ea745404d59a74f4c8b7cec74ef9642`.
- Adopter `7be8acf09b8411a66e90b97ee722707622e488505eee43d3be7a6206d5374ef6`.

Each capture independently reproduces one current and two historical records
and passes both retained consumer directions. The adopter invokes the strict
`upgrade-intent` profile in `refresh_compat_runtime.py`. No named config
profile exists in this checkout. The sealed `adopt-and-export-v2.py` wrapper
restricts config to the two approved, source-backed additions and invokes
`adopt_compat_capture.py` with the verified primary audit.

Following commit `7df5938f8`, complete superseded current files are archived in:

- `tests/compat/history-controls/superseded-upgrade-intent-de5c6ffc0994a05f/`.
- `tests/compat/history-controls/superseded-config-fleet-ce1dc83bdabcb003/`.

The config baseline had no capture manifest. Its archive preserves the complete
prior golden and records that absence. The refresh adds its first qualified
capture manifest. All 63 existing historical corpus files remain byte-exact,
including all 20 rollout and six configuration rows. Nine exported files were
hash-checked before integration. Only the two selected current rows change.

`capture-primary-audit.json` and `adopter-primary-audit.json` verify requests,
source Git objects, input blobs, logs, runtime identities, CAS receipts, and
cleanup. The first two queued captures were withdrawn before execution when
the extra config field required lead clarification. The first adopter failed
because its wrapper assumed an existing config capture manifest. Its audit
and `excluded-actions.json` retain that failure without acceptance credit.

## Validation scope

Final-head x86 validation runs the controller compatibility file and the wire
inventory, duplication baseline, and design line-reference files. ARM validation
runs the latter three files. Both use published foreground `pbtest.py`, two CPUs
per shard, native threads one, four GiB, `/home/rob/pbt17`, per-test timeout
180 seconds, and shard timeout 3600 seconds. Commands, action keys, counts, and
primary audits are retained as `final-{x86,arm}.*` in the evidence directory.
This refresh grants no full-gate, deployment, or runtime-publication acceptance.
