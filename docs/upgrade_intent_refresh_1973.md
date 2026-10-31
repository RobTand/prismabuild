# Upgrade-intent current-golden refresh for #1973

The namespace gate on `9284c8e1585709efaf4fcce8f2813464e68bf8fd` exposed an
intended current-byte mismatch in `upgrade-intent/armed`. W2 adds
`stage_ownership.py` to `FLEET_SCRIPTS` in `tools/fleet/publish_runtime.py`.
That source file's SHA-256 is the intent's coordinator identity.

The decoded wire changes only `/coordinator_sha256`:

- Old: `d02d7a2474241a318a00bd4a3b5e6d26df0c3dcb9c7328f1f223b891d6bb6c33`.
- New: `a898b4111065f90bccc62e2f10e5c7755aa6aec576461194c03042600dd0dfef`.

The new value equals the complete producer's `publish_runtime.py` bytes. The
agent identity and every other wire field remain unchanged. This follows the
[reviewed #1894 bell refresh](bell_golden_refresh_1894.md).

## Capture and strict adoption

Evidence is retained under
`/mnt/shared/pbtl-probe-tmp/pb-lead-dl380/1973/upgrade-intent-refresh/`.
Both actions freeze the clean committed producer
`9284c8e1585709efaf4fcce8f2813464e68bf8fd`; their Git-object audits verify that
only PB's closure stamp differs from that parent.

- Capture: `582f9773153d87cbe6fa51b69cbab39b45acedb5a7b5d047c68f8fd3ce4e0ce7`.
- Adoption: `f14f40e8ead7e7b59b8c6ccd8b76ee70731f7111da0754eb2a388b58ff79bb5f`.

Published foreground `pbrun.py` admitted each action with `--anywhere`, one CPU,
one GiB, priority `-10`, native threads one, and worker/client
`TMPDIR=/home/rob/pbt17`. Exact sealed commands are `capture-command.txt` and
`adoption-command.txt` in the evidence directory.

The capture runs `tests/compat/support/record_compat.py --generate` for exactly
`upgrade-intent/armed`. It independently reproduces one current and two
historical records and passes both retained consumer directions, with no
absent participants or failed direction. A sealed wrapper requires exactly the
coordinator-hash delta and binds all 14 capture files into the PB result.

The adopter authenticates those capture bytes against the verified result and
calls `tools/maintenance/refresh_compat_runtime.py --profile upgrade-intent`.
Its sealed wrapper restricts the broader source-identity profile to precisely
`/coordinator_sha256`; any other delta refuses. Capture inputs and primary-audit
files are declared through PB's dependency-digest fence.

The complete superseded current golden and capture manifest, plus tool-generated
provenance, are archived at:

`tests/compat/history-controls/superseded-upgrade-intent-563da2ff3aa4ad5d/`.

Only the selected current row changes. All nine unselected rollout current rows,
all 20 rollout historical rows, and all 63 historical corpus files containing
936 rows remain byte-exact. The archive is a superseded-current control, with
no new supported historical-generation credit. No golden was hand-edited.

## Primary audits and export authentication

`capture-primary-audit.json` and `adoption-primary-audit.json` are produced by
`tests/compat/support/audit_compat_actions.py --expect-head 9284c8e1585709efaf4fcce8f2813464e68bf8fd --require-clean`.
They verify terminal status and cleanup, canonical requests, source Git objects,
bundle/input blobs, immutable logs, runtime identities, CAS receipts, and result
blobs. The audit Git store is private to this evidence directory.

`capture-authentication.json` and `adoption-authentication.json` additionally bind
all exported file bytes and the strict-adoption report to manifests in those
verified result blobs. The adopter exports five files. Before integration,
each export's SHA-256 and size is checked against that verified result, the
archived complete files are compared to the local baseline, and the sole wire
delta is checked again. Integration copies the authenticated files without
rewriting them. `integration-authentication.json` records readback and complete
historical-corpus preservation.

## Validation scope

Final committed-source validation uses published foreground `pbtest.py` for
all nine `tests/test_record_compatibility*.py` files on the qualified x86
archival-replay lane, and the previous 45-file ownership, SDK, reader, public
surface, and guard population on x86/Python 3.14 and ARM/Python 3.12.
Each shard reserves two CPUs and four GiB, with one native thread. Client
`TMPDIR` and worker `--tmpdir` are `/home/rob/pbt17`. Commands, per-shard action
keys, counts, and primary audits are retained as `validation-*` in the evidence
directory. This refresh grants no deployment or runtime-publication acceptance.
