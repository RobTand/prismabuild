# SDK5 retained supplier custody and adoption prerequisites

The retained wheel and source/helper tree are now readable and authenticated.
End-to-end [#1485](https://github.com/RobTand/prismabuild/issues/1485)
qualification remains unmet. This is a prerequisite handoff for the Principal
Engineer and the consumer/campaign owners, dated 2026-10-25.

The [2026-10-26 install diagnosis](pack-install-diagnosis.md) adds a failing
Python 3.12 aarch64 reproduction and repaired scratch installs on both Sparks.
Pack production and delivered-interpreter qualification remain gated.

The baseline remains the accepted packet
`local://pb1485-sdk5-publication-compatibility-predecision.json#3441`,
SHA256 `50e87c70ea4c5c64161c7f7e5e77a424685ac42b26a53fdb6c26f276b445f46b`,
16,732 bytes. Its original bytes were not recovered here. The accepted
[parent conclusion](https://github.com/RobTand/prismabuild/issues/1485#issuecomment-5974826036)
still applies: actual SDK5 launch/helper adoption is required. A global
repository-pointer move has not been proved necessary.

## Newly authenticated evidence

Published `pbrun.py` action
`ac7ddbc0909ec5dcfda5f8ead291af6a8e44e7d150e78f733cd44181125794fe`
executed on sparklina, attempt 1, return code 0. Demand was one CPU,
2 GiB memory and zero GPU, with native thread limits of one.
Cleanup completed. This action reads retained artifacts without installing
packages, importing the candidate, or rerunning producer/shape qualification.
Its receipt attributes execution to worker generation
`e9e9d9077212-1792715962-50f991688c86`, separately from candidate source027.

- [Result](audit-result.json), [original CAS receipt](audit-receipt.json),
  and [terminal-record projection](audit-run.json).
- [Published MCP claim verification](audit-claim-verification.json) reports
  `complete: true`, `checks_passed: true`, and matching payload SHA256.
  `attestation_verified: null` remains explicit. This MCP check does not
  independently verify the worker attestation.
- [Original supplier export receipt](supplier-export-receipt.json) binds
  action `2617cf36f98ca20c608cd6003d8960077099a723a7c8f37809caaf0facc5c438`
  to the [4,372-byte export manifest](supplier-export-manifest.json), SHA256
  `15398fef669ff483a88dfbb2ea859853b10c9fff46a2938718fb6cfb3f44e9b0`.
  The audit checked its successful terminal record, request input linkage,
  result bytes, and log publication receipt. All 15 exported custody files
  matched that manifest.

| Constituent | Observed identity and scope |
| --- | --- |
| Candidate source | Frozen PB `027103d9a8417e06c7f13356e58779a313cd7088`, accepted child `855032b1f3f2edcef0b6cc49c16078942ea1644e`. No candidate bytes changed. |
| Ordinary wheel | `prismabuild-0.1.0-py3-none-any.whl`, 1,104,571 bytes, SHA256 `9619e58d2be40ccd74a01d79847d73d5d643ffe770dc3a5f88c1029582832c55`. Wheel RECORD digests and all 39 package files checked against frozen source. Metadata names version 0.1.0, Python >=3.12, purelib and `py3-none-any`. |
| Exported Git-install metadata | Installer-produced `install-direct_url.json` names noneditable Git027. Its bytes match its exported RECORD row. The audit read 88 RECORD rows and checked 40 against independently read package/source or direct-url bytes. This is retained x86 Python 3.14 installation evidence, not a current installed-interpreter pass or ARM proof. |
| Source/helper tree | 1,194 tracked regular files matched Git027. The recomputed Git archive matches the delivered marker's `94727274fd0515c5f1ee7c031c90c1106793ef22b38b286caf44f1755675677d`, 23,685,120 bytes. This authenticates the delivered tree and source binding; no original archive file was recovered or republished. |

The supplier root is
`/mnt/shared/prismabuild-fleet/qualification/pb1485-sdk5-027103d9-20261004`.
The source/helper root is
`/mnt/shared/prismabuild-fleet/qualification/pq-pb-sdk5-20261009/027103d9a8417e06c7f13356e58779a313cd7088`.
These are retained delivery locations, not application defaults. The consumer's
existing pin configuration owns location selection.

Earlier [#2112 inventory](../pb2112/README.md) remains historical evidence of
its reader's unresolved access. This audit resolves the retained wheel,
exported metadata and helper-tree byte boundaries, not its entire CPU tuple.
Prior `1a775d74…` source qualification, `ffec97aa…` shape receipt and
`20cb79a0…` installed guard result were not replayed or upgraded into new proof.

## Consumer and helper ownership

Read-only source inspection used PQ main
`9f35f1e45254a099fba7bc075aa887a87343257e`, confirmed by `ls-remote`.
[Consumer-source observations](consumer-source.json) retain file identities
and bounded excerpts. These are source facts, not live environment probes.

- `prismaquant/staged_lease.py` requires SDK5 and exact Git027.
  Required lease names remain `injected_context`, `acquire_for`, `open_pinned`,
  `release`, `covers_for_keys`, and `READER_LEASE_TAG`.
- `source_generation.py`, `shape_runtime_prices.py`, and
  `tools/dispatch_joint_quanta.py` use the SDK's verified-result and
  standard-capture-command contracts. Selected native context remains a
  separate requirement from generic result reading.
- The package-coherence check and `sdk_submodule` keep internal Core/package
  imports under the same helper-owned package. Production resolves the
  launch-owned `PRISMABUILD_READER_HELPER_ROOT`; test-only installed injection
  cannot establish worker/proxy/helper adoption.
- `tests/pb_runtime_generation_pin.json` already pins SDK5/027 and its
  20-file union for seven connected fixture consumers. Its
  `runtime_generation` value is a source-fixture identity, not activation.
- `tools/fleet_interpreters.json` still records SDK4/dc480 host environments
  alongside b40, including both Sparks. Those entries describe historical
  attestations. Their source presence does not establish today's installations.
  No guard was relaxed to accept both SDK4 and SDK5.

Original remains the sole consumer source owner. This handoff changes none
of its source, fixtures, pins, interpreters, or launch defaults.

## Remaining acceptance and custody

| Ticket | Remaining prerequisite and owner |
| --- | --- |
| [#2113](https://github.com/RobTand/prismabuild/issues/2113) | Supplier must deliver an immutable, portable Python 3.12 aarch64 interpreter with genuine Git027 SDK5, exact Tessera `b40c93cb73745097e57a1ba4cf5b9eee166c759a`, CUDA torch and the native dependency closure. It needs original production linkage and outside-checkout target-layout evidence. No such pack was identified in the bounded search below. The x86 wheel/metadata cannot substitute. |
| [#2114](https://github.com/RobTand/prismabuild/issues/2114) | Consumer owner must identify an ordinarily accepted revision containing `tests/test_projected_staging_copy_2039.py`; that path is absent from inspected PQ main. Then use the delivered pack for pin-guard evidence on both Sparks and actual guarded GPU execution. Record the smallest selection and GPU-minute estimate before that separately authorized leg. No GPU action was submitted here. |
| [#2115](https://github.com/RobTand/prismabuild/issues/2115) | Principal reviews this bounded custody handoff. Campaign parent retains the eventual single Root packet, after same-commit shape, accepted consumer cutover, staged-generation/canonical-canary evidence, admitted-job preservation and rollback prerequisites exist. This packet is not deployment-ready. |
| [#1485](https://github.com/RobTand/prismabuild/issues/1485) | End-to-end compatibility/adoption remains open. Neither this static audit nor a source merge establishes live helper adoption or native/model acceptance. |

Before new pack production, the supplier needs an established artifact-production
route with its actual all-write identity/allowance evidence. The prior
[#2050](https://github.com/RobTand/prismabuild/issues/2050) route-qualification
boundary remains open, as does
[#2049](https://github.com/RobTand/prismabuild/issues/2049).
This is an evidence prerequisite, not a newly observed fleet outage or a request
for generic privileges. The current B770-specific Tessera provisioning tool
does not authorize a historical b40 environment builder.

Bounded search used the named supplier qualification directory, its export
action and CAS records, `/home/rob/fleet/inventory/*1485*`, and the current
consumer pin/interpreter catalog. The inventory contained a qualification copy,
intake and shape receipt. No ARM deliverable appeared in those indexes.
Hindsight supplied the failed pack lead
`47f0d2fc05361819a203e921072c0ae798c49b0bf76f97bc0b3fae7a2f75361e`;
its actual retained terminal record is failed, return code 1. This is a bounded
absence result, not a claim that no artifact exists elsewhere.

Held producer `d3966020c8e8b236ad858a0d4569f107448b8003373ca8d9da97dbbd745bdbc4`
and prior c437 producer/rollback identities remain historical and distinct
from the SDK5 consumer. Neither was rerun. Existing admitted jobs retain their
sealed generations. Any Rob deployment decision still belongs to Rob through
the campaign parent; this work requests no deployment decision.

## Reproduce the static audit

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /path/to/prismabuild --anywhere --cpus 1 --demand mem_gb=2 \
  --env OMP_NUM_THREADS=1 --env MKL_NUM_THREADS=1 \
  --env OPENBLAS_NUM_THREADS=1 --env PYTHONDONTWRITEBYTECODE=1 \
  --timeout-s 180 -- \
  /usr/bin/python3 -B docs/evidence/pb1485/audit_retained.py \
  --supplier-root /path/to/retained/supplier \
  --helper-root /path/to/retained/source-helper \
  --cas-root /path/to/cas --queue-root /path/to/queue
```

This command proves byte custody and source binding only. It does not launch
the old qualification, install a dependency, or select a new runtime generation.
[Merge acceptance](merge-acceptance.json) is scoped to this evidence handoff.
