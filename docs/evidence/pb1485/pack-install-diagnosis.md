# SDK5 ARM pack install diagnosis, 2026-10-26

The failed install step now has a reproducible repair. Pack delivery remains
unqualified. This supplements the [supplier custody handoff](README.md).

The original action `47f0d2fc05361819a203e921072c0ae798c49b0bf76f97bc0b3fae7a2f75361e`
failed on sparky while installing Git027 into its overlay, before exporting an
archive. Its authenticated checkout bundle is SHA256
`8621e09b82c8a19c622b6fe279d6ac2f0d9f8472661ac1c4178c10c53b212ebc`,
14,901,386 bytes, snapshot `b50b6763de3467c45efb4de65349ed9836a3ee71`.
The retained `_pb1485/pack.py` captures pip output and raises on failure without
printing the captured stderr. Its terminal record preserves the rejected
command, but cannot establish the original pip diagnostic or version.

## Reproduced mechanism and repair

On Python 3.12.3 aarch64 with pip 24.0, the original named direct requirement
fails before cloning or building. The bare VCS requirement succeeds:

```diff
-prismabuild @ git+file:///scratch/source@027103d9a8417e06c7f13356e58779a313cd7088
+git+file:///scratch/source@027103d9a8417e06c7f13356e58779a313cd7088#egg=prismabuild
```

The [executable regression check](reproduce_pack_install.py) uses only disposable
scratch. It verifies the historical bundle digest before cloning it, uses its
vendored setuptools, and installs exact Git027 through pip. It then invokes the
historical snapshot's `verify_install` under the disposable interpreter, with
`-I` and a working directory outside the source checkout. The check requires
SDK5 and an import owned by the disposable interpreter. It preserves pip's
stdout and stderr. No installed metadata is manufactured or rewritten.

| Check | Action key | Result |
| --- | --- | --- |
| Original requirement, sparklina | `3ed46d1d6b6d50c0f73df935834610e8a3e813b24e3479e043f3d7e2bcbc7dc8` | Failed, rc 1, `ERROR: Invalid requirement` |
| Repaired requirement, sparky | `edf0d6e0ac156ffb7f6602e9df493528b128301a7d81682032dc917f74fb96a0` | Executed, rc 0; Git027, SDK5, 48 RECORD files verified |
| Repaired requirement, sparklina | `0012609a5ddb952abb2b22b8b7df8e6a724ac8f35ae96904609c4e8f9d56b314` | Executed, rc 0; Git027, SDK5, 48 RECORD files verified |

All actions declared one CPU, 2 GiB RAM, zero GPU, native thread limits of one,
and a 180-second payload limit. Cleanup completed. The two positive CAS
receipts and their payload and log digests were read and checked. Worker
attestation signatures were not independently verified. [Results and original
receipt links](pack-install-results.json) retain the exact identities.
Execution used worker generation `9dce28460e76-1792959923-b5904f02524e`;
installed candidate Git027 is a separate identity.

These are scratch SDK install checks with the historical guard. They do not
qualify a delivered interpreter, the current guard, Tessera b40, CUDA torch,
native dependency closure, relocation, live helper adoption or a GPU test.
The temporary interpreters were removed after each check.

Reproduce through the published client:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /path/to/prismabuild --tag gb10 --cpus 1 --demand mem_gb=2 \
  --env OMP_NUM_THREADS=1 --env MKL_NUM_THREADS=1 \
  --env OPENBLAS_NUM_THREADS=1 --env PYTHONDONTWRITEBYTECODE=1 \
  --filesystem-growth-gib 1 --timeout-s 180 --priority -10 -- \
  /usr/bin/python3 -B docs/evidence/pb1485/reproduce_pack_install.py
```

Add `--legacy-requirement` after the script path for the failing requirement.
The GB10 constraint declares the Python 3.12 aarch64 reproduction requirement.
The explicit sparklina run checked the repair on the host of the red control.
This command requires the retained historical bundle at its recorded CAS path.
It does not invoke the historical packer's main function.

## Review correction: require the pip24 parser

The initial recipe accepted any ensurepip version and fell back to bundled
pip 26.2.1. That changed the parser under test and could make the negative
control succeed. The recipe now requires exactly pip 24.0 from the disposable
interpreter before installing setuptools or selecting either requirement.
Missing ensurepip, missing pip and a different pip version refuse. There is
no fallback or download. The original fleet evidence above remains valid
because those logs record pip 24.0.

The bootstrap was extracted without a behavior change before adding the
[focused regression checks](../../../tests/test_pb1485_pack_install_recipe.py).
The checks inject pip versions and bootstrap failures at the subprocess
boundary. They run without a real installation or network access.

| Check | Action key | Result |
| --- | --- | --- |
| Prerequisite checks before repair, dl380g10 | `b46e908bb7daf35c8caa19009adbe79be1293fbc52f1b2488697f47f7e0b1883` | 4 failed, 1 passed; newer pip versions accepted and unavailable pip attempted fallback |
| Prerequisite checks after repair, dl380g10 | `62c6887f2f6bf8fd4c24a2e7eb6e73a520fa00afaa03977f6102427496ff3a8b` | 5 passed |
| Legacy requirement with enforced pip24, sparklina | `60eac6948c095bbfd4d440a0ec649fb6651d7e70d969b1f08660205a85ff2703` | Failed, rc 1, `ERROR: Invalid requirement` |
| Repaired requirement with enforced pip24, sparklina | `7a8b5deb7e6da7dd2afb5db4e387b876b0bd3e92c7ed0db72fa094083765508e` | Executed, rc 0; Git027, SDK5, 48 RECORD files verified |

[Review-correction evidence](pip-prerequisite-results.json) retains the terminal
identities, complete log hashes and successful receipts. All four actions used
one CPU, 2 GiB RAM, zero GPU, one native thread and a 180-second payload limit,
at priority -10. Cleanup completed. Both successful local-result claims pass
published `pb_verify_claim(hash_payload=true)` with `complete=true`,
`checks_passed=true` and no unavailable reads. Attestation signatures remain
unverified. These actions used existing worker generation
`9dce28460e76-1792959923-b5904f02524e`; this correction publishes no runtime.

## Remaining production and consumer gates

The retained builder cannot supply #2113 even after this repair. Its contract
exports pure Python installs and declares torch, NumPy and safetensors external.
It refuses native extensions and exports no relocatable interpreter. Reusing
that output as a complete CUDA interpreter pack would overstate its scope.

No supported, qualified supplier production route was established. The bounded
inspection covered the retained builder, the named supplier directory, current
PB submission and floor contracts, and Tessera's existing provisioner. The
[all-write prerequisite](README.md#remaining-acceptance-and-custody) remains:

- Authenticated filesystem mappings for every actual destination.
- A closed allowance population, including private and transferred holders.
- Complete concurrent growth bounds and operation-to-action custody evidence
  for build, temporary, log, archive and export writes.

The [existing #2045 packet](../../filesystem_identity_allowance_contract_2045.json)
names the Principal Engineer release train, Paperclip PRI-33, as the existing
supplier. Its physical NFS mapping decision belongs to Rob. The
[floor contract](../../filesystem_floor.md) says these facts remain missing.
The observed live floor mode file was absent, which defaults to `off`;
coordinator write-window support and a successful scratch check do not supply
the missing all-writers certificate. Production is held at that gate. No
activation or generic host privilege is requested.

Tessera's current provisioner qualifies B770 ancestry and uses a hardlinked
dependency overlay with an absolute `.pth`. It is not an established relocated
b40 archive route. Tessera and PrismaQuant are held by Rob's 2026-10-25 direction;
this work changes neither repository.

| Ticket | Current evidence and exact next condition |
| --- | --- |
| #2113 | Install mechanism repaired and checked on both Sparks. Pack absent. Existing supplier must establish the all-write production route before an exact Python 3.12 aarch64 / Git027 / b40 / CUDA closure can be built and unpacked outside the checkout. No shared environment is changed. |
| #2114 | Delivered-interpreter guard and GPU acceptance remain unqualified. The requested `tests/test_projected_staging_copy_2039.py` is absent from the accepted PQ source inspected in PRI-51. The current assignment preserves that finding and orders this GPU step stopped while PQ is held. Resume only with the delivered pack and an ordinarily accepted consumer revision containing the test; retain the separate GPU selection/authority gate. |
| #2115 | This is an updated prerequisite dossier. Same-commit shape, accepted consumer cutover, staging, canonical canary, admitted-job preservation and rollback evidence remain owed. |
| #1485 | End-to-end compatibility and actual launch/helper adoption remain open. Adoption and deployment are Rob's decision. |

All four tickets stay open. This handoff requests no runtime publication,
service restart, pin/default change, shared interpreter installation, producer
replay or deployment decision. [Merge acceptance](pack-install-acceptance.json)
is scoped to the diagnostic recipe and evidence.
