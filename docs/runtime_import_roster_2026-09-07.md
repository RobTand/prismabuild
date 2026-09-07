# Runtime import probe file roster — 2026-09-07

The real staged-generation import probe created three unlisted `.pyc` files
before sealing. Passing Python's `-B` switch makes the probe read without
adding bytecode, independently of the publisher's environment. This fixes
publication's file roster; it does not claim to resolve the NFS state traffic.

The regression removes bytecode environment overrides, imports actual stub
package files through `_probe`, and compares the complete before/after file
roster and bytes. On unchanged production source, PB action
`0774eace575e4142c59207ee5c1a40eadcc204ece42a6f4586a56d883918ca0c`
failed because three bytecode files appeared (DL380 CPU, exit 1, clean scope).
After the fix, action
`5479797e4f0ce9d7480ad9d0c7712f5051e2207fd65fc94f4c5ff81a80435c33`
passed all 14 publication tests in 6.32 seconds, with no skips or missing
collection (DL380 CPU, four workers, 4 GiB total, native threads one).

Command: `/home/rob/venvs/pb-cpu/bin/python -m pytest -q -n 4 tests/test_publish_runtime.py`.
Both actions used the published `pbrun.py`, portable placement and priority -10.
Verified the actual stdout, terminal exit and scope cleanup, canonical CAS
receipt `e577632a840c94da2acea26e1d4ef84dfbbce76dc7a7082d1de06004b742ed51`,
payload SHA256 `aaa07b4b4df34024323ef9ae14dde0c3e24e41c9cd649d05b487503ef78f0689`
and source bundle SHA256
`adec51c01e41724e8a29846f16c67ac995ac196c82bc4df1af11b0b56a430570`.

The first regression attempt `60c575613d01` timed out during NFS disruption
with no assertion output; it is not red-test proof. A later accidentally
host-constrained submission `6d6b0a22f90e` was withdrawn from READY with zero
attempts before submitting the portable action above.

Dedicated PB compile action `6dd2c641a15ec23a9aeefbe61c7c7a79f2cfddaa9442713d301fda534be2a723` also exited zero for the changed tool and test module.
