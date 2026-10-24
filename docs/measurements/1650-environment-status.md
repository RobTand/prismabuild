# #1650: verified environment state and outstanding owner dependencies

This is a partial verification record, not completion of O1 or O2.

## Source ownership (O1)

Read-only inspection of `tl/prismabuild-c06c9d5ef684-wp1` at
`676d61ea98243decaa98e0eda4a59869983d0329` confirms that
`tools/maintenance/pb_cpu/x86_64-cp314.json` contains versions, purposes and
wheel hashes for jsonschema, attrs, jsonschema-specifications, referencing and
rpds-py. The definition was introduced by `d8f6db8e97e233b3115f246e279f9d410e590c10`.
It is absent from this work package's base. Consumption requires the existing
#2111 owner's integration; this attempt does not independently transplant that
feature, create another manifest or alter its review status. TrustLadder must
coordinate that dependency with #2111. No publication was attempted.

## Current admitted x86 interpreter (partial O2)

A read-only probe through `run_tests`, with no installation, imported torch and
jsonschema successfully. It reported Python 3.14.4, x86_64, torch 2.11.0+cpu,
`torch.version.cuda is None`, and CUDA unavailable. Installed schema dependency
versions are jsonschema 4.26.0, attrs 26.1.0, jsonschema-specifications 2025.9.1,
referencing 0.37.0 and rpds-py 2026.9.1, matching the inspected owner definition.
The retained stdout identifies the interpreter. No reprovisioning is warranted
by this observation, and none was performed.

Command: `python -c 'import sys, platform, importlib.metadata as m; import torch, jsonschema; print("interpreter:", sys.executable); print("python:", sys.version); print("machine:", platform.machine()); print("torch:", torch.__version__, "cuda_build:", torch.version.cuda, "cuda_available:", torch.cuda.is_available()); print("schema_dependencies:", {name: m.version(name) for name in ("jsonschema", "attrs", "jsonschema-specifications", "referencing", "rpds-py")})'`

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"ab8684a09fc0091524f7dce19abbb7e8ca5159908ec0ea5b8de26adf5b47f6df","size":332}},"operation":"8434b0083a59a037c09c80317691b0aaea9d137f69ad7050e6fd1d5708e608bd","receipt":"bc787128aa394b93fe13715e7f0bd50d299ae1ac3b147ab480070d427dd3efca"}]

The exact unchanged Tessera selector
`tests/test_rung_allowability.py::GeometryHarvest::test_increment_reader_findings_preserve_correctness_holds`
was **not executed**. The supplied check environment transports this repository,
not the separate served Tessera checkout, and exposes no additional-source
transport input. A coordinator path is not a worker input. The execution/evidence
owner must supply supported Tessera source transport and retain its source and
interpreter identities, exact command, no-skip terminal pass and receipt/log
linkage without an action-local install. This record does not substitute the
import probe or the manifest test for that regression.

## Repository checks

Command on each arm:

```sh
python -m pytest tests/test_x86_jsonschema_dependency_1650.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

x86: 1 failed, 109 passed. ARM: 1 failed, 109 passed. The sole failure on each
arm is the missing #2111-owned definition, not an observed missing live import.
All existing guard cases pass; acceptance remains unmet.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"8d351facf30f08ce8c4735e143816239a7d3dc642c16de66a0a1dabcb217f511","size":3340}},"operation":"580a7de5c64dbdd767eb1d363b852ee0c49783d6d7558e71a80203a6da0f8700","receipt":"b136241f2efc3401a8a6fb5d3bc220e14fdc53969b5bbcd65295274056713534"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"9b6e29f15399ec77cb08eaa92bb28a41fc5e0b5f31c8721671f9544218100a0b","size":3343}},"operation":"b62e8c1f6446b2738aa6d9b97343107ef8c52ec27ac12bd13de69b8644186d28","receipt":"3d788c2ee2c2b06944578f8165962c164abed96ddf6ee29577db1f98684970e0"}]

## Preserved boundary (O3)

Only this report was added. No Tessera tests or correctness checks, acceptance
files, runtime dependencies, provisioning code or shared environments were
changed. Any future shared mutation remains with the environment owner under
the all-users-idle procedure in `docs/operating_prismabuild.md`.
