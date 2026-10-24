# Connected lifecycle CPU lane (#2100)

## Scope and provenance

The implementation is test-only: `tests/fullstack_lifecycle_harness.py` exposes
`run_case`, using one connected temporary queue/CAS/tier fixture, and
`tests/fullstack_lifecycle_progress.py` drives real subprocess semantic reports.
No production files, acceptance tests, or existing primitive suites are changed.

The fixture snapshots its supplied sources with `PrismaBuildCAS.snapshot_input`
and consumes `input_path` verification (also at completion). Source and manifest
inputs are included in sealed fixture actions. Real `stage_move.move` and
`ram_promote.promote` publish bytes; direct-library movement receipts are filed
with `PoolQueue.record_move`. Promotion holds a live claim and the source-handoff
barrier also has a live pinned reader. RAM is **file_backed**, never qualified
native RAM-broker evidence.

Readers use `client.covers_for_keys`, `acquire_for`, `open_pinned`, and release.
Pipe acknowledgments bound the reader, promotion and competing copy/egress
process barriers. Copy competition uses an immutable CAS input, an actual private
copy and production `StagedPublisher.publish`; it tests identical-content
adoption without replacement, not successful publication of divergent content.
Checks include the named file identity, material generation, refs and charged
holdings, rather than relying on an FD surviving unlink. Final release and
idempotent egress run independently of observer side effects.

Progress subprocesses call `progress.commit`; production `ProgressWatch.sample`
and claim-guarded `PoolQueue.write_lease` own acceptance. Replayed, regressing,
stale-token and invalid-unit reports must leave accepted progress unchanged.
Observers inspect the attempt-bound phase through `tier_cycle.live_consumers`
and the real remaining residency window. This uses worker acceptance interfaces,
not the complete worker launch/monitor loop.

## Recovery qualification boundary

Portable recovery probes missing/wrong scope certificates with the actual
certificate verifier. It kills actual reader children, verifies cleanup refusal,
and retains the unresolved refs, material, named bytes and charged occupancy.
It does **not** manufacture an empty-scope terminal or permission to release refs.
An interrupted reader is not evidence of an interrupted native worker attempt.

A production broker connection can succeed while a safe **isolated fixture**
prerequisite remains unavailable. No mutating requests are sent to that broker.
The probe separately inspects kernel mounts/delegation and attempts namespace
entry, UID/GID mapping and a private cgroup2 mount/delegation in a disposable
child. Namespace/mount changes never occur in pytest's namespace. Temporary
mount targets use TMPDIR. The observed x86 probe failed mapping the private
namespace with EPERM; this is retained as a blocked operation, not inferred from
an implementation omission or a missing broker.

Recovery is **unqualified**, with zero claimed adoptions. Native execution,
authoritative terminal history, execute/reap retry of the same publication,
identity-bound verified-result reads, and byte/accounting adoption qualification
are **not implemented or demonstrated** by this portable fallback. If all probed
isolation prerequisites succeed, the harness fails explicitly instead of
reporting native success or pretending an unavailable prerequisite. Passing the
portable reporting/refusal test is not positive restart qualification. The
isolated native broker/systemd lane remains separate work (see existing #2176
and #728); the reachable production service is not a fixture.

## Verification

Final command on x86 Python 3.14 and ARM Python 3.12:

```sh
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 python -m pytest tests/test_fullstack_connected_lifecycle_acceptance.py tests/test_fullstack_stage_ram_chain.py tests/test_fullstack_reader_boundaries.py tests/test_fullstack_claim_retry_primitives.py tests/test_fullstack_progress_retry.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Both arms: **130 passed** (x86 120.23s; ARM 42.89s). This includes the three new
acceptance cases, four unchanged primitive suites, and all four required guards.

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"dd3779a71e3638c1e2894f467f6122cb64e4ea95c636772bd2d5c51b6c45fe84","size":2842}},"operation":"476ab5c06d4f613e6e0ea694dd000895fce58510ca26fa95cb68b0f4c5317973","receipt":"48b93288ddb430bf70786f543b0d08a9494a2c9e9d219afe61807b59a0e2f634"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"2fdaae2bb61b2ca576355277524de8323ec349c75a0c37fb8ec9d7b84d9ee134","size":2845}},"operation":"c31846ab064ce5606d34e6ffe4edb64570f650483278e3110931f390173fdea4","receipt":"e2d82946abf5018006c51c37bb75317f7ea2946436794275b4b0df99737a5a05"}]

Read-only capability probe command (x86):

```sh
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 python -c 'import sys; sys.path[:0] = ["src", "tests"]; from fullstack_lifecycle_harness import _containment_probe; from pathlib import Path; import json; print(json.dumps(_containment_probe(Path.cwd()), indent=2))'
```

Observed `map fixture namespace uid/gid`: errno 1, Operation not permitted;
production broker connection succeeded; host delegation was not writable.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"619ef96bf8aabe84fe1bf5fd5fdff732e2d16f5d5846428fae033a43a6245ff1","size":2169}},"operation":"d026193f8a650508caf56d2d4e595a638545c33f883b34d3bfb81f49546fe285","receipt":"ac46c3753edddc9004ad274294e5c61a497267125ad9128f00e7fd8e5fe9aa4d"}]

During implementation an incomplete capability probe failed the three cases
rather than falsely claiming qualification, and initial SDK reader integration
failed on a missing worker field; both causes were corrected before the final
runs above. Subagents also reported refused nested wrapper attempts; those are
not validation evidence. All final commands above were plain commands through
`run_tests`, without nested submission. No native restart qualification ran.
