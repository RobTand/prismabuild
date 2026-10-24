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
Each rejection is yielded immediately after its lease publication, so the
synchronous observer runs before the next report can overwrite that observation.
Observers inspect the attempt-bound phase through `tier_cycle.live_consumers`
and the real remaining residency window. The observer audit additionally reads
the live lease's rejection count/reason and unchanged accepted report at every
callback, alongside named-byte/ref/occupancy protection. This uses worker
acceptance interfaces, not the complete worker launch/monitor loop.

## Recovery qualification boundary

Portable recovery probes identifiers with the actual certificate verifier.
Both probes name the pinned reader ref's owner action and attempt nonce. The
missing-scope probe omits only scope_id and must refuse with `certificate names
no attempt`. The identifier-complete wrong-scope probe uses a different scope_id
and must reach the actual authority lookup, which refuses with
`no-broker-attestation-retain` in this portable fixture. Its result explicitly
records **unqualified**, `scope_mismatch_exercised: false`, the reason and the
blocked authoritative scope-mismatch verification operation. There is no broker
attestation here, so this does **not** qualify an attested-scope mismatch; it
establishes refusal/retention after an authority read, not missing-nonce parsing.
No positive authority is manufactured to advance validation.

Recovery kills actual reader children, verifies cleanup refusal, and retains
the unresolved refs, material, named bytes and charged occupancy. It does **not**
manufacture an empty-scope terminal or permission to release refs. An interrupted
reader is not evidence of an interrupted native worker attempt.

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

Repair verification command on x86 Python 3.14 and ARM Python 3.12:

```sh
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 python -m pytest tests/test_fullstack_connected_lifecycle_acceptance.py tests/test_fullstack_lifecycle_observer_audit.py tests/test_fullstack_stage_ram_chain.py tests/test_fullstack_reader_boundaries.py tests/test_fullstack_claim_retry_primitives.py tests/test_fullstack_progress_retry.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Both arms: **132 passed** (x86 140.93s; ARM 35.55s). This includes the three
acceptance cases, the two new observer audits, four unchanged primitive suites,
and all four required guards. The audits inspect each rejection's live lease,
not just a retained event, and distinguish identifier parsing from the real
scope-authority lookup/refusal. These runs validate the repaired harness; the
earlier 130-case runs lacked these audits and did not establish those cells.
The retained stdout for each repaired run was read back and verified.

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"2d9a5ad676ecfbcbab7ef39f1f3a2efeab3d6b9c92b69a5a37043a5d9efdb7d8","size":3066}},"operation":"b30dcd7065fa27eb72b035bbe132b2e75becf39ad18fb36f02ea2305815fcc51","receipt":"a828b19d14bd0bf9c24edf9235383a5829909d7eb8ded5adf7eebfb36df5fdba"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"537281c69d73c66ab39deaaa53c536f87645485a7f5bd4319bb4c60cc425df34","size":3065}},"operation":"7436564ab2f081e59cfd47770f115aac8fd42dbd002938aa6661f38f3ab32e74","receipt":"30340252c2a8c2cf41dc243530234e2b98b862aa6ff89a7902eeef98c00619c8"}]

Earlier read-only capability probe command (x86, before this repair; retained
stdout re-read for this review, not a new native qualification run):

```sh
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 python -c 'import sys; sys.path[:0] = ["src", "tests"]; from fullstack_lifecycle_harness import _containment_probe; from pathlib import Path; import json; print(json.dumps(_containment_probe(Path.cwd()), indent=2))'
```

Observed `map fixture namespace uid/gid`: errno 1, Operation not permitted;
production broker connection succeeded; host delegation was not writable.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"619ef96bf8aabe84fe1bf5fd5fdff732e2d16f5d5846428fae033a43a6245ff1","size":2169}},"operation":"d026193f8a650508caf56d2d4e595a638545c33f883b34d3bfb81f49546fe285","receipt":"ac46c3753edddc9004ad274294e5c61a497267125ad9128f00e7fd8e5fe9aa4d"}]

Earlier implementation diagnostics included an incomplete capability probe and
a missing SDK reader field, corrected before the original submission; those
runs did not detect the observer/scope-probe defects repaired here. Earlier
reported refused nested-wrapper attempts are not validation evidence. The
repair verification above used plain commands through `run_tests`, without
nested submission. No native restart or authoritative scope-mismatch
qualification ran; passing refusal/retention checks is not that qualification.
