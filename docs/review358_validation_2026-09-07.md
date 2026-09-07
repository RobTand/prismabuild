# PR 358 independent review qualification

Reviewed author head `d9657efb5ff68cb22188102fab7388b9b8b0d89a` in an isolated
worktree. Production fixes were validated at
`560f701389b8d58c067bfeecc14053d7345c3506`.

The review corrected four observed defects:

- Unreadable ending records and inaccessible terminal directories still
  certified `complete: true`. Records retain their diagnostic rows; failed
  directory reads now make the endings section unavailable.
- SIGINT, SIGTERM aimed only at the parent, and interrupted waits left a reader
  and/or pipe behind. Cleanup now reuses the worker signal-unwinding context,
  closes the pipe, and kills/reaps the exact child with bounded grace. Any
  survivor is identified on stderr even when cancellation prevents JSON output.
- Default transport metadata was read before the deadline started. Its reader
  now shares the pool census budget, with unknown transport on reader failure.
- The empty-endings queue-root note caught stat failures internally, hiding
  them from completeness. The bounded census now requests error propagation.

## PrismaBuild evidence

All actions used portable placement (no tags), priority -10, CPU only, and
OMP/MKL/OpenBLAS threads set to 1. The fleet chose dl380g10, Python 3.14.4.

| Action | CPU / GiB | Verified result |
|---|---|---|
| `77b65ba072b889b811af177f87ce95527e1fba578b9859f616fe8ba2c5d4c2b1` | 1 / 1 | Red: six regressions failed in 3.55 s on author production code. |
| `fc628e23630c3d4ff1f6f8edf7838c24369374598a47f80679a320bd5a72dbda` | 1 / 1 | Red: queue-root stat regression failed; two endings regressions passed. |
| `3d6b3b68200a9cd6189cda4638820f9e4bc25e7b83d8ee02e1dbda0a8871257d` | 4 / 4 | Intermediate: 186 passed, two existing unreadable-ending tests still expected exit 0. Those assertions were updated to the intended exit 3. |
| `e083a7a22d684e52f41edcd0d0181b6eb82936e6f65346423c9cdfa03a900bd8` | 4 / 4 | Green: compilation succeeded; 189 passed and four subtests passed in 27.55 s; no skips or missing collection. |

The final action executed:

```sh
/home/rob/venvs/pb-cpu/bin/python -m py_compile tools/fleet/pbstatus.py
/home/rob/venvs/pb-cpu/bin/python -m pytest -q -n 4 \
  tests/test_pbstatus*.py tests/test_pbmetrics.py \
  tests/test_design_doc_line_references.py \
  tests/test_tools_do_not_run_on_import.py \
  tests/test_default_transport_travels_with_the_generation.py
```

Submission used the published `pbrun.py --anywhere --cpus 4 --demand mem_gb=4
--priority -10`, with explicit native-thread environment settings. The shell
joined compilation and pytest with `&&`, so a compile failure could not pass.

The final canonical receipt is
`/mnt/shared/prismabuild-fleet/cas/actions/v3/e0/e083a7a22d684e52f41edcd0d0181b6eb82936e6f65346423c9cdfa03a900bd8.json`.
Receipt digest: `9cdee9d179b37bb6500099c6a45d1eb05872201f61fbfcfff06eb7fbaa361930`.
Payload digest: `7d5c6be7f27b6e8fd37a4b7860a2376c0822c324fd4caeaee25d08c7ba61eae6`.

Verified terminal exit 0, actual stdout, canonical receipt, payload length and
SHA-256, snapshot bundle SHA-256, producer input binding, and complete scope
cleanup. Fetching the sealed bundle confirmed that snapshot commit
`9c38f69f632663ac41fa00dd6a0b681b53481458` differs from tested head only by its
generated `.pbrun-closure` record. The same source verification was performed
for both red actions. Local retained logs and verification JSON are under
`/home/rob/tmp/pr358-review-evidence/`; snapshot recovery refs are under
`refs/pr358-review/` in the repository.

The author's latest green action `8b7ba0c027e2` was also checked against its
terminal, source-parent metadata and payload digest: 77 tests passed. It did
not cover the new review regressions.

## Qualification limits

These are private-queue and injected blocking/failure tests, not a new live
hard-NFS/D-state experiment. No performance improvement or NFS repair is
claimed. The final suite emitted 200 Python fork-from-threaded-process
deprecation warnings under pytest-xdist; it is not a qualification for arbitrary
threaded library callers.

The bound covers pool census reads and the default transport lookup after
imports. Imports, blocked output delivery, cleanup grace and explicit SLURM
commands/lane-root reads remain outside it, as stated in the operating guide.
SIGKILL cannot unwind cleanup. Missing terminal directories remain compatible
with transports that have not filed outcomes; unreadable existing directories
are failures.
