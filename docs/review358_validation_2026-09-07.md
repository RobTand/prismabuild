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

## Follow-up: Python 3.14 stat predicates

Root review identified that the first root-stat regression patched
`Path.exists` to raise rather than exercising its actual error handling.
Reading dl380g10's installed `/usr/lib/python3.14/pathlib/__init__.py:664` and
`:675` confirmed that `exists` and `is_dir` delegate to genericpath predicates.
`/usr/lib/python3.14/genericpath.py:16` and `:48` catch every `OSError` from
`os.stat` and return False, including permission and I/O failures.

Three further regressions were committed first at `c0f976e7e`: two real
`chmod(0)` traversal failures at the queue root and a terminal-directory symlink,
and a syscall-injected ENOENT followed by EACCES during the terminal-directory
recheck. Red PB action
`9ddae8bbb5c290e48a79dd592f4898c71a887ddc86830e71d81562ab7d3851c5`
failed all three in 1.26 s, with no skips, on dl380g10 (CPU 1, 1 GiB).

Fix `fc04a3b6b595f289a20457ab710e7b6e59b7b862` replaces those predicates with
explicit stat calls. Only ENOENT means missing; permission/I/O failures reach
the unavailable-section contract. Missing root, non-directory root, and missing
optional terminal-directory diagnostics retain their previous distinctions.
The original synthetic regression now patches stat as well.

Final integrated PB action
`141a4fbb50ceef81126264307bb2fd1abf303c8816236f6111fb0cd132cd7d4a`
ran the same compilation and test command above: **192 passed, four subtests
passed, no skips**, 208 fork warnings, 15.73 s. CPU 4, 4 GiB, portable placement,
priority -10 and native threads 1; dl380g10 was selected again.

Verified terminal exit 0, cleanup complete, canonical receipt, payload hash/size,
producer input binding, and sealed source bundle. Snapshot
`2805e8a45f3ed044278beb36eb1f4ec84b027f14` differs from the fixed head only by
its generated closure record. Receipt digest:
`2862ac6bbf1d4f1bf73d34516fd3edbfdf252749b2a69cda2185fdbff67fc363`.
Payload digest:
`81c0c726211d13b05e579e98b94e9a0558c241a7c938a0cc7bfc2bc29620c50c`.
Canonical receipt path is under `cas/actions/v3/14/` with the full action key.
The same evidence directory retains both new verification records and logs.

Root integrated these fixes with main `e20351e5f` in a separate worktree.
PrismaBuild action `8d62a6e6971cdc5a9536bb6a452d5e719e67041e5bd58212797a4eba382af959`
repeated the final compile and affected matrix: 192 passed, four subtests
passed, zero skips, 208 fork warnings in 19.86 seconds (DL380 CPU4/GiB4,
portable placement, native threads one, priority -10). Root independently
verified terminal exit, scope cleanup, canonical receipt, payload bytes and
source bundle; the tested snapshot differs only by its generated closure.
