# PrismaQuant CPU test environment — 2026-09-07

Issue: [PrismaBuild #356](https://github.com/RobTand/prismabuild/issues/356).

PrismaQuant's lightweight tests still execute an autouse fixture that imports
its package and therefore `compressed_tensors`. The fleet's `pb-cpu` environment
is for PrismaBuild infrastructure tests; it is not a PrismaQuant environment.
An already-provisioned project environment on dl380g10 was overlooked by the
report: `/home/rob/venvs/pq-cpu312/bin/python`. Other PrismaQuant work had used
it successfully. This maintenance qualified it against the reported shard,
completed its common project dependencies, and documented it in the operating
guide so CPU tests can use the x86 worker without occupying a GB10.

## Environment and changes

The target is dl380g10, Linux x86_64, Python 3.12.11. Key existing packages are
CPU PyTorch `2.10.0+cpu`, compressed-tensors `0.15.0`, Transformers `5.16.1`,
datasets `4.8.3`, accelerate `1.13.0`, pytest `9.1.1` and pytest-xdist `3.8.0`.
The [complete installed inventory](evidence/pq-cpu312-20260907.txt) records all
72 distributions; it is an observed environment, not a cross-platform lockfile.

An admitted x86 provisioning action added `gguf==0.19.0`, `pillow==12.2.0`,
`pytest-cov==7.1.0`, `pytest-timeout==2.4.0` and their new dependency
`coverage==7.16.0`. The resolver constrained every pre-existing distribution to
its installed version. Independent before/after inventories confirm no existing
version changed. `uv pip check` then found all installed packages compatible.
The infrastructure `pb-cpu` environment and the GB10 project environments were
not changed. No PrismaBuild worker restart or runtime publication was necessary
for the new packages: subsequent admitted actions imported the target environment.

Provisioning action:
`04c316400d2e436ad90f4c5d9797e5b15732ced949d39c02fb3e57d9629a20cf`,
CPU2/mem4, rc 0. CAS receipt:
`73fae9593ba19e0f8978aaf9b0bfb789836652b3e0ed713687383e6a82439a66`.

## Reproduction and negative evidence

- PB `a6292bd9f05b28017501f5db6f23529b6416917a67ca3eae62dbe9b5f8d81218`
  ran the current PrismaQuant shipcard test with `pb-cpu` on dl380g10 and
  failed collection with `ModuleNotFoundError: compressed_tensors`, rc 1.
  PrismaQuant source was main `d2bef4c4`. The original issue's source-closure
  shard failed at fixture setup; the same import can fail during collection
  when a test imports PrismaQuant at module scope.
- PB `97662f0207458d76c7926e1fc0d83b3f1915cc99f334e8ef26e85065ba993e6a`
  attempted a dry-run resolution of PrismaQuant's `[test]` dependencies while
  preserving the infrastructure environment. It failed because installed
  `fsspec==2026.7.0` exceeds datasets' allowed version. No packages were changed.
  This is why the existing project environment was reused.
- PB `ce2c27a0856211d4527a6cdc9d2f91a737d4e19bbe12765ef26dfe0ecfec57f7`
  passed 18 tests in `pq-cpu312`, but allocator collection failed on the
  separately required Tessera producer package. The action failed overall;
  those partial passes are not a green suite. No dependency gate was weakened.
- Before the additive provisioning, the reported source-closure shard plus
  format and shipcard checks passed 42 tests in action `1920eb34c0f8`, CPU8/mem16.
  This established that the missing compressed-tensors import was resolved by
  selecting the existing project interpreter.

## Final qualification

The immutable source was PrismaQuant PR #299 head
`b5124d75b76d7fd945807f60e44f23fa56ebd62f`, checked out separately from the
reporter's work. This includes the exact 24-test source-closure file in #356.
The final command used published PB fanout:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout /home/rob/tmp/pq299-pb356 \
  --python /home/rob/venvs/pq-cpu312/bin/python --tag x86 \
  --workers-per-shard 2 --threads-per-shard 1 --mem-gb 6 \
  --wait-s 600 \
  --json /home/rob/tmp/pb-maintenance-20260907-356/final-pbtest.json \
  tests/test_pq237_source_closure.py tests/test_shipcard_git_provenance.py \
  tests/test_format_registry.py tests/test_gguf_gptq.py
```

PB chose four independently admitted shards, each CPU2/mem6 with two pytest
workers and OMP/MKL/OpenBLAS threads bounded to one. All executed concurrently
on dl380g10 on distinct preferred CPU pairs, with GPU visibility disabled.
Aggregate reservation was CPU8/mem24. Broker telemetry was complete with no OOM;
per-shard CPU time was 14.4–21.7 seconds and wall time 7.8–15.0 seconds.
The scope contained 50 tests, all passed, no skips. Each shard emitted 28
existing PyTorch `script_method` deprecation warnings.

| Test file | Passed | Action key | CAS receipt SHA-256 |
|---|---:|---|---|
| `test_pq237_source_closure.py` | 24 | `fa2af8151584efd7576034253a6b92247722e69f44f4a34b0c63fa4e8a382615` | `19a2db08118af45702cc29e3c6b0102d2b13254c5cb17e575b66b910eac66f6d` |
| `test_shipcard_git_provenance.py` | 3 | `47115925760b5ae5140a3f38c1665727961f28b5dd6afc2b4fa74cf256a4abe9` | `e78b3c69aec70585a0fa82a730bbe0a11910151b2334aa7f5eefdb1599c5a058` |
| `test_format_registry.py` | 15 | `89fd94bbf0a9e9e01238a00768a3fed53aafce67be630b85e0dd61fde01388bd` | `63faf846ddc4c9ec718d2179fdf6318a300f087fded40bc9d3fed116460e53d4` |
| `test_gguf_gptq.py` | 8 | `22bdc5cd8e1b394c461042f28d08295e677f9e7cac02546ec931c2cc54269cd5` | `5bb045356d75f4c21ff3ae883854278059bcc1d331b9105c9f3b5f563a180dc6` |

For each action, maintenance independently read the terminal record and logs, required
`executed`/rc 0, and verified the canonical receipt and payload through
`PrismaBuildCAS.lookup()` and `result_path()`. Full terminal copies, payloads,
commands, inventories and negative results are retained under
`/home/rob/tmp/pb-maintenance-20260907-356/`; `verified-receipts.json` maps them.

This resolves the reported CPU-shard dependency failure and verifies adoption
by the admitted x86 worker. It does not establish that every PrismaQuant test
is CPU-portable. Tessera-dependent tests still need the source version their
contract pins; CUDA tests, model checkpoints and serving integrations retain
their own dependencies. The project interpreter exists only on the current x86
worker, so use `--tag x86`, not `--anywhere`. New x86 workers must provision and
qualify the project environment before running these commands.
