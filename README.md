# PrismaBuild

Deterministic action keys, immutable CAS, and remote dispatch for quantization
campaigns. **Stdlib-only by construction** — a worker node must be able to
verify and dispatch an action without the numeric stack installed; the action's
own absolute argv selects its pinned per-architecture venv.

## Status, honestly

The core is built and CPU-qualified (252 tests). **It has not yet dispatched a
real quantization stage.** Two things stand between here and that:

- **No queue.** By design — the design doc treats scheduling as someone else's
  problem. Nothing currently decides *which* worker takes *which* action.
- **No usable transport on this fleet.** `slurm.py` shells out to
  `sbatch`/`scontrol`, and SLURM is installed on neither Spark. `dagster.py`
  needs Dagster, also not installed. So both shipped transports are inert here.

`pqwork` — a 100 KB stdlib-only pull-queue running as a live systemd unit on
both Sparks — is the **predecessor** PrismaBuild is meant to replace. What
pqwork proves is that the direct pull-queue shape works on this fleet; what it
lacks is PrismaBuild's action-key determinism and CAS receipts, which is exactly
what quantization work needs (an artifact you cannot reproduce is quarantined).

## Provenance

Split out of `prismaquant` on 2026-08-31 from
`origin/codex/prismabuild-v4-qualified-20260831`. That branch was not chosen by
judgement: the entire PrismaBuild file set is **byte-identical across the seven
branches that carry it** (`prismabuild.py` blob `3f6d115`, 4277 lines), so the
implementation had already converged and there was no merge candidate to pick.
It is the earliest branch holding the final set, so it inherits no trellis work.

The `prismaquant.prismabuild.*.vN` schema strings are **deliberately not
renamed** — they are baked into already-published receipts and campaign state,
and the identity of a receipt is the value it carries. The namespace is history,
not a dependency: this package imports nothing from prismaquant.

## Layout

    src/prismabuild/core.py      4277 L  action keys, CAS, local execution
    src/prismabuild/slurm.py     3023 L  SLURM transport (inert here: no sbatch)
    src/prismabuild/dagster.py    915 L  Dagster transport (inert here)
    tools/prismabuild_worker.py          stdlib-only worker entry point
    tests/                       6788 L  252 passing, CPU-only

## Test

    PYTHONPATH=src python3 -m pytest -q tests/
