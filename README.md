# PrismaBuild

Deterministic action keys, immutable CAS, and remote dispatch for quantization
campaigns. **Stdlib-only by construction** — a worker node must be able to
verify and dispatch an action without the numeric stack installed; the action's
own absolute argv selects its pinned per-architecture venv.

## Status, honestly

The core is built and CPU-qualified, and `pool.py` gives it a transport that
runs on this fleet (277 tests). **It has not yet dispatched a real quantization
stage** — that is the next step, not a done one.

Both originally-shipped transports are inert here: `slurm.py` shells out to
`sbatch`/`scontrol` and SLURM is installed on neither Spark; `dagster.py` needs
Dagster, also absent. `pool.py` is the third transport — a pull-queue on the
shared NFS mount, executing the *same* canonical worker argv SLURM would have
submitted, so a result does not depend on which transport delivered it.

`pqwork` — a stdlib-only pull-queue running as a live systemd unit on both
Sparks — is the **predecessor** PrismaBuild replaces. Its NFS-safe primitives
(claim-by-rename, lease heartbeat, stale requeue) are ported into `pool.py`
because they were argued out against real NFS behaviour. Its **reservation
ledger is deliberately not ported**: the one documented live defect on this
fleet is an admission/reservation failure, not a transport one, so reproducing a
naive static reservation model would import the known failure mode. What pqwork
lacks, and PrismaBuild has, is action-key determinism and CAS receipts — which
is what quantization work needs, since an artifact you cannot reproduce is
quarantined.

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
    src/prismabuild/pool.py       449 L  shared-FS pull queue (the one that runs here)
    tools/prismabuild_worker.py          stdlib-only worker entry point
    tests/                               277 passing, CPU-only

## Test

    PYTHONPATH=src python3 -m pytest -q tests/
