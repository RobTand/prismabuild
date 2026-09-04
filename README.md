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
ledger is not ported but rebuilt**, from the one documented live defect on this
fleet (`/mnt/shared/pq-ops/starvation/REPRO-2026-08-30`) rather than around it:
capacity is held as rename-acquired tokens, acquired inside `claim` and released
in `finish` — and in `withdraw`, which is the same release path reached by an
operator changing their mind rather than by the work ending — so a holder is
always *running* and never waiting; the hold-while-gated circularity has nowhere
to form. A finishing worker carries its claimed-record snapshot into `finish`,
so a reaper winning the claimed-file race cannot erase the host needed to
return that reservation; old terminal orphans are reclaimed only by an
explicit verifier that refuses live claims, leases, non-success outcomes,
multiple holders and host disagreement. Denials age an item to the front of the
ready order, and past `STARVATION_FLOOR` a denied item withholds the host
instead of being overtaken, because "an eviction counter that only counts is a
starvation detector wired to nothing". Retries are bounded by `max_attempts` and
cheap by construction: re-running work that landed is a receipt lookup. A
withdrawal cancels the *run* and not the name — the marker is scoped to the
generation it was filed against and a later submission retires it into
`withdrawn/superseded/` — because the action key is a content hash, so
re-submitting one is how anybody asks for the same work again. What
pqwork lacks, and PrismaBuild has, is action-key determinism and CAS receipts —
which is what quantization work needs, since an artifact you cannot reproduce is
quarantined.

Submission placement distinguishes capability from liveness. The queue keeps
one latest declared-capacity offer per host: `pbrun` uses those retained records
to refuse a tag or demand no recorded box can ever fit, while the offer TTL is
used only to say which boxes are live enough to claim now. A capable box between
announcements therefore leaves the action to its declared `--wait-s`; it no
longer turns a bounded wait into an immediate refusal.

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
