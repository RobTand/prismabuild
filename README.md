# PrismaBuild

Deterministic action keys, immutable CAS, and remote dispatch for quantization
campaigns. **Stdlib-only by construction** — a worker node must be able to
verify and dispatch an action without the numeric stack installed; the action's
own absolute argv selects its per-architecture venv. A box-local executable
retains a host pin unless the caller explicitly names its worker class.

## Status, honestly

The core, shared CAS, pull queue, and three-worker fleet are deployed, and
`pool.py` is the sole execution plane for the current Tessera/PrismaQuant
campaigns. It has dispatched real quantization, test, and measurement stages.
On 2026-09-04, current `main` (`afdd938`) recorded 532 passed / 1 skipped in its
CPU suite; that dated population is evidence, not a permanent suite-count
claim.

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
starvation detector wired to nothing". Retries are bounded by `max_attempts`
and cheap by construction: re-running work that landed is a receipt lookup.
They are nevertheless refused once `done` or `failed` carries the same
generation: both stale reaping and the claim boundary treat that outcome as
terminal, while a later `published_unix` for the same content-addressed key
remains claimable. A withdrawal likewise cancels the *run* and not the name —
the marker is scoped to the generation it was filed against and a later
submission retires it into `withdrawn/superseded/` — because the action key is
a content hash, so re-submitting one is how anybody asks for the same work
again. What
pqwork lacks, and PrismaBuild has, is action-key determinism and CAS receipts —
which is what quantization work needs, since an artifact you cannot reproduce is
quarantined.

Submission placement distinguishes capability from liveness. The queue keeps
one latest declared-capacity offer per host: `pbrun` uses those retained records
to refuse a tag or demand no recorded box can ever fit, while the offer TTL is
used only to say which boxes are live enough to claim now. A capable box between
announcements therefore leaves the action to its declared `--wait-s`; it no
longer turns a bounded wait into an immediate refusal.

Git-backed `pbrun` submissions are checkout-portable: the exact dirty tree is
sealed as a shallow Git bundle in the CAS, and the claiming worker executes a
fresh local checkout of that commit. A box-local source worktree therefore no
longer pins ordinary work to that box, and edits after submission cannot change
what a retry executes. Source portability does not imply tool portability.
`pbrun` resolves argv[0] exactly against the declared `PATH`: a submitter-local
executable retains that host's tag, while an absent one refuses unless `--tag`
names the worker class that owns it. Direct path-shaped argv and
caller-environment values receive a conservative placement screen; it is not a
parser or proof for indirect application inputs. `--tag` owns those
dependencies for a worker class, and `--anywhere` is the caller's explicit
assertion that they are identical on every eligible worker. Commands that
embed the submitter checkout path refuse; new submissions from a non-Git
directory refuse instead of falling back to a mutable path. Legacy
path-addressed queue records remain readable while they drain, but `pbrun` does
not create new ones. The 512 MiB hard fleet ceiling bounds both the logical
materialized tree and its compressed bundle; the CLI may lower but never raise
it. Gitlinks, escaping symlinks, and active Git clean/smudge transforms refuse
because their working bytes are not carried unchanged by the parent bundle.
Worker checkout cleanup failure emits a warning and a durable record below the
local materialization root without changing an already-computed task result
into retryable work.

Container work is part of that reservation even after Docker reparents it away
from the action's process group. `pbrun` seals a derived owner id and places a
Docker shim first on `PATH`; the shim labels created containers and marks that
the Docker lifecycle was entered. Finish, withdrawal and stale reaping remove
and re-query those labels on the claiming host before returning tokens. A
remote check, a still-running create transaction, or a Docker error leaves the
claim and its capacity held rather than admitting work on top of an unverified
GPU payload.

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
    tests/                               CPU qualification (dated result above)

## Test

    PYTHONPATH=src python3 -m pytest -q tests/
