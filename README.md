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
On 2026-09-04, `claude/pb-fixes-2026-09-04` (on top of `44b9f8f`) recorded
605 passed / 1 skipped in its CPU suite on sparky; that dated population is
evidence, not a permanent suite-count claim.

**The pull queue is slated for replacement by SLURM.** The 2026-09-04 review
found the memoization core worth owning and the scheduler half to be the
"roll-your-own queue dir" the spec had declined; see
`docs/scheduler_decision_2026-09-04.md` for the evidence, the alternatives,
the per-issue dispositions and the migration plan. Rob ratified it on
2026-09-04. Nothing changes on the fleet until he runs the install
(`fleet/slurm/install.sh`, as root, per box) and then the cutover
(`fleet/slurm/cutover.sh`, on his word, with an idle queue).

SLURM is installed on no box yet. The transport that will carry it is the thin
lane in `slurm_lane.py` (`pbrun --transport slurm`): one `sbatch` per sealed
action, the CAS receipt as the verdict, and a terminal record filed into the
same `pb-queue/done|failed|withdrawn` directories the pool's readers already
read. It has run against a real `slurmctld` and `slurmd` in a privileged
container on sparky (`fleet/slurm/smoke/`). `slurm.py` is the earlier
durable-state SLURM adapter, superseded by the lane and retained until
Phase 3 of the migration; `dagster.py` needs Dagster, absent here. `pool.py`
is the transport that runs today — a pull-queue on the shared NFS mount,
executing the *same* canonical worker argv SLURM would have submitted, so a
result does not depend on which transport delivered it.

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
starvation detector wired to nothing". Retries are bounded by `max_attempts`,
but never inferred from a deterministic action key: an argv can reproducibly
write external state before a later gate fails. Arbitrary `pbrun` commands
therefore default to one attempt; only an explicit `--retry-safe` contract plus
a larger `--max-attempts` opts in. Every success, failure, or lease loss
concluded from its live queue record is first-writer-published under
`attempts/<action-key>/<published-generation>/`, with immutable stdout/stderr
and an outcome record; the mutable ready/terminal record links the ordered
history, so a quick retry refusal cannot erase the causal failure. When a
finisher and stale reaper race, that same immutable first writer also decides
the ready/terminal destination, summary, and caller exit status; disagreement
is refused rather than combining two causes. Retries are
refused once `done` or `failed` carries the same generation: both stale reaping
and the claim boundary treat that outcome as terminal, while a later
`published_unix` for the same content-addressed key remains claimable. A
withdrawal likewise cancels the *run* and not the name —
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

The effective placement conjunction is semantic identity, not queue-only
metadata. `pbrun` normalizes and sorts the tags that actually landed (including
derived host pins) and seals them in action params before computing the action
key, result/stamp names, and container owner. Flag order and duplicate tags do
not move identity; a different admissible worker population does, so a
Sparky-pinned query cannot reuse a gx10 receipt for the same argv.
`pbrun --transport slurm --measurement --host-class gb10 -- <cmd>` seals a
measurement keyed on the host class `gb10` (a node Feature name) and sends it
as `--constraint`; the worker attests the class through the SLURM controller
before running. A measurement's numerics do not transfer across architectures,
so `--measurement` refuses without `--host-class`, and because the submission
binds the submitting box's toolchain, submit it from a box of that class.

Git-backed `pbrun` submissions are checkout-portable: the exact dirty tree is
sealed as a Git bundle in the CAS — the commit, its ancestry, and any branch
names `--snapshot-ref` asked for — and the claiming worker executes a
fresh local checkout of that commit, where `HEAD~1`, `git merge-base` and
`BASE...HEAD` resolve, so a diff-derived gate can run there.
A box-local source worktree therefore no
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
the Docker lifecycle was entered. The owner hashes one versioned pre-owner
identity containing the normalized command, checkout, demand, environment,
placement, determinism, retry policy, marker namespace, and deployed wrapper
path. Only the recursively derived owner and marker variables are excluded, so
an exact repeat keeps its owner while any supported semantic action distinction
moves it. Finish, withdrawal and stale reaping remove
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

    src/prismabuild/core.py       action keys, CAS, local execution
    src/prismabuild/slurm_lane.py the thin SLURM lane: submit one sealed action, wait, file its ending
    src/prismabuild/slurm.py      earlier SLURM adapter, superseded by the lane (retired in Phase 3)
    src/prismabuild/dagster.py    Dagster transport (inert here)
    src/prismabuild/pool.py       shared-FS pull queue (the one that runs today)
    tools/fleet/slurm_job.py      node side of the lane: materialize, run, publish the receipt
    fleet/slurm/                  SLURM configuration, install/verify/cutover/rollback scripts, the smoke
    tools/fleet/pbstatus.py       fleet status: nodes, jobs, recent endings
    tools/prismabuild_worker.py   stdlib-only worker entry point
    tools/fleet/pbrun.py          submit one command as a sealed action
    tools/fleet/pbwait.py         wait for submitted actions, report one table
    tools/fleet/pbcampaign.py     submit a manifest of actions, wait for all
    tests/                        CPU qualification (dated result above)

## Submitting work

One command, waiting for it:

    tools/fleet/pbrun.py --gpu --timeout-s 3600 -- ./stage.sh --shard 3

The same command, handed back as a key instead of waited for. `--detach`
prints one line of JSON -- the action key, the transport, the job id or queue
record, the generation and the paths its ending will be filed at -- and exits
0. An action already in the CAS prints `cache_hit` and submits nothing, and
one already running prints `attached` and joins that run rather than starting
a second copy of it:

    tools/fleet/pbrun.py --detach --gpu -- ./stage.sh --shard 3

Wait for any number of those keys, and get one table back. Exit 0 only if
every action's work is done:

    tools/fleet/pbwait.py 8fc86da0e13f 4b19a02cc551

Or submit and wait for a whole manifest at once. A manifest is a JSON list of
rows, each one argv plus a working tree, a demand, tags and an environment;
every row goes through `pbrun`'s own seal path, so a row's action key is the
key a hand-typed `pbrun` produces and re-running a manifest runs nothing:

    tools/fleet/pbcampaign.py manifest.json

`pbcampaign.py --help` and the module docstring carry the row schema.

All three take `--transport slurm` (or `PRISMABUILD_TRANSPORT=slurm`) to
hand the work to a scheduler instead of the pull queue. The result does not
depend on which one carried it.

## Test

    PYTHONPATH=src python3 -m pytest -q tests/
