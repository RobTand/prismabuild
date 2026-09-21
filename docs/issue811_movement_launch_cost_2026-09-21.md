# A tier-host movement action's fixed cost (#811)

Diagnosis of issue #811: why a movement action that moves 8,356 bytes costs
3.9 s to 5.0 s from claim to terminal record on `dl380g10`, and where that time
goes. Everything below comes from production queue records and one admitted
PrismaBuild measurement replay. No production code was changed; the proposed
change is a contract decision recorded here for the owner of `pool`/`core`.

The diagnostic helper that produced the replay lives at
`tools/maintenance/diag_811_movement_launch_cost.py`.

## Production records: the split

Four actions of the 2026-09-21 one-token Stage A cycle `a3` (owner
`7a5571585944`, generation `d1c640e74d97`), read from their immutable attempt
records. `prelaunch` is `claimed_unix` to the resource scope's start, recovered
from `resource_telemetry.sampled_unix - wall_seconds`; `launcher` is the
launcher's own `elapsed_s`; `tail` is the remainder through the terminal
record.

| Action | claim to terminal | prelaunch (materialise) | launcher | tail |
|---|---|---|---|---|
| mover `81db7b3b6321` | 3.874 s | 2.097 s | 1.564 s | 0.213 s |
| mover `a6ced331c163` | 3.937 s | 2.127 s | 1.628 s | 0.181 s |
| egress `c1bb87224251` | 5.045 s | 2.236 s | 2.587 s | 0.222 s |
| egress `bdf36d7d8123` | 5.035 s | 2.151 s | 2.671 s | 0.213 s |

The box was lightly loaded in that window (Netdata `system.cpu`
`busy_percent_mean` 3.4–5.9 %, `psi_some_avg10_max` 0.0).

What each term is:

- **prelaunch** is `pool.PoolQueue.execute`'s `_execution_checkout` block
  (`src/prismabuild/pool.py:15504`): materialise the sealed checkout snapshot
  into a private tree (`materialize._execution_checkout`,
  `src/prismabuild/materialize.py:202`) plus claim bookkeeping, scope intent
  and lease. It is 54 % of a mover's fixed cost and 43 % of an egress's.
- **launcher** is the contained `worker run-local` launch. The mover payload
  self-reports 0.26 s for the copy itself; the egress payload reads 132–136 MB
  (`process_io.rchar`) to delete four entries. The fixed part of the launcher
  is measured below at 0.88–0.97 s.
- **tail** is the finish path: resource sampling, the box window (Netdata,
  bounded by a 2 s shared budget) and the terminal record write.

Both movement kinds carry the *same* sealed 32,899,482-byte snapshot
(`sha256 97bb30f2…`) and the same closure stamp, so their materialisation cost
is identical; the egress is slower only in its payload.

## Measurement replay

One admitted `pbrun` action on `dl380g10` (2 CPUs, 3 GiB, `--profile sample`,
priority -10) replayed the exact production request's launch path on a private
queue and CAS with a trivial payload, interleaving two arms:

- **A** `checkout_snapshot`: the current path, materialise the 33 MB bundle per
  action.
- **B** `checkout_root`: reuse one already-materialised tree, with a closure
  stamp derived from that tree (see "why reuse is refused today").
- **A2** arm A under the production checkout root
  (`/home/rob/tmp/prismabuild-checkouts`), to separate path effects from load.

Canonical action `27b76fde312e…`; medians of 3/3/2 reps:

| Arm | claim to terminal | prelaunch | launcher | git fetch | git checkout | rmtree |
|---|---|---|---|---|---|---|
| A | 5.952 s | 4.812 s | 0.874 s | 3.93–4.67 s | 0.34–0.37 s | 0.07 s |
| A2 | 5.868 s | 4.950 s | 0.788 s | 4.03–4.77 s | 0.35–0.38 s | 0.07 s |
| B | 0.901 s | 0.001 s | 0.791 s | — | — | — |

An earlier replay (`3fd1f284f1d2…`) under a similar load agrees: A 6.193 s,
B 1.007 s, delta 5.19 s. A2's numbers equal A's on the same filesystem
(`st_dev` 31, same `f_fsid`), so the replay's slower materialisation is the
box's load, not the tree's location: during the canonical replay Netdata
showed `busy_percent_mean` 6.3–10.1 % and `psi_some_avg10_max` 11.3–24.4 %,
against 0.0 in the production window.

**The matched delta is 5.05 s per action** (A − B), all of it materialisation:
`git fetch` of the 33 MB bundle is 89 % of the prelaunch term,
`git checkout --detach` another 7 %, and the 33 MB input-digest read
(`core._file_identity_nofollow`), the private-tree `rmtree` and the claim/
scope/lease bookkeeping make up the rest. Under the production window's lighter
load the same term was 2.10–2.24 s.

A direct, uncontained `run-local` launch of the same synthetic action (no
broker, no `resource_exec` proxy) costs 0.47–0.50 s, of which
`import prismabuild.core` is 0.11–0.12 s (`python -X importtime`). The
contained launcher's fixed part (0.79–0.87 s) is therefore ~0.3 s of
broker/proxy plus the same worker path.

## Profiler artifact

`--profile sample` (py-spy 0.4.2, 100 Hz, `--subprocesses`) filed an 82,993-byte
speedscope profile of the canonical replay action:
`cas/blobs/a8/a8ddf7e2ce1025e6d410b99fff15f748e657b181fd3e86d559f10d598795e434`
(476 samples). Python self time over the run: import machinery 1.31 s,
`core._file_identity_nofollow` 0.61 s (hashing the bundle),
`subprocess._try_wait` 0.50 s, `shutil._rmtree_safe_fd_step` 0.40 s. The
dominant wall cost — the native `git fetch` — is a non-Python child, so py-spy
cannot attribute it; the per-command spans above do.

## The exact bottleneck

`pool.PoolQueue.execute` materialises a full Git checkout for **every**
movement action, although the action's payload never reads it:

- The movement payload's argv is an absolute path under the sealed runtime
  generation (`…/tools/stage_move.py`, `…/tools/stage_release.py`), and both
  scripts insert `generation_root(__file__)/src` into `sys.path`. The
  checkout's `cwd` is unused.
- The only checkout bytes the worker reads are the 153-byte pbrun closure
  stamp, and `git_checkout_identity` (`src/prismabuild/core.py:1727`) walks the
  tree to re-prove it.

So ~2.1 s of tier-host time per action (5.1 s under load) is spent unpacking,
checking out and deleting a tree the action does not use — 7–9 hours over the
512-sample Stage A estimate.

## Why reuse is refused today

`core._verify_pbrun_checkout_identity` (`src/prismabuild/core.py:2148`) holds a
path-addressed `fleet/pbrun` action to its closure stamp: with no
`params.checkout_snapshot`, it requires
`git_checkout_identity(root) == stamp` (`core.py:2222`). The stamp the producer
sealed describes its own (dirty) PrismaQuant tree, so no clean materialised
tree can match it. The first replay's arm B failed with exactly that refusal;
the reported arm B passes only because the harness derived a stamp from the
reused tree itself.

## Proposed change (not implemented here)

For a `fleet/pbrun` action whose code closure is exactly one stamp and whose
`task.argv` resolves entirely under the attested runtime generation root, skip
checkout materialisation: hand the worker a minimal root holding the closure
files (the stamp) and treat the attested runtime — already verified from
`worker_script` and the runtime receipt — as the executed code identity.

Touch points: `pool.execute` / `materialize._execution_checkout` (what root is
yielded), `core.run_local_action` / `_verify_pbrun_checkout_identity` (what a
runtime-only closure must prove), and the `--checkout-root` cwd it passes.

Measured effect of removing the term, as an upper bound: the contract-legal
reuse arm (B) still runs the worker's identity and closure checks and costs
1.007 s claim-to-terminal against arm A's 6.193 s under the same load. Scaled
by the production records' own decomposition, a mover would fall from ~3.9 s
to ~1.8 s and an egress from ~5.0 s to ~2.9 s. A "skip entirely" implementation
would land below arm B, not above it.

The security-relevant half is a contract question, not a measurement one: the
closure is what binds executed bytes, so accepting a runtime-only closure means
accepting the attested runtime as that binding. That decision belongs with the
owner of `pool`/`core`.

Cheaper alternatives, in descending order of scope: cache the fetched pack per
snapshot digest (removes `git fetch`, keeps checkout and `rmtree`, ~1.6 s of
the 2.1 s); or reuse a materialised tree (what arm B measures) if the stamp
rule above is relaxed.

## Evidence

| Artifact | Where |
|---|---|
| Canonical replay action | `27b76fde312e23202f8370e24ce57b77393410d3f456a6761a2072acb9fb16fe` |
| Replay report JSON | attempt stdout of the key above, `DIAG811-REPORT-BEGIN…END` |
| py-spy speedscope | `cas/blobs/a8/a8ddf7e2ce1025e6d410b99fff15f748e657b181fd3e86d559f10d598795e434` |
| Corroborating replay | `3fd1f284f1d26d1308e161fef6335250377c7ea5f56c05b4e098ec153252b2c6` |
| First replay (arm B refusal) | `398fd4e63ba8cc080c1c1deb490f7187e3b6b0af63f1bc6c4a031567121f897c` |
| Direct-launch action | `6ead22d1a705eb40913916568519fe742ded2c540a421d74f9ef11b08e60196c` |
| Production mover/egress records | `pb-queue/attempts/81db7b3b…`, `…/c1bb87224251…`, `…/a6ced331…`, `…/bdf36d7d…` |
| Local result claim | `cas/local-results/v1/2e/2ef8a5e9…` (checks passed) |
