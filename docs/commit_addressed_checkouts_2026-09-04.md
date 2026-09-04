# Commit-addressed checkouts — design, 2026-09-04

**Status: DESIGN. Not implemented.** This is the fix for prismabuild issue #5
step (2). Steps (1) and (3) — the pin said out loud at submit, and a queue
metric for how wide the waiting work is — landed on 2026-09-04 and are
described here only where they bound the design.

## The problem, measured

An action carries `checkout_root`, an absolute path (`pool.PoolQueue.publish`,
written from `pbrun`'s `checkout_root=str(cwd)`). When that path is a box-local worktree —
`/home/rob/tmp/ts101`, which is what an agent naturally creates — the action
must be tagged to the box that holds it or it will be claimed by a worker that
cannot see it. `pbrun.placement_tags` derives that pin from the path, and
correctly (`pbrun.placement_tags`).

Read off the live queue on 2026-09-04, over the 391 items in `ready`,
`claimed`, `done` and `failed`:

| items | tags | checkout_root |
|---:|---|---|
| 121 | `gb10` | `/mnt/shared/prismabuild-fleet/checkout` |
| 114 | `sparky` | `/home/rob/tmp/ts101` and siblings |
| 106 | `x86` | `/mnt/shared/tessera-x86` |
| 28 | *(none)* | *(none recorded)* |
| 15 | `gx10-6b77` | `/home/rob/tmp/ts91/tessera` |
| 7 | other | — |

131 items — a third of everything the fleet has ever been given — carried a
hostname tag, and 129 of those pins were a consequence of a path rather than
of the work (the other two are shared checkouts pinned on purpose).  Recounted
at 04:50 the same night, over 516 items: 243 host-tagged, 208 of them by a
box-local checkout, 192 of those pinning `sparky`.  The pin is growing, not
holding steady, which is the argument for building (2) rather than living with
the notice. The new census reports it directly; live, while
this was being written, with all three boxes announcing:

    ready 18, 18 on exactly one box (sparky 18), 0 on more than one, 0 on none

That is a fleet three boxes wide and one box deep.

`/mnt/shared` is not the answer for a working tree. It is NFSv4.2 mounted
`local_lock=none` (`/proc/mounts`, verified 2026-09-04); a dozen agents
building in worktrees on it is slow and a locking hazard, and
`TRITON_CACHE_DIR` must stay box-local whatever else moves.

## The shape of the fix

**An action names a git commit. The claiming worker materialises or reuses a
worktree at that commit under its own local scratch, and runs there.**

Nothing about the work changes; what changes is that the *location* stops
being part of the request. Three boxes can then each answer the same request,
which is what makes the queue's placement free to do its job.

## Step zero: the action key binds the submitter's path

The coordinator's framing — "the CAS makes re-execution on a different box a
correctness non-event" — is true of the CAS and **not yet true of these
actions**. The submitter's absolute path is bound into the action key in three
places, so the same work submitted from two boxes is today two different
actions with two different keys:

* `params.cwd` is `str(cwd)` (`pbrun`'s action body: `"params": {"command": ..., "cwd": str(cwd), ...}`), and `params` is part of the
  sealed body (`core._ACTION_BODY_KEYS`, `core.seal_action`);
* the closure stamp's *name* embeds a fingerprint over `str(cwd)`
  (`pbrun._closure_names`' sha256 over `str(cwd)`), so the closure member's path differs per box;
* the stamp's *content* records `{"cwd": ...}` (`pbrun`'s stamp payload: `json.dumps({"cwd": str(cwd), **identity}, ...)`).

So step zero of (2) is to rebind the key from *(path, tree delta)* to
*(repository identity, tree commit)*. After that a cache hit across boxes is
correct rather than lucky, and it is the same property the design already
claims for concurrent workers: "Workers in distinct validated checkouts may
execute task argv concurrently and converge through ordinary CAS publication"
(`docs/design.md`). The output-lock claim binds the resolved checkout
(`core._local_result_claim_body`), which is what allows two boxes to hold two claims
without contending.

## Publishing the commit without publishing the tree

The submitting box's commit exists only in its own worktree. Boxes need the
*objects*, which are write-once and read-only afterwards — a very different
NFS load from a working tree.

* A bare repository per source repo at
  `/mnt/shared/prismabuild-fleet/git/<repo>.git`.
* `pbrun` pushes the tree commit there as `refs/pbrun/<sha>`.
* The item carries `checkout_commit`, `checkout_origin` and a repo identity;
  it carries no `checkout_root`.

Git's ref update takes its lock with `O_CREAT|O_EXCL`, which is the primitive
this fleet already relies on for token minting (`pool.ResourceLedger`'s token mint). That it
holds on this mount for `refs/` **must be verified, not assumed** — the mount
is `local_lock=none`, and every concurrent submitter writes a *different* ref
name here, so the contended case is the packed-refs rewrite rather than the
ref itself. Qualify it the way `docs/two_host_qualification_2026-08-31.md`
qualified the rendezvous: two boxes, real concurrency, an explicit predicate.

## Dirty trees are the norm, so the commit is synthesised

Agents submit from dirty trees constantly; `pbrun` has a whole delta digest
for it (`pbrun._git_identity`), and 12 of 50 live failures were closure drift
between sealing and running (`tools/fleet/pool_reset.py`). Requiring a
clean tree would make the feature unusable.

Synthesise a commit from the working tree instead, without touching any
branch or the user's index:

```
GIT_INDEX_FILE=$scratch/index git add -A          # untracked included
tree=$(GIT_INDEX_FILE=$scratch/index git write-tree)
commit=$(git commit-tree $tree -p HEAD -m "pbrun <fingerprint>")
git push <bare> $commit:refs/pbrun/$commit
```

`git stash create` is the tempting shortcut and is the wrong one: it does not
carry untracked files, and `pbrun` learned the hard way that an untracked file
edit must move the action key (`pbrun._git_identity`'s untracked digest). Include the same
exclusions the delta digest already applies — the closure stamp and the result
logs — or every submit will produce a new tree commit for its own droppings.

## Worker side

1. Per-box bare mirror at `/home/rob/.cache/prismabuild/git/<repo>.git`;
   `git fetch <origin> <sha>` when the object is absent.
2. `git worktree add --detach /home/rob/tmp/pb-trees/<repo>/<sha> <sha>`,
   under a per-sha lock so two loops on one box do not race the same
   materialisation. Reuse when it is already there — a second action at the
   same commit costs a lock and a stat.
3. Run exactly as today: `worker_argv` gets `--checkout-root <that path>`
   (`pool.PoolQueue.worker_argv`), and everything downstream is unchanged.
4. The closure check keeps its teeth. The materialiser writes the stamp by
   recomputing `_git_identity` **from the tree it has just built**, exactly as
   `pbrun` does at submit; `core.verify_code_closure`
   then compares that against the action-pinned bytes. A worktree that landed
   on the wrong commit, or that is dirty, produces different bytes and the
   action refuses. This is a real check because the stamp is derived from the
   tree, not copied from the action.
5. An LRU sweep bounds the scratch. It must refuse to remove a tree with a
   live claim — the pool has the claim records to ask, and
   `worktrees under dq-runs are experiment pins` is the standing reminder that
   a removal sweep is the dangerous kind of tidy.

## What this deliberately does not do

* **No worktrees on `/mnt/shared`.** Objects are shared; trees are not.
* **`TRITON_CACHE_DIR` stays `/home/rob/.triton-cache`** — a local path per
  box, same string, different disk (`pbrun`'s default environment).
* **Results still travel through the CAS**, never through the tree. A
  materialised worktree is disposable by construction.
* **It does not unpin `--here`**, which is a deliberate statement about one
  machine, nor an explicit `--tag` naming a hardware class.

## Refusals this needs at submit

* **A non-git checkout.** There is no commit to name; the action stays
  path-addressed and pinned, and says so.
* **An argv token that starts with the submitter's `cwd`.** A relocated tree
  breaks absolute paths silently — the command runs, against the wrong file or
  none. This is the one failure mode of (2) that is not loud, so it is refused
  at the one moment the caller is watching, the way an unplaceable tag already
  is (`pbrun`'s `no live worker can run this action`).
* **A working tree bigger than a stated bound.** A synthesised tree commit of
  a checkout holding a 90 GB cache is not a submission, it is an accident.

## Migration

Both addressings coexist. An item with `checkout_root` behaves exactly as it
does today, including the pin and the notice that now announces it; an item
with `checkout_commit` is placeable anywhere its tags and demand allow. The
census in `placement_census` is the measurement that says whether the change
is working, and `one_box_by_path` is the field to watch rather than `one_box`:
`one_box` also falls when a box goes away, and only the path half says the
migration is happening.  Live on 2026-09-04, after the census learned to cap
width by the tree rather than by the tags:

    ready 21, 20 on exactly one box (sparky 20), 20 by a box-local checkout,
    1 on more than one, 0 on none

## Line references

The design depends on particular lines of particular files, so each is quoted
here beside the file it lives in and `tests/test_design_doc_line_references.py`
checks the quotation still occurs there, exactly once.

Quotations rather than line numbers, learned the hard way twice on this
branch: the first two versions cited `file:line`, and both went stale inside
an hour because the work the design describes moves those very lines. A
line-number check would then fail this suite on every unrelated edit to
`pbrun.py` — five branches edit it at once — which makes the check something
to delete rather than something to keep. A quotation only fails when the code
it names actually changes, which is exactly when the design needs re-reading.

| where | the line it names |
|---|---|
| `pbrun._git_identity` | `def _git_identity(cwd: Path) -> dict[str, str]:` |
| `pbrun._git_identity`, untracked digest | `    # `git diff HEAD` covers tracked edits.  It says nothing about an` |
| `pbrun`, stamp name fingerprint | `    fingerprint = hashlib.sha256(` |
| `pbrun.placement_tags` | `def placement_tags(` |
| `pbrun`, default environment | `    # action key stays box-independent.  TRITON_CACHE_DIR is the one to watch:` |
| `pbrun`, stamp payload | `    payload = json.dumps({"cwd": str(cwd), **identity}, indent=1, sort_keys=True)` |
| `pbrun`, action body params | `        "params": {"command": command, "cwd": str(cwd), "demand": demand},` |
| `pbrun`, unplaceable refusal | `    verdict = q.placeable(intent)` |
| `pbrun`, published checkout_root | `        checkout_root=str(cwd),` |
| `pool`, token mint | `                    descriptor = os.open(token, os.O_WRONLY \| os.O_CREAT \| os.O_EXCL, 0o644)` |
| `pool.PoolQueue.publish` | `    def publish(` |
| `pool`, worker_argv | `            checkout_root=item["checkout_root"],` |
| `core._ACTION_BODY_KEYS` | `_ACTION_BODY_KEYS = frozenset(` |
| `core.verify_code_closure` | `def verify_code_closure(value: object, root: str \| Path) -> dict[str, object]:` |
| `core.seal_action` | `def seal_action(value: object) -> dict[str, object]:` |
| `core._local_result_claim_body` | `def _local_result_claim_body(` |
| `docs/design.md`, concurrent checkouts | `only workers sharing the same live checkout/output-lock identity. Workers in` |
| `tools/fleet/pool_reset.py` | `* twelve died on ``live code closure differs from the action-pinned` |
