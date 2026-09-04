# Commit-addressed checkouts — design, 2026-09-04

**Status: DESIGN. Not implemented.** This is the fix for prismabuild issue #5
step (2). Steps (1) and (3) — the pin said out loud at submit, and a queue
metric for how wide the waiting work is — landed on 2026-09-04 and are
described here only where they bound the design.

## The problem, measured

An action carries `checkout_root`, an absolute path (`pool.py:657-705`,
written by `pbrun.py:650`). When that path is a box-local worktree —
`/home/rob/tmp/ts101`, which is what an agent naturally creates — the action
must be tagged to the box that holds it or it will be claimed by a worker that
cannot see it. `pbrun.placement_tags` derives that pin from the path, and
correctly (`pbrun.py:206-240`).

Read off the live queue on 2026-09-04, over the 394 items in `ready`,
`claimed`, `done` and `failed`:

| items | tags | checkout_root |
|---:|---|---|
| 121 | `gb10` | `/mnt/shared/prismabuild-fleet/checkout` |
| 114 | `sparky` | `/home/rob/tmp/ts101` and siblings |
| 106 | `x86` | `/mnt/shared/tessera-x86` |
| 28 | *(none)* | *(none recorded)* |
| 15 | `gx10-6b77` | `/home/rob/tmp/ts91/tessera` |
| 10 | other | — |

129 items — a third of everything the fleet has ever been given — were
placeable on exactly one box, and 129 of those pins were a consequence of a
path rather than of the work. The new census reports it directly; live, while
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

## What has to move first: the action key binds the path

The coordinator's framing — "the CAS makes re-execution on a different box a
correctness non-event" — is true of the CAS and **not yet true of these
actions**. The submitter's absolute path is bound into the action key in three
places, so the same work submitted from two boxes is today two different
actions with two different keys:

* `params.cwd` is `str(cwd)` (`pbrun.py:603`), and `params` is part of the
  sealed body (`core.py:62-72`, `seal_action` at `core.py:1371-1375`);
* the closure stamp's *name* embeds a fingerprint over `str(cwd)`
  (`pbrun.py:198-202`), so the closure member's path differs per box;
* the stamp's *content* records `{"cwd": ...}` (`pbrun.py:544`).

So step zero of (2) is to rebind the key from *(path, tree delta)* to
*(repository identity, tree commit)*. After that a cache hit across boxes is
correct rather than lucky, and it is the same property the design already
claims for concurrent workers: "Workers in distinct validated checkouts may
execute task argv concurrently and converge through ordinary CAS publication"
(`docs/design.md:44-46`). The output-lock claim binds the resolved checkout
(`core.py:3596-3611`), which is what allows two boxes to hold two claims
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
this fleet already relies on for token minting (`pool.py:284-291`). That it
holds on this mount for `refs/` **must be verified, not assumed** — the mount
is `local_lock=none`, and every concurrent submitter writes a *different* ref
name here, so the contended case is the packed-refs rewrite rather than the
ref itself. Qualify it the way `docs/two_host_qualification_2026-08-31.md`
qualified the rendezvous: two boxes, real concurrency, an explicit predicate.

## Dirty trees are the norm, so the commit is synthesised

Agents submit from dirty trees constantly; `pbrun` has a whole delta digest
for it (`pbrun.py:75-121`), and 12 of 50 live failures were closure drift
between sealing and running (`tools/fleet/pool_reset.py:12-14`). Requiring a
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
edit must move the action key (`pbrun.py:100-118`). Include the same
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
   (`pool.py:1181-1191`), and everything downstream is unchanged.
4. The closure check keeps its teeth. The materialiser writes the stamp by
   recomputing `_git_identity` **from the tree it has just built**, exactly as
   `pbrun` does at submit; `core.verify_code_closure` (`core.py:1136-1149`)
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
  box, same string, different disk (`pbrun.py:472-479`).
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
  is (`pbrun.py:626-642`).
* **A working tree bigger than a stated bound.** A synthesised tree commit of
  a checkout holding a 90 GB cache is not a submission, it is an accident.

## Migration

Both addressings coexist. An item with `checkout_root` behaves exactly as it
does today, including the pin and the notice that now announces it; an item
with `checkout_commit` is placeable anywhere its tags and demand allow. The
census in `placement_census` is the measurement that says whether the change
is working: `one_box` should fall as submitters adopt it, and it costs nothing
to watch it fall.
