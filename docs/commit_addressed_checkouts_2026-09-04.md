# Tree-addressed checkouts — design and what landed, 2026-09-04

**Status: BUILT, gated, not yet rolled out.** This is prismabuild issue #5
step (2). Steps (1) and (3) — the pin said out loud at submit, and a queue
metric for how wide the waiting work is — landed earlier the same day and are
kept. What is new here is the fix itself: an action can now name the **tree**
it runs against instead of the path it was submitted from, and any box that
fits the demand can claim it and build that tree for itself.

It is off until the fleet says it is ready, and the fleet says so per box. See
*Rollout* at the end for what is deliberately not done.

## The problem, measured

An action carries `checkout_root`, an absolute path (`pool.py:1106`, written by
`pbrun.py:1174`). When that path is a box-local worktree —
`/home/rob/tmp/ts101`, which is what an agent naturally creates — the action
must be tagged to the box that holds it or it will be claimed by a worker that
cannot see it. `pbrun.placement_tags` derives that pin from the path, and
correctly (`pbrun.py:301`).

Read off the live queue on 2026-09-04 at 07:15, over the 22 items in `ready`:

    ready 22, 22 on exactly one box (sparky 22), 22 by a box-local checkout,
    0 on more than one, 0 on none

Three boxes wide, one box deep. sparklina held a free GPU throughout and
dl380g10 eighty free cores; `/home/rob/tmp/wf110` and its fifteen siblings
exist on sparky and nowhere else, so neither box could have taken any of it.

`/mnt/shared` is not the answer for a working tree. It is NFSv4.2 mounted
`local_lock=none` (`/proc/mounts`, verified 2026-09-04); a dozen agents
building in worktrees on it is slow and a locking hazard, and
`TRITON_CACHE_DIR` must stay box-local whatever else moves.

## The shape of the fix

**An action names a git tree. The claiming worker materialises or reuses a
worktree at that tree under its own local scratch, and runs there.**

Nothing about the work changes; what changes is that the *location* stops
being part of the request, so three boxes can each answer it. The mechanism
lives in `checkout.py`; `pool.py` grew one line inside `execute`
(`pool.py:2251`) and `pbrun.py` grew the submit half.

### The identity is the tree, not the commit

`git commit-tree` bakes the committer and the clock into its object, so two
submits of an unchanged working tree a second apart produce two different
commits. Binding a commit would make every resubmit a CAS miss — the one
property the action key exists to keep. So the **tree** sha is what the
fingerprint, the sealed `params` and the closure stamp bind
(`pbrun.py:237`, `pbrun.py:1091`, `checkout.py:115`); the commit is transport,
and it is what names the ref. The synthesiser fixes author, committer and dates
for the same reason, so an unchanged tree does not churn a ref per submit.

### Step zero: the action key stops binding the submitter's path

The coordinator's framing — "the CAS makes re-execution on a different box a
correctness non-event" — was true of the CAS and **not** of these actions. The
submitter's absolute path was bound into the action key in three places, so the
same work submitted from two boxes was two different actions with two different
keys. All three moved:

* `params.cwd` became `{repo, prefix, tree}` (`pbrun.py:1091`);
* the closure stamp's *name* fingerprints the tree instead of `str(cwd)`
  (`pbrun.py:237`);
* the stamp's *content* is the tree alone (`pbrun.py:1038`), serialised by the
  one function the materialiser also calls (`checkout.py:115`).

`tests/test_checkout_commit_end_to_end.py` submits the same content from
`ts101` and from `ts102` and asserts one action key.

The repository's identity is its **root commit** and nothing else
(`checkout.py:153`). A first version put the checkout's basename in front of it
for readability, and a test caught what that cost: two worktrees of one
repository produced two identities, two bare repositories and two action keys
for one request — the very defect this addressing removes, reintroduced by its
own naming. The readable name now lives in the bare repository's `description`,
where nothing reads it as data. *Scope:* a **shallow** clone has no reachable
root commit and gets its own identity. Agents make worktrees of full clones, so
this has never arisen in hand; it is a limit on the claim, not a bug in it.

## Publishing the commit without publishing the tree

Objects are write-once and read-only afterwards — a very different NFS load
from a working tree.

* A bare repository per source repo at
  `/mnt/shared/prismabuild-fleet/git/<root-commit>.git`, created by whoever is
  first through a private directory and a `rename`, because `git init` is not
  atomic and ENOTEMPTY is exactly the signal "somebody finished first"
  (`checkout.py:293`).
* `pbrun` pushes the synthesised commit as `refs/pbrun/<sha>`.
* The item carries `checkout_commit`, `checkout_tree`, `checkout_repo`,
  `checkout_origin`, `checkout_prefix` and `checkout_stamp` (`pool.py:1179`).

Git's ref update takes its lock with `O_CREAT|O_EXCL`, the primitive this fleet
already relies on for token minting (`pool.py:613`). That it holds on **this**
mount was qualified rather than assumed:

> **24 concurrent submitters, one box, 2026-09-04.** 24 independent repositories
> each synthesised a commit and pushed it into one bare repository on
> `/mnt/shared`. Predicate: every ref present and naming its own commit. Result:
> **24/24 present, 0 mismatches, `git fsck --connectivity-only` rc=0**, and no
> `packed-refs` file was ever written — every ref stayed loose, so the
> packed-refs rewrite the design named as the contended case did not arise.
> Wall 42.4 s for 24 in flight, slowest single push 42.4 s: the pushes largely
> serialise, so a submitter pays roughly 1.8 s of push under 24-way contention.
>
> **Scope, stated because it bounds the claim: this is one box.** Two-box
> concurrent submission is *not* qualified — reaching the other box means going
> through admission, which this pass deliberately did not do. See
> `docs/two_host_qualification_2026-08-31.md` for the shape that would.

Auto-gc is turned off on the receiving side, since the repacking a gc would
start is the write that *would* contend.

### What the push costs, measured

The qualification above says a submitter pays ~1.8 s of push amortised under
24-way contention. That average hides the real shape, so it was taken apart —
one repository, one three-object commit, same code, only the destination
varying:

| arm | first push | repeat push at the same tree |
|---|---:|---:|
| bare repo on local NVMe | 0.01 s | 0.01 s |
| bare repo on `/mnt/shared` | 11–15 s | **0.68 s** |
| `/mnt/shared`, `core.fsync=none` both sides | 12.9–14.4 s | — |

So the cost is the mount, not durability settings: turning fsync off on either
side changes nothing, and the same push to local disk is three orders of
magnitude cheaper. A batch of 20 stats of a missing path on this mount takes
0.10 s — about 5 ms a lookup — and a git push makes hundreds of round trips
even for three objects. That is the whole explanation.

Two things follow, and both are why this is recorded rather than optimised
away. A **resubmit at an unchanged tree costs 0.68 s**, because git
short-circuits once the ref is there — so a forty-shard fan-out from one
checkout pays the full price once and pennies thereafter, which is the common
shape on this fleet. And 11 s on the first submit of a new tree buys an action
that three boxes can run instead of one; against an action that then occupies a
box for an hour and forty as this issue's own example did, it is 0.3%. A
hand-rolled "does the ref exist" pre-check would save 0.6 s of the 0.68 s and
introduce a second source of truth about what the remote holds; it was measured
and deliberately not taken.

## Dirty trees are the norm, so the commit is synthesised

Agents submit from dirty trees constantly; `pbrun` has a whole delta digest for
it (`pbrun.py:115`), and 12 of 50 live failures were closure drift between
sealing and running (`tools/fleet/pool_reset.py:12-14`). Requiring a clean tree
would make the feature unusable, so the tree is written from the working
directory through a **scratch index** — never the caller's index, never a
branch, never a stash (`checkout.py:244`):

```
GIT_INDEX_FILE=$scratch/index git read-tree HEAD   # seed, see below
GIT_INDEX_FILE=$scratch/index git add -A           # untracked included
tree=$(GIT_INDEX_FILE=$scratch/index git write-tree)
commit=$(git commit-tree $tree -p HEAD -m "pbrun $tree")
git push <bare> $commit:refs/pbrun/$commit
```

`git stash create` is the tempting shortcut and is the wrong one: it does not
carry untracked files, and `pbrun` learned the hard way that an untracked file
edit must move the action key.

The **`read-tree HEAD` seeding** is not decoration. `git add -A` skips a path an
ignore rule matches, tracked or not; from an empty index a tracked-but-ignored
file would silently vanish from the tree, and that is a wrong tree no later
check can catch, because every later check compares against this one
(`checkout.py:269`, and a test for exactly it).

The stamp and the result logs are kept out of the tree through
`.git/info/exclude`, which `pbrun` now writes *before* synthesising rather than
after sealing — otherwise every submit would produce a new tree for its own
droppings and never hit the CAS again.

## Worker side

1. Per-box bare mirror at `/home/rob/.cache/prismabuild/git/<repo>.git`;
   `git fetch <origin> +refs/pbrun/<sha>:refs/pbrun/<sha>` when the object is
   absent. Fetched **by ref, never by raw sha**: `git fetch <path> <sha>` needs
   `uploadpack.allowAnySHA1InWant` on the serving side and is refused by
   default (`checkout.py:615`).
2. `git worktree add --detach /home/rob/tmp/pb-trees/<repo>/<sha>`, under a
   per-sha `O_EXCL` lock so two loops on one box do not race the same
   materialisation (`checkout.py:395`). Reuse when it is already there — a
   second action at the same tree costs a lock and a stat. Reuse tolerates an
   action's untracked droppings and refuses a **tracked** change, which is the
   code the action key pinned.
3. Run exactly as today: `worker_argv` gets `--checkout-root <that path>`,
   resolved by one line in `execute` (`pool.py:2251`, `pool.py:2191`).
4. **The closure keeps its teeth, by derivation.** The materialiser recomputes
   the tree sha *from the worktree it has just built* and writes the stamp from
   that (`checkout.py:457`); `core.verify_code_closure` (`core.py:1136`) then
   compares it against the action-pinned bytes. A worktree that landed on the
   wrong tree produces different bytes and the action refuses. That is the
   difference between a check and a receipt, and it is what makes running on
   another box safe rather than merely possible.
5. An LRU sweep bounds the scratch (`checkout.py:643`), run at the start of an
   idle streak. Three guards, each a lesson: it removes only directories
   carrying a marker it wrote itself, it keeps any tree whose commit appears in
   a live claim — asked of the queue (`pool.py:2160`), not of the clock — and
   what it keeps is the most recently *used*, which is why materialising
   touches the marker on reuse.

`checkout_root` stays the submitter's own path on a tree-addressed item. It is
the honest record of where the work came from, and an empty one would be filed
as an unexecutable stub by `quarantine_orphans` (`pool.py:1659`). Which readers
may treat it as a pin is settled in exactly one place
(`checkout.py:378`), because a second copy is how the pin and the measurement
of the pin end up describing different fleets.

## Refusals at submit

Two, and both are the caller's own mistake rather than the fleet's:

* **An argv token or `--env` value naming the submitter's checkout**
  (`pbrun.py:403`). A relocated tree breaks an absolute path *silently* — the
  command runs, against the wrong file or none. This is the one failure mode of
  the addressing that is not loud, so it is refused at the one moment the
  caller is watching. Matched on the repository toplevel, and only where the
  path ends or continues with a separator, so `/home/rob/tessera-results`
  beside `/home/rob/tessera` is not dragged in.
* **A working tree that would add more than 2 GiB to the shared object store**
  (`checkout.py:216`). Measured as what `add -A` would actually stage, not as a
  `du`: the 90 GB cache this exists to catch is normally `.gitignore`d and
  never enters a tree, so a `du` would refuse the submissions that are fine and
  miss the one that is not.

Everything else **falls back to the pin and says why** (`pbrun.py:438`). A
non-git checkout, a repository with no commit, a fleet that has not announced,
a push that failed on NFS: each leaves the action exactly as pinned as it is
today, with the reason printed beside the pin. This is a widening, and a
widening that can fail a submission would be worse than the pin it removes.

## What this deliberately does not do

* **No worktrees on `/mnt/shared`.** Objects are shared; trees are not.
* **`TRITON_CACHE_DIR` stays `/home/rob/.triton-cache`** — a local path per
  box, same string, different disk (`pbrun.py:919`).
* **Results still travel through the CAS**, never through the tree. A
  materialised worktree is disposable by construction.
* **It does not unpin `--here`**, which is a deliberate statement about one
  machine, nor an explicit `--tag` naming a hardware class.

## Rollout

The gate is **attested, not asserted** (principle 14). A worker announces a
`capabilities` list in its own offer record (`worker_loop.py:271`,
`pool.py:872`, `pool.py:182`), and `pbrun` addresses an action by its tree only
when **every** live offer carries `checkout_commit` (`pbrun.py:367`). A box
running older bytes announces nothing, and that silence is a "no" that cannot
be got wrong — so on the fleet as published today this changes nothing at all,
and it converts itself as the loops reload rather than on a date somebody
picks. `--path-addressed` opts out; `--commit-addressed` overrides the gate.

Both addressings coexist. An item with `checkout_root` alone behaves exactly as
it does today, including the pin and the notice announcing it.
`one_box_by_path` is the field to watch rather than `one_box`: `one_box` also
falls when a box goes away, and only the path half says the migration is
happening (`pool.py:1070`).

**Not qualified, and therefore not claimed:** two-box concurrent submission
against one shared bare repository; the first fetch of a large repository's
history into an empty mirror on a box, which is bounded by lease heartbeats
(`pool.py:2191`) but has not been timed against a real repository; and any
behaviour at all on the live fleet, which has not run these bytes.

## Line references

Every `file:line` above is repeated here with the line it points at, and
`tests/test_design_doc_line_references.py` checks the two still agree. This
table exists because they twice did not: the design was written against one
arrangement of `pbrun.py`, the branch it describes moved those lines, and two
separate commits went to re-pointing them by hand. A citation nothing checks is
a citation that decays into a confident wrong number, which is worse than no
number at all — so the check is mechanical, and a range is anchored by its
first line.

| citation | the line it names |
|---|---|
| `pbrun.py:115` | `def _git_identity(cwd: Path) -> dict[str, str]:` |
| `pbrun.py:237` | `    if tree is None:` |
| `pbrun.py:301` | `def placement_tags(` |
| `pbrun.py:367` | `def fleet_materialises(queue) -> tuple[bool \| None, str]:` |
| `pbrun.py:403` | `def refuse_paths_into_the_checkout(command, variables, toplevel: str) -> None:` |
| `pbrun.py:438` | `def plan_tree_addressing(` |
| `pbrun.py:919` | `    # action key stays box-independent.  TRITON_CACHE_DIR is the one to watch:` |
| `pbrun.py:1038` | `    payload = (ck.stamp_bytes(plan["tree"]).decode("utf-8") if plan is not None` |
| `pbrun.py:1091` | `        "params": ({"command": command, "repo": plan["repo"],` |
| `pbrun.py:1174` | `        checkout_root=str(cwd),` |
| `checkout.py:115` | `def stamp_bytes(tree_sha: str) -> bytes:` |
| `checkout.py:153` | `def repo_identity(cwd: str \| Path) -> dict[str, str] \| None:` |
| `checkout.py:216` | `def pending_add_bytes(toplevel: str \| Path) -> int:` |
| `checkout.py:244` | `def synthesise_tree_commit(toplevel: str \| Path, *, scratch: str \| Path,` |
| `checkout.py:269` | `        _git("read-tree", "HEAD", cwd=toplevel, env=env, timeout=600)` |
| `checkout.py:293` | `def ensure_shared_bare(repo: str, *, name: str = "",` |
| `checkout.py:378` | `def item_is_box_local(item: Mapping[str, object]) -> bool:` |
| `checkout.py:395` | `def _hold_lock(path: Path, *, wait_s: float, stale_s: float,` |
| `checkout.py:457` | `def materialise(` |
| `checkout.py:615` | `def _ensure_commit(mirror: Path, *, commit: str, origin: str,` |
| `checkout.py:643` | `def sweep(` |
| `pool.py:182` | `WORKER_CAPABILITIES: tuple[str, ...] = (ck.CHECKOUT_COMMIT_CAPABILITY,)` |
| `pool.py:613` | `                    descriptor = os.open(token, os.O_WRONLY \| os.O_CREAT \| os.O_EXCL, 0o644)` |
| `pool.py:872` | `            "capabilities": sorted({str(c) for c in (capabilities or ())}),` |
| `pool.py:1070` | `            by_path = ck.item_is_box_local(item)` |
| `pool.py:1179` | `        if checkout_commit and checkout_tree and checkout_repo and checkout_origin:` |
| `pool.py:1106` | `    def publish(` |
| `pool.py:1659` | `                              ("worker_script", "cas_root", "checkout_root")))` |
| `pool.py:2160` | `    def live_commits(self, *, host: str \| None = None,` |
| `pool.py:2191` | `    def resolve_checkout(self, item: Mapping[str, object]) -> str:` |
| `pool.py:2251` | `            checkout_root=self.resolve_checkout(item),` |
| `worker_loop.py:271` | `            capabilities=pool.WORKER_CAPABILITIES,` |
| `core.py:1136` | `def verify_code_closure(value: object, root: str \| Path) -> dict[str, object]:` |
| `tools/fleet/pool_reset.py:12-14` | `* twelve died on ``live code closure differs from the action-pinned` |
