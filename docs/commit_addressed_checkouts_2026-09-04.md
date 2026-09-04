# Commit-addressed checkouts — implementation, 2026-09-04

**Status: implemented on `codex/pb-5-materialize`; deployment remains a
separate operation.** This is prismabuild issue #5 step (2). The earlier pin
notice and placement census remain useful for legacy queue records, while new
`pbrun` submissions no longer execute a submitter-owned mutable path.

## The measured problem

Before this change an action carried `checkout_root`, an absolute path. A
box-local agent worktree therefore forced a hostname tag: sending it elsewhere
would either fail because the path was absent or, worse, execute a different
tree at the same spelling. Moving working trees to `/mnt/shared` was not a safe
answer; the mount is NFSv4.2 with `local_lock=none`, and local build/cache trees
must remain local.

The live queue census on 2026-09-04 made the cost visible. Of 391 historical
items, 131 named a host and 129 of those pins were consequences of a checkout
path. A later recount found 243 host-tagged items among 516, 208 caused by a
box-local checkout. At one sampled instant all 18 ready items were placeable
only on Sparky while the other two boxes were idle.

## The implemented contract

Every new `pbrun` action must start inside a Git worktree. The submitter makes a
deterministic synthetic root commit for the exact working tree it sees,
including tracked edits, deletions, and non-ignored untracked files. It adds the
action-specific closure stamp even though local Git excludes that stamp from
ordinary status. The synthetic commit and one advertised ref are serialized as
a shallow Git bundle and ingested as a verified CAS input.

The action and queue item carry a `prismaquant.prismabuild.
pbrun_checkout_snapshot.v1` contract containing:

* the synthetic commit object id;
* the requested working directory relative to the repository root; and
* the CAS input contract for the bundle.

The source's absolute location is absent from result/stamp naming, container
ownership, the closure stamp, action params, and queue addressing. Two clones
of the same bytes and logical subdirectory therefore describe the same work.

The claiming worker fully verifies the CAS blob, creates a unique directory
under its local `PRISMABUILD_LOCAL_CHECKOUT_ROOT`, initializes a repository,
fetches only the advertised sealed ref, checks out the exact commit detached,
and runs from the recorded relative subdirectory. Core preflight verifies that
the materialized tree is clean at that commit and that the stamp, action params,
and snapshot name the same subdirectory. The per-action tree is removed after
execution. A cleanup failure is warned and written below the worker's local
materialization root, without converting completed task work into a retry.
There is no shared mutable worktree, reuse cache, or eviction policy in this
implementation.

## Why the temporary index starts from HEAD

The snapshot uses a private `GIT_INDEX_FILE`, so it cannot disturb the user's
index. A new private index is empty. Running only `git add -A` against it would
misclassify a file that is tracked in `HEAD` but now matches an ignore rule as
an ignored untracked file, silently deleting it from the snapshot. The builder
therefore runs `read-tree HEAD` first, then overlays the live working tree with
`git add -A`. The resulting tree preserves the tracked roster while still
recording edits and deletions.

Author and committer identity, timestamps, message, and parentless commit shape
are fixed. That makes the synthetic commit a function of the tree rather than
of the submitter. The bundle has a fixed advertised ref. The 512 MiB hard fleet
ceiling is applied independently to the logical materialized tree (each path's
blob bytes are counted, even when paths share an object) and to the compressed
bundle before CAS ingestion. The CLI can lower but cannot raise it. Compression
or sparse storage therefore cannot turn a large worker checkout into a small
accepted transport.

## Refusals

There is deliberately no mutable-path escape hatch for a new submission.

* A non-Git directory refuses because it has no repository closure to seal.
* argv[0] is resolved exactly against the declared `PATH`. An executable
  outside the repository and shared storage retains the submitter host tag.
  When it is absent, submission refuses unless an explicit `--tag` names the
  worker class that owns it; transporting source does not transport a
  user-local interpreter. Direct path-shaped argv and caller-environment
  values receive a conservative screen, not heuristic proof of shell or
  application indirection. `--tag` owns those dependencies for a worker class;
  `--anywhere` explicitly asserts that they are portable.
* An absolute path containing the submitter's repository root in argv or the
  environment refuses. Checking only the requested subdirectory would let
  `--cwd repo/subdir` escape through the submitter's mutable `repo/sibling`
  after relocation. A relative `../sibling` is allowed when it stays within
  the repository, because the worker resolves it inside the sealed snapshot.
* A closure stamp that is a symlink, outside the repository, or not a regular
  file refuses.
* A logical tree or compressed bundle above the declared byte limit refuses
  before it reaches the queue, and a requested limit above the hard fleet
  ceiling refuses before Git hashes any checkout bytes.
* A gitlink/submodule refuses because the parent bundle does not carry the
  nested repository's working bytes.
* An absolute symlink or a relative symlink whose lexical target escapes the
  repository (including into `.git`) refuses. Internal relative symlinks keep
  their literal link text and are materialized unchanged.
* Active `text`, `eol`, `crlf`, `ident`, `filter`, or
  `working-tree-encoding` attributes, and active `core.autocrlf`, refuse.
  They would let Git clean different bytes into the synthetic tree or smudge
  different bytes on a worker. The supported boundary is a checkout whose
  content bytes Git stores and checks out unchanged.
* A snapshot absent from `action.inputs`, naming an unadvertised commit, or
  materializing a dirty/wrong tree refuses before task argv.

External model/data paths may remain absolute when they are outside the source
repository. They are data dependencies, not a way to reach mutable source, and
their own action-input/provenance contracts remain unchanged.

## Migration and helpers

Workers keep accepting an old queue item with `checkout_root` so work already
published by the previous runtime can drain. `PoolQueue.publish` accepts exactly
one of `checkout_root` and `checkout_snapshot`; current `pbrun` always supplies
the latter and refuses a non-Git source. Once the old queue is empty, the
compatibility reader can be removed in a separately reviewed change.

`pbtest` now accepts a box-local Git checkout. It gives the source location only
to `pbrun --cwd`, while task argv uses repository-relative test paths and
`PYTHONPATH=src:experiments`. The snapshot, rather than an NFS working tree,
makes those shards portable to dl380g10.

## Evidence contract

The regression population covers:

* a tracked file that also matches `.gitignore` survives materialization;
* nested untracked bytes move the snapshot;
* logical bytes are bounded before Git hashes them and again from the written
  tree, independently of the compressed bundle bound;
* active Git content transforms and gitlinks refuse;
* mutation after queue publication cannot change executed bytes;
* source-location-independent result/stamp and container identities;
* exact declared-`PATH` resolution, host placement for an existing box-local
  executable, direct external flag/environment paths, explicit `--anywhere`,
  and named refusal for an unplaced absent executable;
* repository-sibling relative paths from a subdirectory remain valid while an
  absolute submitter-repository path refuses;
* non-Git submission refusal;
* CAS tamper, absent-ref, missing-subdirectory, ordinary cleanup, and durable
  cleanup-failure reporting cases; and
* legacy queue-record compatibility while the queue drains.

The pre-fix and post-fix PrismaBuild action keys are recorded on issue #5. A
runtime publication and a cross-box action from a genuinely box-local source
remain deployment qualification, not something this source commit can claim.

## Line references

These quotations make the design/reference test fail only when the named code
moves semantically, rather than whenever an unrelated edit changes a line
number.

| where | the line it names |
|---|---|
| `core.git_checkout_identity` | `def git_checkout_identity(root: str \| Path) -> dict[str, str]:` |
| `core.git_checkout_identity`, untracked digest | `    # Let Git delimit untracked pathnames. Line-oriented porcelain C-quotes` |
| `pbrun.build_git_checkout_snapshot` | `def build_git_checkout_snapshot(` |
| `pbrun.require_relocatable_checkout` | `def require_relocatable_checkout(` |
| `pbrun`, snapshot publication | `    publication["checkout_snapshot"] = checkout_snapshot` |
| `pool._execution_checkout` | `def _execution_checkout(item: Mapping[str, object]) -> Iterator[Path]:` |
| `pool`, mutually exclusive addressing | `                    "checkout_root and checkout_snapshot are mutually exclusive"` |
| `pool.PoolQueue.execute` | `        with _execution_checkout(item) as checkout_root:` |
| `core`, subdirectory agreement | `                "pbrun checkout stamp cwd differs from snapshot subdirectory"` |
| `core.verify_code_closure` | `def verify_code_closure(value: object, root: str \| Path) -> dict[str, object]:` |
| `core.seal_action` | `def seal_action(value: object) -> dict[str, object]:` |
