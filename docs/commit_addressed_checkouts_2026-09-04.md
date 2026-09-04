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
deterministic synthetic commit for the exact working tree it sees,
including tracked edits, deletions, and non-ignored untracked files. It adds the
action-specific closure stamp even though local Git excludes that stamp from
ordinary status. The commit's parent is the source's own `HEAD`, so the sealed
history is the source history with one commit on top of it. The commit, its
ancestry, and any branch names the caller asked for are serialized as a Git
bundle and ingested as a verified CAS input.

The action and queue item carry a `prismaquant.prismabuild.
pbrun_checkout_snapshot.v2` contract containing:

* the synthetic commit object id;
* its parent, the source `HEAD` at submission (`null` only for the unborn
  case the submitter refuses, below);
* the requested working directory relative to the repository root;
* the CAS input contract for the bundle; and
* `refs`, a mapping from each requested short branch name to the object id
  the bundle also advertises for it (empty when none were requested).

The source's absolute location is absent from result/stamp naming, container
ownership, the closure stamp, action params, and queue addressing. Two clones
of the same bytes and logical subdirectory therefore describe the same work.

The claiming worker fully verifies the CAS blob, creates a unique directory
under its local `PRISMABUILD_LOCAL_CHECKOUT_ROOT`, initializes a repository,
points its `HEAD` at a reserved name no record may claim, fetches the sealed
ref and each recorded branch, checks out the exact commit detached,
and runs from the recorded relative subdirectory. Core preflight verifies that
the materialized tree is clean at that commit, that `HEAD`'s single parent is
the recorded one, that each recorded branch resolves to its recorded id, and
that the stamp, action params,
and snapshot name the same subdirectory. The per-action tree is removed after
execution. A cleanup failure is warned and written below the worker's local
materialization root, without converting completed task work into a retry.
There is no shared mutable worktree, reuse cache, or eviction policy in this
implementation.

## Why the snapshot commit has a parent

The first implementation sealed a root commit. Inside a materialized checkout
that made `git rev-parse HEAD~1`, `git merge-base`, `<base>...HEAD` and
`master...HEAD` all `fatal: ambiguous argument` — so a diff-derived gate could
not run under portable `pbrun` execution at all. Tessera's required pre-merge
gate `tools/impacted_tests.py --ref BASE...HEAD` is exactly that shape, and two
real actions failed three retries each on runtime generation
`aa6d3cfa2f77-1788542034-2b84265567ac`.

Sealing the parent fixes it by construction: `git bundle create` walks from
every named ref, so the ancestry travels with no history depth to choose. The
measured repositories fit far inside the existing ceiling — the Tessera pack is
4.94 MiB over 1375 commits and PrismaQuant's 46.8 MiB over 2173, against a
512 MiB bundle limit that still applies unchanged.

`--snapshot-ref NAME` is repeatable and adds `refs/heads/NAME` to the bundle,
because a gate is usually spelled with a branch name rather than a hash. The
name is resolved fully qualified in the source and refused, by name and without
a traceback, before anything is ingested or queued: a name Git's
`check-ref-format --branch` rejects, one the source does not resolve, a
duplicate, or one whose spelling could act as a `git fetch` refspec or option on
a worker. The materializer refuses a recorded id the bundle contradicts, since
the record and the bundle travel separately and a branch created at an
unreachable id would make every later comparison a silent lie.

**Every pbrun action key moves once with this change.** The snapshot input bytes
are part of the action, so the same argv over the same tree now hashes
differently. That is accepted: it costs one round of cache misses, and it is
the honest consequence of the sealed bytes changing.

Two sources cannot supply the ancestry and are refused up front with a named
message rather than a Git internal error: a shallow clone and a partial clone.
A repository with no commits was refused before this change and still is — the
submitter takes the checkout identity first, and `rev-parse HEAD` fails on an
unborn `HEAD`. The contract's `parent: null` exists so the shape is total, not
because the submitter produces it.

## Why the temporary index starts from HEAD

The snapshot uses a private `GIT_INDEX_FILE`, so it cannot disturb the user's
index. A new private index is empty. Running only `git add -A` against it would
misclassify a file that is tracked in `HEAD` but now matches an ignore rule as
an ignored untracked file, silently deleting it from the snapshot. The builder
therefore runs `read-tree HEAD` first, then overlays the live working tree with
`git add -A`. The resulting tree preserves the tracked roster while still
recording edits and deletions.

Author and committer identity, timestamps, and message are fixed. That makes the
synthetic commit a function of the tree and its parent rather than
of the submitter. The bundle has a fixed advertised ref for the snapshot itself,
plus one per requested branch name. The 512 MiB hard fleet
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
  `--anywhere` explicitly asserts that they are portable. The sorted,
  deduplicated effective tag conjunction is sealed in action params and in the
  result/stamp and container-owner fingerprints, so changing admissible workers
  changes identity while reordered or repeated tags do not.
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
  materializing a dirty/wrong tree refuses before task argv. So does one whose
  materialized `HEAD` does not carry the recorded parent, or whose recorded
  branch does not resolve to its recorded id.
* A shallow or partial clone refuses before any bytes are hashed: the ancestry
  the bundle must walk is not in the source, and the alternative is a
  `Failed to traverse parents` deep inside `pack-objects`.
* An unresolvable, malformed, duplicated, or reserved `--snapshot-ref` name
  refuses before the closure stamp is written. A `refs` name arriving on a
  queue record is validated the same way: it becomes `refs/heads/<name>` and a
  `git fetch` refspec inside a worker, so the revision grammar, the option
  parser, `HEAD`, and the snapshot's own reserved name all stay out of it.

External model/data paths may remain absolute when they are outside the source
repository. They are data dependencies, not a way to reach mutable source, and
their own action-input/provenance contracts remain unchanged.

## Migration and helpers

Workers keep accepting an old queue item with `checkout_root` so work already
published by the previous runtime can drain. `PoolQueue.publish` accepts exactly
one of `checkout_root` and `checkout_snapshot`; current `pbrun` always supplies
the latter and refuses a non-Git source. Once the old queue is empty, the
compatibility reader can be removed in a separately reviewed change.

The `v1` snapshot contract drains the same way. Each schema owns one exact key
set, so a `v1` record cannot carry ancestry and a `v2` record cannot omit it;
a `v1` record materializes exactly as it did, which is what lets items already
in `ready/` survive the rollout that introduces `v2`.

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
  cleanup-failure reporting cases;
* a materialized checkout answering `HEAD~1` and `<sourceHEAD>...HEAD`, and a
  requested branch answering `NAME...HEAD`;
* named refusal for an unknown, malformed, or dangerous ref name, for a
  bundle that contradicts a recorded ref, for a shallow source, and for a
  repository with no commits; and
* legacy queue-record compatibility while the queue drains, including a `v1`
  snapshot record materializing unchanged.

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
| `pbrun.require_complete_history` | `def require_complete_history(root: Path) -> None:` |
| `pbrun.resolve_snapshot_refs` | `def resolve_snapshot_refs(` |
| `pbrun`, the parent that carries ancestry | `            ["commit-tree", tree, "-p", parent],` |
| `core.validate_pbrun_snapshot_ref_name` | `def validate_pbrun_snapshot_ref_name(value: object, *, where: str) -> str:` |
| `core._verify_pbrun_checkout_ancestry` | `def _verify_pbrun_checkout_ancestry(` |
| `pool._execution_checkout` | `def _execution_checkout(item: Mapping[str, object]) -> Iterator[Path]:` |
| `pool`, the record the bundle must agree with | `                    f"checkout snapshot bundle contradicts sealed ref {name!r}"` |
| `pool`, mutually exclusive addressing | `                    "checkout_root and checkout_snapshot are mutually exclusive"` |
| `pool.PoolQueue.execute` | `        with _execution_checkout(item) as checkout_root:` |
| `core`, subdirectory agreement | `                "pbrun checkout stamp cwd differs from snapshot subdirectory"` |
| `core.verify_code_closure` | `def verify_code_closure(value: object, root: str \| Path) -> dict[str, object]:` |
| `core.seal_action` | `def seal_action(value: object) -> dict[str, object]:` |
