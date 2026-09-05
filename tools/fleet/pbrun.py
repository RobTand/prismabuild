#!/usr/bin/env python3
"""Run one command through the PrismaBuild pool instead of a local flock.

Why this exists: agent work was scheduled by a box-local ``flock`` semaphore,
which cannot coordinate across boxes (the lock file is local) and reproduces
three bugs ``pool.py`` already solves -- hold-while-gated, no aging, and
partial-hold waste.  This is the submit side that makes the pool the only
path an agent needs.

Two things are deliberate.

*Exclusivity is a demand, not a token kind.*  ``--exclusive`` asks for the
whole GPU capacity of a box.  The ledger's all-or-nothing ``acquire`` turns
that into exclusion for free, and ``STARVATION_FLOOR`` stops a big demand
being leapfrogged forever by small ones.  A second "exclusive" lock would be
policy where arithmetic already answers.

*The closure is an immutable checkout.* A Git working tree, including its
dirty and untracked bytes, is synthesized as a shallow root commit and carried
through the CAS. The claiming box executes a fresh local checkout, while the
closure stamp keeps the source identity visible in the action. New submissions
that cannot be snapshotted refuse; only already-published legacy queue records
retain mutable path addressing while they drain.

*Cancelling is a first-class verb, not an edit.*  ``--withdraw`` is the other
half of the submit path: this is the only way an agent may put work on the
fleet, so it has to be the way work comes back off it.  Without it, stopping a
running action meant hand-editing ``max_attempts`` into a live claimed record
and racing the retry -- see ``PoolQueue.withdraw``.

*Retry safety is not numerical determinism.*  A deterministic action may write
external state and then fail, so arbitrary commands get one attempt.  A larger
``--max-attempts`` is accepted only with ``--retry-safe``, which declares the
whole command idempotent, and that policy is sealed into the action identity.

The stamp carrying that identity has to live *inside* the checkout, because
the worker verifies the closure against ``checkout_root`` on the box that
runs it.  So it is excluded from the identity it records -- otherwise each
submit would dirty the tree it is describing and no two submits of the same
command would ever agree -- and it is added to ``.git/info/exclude`` (local
only, never the committed ignore file) so it cannot make a clean tree look
dirty to anything else.
"""
from __future__ import annotations

import argparse
import getpass
import hashlib
import inspect
import json
import os
import posixpath
import shlex
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Sequence
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root
# The transport default is read in one place for the whole fleet.  pbrun used
# to spell it itself, which was the same expression until the published
# generation became able to carry a default and then was silently not.
from fleet_submit import default_transport

SH = Path("/mnt/shared/prismabuild-fleet")
#: Every box mounts this at the same path, so a checkout underneath it is
#: visible to all of them and an action that runs there can run anywhere.
#: A checkout outside it exists on exactly one box.  That is a *fact about
#: the path*, which is why placement below is derived from it rather than
#: asked of the submitter.
SHARED_ROOT = Path("/mnt/shared")
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb, pool, slurm_lane  # noqa: E402

POLL_S = 5.0
#: Which transport carries a submission.  The pull queue is still the default:
#: SLURM is installed box by box, and the day a controller comes up is not the
#: day every agent's ``pbrun`` should start talking to it.  Cutover is one
#: environment variable, and rollback is unsetting it -- the pool path below is
#: untouched by any of this.
TRANSPORTS = ("pool", "slurm")
DEFAULT_TRANSPORT_ENV = "PRISMABUILD_TRANSPORT"
#: What ``pbrun`` exits with when it stopped waiting before the work finished.
#: The pool path already spells it this way; the SLURM path means the same
#: thing by it, and in both cases the work is still running.
GAVE_UP_EXIT = 75
# An arbitrary command can write state outside its declared CAS result before
# a later check fails.  Retrying that command is never implied by numerical
# determinism; the producer must opt the whole action into a larger bound.
DEFAULT_MAX_ATTEMPTS = 1
#: Submission asks what the recorded fleet can *ever* fit, not which worker
#: happened to refresh inside the claim TTL.  ``PoolQueue`` retains one latest
#: offer per host, so an unbounded age reads that capability ledger without
#: inventing a second placement rule; workers still use the ordinary live
#: window when deciding what may claim now.
RECORDED_OFFER_MAX_AGE_S = float("inf")
CHECKOUT_SNAPSHOT_MAX_BYTES = 512 * 1024 * 1024
CONTENT_TRANSFORM_ATTRIBUTES = frozenset(
    {"crlf", "eol", "filter", "ident", "text", "working-tree-encoding"}
)
#: What ``pbrun`` exits with when the action it was waiting for was withdrawn.
#: 128+SIGTERM, which is the shell's own word for "this was stopped on purpose",
#: and it is literally the signal a withdrawal sends to the action's process
#: group -- ``core.run_local_action`` reports the same event as status ``-15``.
#: Non-zero because the command did not run; distinct from a real failure
#: because nothing about it was a defect.
WITHDRAWN_EXIT = 143

#: The one line ``--detach`` prints.  Versioned because ``pbcampaign`` and
#: ``pbwait`` parse it, and a fleet runs a published runtime generation that
#: may be older than the tool reading its output.
DETACH_SCHEMA_V1 = "prismaquant.prismabuild.pbrun_detach.v1"
#: The receipt ``publish_runtime`` leaves for which bytes the fleet is serving.
#: A worker loop holds the module it imported at start, so this is the only
#: thing that says whether a given box's loop can see a withdrawal at all.
RUNTIME_VERSION = RUNTIME_ROOT / "RUNTIME_VERSION.json"


def require_checkout_snapshot_limit(max_bytes: int) -> int:
    """Validate a caller's lowering of the fleet-wide checkout disk bound."""

    if type(max_bytes) is not int or max_bytes < 1:
        raise SystemExit("pbrun: checkout snapshot byte limit must be positive")
    if max_bytes > CHECKOUT_SNAPSHOT_MAX_BYTES:
        raise SystemExit(
            "pbrun: checkout snapshot byte limit exceeds the hard fleet "
            f"ceiling of {CHECKOUT_SNAPSHOT_MAX_BYTES} bytes; the per-action "
            "flag may only lower this unaccounted local-disk bound"
        )
    return max_bytes


def published_commit() -> str:
    """The commit whose bytes are currently published, or "" if unknown."""

    try:
        return str(json.loads(RUNTIME_VERSION.read_text()).get("commit") or "")
    except (OSError, ValueError):
        return ""
#: One stamp per ACTION, not per checkout.  A single shared name looked
#: harmless because concurrent submits from one tree write the same bytes --
#: but the worker re-verifies the live stamp against the closure its action
#: pinned, and by then a later submit has replaced it with a *different*
#: identity, because the tree moved in between (pytest bytecode, result logs,
#: whatever a neighbouring shard did).  Hence "live code closure differs from
#: the action-pinned closure", ten of them in one fan-out.  Atomic writing
#: fixes torn reads and does nothing for this; separate files fix both.
# Keep ``--help`` usable while a coherent runtime publication is rolling from
# an older core to this pbrun. A real submission still calls the new shared
# identity function below and therefore fails closed rather than mixing rules.
STAMP_PREFIX = getattr(pb, "PBRUN_STAMP_PREFIX", ".pbrun-closure.")
#: Every action tees its output to a file inside the checkout, and the
#: worker refuses to start when that file already exists.  A fixed name
#: therefore lets the first submit from a tree poison every later one:
#: 19 of the queue's failures were exactly this, all reading "declared
#: result path must be absent before execution".  The name is derived
#: from what distinguishes the action, so two different commands get two
#: files while a resubmit of the same command still lands on the same
#: name and stays a CAS hit.
RESULT_PREFIX = getattr(pb, "PBRUN_RESULT_PREFIX", "pbrun_result.")
CONTAINER_OWNER_ENV = "PRISMABUILD_CONTAINER_OWNER"
CONTAINER_MARKER_ENV = "PRISMABUILD_CONTAINER_MARKER"
CONTAINER_WRAPPER_DIR = RUNTIME_ROOT / "tools"


def _git_identity(cwd: Path) -> dict[str, str]:
    """Commit plus a digest of the working-tree delta, or a named refusal."""

    try:
        return pb.git_checkout_identity(cwd)
    except pb.ActionContractError as exc:
        raise SystemExit(f"pbrun: cannot identify checkout: {exc}") from None


def git_repository_root(cwd: Path) -> Path | None:
    """Return the worktree root, or ``None`` when it cannot be snapshotted."""

    try:
        completed = subprocess.run(
            ["git", "-C", str(cwd), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    root = Path(completed.stdout.strip()).resolve()
    try:
        cwd.resolve().relative_to(root)
    except ValueError:
        return None
    return root


def _snapshot_git(
    cwd: Path,
    argv: list[str],
    *,
    environment: dict[str, str] | None = None,
    input_text: str | None = None,
    accepted_returncodes: tuple[int, ...] = (0,),
    strip: bool = True,
) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(cwd), *argv],
            env=environment,
            input=input_text,
            capture_output=True,
            text=True,
            errors="surrogateescape",
            timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(f"pbrun: cannot snapshot checkout: {exc}") from exc
    if completed.returncode not in accepted_returncodes:
        detail = (completed.stderr or completed.stdout).strip()
        raise SystemExit(
            f"pbrun: cannot snapshot checkout: {detail or completed.returncode}"
        )
    return completed.stdout.strip() if strip else completed.stdout


def snapshot_path_roster(
    root: Path, *, extra_paths: tuple[str, ...] = ()
) -> list[str]:
    """Tracked plus nonignored-untracked paths, each counted once."""

    raw_paths = _snapshot_git(
        root,
        ["ls-files", "-co", "--exclude-standard", "-z"],
        strip=False,
    )
    return list(dict.fromkeys(
        [path for path in raw_paths.split("\0") if path] + list(extra_paths)
    ))


def require_working_tree_size(
    root: Path, paths: list[str], *, max_bytes: int
) -> int:
    """Cheap pre-hash bound over logical bytes at each checkout path."""

    logical_bytes = 0
    for relative in paths:
        candidate = root / relative
        try:
            metadata = candidate.lstat()
        except FileNotFoundError:
            continue  # a tracked deletion contributes no materialized bytes
        except OSError as exc:
            raise SystemExit(
                f"pbrun: cannot inspect checkout path {relative!r}: {exc}"
            ) from exc
        if stat.S_ISREG(metadata.st_mode):
            size = metadata.st_size
        elif stat.S_ISLNK(metadata.st_mode):
            try:
                size = len(os.fsencode(os.readlink(candidate)))
            except OSError as exc:
                raise SystemExit(
                    f"pbrun: cannot read checkout symlink {relative!r}: {exc}"
                ) from exc
        elif stat.S_ISDIR(metadata.st_mode):
            # ``ls-files`` emits a directory path only for a gitlink. The
            # authoritative tree pass below refuses mode 160000 by name.
            continue
        else:
            raise SystemExit(
                "pbrun: checkout path has an unsupported file type: "
                f"{relative!r}"
            )
        # Per path, deliberately: hard-linked files become separate checkout
        # files and therefore consume their logical size more than once.
        logical_bytes += size
    if logical_bytes > max_bytes:
        raise SystemExit(
            "pbrun: logical working tree is "
            f"{logical_bytes} bytes, above the {max_bytes}-byte safety limit; "
            "refused before Git hashes or compresses the checkout"
        )
    return logical_bytes


def require_untransformed_checkout(root: Path, paths: list[str]) -> None:
    """Refuse Git clean/smudge rules that can change snapshotted bytes."""

    autocrlf = _snapshot_git(
        root,
        ["config", "--get", "core.autocrlf"],
        accepted_returncodes=(0, 1),
    ).strip().lower()
    if autocrlf not in {"", "0", "false", "no", "off"}:
        raise SystemExit(
            "pbrun: checkout has an active Git content transform: "
            f"core.autocrlf={autocrlf!r}. Disable it before snapshotting."
        )

    if not paths:
        return
    attributes = _snapshot_git(
        root,
        ["check-attr", "-z", "-a", "--stdin"],
        input_text="\0".join(paths) + "\0",
        strip=False,
    ).split("\0")
    if attributes and attributes[-1] == "":
        attributes.pop()
    if len(attributes) % 3:
        raise SystemExit("pbrun: Git returned malformed content attributes")
    for index in range(0, len(attributes), 3):
        path, name, value = attributes[index:index + 3]
        if (
            name in CONTENT_TRANSFORM_ATTRIBUTES
            and value not in {"unset", "unspecified"}
        ):
            raise SystemExit(
                "pbrun: checkout has an active Git content transform at "
                f"{path!r}: {name}={value!r}. Snapshot transport supports "
                "only worktree bytes that Git stores and checks out unchanged."
            )


def require_supported_snapshot_tree(
    root: Path,
    tree: str,
    *,
    environment: dict[str, str],
    max_bytes: int,
) -> int:
    """Return logical checkout bytes after rejecting unbundled gitlinks."""

    listing = _snapshot_git(
        root,
        ["ls-tree", "-r", "-l", "-z", tree],
        environment=environment,
        strip=False,
    )
    logical_bytes = 0
    for row in listing.split("\0"):
        if not row:
            continue
        metadata, separator, path = row.partition("\t")
        fields = metadata.split()
        if not separator or len(fields) != 4:
            raise SystemExit("pbrun: Git returned a malformed snapshot tree")
        mode, kind, object_id, size = fields
        if mode == "160000" or kind == "commit":
            raise SystemExit(
                "pbrun: checkout snapshot refuses gitlink/submodule path "
                f"{path!r}; the parent Git bundle does not carry its working bytes"
            )
        if kind != "blob" or not size.isdigit():
            raise SystemExit(
                "pbrun: checkout snapshot contains unsupported Git entry "
                f"{path!r} ({mode} {kind})"
            )
        if mode == "120000":
            target = _snapshot_git(
                root,
                ["cat-file", "blob", object_id],
                environment=environment,
                strip=False,
            )
            normalized = posixpath.normpath(
                posixpath.join(posixpath.dirname(path), target)
            )
            if (
                not target
                or target.startswith("/")
                or normalized == ".."
                or normalized.startswith("../")
                or normalized == ".git"
                or normalized.startswith(".git/")
            ):
                raise SystemExit(
                    "pbrun: checkout snapshot symlink points outside the "
                    f"sealed repository: {path!r} -> {target!r}"
                )
        # Count each materialized pathname, not unique object ids: two paths
        # naming one blob occupy two files in the worker checkout.
        logical_bytes += int(size)
    if logical_bytes > max_bytes:
        raise SystemExit(
            "pbrun: logical checkout tree is "
            f"{logical_bytes} bytes, above the {max_bytes}-byte safety limit; "
            "remove generated data from the worktree or lower its footprint"
        )
    return logical_bytes


def require_complete_history(root: Path) -> None:
    """Refuse a source whose own history it cannot hand a worker.

    The bundle now walks from the snapshot commit through its parents, so a
    shallow or partial clone has nothing to walk into: ``bundle create``
    fails deep inside pack-objects with ``Failed to traverse parents``, which
    tells the submitter nothing about what to do.  Say the actual thing --
    the ancestry a diff-derived gate needs is not in this checkout -- and say
    it before any bytes are hashed.
    """

    if _snapshot_git(root, ["rev-parse", "--is-shallow-repository"]) == "true":
        raise SystemExit(
            "pbrun: this checkout is a shallow clone, so its snapshot cannot "
            "carry the ancestry a worker needs; unshallow it "
            "(git fetch --unshallow) before submitting"
        )
    partial = _snapshot_git(
        root,
        ["config", "--get", "extensions.partialclone"],
        accepted_returncodes=(0, 1),
    )
    if partial:
        raise SystemExit(
            "pbrun: this checkout is a partial clone, so its snapshot cannot "
            "carry the ancestry a worker needs; fetch the missing objects "
            "(git repack -a -d) before submitting"
        )


def resolve_snapshot_refs(
    root: Path, names: Sequence[str]
) -> dict[str, str]:
    """Bind each requested branch name to the id it has in the source now.

    Refused here rather than on a worker: a name that does not resolve is a
    typo, and the honest place to say so is the terminal of the person who
    typed it, before an action key exists.  Every name is resolved fully
    qualified -- a bare ``master`` could otherwise pick up a tag or a remote
    branch of the same name and seal a different object than the submitter
    meant.
    """

    resolved: dict[str, str] = {}
    for name in names:
        if name in resolved:
            raise SystemExit(
                f"pbrun: --snapshot-ref {name} was requested twice"
            )
        try:
            # The same rule a worker will apply to the queued record, applied
            # where the person who typed it is still watching.
            pb.validate_pbrun_snapshot_ref_name(
                name, where=f"--snapshot-ref {name}"
            )
        except pb.ActionContractError as exc:
            raise SystemExit(f"pbrun: {exc}") from exc
        checked = _snapshot_git(
            root,
            ["check-ref-format", "--branch", name],
            accepted_returncodes=(0, 1, 128),
        )
        # ``check-ref-format --branch`` also expands ``@{-1}``, so a name it
        # rewrites is a name that means something else on a worker.
        if checked != name:
            raise SystemExit(
                f"pbrun: --snapshot-ref {name} is not a plain branch name"
            )
        oid = _snapshot_git(
            root,
            [
                "rev-parse", "--verify", "--quiet",
                f"refs/heads/{name}^{{commit}}",
            ],
            accepted_returncodes=(0, 1),
        )
        if not oid:
            raise SystemExit(
                f"pbrun: --snapshot-ref {name} names no branch in this "
                "checkout; the snapshot can only advertise refs the source has"
            )
        resolved[name] = oid
    return resolved


def build_git_checkout_snapshot(
    cwd: Path,
    *,
    stamp_name: str,
    cas: pb.PrismaBuildCAS,
    max_bytes: int = CHECKOUT_SNAPSHOT_MAX_BYTES,
    expected_identity: dict[str, str] | None = None,
    snapshot_refs: Sequence[str] = (),
) -> dict[str, object]:
    """Publish the exact dirty tree as an immutable Git bundle with ancestry."""

    root = git_repository_root(cwd)
    if root is None:
        raise SystemExit("pbrun: a non-Git checkout cannot be materialized")
    require_checkout_snapshot_limit(max_bytes)
    declared_stamp = cwd / stamp_name
    if declared_stamp.is_symlink():
        raise SystemExit("pbrun: checkout stamp must not be a symlink")
    stamp = declared_stamp.resolve(strict=True)
    try:
        observed_stamp_relative = stamp.relative_to(root).as_posix()
    except ValueError as exc:
        raise SystemExit("pbrun: checkout stamp is outside its Git worktree") from exc
    if not stamp.is_file():
        raise SystemExit("pbrun: checkout stamp must be a regular file")
    subdirectory = cwd.relative_to(root).as_posix() or "."
    stamp_relative = (
        Path(stamp_name)
        if subdirectory == "."
        else Path(subdirectory) / stamp_name
    ).as_posix()
    if observed_stamp_relative != stamp_relative:
        raise SystemExit("pbrun: checkout stamp resolves through a symlinked path")
    paths = snapshot_path_roster(root, extra_paths=(stamp_relative,))
    require_working_tree_size(root, paths, max_bytes=max_bytes)
    require_untransformed_checkout(root, paths)
    identity = expected_identity or _git_identity(cwd)
    if _git_identity(cwd) != identity:
        raise SystemExit("pbrun: checkout changed before it could be snapshotted")
    parent = identity["head"]
    require_complete_history(root)
    resolved_refs = resolve_snapshot_refs(root, snapshot_refs)

    with tempfile.TemporaryDirectory(prefix="pbrun-snapshot.") as temporary_raw:
        temporary = Path(temporary_raw)
        object_directory = temporary / "objects"
        (object_directory / "info").mkdir(parents=True)
        (object_directory / "pack").mkdir()
        index = temporary / "index"
        source_objects_raw = _snapshot_git(root, ["rev-parse", "--git-path", "objects"])
        source_objects = Path(source_objects_raw)
        if not source_objects.is_absolute():
            source_objects = root / source_objects
        object_environment = dict(os.environ)
        object_environment.update(
            {
                "GIT_INDEX_FILE": str(index),
                "GIT_OBJECT_DIRECTORY": str(object_directory),
                "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(source_objects.resolve()),
                "GIT_AUTHOR_NAME": "PrismaBuild",
                "GIT_AUTHOR_EMAIL": "prismabuild@example.invalid",
                "GIT_AUTHOR_DATE": "2000-01-01T00:00:00+00:00",
                "GIT_COMMITTER_NAME": "PrismaBuild",
                "GIT_COMMITTER_EMAIL": "prismabuild@example.invalid",
                "GIT_COMMITTER_DATE": "2000-01-01T00:00:00+00:00",
            }
        )
        # An alternate index begins empty. Overlaying the worktree directly
        # would therefore treat a HEAD-tracked file that now matches an ignore
        # rule as untracked and omit it. Seed the exact tracked roster first;
        # ``git add -A`` then applies deletions and live-byte changes on top.
        _snapshot_git(
            root, ["read-tree", "HEAD"], environment=object_environment
        )
        _snapshot_git(root, ["add", "-A"], environment=object_environment)
        _snapshot_git(
            root,
            ["add", "-f", "--", stamp_relative],
            environment=object_environment,
        )
        tree = _snapshot_git(root, ["write-tree"], environment=object_environment)
        require_supported_snapshot_tree(
            root,
            tree,
            environment=object_environment,
            max_bytes=max_bytes,
        )
        # The snapshot's parent is the source HEAD, so the sealed commit is
        # the source history with one more commit on it.  A worker's
        # ``HEAD~1``, ``merge-base`` and ``BASE...HEAD`` then resolve, which
        # is what a diff-derived gate is made of; the parentless shape this
        # replaces made every one of those a ``fatal: ambiguous argument``.
        # Identity, timestamps and message stay fixed, so the commit remains
        # a deterministic function of (tree, parent) rather than of who ran
        # the submit.
        commit = _snapshot_git(
            root,
            ["commit-tree", tree, "-p", parent],
            environment=object_environment,
            input_text="PrismaBuild pbrun checkout snapshot v2\n",
        )
        bare = temporary / "bundle.git"
        _snapshot_git(root, ["init", "-q", "--bare", str(bare)])
        ref = f"refs/heads/{pb.PBRUN_CHECKOUT_SNAPSHOT_REF_NAME}"
        _snapshot_git(
            root,
            [f"--git-dir={bare}", "update-ref", ref, commit],
            environment=object_environment,
        )
        for name, sealed_id in sorted(resolved_refs.items()):
            _snapshot_git(
                root,
                [
                    f"--git-dir={bare}", "update-ref",
                    f"refs/heads/{name}", sealed_id,
                ],
                environment=object_environment,
            )
        bundle = temporary / "checkout.bundle"
        # ``bundle create`` walks every named ref, so the ancestry travels by
        # construction: no explicit history depth to choose, and a requested
        # branch that has diverged simply adds its own side.  The byte ceiling
        # below is what keeps that bounded.
        _snapshot_git(
            root,
            [
                f"--git-dir={bare}", "bundle", "create", str(bundle), ref,
                *(f"refs/heads/{name}" for name in sorted(resolved_refs)),
            ],
            environment=object_environment,
        )
        size = bundle.stat().st_size
        if size > max_bytes:
            raise SystemExit(
                "pbrun: checkout snapshot is "
                f"{size} bytes, above the {max_bytes}-byte safety limit; "
                "remove generated data from the worktree or lower its footprint"
            )
        if _git_identity(cwd) != identity:
            raise SystemExit("pbrun: checkout changed while it was snapshotted")
        snapshot_input, _ = cas.ingest_input(
            bundle, input_id=pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID
        )
    return pb.validate_pbrun_checkout_snapshot(
        {
            "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2,
            "commit": commit,
            "parent": parent,
            "subdirectory": subdirectory,
            "input": snapshot_input,
            "refs": resolved_refs,
        }
    )


def require_relocatable_checkout(
    command: list[str],
    variables: dict[str, str],
    cwd: Path,
    *,
    repository_root: Path | None = None,
) -> None:
    """Refuse any value that would escape a materialized tree to the source."""

    root = repository_root or git_repository_root(cwd)
    if root is None:
        raise SystemExit(
            "pbrun: cannot verify relocation outside a Git checkout"
        )
    source = str(root.resolve())
    offenders = [f"argv: {token}" for token in command if source in str(token)]
    offenders.extend(
        f"environment {name}: {value}"
        for name, value in variables.items()
        if source in str(value)
    )
    if offenders:
        rendered = "\n".join(f"  - {value}" for value in offenders)
        raise SystemExit(
            "pbrun: portable execution refuses a submitter repository path "
            "(a submitter checkout path):\n"
            f"{rendered}\n"
            "Use paths relative to --cwd, move external data outside the "
            "checkout, or ingest external data as a declared CAS input."
        )


def _parse_demand(text: str) -> dict[str, int]:
    demand: dict[str, int] = {}
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise SystemExit(f"--demand wants k=v pairs, got {part!r}")
        key, _, value = part.partition("=")
        demand[key.strip()] = int(value)
    return demand


def exclusive_gpu_demand(queue, tags) -> int:
    """The GPU slots "the whole box" means, from what the boxes announce.

    The largest capacity among live workers that carry every required tag: a
    demand smaller than that would leave a box able to run something else
    alongside, which is what ``--exclusive`` is asking not to happen, and a
    demand larger than that is unclaimable on every box in the fleet.
    """

    wanted = {str(x) for x in (tags or [])}
    best = 0
    for offer in queue.offers():
        if not offer.get("has_gpu"):
            continue
        if not wanted.issubset({str(x) for x in (offer.get("tags") or [])}):
            continue
        capacity = offer.get("capacity") or {}
        best = max(best, int(capacity.get("gpu", 0)))
    if best <= 0:
        raise SystemExit(
            "pbrun: --exclusive needs to know how many GPU slots one box has, "
            "and no live worker matching "
            f"{sorted(wanted) or '(any tag)'} has announced one. Start a "
            "worker, or say it explicitly with --gpu-capacity N.")
    return best


def result_and_stamp_names(
    command,
    cwd,
    demand,
    variables,
    *,
    identity=None,
    logical_cwd=None,
    placement=None,
):
    """The result file and the closure stamp this submission writes.

    Returned together because they share one fingerprint and one reason for
    its shape.  The commit is IN that fingerprint, so both names belong to the
    commit they describe.  Without it, one command run from one checkout has
    one stamp path and one result path forever while the *content* of both
    moves with every commit:

    * the stamp gets rewritten under a worker still verifying the previous
      commit's action, which reads as "live code closure differs from the
      action-pinned closure" -- a real refusal for a file that was correct
      when the action was sealed; and
    * a 31-minute suite at one commit and its re-run at the next write the
      same ``pbrun_result.*.txt``, so whichever finishes second destroys the
      other's **declared** result and the runner reports "action succeeded
      without its declared result file".  Not hypothetical: that ate a green
      1268-test suite on 2026-09-04.

    The same command, commit, and normalized effective placement still
    fingerprint identically, so reordered/duplicate tags do not disturb a CAS
    hit. A different admissible worker population gets different paths just as
    it gets a different action key.
    """

    identity = _git_identity(cwd) if identity is None else identity
    cwd_identity = str(cwd) if logical_cwd is None else str(logical_cwd)
    fingerprint = hashlib.sha256(
        json.dumps([command, cwd_identity, demand, variables, identity,
                    placement or {"required_tags": []}],
                   sort_keys=True).encode()
    ).hexdigest()[:16]
    return (f"{RESULT_PREFIX}{fingerprint}.txt",
            f"{STAMP_PREFIX}{fingerprint}.json")


def container_owner(
    command,
    cwd,
    demand,
    variables,
    *,
    determinism,
    retry_policy,
    marker_root,
    identity=None,
    logical_cwd=None,
    placement=None,
) -> str:
    """Stable ownership id sealed before the action key exists.

    The action key includes the environment, and the environment needs this id,
    so using the final key would be recursive.  Hash the complete pre-lifecycle
    submission identity, including task and retry policy, normalized effective
    placement, the pre-owner environment, and the marker namespace, instead.
    Adding the owner and marker variables afterwards is deterministic and
    leaves no caller-chosen ownership namespace.
    """

    identity = _git_identity(Path(cwd)) if identity is None else identity
    cwd_identity = str(cwd) if logical_cwd is None else str(logical_cwd)
    pre_owner_identity = {
        "schema": "prismaquant.prismabuild.container_owner_identity.v1",
        "task": {"determinism": determinism},
        "checkout": identity,
        "params": {
            "command": command,
            "cwd": cwd_identity,
            "demand": demand,
            "placement": placement or {"required_tags": []},
            "retry_policy": retry_policy,
        },
        "environment": {"variables": variables},
        "container_lifecycle": {"marker_root": str(marker_root)},
    }
    return hashlib.sha256(
        json.dumps(pre_owner_identity, sort_keys=True).encode()
    ).hexdigest()


def keep_droppings_out_of_git(cwd: Path) -> Path | None:
    """Teach git to ignore the stamp and the result logs, locally.

    Ask git where its exclude file is; do not compute it.  ``cwd/.git`` is a
    DIRECTORY only for a repository root that is not a linked worktree -- in
    a ``git worktree`` checkout it is a file, and in a subdirectory of the
    repo it is nothing -- so the old path silently did nothing in exactly the
    checkouts agents make.  The stamp then showed as untracked, and in a tree
    several agents stage broadly in, an untracked file is a file that gets
    committed: one landed on this branch.

    ``--git-common-dir``, not ``--git-dir``.  Measured, because the two differ
    in a worktree and only one is read: a pattern in
    ``.git/worktrees/<name>/info/exclude`` does not match (``git check-ignore``
    exits 1), the same pattern in the common ``.git/info/exclude`` does.  That
    is also the right scope -- these generated basename grammars are pbrun's
    everywhere in the repo, not per worktree.

    Returns the file it wrote, or ``None`` when Git says this is not a
    repository. Once Git identifies a checkout, inspection or publication
    failure refuses: proceeding could leave a broad legacy glob hiding input
    bytes from the action identity.
    """

    try:
        marker = pb.find_git_worktree_marker(cwd)
    except pb.ActionContractError as exc:
        raise SystemExit(f"pbrun: {exc}") from exc
    try:
        out = subprocess.run(["git", "-C", str(cwd), "rev-parse",
                              "--git-common-dir"],
                             capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as exc:
        raise SystemExit(
            f"pbrun: cannot inspect local Git excludes: {exc}"
        ) from exc
    if out.returncode != 0:
        if marker is not None:
            detail = (out.stderr or out.stdout).strip()
            raise SystemExit(
                "pbrun: cannot inspect local Git excludes: Git rev-parse "
                f"failed for recognized checkout {cwd}: "
                f"{detail or out.returncode}"
            )
        return None                           # true plain directory
    try:
        common = Path(out.stdout.strip())
        if not common.is_absolute():
            common = cwd / common             # older git answers ".git"
        exclude = common / "info" / "exclude"
        exclude.parent.mkdir(parents=True, exist_ok=True)
        current = exclude.read_text() if exclude.exists() else ""
        legacy_patterns = {f"{STAMP_PREFIX}*", f"{RESULT_PREFIX}*"}
        lines = [
            line
            for line in current.splitlines(keepends=True)
            if line.rstrip("\r\n") not in legacy_patterns
        ]
        updated = "".join(lines)
        if updated and not updated.endswith(("\n", "\r")):
            updated += "\n"
        present = {line.rstrip("\r\n") for line in lines}
        for pattern in pb.pbrun_git_exclude_patterns():
            if pattern not in present:
                updated += pattern + "\n"
        if updated != current:
            scratch = exclude.with_name(
                f"{exclude.name}.pbrun.{os.getpid()}.{uuid.uuid4().hex[:8]}"
            )
            try:
                scratch.write_text(updated, encoding="utf-8")
                os.replace(scratch, exclude)
            finally:
                if scratch.exists():
                    scratch.unlink()
        return exclude
    except OSError as exc:
        raise SystemExit(
            f"pbrun: cannot update pbrun Git excludes: {exc}"
        ) from exc


def placement_tags(
    cwd: Path,
    *,
    explicit: list[str],
    here: bool,
    hostname: str,
    portable_checkout: bool = False,
    command: list[str] | None = None,
    repository_root: Path | None = None,
    environment: dict[str, str] | None = None,
    caller_environment: dict[str, str] | None = None,
    anywhere: bool = False,
) -> list[str]:
    """Return the placement tags for an action whose working directory is ``cwd``.

    Placement is PrismaBuild's decision, not the submitter's.  The submitter
    knows one thing the pool cannot infer -- an explicit ``--tag`` naming a
    hardware class the work requires -- and everything else follows from where
    the checkout lives:

    * A Git checkout is snapshotted through the CAS. Its command executable is
      resolved exactly from argv[0] and the declared PATH. A submitter-local
      executable retains the source-host pin; an absent executable refuses.
      Direct argv and caller-environment paths are also screened
      conservatively. This lexical screen is not proof of a shell program's or
      application code's indirect inputs: ``--tag`` names the worker class
      owning those dependencies, while ``--anywhere`` explicitly asserts they
      are portable.
    * The path rule remains only for already-published legacy queue records.
      New ``pbrun`` submissions refuse a non-Git directory rather than execute
      mutable bytes. ``--here`` still forces a host pin for work genuinely
      about *this* machine.

    Nothing here decides *which* free box runs a shared-checkout action; the
    queue does, from the demand and what each worker offers.  That separation
    is the point.
    """

    if explicit:
        return list(explicit)
    if here:
        return [hostname]
    if anywhere:
        return []
    if portable_checkout:
        root = (repository_root or cwd).resolve()
        if command is None:
            # Library callers asking only about source-checkout placement do
            # not have a command contract to classify. The CLI always passes
            # argv and therefore always takes the exact executable gate below.
            return []

        def scope(candidate: Path) -> tuple[Path, bool, bool]:
            try:
                resolved_path = candidate.resolve(strict=False)
            except (OSError, RuntimeError) as exc:
                raise SystemExit(
                    f"pbrun: cannot resolve declared path {candidate}: {exc}"
                ) from exc
            try:
                resolved_path.relative_to(root)
                inside_repository = True
            except ValueError:
                inside_repository = False
            try:
                resolved_path.relative_to(SHARED_ROOT.resolve())
                on_shared_storage = True
            except ValueError:
                on_shared_storage = False
            return resolved_path, inside_repository, on_shared_storage

        def command_executable() -> Path:
            if not command or not command[0]:
                raise SystemExit("pbrun: portable placement requires argv[0]")
            raw = command[0]
            if os.sep in raw:
                candidate = Path(raw)
                if not candidate.is_absolute():
                    candidate = cwd / candidate
                executable = candidate.resolve(strict=False)
                if not executable.is_file() or not os.access(executable, os.X_OK):
                    raise SystemExit(
                        "pbrun: command executable is absent or not executable "
                        f"on the submitting box: {raw!r}. Pass --tag for the "
                        "worker class that owns it, or --anywhere to assert an "
                        "identical executable contract on every eligible worker."
                    )
                return executable

            declared_path = (environment or {}).get("PATH") or os.defpath
            search_parts = []
            for entry in declared_path.split(os.pathsep):
                directory = Path(entry) if entry else cwd
                if not directory.is_absolute():
                    directory = cwd / directory
                search_parts.append(str(directory.resolve(strict=False)))
            found = shutil.which(raw, path=os.pathsep.join(search_parts))
            if found is None:
                raise SystemExit(
                    "pbrun: command executable cannot be resolved from the "
                    f"declared PATH: {raw!r}. Pass --tag for the worker class "
                    "that owns it, or --anywhere to assert an identical "
                    "executable contract on every eligible worker."
                )
            return Path(found).resolve(strict=True)

        executable, executable_in_repo, executable_shared = scope(
            command_executable()
        )
        if not executable_in_repo and not executable_shared:
            return [hostname]

        # This is intentionally a conservative lexical screen, never the
        # authority for command interpretation. argv[0] above is exact. Here
        # only direct path-shaped tokens and caller-declared values can add a
        # host pin; shell strings and application configuration remain the
        # caller's explicit --tag/--anywhere responsibility.
        candidates: list[Path] = []
        for token in (command or [])[1:]:
            raw = token.split("=", 1)[1] if token.startswith("-") and "=" in token else token
            if token.startswith("-") and raw == token:
                continue
            candidate = Path(raw)
            relative_candidate = cwd / candidate
            if candidate.is_absolute() or os.sep in raw or relative_candidate.exists():
                candidates.append(candidate)
        for name, value in (caller_environment or {}).items():
            if name == "PATH":
                continue  # argv[0] was resolved against the complete value above
            for raw in value.split(os.pathsep):
                candidate = Path(raw)
                relative_candidate = cwd / candidate
                if candidate.is_absolute() or os.sep in raw or relative_candidate.exists():
                    candidates.append(candidate)

        for candidate in dict.fromkeys(candidates):
            path = candidate if candidate.is_absolute() else cwd / candidate
            resolved_path, inside_repository, on_shared_storage = scope(path)
            if inside_repository or on_shared_storage:
                continue
            if not resolved_path.exists() and not resolved_path.is_symlink():
                raise SystemExit(
                    "pbrun: direct argv or caller environment names an "
                    "external path absent from the submitting box: "
                    f"{candidate}. Pass --tag for the worker class that owns "
                    "it, or --anywhere to assert its portability."
                )
            return [hostname]
        return []
    return [hostname] if is_box_local(cwd) else []


def is_box_local(cwd: Path) -> bool:
    """Does this path exist on exactly one box?

    Resolution happens here and only here.  The rule itself lives in
    ``pool.is_box_local_path`` because the queue applies it too, to paths
    other boxes recorded -- and it must not resolve those, since the reading
    box's symlinks say nothing about a tree it does not have.  At submit the
    path is local and real, so a symlink into shared storage is followed and
    the checkout is correctly called shared.
    """

    return pool.is_box_local_path(cwd.resolve())


def require_checkout_owned_scripts(
    command: list[str],
    cwd: Path,
    *,
    repository_root: Path | None = None,
) -> None:
    """Refuse script files whose bytes the repository snapshot cannot bind.

    The literal argv is part of an action, but a pathname is not the bytes at
    that pathname.  ``pbrun``'s code closure binds the checkout HEAD and dirty
    state, so a script anywhere below the repository root is covered. Relative
    tokens are resolved from the requested working directory, exactly as task
    argv will resolve them after relocation. A helper outside the repository
    could change after the action was sealed and the same action key would then
    execute different code.

    This checks every *direct* argv token that resolves to a script, including
    an interpreter's ``python /path/tool.py`` argument.  It does not pretend
    to parse shell programs passed through ``sh -c``; callers using shell
    indirection must keep executable helpers under the checkout.  Native
    executables are toolchain members rather than scripts and are not covered
    by this gate.
    """

    working_directory = cwd.resolve()
    root = (repository_root or working_directory).resolve()
    offenders: list[Path] = []
    script_suffixes = {".bash", ".py", ".rb", ".sh"}
    for token in command:
        if not token or token.startswith("-"):
            continue
        candidate = Path(token)
        if not candidate.is_absolute():
            candidate = working_directory / candidate
        try:
            resolved = candidate.resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        if not resolved.is_file():
            continue
        try:
            with resolved.open("rb") as handle:
                prefix = handle.read(2)
        except OSError:
            continue
        if prefix != b"#!" and resolved.suffix.lower() not in script_suffixes:
            continue
        try:
            resolved.relative_to(root)
        except ValueError:
            offenders.append(resolved)

    if offenders:
        rendered = "\n".join(f"  - {path}" for path in sorted(set(offenders)))
        raise SystemExit(
            "pbrun: executable script bytes are outside the snapshotted "
            "repository:\n"
            f"{rendered}\n"
            "Move each helper under the repository so its bytes are bound by "
            "the action's code closure. Shell-indirected helpers must follow "
            "the same rule."
        )


def _width_of_the_pin(queue, intent, tags: list[str], hostname: str) -> str:
    """How many boxes this action WOULD have had, with the host tag taken off.

    Not "how many boxes match the pinned tags" -- that is one, by
    construction, and saying it would be a tautology dressed as a
    measurement.  A demand only this box can meet costs nothing to pin, and
    saying so keeps the notice from crying wolf on every GPU-heavy
    submission.  ``None`` from ``placeable_hosts`` means no worker has
    announced, and that stays unknown rather than being printed as zero.
    """

    unpinned = dict(intent)
    unpinned["tags"] = [t for t in tags if t != hostname]
    hosts = queue.placeable_hosts(unpinned)
    if hosts is None:
        return "Fleet width unknown: no worker has announced."
    others = [h for h in hosts if h != hostname]
    if not others:
        return "No other live box fits this demand, so the pin costs nothing now."
    return (f"{len(others)} other live box{'es' if len(others) > 1 else ''} "
            f"fit{'' if len(others) > 1 else 's'} this demand: "
            f"{', '.join(others)}.")


def pin_notice(
    queue,
    intent,
    *,
    cwd: Path,
    hostname: str,
    here: bool,
    portable_checkout: bool = False,
) -> str:
    """What the submitter is not otherwise told: this action is one box wide.

    The pin is a silent consequence of a path.  ``pbrun`` printed
    ``tags=['sparky']`` and nothing else, so the submitter -- usually an agent
    that just made itself a worktree under ``/home/rob/tmp`` -- had no way to
    know it had narrowed the fleet to one box.  Measured on the live queue,
    2026-09-04: 131 of 391 items carried a hostname tag, and 129 of those were
    a consequence of a path -- 114 pinned to ``sparky`` by a
    ``/home/rob/tmp/ts*`` worktree -- while sparky's queue backed up and the
    other two boxes idled.

    **Everything below is read off the tags that LANDED, never off the flags
    that asked for them.**  ``placement_tags`` returns ``list(explicit)`` the
    moment any ``--tag`` is given, so ``--here`` and a box-local checkout are
    both silently overridden by it.  A first version asked the ``here`` flag
    instead, and so announced "PINNED to sparky by --here, so no other box can
    claim this action" for a submission whose tags were ``['x86']`` -- naming,
    as the *other* box, the only box that could actually run it.  A notice
    about a pin has one job and that was it.

    Exclusivity is claimed only where it is provable.  A tag naming this host
    cannot be claimed elsewhere; a tag that merely happens to match one live
    box today -- ``gb10``, ``sparklina`` -- is a fact about the fleet as
    announced at this instant, and the second box offering it can claim an
    action whose tree it does not have.  So that case is reported as the
    contingency it is rather than as "match only this box".

    The explicit ``--tag`` list is deliberately NOT a parameter here.  The
    only thing it decides is what ``placement_tags`` returned, and that is
    already in ``intent``; taking it as well would leave a second way to ask
    the flags what the tags already answer, which is the bug this function
    was rewritten to close.  ``here`` stays, because ``--here`` on a shared
    checkout is indistinguishable from ``--tag <this host>`` by tags alone,
    and the override needs to know it was asked for.

    Returns "" when there is nothing to say -- an immutable snapshot that was
    already free to run anywhere.
    """

    tags = [str(t) for t in (intent.get("tags") or [])]
    local = is_box_local(cwd) and not portable_checkout
    pinned = hostname in tags               # the pin as it landed, not as asked
    claimants = queue.placeable_hosts(intent)
    others = None if claimants is None else sorted(
        h for h in claimants if h != hostname)

    if pinned:
        if others:
            # A host tag should be this box's alone; a worker started
            # elsewhere with ``--tag sparky`` makes it not.  Ask the placer
            # rather than assert the construction.
            return (f"pbrun: WARNING -- tags {tags} name {hostname}, but "
                    f"{', '.join(others)} offer that tag too, so this action "
                    f"is not exclusive to this box.  Check what those workers "
                    f"were started with.")
        if here and not local:
            head = (f"pbrun: PINNED to {hostname} by --here, so no other box "
                    f"can claim this action.")
        elif local:
            head = (f"pbrun: PINNED to {hostname} -- the checkout {cwd} is "
                    f"box-local, so no other box can claim this action.")
        else:
            head = (f"pbrun: PINNED to {hostname} by --tag {hostname}, so no "
                    f"other box can claim this action.")
        tail = ("" if not local else
                f"  Move the checkout under {SHARED_ROOT} to let any box claim "
                f"it, or accept the pin knowingly.")
        return f"{head}  {_width_of_the_pin(queue, intent, tags, hostname)}{tail}"

    # No host tag landed.  Say what did, and what it costs.
    notes: list[str] = []
    if here:
        notes.append(f"--here did NOT pin this action: an explicit --tag "
                     f"REPLACES the host tag rather than adding to it, so "
                     f"tags {tags} alone place it.")
    if local:
        if others is None:
            notes.append(f"WARNING -- the checkout {cwd} exists only on "
                         f"{hostname}, and no worker has announced, so tags "
                         f"{tags} may let another box claim this action and "
                         f"fail on the missing tree.")
        elif others:
            notes.append(f"WARNING -- the checkout {cwd} exists only on "
                         f"{hostname}, but tags {tags} let {', '.join(others)} "
                         f"claim this action.  It will fail there rather than "
                         f"run on the wrong tree.")
        else:
            # True of the fleet as announced, and only of that.  Nothing
            # reserves ``gb10`` or ``sparklina`` for one box, so the second
            # box offering it can claim a tree it does not have -- which is
            # the case the WARNING above exists to catch, arriving later.
            notes.append(f"the checkout {cwd} exists only on {hostname}, and "
                         f"no other live box offers tags {tags} -- but nothing "
                         f"reserves those tags for this box, so a box that "
                         f"starts offering them can claim this action and fail "
                         f"on the missing tree.")
        notes.append(f"Add --tag {hostname} if you meant this box, or move the "
                     f"checkout under {SHARED_ROOT}.")
    elif here:
        if claimants is None:
            notes.append("No worker has announced, so which box claims it is "
                         "unknown.")
        elif claimants:
            notes.append(f"{len(claimants)} live "
                         f"box{'es' if len(claimants) > 1 else ''} can claim "
                         f"it: {', '.join(claimants)}.")
        else:
            notes.append("No live box offers these tags.")
        notes.append(f"Add --tag {hostname} if you meant this box.")
    if not notes:
        return ""
    return "pbrun: " + "  ".join(notes)


def detach_line(
    key: str,
    *,
    transport: str,
    status: str,
    queue_root,
    published_unix: float | None = None,
    job_id: str | None = None,
    submission=None,
) -> str:
    """One line saying where a detached submission went, for a machine to read.

    Everything ``pbrun`` prints for a person goes to stderr, so stdout carries
    this and nothing else: a caller reads one line of JSON instead of parsing
    prose written to be read aloud.

    ``status`` is one of ``submitted`` (this call put the work somewhere),
    ``cache_hit`` (it was already in the CAS, so nothing was submitted) or
    ``attached`` (it was already running under an earlier submission, which
    this call joined rather than duplicated).  All three exit 0, and the last
    two name a run this process did not start.

    The generation travels in it because the terminal record a later wait looks
    for is identified by generation and not by key.  An action key is a content
    hash, so one key accumulates the records of every earlier run of the same
    work, and a waiter given only the key cannot tell this run's ending from
    the ending of a run that finished last week.
    """

    queue = Path(queue_root)
    return json.dumps(
        {
            "schema": DETACH_SCHEMA_V1,
            "action_key": str(key),
            "transport": str(transport),
            "status": str(status),
            "published_unix": published_unix,
            "job_id": str(job_id) if job_id is not None else None,
            "submission": str(submission) if submission is not None else None,
            "done": str(queue / pool.DONE / f"{key}.json"),
            "failed": str(queue / pool.FAILED / f"{key}.json"),
            "withdrawn": str(queue / pool.WITHDRAWN / f"{key}.json"),
        },
        sort_keys=True,
    )


def published_generation(q, key: str, path) -> float | None:
    """The generation the pull queue stamped on this submission, or ``None``.

    Read back rather than assumed: ``PoolQueue.publish`` stamps
    ``published_unix`` itself, and a worker may have renamed the item into
    ``claimed`` -- or run it to completion -- before this looks.  ``None`` is
    the honest answer when none of those places has it, and it tells a later
    wait to accept any record for the key rather than to pretend it knows which
    run the record belongs to.
    """

    candidates = [Path(path)]
    for state in (pool.CLAIMED, pool.DONE, pool.FAILED):
        candidates.append(q.item_path(state, key))
    for candidate in candidates:
        try:
            value = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        stamped = value.get("published_unix") if isinstance(value, dict) else None
        if isinstance(stamped, (int, float)) and not isinstance(stamped, bool):
            return float(stamped)
    return None


def terminal_record(path: Path, generation: float | None):
    """The record filed at ``path``, when it belongs to ``generation``.

    An action key is a content hash, so one key accumulates the endings of
    every run of the same work.  ``published_unix`` equality is the queue's own
    generation rule -- ``PoolQueue.terminal_outcome_covers`` states it, and
    ``slurm_lane._same_generation`` applies it on the other transport -- so a
    reader waiting for one run must skip the record of another.

    A record carrying no generation stands.  ``PoolQueue.finish`` writes one
    with no ``published_unix`` when a reaper concluded the claim underneath it,
    and records filed before generations were stamped have none either;
    staleness cannot be proved of those, and refusing them would hang a caller
    on the outcome that is the only account of what happened.
    """

    try:
        # Poll by readdir, not by stat.  The queue lives on NFS, where a stat
        # of a path that did not exist yet is negatively cached: the outcome
        # landed and a bare ``exists()`` kept answering False.  Listing the
        # directory revalidates it.
        if path.name not in os.listdir(path.parent):
            return None
    except OSError:
        return None
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(record, dict):
        return None
    if generation is None:
        return record
    theirs = record.get("published_unix")
    if isinstance(theirs, (int, float)) and not isinstance(theirs, bool):
        return record if float(theirs) == float(generation) else None
    return record


def landed_outcome(
    q, key: str, *, wait_s: float, generation: float | None = None
):
    """Block until this action's ending lands, and return it with its path.

    Watches all THREE terminal directories.  An action whose argv exits
    non-zero is retried and then filed under ``failed``, never under ``done``
    -- and this loop used to watch ``done`` alone, so a caller whose suite
    legitimately failed sat here until ``--wait-s`` expired (a DAY, by default)
    and then got exit 75 and the words "gave up waiting".  The work had run,
    three times, and said why each time; none of it reached the person waiting.
    Sixty-six items sat in ``failed`` when this was found, and the agents who
    submitted them reported the pool as having never scheduled their work.
    ``withdrawn`` is the third and is watched for exactly the same reason --
    and it is the one whose whole point is that a person decided it, so it
    would be the worst of the three to make somebody wait a day to hear about.

    Returns ``None`` when the caller's patience ran out first.  Split out of
    ``await_outcome`` so a waiter that reports many actions at once can share
    one deadline across them instead of spending ``--wait-s`` on each in turn.

    A caller that cannot name the generation gets the NEWEST ending rather than
    the first directory in order.  One key can hold a ``done`` from a run last
    week beside a ``failed`` from the run just now, and answering with the
    ``done`` because ``done`` is looked at first would report success for work
    that failed.
    """

    def _stamp(entry) -> float:
        value = entry[1].get("published_unix")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        return float("-inf")

    watched = [q.item_path(state, key)
               for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN)]
    deadline = time.monotonic() + wait_s
    while True:
        found = []
        for path in watched:
            record = terminal_record(path, generation)
            if record is not None:
                found.append((path, record))
        if len(found) == 1 or (found and generation is not None):
            return found[0]
        if found:
            return max(found, key=_stamp)
        # ``>=``, so a non-blocking probe (``wait_s=0``) does not spend a poll
        # interval finding out that it had none to spend.
        if time.monotonic() >= deadline:
            return None
        time.sleep(POLL_S)


def outcome_summary(q, outcome_path, outcome) -> dict:
    """One ending reduced to the fields a caller reports, after verification.

    The mutable terminal summary is state-machine output.  Where the record
    links immutable attempt records, the first-writer-published attempt decides
    what this returns, so a later stale-output refusal cannot replace the
    causal failure on the submitter's screen.  ``adopted_attempt_summary``
    verifies every canonical path, digest, byte count and generation on the way
    through, and refuses a summary that disagrees with the attempt it adopted.

    ``transport`` defaults to the pull queue: the SLURM lane stamps its own
    records and the pool's predate the field.
    """

    detail = outcome.get("detail") or {}
    status = str(outcome.get("status"))
    adopted = None
    if (
        "attempt_history" in outcome
        or "attempt_history_missing_before" in outcome
    ):
        adopted = q.adopted_attempt_summary(outcome)
        disposition = adopted["disposition"]
        if disposition != Path(outcome_path).parent.name:
            raise pool.PoolContractError(
                "terminal queue directory disagrees with the adopted immutable "
                f"attempt: {Path(outcome_path).parent.name!r} != {disposition!r}"
            )
        for field in ("status", "finished_unix", "finished_host", "detail"):
            if outcome.get(field) != adopted[field]:
                raise pool.PoolContractError(
                    "terminal queue summary disagrees with the adopted "
                    f"immutable attempt field {field!r}"
                )
        detail = adopted["detail"]
        status = str(adopted["status"])
    return {
        "action_key": str(outcome.get("action_key") or ""),
        "status": status,
        "detail": detail,
        "adopted": adopted,
        # ``executed`` and ``cache_hit`` both mean the work is done; that is
        # the pull queue's own rule, in ``adopted_attempt_summary``, which
        # routes both to ``done/``.
        "succeeded": status in {"executed", "cache_hit"},
        "transport": str(outcome.get("transport") or "pool"),
        "finished_host": outcome.get("finished_host"),
        "elapsed_s": detail.get("elapsed_s"),
        "returncode": detail.get("returncode"),
        "receipt_published": detail.get("receipt_published"),
        "attempts": outcome.get("attempts"),
        "withdrawn_by": outcome.get("withdrawn_by"),
        "reason": outcome.get("reason"),
    }


def outstanding_submission(q, key: str, *, lane_root=None):
    """The newest submission of this key anybody recorded, or ``None``.

    Returns ``(transport, generation, submission)``.  Newest wins because a key
    can legitimately have been carried by both transports -- the pull queue
    last week, SLURM today -- and the run being asked about is the one somebody
    just asked for.
    """

    candidates = []
    lane = slurm_lane.recorded_submission(key, root=lane_root)
    if isinstance(lane, dict):
        generation = lane.get("published_unix")
        if isinstance(generation, (int, float)) and not isinstance(generation, bool):
            candidates.append(("slurm", float(generation), lane))
    for state in (pool.READY, pool.CLAIMED):
        try:
            item = json.loads(
                q.item_path(state, key).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        generation = item.get("published_unix") if isinstance(item, dict) else None
        if isinstance(generation, (int, float)) and not isinstance(generation, bool):
            candidates.append(("pool", float(generation), item))
    if not candidates:
        return None
    return max(candidates, key=lambda entry: entry[1])


def live_submission(q, key: str, *, lane_root=None, **lane_commands):
    """The submission this key is still running under, or ``None``.

    A key is a content hash, so asking for the same work twice is the normal
    way to ask whether it is done -- and while the first ask is still running,
    the second must attach to it rather than start a second copy of it.  Two
    copies is not merely waste: they materialize the same checkout, take the
    same GPU twice and race to publish one receipt.

    Live means three things together: something was recorded, no ending covers
    that generation, and the thing that carries it still exists -- a job the
    controller still knows in a non-terminal state, an item nobody has claimed,
    or a claim whose lease is still being refreshed.  A forgotten job or a dead
    lease is not live, and the caller submits afresh, which is what the pull
    queue's own reaper would arrange for anyway.
    """

    found = outstanding_submission(q, key, lane_root=lane_root)
    if found is None:
        return None
    transport, generation, submission = found
    if landed_outcome(q, key, wait_s=0.0, generation=generation) is not None:
        return None
    if transport == "slurm":
        job_id = str(submission.get("job_id") or "")
        if not job_id:
            return None
        state = slurm_lane.query_state(job_id, **lane_commands)
        if state is None or state[0] in slurm_lane.TERMINAL_STATES:
            return None
        return found
    if q.item_path(pool.READY, key).exists():
        return found
    age = q.lease_age(key)
    if age is not None and age < pool.LEASE_TIMEOUT_S:
        return found
    return None


def await_outcome(
    q, key: str, *, wait_s: float, generation: float | None = None
) -> int:
    """Block until this action reaches a terminal directory, then report it.

    Split out of ``main`` so the outcome half can be tested without a
    submission: the bug this exists to prevent lived entirely in which
    directories the loop watched, which is exactly the part a live-queue
    test would have been least likely to reach.
    """

    landed = landed_outcome(q, key, wait_s=wait_s, generation=generation)
    if landed is None:
        print(f"pbrun: gave up waiting for {key[:12]}", file=sys.stderr)
        return 75
    outcome_path, outcome = landed

    summary = outcome_summary(q, outcome_path, outcome)
    detail = summary["detail"]
    status = summary["status"]
    if summary["adopted"] is not None:
        attempts = q.attempt_outcomes(outcome)
        missing = outcome.get("attempt_history_missing_before", 0)
        if isinstance(missing, int) and not isinstance(missing, bool) and missing:
            noun = "attempt" if missing == 1 else "attempts"
            print(
                f"pbrun: {missing} earlier {noun} "
                "predates immutable history",
                file=sys.stderr,
            )
        total_attempts = int(outcome.get("attempts", len(attempts)))
        for attempt in attempts:
            print(
                f"pbrun: attempt {attempt['attempt']}/{total_attempts} "
                f"{attempt.get('status')} ({attempt.get('disposition')})",
                file=sys.stderr,
            )
            sys.stdout.write(str(attempt.get("stdout") or ""))
            sys.stderr.write(str(attempt.get("stderr") or ""))
    else:
        # Backward compatibility for outcomes filed by a pre-history runtime.
        sys.stdout.write(str(detail.get("stdout") or ""))
        sys.stderr.write(str(detail.get("stderr") or ""))
    if status == "withdrawn":
        who = outcome.get("withdrawn_by") or "an operator"
        why = str(outcome.get("reason") or "").strip()
        print(f"pbrun: withdrawn by {who}"
              f"{' -- ' + why if why else ''}", file=sys.stderr)
        return WITHDRAWN_EXIT
    # ``elapsed_s`` is present and null on a SLURM record whose scheduler
    # provenance was purged, so the key's presence must not defeat the default.
    print(f"pbrun: {status} on {outcome.get('finished_host')} "
          f"in {(detail.get('elapsed_s') or 0):.0f}s", file=sys.stderr)
    if status == "cache_hit":
        return 0
    rc = detail.get("returncode")
    if isinstance(rc, int):
        return rc
    if status == "executed":
        return 0
    # A failure the worker itself raised carries no returncode -- the argv's
    # status is inside the exception text.  Surface the text; the caller gets a
    # non-zero exit either way, but the text is what makes it actionable.
    error = str(detail.get("error") or detail.get("exception") or "").strip()
    if error:
        print(f"pbrun: {error}", file=sys.stderr)
    print(f"pbrun: outcome filed under {outcome_path.parent.name} after "
          f"{outcome.get('attempts', '?')} attempt(s)", file=sys.stderr)
    return 1


def _report_stall(key: str, report) -> None:
    """Say that a running job has not moved.  Say it; do nothing about it.

    The lane decides *when* (``STALL_WINDOW_S`` of unchanged samples, repeated
    at ``STALL_REPORT_EVERY_S``); this decides the words.  The job is still
    running and keeps running: a stall is evidence for a person, and the only
    thing that ends it is that person's ``--withdraw`` or a ``--timeout-s``
    they asked for.  Wall-clock is never evidence of death.
    """

    minutes = int(report.stalled_for_s // 60)
    where = report.node or "an unknown node"
    print(f"pbrun: {key[:12]} slurm job {report.job_id} has shown no progress "
          f"for {minutes} min on {where}; it is still running. Withdraw with "
          f"pbrun --withdraw {key[:12]} if it is dead.",
          file=sys.stderr, flush=True)


#: The interpreter pbrun's sealed argv starts with.  A nonportable action
#: binds its exact bytes, so the name is stated once, where the scope is built.
SEALED_ARGV0 = "/bin/bash"


def require_host_class_scope(
    *, measurement: bool, host_class: str | None, transport: str
) -> None:
    """Refuse a scope the design cannot honour, before anything is sealed."""

    if measurement and host_class is None:
        raise SystemExit(
            "pbrun: --measurement requires --host-class CLASS.\n"
            "A measurement's numerics do not transfer across architectures, "
            "so its result is keyed on the host class that produced it "
            "(docs/design.md, \"Cache/action-key semantics\"); a portable "
            "measurement would let any box's KL stand in for another's."
        )
    if host_class is not None and transport != "slurm":
        raise SystemExit(
            "pbrun: --host-class needs --transport slurm.\n"
            "A host_class_keyed action is attested through the SLURM "
            "controller (docs/design.md, \"Worker preflight and execution "
            "attestation\"); a pull-queue worker refuses it at preflight, so "
            "submitting it there queues work that cannot run."
        )


def host_class_scope(
    host_class: str | None,
) -> tuple[dict[str, object], dict[str, str]]:
    """The execution scope and the toolchain a submission seals.

    A portable action declares no toolchain.  A host-class-keyed one is
    nonportable, and the core requires a nonportable action to bind the
    executable behind argv[0] and the ABI and accelerator facts of the box
    that runs it -- facts pbrun can read only from the box it runs on.  So a
    class-keyed submission carries this box's facts, and a worker of the
    class verifies each of them at preflight; a submission from a box of
    another class is refused there, naming the field that differs.
    """

    if host_class is None:
        return (
            {"portability": "portable", "platform_key": None, "host_class": None},
            {},
        )
    toolchain = {
        **pb.executable_toolchain_contract(SEALED_ARGV0),
        **pb.live_platform_toolchain_contract(),
    }
    return (
        {
            "portability": "host_class_keyed",
            "platform_key": None,
            "host_class": host_class,
        },
        toolchain,
    )


def slurm_outcome(
    action,
    *,
    cas,
    request_path,
    tags: list[str],
    demand: dict,
    exclusive: bool,
    timeout_s: float | None,
    wait_s: float,
    retry_safe: bool,
    max_attempts: int,
    anywhere: bool = False,
    detach: bool = False,
    runtime_root: Path = RUNTIME_ROOT,
    lane_root=None,
    queue_root=None,
    **lane_commands,
) -> int:
    """Run one sealed action through SLURM and report it the way the pool does.

    The user-facing contract is this transport's whole point of contact: an
    agent that submits with ``--transport slurm`` must read the same lines and
    get the same exit codes it got from the queue, or the cutover is a change
    to every caller rather than a change to one dispatcher.

    So the verdict comes from the CAS, not from ``sbatch``.  A receipt means
    the work was done -- whatever the job's exit status said afterwards -- and
    no receipt means it was not, even from a job that exited zero.  That is the
    rule ``PoolQueue.finish`` already applies; only the machinery underneath it
    differs.

    ``detach`` stops after ``sbatch`` accepted the job: the submission is
    announced on stdout as one line of JSON and this returns 0.  No ending is
    filed, because none has been observed -- whoever waits later files it, from
    the same recorded submission ``--withdraw`` already builds a record out of.
    """

    key = str(action["action_key"])
    resources = slurm_lane.LaneResources.from_demand(demand, exclusive=exclusive)
    lane_commands.setdefault("on_stall", lambda report: _report_stall(key, report))
    lane_commands.setdefault(
        "on_notice",
        lambda text: print(f"pbrun: {text}", file=sys.stderr, flush=True))
    # sbatch's own refusal is this transport's capability gate: an unknown
    # Feature or an impossible GRES is rejected at submit time, which is the
    # moment the pool path's ``capability_verdict`` spoke.  So it reaches the
    # caller as the message SLURM wrote, in the shape that message had, rather
    # than as a traceback.
    try:
        result = slurm_lane.run(
            action,
            cas=cas,
            request_path=request_path,
            placement=tags,
            resources=resources,
            partition=slurm_lane.partition_for(
                resources, tags, anywhere=anywhere),
            timeout_s=timeout_s,
            worker_script=runtime_root / "tools" / "prismabuild_worker.py",
            job_entry=runtime_root / "tools" / "fleet" / "slurm_job.py",
            retry_safe=retry_safe,
            max_attempts=max_attempts,
            root=lane_root,
            # Eleven fleet tools and Tessera's ``merge_suite`` read one action's
            # ending out of this directory.  The lane files it there so the
            # cutover is a change to one dispatcher and not to every reader.
            queue_root=SH / "pb-queue" if queue_root is None else queue_root,
            wait_s=wait_s,
            detach=detach,
            on_submit=lambda job: print(
                f"pbrun: submitted {key[:12]} as slurm job {job.job_id} "
                f"(attempt {job.attempt}) tags={tags} demand={demand}",
                file=sys.stderr, flush=True),
            **lane_commands,
        )
    except slurm_lane.SlurmLaneError as exc:
        raise SystemExit(
            f"pbrun: slurm refused this action.\n"
            f"  required tags: {tags or '(any box)'}\n"
            f"  demand:        {demand}\n"
            f"  {exc}\n"
            f"Fix the --tag, or read `sinfo -N -l` for a node that offers it."
        ) from exc

    last = result.last
    if last is None:                       # unreachable: run always submits
        print("pbrun: nothing was submitted", file=sys.stderr)
        return 1
    job, outcome = last
    if detach:
        print(detach_line(
            key,
            transport="slurm",
            status="submitted",
            queue_root=SH / "pb-queue" if queue_root is None else queue_root,
            published_unix=result.published_unix,
            job_id=job.job_id,
            submission=job.record_path,
        ), flush=True)
        return 0
    total = len(result.attempts)
    for index, (attempted, reported) in enumerate(result.attempts, start=1):
        print(f"pbrun: attempt {index}/{total} slurm job {attempted.job_id} "
              f"{reported.state}", file=sys.stderr)
        _echo(attempted.stdout_path, sys.stdout)
        _echo(attempted.stderr_path, sys.stderr)

    if result.receipt is not None:
        print(f"pbrun: executed via slurm job {job.job_id} ({outcome.state})",
              file=sys.stderr)
        return 0
    if outcome.state == "CANCELLED":
        print(f"pbrun: withdrawn -- slurm job {job.job_id} was cancelled",
              file=sys.stderr)
        return WITHDRAWN_EXIT
    if outcome.state == slurm_lane.WAIT_TIMEOUT_STATE:
        print(f"pbrun: gave up waiting for {key[:12]}; slurm job "
              f"{job.job_id} is still queued or running "
              f"(pbrun --transport slurm --withdraw {key[:12]} stops it)",
              file=sys.stderr)
        return GAVE_UP_EXIT
    if outcome.state == slurm_lane.UNKNOWN_STATE:
        # The controller answered that it knows no such job and there is no
        # receipt.  That is not a failure and it is not filed as one: the job
        # may have been purged past MinJobAge, or the controller's memory of
        # it went with a restart while it runs on.  Same exit as giving up,
        # because the truth is the same -- no verdict yet.
        print(f"pbrun: no scheduler command can describe slurm job "
              f"{job.job_id} for {key[:12]} and it has published no receipt; "
              f"it may still be running as slurm job {job.job_id}, or have "
              f"been purged past MinJobAge; look under {job.directory}",
              file=sys.stderr)
        return GAVE_UP_EXIT
    # Say the thing that is actually wrong.  A job that exits zero without
    # publishing a receipt has not done the work, and reporting its status
    # would report success for an action nothing can look up.
    #
    # `succeeded`, not `exit_code == 0`: SLURM reports a job it killed at the
    # time limit as `ExitCode=0:15` -- exit code zero, signal fifteen -- so
    # reading the code alone told an operator whose job was killed by the
    # scheduler that it "exited 0 but published no receipt", which sends them
    # to a job log that says nothing while the state that explains it,
    # TIMEOUT, was already in hand.  Measured in fleet/slurm/smoke row 5 on
    # 2026-09-04: state=TIMEOUT, returncode=0, signal=15.
    if outcome.succeeded:
        print(f"pbrun: slurm job {job.job_id} exited 0 but published no "
              f"receipt for {key[:12]}; see {job.stdout_path} and "
              f"{job.stderr_path}", file=sys.stderr)
        return 1
    print(f"pbrun: failed ({outcome.state}) after {total} attempt(s); "
          f"logs {job.stdout_path} and {job.stderr_path}", file=sys.stderr)
    if isinstance(outcome.exit_code, int) and outcome.exit_code:
        return outcome.exit_code
    return 1


def _echo(path, stream) -> None:
    """Print a job log if it is there, and say nothing when it is not.

    An absent log is normal -- a job cancelled before it started never opened
    one -- and turning that into an error would replace the reason the action
    failed with a complaint about the file that would have explained it.
    """

    try:
        stream.write(Path(path).read_text(encoding="utf-8", errors="replace"))
    except OSError:
        return


def withdraw_routed(
    prefixes, *, transport: str, reason: str = "", by: str = "",
    lane_root=None, queue_root=None, scancel: str = "scancel", queue=None,
) -> int:
    """Send each prefix to the transport that recorded it.

    An operator's shell need not name the transport.  A SLURM job has a
    submission record under the lane root and a pull-queue item has none, so
    the record decides where a prefix goes; ``--transport slurm`` still sends
    every prefix to the lane.  Before this, ``--withdraw`` on a box whose
    environment did not name the transport went to the pull queue, found
    nothing there, and left the SLURM job running with exit status 2.
    """

    lane_prefixes, pool_prefixes = [], []
    for prefix in prefixes:
        if transport == "slurm" or slurm_lane.resolve_recorded(
            str(prefix), root=lane_root
        ):
            lane_prefixes.append(prefix)
        else:
            pool_prefixes.append(prefix)
    rc = 0
    if lane_prefixes:
        rc = max(rc, withdraw_slurm_main(
            lane_prefixes, reason=reason, by=by, lane_root=lane_root,
            queue_root=queue_root, scancel=scancel,
        ))
    if pool_prefixes:
        if queue is None:
            root = SH / "pb-queue" if queue_root is None else Path(queue_root)
            queue = pool.PoolQueue(root)
        rc = max(rc, withdraw_main(queue, pool_prefixes, reason=reason, by=by))
    return rc


def withdraw_slurm_main(
    prefixes, *, reason: str = "", by: str = "", lane_root=None,
    queue_root=None, scancel: str = "scancel",
) -> int:
    """Cancel each named action's recorded job, refusing an ambiguous prefix.

    Same shape as the pool's withdrawal and for the same reasons: a prefix is
    what an operator has, one bad name must not stop the other three, and a
    prefix matching two actions is refused rather than guessed -- the wrong
    guess here kills somebody else's work.

    The marker and the terminal record are written *before* ``scancel``, as the
    pool writes its marker before it signals anything.  Two things depend on
    that order.  ``pool_reset`` skips a re-submission only on a marker or a
    ``withdrawn_unix``, so a cancellation with neither is re-submitted by the
    next bulk reset -- a decision undone by a tool that could not see it was a
    decision.  And the submitting ``pbrun``, still blocked in ``wait``, will
    file its own account of the same generation the moment the job reports
    ``CANCELLED``; whichever arrives first is kept, and this one knows who
    asked and why, which the other cannot.
    """

    rc = 0
    for prefix in prefixes:
        found = slurm_lane.resolve_recorded(str(prefix), root=lane_root)
        if not found:
            print(f"pbrun: no slurm submission matches {prefix!r}",
                  file=sys.stderr)
            rc = 2
            continue
        if len(found) > 1:
            keys = ", ".join(sorted(str(r["action_key"])[:12] for r in found))
            print(f"pbrun: {prefix!r} matches {len(found)} submissions "
                  f"({keys}); name more characters", file=sys.stderr)
            rc = 2
            continue
        record = found[0]
        key = str(record["action_key"])
        job_id = str(record["job_id"])
        queue = SH / "pb-queue" if queue_root is None else Path(queue_root)
        marker = _file_slurm_withdrawal(
            queue, record, reason=reason, by=by, scancel_command=scancel)
        if marker is None:
            print(f"pbrun: {key[:12]} already has an outcome filed; "
                  f"nothing to withdraw", file=sys.stderr)
            continue
        if slurm_lane.cancel(job_id, scancel=scancel):
            why = f" -- {reason}" if reason else ""
            print(f"pbrun: cancelled slurm job {job_id} for {key[:12]}"
                  f" by {by or 'an operator'}{why}", file=sys.stderr)
        else:
            print(f"pbrun: scancel refused slurm job {job_id} for {key[:12]}; "
                  f"it may already have finished", file=sys.stderr)
            rc = 2
    return rc


def _file_slurm_withdrawal(
    queue_root: Path, submission, *, reason: str, by: str, scancel_command: str,
):
    """File the marker and the terminal record for one cancellation.

    Returns ``None`` when this generation already has an outcome filed -- the
    action finished a moment before the operator asked, which is them getting
    what they wanted rather than them mistyping, and is what
    ``PoolQueue.withdraw`` reports as ``already_finished``.
    """

    key = str(submission["action_key"])
    published_unix = submission.get("published_unix")
    if not isinstance(published_unix, (int, float)):
        # A submission record from before the generation stamp. Withdraw it,
        # but do not claim to know which request it belonged to.
        published_unix = float(submission.get("submitted_unix") or 0.0)
    for state in (pool.DONE, pool.FAILED):
        filed = queue_root / state / f"{key}.json"
        if filed.exists() and slurm_lane._same_generation(filed, published_unix):
            return None

    _, marker = slurm_lane.publish_withdrawal(
        queue_root=queue_root, action_key=key, reason=reason, by=by,
        submission=submission,
    )
    job_id = str(submission["job_id"])
    directory = Path(str(submission.get("directory") or "."))
    slurm_lane.publish_outcome(
        queue_root=queue_root,
        action_key=key,
        published_unix=published_unix,
        published_by=str(submission.get("published_by") or ""),
        status="withdrawn",
        attempts=int(submission.get("attempt") or 1),
        max_attempts=int(submission.get("max_attempts") or 1),
        retry_safe=submission.get("retry_safe"),
        addressing={},
        resources=submission.get("resources") or {},
        tags=submission.get("constraint") or [],
        detail={
            "slurm": {
                "job_id": job_id,
                "state": "CANCELLED",
                "partition": None,
                # Derived, not spelled: the record is named by generation and
                # attempt together, because one action key is submitted again
                # every time somebody asks for the same work again.
                "submission_record_path": str(
                    slurm_lane.submission_record_path(
                        directory,
                        published_unix=published_unix,
                        attempt=int(submission.get("attempt") or 1),
                    )),
                "stdout_path": str(submission.get("stdout") or ""),
                "stderr_path": str(submission.get("stderr") or ""),
            },
            "cancelled_with": scancel_command,
            # The last sample the submitter's wait recorded, so the record of
            # a withdrawal says what the job was (not) doing when the operator
            # decided.  None when no wait ever sampled it.
            "liveness": slurm_lane.read_liveness(
                key, root=directory.parent if directory.name == key else None),
        },
        # No ``SubmittedJob`` here -- this runs from the operator's box, off the
        # recorded submission -- so the job id the readers use as ``claimed_by``
        # is passed rather than derived.
        claimed_by=job_id,
        withdrawn_by=marker.get("withdrawn_by"),
        withdrawn_unix=marker.get("withdrawn_unix"),
        reason=str(marker.get("reason") or ""),
    )
    return marker


def withdraw_main(q, prefixes, *, reason: str = "", by: str = "") -> int:
    """Withdraw each named action and say what happened to it.

    Takes the queue rather than building one, for the same reason
    ``await_outcome`` does: the part worth testing is the reporting and the
    prefix resolution, and neither should need a live fleet to exercise.

    Keys are accepted as prefixes because a prefix is what an operator has --
    ``pbrun`` prints ``queued 8fc86da0e13f`` and the worker loop logs the same
    twelve characters.  One bad name does not stop the rest: withdrawing four
    suites at once is the case this exists for, and three of four is a better
    outcome than none of four.
    """

    rc = 0
    published = published_commit()
    for prefix in prefixes:
        try:
            key = q.find_key(str(prefix))
            result = q.withdraw(key, reason=reason, by=by)
        except Exception as exc:                                  # noqa: BLE001
            print(f"pbrun: {exc}", file=sys.stderr)
            rc = 2
            continue
        status = str(result.get("status"))
        if status == "already_finished":
            print(f"pbrun: {key[:12]} had already finished "
                  f"({result.get('state')}); nothing to withdraw",
                  file=sys.stderr)
            continue
        where = result.get("state") or "nowhere"
        note = [f"released {result.get('released', 0)} token(s)"]
        container_cleanup = result.get("container_cleanup") or {}
        if not container_cleanup.get("complete", True):
            note.append("container cleanup unverified; claim and tokens retained")
            error = str(container_cleanup.get("error") or "").strip()
            if error:
                note.append(error)
            rc = 2
        signalled = result.get("signalled") or {}
        if signalled.get("signals"):
            note.append("signalled " + ", ".join(signalled["signals"]))
        elif where == "claimed":
            # Say so rather than imply the work stopped.  Cross-box that is the
            # normal case and the remote worker stops within a heartbeat, but a
            # caller who reads "withdrawn" and assumes "already dead" would be
            # wrong for those seconds.
            note.append(f"no local child to signal on "
                        f"{result.get('host') or 'an unknown host'}; its worker "
                        f"stops within a heartbeat")
        if status == "already_withdrawn":
            note.insert(0, "already withdrawn")
        print(f"pbrun: withdrew {key[:12]} from {where}; " + "; ".join(note),
              file=sys.stderr)
        # Say when the withdrawal is one the holder's worker cannot see.  Every
        # guard this verb relies on lives in bytes the loop imported at start,
        # so a box that has not rolled runs the action to completion -- with
        # the tokens this just handed back, which is the load-average-371
        # shape the issue is about.  The record is filed and the retry is
        # closed either way; what is not bounded is the current run.
        if where == "claimed":
            runtime = result.get("holder_runtime")
            host = result.get("host") or "the holder"
            if runtime is None:
                print(f"pbrun: WARNING no live offer from {host}; cannot tell "
                      f"whether its worker can see this withdrawal",
                      file=sys.stderr)
            elif not runtime:
                print(f"pbrun: WARNING {host} announces no runtime commit; "
                      f"cannot tell whether its worker can see this withdrawal",
                      file=sys.stderr)
            elif published and runtime != published:
                print(f"pbrun: WARNING {host} is running runtime "
                      f"{runtime[:12]}, not the published {published[:12]}: "
                      f"its loop cannot see withdrawn/, so the action may run "
                      f"to completion with the tokens just released.  Roll the "
                      f"fleet, or watch the box.", file=sys.stderr)
    return rc


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Submit one command to the PrismaBuild pool and wait for it."
    )
    ap.add_argument("--demand", default="",
                    help="resource demand, e.g. gpu=1,mem_gb=16")
    ap.add_argument("--gpu", action="store_true",
                    help="shorthand for gpu=1,mem_gb=16")
    ap.add_argument("--cpus", type=int, default=1,
                    help="cores this action will actually use; a parallel test "
                         "run wants its -n, not 1")
    ap.add_argument("--exclusive", action="store_true",
                    help="demand the whole GPU capacity of one box")
    ap.add_argument("--gpu-capacity", type=int, default=0,
                    help="slots to demand for --exclusive; 0 reads the largest "
                         "a matching box actually offers")
    ap.add_argument("--tag", action="append", default=[],
                    help="require a box offering this tag (e.g. a hardware class)")
    ap.add_argument("--measurement", action="store_true",
                    help="seal task_class=measurement: the result is numerics "
                         "that do not transfer across architectures, so it "
                         "requires --host-class")
    ap.add_argument("--host-class", default=None, metavar="CLASS",
                    help="key the action on a host class, a node Feature name "
                         "(e.g. gb10): seals execution_scope host_class_keyed, "
                         "adds CLASS to the placement, and the SLURM lane "
                         "sends it as --constraint; the worker attests it "
                         "through the controller before running")
    ap.add_argument("--anywhere", action="store_true",
                    help="assert that command/tool/data dependencies outside "
                         "the snapshot are identical on every eligible worker")
    ap.add_argument("--here", action="store_true",
                    help="pin the materialized checkout to this box")
    ap.add_argument(
        "--checkout-snapshot-max-bytes",
        type=int,
        default=CHECKOUT_SNAPSHOT_MAX_BYTES,
        help="lower the hard fleet ceiling for both the logical materialized "
             "Git tree and compressed bundle (cannot raise it)",
    )
    ap.add_argument(
        "--snapshot-ref",
        action="append",
        default=[],
        metavar="NAME",
        help="also advertise this source branch in the snapshot bundle, so "
             "the action can spell it (e.g. --snapshot-ref master for a "
             "master...HEAD gate); repeatable",
    )
    ap.add_argument("--cwd", default=os.getcwd())
    ap.add_argument("--deterministic", action="store_true",
                    help="declare byte-identical output; enables CAS reuse")
    ap.add_argument(
        "--retry-safe",
        action="store_true",
        help=("declare the whole command idempotent across failed attempts, "
              "including every external side effect"),
    )
    ap.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
        help=("bounded attempts for a --retry-safe action; arbitrary commands "
              "default to one"),
    )
    # Honoured on the SLURM path, where it becomes --time and the scheduler
    # enforces it (TERM, then KILL after KillWait).  On the pool path it is
    # still only parsed: the worker loop's own --timeout-s bounds an action
    # there, and a submitter-declared bound has nowhere to be recorded.  See
    # issue #32; the SLURM lane is the half of it that this closes.
    ap.add_argument("--timeout-s", type=float, default=None,
                    help="an explicit deadline for the action, enforced by "
                         "SLURM under --transport slurm; unset means the "
                         "action runs while it is running, because elapsed "
                         "time is not evidence that a worker is dead")
    ap.add_argument("--wait-s", type=float, default=86400.0,
                    help="give up waiting for a worker to pick this up")
    ap.add_argument(
        "--detach", action="store_true",
        help="seal and submit exactly as usual, print one JSON line naming the "
             "action key, the transport, the job id or queue record and the "
             "terminal-record paths, and exit 0 without waiting; an action "
             "already in the CAS prints status=cache_hit and submits nothing, "
             "and one already running prints status=attached and joins that "
             "run rather than starting a second copy of it. "
             "Wait for it later with pbwait.py. Incompatible with "
             "--max-attempts greater than 1: a retry needs somebody alive to "
             "see the attempt fail",
    )
    ap.add_argument("--priority", type=int, default=0)
    ap.add_argument("--env", action="append", default=[],
                    help="K=V added to the action's environment (repeatable)")
    ap.add_argument("--no-default-env", action="store_true",
                    help="declare only --env, without the fleet defaults")
    ap.add_argument("--withdraw", action="append", default=[], metavar="KEY",
                    help="cancel this queued or running action (a key prefix is "
                         "enough) instead of submitting; repeatable")
    ap.add_argument("--reason", default="",
                    help="why, recorded on the withdrawal record")
    ap.add_argument(
        "--transport", choices=TRANSPORTS,
        default=default_transport(),
        help="which dispatcher carries this submission (env "
             "PRISMABUILD_TRANSPORT, else the published runtime generation's "
             "default_transport); the pull queue stays the default until the "
             "fleet has cut over to SLURM")
    ap.add_argument("command", nargs=argparse.REMAINDER)
    args = ap.parse_args()

    if args.withdraw:
        # Withdrawing is not a submission and must not need one: the operator
        # cancelling four suites has no command to give and no checkout to
        # stamp, so this returns before any of the submit machinery runs.
        if [c for c in args.command if c != "--"]:
            raise SystemExit("pbrun: --withdraw takes no command")
        try:
            who = getpass.getuser()
        except Exception:                                        # noqa: BLE001
            who = "unknown"      # no passwd entry is not a reason to refuse
        return withdraw_routed(
            args.withdraw, transport=args.transport, reason=args.reason,
            by=f"{who}@{socket.gethostname()}",
        )

    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("nothing to run: pbrun [options] -- <command>")
    if args.max_attempts < 1:
        raise SystemExit("pbrun: --max-attempts must be at least 1")
    if args.max_attempts > 1 and not args.retry_safe:
        raise SystemExit(
            "pbrun: --max-attempts greater than 1 requires --retry-safe; "
            "--deterministic covers result bytes, not external side effects"
        )
    if args.detach and args.max_attempts > 1:
        # A retry is a second submission made after somebody watched the first
        # one fail.  Detaching means nobody is watching, so the choice is
        # between silently running one attempt for a caller who asked for
        # three, and saying so here.
        raise SystemExit(
            "pbrun: --detach submits one attempt and returns, so it cannot "
            "honour --max-attempts greater than 1; submit it attached, or "
            "detach with a single attempt"
        )
    retry_policy = {
        "max_attempts": args.max_attempts,
        "retry_safe": args.retry_safe,
    }
    determinism = "deterministic" if args.deterministic else "stochastic"

    cwd = Path(args.cwd).resolve()
    if not cwd.is_dir():
        # Say which of the two things went wrong.  A closure is computed from
        # the checkout and stamped inside it, so pbrun submits only for a
        # checkout on the box it is running on -- the QUEUE is shared, the
        # filesystem is not.  The old message named a missing directory, which
        # is right for a typo and actively misleading for the other case: a
        # cross-box submission ("run the suite on sparklina's checkout, from
        # sparky") reads as "the path is wrong" when the path is correct and
        # simply belongs to another box.  Submitting from that box is not a
        # workaround; it is where the closure can honestly be taken.
        raise SystemExit(
            f"--cwd is not a directory on {socket.gethostname()}: {cwd}\n"
            f"pbrun stamps the code closure inside the checkout, so it can "
            f"only submit for a checkout on the box it runs on. If this path "
            f"exists on another box, submit from there -- the queue is "
            f"shared, the filesystem is not."
        )

    repository_root = git_repository_root(cwd)
    if repository_root is None:
        raise SystemExit(
            "pbrun: --cwd must be inside a Git checkout so its exact bytes "
            "can be sealed and materialized through the CAS; mutable "
            "path-addressed submission is not supported"
        )
    require_checkout_snapshot_limit(args.checkout_snapshot_max_bytes)
    # Before the stamp is written, before Git hashes a byte, and before
    # anything reaches the CAS or the queue: an unresolvable ref name is a
    # typo, and the only cheap moment to say so is now.
    require_complete_history(repository_root)
    resolve_snapshot_refs(repository_root, list(args.snapshot_ref))
    early_paths = snapshot_path_roster(repository_root)
    require_working_tree_size(
        repository_root,
        early_paths,
        max_bytes=args.checkout_snapshot_max_bytes,
    )
    require_untransformed_checkout(repository_root, early_paths)
    require_checkout_owned_scripts(
        command, cwd, repository_root=repository_root
    )
    portable_checkout = True
    logical_cwd = cwd.relative_to(repository_root).as_posix() or "."

    # `run_local_action` builds the child's environment from *these* and
    # nothing else, so an empty dict is not "inherit the caller". Keep the
    # caller's additions separate as well: placement resolves argv[0] against
    # the complete declared PATH, while its conservative data-path screen must
    # not mistake fleet-owned defaults for caller-owned external inputs.
    variables = {} if args.no_default_env else {
        "HOME": "/home/rob",
        "TMPDIR": "/home/rob/tmp",
        "TRITON_CACHE_DIR": "/home/rob/.triton-cache",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "OMP_NUM_THREADS": "4",
        "MKL_NUM_THREADS": "4",
        "OPENBLAS_NUM_THREADS": "4",
    }
    caller_variables: dict[str, str] = {}
    for entry in args.env:
        if "=" not in entry:
            raise SystemExit(f"--env expects K=V, got {entry!r}")
        key, value = entry.split("=", 1)
        if key in {CONTAINER_OWNER_ENV, CONTAINER_MARKER_ENV}:
            raise SystemExit(
                f"pbrun: {key} is derived by the container lifecycle; "
                "callers may not set it")
        variables[key] = value
        caller_variables[key] = value

    demand = _parse_demand(args.demand)
    if args.gpu:
        demand.setdefault("gpu", 1)
        demand.setdefault("mem_gb", 16)
    demand.setdefault("mem_gb", 4)
    # Cores are a demand like any other, and the default of one is what makes
    # this safe to add to a live fleet: every action already in flight keeps
    # the admission it had.  What it buys is a way for an action that will
    # take twenty-four cores to SAY twenty-four, which nothing could express
    # before -- and on 2026-09-04 four `pytest -n 24` runs each declaring
    # `mem_gb=4` were admitted to one 80-core box together, load average 371.
    if args.cpus < 1:
        raise SystemExit("--cpus must be at least 1")
    demand.setdefault("cpu", args.cpus)

    if args.anywhere and args.here:
        raise SystemExit("--anywhere and --here contradict each other")
    require_host_class_scope(
        measurement=args.measurement, host_class=args.host_class,
        transport=args.transport,
    )
    tags = pool.normalize_placement_tags(
        placement_tags(
            cwd,
            explicit=list(args.tag),
            here=args.here,
            hostname=socket.gethostname(),
            portable_checkout=portable_checkout,
            command=command,
            repository_root=repository_root,
            environment=variables,
            caller_environment=caller_variables,
            anywhere=args.anywhere,
        )
    )
    if args.host_class is not None:
        # The class rides the placement axis, the same way --tag does, so the
        # action key moves with it and the SLURM lane seals it as
        # --constraint.  A union rather than a replacement: a hostname pin a
        # box-local executable earned stays, and the class narrows it further.
        tags = pool.normalize_placement_tags([*tags, args.host_class])
    placement = {"required_tags": tags}
    if args.exclusive and args.transport == "slurm":
        # SLURM already has a word for the whole device.  ``gpu:1`` and
        # ``shard:N`` are mutually exclusive requests against one GPU, so
        # exclusivity is a different GRES name rather than a bigger count, and
        # the count the pool had to read off worker offers is not needed.
        demand["gpu"] = args.gpu_capacity or 1
        demand["mem_gb"] = max(int(demand.get("mem_gb", 0)), 16)
    elif args.exclusive:
        # "All of one box" is a fact about the boxes, and guessing it does not
        # fail loudly -- it fails as an action nobody can ever claim.  The
        # default was 4 while sparky declares 2 and sparklina 1, so every
        # --exclusive submission asked for twice the slots that exist and sat
        # in ``ready`` forever.  Read it from what the fleet announces, which
        # needs the placement tags, so it happens after them.
        demand["gpu"] = args.gpu_capacity or exclusive_gpu_demand(
            pool.PoolQueue(SH / "pb-queue"), tags)
        demand["mem_gb"] = max(int(demand.get("mem_gb", 0)), 16)

    if portable_checkout:
        require_relocatable_checkout(
            command, variables, cwd, repository_root=repository_root
        )

    # A CPU slot must not be able to run GPU work.  The pool's whole claim is
    # that the ledger knows what is on each accelerator, and that claim was
    # false in one direction: an action submitted WITHOUT ``--gpu`` inherited a
    # visible device and ran CUDA anyway.  A pytest suite queued as a 4 GB CPU
    # action executed its ``skipif(not torch.cuda.is_available())`` tests on a
    # box whose GPU slots were held by somebody else -- work the ledger could
    # not see, contending with work it had promised exclusivity to.
    #
    # The rule is enforced the way ``require_pool.py`` enforces its own escape
    # hatch, by the kernel rather than by belief: with no device visible the
    # child cannot do GPU work, so a mis-declared action fails instead of
    # stealing.  Declaring a device on a slot that did not reserve one is the
    # mis-declaration itself, so it is refused rather than honoured -- the fix
    # is ``--gpu``, and the message says so.  This applies under
    # ``--no-default-env`` too: an empty environment means every device is
    # visible, which is the case this exists for.
    declared = variables.get("CUDA_VISIBLE_DEVICES")
    if not demand.get("gpu"):
        if declared not in (None, ""):
            raise SystemExit(
                f"pbrun: this action reserves no GPU but sets "
                f"CUDA_VISIBLE_DEVICES={declared!r}.\n"
                "A CPU slot that touches the GPU is work the ledger cannot "
                "see, contending with work it promised exclusivity to.\n"
                "Add --gpu (and --gpu-capacity N if you need more than one "
                "slot), or drop the variable.")
        variables["CUDA_VISIBLE_DEVICES"] = ""

    # Docker's payload is reparented to containerd-shim and therefore survives
    # a kill of every process group below the action launcher.  Put the fleet's
    # Docker shim first even under --no-default-env; it records a durable marker
    # and adds the derived ownership label which withdrawal/finish query before
    # returning capacity.  This is control-plane state, not an optional action
    # convenience, so a caller cannot override either identity variable.
    #
    # Normalize every other environment value first.  The owner then hashes
    # the exact action-defining state available before its own two recursive
    # variables are injected, including the deployed wrapper path.
    prior_path = variables.get("PATH") or "/usr/local/bin:/usr/bin:/bin"
    variables["PATH"] = f"{CONTAINER_WRAPPER_DIR}:{prior_path}"
    identity = _git_identity(cwd)
    marker_root = SH / "pb-queue" / pool.CONTAINER_OWNERS
    owner = container_owner(
        command,
        cwd,
        demand,
        variables,
        determinism=determinism,
        retry_policy=retry_policy,
        marker_root=marker_root,
        identity=identity,
        logical_cwd=logical_cwd,
        placement=placement,
    )
    marker = marker_root / f"{owner}.used"
    variables[CONTAINER_OWNER_ENV] = owner
    variables[CONTAINER_MARKER_ENV] = str(marker)

    # Migrate the former broad prefix globs before identity asks Git for its
    # untracked roster; otherwise a legitimate prefix-bearing payload remains
    # hidden for this submission even though the new grammar is exact.
    keep_droppings_out_of_git(cwd)
    log_name, stamp_name = result_and_stamp_names(
        command,
        cwd,
        demand,
        variables,
        identity=identity,
        logical_cwd=logical_cwd,
        placement=placement,
    )
    # The closure member must be under checkout_root: that is where the
    # worker re-verifies it, on whichever box claimed the action.
    # Written through a private temp file and renamed, because rename is the
    # one primitive this fleet trusts on NFS and a plain write is not atomic.
    # Concurrent submits from one checkout -- forty test shards, say -- all
    # write this same file, and a reader that catches a partial one gets
    # "cannot open code closure file as a regular file" or "live code closure
    # differs from the action-pinned closure".  The content is identical across
    # those submits *because the commit is in the name*, so atomicity is the
    # whole fix and ordering does not matter.  It was not identical before
    # that: the name held the command and the content held the commit.
    payload = json.dumps(
        {"cwd": logical_cwd, **identity}, indent=1, sort_keys=True
    )
    scratch = cwd / f"{stamp_name}.{os.getpid()}.{uuid.uuid4().hex[:8]}"
    try:
        # fsync both the file and its directory before publishing.  The submit
        # side is usually an NFS client and the worker may be the box holding
        # the export, so a write that has only reached the client's page cache
        # is invisible to the reader that is about to verify it -- the action
        # gets published, a worker claims it within milliseconds, and it fails
        # with "cannot open code closure file as a regular file" for a file
        # that plainly exists a second later.  Durability before publication is
        # the ordering the queue already assumes everywhere else.
        with scratch.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(scratch, cwd / stamp_name)
        directory = os.open(cwd, os.O_RDONLY)
        try:
            os.fsync(directory)
        except OSError:
            pass                     # some filesystems refuse directory fsync
        finally:
            os.close(directory)
    finally:
        if scratch.exists():
            scratch.unlink()
    cas = pb.PrismaBuildCAS(SH / "cas")
    checkout_snapshot = build_git_checkout_snapshot(
        cwd,
        stamp_name=stamp_name,
        cas=cas,
        max_bytes=args.checkout_snapshot_max_bytes,
        expected_identity=identity,
        snapshot_refs=list(args.snapshot_ref),
    )
    execution_scope, toolchain = host_class_scope(args.host_class)
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "fleet/pbrun",
            "definition_version": "v1",
            "task_class": "measurement" if args.measurement else "generation",
            # A pytest or a timing run is not byte-reproducible and must not
            # claim to be: the CAS only enforces canonical equality on
            # "deterministic", so mislabelling one would be a false receipt.
            "determinism": determinism,
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": [SEALED_ARGV0, "-lc",
                     f"export PATH={shlex.quote(str(CONTAINER_WRAPPER_DIR))}:$PATH; "
                     f"{shlex.join(command)} 2>&1 | tee {shlex.quote(log_name)}; "
                     f"exit ${{PIPESTATUS[0]}}"],
            "working_directory": ".",
            "result_path": log_name,
        },
        "inputs": [checkout_snapshot["input"]],
        "code_closure": pb.build_code_closure(cwd, [stamp_name]),
        "params": {
            "command": command,
            "cwd": logical_cwd,
            "demand": demand,
            "placement": placement,
            "checkout_snapshot": checkout_snapshot,
            "retry_policy": retry_policy,
        },
        "environment": {"variables": variables, "toolchain": toolchain},
        "execution_scope": execution_scope,
    }
    try:
        action = pb.seal_action(body)
    except pb.ActionContractError as exc:
        # A refused contract is the caller's to fix; nothing has been queued
        # or ingested, so say what was refused and stop.  A traceback here
        # names core.py internals for what is a submission error (issue #21).
        raise SystemExit(f"pbrun: refusing to seal the action: {exc}") from None
    key = str(action["action_key"])

    request_path = cas.publish_action_request(action)

    if args.detach and cas.lookup(action) is not None:
        # Nothing to submit and nothing to wait for.  The attached path lets
        # the worker discover this and file an ending, which is right when
        # somebody is holding the terminal open; detached, that ending would be
        # a job scheduled, a checkout materialized and a node occupied to learn
        # what this process already knows.  A campaign re-run is the case: every
        # row a hit, no new job ids.
        print(f"pbrun: {key[:12]} is already in the CAS; nothing submitted",
              file=sys.stderr, flush=True)
        print(detach_line(
            key,
            transport=args.transport,
            status="cache_hit",
            queue_root=SH / "pb-queue",
        ), flush=True)
        return 0

    if args.detach:
        # Not in the CAS, but perhaps already running: a campaign whose waiter
        # died is re-run to find out where it got to, and every row still on a
        # node must be attached to rather than submitted again.
        live = live_submission(pool.PoolQueue(SH / "pb-queue"), key)
        if live is not None:
            transport, generation, submission = live
            if transport == "slurm":
                directory = Path(str(submission.get("directory") or "."))
                record = slurm_lane.submission_record_path(
                    directory, published_unix=generation,
                    attempt=int(submission.get("attempt") or 1),
                )
                job_id = str(submission.get("job_id") or "")
            else:
                ready = (SH / "pb-queue" / pool.READY / f"{key}.json")
                record = ready if ready.exists() else (
                    SH / "pb-queue" / pool.CLAIMED / f"{key}.json")
                job_id = ""
            print(f"pbrun: {key[:12]} is already running "
                  f"({transport}{' job ' + job_id if job_id else ''}); "
                  f"attaching to it rather than submitting a second copy",
                  file=sys.stderr, flush=True)
            print(detach_line(
                key,
                transport=transport,
                status="attached",
                queue_root=SH / "pb-queue",
                published_unix=generation,
                job_id=job_id or None,
                submission=record,
            ), flush=True)
            return 0

    if args.transport == "slurm":
        # Everything below this point reads the pull queue -- worker offers,
        # the placement census, the ready directory -- and none of it describes
        # a SLURM fleet.  Worse, it would answer *wrongly*: a retained offer
        # from a loop that has been stopped for the cutover would refuse a
        # submission the scheduler can place perfectly well.  The capability
        # check SLURM keeps is its own: an unknown Feature or an impossible
        # GRES makes ``sbatch`` refuse at submit time, which is the same moment
        # and the same intent as ``capability_verdict`` below.
        return slurm_outcome(
            action,
            cas=cas,
            request_path=request_path,
            tags=tags,
            demand=demand,
            exclusive=args.exclusive,
            timeout_s=args.timeout_s,
            wait_s=args.wait_s,
            retry_safe=args.retry_safe,
            max_attempts=args.max_attempts,
            anywhere=args.anywhere,
            detach=args.detach,
        )

    q = pool.PoolQueue(SH / "pb-queue")

    # Refuse work the RECORDED fleet cannot run, at the one moment the caller
    # is still watching.  A required tag no box has offered is not a slow
    # submission: the item matches no worker's placement filter, so it sits in
    # `ready` -- counted, reported as pending -- while every idle worker polls
    # past it until `--wait-s` expires a day later.
    #
    # Capability and liveness are deliberately different reads of the SAME
    # matcher.  Worker offers expire for claiming and fleet-width diagnostics,
    # but the latest record from each host remains evidence of what that box
    # can fit.  dl380g10 and gx10-6b77 have both spent longer than the 120 s TTL
    # inside work; while they were between announcements a fresh nonmatching
    # offer made ``placeable`` answer False and pbrun rejected a caller willing
    # to wait two hours.  An unbounded age reads retained capability and lets
    # ``--wait-s`` own an offline/busy box.  ``None`` still means no worker has
    # ever announced and stays a warning, so a fleet whose loops predate the
    # registry can submit unchecked.
    intent = {"tags": tags, "needs_gpu": bool(demand.get("gpu")), "resources": demand}
    # Say how wide this action is before saying it was queued.  A pin is a
    # consequence of the checkout path, and nothing used to report it, so a
    # submitter narrowed the fleet to one box without being told.
    notice = pin_notice(
        q,
        intent,
        cwd=cwd,
        hostname=socket.gethostname(),
        here=args.here,
        portable_checkout=portable_checkout,
    )
    if notice:
        print(notice, file=sys.stderr, flush=True)
    live_verdict = q.placeable(intent)
    capability_verdict = q.placeable(
        intent, max_age_s=RECORDED_OFFER_MAX_AGE_S)
    if capability_verdict is False:
        raise SystemExit(
            f"pbrun: no recorded worker can run this action.\n"
            f"  required tags: {tags or '(any box)'}\n"
            f"  demand:        {demand}\n"
            f"  offered on record: "
            f"{q.offered_tags(max_age_s=RECORDED_OFFER_MAX_AGE_S)}\n"
            f"Fix the --tag, or start a worker on a box that offers it."
        )
    if capability_verdict is None:
        print("pbrun: no worker offers on record; submitting unchecked",
              file=sys.stderr, flush=True)
    elif live_verdict is not True:
        print(
            "pbrun: no matching worker is live now; a recorded capable worker "
            "is between announcements or offline.  Submitting so --wait-s "
            f"{args.wait_s:g} owns how long to wait.",
            file=sys.stderr, flush=True,
        )

    # Read the decision this submission is about to supersede, so the caller is
    # told rather than surprised.  ``publish`` retires the marker -- a key is a
    # content hash, so re-submitting one is how anybody asks for the same work
    # again -- and a submission that silently revived somebody's cancellation
    # would be as bad as the blacklist it replaced.
    superseding = None
    try:
        superseding = json.loads(
            q.item_path("withdrawn", key).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        superseding = None

    publication = {
        "action_key": key,
        "cas_root": str(SH / "cas"),
        "worker_script": str(RUNTIME_ROOT / "tools" / "prismabuild_worker.py"),
        "tags": tags,
        "needs_gpu": bool(demand.get("gpu")),
        "priority": args.priority,
        "resources": demand,
        "max_attempts": args.max_attempts,
        "container_owner": owner,
    }
    publication["checkout_snapshot"] = checkout_snapshot
    # The repo checkout can advance just before the atomic runtime generation
    # rolls.  The previous PoolQueue already accepts the safety-critical bound,
    # so keep that mixed window usable; add the explanatory annotation once the
    # loaded runtime exposes it.  The sealed action params carry the full
    # contract in both cases.
    if "retry_safe" in inspect.signature(q.publish).parameters:
        publication["retry_safe"] = args.retry_safe
    queued_path = q.publish(**publication)
    # Say that the slot has no device, every time.  The mask is correct and it
    # is also a silent narrowing: a suite that used to run its CUDA tests now
    # skips them, and a skip that nobody announced reads as the same green.
    if superseding is not None:
        who = superseding.get("withdrawn_by") or "an operator"
        why = str(superseding.get("reason") or "").strip()
        print(f"pbrun: {key[:12]} had been withdrawn by {who}"
              f"{' -- ' + why if why else ''}; this submission supersedes that "
              f"decision", file=sys.stderr, flush=True)
    masked = "" if demand.get("gpu") else "  [no GPU: CUDA_VISIBLE_DEVICES='']"
    print(f"pbrun: queued {key[:12]} tags={tags} demand={demand}{masked}",
          file=sys.stderr, flush=True)

    if args.detach:
        print(detach_line(
            key,
            transport="pool",
            status="submitted",
            queue_root=q.root,
            published_unix=published_generation(q, key, queued_path),
            submission=queued_path,
        ), flush=True)
        return 0

    return await_outcome(
        q, key, wait_s=args.wait_s,
        generation=published_generation(q, key, queued_path),
    )


if __name__ == "__main__":
    raise SystemExit(main())
