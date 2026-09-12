#!/usr/bin/env python3
"""Run one command through the PrismaBuild pool instead of a local flock.

Why this exists: agent work was scheduled by a box-local ``flock`` semaphore,
which cannot coordinate across boxes (the lock file is local) and reproduces
three bugs ``pool.py`` already solves -- hold-while-gated, no aging, and
partial-hold waste.  This is the submit side that makes the pool the only
path an agent needs.

Two things are deliberate.

*GPU exclusivity is sealed intent.* ``--exclusive`` records
``params.gpu_exclusive=true``. Shared generation work records false, so the
adaptive pool may probe concurrency on one physical GPU without weakening an
exclusive request. Legacy requests without this marker remain exclusive;
historical multi-slot demands retain their identity but reserve one device.

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

The stamp carrying that identity lives inside the materialized checkout, because
the worker verifies the closure against ``checkout_root`` on the box that
runs it. A private snapshot index injects it without writing the source tree.
Legacy stamps are excluded from the identity they record -- otherwise each
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
import re
import shlex
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
import math
import uuid
from collections.abc import Mapping, Sequence
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
from prismabuild import adaptive_gpu, core as pb, pool, slurm_lane  # noqa: E402

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
#: Filesystem/record persistence failure (sysexits.h ``EX_IOERR``). The legacy
#: name remains for callers; diagnostics distinguish an accepted job's failed
#: record write from other filesystem access failures and withdrawal stages.
#: Neither a timeout nor a withdrawal verdict: recovery requires reading the
#: reported path, reason and known job id.
RECORD_WRITE_FAILED_EXIT = 74
#: The exit codes ``pbrun`` decides for itself, and therefore the codes a run's
#: own status must never be allowed to impersonate.  A terminal record carries
#: the far side's launcher status as a plain integer, and both report paths
#: returned it verbatim: a launcher that exited 143 reached the caller as
#: ``WITHDRAWN_EXIT``, which claims an operator made a decision that nothing on
#: disk records.  That is reachable rather than theoretical --
#: ``core._sigterm_unwinds_this_process`` raises ``SystemExit(128 + signum)``,
#: so every SIGTERM that is not a withdrawal leaves 143 in the record -- and 2
#: (a refusal), 74 and 75 are the same hole with different remedies attached.
#: Zero is deliberately absent: it is ``pbrun``'s word for success, no producer
#: files it under a status that is not one, and the SLURM site below already
#: excludes it by truthiness.
RESERVED_EXITS = frozenset(
    {2, RECORD_WRITE_FAILED_EXIT, GAVE_UP_EXIT, WITHDRAWN_EXIT}
)

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
#: name.  That name is only half of what a CAS hit needs: the sealed checkout
#: bundle is the other half, and its bytes were nondeterministic until
#: ``write_deterministic_bundle`` pinned the pack, so a resubmit that landed
#: on this same name still missed the cache on every real repository.
RESULT_PREFIX = getattr(pb, "PBRUN_RESULT_PREFIX", "pbrun_result.")
CONTAINER_OWNER_ENV = pool.CONTAINER_OWNER_ENV
CONTAINER_MARKER_ENV = pool.CONTAINER_MARKER_ENV
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


#: Git reads three exclude sources under ``--exclude-standard``: the
#: repository's own ``.gitignore`` files, ``$GIT_DIR/info/exclude``, and
#: ``core.excludesFile``.  The first describes the repository and the second is
#: where ``keep_droppings_out_of_git`` puts pbrun's own generated basenames, so
#: both belong to the seal.  ``core.excludesFile`` is a personal setting on the
#: box that submits, and the sealed tree must not be a function of it.
#: Measured: one untracked file matched by a submitter's global exclude seals a
#: different tree, and therefore a different action key, for identical bytes.
#: Pinned to an empty file rather than cleared, because an empty value falls
#: back to Git's default of ``$XDG_CONFIG_HOME/git/ignore``, which is the very
#: file this has to stop reading.
PERSONAL_EXCLUDES_PIN: tuple[str, ...] = ("-c", "core.excludesFile=/dev/null")


def snapshot_path_roster(
    root: Path, *, extra_paths: tuple[str, ...] = ()
) -> list[str]:
    """Tracked plus nonignored-untracked paths, each counted once."""

    raw_paths = _snapshot_git(
        root,
        [*PERSONAL_EXCLUDES_PIN, "ls-files", "-co", "--exclude-standard", "-z"],
        strip=False,
    )
    return list(dict.fromkeys(
        [path for path in raw_paths.split("\0") if path] + list(extra_paths)
    ))


def _seed_index_roster(root: Path, environment: dict[str, str]) -> None:
    """Force every source-index path into the alternate index.

    ``git add -A`` honours the ignore rules for a path the alternate index
    does not already carry, and an index seeded from ``HEAD`` does not carry a
    path the submitter staged with ``git add -f``.  That path is in the roster
    the snapshot identity hashes, so omitting it seals a tree the identity
    does not describe.  Force-add the source index roster instead, with the
    working-tree bytes each path has now.

    Paths whose working-tree entry is absent are left out: a staged addition
    that was then removed from the worktree has nothing to force-add, and the
    ``git add -A`` that follows records the removal, which is what a tracked
    deletion already did.
    """

    raw_paths = _snapshot_git(root, ["ls-files", "-z"], strip=False)
    staged = []
    for relative in raw_paths.split("\0"):
        if not relative:
            continue
        try:
            (root / relative).lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise SystemExit(
                f"pbrun: cannot inspect checkout path {relative!r}: {exc}"
            ) from exc
        staged.append(relative)
    if not staged:
        return
    # ``GIT_LITERAL_PATHSPECS`` keeps a pathname that looks like pathspec
    # magic or a glob from being read as one; these are exact paths Git just
    # reported, never patterns.
    literal_environment = dict(environment)
    literal_environment["GIT_LITERAL_PATHSPECS"] = "1"
    _snapshot_git(
        root,
        ["add", "-f", "--pathspec-from-file=-", "--pathspec-file-nul"],
        environment=literal_environment,
        input_text="\0".join(staged) + "\0",
    )


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


SNAPSHOT_LINK_RESOLUTION_LIMIT = 64


def require_contained_snapshot_links(symlinks: dict[str, str]) -> None:
    """Refuse a sealed tree whose link graph reaches outside the repository.

    Normalizing one target string at a time is not enough, because the escape
    can be composed out of links that each normalize inside the tree.  With
    ``a -> .`` in the tree, ``b -> a/../outside.txt`` normalizes to
    ``outside.txt``, yet the filesystem resolves ``a`` to the repository root
    first and then applies ``..``, so ``b`` names the repository's parent.  The
    snapshot record, the bundle digest and the link texts all stay the same
    while what the action reads through ``b`` is an unsealed host file.

    Resolve every sealed link the way the kernel does instead: component by
    component, following any component that is itself a sealed link, and refuse
    a traversal that leaves the tree, enters ``.git`` or exceeds the resolution
    budget.  A dangling link whose resolution stays inside the tree is still
    accepted: it seals a link text, not a target.
    """

    for path in sorted(symlinks):
        budget = [SNAPSHOT_LINK_RESOLUTION_LIMIT]
        _resolve_snapshot_link(path, symlinks, budget, {path})


def _resolve_snapshot_link(
    path: str,
    symlinks: dict[str, str],
    budget: list[int],
    active: set[str],
) -> list[str]:
    """Return the in-tree components a sealed link resolves to."""

    base = [part for part in posixpath.dirname(path).split("/") if part]
    return _resolve_snapshot_components(
        symlinks[path].split("/"), base, symlinks, budget, active, path
    )


def _resolve_snapshot_components(
    components: list[str],
    base: list[str],
    symlinks: dict[str, str],
    budget: list[int],
    active: set[str],
    origin: str,
) -> list[str]:
    """Walk one target's components through the sealed tree's link graph."""

    def refuse(detail: str) -> None:
        raise SystemExit(
            "pbrun: checkout snapshot symlink points outside the sealed "
            f"repository: {origin!r} -> {symlinks[origin]!r} ({detail})"
        )

    current = list(base)
    for component in components:
        if component in {"", "."}:
            continue
        if component == "..":
            if not current:
                refuse("resolution leaves the repository root")
            current.pop()
            continue
        candidate = current + [component]
        key = "/".join(candidate)
        if key in symlinks:
            if key in active:
                refuse(f"resolution cycles through {key!r}")
            budget[0] -= 1
            if budget[0] < 0:
                refuse("resolution exceeds the sealed link depth limit")
            target = symlinks[key]
            if target.startswith("/"):
                refuse(f"resolution reaches the absolute target of {key!r}")
            active.add(key)
            current = _resolve_snapshot_components(
                target.split("/"), current, symlinks, budget, active, origin
            )
            active.discard(key)
        else:
            current = candidate
        if current[:1] == [".git"]:
            refuse("resolution enters the repository's own Git directory")
    return current


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
    symlinks: dict[str, str] = {}
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
            symlinks[path] = target
        # Count each materialized pathname, not unique object ids: two paths
        # naming one blob occupy two files in the worker checkout.
        logical_bytes += int(size)
    require_contained_snapshot_links(symlinks)
    if logical_bytes > max_bytes:
        raise SystemExit(
            "pbrun: logical checkout tree is "
            f"{logical_bytes} bytes, above the {max_bytes}-byte safety limit; "
            "remove generated data from the worktree or lower its footprint"
        )
    return logical_bytes


#: How many missing paths a refusal names before it stops listing them.  A cone
#: that hides a large subtree would otherwise print thousands of lines.
SPARSE_REFUSAL_SAMPLE = 3


def require_materialized_checkout(root: Path) -> None:
    """Refuse a checkout whose bytes the submitter does not have on disk.

    Git marks a path it deliberately leaves out of the working tree with the
    skip-worktree bit: that is how ``git sparse-checkout`` works, and
    ``git update-index --skip-worktree`` sets the same bit by hand.  ``git add
    -A`` honours the bit, so the sealed tree keeps HEAD's bytes for every such
    path.  Measured: with ``sparse-checkout set keep``, the bundle carried
    ``away/b.txt`` from HEAD while the submitter had no copy of it.

    That is not a seal.  The action key would claim bytes the person who typed
    the command could not read, review, or change, and two submitters with the
    same HEAD and different cones would get the same key for trees they never
    both saw.  Refuse instead, the way this sealer already refuses a shallow
    clone and an active content filter: say what is missing and name the one
    command that fixes it.

    Not ``--snapshot-ref``: that flag pins extra branch refs into the bundle
    and has no bearing on which working-tree paths are sealed, so sending a
    sparse submitter there would be the wrong lever.
    """

    listing = _snapshot_git(root, ["ls-files", "-t", "-z"], strip=False)
    skipped = [
        entry[2:]
        for entry in listing.split("\0")
        if entry.startswith("S ")
    ]
    if not skipped:
        return
    named = sorted(skipped)[:SPARSE_REFUSAL_SAMPLE]
    remaining = len(skipped) - len(named)
    sample = ", ".join(named)
    if remaining > 0:
        sample += f", and {remaining} more"
    raise SystemExit(
        f"pbrun: this checkout leaves {len(skipped)} tracked path(s) out of "
        f"the working tree ({sample}), so their sealed bytes would come from "
        "HEAD rather than from anything you have on disk; restore the full "
        "worktree (git sparse-checkout disable) before submitting"
    )


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


#: Everything that measurably changes the bytes ``git pack-objects`` emits,
#: pinned on the command line so the bundle is a function of the objects and
#: not of the box, the clock or the source repository's storage layout.  The
#: sealer's bundle bytes are hashed into ``params.checkout_snapshot`` and into
#: ``inputs``, so a byte that moves moves the action key, and an action key
#: that moves can never be a CAS hit, resume a campaign, or hold a singleton.
#:
#: ``pack.threads`` is the defect that was measured: Git's delta search splits
#: the object list across one thread per core, and which thread wins which
#: candidate decides which delta bases are chosen.  On sparky (git 2.43.0, 20
#: cores) three unmodified seals of this repository produced three different
#: bundle digests and three different action keys.  A fifteen-object fixture
#: never sees it, which is why the suite did not.
#:
#: Each value is the one Git documents as its default, so this costs one
#: round of cache misses and no further key movement.  ``pack.compression``
#: and ``core.compression`` are spelled ``6`` rather than the ``-1`` that
#: means "the zlib default": the two were measured byte-identical here, and
#: the explicit level is the one that cannot drift with a zlib release.
#: ``pack.usePathWalk`` does not exist before git 2.49 and is ignored there;
#: on a newer Git it selects a different delta ordering.  ``pack.deltaCache*``
#: bound memory rather than output and are deliberately absent.  The delta-
#: island pin is measured inert and kept as belt and braces: on a two-branch
#: fixture, on both git 2.43.0 and 2.53.0, ``repack.useDeltaIslands=true``
#: with a ``pack.island`` regex changed nothing, while ``--delta-islands`` on
#: the command line changed the pack -- so only the option this sealer never
#: passes engages islands, and a ``pack.island`` regex in a user's config
#: cannot reach the seal.  ``-c`` cannot clear that multi-valued key anyway.
BUNDLE_PACK_CONFIGURATION: tuple[tuple[str, str], ...] = (
    ("pack.threads", "1"),
    ("pack.window", "10"),
    ("pack.depth", "50"),
    ("pack.windowMemory", "0"),
    ("pack.compression", "6"),
    ("core.compression", "6"),
    ("core.bigFileThreshold", "512m"),
    ("pack.allowPackReuse", "false"),
    ("pack.useBitmaps", "false"),
    ("pack.useSparse", "true"),
    ("pack.usePathWalk", "false"),
    ("repack.useDeltaIslands", "false"),
)

#: Configuration alone leaves one source of movement that no ``-c`` key can
#: reach: a delta already present in a source pack is reused verbatim, so the
#: same tree seals to different bytes before and after a ``git gc`` -- and
#: auto-gc runs on its own.  These are the pack-objects options that turn the
#: reuse off, and they are the reason this code no longer calls ``git bundle
#: create``, which accepts no pack-objects options.  Measured on this
#: repository: loose and packed storage seal to one digest with them, and to
#: two without.
BUNDLE_PACK_OPTIONS: tuple[str, ...] = (
    "--stdout",
    "--thin",
    "--delta-base-offset",
    "--all-progress-implied",
    "--quiet",
    "--no-reuse-delta",
    "--no-reuse-object",
)

#: A full delta search with no reuse is the price of a stable key: 5.2 s
#: against 1.0 s for the reusing ``bundle create``, measured on the largest
#: repository this fleet seals (prismaquant, 144 MiB of objects, 2173
#: commits).  The bound below is far above that because a slow disk is a
#: stall to report, not a failure to manufacture.
BUNDLE_PACK_TIMEOUT_S = 1800


def deterministic_bundle_argv(git_dir: Path) -> list[str]:
    """The exact ``pack-objects`` command line the sealer runs."""

    pins: list[str] = []
    for key, value in BUNDLE_PACK_CONFIGURATION:
        pins += ["-c", f"{key}={value}"]
    return [
        "git", f"--git-dir={git_dir}", *pins, "pack-objects",
        *BUNDLE_PACK_OPTIONS,
    ]


def write_deterministic_bundle(
    root: Path,
    bundle: Path,
    refs: Sequence[tuple[str, str]],
    *,
    git_dir: Path,
    environment: dict[str, str],
) -> None:
    """Write a Git bundle whose bytes depend only on the objects in it.

    ``git bundle create`` is a header followed by the output of ``git
    pack-objects --stdout --thin --delta-base-offset``, fed the tip object ids
    on standard input.  Writing those two halves here rather than calling the
    porcelain is what makes ``--no-reuse-delta`` reachable; with the pins above
    and identical objects the two spellings were measured byte-identical.

    Args:
        root: The source worktree the child Git runs in.
        bundle: The file to create, header and pack.
        refs: ``(fully qualified name, object id)`` in advertisement order.
        git_dir: The bare repository the pack is walked in, so that a
            ``refs/replace`` or a graft in the source cannot alter the walk.
        environment: The object-directory environment the snapshot was built
            in, without which the sealed commit is not visible.
    """

    object_format = _snapshot_git(
        root,
        [f"--git-dir={git_dir}", "rev-parse", "--show-object-format"],
        environment=environment,
    )
    # v2 carries no capability lines and cannot name a hash algorithm, so a
    # non-SHA-1 repository needs v3 -- which is what ``bundle create`` emits
    # in the same case.
    if object_format == "sha1":
        header = "# v2 git bundle\n"
    else:
        header = f"# v3 git bundle\n@object-format={object_format}\n"
    header += "".join(f"{oid} {name}\n" for name, oid in refs) + "\n"
    tips = "".join(f"{oid}\n" for _, oid in refs)
    argv = deterministic_bundle_argv(git_dir)
    try:
        with bundle.open("wb") as handle:
            handle.write(header.encode("utf-8"))
            handle.flush()
            completed = subprocess.run(
                argv,
                cwd=str(root),
                env=environment,
                input=tips.encode("utf-8"),
                stdout=handle,
                stderr=subprocess.PIPE,
                timeout=BUNDLE_PACK_TIMEOUT_S,
            )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(f"pbrun: cannot snapshot checkout: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or b"").decode("utf-8", "replace").strip()
        raise SystemExit(
            f"pbrun: cannot snapshot checkout: {detail or completed.returncode}"
        )


def build_stamp_closure(stamp_name: str, payload: str) -> dict[str, object]:
    """Describe the UTF-8 bytes injected into the snapshot's private index."""
    raw = payload.encode("utf-8")
    body = {"schema": pb.CODE_CLOSURE_SCHEMA_V1,
            "files": [{"path": stamp_name, "sha256": hashlib.sha256(raw).hexdigest(),
                       "bytes": len(raw)}]}
    return pb.validate_code_closure(
        {**body, "closure_sha256": pb.canonical_sha256(body)})


def build_git_checkout_snapshot(
    cwd: Path,
    *,
    stamp_name: str | None = None,
    stamp_payload: str | None = None,
    cas: pb.PrismaBuildCAS,
    max_bytes: int = CHECKOUT_SNAPSHOT_MAX_BYTES,
    expected_identity: dict[str, str] | None = None,
    snapshot_refs: Sequence[str] = (),
) -> dict[str, object]:
    """Publish the exact dirty tree as an immutable Git bundle with ancestry.

    Args:
        cwd: The directory the action runs in, inside a Git worktree.
        stamp_name: The pbrun closure stamp to seal alongside the tree, or
            ``None`` for a producer that seals its own action body. The stamp
            exists so a pull-queue worker can compare the live tree against the
            action that pinned it; a snapshot-addressed action is compared
            against its own sealed commit instead, so a producer that never
            writes a stamp does not need one invented for it.
        stamp_payload: Optional UTF-8 stamp contents injected into the private
            Git index, without writing a stamp into the submitting worktree.
            The name, bytes and Git mode remain identical to a regular stamp.
        cas: The store the bundle is ingested into.
        max_bytes: The local-disk bound this snapshot may not exceed.
        expected_identity: The checkout identity the caller already read, so
            the seal refuses a tree that moved between the two observations.
        snapshot_refs: Source branches the bundle also advertises.

    Returns:
        The validated ``params.checkout_snapshot`` record.
    """

    root = git_repository_root(cwd)
    if root is None:
        raise SystemExit("pbrun: a non-Git checkout cannot be materialized")
    require_checkout_snapshot_limit(max_bytes)
    if stamp_payload is not None:
        if (not stamp_name or Path(stamp_name).name != stamp_name
                or stamp_name in {".", ".."}):
            raise SystemExit("pbrun: overlay stamp name must be a plain basename")
        subdirectory = cwd.relative_to(root).as_posix() or "."
        stamp_relative = (Path(subdirectory) / stamp_name).as_posix()
        return _build_git_checkout_snapshot(
            cwd, root, subdirectory, stamp_relative, (),
            stamp_payload=stamp_payload, cas=cas, max_bytes=max_bytes,
            expected_identity=expected_identity, snapshot_refs=snapshot_refs,
        )
    if stamp_name is None:
        subdirectory = cwd.relative_to(root).as_posix() or "."
        stamp_relative = None
        stamp_paths: tuple[str, ...] = ()
        return _build_git_checkout_snapshot(
            cwd, root, subdirectory, stamp_relative, stamp_paths,
            cas=cas, max_bytes=max_bytes,
            expected_identity=expected_identity, snapshot_refs=snapshot_refs,
        )
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
    return _build_git_checkout_snapshot(
        cwd, root, subdirectory, stamp_relative, (stamp_relative,),
        cas=cas, max_bytes=max_bytes,
        expected_identity=expected_identity, snapshot_refs=snapshot_refs,
    )


def _build_git_checkout_snapshot(
    cwd: Path,
    root: Path,
    subdirectory: str,
    stamp_relative: str | None,
    stamp_paths: tuple[str, ...],
    *,
    cas: pb.PrismaBuildCAS,
    max_bytes: int,
    expected_identity: dict[str, str] | None,
    snapshot_refs: Sequence[str],
    stamp_payload: str | None = None,
) -> dict[str, object]:
    """Seal the tree once the caller has settled where the stamp is, if any."""

    paths = snapshot_path_roster(root, extra_paths=stamp_paths)
    working_bytes = require_working_tree_size(root, paths, max_bytes=max_bytes)
    if stamp_payload is not None:
        overlay_bytes = len(stamp_payload.encode("utf-8"))
        if working_bytes + overlay_bytes > max_bytes:
            raise SystemExit("pbrun: working tree plus closure stamp exceeds "
                             "checkout snapshot size limit")
    require_untransformed_checkout(
        root, paths + ([stamp_relative] if stamp_payload is not None else []))
    identity = expected_identity or _git_identity(cwd)
    if _git_identity(cwd) != identity:
        raise SystemExit("pbrun: checkout changed before it could be snapshotted")
    parent = identity["head"]
    require_materialized_checkout(root)
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
        # rule as untracked and omit it, and ``git add -A`` would drop a path
        # that the submitter staged with ``git add -f`` but that HEAD has never
        # carried. Seed the sealed roster from the source index, which is the
        # roster the snapshot identity hashes, so the roster hashed and the
        # roster sealed are the same roster. ``git add -A`` then applies
        # deletions and live-byte changes on top.
        _snapshot_git(
            root, ["read-tree", "HEAD"], environment=object_environment
        )
        _seed_index_roster(root, object_environment)
        # Same exclude pin as the roster, and for the same reason: ``add -A``
        # applies the ignore rules to an untracked path, so without it the
        # roster the identity hashes and the tree the bundle carries disagree
        # on exactly the paths a submitter's global excludes match.
        _snapshot_git(
            root,
            [*PERSONAL_EXCLUDES_PIN, "add", "-A"],
            environment=object_environment,
        )
        if stamp_payload is not None:
            # This index and object store belong only to this submission.
            # Keep the historical pathname and mode so unchanged action bytes
            # produce exactly the same commit, bundle, and closure identity.
            stamp_blob = _snapshot_git(
                root, ["hash-object", "-w", "--stdin", "--no-filters"],
                environment=object_environment, input_text=stamp_payload,
            )
            _snapshot_git(
                root, ["update-index", "--add", "--cacheinfo",
                       f"100644,{stamp_blob},{stamp_relative}"],
                environment=object_environment,
            )
        elif stamp_relative is not None:
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
        # The bundle walks every named ref, so the ancestry travels by
        # construction: no explicit history depth to choose, and a requested
        # branch that has diverged simply adds its own side.  The byte ceiling
        # below is what keeps that bounded.
        write_deterministic_bundle(
            root,
            bundle,
            (
                (ref, commit),
                *(
                    (f"refs/heads/{name}", resolved_refs[name])
                    for name in sorted(resolved_refs)
                ),
            ),
            git_dir=bare,
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
    # Keep scanning embedded shell/application strings (including escaped
    # quotes), but require the checkout's last component to end. A hyphen,
    # dot, underscore or other filename continuation names a sibling instead.
    # Colons delimit path lists; quotes and shell punctuation delimit values.
    source_path = re.compile(
        re.escape(source) + r'''(?=$|[/\s:=,;'"`()\[\]{}<>|&]|\\['"])'''
    )
    offenders = [f"argv: {token}" for token in command if source_path.search(str(token))]
    offenders.extend(
        f"environment {name}: {value}"
        for name, value in variables.items()
        if source_path.search(str(value))
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

    ``--tag`` and ``--here`` are two constraints, not two spellings of one.
    A submitter who passes both asks for a box of that class *and* for this
    box, so both land: the explicit tags, then ``hostname``, deduplicated and
    with the hostname last.  Returning ``list(explicit)`` instead dropped the
    host pin without saying so, which is a narrowing the submitter asked for
    and did not get.  (The order is for a reader: the conjunction is sorted
    by ``pool.normalize_placement_tags`` before it is sealed.)

    Nothing here decides *which* free box runs a shared-checkout action; the
    queue does, from the demand and what each worker offers.  That separation
    is the point.
    """

    if explicit:
        if here:
            return [*dict.fromkeys(t for t in explicit if t != hostname),
                    hostname]
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


def require_reachable_runtime(
    tags: Sequence[str],
    *,
    hostname: str,
    runtime_root: Path,
) -> None:
    """Refuse an action whose sealed runtime the chosen boxes cannot open.

    ``pbrun`` transports the *checkout* through the CAS, so a box-local
    checkout runs anywhere -- measured, not assumed: a worktree under
    ``/home/rob/tmp`` submitted through the published runtime executed on
    dl380g10 clean.  What ``pbrun`` does not transport is itself.  The worker
    launcher and ``core.py`` are sealed as absolute paths into the *submitting*
    runtime's tree (``worker_script`` in the pool publication, ``worker_script``
    and ``job_entry`` in the SLURM lane, and the ``runtime`` block of every
    receipt), so a ``pbrun`` invoked out of a developer worktree can be executed
    only by the box that worktree is on.

    ``placement_tags`` cannot see this and should not: it screens argv and the
    caller's environment, which are the submitter's inputs, not ``pbrun``'s own
    installation.  So an explicit ``--tag`` -- which by design outranks every
    pin ``placement_tags`` derives -- sends the action to a box where the
    launcher path does not exist, and the failure arrives from the far side as
    ``can't open file '<worktree>/tools/prismabuild_worker.py'``, after a
    claim, a checkout materialization and a wasted slot.  Measured 2026-09-06
    on dl380g10, and it is what #292 cost four suite shards.

    This is a statement about *this* runtime, not a placement decision: it
    narrows nothing the queue may choose, and a placement naming this box is
    admitted whatever else it also names, so ``--tag x86 --here`` on the x86
    box is a real request and it is still met.
    """

    if not pool.is_box_local_path(Path(runtime_root).resolve()):
        return
    if hostname in tags:
        return
    named = ", ".join(tags) if tags else "any eligible worker"
    raise SystemExit(
        f"pbrun: this runtime is box-local ({runtime_root}), so its worker "
        f"launcher exists only on {hostname}, but the placement admits "
        f"{named}.  Add --tag {hostname} (or --here) to keep the action on "
        "the box that has it, or submit through the published runtime at "
        "/mnt/shared/prismabuild-fleet/repo, which every box can open."
    )


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


#: What the notice says where it has no fleet census to read.
#:
#: The pull queue's census is the worker-offer registry, and ``None`` from
#: ``placeable_hosts`` means nothing has announced.  A transport that keeps no
#: such registry is a different fact with the same shape, and printing the
#: queue's sentence for it would be a claim about a fleet nobody asked.
UNANNOUNCED_CENSUS = "no worker has announced"
NO_CENSUS = "this transport keeps no worker census"


class _NoCensus:
    """The placement census a transport without worker offers has: none.

    The SLURM branch deliberately builds no ``PoolQueue`` -- a retained offer
    from a loop stopped for the cutover would answer wrongly -- but the pin a
    box-local checkout imposes is just as real there, and it is the thing the
    submitter is otherwise never told.  So the notice is printed with the
    census unavailable, which every branch of it already handles.
    """

    @staticmethod
    def placeable_hosts(_intent):
        return None


def _width_of_the_pin(queue, intent, tags: list[str], hostname: str,
                      *, unknown: str = UNANNOUNCED_CENSUS) -> str:
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
        return f"Fleet width unknown: {unknown}."
    others = [h for h in hosts if h != hostname]
    if not others:
        return "No other live box fits this demand, so the pin costs nothing now."
    return (f"{len(others)} other live box{'es' if len(others) > 1 else ''} "
            f"fit{'' if len(others) > 1 else 's'} this demand: "
            f"{', '.join(others)}.")


def parse_progress_phases(
    declared: Sequence[str] | None, *, cycle: bool = False,
) -> dict[str, object] | None:
    """Turn repeated ``--progress-phase NAME=SECONDS`` into a sealed policy.

    Order is the order they were given, and it is load-bearing: entering a
    later phase re-arms the allowance once, so the declaration reads as the
    shape of the work -- load, then compile, then the loop, then publish --
    and the quiet with no new committed units is bounded by the sum declared.
    Cyclic policies renew these phase grants when the cumulative count rises.
    """

    if not declared:
        if cycle:
            raise SystemExit("pbrun: --progress-cycle requires --progress-phase")
        return None
    phases = []
    for entry in declared:
        name, sep, seconds = str(entry).partition("=")
        if not sep:
            raise SystemExit(
                f"pbrun: --progress-phase {entry!r} must be NAME=SECONDS")
        try:
            grace = float(seconds)
        except ValueError:
            raise SystemExit(
                f"pbrun: --progress-phase {entry!r} has a non-numeric allowance"
            ) from None
        phases.append({"name": name, "grace_s": grace})
    policy = {"schema": pb.PROGRESS_POLICY_SCHEMA_V1, "phases": phases}
    if cycle:
        policy["cycle"] = True
    try:
        return pb.validate_progress_policy(policy)
    except pb.ActionContractError as exc:
        raise SystemExit(f"pbrun: {exc}") from None


def progress_required_tags(policy: Mapping[str, object]) -> list[str]:
    return [pb.PROGRESS_TAG, pb.PROGRESS_HELPER_TAG] + (
        [pb.PROGRESS_CYCLE_TAG] if policy.get("cycle") else [])


def progress_contract_notice(
    queue,
    intent: Mapping[str, object],
    *,
    policy: Mapping[str, object] | None,
    requested_timeout_s: float | None = None,
) -> str:
    """Say what this action's stall allowance is, and who cannot honour it.

    Refused rather than warned when no eligible box announces the contract,
    and that asymmetry with ``timeout_ceiling_notice`` is the point.  A
    ceiling that cuts a request short is a smaller budget than asked for; a
    box that does not know the contract applies its whole-run ceiling to an
    action submitted *without* a total-duration limit, which is the exact
    silent kill of a progressing run that #480 is about.  Submitting into that
    would hand back the defect wearing the fix's name.

    On a mixed fleet this only *reports*.  What stops an old box claiming the
    work is the required watchdog and helper tags -- a message cannot decline
    a claim, and during a rolling upgrade both generations are polling the
    same queue.  Read this as the census behind those requirements: which boxes
    it admits, and which it is now waiting past.
    """

    if policy is None:
        return ""
    phases = policy["phases"]
    assert isinstance(phases, Sequence)
    total = sum(float(phase["grace_s"]) for phase in phases)
    lines = [
        "pbrun: progress contract: "
        + ", ".join(f"{phase['name']} {float(phase['grace_s']):g}s"
                    for phase in phases)
        + f"; at most {total:g}s of quiet in total if it never commits work, "
        + ("and no total-duration limit while it does."
           if requested_timeout_s is None else
           f"with an explicit hard execution deadline of {requested_timeout_s:g}s."),
        # The half of the contract the submitter still owes, said at the moment
        # they are declaring it.  An action that declares phases and reports
        # nothing is not bounded by its work; it just ends at the sum above.
        "pbrun: the action must report committed units, or this is only a "
        f"{total:g}s stall budget: prismabuild.progress.commit(units, "
        f'"{phases[0]["name"]}") after each unit is durable, or '
        'python3 "$PRISMABUILD_ACTION_PROGRESS_HELPER" --phase '
        f'{phases[0]["name"]} --units N from a shell.',
    ]
    announced = queue.placement_progress_contracts(intent)
    if not announced:
        if policy.get("cycle"):
            raise SystemExit(
                f"pbrun: no eligible worker offers {pb.PROGRESS_CYCLE_TAG}; "
                "cyclic progress support is unknown. Update the fleet's "
                "published generation and wait for worker adoption.")
        lines.append(
            "pbrun: no worker offers on record; whether any box honours the "
            "contract is unknown.")
        return "\n".join(lines)
    unsupported = sorted(
        host for host, contracts in announced.items()
        if contracts is None or pb.PROGRESS_RECORD_SCHEMA_V1 not in contracts)
    if len(unsupported) == len(announced):
        raise SystemExit(
            "pbrun: no eligible worker announces "
            f"{pb.PROGRESS_RECORD_SCHEMA_V1} ({', '.join(unsupported)}), so "
            "this action would be admitted under the progress contract and "
            "then killed by a whole-run ceiling it never asked for.  Update "
            "the fleet's published generation, or submit with --timeout-s.")
    if unsupported:
        lines.append(
            "pbrun: " + ", ".join(unsupported) + " do not announce "
            f"{pb.PROGRESS_RECORD_SCHEMA_V1}, so this action is not offered "
            f"to them: it requires the {pb.PROGRESS_TAG} tag they do not "
            "publish.  It waits for a box that does rather than being killed "
            "by a ceiling it never asked for.")
    watchdog_hosts = set(announced) - set(unsupported)
    helper_intent = {
        **intent,
        "tags": [*list(intent.get("tags") or []), pb.PROGRESS_TAG,
                 pb.PROGRESS_HELPER_TAG],
    }
    helper_hosts = set(queue.placeable_hosts(helper_intent) or [])
    eligible = watchdog_hosts & helper_hosts
    helper_missing = sorted(watchdog_hosts - helper_hosts)
    if not eligible:
        raise SystemExit(
            "pbrun: no eligible worker both announces "
            f"{pb.PROGRESS_RECORD_SCHEMA_V1} and offers "
            f"{pb.PROGRESS_HELPER_TAG}"
            + (f" ({', '.join(helper_missing)} announce the watchdog but do not "
               "offer the helper)" if helper_missing else "")
            + "; this action uses the helper environment. Update the fleet's "
            "published generation, or submit an action that does not declare "
            "progress phases.")
    if helper_missing:
        lines.append(
            "pbrun: " + ", ".join(helper_missing) + " announce "
            f"{pb.PROGRESS_RECORD_SCHEMA_V1} but do not offer "
            f"{pb.PROGRESS_HELPER_TAG}, so this action is not offered to them: "
            f"it requires {pb.PROGRESS_TAG} and {pb.PROGRESS_HELPER_TAG}. "
            "It waits for a helper-capable box rather than starting without "
            "the documented reporting path.")
    if policy.get("cycle"):
        cycle_intent = {**helper_intent, "tags": [
            *helper_intent["tags"], pb.PROGRESS_CYCLE_TAG]}
        cycle_hosts = set(queue.placeable_hosts(cycle_intent) or [])
        cycle_missing = sorted(eligible - cycle_hosts)
        eligible &= cycle_hosts
        if not eligible:
            raise SystemExit(
                f"pbrun: no eligible worker offers {pb.PROGRESS_CYCLE_TAG} "
                f"({', '.join(cycle_missing)}); update the fleet's published "
                "generation before submitting cyclic progress.")
        if cycle_missing:
            lines.append("pbrun: " + ", ".join(cycle_missing)
                         + f" do not offer {pb.PROGRESS_CYCLE_TAG}; "
                         "this cyclic action waits for a capable worker.")
        lines.append(
            "pbrun: cyclic phases: each phase grants its allowance once "
            "between increases in cumulative committed units; "
            f"at most {total:g}s of quiet after the count stops increasing.")
        helper_intent = cycle_intent
    ceilings = queue.placement_timeout_ceilings(helper_intent)
    for host in sorted(eligible):
        ceiling = ceilings.get(host)
        if ceiling is None:
            lines.append(f"pbrun: {host} announces no phase-grace ceiling; "
                         "its effective allowances are unknown until execution.")
            continue
        for phase in phases:
            requested_grace = float(phase["grace_s"])
            if ceiling < requested_grace:
                lines.append(
                    f"pbrun: {host} limits {phase['name']} grace to {ceiling:g}s "
                    f"(requested {requested_grace:g}s); an explicit hard "
                    "execution deadline is unchanged.")
    return "\n".join(lines)


def timeout_ceiling_notice(
    queue,
    intent: Mapping[str, object],
    *,
    requested: float | None,
) -> str:
    """Say when the boxes that could run this will cut ``--timeout-s`` short.

    Every worker loop enforces a safety ceiling of its own (7200 s by
    default) and ``pool._execution_timeout`` applies it as a silent ``min``.
    Nothing said so: the PrismaQuant #275 campaign asked for 13000 s, was
    admitted without a word, and was killed at 7200 s -- its own
    ``--deadline-seconds`` never fired, so the attempt ended rc=1 with no
    sealed payload, and 554 anchor rows survived only because the job
    checkpoints every ten (#293).

    Warned rather than refused, deliberately.  An action that asks for more
    than it needs and finishes inside the ceiling is not wrong, and refusing
    it would break every long submission on a fleet whose loops all default to
    7200.  What was wrong was being told nothing.  The number to act on is
    the *smallest* eligible ceiling, because the submitter does not choose
    which box claims: any box in this set may.

    A box that announces no ceiling is named as unknown rather than assumed
    unbounded -- the loops that starved #275 announced nothing, and reading
    silence as "no limit" is the same false confidence one layer up.
    """

    if requested is None:
        return ""
    ceilings = queue.placement_timeout_ceilings(intent)
    if not ceilings:
        return ""
    bounded = {host: value for host, value in ceilings.items() if value is not None}
    silent = sorted(host for host, value in ceilings.items() if value is None)
    cutting = {host: value for host, value in bounded.items() if value < requested}
    if not cutting and not silent:
        return ""
    lines = []
    if cutting:
        lowest = min(cutting.values())
        named = ", ".join(f"{host} {value:g}s" for host, value in sorted(cutting.items()))
        certain = len(cutting) == len(ceilings)
        lines.append(
            f"pbrun: --timeout-s {requested:g} exceeds the execution ceiling "
            f"{'every' if certain else 'some'} eligible worker announces "
            f"({named}), so this action "
            f"{'will' if certain else 'may'} be killed at {lowest:g}s, not "
            f"{requested:g}s."
        )
    if silent:
        lines.append(
            "pbrun: " + ", ".join(silent) + " announce no execution ceiling "
            "(offers predating the field), so what they would enforce is "
            "unknown rather than unlimited."
        )
    return "\n".join(lines)


def pin_notice(
    queue,
    intent,
    *,
    cwd: Path,
    hostname: str,
    here: bool,
    portable_checkout: bool = False,
    unknown_census: str = UNANNOUNCED_CENSUS,
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
    that asked for them.**  A first version asked the ``here`` flag instead,
    and so announced "PINNED to sparky by --here, so no other box can claim
    this action" for a submission whose tags were ``['x86']`` -- naming, as
    the *other* box, the only box that could actually run it.  A notice about
    a pin has one job and that was it.  ``placement_tags`` no longer drops the
    host pin that way, but the reading rule is what keeps this correct
    whatever it returns.

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
        width = _width_of_the_pin(
            queue, intent, tags, hostname, unknown=unknown_census)
        return f"{head}  {width}{tail}"

    # No host tag landed.  Say what did, and what it costs.
    notes: list[str] = []
    if local:
        if others is None:
            notes.append(f"WARNING -- the checkout {cwd} exists only on "
                         f"{hostname}, and {unknown_census}, so tags "
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
            notes.append(f"Which box claims it is unknown: {unknown_census}.")
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


class UnreadableTerminal(ValueError):
    """A published ending exists but cannot supply a verdict."""

    def __init__(self, path: Path, reason: str):
        self.path = path
        self.reason = reason
        super().__init__(f"{reason}: {path}")


def terminal_record(
    path: Path, generation: float | None, *, report_unreadable: bool = False,
):
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

    With ``report_unreadable=True``, a present but unreadable record raises
    ``UnreadableTerminal``. Nonblocking compatibility probes keep returning
    ``None``; waiters opt in so corruption is not reported as a timeout.
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
    except FileNotFoundError:
        # A concurrent requeue can remove an ending after readdir.
        return None
    except (OSError, ValueError) as exc:
        if report_unreadable:
            reason = (str(exc.strerror or type(exc).__name__).lower()
                      if isinstance(exc, OSError) else "not valid JSON")
            raise UnreadableTerminal(path, reason) from exc
        return None
    if not isinstance(record, dict):
        if report_unreadable:
            raise UnreadableTerminal(path, "not a JSON object")
        return None
    if generation is None:
        return record
    theirs = record.get("published_unix")
    if isinstance(theirs, (int, float)) and not isinstance(theirs, bool):
        return record if float(theirs) == float(generation) else None
    return record


def _preemption_requeue(q, key: str, ending, generation) -> float | None:
    """The generation a preemption requeued this one as, or ``None``.

    ``PoolQueue`` stops an admitted background holder to admit foreground work
    and immediately re-publishes it (#364).  The requeue is a NEW generation --
    it has to be, because the cancellation it revives is generation-scoped and
    would otherwise cover its own retry -- so a waiter watching the stopped
    generation sees a withdrawal and would report exit 143 for work the queue
    is about to run again.  That would make "retried, not lost" true of the
    queue and false of everyone waiting on it.

    Deliberately narrow.  It follows only an ending stamped ``preempted_by``,
    which nothing but admission writes, and only to a generation the queue
    actually holds -- waiting to run, running, or already ended.  An operator's
    withdrawal still ends the wait; so does a preemption whose requeue was
    never published, because then there is no newer generation to find and the
    cancellation is the whole account of what happened.

    The terminal directories are searched as well as the live ones, because a
    caller that arrives after the requeued run finished would otherwise be told
    about the stop and never about the ending that followed it.
    """

    if generation is None or ending.get("preempted_by") is None:
        return None
    if str(ending.get("status") or "") != "withdrawn":
        return None
    # Admission holds this lock across withdrawal and replacement publication.
    # Reading the marker before the replacement exists is an intermediate
    # transition, not evidence that the preempted action was abandoned.
    with q._transition_locked(key):
        records = [record for _, record in q.withdrawal_decisions(key)]
        records.extend(record for _, record in q.archived_preemption_outcomes(key))
        for state in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED,
                      pool.WITHDRAWN):
            try:
                record = json.loads(
                    q.item_path(state, key).read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if isinstance(record, dict):
                records.append(record)
        successors = []
        for record in records:
            theirs = record.get("published_unix")
            parent = record.get("supersedes_withdrawal")
            if (type(theirs) not in (int, float)
                    or not isinstance(parent, dict)
                    or parent.get("published_unix") != generation
                    or parent.get("preempted_by") != ending.get("preempted_by")):
                continue
            # Follow the actual handoff, never an unrelated later submission
            # of the same key. Immutable decisions retain intermediate links
            # when the action was preempted more than once.
            if float(theirs) > float(generation):
                successors.append(float(theirs))
        return min(successors) if successors else None


def landed_outcome(
    q, key: str, *, wait_s: float, generation: float | None = None,
    report_unreadable: bool = False,
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
        broken = []
        for path in watched:
            try:
                record = terminal_record(
                    path, generation, report_unreadable=report_unreadable)
            except UnreadableTerminal as exc:
                broken.append(exc)
                continue
            if record is not None:
                found.append((path, record))
        # The immutable cancellation is already an ending if its writer died
        # before updating withdrawn/<key>.json, or publication retired that
        # visible marker while an earlier generation's waiter was still here.
        decisions = (
            q.withdrawal_decisions(key)
            if generation is None
            else q.withdrawal_decisions(key, generation=generation)
        )
        for path, record in decisions:
            if generation is None or float(record["published_unix"]) == float(generation):
                if not any(existing.get("status") == "withdrawn"
                           and existing.get("published_unix") == record.get("published_unix")
                           for _, existing in found):
                    found.append((path, record))
        if generation is not None and not any(
                record.get("published_unix") == generation for _, record in found):
            # A later same-status generation may have replaced the only mutable
            # terminal row. The preemption successor's immutable attempt still
            # carries its generation link, complete history and original verdict.
            found.extend(q.archived_preemption_outcomes(key, generation=generation))
        if generation is not None and found:
            # A legacy ending with no generation remains the fallback when it
            # is the only account of this run.  It must not outrank an exact
            # ending that is also present: otherwise an old unstamped DONE can
            # hide this generation's withdrawal and report cancelled work as
            # successful.
            exact = [
                entry for entry in found
                if isinstance(entry[1].get("published_unix"), (int, float))
                and not isinstance(entry[1].get("published_unix"), bool)
                and float(entry[1]["published_unix"]) == float(generation)
            ]
            landed = (exact or found)[0]
            requeued = _preemption_requeue(q, key, landed[1], generation)
            if requeued is None:
                return landed
            # Admission stopped this generation to give a foreground item the
            # box, and published another one to run it again (#364).  Reporting
            # the cancellation would tell the caller its work was decided
            # against, when the queue is already running it: follow the
            # generation the requeue published instead.
            generation = requeued
            continue
        if len(found) == 1:
            return found[0]
        if found:
            return max(found, key=_stamp)
        if broken:
            raise broken[0]
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
        archived_source = (
            outcome.get("preemption_context") is not None
            and Path(outcome_path) == q.attempt_path(outcome, outcome["attempts"]))
        if not archived_source and disposition != Path(outcome_path).parent.name:
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
    claimed_host = outcome.get("claimed_host")
    if not isinstance(claimed_host, str) or not claimed_host:
        # A terminal record filed by a path that did not carry the field can
        # still have it in the immutable attempt, which ``archive_attempt``
        # stamps from the claim itself. Read the newest attempt that names a
        # box; never invent one, because "unknown" is the answer that sends
        # nobody anywhere.
        claimed_host = None
        if adopted is not None:
            for attempt in reversed(q.attempt_outcomes(outcome)):
                candidate = attempt.get("claimed_host")
                if isinstance(candidate, str) and candidate:
                    claimed_host = candidate
                    break
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
        # The box the action was ON. ``finished_host`` is the box that
        # concluded it, and for a reaped claim those are different machines.
        "claimed_host": claimed_host,
        "elapsed_s": detail.get("elapsed_s"),
        # What the run cost and how loaded its box was (#372 Tier 0).  Carried
        # whole rather than flattened, because the record is what a reader
        # comes back to and the summary is only what fits on a line.
        "resource_profile": detail.get("resource_profile"),
        "returncode": detail.get("returncode"),
        # The action's own ending, where the transport recorded one.  The
        # launcher's status above is 1 for every failure, so an action that
        # exited 7 reads as 1 without this.
        "action_returncode": detail.get("action_returncode"),
        "action_signal": detail.get("action_signal"),
        "receipt_published": detail.get("receipt_published"),
        # Present as ``None`` on every unprofiled run so a reader tests one
        # field rather than the absence of one.
        "profile": detail.get("profile"),
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


def publish_or_refuse(q, publication: Mapping[str, object]):
    """Enqueue one submission, or say why the queue would not take it.

    ``PoolQueue.publish`` refuses a fenced queue: ``fleet/slurm/cutover.sh``
    removes the write bit on ``pb-queue/ready`` while it retires the pull
    queue's execution plane, so a rename into that directory fails with
    EACCES rather than leaving an accepted action in a queue whose workers are
    being stopped.  That refusal is the caller's to read -- the submitter is
    the one who can resubmit through SLURM or wait -- so it arrives as a line
    rather than as a traceback.
    """

    try:
        return q.publish(**publication)
    except pool.PoolContractError as exc:
        raise SystemExit(f"pbrun: {exc}") from exc


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

    try:
        landed = landed_outcome(
            q, key, wait_s=wait_s, generation=generation,
            report_unreadable=True)
    except UnreadableTerminal as exc:
        print(f"pbrun: unreadable ending for {key[:12]}: {exc}", file=sys.stderr)
        return 1
    if landed is None:
        print(f"pbrun: gave up waiting for {key[:12]}", file=sys.stderr)
        return GAVE_UP_EXIT
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
    print(f"pbrun: {outcome_headline(summary)}", file=sys.stderr)
    if status == "cache_hit":
        return 0
    rc = detail.get("returncode")
    if isinstance(rc, int):
        return reported_exit(rc, key=key)
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


def action_status_suffix(detail: Mapping[str, object]) -> str:
    """What to add to an outcome line when the action's status is not the run's.

    Nothing at all when the two agree, which is the ordinary case: an action
    that exited 3 under a transport that reports its own launcher's status
    would say the same number twice. When they differ -- the launcher exits 1
    for every failure -- the run's number stays first, because that is the one
    ``pbrun`` returns as its own exit status, and the action's is named as the
    action's.
    """

    action = detail.get("action_returncode")
    if not isinstance(action, int) or isinstance(action, bool):
        return ""
    if action == detail.get("returncode"):
        return ""
    signal = detail.get("action_signal")
    if isinstance(signal, int) and not isinstance(signal, bool):
        return f"; rc={detail.get('returncode')} (action killed by signal {signal})"
    return f"; rc={detail.get('returncode')} (action exited {action})"


def resource_suffix(detail: Mapping[str, object]) -> str:
    """What the run cost, on the line that says it finished, or nothing.

    Nothing rather than a row of dashes: a run whose box had no recorder should
    not spend a line saying so every time, and the record still carries the
    reason under ``detail.resource_profile.box_window``.
    """

    described = pool.describe_resource_profile(
        pool.resource_profile_summary(detail))
    return f"; {described}" if described != "-" else ""


def profile_suffix(detail: Mapping[str, object]) -> str:
    """Where a profiled run left its profile, said on the line that reports it.

    Only on a run that asked for one, so an ordinary ending is unchanged.  The
    phrasing itself lives in ``core.describe_profile`` so this and
    ``pbstatus`` abbreviate one digest the same way.
    """

    described = pb.describe_profile(detail.get("profile"))
    return f"; {described}" if described else ""


def outcome_headline(summary: Mapping[str, object]) -> str:
    """One line saying how a run ended, and where -- and who "where" is.

    ``finished_host`` is the box that *filed* the record, which for anything a
    worker ran is also the box that ran it.  For a claim the reaper concluded
    it is not: ``reap_stale`` stamps its own hostname there, correctly, and
    rendering that as the place the action failed sends an investigation at
    the machine that merely noticed.  It did, on 2026-09-06.

    The bias outlives the one wrong trip.  The box that runs most of the
    fleet's work reaps most of it too, so misattributed failures accumulate on
    the box that already looks busiest and the fleet's failure profile leans
    toward whichever box reaps.  That is a measurement defect: it corrupts the
    evidence used to decide which box is unhealthy.

    ``detail["lease_age_s"]`` is the discriminator, not the status text.  The
    reaper is its only producer, it is on every record the reaper files
    whatever status it chose, and a gate can read it.  Its value carries a
    second distinction the operator needs: a number is a lease that stopped
    being refreshed, while ``None`` means no lease was ever written -- the
    claim was lost before the worker launched anything.
    """

    detail = summary.get("detail") or {}
    status = str(summary.get("status"))
    finished = summary.get("finished_host") or "(not recorded)"
    if "lease_age_s" not in detail:
        # ``elapsed_s`` is present and null on a SLURM record whose scheduler
        # provenance was purged, so the key's presence must not defeat the
        # default.
        return (f"{status} on {finished} "
                f"in {(detail.get('elapsed_s') or 0):.0f}s"
                f"{action_status_suffix(detail)}"
                f"{resource_suffix(detail)}"
                f"{profile_suffix(detail)}")
    held = summary.get("claimed_host")
    age = detail.get("lease_age_s")
    if isinstance(age, (int, float)) and not isinstance(age, bool):
        lease = f"lease {float(age):.1f}s stale"
    else:
        lease = "no lease was ever written"
    return (f"{status} -- held by {held or '(not recorded)'}, "
            f"reaped by {finished}, {lease}")


def reported_exit(returncode: int, *, key: str) -> int:
    """One run's recorded status as ``pbrun``'s own exit status.

    Every status but ``pbrun``'s own reserved words passes through unchanged,
    which is the contract callers already have: an action that exits 7 exits
    7.  One that lands on a reserved word is reported as 1, an ordinary
    failure, with the real number said on a line of its own so nothing is
    hidden.  The record keeps it under ``detail.returncode`` either way, and
    ``pbstatus`` and ``pbwait`` print it from there.

    Clamping rather than renumbering, because the codes have readers this
    cannot see: ``pbcampaign``, the harness, and whatever an operator wrapped
    ``pbrun`` in.  Every existing condition keeps the code it had, and the one
    case that was ambiguous stops being ambiguous.

    Args:
        returncode: The status the terminal record recorded for the run.
        key: The action key, for the prefix every fleet tool takes.

    Returns:
        ``returncode``, or 1 when returning it would impersonate a verdict
        ``pbrun`` did not reach.
    """

    if returncode not in RESERVED_EXITS:
        return returncode
    print(f"pbrun: {key[:12]} exited {returncode}, which is one of pbrun's own "
          f"exit codes; reporting it as 1 so it is not read as pbrun's "
          f"verdict. The run's own status is detail.returncode in the record.",
          file=sys.stderr)
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


#: The scheduler commands ``live_submission`` needs to ask whether a recorded
#: job is still alive, and the keyword arguments ``slurm_lane.resume`` accepts.
#: Named explicitly rather than passed through: ``slurm_outcome`` forwards
#: whatever a caller gave it to ``run``, and ``run`` takes flags -- ``sbatch``,
#: ``retry_safe`` -- that neither of these two has any use for.
_QUERY_COMMANDS = frozenset({"sacct", "scontrol", "squeue"})
_RESUME_COMMANDS = frozenset({
    "poll_s", "sacct", "scontrol", "squeue", "sstat", "sleep", "clock",
    "on_stall", "on_notice",
})

#: The interpreter pbrun's sealed argv starts with.  A nonportable action
#: binds its exact bytes, so the name is stated once, where the scope is built.
SEALED_ARGV0 = "/bin/bash"


def detached_attempts_refusal(max_attempts: int) -> str:
    """Why a detached submission cannot carry more than one attempt.

    A retry is a second submission made after somebody watched the first one
    fail.  Detaching means nobody is watching, so the choice is between
    silently running one attempt for a caller who asked for three, and saying
    so.  A function rather than a literal because ``pbcampaign`` submits every
    row detached and has to refuse the same row for the same reason, at
    manifest load; two copies of the sentence would be two policies.
    """

    return (
        f"pbrun: --detach submits one attempt and returns, so it cannot "
        f"honour --max-attempts greater than 1 (this asks for {int(max_attempts)}); "
        f"submit it attached, or detach with a single attempt"
    )


def require_gpu_memory_scope(*, gpu_memory_gb, gpu: bool, transport: str) -> None:
    """Share GPU-budget scope refusals with campaign manifest preflight."""

    if gpu_memory_gb is not None and not gpu:
        raise ValueError("--gpu-memory-gb requires GPU demand")
    if gpu_memory_gb is not None and transport == "slurm":
        raise ValueError(
            "--gpu-memory-gb requires pool transport; SLURM VRAM budgets are not supported"
        )


def require_progress_scope(*, progress: Mapping[str, object] | None,
                          transport: str) -> None:
    """Refuse a progress contract on a transport with nothing to enforce it.

    The stall watchdog is ``pool.execute``'s: it holds the progress file, mints
    the launch token, and samples on the heartbeat cadence.  The SLURM lane
    runs the same sealed action through the same launcher, but the enforcement
    it has is ``--time``, sent only when ``--timeout-s`` was given -- a total
    duration, which is the policy #480 exists to stop standing in for progress.

    Sealing the contract there would be worse than not offering it.  The action
    would be admitted on the promise that its own advancement bounds it, and
    then run under no watchdog at all and, absent ``--timeout-s``, under no
    deadline either: unbounded, which is the outcome the contract's fifth point
    forbids.  ``core._progress_environment`` refuses the same launch from the
    other end; this is the refusal at the moment the submitter is still
    watching.
    """

    if progress is not None and transport != "pool":
        raise ValueError(
            "--progress-phase requires pool transport: the stall watchdog is "
            "the pull-queue worker's, and the SLURM lane can only enforce a "
            "total duration (--timeout-s becomes --time).  Submit this action "
            "to the pool, or bound it there with --timeout-s and no phases"
        )


def require_host_class_scope(
    *, measurement: bool, host_class: str | None, transport: str, anywhere: bool = False
) -> None:
    """Refuse a scope the design cannot honour, before anything is sealed."""

    if measurement and transport == "pool" and anywhere:
        if host_class is not None:
            raise SystemExit("pbrun: --host-class already scopes pool measurement placement; "
                             "drop --anywhere")
        raise SystemExit(
            "pbrun: pool measurements run on the submitting host whose "
            "platform/toolchain is sealed; --anywhere contradicts that placement."
        )

    if measurement and host_class is None and transport != "pool":
        raise SystemExit(
            "pbrun: --measurement requires --host-class CLASS.\n"
            "A measurement's numerics do not transfer across architectures, "
            "so its result is keyed on the host class that produced it "
            "(docs/design.md, \"Cache/action-key semantics\"); a portable "
            "measurement would let any box's KL stand in for another's."
        )
    if host_class is not None and transport != "slurm" and not (
        transport == "pool" and measurement
    ):
        raise SystemExit(
            "pbrun: --host-class needs --transport slurm or pool --measurement.\n"
            "A host_class_keyed action is attested through the SLURM "
            "controller (docs/design.md, \"Worker preflight and execution "
            "attestation\"); a pull-queue worker refuses it at preflight, so "
            "submitting it there queues work that cannot run."
        )


def host_class_scope(
    host_class: str | None, *, measurement: bool = False, transport: str = "slurm",
) -> tuple[dict[str, object], dict[str, str]]:
    """The execution scope and the toolchain a submission seals.

    Ordinary portable generation declares no toolchain. Pool measurements
    seal the submitting platform and toolchain; main pins their placement to
    that host unless an explicit class asserts shared external dependencies.
    A class-scoped pool measurement remains platform-keyed and additionally
    binds actual device models. A SLURM host-class-keyed action is
    nonportable, and the core requires a nonportable action to bind the
    executable behind argv[0] and the ABI and accelerator facts of the box
    that runs it -- facts pbrun can read only from the box it runs on.  So a
    class-keyed submission carries this box's facts, and a worker of the
    class verifies each of them at preflight; a submission from a box of
    another class is refused there, naming the field that differs.
    """

    if measurement and transport == "pool":
        # A pool worker can attest its platform and executable/ABI directly.
        # A class is placement intent, not a forged SLURM attestation. The
        # actual platform, ABI, driver and device models constrain numerics;
        # physical UUIDs and the selected worker remain receipt provenance.
        evidence = pb._collect_worker_evidence(
            **({"attest_accelerator_identity": True} if host_class is not None else {})
        )
        toolchain = {
            **pb.executable_toolchain_contract(SEALED_ARGV0),
            **pb.live_platform_toolchain_contract(evidence=evidence),
        }
        if host_class is not None:
            toolchain["accelerator_models.sha256"] = pb.accelerator_models_contract(evidence)
        return (
            {"portability": "platform_keyed",
             "platform_key": pb._platform_key_from_evidence(evidence),
             "host_class": None},
            toolchain,
        )
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


def cached_outcome(
    key: str,
    *,
    receipt,
    queue_root,
    resources,
    tags: list[str],
    retry_safe: bool,
    max_attempts: int,
) -> int:
    """Report receipted work as a finished run, submitting nothing.

    Prints the lines a pull-queue cache hit printed and exits 0.  A ``done/``
    record is filed only when the key has none: the record of the run that
    did the work is the one every reader wants, and a re-run has nothing to
    add to it.  With no record at all -- a receipt published from another
    store, or a record moved aside -- the fleet's readers still get an ending,
    with ``status=cache_hit`` as the pull queue filed one.

    Args:
        key: The action key.
        receipt: The CAS receipt ``lookup`` returned.
        queue_root: The queue whose ``done/`` is consulted and, if empty for
            this key, written.
        resources: The lane resources the submission would have carried.
        tags: The sealed placement tags.
        retry_safe: The retry policy, recorded as the pool recorded it.
        max_attempts: Likewise.

    Returns:
        0, always: the work is done.
    """

    print(f"pbrun: {key[:12]} is already in the CAS; nothing submitted",
          file=sys.stderr, flush=True)
    host = socket.gethostname()
    existing = Path(queue_root) / pool.DONE / f"{key}.json"
    if not existing.exists():
        slurm_lane.publish_outcome(
            queue_root=queue_root,
            action_key=key,
            published_unix=time.time(),
            published_by=host,
            status="cache_hit",
            attempts=0,
            max_attempts=int(max_attempts),
            retry_safe=bool(retry_safe),
            resources=resources.demand(),
            tags=tags,
            receipt=receipt,
            claimed_by=host,
            detail={"elapsed_s": 0.0},
        )
    print(f"pbrun: cache_hit on {host} in 0s", file=sys.stderr, flush=True)
    return 0


def _unfiled_record(
    exc: BaseException,
    *,
    key: str,
    action,
    cas,
    record: str = "",
    tool: str = "pbrun",
) -> int:
    """Report a record write that failed after the thing it records happened.

    The lane writes every fact it keeps after the fact is already true: the
    submission record after ``sbatch`` returned an id, the terminal record
    after the receipt landed in the CAS. So a full mount or a queue directory
    somebody tightened produces an ``OSError`` at a point where the job is
    real and the work may be finished. Reported as a traceback, that told an
    operator a temp file name and nothing else: not the job id, not whether
    the work was done, not which command would file the ending.

    Args:
        exc: The failure the lane raised. ``exc.job_id`` names the accepted
            job when the lane knew one; see ``slurm_lane._naming_job``.
        key: The action key, for the prefix every fleet tool takes.
        action: The sealed action, to ask the CAS whether the work is done.
        cas: The CAS to ask.
        record: The path that would not write, when the caller knows it and
            the exception does not carry one.

    Returns:
        ``RECORD_WRITE_FAILED_EXIT``.
    """

    job_id = str(getattr(exc, "job_id", "") or "")
    path = str(getattr(exc, "filename", "") or record or "")
    # A ``SlurmLaneError`` carries its path inside its message and has no
    # ``strerror``; an ``OSError`` carries both as fields.  Print whichever the
    # failure actually has rather than a placeholder for the other.
    reason = str(getattr(exc, "strerror", "") or exc)
    where = f"  record:    {path}\n" if path else ""
    try:
        done = cas.lookup(action) is not None
    except OSError:
        # The mount that would not take the record may not answer this either.
        done = None
    if done:
        advice = (
            f"The receipt is in the CAS, so the work is done and re-running "
            f"costs nothing.\n"
            f"Clear what blocked the write, then run "
            f"`tools/fleet/pbwait.py {key[:12]}` to file the ending."
        )
    else:
        advice = (
            ("The CAS could not be read, so receipt status is unknown.\n"
             if done is None else
             "No receipt is in the CAS, so the job may still be running.\n")
            + f"Clear what blocked the write, then run "
            f"`tools/fleet/pbwait.py {key[:12]}` "
            f"to wait on it and file the ending, or "
            f"`tools/fleet/pbrun.py --transport slurm --withdraw {key[:12]}` "
            f"to stop it."
        )
    print(
        f"{tool}: slurm took this action, but {tool} could not write its "
        f"record.\n"
        f"  slurm job: {job_id or '(none accepted)'}\n"
        f"{where}"
        f"  reason:    {reason}\n"
        f"{advice}",
        file=sys.stderr, flush=True)
    return RECORD_WRITE_FAILED_EXIT


def _lane_io_failure(exc, *, key: str, action, cas, tool="pbrun", job_id="") -> int:
    """Separate stamped post-submission writes from other filesystem faults."""

    if getattr(exc, "job_id", None):
        return _unfiled_record(exc, key=key, action=action, cas=cas, tool=tool)
    path = str(getattr(exc, "filename", "") or "")
    reason = str(getattr(exc, "strerror", "") or exc)
    print(
        f"{tool}: filesystem access failed while processing slurm action {key[:12]}.\n"
        f"  slurm job: {job_id or '(not identified; acceptance is unknown)'}\n"
        f"  path:      {path or '(not supplied)'}\n"
        f"  reason:    {reason}\n"
        "Submission and receipt status could not be fully verified.\n"
        f"Restore filesystem access, then run `tools/fleet/pbwait.py {key[:12]}` "
        "to recover a recorded submission or ending; if nothing was submitted, "
        "retry the original command.",
        file=sys.stderr, flush=True,
    )
    return RECORD_WRITE_FAILED_EXIT


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
    priority: int = 0,
    placement_notice: str = "",
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
    if placement_notice:
        # How wide this action is, said before anything is submitted, exactly
        # as the pool path says it.  The pin a box-local checkout imposes is a
        # consequence of a path rather than of a flag, and a submitter that is
        # not told has narrowed the fleet to one box without knowing.
        print(placement_notice, file=sys.stderr, flush=True)
    slots = int(demand.get("gpu", 0) or 0)
    if exclusive and slots > 1:
        # ``LaneResources.gres()`` answers ``gpu:1`` for an exclusive action
        # whatever the count says, because ``gpu:N`` and ``shard:N`` are
        # mutually exclusive requests against one device and exclusivity is the
        # first.  On today's one-device boxes that is right and the count is
        # redundant; on a two-GPU box it would silently hand back half of what
        # was asked for.  Refusing is the honest answer either way -- the pool
        # read the count off worker offers, and SLURM has no such thing here.
        raise SystemExit(
            f"pbrun: --exclusive --gpu-capacity {slots} is not something this "
            f"transport can express.\n"
            "Under SLURM, exclusivity IS the whole device: the lane sends "
            "--gres=gpu:1, and a count above one would have to name that many "
            "whole devices, which nothing here derives or checks.\n"
            "Drop --gpu-capacity to take one device exclusively, or drop "
            "--exclusive and ask for --gpu-capacity slots (shards) instead."
        )
    resources = slurm_lane.LaneResources.from_demand(demand, exclusive=exclusive)
    lane_commands.setdefault("on_stall", lambda report: _report_stall(key, report))
    lane_commands.setdefault(
        "on_notice",
        lambda text: print(f"pbrun: {text}", file=sys.stderr, flush=True))
    # Say that the slot has no device, every time, on the line that announces
    # the submission.  The mask is applied before the transport branch and it
    # is also a silent narrowing: a suite that used to run its CUDA tests now
    # skips them, and a skip that nobody announced reads as the same green.
    masked = "" if slots else "  [no GPU: CUDA_VISIBLE_DEVICES='']"
    queue = SH / "pb-queue" if queue_root is None else queue_root
    # Ask the CAS before asking the scheduler for a node.  A key is a content
    # hash, and asking for the same work again is the normal way to ask
    # whether it is done; the answer is a receipt on the mount, readable from
    # here.  ``--detach`` made this check from the start.  The attached path
    # let the job discover it instead, which spent a job id, a materialized
    # checkout and a node to learn what this process could have read -- so a
    # campaign re-run through ``pbcampaign`` was free and a hand re-run of one
    # of its rows was not.  The node still checks (``run-local`` looks the
    # action up before it runs anything), so a receipt that lands between
    # this check and the job's start costs a materialization, never a rerun.
    # One lookup: it verifies the result blob, which on a rendered model is
    # gigabytes hashed over NFS.
    try:
        receipt = None if detach else cas.lookup(action)
    except OSError as exc:
        return _lane_io_failure(exc, key=key, action=action, cas=cas)
    if receipt is not None:
        return cached_outcome(
            key,
            receipt=receipt,
            queue_root=queue,
            resources=resources,
            tags=tags,
            retry_safe=retry_safe,
            max_attempts=max_attempts,
        )
    # Attach to a run already in flight rather than start a second copy of it.
    #
    # A key is a content hash, so asking for the same work twice is the normal
    # way to ask whether it is done.  The pull queue answered that with one
    # ``ready/<key>.json`` and a claim: the second ask could not become a
    # second execution.  SLURM has no claim, and this path submitted
    # unconditionally -- so two attached ``pbrun``s of one key were two jobs of
    # one action on the fleet, materializing the same checkout, taking the same
    # GPU twice and racing to publish one receipt.
    #
    # ``--detach`` already made this check (it is the same
    # ``live_submission``); it just never ran for a caller who waits.  Live
    # means recorded, no ending covering that generation, and a job the
    # controller still knows in a non-terminal state -- so a terminal or
    # forgotten submission submits afresh here exactly as it does there.
    attached = None
    if not detach:
        found = live_submission(
            pool.PoolQueue(queue), key, lane_root=lane_root,
            **{name: lane_commands[name]
               for name in _QUERY_COMMANDS if name in lane_commands},
        )
        # A live *pool* item is not this transport's to wait on: it belongs to
        # a worker, and ``resume`` reconstructs a SLURM submission record.
        attached = found if found is not None and found[0] == "slurm" else None
    submitted_job_ids = []

    def announce_submission(job):
        submitted_job_ids.append(job.job_id)
        print(
            f"pbrun: submitted {key[:12]} as slurm job {job.job_id} "
            f"(attempt {job.attempt}) tags={tags} demand={demand}{masked}",
            file=sys.stderr, flush=True)

    # sbatch's own refusal is this transport's capability gate: an unknown
    # Feature or an impossible GRES is rejected at submit time, which is the
    # moment the pool path's ``capability_verdict`` spoke.  So it reaches the
    # caller as the message SLURM wrote, in the shape that message had, rather
    # than as a traceback.
    try:
        if attached is not None:
            submission = attached[2]
            job_id = str(submission.get("job_id") or "")
            print(f"pbrun: {key[:12]} is already running (slurm job {job_id}); "
                  f"attaching to it rather than submitting a second copy"
                  f"{masked}", file=sys.stderr, flush=True)
            # ``resume`` files the ending ``run`` would have filed, off the
            # recorded submission alone -- the same reconstruction
            # ``--withdraw`` builds a terminal record from.  So the caller
            # reads the same lines and gets the same exit code whether it
            # submitted this job or joined it.
            result = slurm_lane.resume(
                submission,
                action=action,
                cas=cas,
                queue_root=queue,
                wait_s=wait_s,
                **{name: value for name, value in lane_commands.items()
                   if name in _RESUME_COMMANDS},
            )
        else:
            result = slurm_lane.run(
                action,
                cas=cas,
                request_path=request_path,
                placement=tags,
                resources=resources,
                partition=slurm_lane.partition_for(
                    resources, tags, anywhere=anywhere),
                # ``--priority`` is a queue hint on either transport: the pool
                # sorts its ready list on it, and SLURM subtracts the derived nice
                # from the base priority its scheduler assigned.  Dropping it here
                # is what let ``pool_reset``'s bulk ``--priority -10`` land
                # alongside interactive work instead of behind it.
                priority=priority,
                timeout_s=timeout_s,
                worker_script=runtime_root / "tools" / "prismabuild_worker.py",
                job_entry=runtime_root / "tools" / "fleet" / "slurm_job.py",
                retry_safe=retry_safe,
                max_attempts=max_attempts,
                root=lane_root,
                # Eleven fleet tools and Tessera's ``merge_suite`` read one action's
                # ending out of this directory.  The lane files it there so the
                # cutover is a change to one dispatcher and not to every reader.
                queue_root=queue,
                wait_s=wait_s,
                detach=detach,
                on_submit=announce_submission,
                **lane_commands,
            )
    except slurm_lane.SubmissionFateUnknown as exc:
        # Not a refusal, so not reported as one and not filed as one.  sbatch
        # stopped answering and the controller could not settle whether it
        # took the job, so a job of this action may be queued right now.  The
        # exit code is the one that already means "no verdict yet".
        print(f"pbrun: the fate of this submission is unknown.\n  {exc}",
              file=sys.stderr, flush=True)
        return GAVE_UP_EXIT
    except slurm_lane.SlurmLaneError as exc:
        if getattr(exc, "job_id", None):
            # sbatch accepted this before the lane failed, so this is a record
            # that would not write and not a refusal.  Reported as a refusal it
            # told a submitter to fix the ``--tag`` of a job that was already
            # queued, which is both wrong and expensive to act on.
            return _unfiled_record(exc, key=key, action=action, cas=cas)
        raise SystemExit(
            f"pbrun: slurm refused this action.\n"
            f"  required tags: {tags or '(any box)'}\n"
            f"  demand:        {demand}\n"
            f"  {exc}\n"
            f"Fix the --tag, or read `sinfo -N -l` for a node that offers it."
        ) from exc
    except OSError as exc:
        return _lane_io_failure(
            exc, key=key, action=action, cas=cas,
            job_id=(str(attached[2].get("job_id") or "") if attached else
                    str(submitted_job_ids[-1]) if submitted_job_ids else ""))

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
            queue_root=queue,
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
        if slurm_lane.job_was_cache_hit(job):
            # The job started, read the receipt somebody else published, and
            # ran nothing.  Saying "executed" here would attribute that
            # somebody else's work to this job, with this job's elapsed time.
            print(f"pbrun: cache_hit -- slurm job {job.job_id} found "
                  f"{key[:12]} already in the CAS and ran nothing",
                  file=sys.stderr)
            return 0
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
    if outcome.state == "OUT_OF_MEMORY":
        # The state alone sends an operator to a job log that a SIGKILLed
        # process never got to write.  What decided the ending is the number
        # this submission declared, which the log does not carry and the
        # caller may not have typed at all -- `mem_gb` defaults to 4.
        declared = int(demand.get("mem_gb", 0) or 0)
        print(f"pbrun: slurm job {job.job_id} exceeded the "
              f"{declared} GiB it declared; raise it with "
              f"--demand mem_gb={max(declared * 2, 8)} (or whatever the "
              f"action really needs)", file=sys.stderr)
    print(f"pbrun: failed ({outcome.state}) after {total} attempt(s); "
          f"logs {job.stdout_path} and {job.stderr_path}", file=sys.stderr)
    if isinstance(outcome.exit_code, int) and outcome.exit_code:
        return reported_exit(outcome.exit_code, key=key)
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
    lane_root=None, queue_root=None, scancel: str = "scancel",
    squeue: str = "squeue", queue=None,
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
            queue_root=queue_root, scancel=scancel, squeue=squeue,
        ))
    if pool_prefixes:
        if queue is None:
            root = SH / "pb-queue" if queue_root is None else Path(queue_root)
            queue = pool.PoolQueue(root)
        rc = max(rc, withdraw_main(queue, pool_prefixes, reason=reason, by=by))
    return rc


def withdraw_slurm_main(
    prefixes, *, reason: str = "", by: str = "", lane_root=None,
    queue_root=None, scancel: str = "scancel", squeue: str = "squeue",
) -> int:
    """Cancel every job under each named action, refusing an ambiguous prefix.

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

    Every job under the key's name is cancelled, not just the one
    ``latest.json`` records.  The submitter cannot close the double-submit
    window and ``slurm_lane.submit`` says so: two ``pbrun``s that look at the
    same instant both find nothing in the CAS and both submit, and the
    controller holds the second PENDING on ``Dependency``.  Cancelling the
    recorded id alone left that sibling queued, and when the first job left the
    singleton released it -- so it ran the action the marker on disk exists to
    stop.  ``sibling_jobs`` is the listing, scoped to this user, and a
    controller that will not answer it costs the enrichment rather than the
    cancellation: the recorded id is always asked for.

    A refusal is reported per job and the verb fails only when every cancel was
    refused.  A sibling that finished between the listing and the cancel is the
    ordinary case, and the operator still got the run stopped.
    """

    rc = 0
    for prefix in prefixes:
        found = slurm_lane.resolve_recorded(str(prefix), root=lane_root)
        if not found:
            print(f"pbrun: no slurm submission matches {prefix!r}",
                  file=sys.stderr)
            rc = rc or 2
            continue
        if len(found) > 1:
            keys = ", ".join(sorted(str(r["action_key"])[:12] for r in found))
            print(f"pbrun: {prefix!r} matches {len(found)} submissions "
                  f"({keys}); name more characters", file=sys.stderr)
            rc = rc or 2
            continue
        record = found[0]
        key = str(record["action_key"])
        job_id = str(record["job_id"])
        queue = SH / "pb-queue" if queue_root is None else Path(queue_root)
        try:
            marker = _file_slurm_withdrawal(
                queue, record, reason=reason, by=by, scancel_command=scancel)
        except (OSError, slurm_lane.SlurmLaneError) as exc:
            _withdrawal_write_failure(exc, key=key, job_id=job_id, accepted=[])
            rc = RECORD_WRITE_FAILED_EXIT
            continue
        if marker is None:
            print(f"pbrun: {key[:12]} already has an outcome filed; "
                  f"nothing to withdraw", file=sys.stderr)
            continue
        accepted, refused = [], []
        for target in _jobs_to_cancel(key, job_id, squeue=squeue):
            if slurm_lane.cancel(target, scancel=scancel):
                accepted.append(target)
            else:
                refused.append(target)
        if accepted:
            try:
                _stamp_scancel_accepted(queue, record)
            except (OSError, slurm_lane.SlurmLaneError) as exc:
                _withdrawal_write_failure(
                    exc, key=key, job_id=job_id, accepted=accepted)
                rc = RECORD_WRITE_FAILED_EXIT
            why = f" -- {reason}" if reason else ""
            jobs = ", ".join(accepted)
            plural = "s" if len(accepted) > 1 else ""
            print(f"pbrun: cancelled slurm job{plural} {jobs} for {key[:12]}"
                  f" by {by or 'an operator'}{why}", file=sys.stderr)
        for target in refused:
            print(f"pbrun: scancel refused slurm job {target} for {key[:12]}; "
                  f"it may already have finished", file=sys.stderr)
        if refused and not accepted:
            rc = rc or 2
    return rc


def _withdrawal_write_failure(exc, *, key: str, job_id: str, accepted) -> None:
    stage = (f"scancel accepted cancellation for job(s) {', '.join(accepted)}, "
             "but its acceptance stamp could not be written"
             if accepted else
             "the withdrawal record could not be written; no cancellation was sent")
    print(
        f"pbrun: {stage}.\n"
        f"  slurm job: {job_id}\n"
        f"  record:    {getattr(exc, 'filename', '') or '(see reason)'}\n"
        f"  reason:    {getattr(exc, 'strerror', '') or exc}\n"
        "Restore record writes, then retry "
        f"`tools/fleet/pbrun.py --transport slurm --withdraw {key[:12]}`.",
        file=sys.stderr, flush=True,
    )


def _jobs_to_cancel(action_key: str, job_id: str, *, squeue: str) -> list[str]:
    """The recorded job and every sibling the controller still holds for it.

    The recorded id leads and is never dropped: it is the one fact that does
    not depend on the controller answering.  A ``squeue`` that fails costs the
    siblings, not the cancellation.
    """

    targets = [job_id] if job_id else []
    try:
        siblings = slurm_lane.sibling_jobs(action_key, squeue=squeue)
    except slurm_lane.SlurmLaneError:
        return targets
    for sibling, _state in siblings:
        if sibling not in targets:
            targets.append(sibling)
    return targets


def _file_slurm_withdrawal(
    queue_root: Path, submission, *, reason: str, by: str, scancel_command: str,
):
    """File the marker and the terminal record for one cancellation.

    Returns ``None`` when this generation already has an outcome filed -- the
    action finished a moment before the operator asked, which is them getting
    what they wanted rather than them mistyping, and is what
    ``PoolQueue.withdraw`` reports as ``already_finished``.

    Each directory is listed before the name in it is read.  These are the same
    NFS directories ``read_withdrawal_marker`` was written for: a lookup of a
    name that did not exist yet is negatively cached, so ``exists()`` keeps
    answering False after the ending has landed.  On a stale answer this verb
    filed a withdrawal over a run already in ``done/`` and reported work that
    succeeded as cancelled.  ``slurm_lane._read_json_object`` is the general
    form of that revalidation; ``read_withdrawal_marker`` itself is pinned to
    ``withdrawn/`` and this loop reads all three terminal directories.
    """

    key = str(submission["action_key"])
    published_unix = _submission_generation(submission)
    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        filed = queue_root / state / f"{key}.json"
        filed_record = slurm_lane._read_json_object(filed)
        if filed_record is None:
            continue
        theirs = slurm_lane._record_generation(filed_record)
        if theirs is None or theirs != published_unix:
            continue
        if state == pool.WITHDRAWN:
            # A bare marker is a withdrawal still in flight (or one whose
            # scancel never landed); only a record carrying the job's ending
            # says this generation is over.
            if "detail" not in filed_record:
                continue
            # The record this verb writes below carries ``detail`` too, and it
            # is written before ``scancel`` runs.  Until ``scancel`` accepts,
            # that record is a decision the scheduler has not honoured, not an
            # ending; a second ask must reach ``scancel`` again rather than
            # read its own first attempt as "already finished".
            if _withdrawal_still_in_flight(filed_record):
                continue
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


def _submission_generation(submission) -> float:
    """The ``published_unix`` a submission record belongs to."""

    published_unix = submission.get("published_unix")
    if isinstance(published_unix, (int, float)):
        return float(published_unix)
    # A submission record from before the generation stamp. Withdraw it, but
    # do not claim to know which request it belonged to.
    return float(submission.get("submitted_unix") or 0.0)


def _withdrawal_still_in_flight(record) -> bool:
    """Whether a ``withdrawn/`` record is one ``--withdraw`` wrote and has not
    yet seen ``scancel`` accept.

    ``cancelled_with`` is stamped by ``_file_slurm_withdrawal`` before the
    cancel is attempted; ``scancel_accepted_unix`` only after ``scancel``
    returned 0.  The submitter's own ending never writes the first, so a record
    with the first and without the second is a withdrawal whose ``scancel``
    was refused or never ran.
    """

    detail = record.get("detail")
    return (
        isinstance(detail, dict)
        and "cancelled_with" in detail
        and "scancel_accepted_unix" not in detail
    )


def _stamp_scancel_accepted(queue_root: Path, submission) -> None:
    """Record on the withdrawal that ``scancel`` took the job.

    Rewrites only a record of this generation that ``_file_slurm_withdrawal``
    wrote; the submitter's ending for the same generation is refused by
    ``publish_outcome`` while that record exists, so nothing else writes this
    file in between.
    """

    key = str(submission["action_key"])
    filed = queue_root / pool.WITHDRAWN / f"{key}.json"
    try:
        record = json.loads(filed.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    if not isinstance(record, dict):
        return
    if not slurm_lane._same_generation(filed, _submission_generation(submission)):
        return
    detail = record.get("detail")
    if not isinstance(detail, dict) or "cancelled_with" not in detail:
        return
    detail["scancel_accepted_unix"] = time.time()
    slurm_lane._write_json_atomic(filed, record)


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
        if not container_cleanup.get("complete", True) and not container_cleanup.get("deferred"):
            note.append("container cleanup unverified; claim and tokens retained")
            error = str(container_cleanup.get("error") or "").strip()
            if error:
                note.append(error)
            rc = 2
        stop_pending = result.get("stop_pending")
        if stop_pending:
            # Say that the reservation is still out, and whose it is to return.
            # A caller told "withdrawn, released 0" without this would read the
            # zero as "there was nothing to release".
            note.append(
                "release pending on "
                f"{stop_pending.get('holder_host') or result.get('host') or 'the holder'}"
                "; its worker returns the tokens when it stops the action")
            detail = str(stop_pending.get("reason") or "").strip()
            if detail:
                note.append(detail)
        signalled = result.get("signalled") or {}
        if signalled.get("signals"):
            note.append("signalled " + ", ".join(signalled["signals"]))
        elif where == "claimed":
            # Say so rather than imply the work stopped.  Cross-box that is the
            # normal case and the remote worker stops within a heartbeat, but a
            # caller who reads "withdrawn" and assumes "already dead" would be
            # wrong for those seconds.
            note.append("stop requested through the generation marker; "
                        "its worker checks it at the next heartbeat")
        if status == "already_withdrawn":
            note.insert(0, "already withdrawn")
        print(f"pbrun: withdrew {key[:12]} from {where}; " + "; ".join(note),
              file=sys.stderr)
        # An older loop may not understand durable generation decisions.
        # Its reservation remains held; upgrade through the drained rollout.
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
                      f"its support for durable cancellation is unverified; "
                      f"the reservation stays held until its worker or reaper "
                      f"completes cleanup. Upgrade through the drained fleet rollout.",
                      file=sys.stderr)
    return rc


def _profile_mode(text: str) -> str:
    """``--profile`` values, validated here so a bad one costs no submission.

    A mode may carry one option after a colon (``nsys:600`` traces the first
    ten minutes), and the whole string is sealed, so two windows are two
    actions.
    """

    try:
        return pb.parse_profile_mode(text)
    except pb.ProfileBackendUnavailable as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


# --------------------------------------------------------------------------
# Sealing an action, in two stages
#
# Everything that reads the filesystem or the environment happens once, in
# ``freeze_action_template``; turning that template into a sealed action
# happens once per action, in ``seal_action_from_template``.  An ordinary
# submission is the two in a row and nothing else, which is what
# ``test_the_ordinary_pbrun_action_is_sealed_unchanged.py`` pins.
#
# The split exists for #517.  A decomposed parent seals many children off one
# frozen source tree: re-snapshotting a mutable checkout per child would give
# the children different code closures, and cloning this file's hashing rules
# into the decomposer would give them different keys.  So the decomposer calls
# stage A once and stage B per child, through the same code an ordinary
# ``pbrun`` uses -- there is no second sealer to keep in step.
# --------------------------------------------------------------------------


def freeze_action_template(
    *,
    command: Sequence[str],
    cwd: Path,
    logical_cwd: str,
    demand: Mapping[str, int],
    placement: Mapping[str, object],
    variables: Mapping[str, str],
    determinism: str,
    retry_policy: Mapping[str, object],
    host_class: str | None,
    measurement: bool,
    transport: str,
    pool_measurement_class: bool,
    data_manifest_path: str | None,
    checkout_snapshot_max_bytes: int,
    snapshot_refs: Sequence[str],
    exclusive: bool,
    gpu_memory_gb: float | None,
    execution_timeout_s: float | None,
    progress: Mapping[str, object] | None,
    profile: object | None,
) -> dict[str, object]:
    """Read the tree and the environment once, and freeze what they say.

    Everything in here is a measurement of the submitter's box at one instant
    -- the checkout's commit and its dirty digest, the bytes of the data
    manifest, the local toolchain -- so taking it a second time for a second
    action can produce a different answer but never a better one.  What comes
    back is the half of an action body that every action sealed from this
    template shares, plus the CAS the ingestion went into.

    Placement and demand are resolved against the live fleet by the caller, so
    they arrive already decided; this reads nothing about who might run the
    work.

    ``measurement`` spells the task class and the scope at once, deliberately:
    they are the same statement, and a caller that could set them separately
    could seal a measurement with no platform to measure on.
    """

    variables = dict(variables)
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
    # This owner belongs to the template's own command, and its only job here
    # is to be part of what the stamp name is fingerprinted over.  Ownership
    # itself is settled per action in ``seal_action_from_template``, because
    # two actions sealed off one template run as two lifecycles: the Docker
    # label a shared owner would give them makes one child's cleanup remove
    # the other's live payload, and one child's ``<owner>.used`` marker blocks
    # the other's reclaim.  An unmodified command re-derives this exact digest
    # there, so an ordinary submission is unchanged.
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
    # Seal the stamp only in the private snapshot index. Publishing it in the
    # source tree creates both litter and races: another submitter can hash a
    # scratch name just as it is renamed. Unlinking the final stamp also races
    # with readers sealing the same fingerprint. No shared stamp path exists
    # now; workers still verify the same name and bytes in the materialization.
    payload = json.dumps(
        {"cwd": logical_cwd, **identity}, indent=1, sort_keys=True
    )
    cas = pb.PrismaBuildCAS(SH / "cas")
    checkout_snapshot = build_git_checkout_snapshot(
        cwd,
        stamp_name=stamp_name,
        stamp_payload=payload,
        cas=cas,
        max_bytes=checkout_snapshot_max_bytes,
        expected_identity=identity,
        snapshot_refs=list(snapshot_refs),
    )
    inputs = [checkout_snapshot["input"]]
    if data_manifest_path is not None:
        # Validated before ingestion, not after: a malformed manifest must
        # fail at the submitter, where the operator can read the reason,
        # rather than becoming an immutable CAS blob that every later reader
        # has to refuse. The bytes are ingested unchanged so the input's
        # digest is the digest of the file the operator named.
        manifest = pb.load_data_manifest(data_manifest_path)
        manifest_input, _ = cas.ingest_input(
            data_manifest_path,
            input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
        )
        inputs.append(manifest_input)
        data_manifest_summary = {
            "input": manifest_input,
            "mount_prefix": manifest["mount_prefix"],
            "entry_count": manifest["entry_count"],
            "total_bytes": manifest["total_bytes"],
        }
    else:
        data_manifest_summary = None
    execution_scope, toolchain = host_class_scope(
        host_class, measurement=measurement, transport=transport)
    if pool_measurement_class and demand.get("gpu", 0) and (
        "cuda_compute_capability" not in toolchain or "nvidia_driver" not in toolchain
    ):
        raise SystemExit("pbrun: class-scoped GPU measurement requires live accelerator "
                         "model, compute capability and driver evidence")
    params: dict[str, object] = {
        "command": list(command),
        "cwd": logical_cwd,
        "demand": demand,
        "placement": placement,
        "checkout_snapshot": checkout_snapshot,
        "retry_policy": retry_policy,
    }
    if data_manifest_summary is not None:
        # A summary, not the list: the prewarm budget and the ARC check read
        # these two numbers every poll, and making them fetch and parse a
        # 200 KB blob to learn a byte count would put the manifest on the
        # scheduler's hot path. The list itself stays in the CAS.
        params["data_manifest"] = data_manifest_summary
    if demand.get("gpu"):
        params["gpu_exclusive"] = bool(exclusive)
        if gpu_memory_gb is not None:
            params["gpu_memory_gb"] = gpu_memory_gb
    if execution_timeout_s is not None:
        params["execution_timeout_s"] = execution_timeout_s
    if progress is not None:
        # Sealed, like the profiler mode and for the same reason: an action
        # admitted under the progress contract is a different action from its
        # unbounded twin, so the store never answers one with the other's
        # receipt.  Absent, the key is byte-identical to what it was before
        # this flag existed.
        params[pb.PROGRESS_PARAM] = progress
    if profile is not None:
        # Sealed, and only when asked for.  Present, it makes a profiled run a
        # different action from its unprofiled twin, which is what stops the
        # CAS from answering a profile request with a receipt that has none.
        # Absent, the key is byte-identical to what it was before this flag
        # existed, so nothing already in the store is orphaned.
        params[pb.PROFILE_PARAM] = profile
    return {
        "cas": cas,
        "marker_root": marker_root,
        "checkout_identity": identity,
        "log_name": log_name,
        "stamp_name": stamp_name,
        "task": {
            "definition_id": "fleet/pbrun",
            "definition_version": "v1",
            "task_class": "measurement" if measurement else "generation",
            # A pytest or a timing run is not byte-reproducible and must not
            # claim to be: the CAS only enforces canonical equality on
            # "deterministic", so mislabelling one would be a false receipt.
            "determinism": determinism,
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "working_directory": ".",
        },
        "inputs": inputs,
        "code_closure": build_stamp_closure(stamp_name, payload),
        "params": params,
        "environment": {"variables": variables, "toolchain": toolchain},
        "execution_scope": execution_scope,
    }


#: The template entries that are the submitter's own handles rather than any
#: part of a sealed action: the CAS it ingested into, the marker namespace and
#: the recorded checkout identity ownership is re-derived against, and the two
#: fingerprinted names.  Everything else in a template is, by construction,
#: the half of an action that no action sealed from it varies -- which is why
#: :func:`template_action_common` subtracts rather than enumerates.  A field
#: added to the template is therefore never silently dropped from a parent's
#: identity: it either belongs to the shared half, in which case it must also
#: be named in ``decomposition._ACTION_COMMON_KEYS``, or it is a submitter
#: handle and belongs here.  Named in neither, ``validate_action_common``
#: refuses the record -- which is the right answer, because nobody has yet
#: decided which of the two it is.
_TEMPLATE_SUBMITTER_KEYS = frozenset(
    {"cas", "marker_root", "checkout_identity", "log_name", "stamp_name"}
)


def template_action_common(template: Mapping[str, object]) -> dict[str, object]:
    """The half of every action sealed from this template that none of them varies.

    A decomposition's parent has to be keyed on exactly this.  Key it on less
    -- on the producer's declared command, demand and environment alone -- and
    two campaigns that differ in placement, retry policy, timeout, profiler
    mode or the local toolchain collapse onto one parent while their children
    take different keys; the publication index then refuses the second run with
    a key mismatch that names the child rather than the cause.

    The two container variables come off, because ownership is per action by
    the time anything is sealed, and a child's own owner is a function of this
    record and its own command.
    """

    shared = {name: value for name, value in template.items()
              if name not in _TEMPLATE_SUBMITTER_KEYS}
    shared["params"] = {name: value
                        for name, value in shared["params"].items()
                        if name != "command"}
    variables = dict(shared["environment"]["variables"])
    variables.pop(CONTAINER_OWNER_ENV, None)
    variables.pop(CONTAINER_MARKER_ENV, None)
    shared["environment"] = {**shared["environment"], "variables": variables}
    return shared


def seal_action_from_template(
    template: Mapping[str, object],
    *,
    command: Sequence[str] | None = None,
    result_path: str | None = None,
    extra_inputs: Sequence[Mapping[str, object]] = (),
    extra_params: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Turn one frozen template into one sealed action.

    The command and the result path are rebuilt here rather than carried in
    the template because they are the two things a decomposed child varies:
    its own batch resolved into the argument list, its own manifest to write.
    For an ordinary submission nothing is overridden and this is the
    template's own command, teed to the template's own log.

    The four overrides are the whole of what makes a child different from an
    ordinary action, and they are deliberately narrow.  Each one lands in the
    sealed body, so a child's key is an ordinary ``pbrun`` key over a command,
    an input list and a ``params`` that say which batch of which plan it
    measured -- not a new kind of action with its own hashing rules.

    ``command`` still runs under the same wrapper and still tees to a log; the
    log just stops being the declared result, because a decomposed child's
    result is its manifest.  ``extra_inputs`` are appended after the
    template's, so the checkout snapshot stays ``inputs[0]``, which is where
    the worker's materialization looks for it.  ``extra_params`` may not
    rewrite anything the template froze: a child that could restate its own
    command, demand or snapshot would be a different action wearing a
    template's identity.

    Container ownership is settled here rather than in the template because it
    is a property of one running action, not of the source tree.  Two children
    sealed off one template are two Docker lifecycles on what may be one box:
    a shared ownership label makes the first to finish ``docker rm -f`` the
    other's live payload, and a shared ``<owner>.used`` marker makes each
    one's reclaim wait on the other's use.  So the owner is re-derived from
    this action's own command and re-injected.  With nothing overridden the
    inputs are the template's, so the digest is the template's and an ordinary
    submission seals byte for byte what it did before this split existed.
    """

    params = dict(template["params"])
    if extra_params:
        overwritten = sorted(set(extra_params) & set(params))
        if overwritten:
            raise SystemExit(
                "pbrun: refusing to seal an action whose extra params restate "
                f"the frozen template's: {', '.join(overwritten)}"
            )
        params.update(extra_params)
    command = list(template["params"]["command"] if command is None else command)
    params["command"] = command
    # Strip the two injected variables back off to recover exactly the mapping
    # the template hashed, so an unoverridden command lands on the same digest.
    variables = dict(template["environment"]["variables"])
    variables.pop(CONTAINER_OWNER_ENV, None)
    variables.pop(CONTAINER_MARKER_ENV, None)
    marker_root = template["marker_root"]
    owner = container_owner(
        command,
        params["cwd"],
        params["demand"],
        variables,
        determinism=template["task"]["determinism"],
        retry_policy=params["retry_policy"],
        marker_root=marker_root,
        # Recorded by the template, never re-read: the tree may have moved on
        # since, and every action sealed from one template must answer for the
        # tree that template froze.
        identity=template["checkout_identity"],
        logical_cwd=params["cwd"],
        placement=params["placement"],
    )
    variables[CONTAINER_OWNER_ENV] = owner
    variables[CONTAINER_MARKER_ENV] = str(marker_root / f"{owner}.used")
    log_name = str(template["log_name"])
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            **template["task"],
            "argv": [SEALED_ARGV0, "--noprofile", "--norc", "-c",
                     f"export PATH={shlex.quote(str(CONTAINER_WRAPPER_DIR))}:$PATH; "
                     f"{shlex.join(command)} 2>&1 | tee {shlex.quote(log_name)}; "
                     f"exit ${{PIPESTATUS[0]}}"],
            "result_path": log_name if result_path is None else result_path,
        },
        "inputs": [*template["inputs"], *extra_inputs],
        "code_closure": template["code_closure"],
        "params": params,
        "environment": {**template["environment"], "variables": variables},
        "execution_scope": template["execution_scope"],
    }
    try:
        return pb.seal_action(body)
    except pb.ActionContractError as exc:
        # A refused contract is the caller's to fix; nothing has been queued
        # or ingested, so say what was refused and stop.  A traceback here
        # names core.py internals for what is a submission error (issue #21).
        raise SystemExit(f"pbrun: refusing to seal the action: {exc}") from None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Read one submission's arguments, and nothing about the world.

    Split out so a caller that is not the command line -- ``pbcampaign``
    decomposing one logical request into many actions -- can build the same
    namespace the CLI builds and hand it to ``prepare_submission``.  Every
    refusal below is a statement about the arguments alone, so it holds
    wherever they came from.

    ``--progress-phase`` is parsed here rather than at its use: the policy is
    the normalized form of two flags, a typo in one of them is an argument
    error, and saying so before ``--withdraw`` is where ``main`` said it.
    """

    ap = argparse.ArgumentParser(
        description="Submit one command to the PrismaBuild fleet and wait for "
                    "it. Either the pull queue or SLURM carries it, per "
                    "--transport or the published generation's default; the "
                    "result does not depend on which."
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
    ap.add_argument("--gpu-memory-gb", type=float, default=None,
                    help="GPU memory budget in GiB; separate VRAM on discrete GPUs, "
                         "a subset of --demand mem_gb on unified-memory GPUs")
    ap.add_argument("--gpu-capacity", type=int, default=0,
                    help="slots to demand for --exclusive; 0 reads the largest "
                         "a matching box actually offers")
    ap.add_argument("--tag", action="append", default=[],
                    help="require a box offering this tag (e.g. a hardware class)")
    ap.add_argument("--measurement", action="store_true",
                    help="seal task_class=measurement (pool: verified platform, "
                         "local unless --host-class is explicit; "
                         "SLURM: --host-class required): the result is numerics "
                         "that do not transfer across architectures")
    ap.add_argument("--host-class", default=None, metavar="CLASS",
                    help="require CLASS placement (e.g. gb10). Pool measurements "
                         "opt into matching workers, with verified platform, ABI, "
                         "driver and device models; external dependencies must be "
                         "identical across the class. SLURM seals host_class_keyed "
                         "and attests its constraint through the controller")
    ap.add_argument("--anywhere", action="store_true",
                    help="assert that command/tool/data dependencies outside "
                         "the snapshot are identical on every eligible worker")
    ap.add_argument("--here", action="store_true",
                    help="pin the materialized checkout to this box")
    ap.add_argument(
        "--data-manifest",
        help="path to a data manifest naming the shared-mount bytes this "
             "action will read; attached as a second content-addressed input "
             "so a storage-role loop can make them resident before the action "
             "is claimed. It is part of the action key: the same command with "
             "a manifest is a different action from the same command without "
             "one, and from the same command with a different manifest",
    )
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
    ap.add_argument("--cwd", default=os.getcwd(),
                    help="directory the command runs in, on this box and "
                         "inside the checkout being sealed; it is recorded "
                         "in the action as a path relative to the checkout "
                         "root, so the action stays portable")
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
    ap.add_argument("--timeout-s", type=float, default=None,
                    help="positive execution deadline in seconds, enforced by "
                         "both transports; the pool worker's safety ceiling "
                         "(7200 s by default, announced per box and reported "
                         "here when it would cut this request short) also "
                         "applies. Queue waiting is bounded by --wait-s")
    ap.add_argument("--progress-phase", "--progress", action="append", default=None,
                    metavar="NAME=SECONDS",
                    help="declare one phase of this action and the quiet it is "
                         "allowed in it, in order, once per phase. Declaring "
                         "any of them admits the action under the progress "
                         "contract: it is then bounded by how long it goes "
                         "without committing work rather than by how long it "
                         "runs, the worker's ceiling clamps each phase's "
                         "allowance instead of the whole run, and --timeout-s "
                         "if given still ends it whatever it is doing. The "
                         "action reports with prismabuild.progress.commit, or "
                         "by running $PRISMABUILD_ACTION_PROGRESS_HELPER when "
                         "it cannot import PrismaBuild")
    ap.add_argument("--progress-cycle", action="store_true",
                    help="allow progress phases to repeat; each phase gets one "
                         "allowance between increases in cumulative committed "
                         "units. Requires --progress-phase and cyclic-capable workers")
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
    ap.add_argument("--priority", type=int, default=0,
                    help="a queue hint, higher runs sooner; not part of the "
                         "action's identity, so two submissions that differ "
                         "only in priority are the same action. The pull "
                         "queue sorted its ready list on it, before age; the "
                         "SLURM lane spends it as a --nice "
                         "(slurm_lane.nice_for), scaled so one priority step "
                         "outranks submission order rather than one later "
                         "submission")
    ap.add_argument("--profile", type=_profile_mode, default=None,
                    metavar="{" + ",".join(pb.PROFILE_MODES) + "}",
                    help="run a profiler around this action's child and store "
                         "the profile as a CAS blob named on the ending. "
                         "Unlike a queue hint this IS part of the action's "
                         "identity: a profiled run has its own key, so it is "
                         "never answered from an unprofiled receipt and never "
                         "an A/B arm against one. 'sample' is py-spy at "
                         f"{pb.PROFILE_SAMPLE_RATE_HZ} Hz over the whole "
                         "process tree; 'nsys' is Nsight Systems over CUDA and "
                         "NVTX, and takes a window in seconds ('nsys:600' "
                         "traces the first ten minutes and lets the action run "
                         "on); 'torch' is a contract the action opts into, "
                         "exporting its own Chrome trace to the path in "
                         f"{pb.TorchProfileBackend.OUT_ENV}. Measured overhead "
                         "per mode is in docs/operating_prismabuild.md, with "
                         "the box load it was measured under")
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
    ap.add_argument("command", nargs=argparse.REMAINDER,
                    help="the command to run, after a bare --; every word "
                         "past it belongs to the command and not to pbrun")
    args = ap.parse_args(argv)
    if args.timeout_s is not None and (
        not math.isfinite(args.timeout_s) or args.timeout_s <= 0
    ):
        raise SystemExit("pbrun: --timeout-s must be a positive finite number")
    args.progress_policy = parse_progress_phases(
        args.progress_phase, cycle=args.progress_cycle)
    # Some things an argument gets wrong can only be judged once the demand
    # is resolved -- a GPU budget on a slot that reserves no GPU is the case
    # -- and they are argument errors all the same.  So the parser that
    # produced this namespace travels with it, and ``prepare_submission``
    # reports them the way every other argument error reports: usage,
    # message, exit 2.
    args.refuse_argument = ap.error
    return args


def prepare_submission(args: argparse.Namespace) -> dict[str, object]:
    """Resolve one submission against this box, and freeze what it says.

    Everything between the arguments and the frozen template: the checkout
    gates, the default environment, the demand, the placement the fleet's
    offers decide, and the refusals each of those can raise.  None of it is
    optional for a decomposed submission -- a parent's children inherit this
    placement and this environment, so they inherit these refusals too, and
    a second copy of the block in ``pbcampaign`` would be a second answer to
    the same questions.

    The returned ``cwd`` and ``portable_checkout`` are here only because the
    submission notices report them; the template carries everything else,
    including the resolved tags and demand.
    """

    progress_policy = args.progress_policy
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
        raise SystemExit(detached_attempts_refusal(args.max_attempts))
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
            f"pbrun reads the source checkout to seal its bytes, so it can "
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
    if args.gpu_memory_gb is not None:
        try:
            adaptive_gpu.memory_budget_bytes(args.gpu_memory_gb)
        except ValueError as exc:
            args.refuse_argument(f"--gpu-memory-gb: {exc}")
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
    if not args.no_default_env:
        for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
            variables.setdefault(name, str(demand["cpu"]))

    if args.anywhere and args.here:
        raise SystemExit("--anywhere and --here contradict each other")
    if args.anywhere and args.tag:
        # The same contradiction with the second constraint spelled as a
        # class rather than as a hostname: --anywhere asserts that every
        # eligible worker can run this action, and --tag says only the boxes
        # offering that tag may.  Both landed before, and --anywhere won the
        # part the SLURM lane reads -- an action tagged x86 went to the
        # default partition as portable work.
        raise SystemExit(
            "--anywhere and --tag contradict each other: --anywhere asserts "
            "every eligible worker can run this action, and --tag admits only "
            "the boxes offering "
            f"{', '.join(sorted(set(args.tag)))}.  Drop whichever is not true."
        )
    require_host_class_scope(
        measurement=args.measurement, host_class=args.host_class,
        transport=args.transport, anywhere=args.anywhere,
    )
    pool_measurement = args.measurement and args.transport == "pool"
    pool_measurement_class = pool_measurement and args.host_class is not None
    tags = pool.normalize_placement_tags(
        placement_tags(
            cwd,
            explicit=[*args.tag, *([args.host_class] if pool_measurement_class else [])],
            here=args.here or (pool_measurement and not pool_measurement_class),
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
    if progress_policy is not None:
        # A capability, not a place: the boxes that cannot enforce this
        # action's stall policy must not be able to claim it.  A worker offer
        # says who understands the contract, but nothing consults an offer at
        # claim time; item tags are what the matcher already checks, so the
        # requirement rides them.  Sealed with the rest of the placement, so
        # the receipt says the action was admitted under the contract *and*
        # ran on a box that could keep it.
        tags = pool.normalize_placement_tags(
            [*tags, *progress_required_tags(progress_policy)])
    require_reachable_runtime(
        tags, hostname=socket.gethostname(), runtime_root=RUNTIME_ROOT)
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
    try:
        require_gpu_memory_scope(
            gpu_memory_gb=args.gpu_memory_gb, gpu=bool(demand.get("gpu")),
            transport=args.transport,
        )
    except ValueError as exc:
        args.refuse_argument(str(exc))
    try:
        require_progress_scope(
            progress=progress_policy, transport=args.transport)
    except ValueError as exc:
        args.refuse_argument(str(exc))
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

    template = freeze_action_template(
        command=command,
        cwd=cwd,
        logical_cwd=logical_cwd,
        demand=demand,
        placement=placement,
        variables=variables,
        determinism=determinism,
        retry_policy=retry_policy,
        host_class=args.host_class,
        measurement=args.measurement,
        transport=args.transport,
        pool_measurement_class=bool(pool_measurement_class),
        data_manifest_path=args.data_manifest,
        checkout_snapshot_max_bytes=args.checkout_snapshot_max_bytes,
        snapshot_refs=args.snapshot_ref,
        exclusive=args.exclusive,
        gpu_memory_gb=args.gpu_memory_gb,
        execution_timeout_s=args.timeout_s,
        progress=progress_policy,
        profile=args.profile,
    )
    return {
        "args": args,
        "template": template,
        "cwd": cwd,
        "portable_checkout": portable_checkout,
    }


def publication_row(
    action: Mapping[str, object],
    *,
    args: argparse.Namespace,
    queue,
) -> dict[str, object]:
    """The queue row that submits one sealed action.

    A queue row and the action it points at are two spellings of one
    submission, so every field that describes the work is read off the sealed
    body itself rather than off a template or a local the caller happens to
    still be holding.  That matters most for the owner: the lifecycle cleanup
    asks Docker for a label derived from it, so a row naming a different owner
    than the body would query a label nothing carries.  It matters for the
    rest because a decomposed child seals its own ``params``: reading the row
    off the body is what makes a child's row describe the child.

    Only the submitter's own handles -- priority, the attempt ceiling, retry
    safety -- come from ``args``, and they are exactly the fields no action
    body carries, because they say how hard to try rather than what to run.

    ``pbcampaign`` publishes decomposed children through this too.  The
    alternative is a second copy of the literal, which is how a row and a body
    come to disagree about one action.
    """

    params = action["params"]
    demand = params["demand"]
    variables = action["environment"]["variables"]
    row: dict[str, object] = {
        "action_key": str(action["action_key"]),
        "cas_root": str(SH / "cas"),
        "worker_script": str(RUNTIME_ROOT / "tools" / "prismabuild_worker.py"),
        "tags": params["placement"]["required_tags"],
        "needs_gpu": bool(demand.get("gpu")),
        "priority": args.priority,
        "resources": demand,
        "max_attempts": args.max_attempts,
        "container_owner": str(variables[CONTAINER_OWNER_ENV]),
        "checkout_snapshot": params["checkout_snapshot"],
    }
    # The repo checkout can advance just before the atomic runtime generation
    # rolls.  The previous PoolQueue already accepts the safety-critical bound,
    # so keep that mixed window usable; add the explanatory annotation once the
    # loaded runtime exposes it.  The sealed action params carry the full
    # contract in both cases.
    if "retry_safe" in inspect.signature(queue.publish).parameters:
        row["retry_safe"] = args.retry_safe
    return row


def main() -> int:
    args = parse_args()
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
    prepared = prepare_submission(args)
    template = prepared["template"]
    cwd = prepared["cwd"]
    portable_checkout = prepared["portable_checkout"]
    # The resolved half of the submission is the template's, not a second
    # copy kept alongside it: what was sealed is what the notices describe
    # and what the queue row is published with.
    tags = template["params"]["placement"]["required_tags"]
    demand = template["params"]["demand"]
    progress_policy = args.progress_policy
    cas = template["cas"]
    action = seal_action_from_template(template)
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
            # Built here because only ``main`` knows the checkout and the
            # flags it was asked with.  The census is unavailable rather than
            # empty: this branch builds no PoolQueue on purpose, and worker
            # offers do not describe a SLURM fleet.
            placement_notice=pin_notice(
                _NoCensus(),
                {"tags": tags, "needs_gpu": bool(demand.get("gpu")),
                 "resources": demand},
                cwd=cwd,
                hostname=socket.gethostname(),
                here=args.here,
                portable_checkout=portable_checkout,
                unknown_census=NO_CENSUS,
            ),
            tags=tags,
            demand=demand,
            exclusive=args.exclusive,
            timeout_s=args.timeout_s,
            wait_s=args.wait_s,
            retry_safe=args.retry_safe,
            max_attempts=args.max_attempts,
            priority=args.priority,
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

    # An offer can be fresh but stamped ahead of this submitter. Keep that
    # discrepancy visible, including an offer too far ahead to use at all.
    for host, skew in q.offer_clock_skews().items():
        disposition = "tolerated" if skew <= pool.OFFER_FUTURE_TOLERANCE_S else "ignored"
        print(
            f"pbrun: {host} offer announced {skew:.3f}s in the future "
            f"(clock skew; {disposition}, limit {pool.OFFER_FUTURE_TOLERANCE_S:g}s)",
            file=sys.stderr, flush=True,
        )

    # Before the placement verdicts, not after.  ``intent`` now requires
    # ``PROGRESS_TAG``, so on a fleet that offers none the generic "no recorded
    # worker can run this action" would fire first and name a tag the operator
    # never typed.  Asked of the boxes eligible on every OTHER tag, which is
    # also the honest question: of the boxes that could run this work, which
    # can keep its stall policy?
    progress_notice = progress_contract_notice(
        q,
        {**intent, "tags": [
            t for t in tags
            if t not in (pb.PROGRESS_TAG, pb.PROGRESS_HELPER_TAG, pb.PROGRESS_CYCLE_TAG)
        ]},
        policy=progress_policy,
        requested_timeout_s=args.timeout_s,
    )
    if progress_notice:
        print(progress_notice, file=sys.stderr, flush=True)

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

    ceiling_notice = timeout_ceiling_notice(
        q, intent, requested=args.timeout_s if progress_policy is None else None)
    if ceiling_notice:
        print(ceiling_notice, file=sys.stderr, flush=True)

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

    queued_path = publish_or_refuse(
        q, publication_row(action, args=args, queue=q))
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
