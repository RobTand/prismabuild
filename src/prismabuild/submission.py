"""Request preparation, immutable sealing and fleet publication."""
from __future__ import annotations
import bisect
import contextlib
import getpass
import hashlib
import inspect
import json
import functools
import os
import posixpath
import re
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
from dataclasses import replace
from pathlib import Path
from typing import NoReturn
from . import action_client, action_wait, client_channel, endings, contracts, manifest_layout
from . import (action_edges, adaptive_gpu, capability_eligibility, container_images,
    core as pb, decomposition as dc, dependency_digest, filesystem_floor, fleet_roster,
    materialize, movement_actions, pool, residency_plan, slurm_lane, storage_tiers,
    local_dependencies, _bounded_reader, d38_gate, target_evidence)
from .digest_primitives import sorted_json
from .record_store import Absent, Moving, Present, QueueRow, RecordStore, Unknown
SHARED_ROOT = Path("/mnt/shared")
from .submission_types import (FleetStore, SubmissionRequest, SubmissionRefused,
    OfferDiscoveryTimedOut, DisplaySink, discard_display, notice as emit_display, ProducerPublication)

TRANSPORTS = ("pool", "slurm")


DEFAULT_TRANSPORT_ENV = "PRISMABUILD_TRANSPORT"


RESIDENCY_MODES = ("none", "stage")


RESIDENCY_RAM_MODES = ("auto", "off")


DEFAULT_MAX_ATTEMPTS = 1


RECORDED_OFFER_MAX_AGE_S = float("inf")


SUBMISSION_OFFER_READ_TIMEOUT_S = 5.0


ATTACHMENT_READ_TIMEOUT_S = 5.0


CHECKOUT_SNAPSHOT_MAX_BYTES = 512 * 1024 * 1024


CONTENT_TRANSFORM_ATTRIBUTES = frozenset(
    {"crlf", "eol", "filter", "ident", "text", "working-tree-encoding"}
)


def require_checkout_snapshot_limit(max_bytes: int) -> int:
    """Validate a caller's lowering of the fleet-wide checkout disk bound."""

    if type(max_bytes) is not int or max_bytes < 1:
        raise SubmissionRefused("pbrun: checkout snapshot byte limit must be positive")
    if max_bytes > CHECKOUT_SNAPSHOT_MAX_BYTES:
        raise SubmissionRefused(
            "pbrun: checkout snapshot byte limit exceeds the hard fleet "
            f"ceiling of {CHECKOUT_SNAPSHOT_MAX_BYTES} bytes; the per-action "
            "flag may only lower this unaccounted local-disk bound"
        )
    return max_bytes


def published_commit(*, store: FleetStore) -> str:
    """The commit whose bytes are currently published, or "" if unknown."""

    try:
        return str(json.loads(store.runtime_version.read_text()).get("commit") or "")
    except (OSError, ValueError):
        return ""


STAMP_PREFIX = getattr(pb, "PBRUN_STAMP_PREFIX", ".pbrun-closure.")


RESULT_PREFIX = getattr(pb, "PBRUN_RESULT_PREFIX", "pbrun_result.")


CONTAINER_OWNER_ENV = pool.CONTAINER_OWNER_ENV


CONTAINER_MARKER_ENV = pool.CONTAINER_MARKER_ENV


def _git_identity(cwd: Path) -> dict[str, str]:
    """Commit plus a digest of the working-tree delta, or a named refusal."""

    try:
        return pb.git_checkout_identity(cwd)
    except pb.ActionContractError as exc:
        raise SubmissionRefused(f"pbrun: cannot identify checkout: {exc}") from None


def git_repository_root(cwd: Path) -> Path | None:
    """Return the worktree root, or ``None`` when it cannot be snapshotted."""

    try:
        completed = pb._git_run(cwd, "rev-parse", "--show-toplevel", timeout=30)
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


def tested_repository_name(cwd: Path) -> str:
    """The repository a sealed checkout belongs to, or ``"unknown"`` (#1565).

    A linked worktree names its main repository, not its own directory: the
    checkout's Git common dir places the repository the tree belongs to, in
    the same ``rev-parse`` style :func:`git_repository_root` already uses --
    no new mechanism.  A normal ``.git`` common dir names the working tree
    that owns it (the checkout itself for a plain clone); a bare common dir
    named ``X.git`` names ``X``; an exotic gitdir layout falls back to the
    sealed root's own basename.  A checkout Git cannot place -- a plain
    directory, an unreadable ``.git`` -- is ``"unknown"`` explicitly: never
    blank, and never guessed from host, interpreter or parent process.
    """
    root = git_repository_root(cwd)
    if root is None:
        return pool.TESTED_REPOSITORY_UNKNOWN
    try:
        completed = pb._git_run(
            root, "rev-parse", "--git-common-dir", timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return pool.TESTED_REPOSITORY_UNKNOWN
    if completed.returncode != 0:
        return pool.TESTED_REPOSITORY_UNKNOWN
    text = completed.stdout.strip()
    if not text:
        return pool.TESTED_REPOSITORY_UNKNOWN
    common = Path(text)
    if not common.is_absolute():
        # Git spells a co-located common dir relative (".git").
        common = root / common
    name = common.name
    if name == ".git":
        candidate = common.parent.name
    elif name.endswith(".git"):
        candidate = name[: -len(".git")]
    else:
        candidate = root.name
    candidate = candidate.strip()
    if not candidate:
        return pool.TESTED_REPOSITORY_UNKNOWN
    return candidate


def _snapshot_fail(
    message: str, *, cause: BaseException | None = None,
) -> NoReturn:
    """Map a checked snapshot failure, retaining an explicit transport cause."""
    refusal = SubmissionRefused(f"pbrun: cannot snapshot checkout: {message}")
    if cause is not None:
        raise refusal from cause
    raise refusal


def _snapshot_git(
    cwd: Path,
    argv: list[str],
    *,
    environment: dict[str, str] | None = None,
    input_text: str | None = None,
    accepted_returncodes: tuple[int, ...] = (0,),
    strip: bool = True,
) -> str:
    caller_exception = sys.exception()

    def fail_snapshot_git(message: str) -> NoReturn:
        # The canonical runner calls fail inside its transport handler. Do not
        # promote an exception already active in our caller on a nonzero exit.
        cause = sys.exception()
        if cause is caller_exception or not isinstance(
            cause, (OSError, subprocess.TimeoutExpired),
        ):
            cause = None
        _snapshot_fail(message, cause=cause)

    output = pb._git(
        cwd, *argv,
        env=environment,
        input_text=input_text,
        errors="surrogateescape",
        timeout=120,
        accepted_returncodes=accepted_returncodes,
        fail=fail_snapshot_git,
    )
    return output.strip() if strip else output


PERSONAL_EXCLUDES_PIN: tuple[str, ...] = ("-c", "core.excludesFile=/dev/null")


def snapshot_path_roster(root: Path) -> list[str]:
    """Tracked plus nonignored-untracked paths, each counted once."""

    raw_paths = _snapshot_git(
        root,
        [*PERSONAL_EXCLUDES_PIN, "ls-files", "-co", "--exclude-standard", "-z"],
        strip=False,
    )
    return list(dict.fromkeys(path for path in raw_paths.split("\0") if path))


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
            raise SubmissionRefused(
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
            raise SubmissionRefused(
                f"pbrun: cannot inspect checkout path {relative!r}: {exc}"
            ) from exc
        if stat.S_ISREG(metadata.st_mode):
            size = metadata.st_size
        elif stat.S_ISLNK(metadata.st_mode):
            try:
                size = len(os.fsencode(os.readlink(candidate)))
            except OSError as exc:
                raise SubmissionRefused(
                    f"pbrun: cannot read checkout symlink {relative!r}: {exc}"
                ) from exc
        elif stat.S_ISDIR(metadata.st_mode):
            # ``ls-files`` emits a directory path only for a gitlink. The
            # authoritative tree pass below refuses mode 160000 by name.
            continue
        else:
            raise SubmissionRefused(
                "pbrun: checkout path has an unsupported file type: "
                f"{relative!r}"
            )
        # Per path, deliberately: hard-linked files become separate checkout
        # files and therefore consume their logical size more than once.
        logical_bytes += size
    if logical_bytes > max_bytes:
        raise SubmissionRefused(
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
        raise SubmissionRefused(
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
        raise SubmissionRefused("pbrun: Git returned malformed content attributes")
    for index in range(0, len(attributes), 3):
        path, name, value = attributes[index:index + 3]
        if (
            name in CONTENT_TRANSFORM_ATTRIBUTES
            and value not in {"unset", "unspecified"}
        ):
            raise SubmissionRefused(
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
        raise SubmissionRefused(
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
