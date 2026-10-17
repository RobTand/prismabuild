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
            raise SubmissionRefused("pbrun: Git returned a malformed snapshot tree")
        mode, kind, object_id, size = fields
        if mode == "160000" or kind == "commit":
            raise SubmissionRefused(
                "pbrun: checkout snapshot refuses gitlink/submodule path "
                f"{path!r}; the parent Git bundle does not carry its working bytes"
            )
        if kind != "blob" or not size.isdigit():
            raise SubmissionRefused(
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
                raise SubmissionRefused(
                    "pbrun: checkout snapshot symlink points outside the "
                    f"sealed repository: {path!r} -> {target!r}"
                )
            symlinks[path] = target
        # Count each materialized pathname, not unique object ids: two paths
        # naming one blob occupy two files in the worker checkout.
        logical_bytes += int(size)
    require_contained_snapshot_links(symlinks)
    if logical_bytes > max_bytes:
        raise SubmissionRefused(
            "pbrun: logical checkout tree is "
            f"{logical_bytes} bytes, above the {max_bytes}-byte safety limit; "
            "remove generated data from the worktree or lower its footprint"
        )
    return logical_bytes


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
    raise SubmissionRefused(
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
        raise SubmissionRefused(
            "pbrun: this checkout is a shallow clone, so its snapshot cannot "
            "carry the ancestry a worker needs; unshallow it "
            "(git fetch --unshallow) before submitting"
        )
    partial = _snapshot_git(
        root,
        ["config", "--get", "extensions.partialclone"],
        accepted_returncodes=(0, 1),
    )
    # Modern filtered clones mark their promisor remote, without the legacy
    # extensions.partialclone key. Read Git-normalized booleans so an explicit
    # false marker does not turn an ordinary complete source into a refusal.
    promisors = _snapshot_git(
        root, ["config", "--bool", "--get-regexp", r"^remote\..*\.promisor$"],
        accepted_returncodes=(0, 1),
    )
    filters = _snapshot_git(
        root, ["config", "--get-regexp", r"^remote\..*\.partialclonefilter$"],
        accepted_returncodes=(0, 1),
    )
    if partial or filters or any(line.rsplit(" ", 1)[-1] == "true"
                                for line in promisors.splitlines()):
        raise SubmissionRefused(
            "pbrun: this checkout is a partial clone, so its snapshot cannot "
            "prove all ancestry and objects a worker needs. Use a fresh full "
            "unfiltered clone (git clone --no-filter URL NEW; no --depth), or "
            "hydrate a separate complete checkout from its trusted remote. "
            "git repack does not fetch missing objects; do not remove "
            "promisor/filter markers to hide an incomplete source before submitting"
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
            raise SubmissionRefused(
                f"pbrun: --snapshot-ref {name} was requested twice"
            )
        try:
            # The same rule a worker will apply to the queued record, applied
            # where the person who typed it is still watching.
            pb.validate_pbrun_snapshot_ref_name(
                name, where=f"--snapshot-ref {name}"
            )
        except pb.ActionContractError as exc:
            raise SubmissionRefused(f"pbrun: {exc}") from exc
        checked = _snapshot_git(
            root,
            ["check-ref-format", "--branch", name],
            accepted_returncodes=(0, 1, 128),
        )
        # ``check-ref-format --branch`` also expands ``@{-1}``, so a name it
        # rewrites is a name that means something else on a worker.
        if checked != name:
            raise SubmissionRefused(
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
            raise SubmissionRefused(
                f"pbrun: --snapshot-ref {name} names no branch in this "
                "checkout; the snapshot can only advertise refs the source has"
            )
        resolved[name] = oid
    return resolved


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


BUNDLE_PACK_OPTIONS: tuple[str, ...] = (
    "--stdout",
    "--thin",
    "--delta-base-offset",
    "--all-progress-implied",
    "--quiet",
    "--no-reuse-delta",
    "--no-reuse-object",
)


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
        _snapshot_fail(f"Git {' '.join(argv[1:])} failed: {exc}", cause=exc)
    if completed.returncode != 0:
        detail = (completed.stderr or b"").decode("utf-8", "replace").strip()
        _snapshot_fail(
            f"Git {' '.join(argv[1:])} failed: {detail or completed.returncode}"
        )


def build_stamp_closure(stamp_name: str, payload: str) -> dict[str, object]:
    """Describe the UTF-8 bytes injected into the snapshot's private index."""
    raw = payload.encode("utf-8")
    body = {"schema": pb.CODE_CLOSURE_SCHEMA_V1,
            "files": [{"path": stamp_name, "sha256": hashlib.sha256(raw).hexdigest(),
                       "bytes": len(raw)}]}
    return pb.validate_code_closure(
        {**body, "closure_sha256": pb.canonical_sha256(body)})


def snapshot_checkout(
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
            ``None`` for a producer that seals its own action body. A stamp
            requires both a plain basename and its payload.
        stamp_payload: UTF-8 stamp contents injected into the private Git
            index with regular-file mode, without reading or writing a stamp
            in the submitting worktree. Omit along with ``stamp_name`` to seal
            only the checkout.
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
        raise SubmissionRefused("pbrun: a non-Git checkout cannot be materialized")
    require_checkout_snapshot_limit(max_bytes)
    if (stamp_name is None) != (stamp_payload is None):
        raise SubmissionRefused("pbrun: stamp name and payload must be supplied together")
    subdirectory = cwd.relative_to(root).as_posix() or "."
    stamp_relative = None
    if stamp_payload is not None:
        if (not stamp_name or Path(stamp_name).name != stamp_name
                or stamp_name in {".", ".."}):
            raise SubmissionRefused("pbrun: overlay stamp name must be a plain basename")
        stamp_relative = (Path(subdirectory) / stamp_name).as_posix()

    # Source identity itself may traverse historic blobs. Refuse incomplete
    # sources before that walk, not merely before the eventual pack command.
    require_materialized_checkout(root)
    require_complete_history(root)
    paths = snapshot_path_roster(root)
    working_bytes = require_working_tree_size(root, paths, max_bytes=max_bytes)
    if stamp_payload is not None:
        overlay_bytes = len(stamp_payload.encode("utf-8"))
        if working_bytes + overlay_bytes > max_bytes:
            raise SubmissionRefused("pbrun: working tree plus closure stamp exceeds "
                             "checkout snapshot size limit")
    require_untransformed_checkout(
        root, paths + ([stamp_relative] if stamp_payload is not None else []))
    identity = expected_identity or _git_identity(cwd)
    if _git_identity(cwd) != identity:
        raise SubmissionRefused("pbrun: checkout changed before it could be snapshotted")
    parent = identity["head"]
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
            raise SubmissionRefused(
                "pbrun: checkout snapshot is "
                f"{size} bytes, above the {max_bytes}-byte safety limit; "
                "remove generated data from the worktree or lower its footprint"
            )
        if _git_identity(cwd) != identity:
            raise SubmissionRefused("pbrun: checkout changed while it was snapshotted")
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
        raise SubmissionRefused(
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
        raise SubmissionRefused(
            "pbrun: portable execution refuses a submitter repository path "
            "(a submitter checkout path):\n"
            f"{rendered}\n"
            "Use paths relative to --cwd, move external data outside the "
            "checkout, or ingest external data as a declared CAS input."
        )


_FLEET_DEMAND_KINDS = frozenset({"cpu", "gpu", "mem_gb", "disk_metadata"})


_SPOOL_WINDOW_KIND = "spool_gb"


_SPOOL_WINDOW_ENV = "PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW"


_SPOOL_MAX_ENV = "PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES"


def normalize_spool_declaration(variables: dict[str, str]) -> None:
    """Seal a declared spool bound's effective window switch before sealing (#905).

    A new submission that declares a spool bound
    (``PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES``) is accounted by default:
    unless the environment already says so, the effective switch
    ``PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW`` is set to ``"1"`` **before**
    the ownership and stamp fingerprints are taken, so the sealed action's
    identity carries the accounting its claim charges.  ``HOST_WINDOW=0``
    beside a declared bound is a contradictory declaration and is refused by
    name: a declared spool that is not charged would silently evade the host
    ledger.  No bound declares nothing, so an unchanged request without a
    spool bound is byte-for-byte what it was.

    ``produced_spool.host_window_terms`` itself stays switch-gated: an
    already-sealed request keeps its meaning, explicit ``0`` included.  This
    normalization is only ever applied while a new submission is being
    sealed.
    """

    if _SPOOL_MAX_ENV not in variables:
        return
    switch = variables.get(_SPOOL_WINDOW_ENV, "")
    if switch == "0":
        raise SubmissionRefused(
            f"pbrun: {_SPOOL_MAX_ENV} declares a spool bound, but "
            f"{_SPOOL_WINDOW_ENV}=0 would not charge it to the host "
            f"ledger's {_SPOOL_WINDOW_KIND}; a declared spool is accounted "
            f"by default (#905). Drop the bound to declare no spool, or "
            f"drop {_SPOOL_WINDOW_ENV}=0.")
    if switch in ("", "1"):
        variables[_SPOOL_WINDOW_ENV] = "1"


def spool_window_terms(variables: dict[str, str], *, transport: str) -> dict[str, int]:
    """The host demand a sealed environment's spool window derives, or ``{}``.

    A declared spool bound is accounted by default (#905):
    :func:`normalize_spool_declaration` has sealed the effective switch in
    ``variables``, so the action's identity carries the accounting the claim
    charges and the producer's own claim check asks for the reservation.

    The window is a ``spool_gb`` reservation charged through the host ledger
    at claim, which only the pull queue holds: a SLURM allocation has no host
    ledger to charge it to.  Off -- neither the switch nor a declared bound
    -- nothing is imported or read, and every submission seals exactly as
    before.
    """

    if _SPOOL_WINDOW_ENV not in variables and _SPOOL_MAX_ENV not in variables:
        return {}     # the undeclared path imports and reads nothing new
    normalize_spool_declaration(variables)
    from prismabuild import produced_spool

    try:
        terms = produced_spool.host_window_terms(variables)
    except produced_spool.SpoolError as exc:
        raise SubmissionRefused(f"pbrun: {exc}") from None
    if terms and transport != "pool":
        raise SubmissionRefused(
            f"pbrun: {_SPOOL_WINDOW_ENV}=1 needs the pull queue: the spool "
            f"window is a host-ledger reservation, and --transport {transport} "
            "cannot hold a host spool")
    return terms


_SCRATCH_PAIRS_ENV = "PRISMABUILD_LOCAL_SCRATCH_PAIRS"


_SCRATCH_IO_ENV = "PRISMABUILD_LOCAL_SCRATCH_IO"


def scratch_io_intent(variables, *, transport):
    if _SCRATCH_IO_ENV not in variables:
        return None
    from prismabuild import local_scratch
    try:
        return local_scratch.io_intent(variables, transport=transport)
    except local_scratch.LocalScratchError as exc:
        raise SubmissionRefused(f"pbrun: {exc}") from None


def scratch_window_terms(variables: Mapping[str, str], *, transport: str) -> dict[str, int]:
    """The host demand a sealed environment's scratch pairs derive, or ``{}``.

    Off -- no ``PRISMABUILD_LOCAL_SCRATCH_PAIRS``, or an empty one -- nothing
    is imported or read.  On, each declared ``ROOT_ENV:MAX_ENV`` pair charges
    its ceiling to the box's local-disk kind at claim, which only the pull
    queue holds.
    """

    scratch_io_intent(variables, transport=transport)
    if not variables.get(_SCRATCH_PAIRS_ENV):
        return {}     # the undeclared path imports and reads nothing new
    from prismabuild import local_scratch

    try:
        terms = local_scratch.scratch_terms(variables)
    except local_scratch.LocalScratchError as exc:
        raise SubmissionRefused(f"pbrun: {exc}") from None
    if terms and transport != "pool":
        raise SubmissionRefused(
            f"pbrun: {_SCRATCH_PAIRS_ENV} needs the pull queue: bounded local "
            f"scratch is a host-ledger reservation, and --transport {transport} "
            "cannot hold one")
    return terms


def local_disk_terms(variables: dict[str, str], *, transport: str) -> dict[str, int]:
    """The one local-disk demand: the spool window plus declared scratch.

    Both draw from the ``spool_gb`` kind a box declares with ``--spool-gb``,
    because they share the box's disk (#747, #911).  A declared spool bound
    is normalised to an accounted window here (#905), so the sum is the one
    reservation the claim takes.  With neither declared this is ``{}``.
    """

    total = sum(terms.get(_SPOOL_WINDOW_KIND, 0) for terms in (
        spool_window_terms(variables, transport=transport),
        scratch_window_terms(variables, transport=transport)))
    return {_SPOOL_WINDOW_KIND: total} if total else {}


def validate_fleet_demand(demand: Mapping[str, object]) -> None:
    """Refuse a ``pbrun`` resource that no live worker offer can hold.

    The generic pool ledger intentionally remains open to producer-specific
    resources. ``pbrun`` is the fleet-command client, though, and both its
    live pool offers and SLURM translation have this closed vocabulary.
    """

    if "" in demand:
        raise SubmissionRefused("--demand resource name cannot be empty")
    if _SPOOL_WINDOW_KIND in demand:
        # Named apart from the generic refusal because it is a host kind a
        # worker can offer, just never a typed one (#747).
        raise SubmissionRefused(
            f"--demand must not name {_SPOOL_WINDOW_KIND!r}: it is derived from "
            f"the sealed {_SPOOL_WINDOW_ENV}=1 and {_SPOOL_MAX_ENV} environment "
            f"and the pairs {_SCRATCH_PAIRS_ENV} names")
    unsupported = sorted(set(demand) - _FLEET_DEMAND_KINDS)
    if unsupported:
        rendered = ", ".join(repr(kind) for kind in unsupported)
        accepted = ", ".join(sorted(_FLEET_DEMAND_KINDS))
        raise SubmissionRefused(
            f"--demand has unsupported resource {rendered}; accepted resources are {accepted}"
        )


def _parse_demand(text: str) -> dict[str, int]:
    demand: dict[str, int] = {}
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise SubmissionRefused(f"--demand wants k=v pairs, got {part!r}")
        key, _, value = part.partition("=")
        key = key.strip()
        if not key:
            raise SubmissionRefused("--demand resource name cannot be empty")
        try:
            demand[key] = int(value)
        except ValueError:
            raise SubmissionRefused(
                f"--demand resource {key!r} needs an integer count, got {value!r}"
            ) from None
    validate_fleet_demand(demand)
    return demand


class _OfferSnapshot(pool.PoolQueue):
    """One bounded, read-only worker-offer discovery for submission advice.

    The child reads records only.  The parent deliberately applies offer age
    and future-skew rules every time it asks a verdict, after that full scan
    has completed.  A snapshot therefore cannot turn a slow scan into a fresh
    offer, and retained capability keeps its existing infinite-age meaning.
    """

    def __init__(self, root, records) -> None:
        super().__init__(root)
        self._records = list(records)

    def _offer_records(self):
        return list(self._records)


def bounded_offer_snapshot(queue) -> _OfferSnapshot:
    """Read worker offers once in an abandonable child, or refuse before READY.

    Queue publication, claims and waits stay on the real ``PoolQueue``.  This
    boundary covers only the advisory/capability offer scan which pbrun makes
    before it can publish runnable work; request/CAS and later queue I/O are
    still synchronous shared-filesystem operations.
    """

    abandoned: list[dict[str, object]] = []
    result = _bounded_reader.bounded(
        "worker-offers", queue._offer_records,
        deadline=_bounded_reader.Deadline(SUBMISSION_OFFER_READ_TIMEOUT_S),
        abandoned=abandoned,
    )
    if result.get("status") == "ok":
        value = result.get("value")
        if isinstance(value, list):
            return _OfferSnapshot(queue.root, value)
        reason = "reader returned an invalid snapshot"
    elif result.get("status") == "timed_out":
        reason = (f"timed out after {result.get('elapsed_s', SUBMISSION_OFFER_READ_TIMEOUT_S)}s")
    else:
        reason = f"failed: {result.get('type', 'RuntimeError')}: {result.get('error', '')}"
    retained = f" retained reader={json.dumps(abandoned, sort_keys=True)}" if abandoned else ""
    message = ("pbrun: worker-offer discovery " + reason + "; refusing submission; "
               "no runnable submission was published." + retained)
    if result.get("status") == "timed_out" and not abandoned:
        raise OfferDiscoveryTimedOut(message)
    raise SubmissionRefused(message)


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
        if not capability_eligibility.HostPresence.from_offer(offer).matches(wanted, needs_gpu=True):
            continue
        capacity = offer.get("capacity") or {}
        best = max(best, int(capacity.get("gpu", 0)))
    if best <= 0:
        raise SubmissionRefused(
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
    container_images=(),
) -> str:
    """The submitter's ownership hash: git identity stays with pbrun.

    The construction lives in ``prismabuild.movement_actions.container_owner``
    (the one implementation, shared with the produced-output writer lane);
    this wrapper injects pbrun's ``_git_identity`` so a template without an
    explicit identity keeps its exact historical digest.
    """

    return movement_actions.container_owner(
        command, cwd, demand, variables,
        determinism=determinism, retry_policy=retry_policy,
        marker_root=marker_root, identity=identity,
        logical_cwd=logical_cwd, placement=placement,
        container_images=container_images, identity_fn=_git_identity)


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
        raise SubmissionRefused(f"pbrun: {exc}") from exc
    try:
        out = pb._git_run(cwd, "rev-parse", "--git-common-dir", timeout=30)
    except (OSError, subprocess.SubprocessError) as exc:
        raise SubmissionRefused(
            f"pbrun: cannot inspect local Git excludes: {exc}"
        ) from exc
    if out.returncode != 0:
        if marker is not None:
            detail = (out.stderr or out.stdout).strip()
            raise SubmissionRefused(
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
        raise SubmissionRefused(
            f"pbrun: cannot update pbrun Git excludes: {exc}"
        ) from exc


def command_dependency_contract(
    cwd: Path, command: list[str], *, repository_root: Path,
    environment: dict[str, str] | None, caller_environment: dict[str, str] | None,
) -> tuple[list[str], dict[str, str], dict[str, str]]:
    """Bind the invocation and its direct path requirements in one resolution."""
    cwd = cwd.resolve()
    root = repository_root.resolve()

    def resolved_path(candidate: Path) -> Path:
        try:
            return candidate.resolve(strict=False)
        except (OSError, RuntimeError) as exc:
            raise SubmissionRefused(f"pbrun: cannot resolve declared path {candidate}: {exc}") from exc

    def stays_within(candidate: Path, owner: Path) -> bool:
        try:
            parts = candidate.relative_to(owner).parts
        except ValueError:
            return False
        depth = 0
        for part in parts:
            depth += -1 if part == ".." else 1
            if depth < 0:
                return False
        return True

    def portable(candidate: Path) -> bool:
        return stays_within(candidate, root) or stays_within(candidate, SHARED_ROOT.resolve())

    def path_scope(candidate: Path) -> tuple[bool, bool]:
        location = resolved_path(candidate.parent) / candidate.name
        target = resolved_path(candidate)
        local = not portable(candidate) or not portable(location) or not portable(target)
        checkout_local = (stays_within(candidate, root) and stays_within(location, root)
                          and stays_within(target, root))
        return local, checkout_local

    if not command or not command[0]:
        raise SubmissionRefused("pbrun: portable placement requires argv[0]")
    raw = command[0]
    if os.sep in raw:
        executable_path = os.path.join(str(cwd), raw)
        if not os.path.isfile(executable_path) or not os.access(executable_path, os.X_OK):
            raise SubmissionRefused(
                "pbrun: command executable is absent or not executable "
                f"on the submitting box: {raw!r}. Pass --tag for the "
                "worker class that owns it, or --anywhere to assert an "
                "identical executable contract on every eligible worker.")
        executable = Path(executable_path)
    else:
        declared_path = (environment or {}).get("PATH") or os.defpath
        search = [str(cwd / entry) for entry in declared_path.split(os.pathsep)]
        found = shutil.which(raw, path=os.pathsep.join(search))
        if found is None:
            raise SubmissionRefused(
                "pbrun: command executable cannot be resolved from the "
                f"declared PATH: {raw!r}. Pass --tag for the worker class "
                "that owns it, or --anywhere to assert an identical "
                "executable contract on every eligible worker.")
        executable = Path(found)
    # Traverse directory symlinks and .. before making a relocatable command.
    # relpath/abspath would cancel .. lexically and could select another file.
    # Keep the leaf: dereferencing a venv Python loses the venv invocation.
    executable = resolved_path(executable.parent) / executable.name
    # Executables and inputs need evidence for a retained local alias.
    # A portable target does not make that alias available to the worker.
    requirements = {str(executable): "executable"} if path_scope(executable)[0] else {}
    bindings: dict[str, str | None] = {}

    def bind_input(raw: str) -> str:
        if not (os.path.isabs(raw) or os.sep in raw or os.path.exists(os.path.join(str(cwd), raw))):
            return raw
        if raw not in bindings:
            path = os.path.join(str(cwd), raw)
            local, checkout_local = path_scope(Path(path))
            if local:
                if not os.path.exists(path):
                    raise SubmissionRefused(
                        "pbrun: direct argv or caller environment names an "
                        "external path absent from the submitting box: "
                        f"{raw}. Pass --tag for the worker class that owns "
                        "it, or --anywhere to assert its portability.")
                requirements.setdefault(path, "path")
            bindings[raw] = (path if not os.path.isabs(raw) and not checkout_local else None)
        bound = bindings[raw]
        return raw if bound is None else bound

    arguments = []
    for token in command[1:]:
        if token.startswith("-"):
            if "=" not in token:
                arguments.append(token)
                continue
            option, raw = token.split("=", 1)
            arguments.append(option + "=" + bind_input(raw))
        else:
            arguments.append(bind_input(token))
    bound_caller = dict(caller_environment or {})
    for name, value in bound_caller.items():
        if name != "PATH":
            bound_caller[name] = os.pathsep.join(bind_input(raw) for raw in value.split(os.pathsep))

    invocation = (os.path.relpath(executable, cwd)
                  if executable.is_relative_to(root) else str(executable))
    if not os.path.isabs(invocation) and os.sep not in invocation:
        invocation = "./" + invocation
    return [invocation, *arguments], local_dependencies.normalize(requirements), bound_caller


def placement_contract(
    cwd: Path, *, explicit: list[str], here: bool, hostname: str,
    portable_checkout: bool = False, command: list[str] | None = None,
    repository_root: Path | None = None, environment: dict[str, str] | None = None,
    caller_environment: dict[str, str] | None = None, anywhere: bool = False,
    needs_gpu: bool = False, offer_queue=None,
store: FleetStore, display: DisplaySink = discard_display) -> tuple[list[str], dict[str, str], list[str] | None, dict[str, str]]:
    """Return placement, dependency questions, command, and caller environment."""
    bound_caller = dict(caller_environment or {})
    if explicit:
        return (([*dict.fromkeys(t for t in explicit if t != hostname), hostname]
                 if here else list(explicit)), {}, command, bound_caller)
    if here:
        return [hostname], {}, command, bound_caller
    if anywhere:
        return [], {}, command, bound_caller
    if not portable_checkout:
        return ([hostname] if is_box_local(cwd) else []), {}, command, bound_caller
    if command is None:
        return [], {}, command, bound_caller
    command, requirements, bound_caller = command_dependency_contract(
        cwd, command, repository_root=repository_root or cwd,
        environment=environment, caller_environment=caller_environment)
    if offer_queue is not None:
        try:
            roster = fleet_roster.Roster.read(store.roster_path)
            members = fleet_roster.class_members(roster.document, "gb10")
        except (OSError, ValueError, TypeError) as exc:
            emit_display(display, f"pbrun: keeping {hostname} placement: class inventory unavailable: {exc}",
                  channel="stderr", flush=True)
            return [hostname], requirements, command, bound_caller
        aliases = {alias for names in members.values() for alias in names}
        if hostname in aliases or (hostname == "celestia" and needs_gpu):
            reason = offer_queue().class_dependency_gap("gb10", members, requirements)
            if reason is None:
                return ["gb10", *([local_dependencies.TAG] if requirements else [])], requirements, command, bound_caller
            emit_display(display, f"pbrun: keeping host pin {hostname}: gb10 dependencies not proven: {reason}",
                  channel="stderr", flush=True)
            return [hostname], requirements, command, bound_caller
    return ([hostname] if requirements else []), requirements, command, bound_caller


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

    ``placement_contract`` cannot see this and should not: it screens argv and the
    caller's environment, which are the submitter's inputs, not ``pbrun``'s own
    installation.  So an explicit ``--tag`` -- which by design outranks every
    pin ``placement_contract`` derives -- sends the action to a box where the
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
    raise SubmissionRefused(
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
        raise SubmissionRefused(
            "pbrun: executable script bytes are outside the snapshotted "
            "repository:\n"
            f"{rendered}\n"
            "Move each helper under the repository so its bytes are bound by "
            "the action's code closure. Shell-indirected helpers must follow "
            "the same rule."
        )


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
            raise SubmissionRefused("pbrun: --progress-cycle requires --progress-phase")
        return None
    phases = []
    for entry in declared:
        name, sep, seconds = str(entry).partition("=")
        if not sep:
            raise SubmissionRefused(
                f"pbrun: --progress-phase {entry!r} must be NAME=SECONDS")
        try:
            grace = float(seconds)
        except ValueError:
            raise SubmissionRefused(
                f"pbrun: --progress-phase {entry!r} has a non-numeric allowance"
            ) from None
        phases.append({"name": name, "grace_s": grace})
    policy = {"schema": pb.PROGRESS_POLICY_SCHEMA_V1, "phases": phases}
    if cycle:
        policy["cycle"] = True
    try:
        return pb.validate_progress_policy(policy)
    except pb.ActionContractError as exc:
        raise SubmissionRefused(f"pbrun: {exc}") from None


def parse_awaited_batch(declared: str | None) -> dict[str, object] | None:
    """Turn ``--awaited-batch PARENT:PLAN:STATE`` into a sealed declaration (#1666).

    The first two halves are 64-hex action identities: the parent the
    campaign cut and the plan that fixed the children's membership. The
    third names the controller-state directory whose accepted members
    the worker reads under an exact shape check. Refused at the
    terminal, before anything seals, like the progress phases beside it.
    """

    if declared is None:
        return None
    parts = str(declared).split(":")
    if len(parts) != 3 or not all(part.strip() for part in parts):
        raise SubmissionRefused(
            "pbrun: --awaited-batch must be PARENT:PLAN:STATE, two 64-hex "
            "keys and the controller-state directory")
    parent, plan, state = (part.strip() for part in parts)
    try:
        return pb.validate_awaited_batch({
            "schema": pb.AWAITED_BATCH_SCHEMA_V1,
            "parent_key": parent, "plan_key": plan,
            "controller_state": state})
    except pb.ActionContractError as exc:
        raise SubmissionRefused(f"pbrun: {exc}") from None


def container_image_refusal(images: Sequence[object]) -> str | None:
    """Why ``--container-image`` refuses this list, or ``None``.

    Shared with ``pbcampaign``'s row validation so a manifest is refused at
    load time in the words the flag would use at submit time.
    """

    try:
        container_images.normalize_refs(images)
    except ValueError as exc:
        return str(exc)
    return None


def require_container_image_scope(*, images: Sequence[str], transport: str) -> None:
    """Refuse image declarations the named transport cannot enforce.

    The check is a pull-queue claim decision over worker offers; the SLURM
    lane has no worker inventory to read and would seal a requirement nothing
    ever verifies.  Refused rather than silently dropped, because the whole
    point of the declaration is that its absence is what #714 was.
    """

    if images and transport != "pool":
        raise ValueError(
            "--container-image is a pull-queue placement requirement; the "
            "SLURM lane cannot verify a box's local Docker inventory. Submit "
            "this action on the pool")


def container_image_notice(queue, intent: Mapping[str, object]) -> str:
    """Which recorded eligible boxes can positively run the declared images.

    A report, not the control.  What stops a loop from before the claim check
    taking the work is the required capability tag; what stops a box without
    the image taking it is the claim-time inventory check; and what stops a
    submission into a fleet that reports the image nowhere is the ordinary
    placement refusal.  This is the census beside them, so the operator can
    tell "the box that has it is between announcements" apart from "the box
    that has the tags never adopted the capability" and from "nobody has it".
    """

    images = list(intent.get("container_images") or [])
    if not images:
        return ""
    fit = queue.image_placement_census(intent, max_age_s=RECORDED_OFFER_MAX_AGE_S)
    if not fit or not fit["capable"]:
        return ""  # capacity or tags, not the image, blocks this request
    census = queue.image_placement_census(
        {**intent, "resources": {}}, max_age_s=RECORDED_OFFER_MAX_AGE_S)
    if census is None:
        return ""
    eligible, capable, reporting = census["eligible"], set(census["capable"]), census["reporting"]
    line = ("pbrun: container image " + ", ".join(images)
            + " required; reported on record by: "
            + (", ".join(reporting) if reporting else "(nobody)"))
    not_reporting = sorted(set(capable) - set(reporting))
    if not_reporting:
        line += "; capable but not reporting it: " + ", ".join(not_reporting)
    no_capability = sorted(set(eligible) - capable)
    if no_capability:
        line += (f"; not offering {contracts.CONTAINER_IMAGE_TAG}: "
                 + ", ".join(no_capability))
    return line + "."


def producer_allowance_room_refusal(queue, action: Mapping[str, object]) -> str | None:
    """Why a spool producer's demand leaves no room for its exports, or ``None`` (#1571).

    A producer whose sealed environment configures a produced-output spool
    will have exports, each needing ``EXPORT_DEMAND`` from the export
    allowance the producer's own claim reserves beside its demand. The
    allowance is the sealed ``EXPORT_SLOTS_ENV`` when declared, else the
    unmeasured default of one slot. When the demand plus that minimum
    allowance exceeds the largest recorded ``mem_gb`` among the boxes
    eligible by tags, the producer can never run with room for even one
    export: the 2026-10-06 forward declared mem_gb 104 of sparky's 104
    tokens and its 1 GiB dependents starved on ``token_shortage``. Refuse
    at submission with the numbers, before publication. Unknown capacity
    never refuses: no recorded offer means no evidence, not no room.
    """

    from prismabuild import adaptive_cpu as cpu_admission
    variables = (action.get("environment") or {}).get("variables") or {}
    if not isinstance(variables, dict) or not variables.get(cpu_admission.SPOOL_ROOT_ENV):
        return None
    demand = (action.get("params") or {}).get("demand") or {}
    if not isinstance(demand, dict):
        return None
    raw_slots = variables.get(cpu_admission.EXPORT_SLOTS_ENV)
    if raw_slots is None:
        slots = cpu_admission.DEFAULT_EXPORT_SLOTS
        basis = "unmeasured default of one slot"
    elif (isinstance(raw_slots, str) and raw_slots.isascii() and raw_slots.isdigit()
            and int(raw_slots) > 0):
        slots = int(raw_slots)
        basis = f"sealed {cpu_admission.EXPORT_SLOTS_ENV}={slots}"
    else:
        return None
    need = int(demand.get("mem_gb", 0) or 0) + slots * int(
        cpu_admission.EXPORT_DEMAND.get("mem_gb", 0))
    if need <= 0:
        return None
    tags = ((action.get("params") or {}).get("placement") or {}).get("required_tags") or []
    best = 0
    for offer in queue.offers(max_age_s=RECORDED_OFFER_MAX_AGE_S):
        if not capability_eligibility.HostPresence.from_offer(offer).matches(tags, needs_gpu=False):
            continue
        capacity = offer.get("capacity") or {}
        if isinstance(capacity, Mapping) and "mem_gb" in capacity:
            try:
                best = max(best, int(capacity["mem_gb"]))
            except (TypeError, ValueError):
                continue
    if best <= 0 or need <= best:
        return None
    return (
        f"pbrun: this spool producer declares mem_gb "
        f"{int(demand.get('mem_gb', 0) or 0)} but its export allowance needs "
        f"{slots * int(cpu_admission.EXPORT_DEMAND.get('mem_gb', 0))} more "
        f"({basis}), for {need} GiB against the largest recorded box total "
        f"of {best} GiB. Its exports could never run beside it. Declare at "
        f"most {best - slots * int(cpu_admission.EXPORT_DEMAND.get('mem_gb', 0))} "
        f"GiB, or wait for a larger box. No runnable submission was published.")


def placement_capacity_notice(queue, intent: Mapping[str, object]) -> str:
    """Name recorded capacity terms that prevent this request from fitting."""

    request = {**intent, "resources": {}}
    if intent.get("container_images"):
        census = queue.image_placement_census(request, max_age_s=RECORDED_OFFER_MAX_AGE_S)
        eligible = census["capable"] if census is not None else []
    else:
        eligible = queue.placeable_hosts(request, max_age_s=RECORDED_OFFER_MAX_AGE_S) or []
    if not eligible:
        return ""
    demand = pool.PoolQueue.demand_of(intent)
    lines = []
    spool_short = False
    for offer in queue.offers(max_age_s=RECORDED_OFFER_MAX_AGE_S):
        host = str(offer.get("host") or "?")
        if host not in eligible:
            continue
        capacity = offer.get("capacity") or {}
        if not isinstance(capacity, Mapping):
            continue
        timing = pool.offer_timing(offer.get("announced_unix"), now=time.time())
        # ``announced_unix`` is when the loop last announced, not when it
        # measured the capacity it reports.  The offer record carries no
        # measurement timestamp, so state the age the reader actually holds
        # and leave the measurement's age unknown rather than presenting the
        # announcement age as one (#1189).
        age = ("offer announced "
               f"{timing.age_s:.0f}s ago; capacity measurement age unknown"
               if timing.age_s is not None else
               "offer announcement time unknown; "
               "capacity measurement age unknown")
        for kind, need in demand.items():
            if kind in capacity and int(capacity[kind]) < need:
                if kind == "spool_gb":
                    spool_short = True
                lines.append(
                    f"{kind} {need} > {host} {capacity[kind]} ({age})")
    if not lines:
        return ""
    notice = "  capacity: " + "; ".join(lines) + "\n"
    if spool_short:
        # The deployed refresh path (#1190): a live box re-measures its spool
        # offer on every tick while nothing holds spool_gb, but a declaration
        # refused at start is not revisited without a restart.
        notice += ("  spool_gb: the box's supervisor re-measures its spool "
                   "offer on every tick while no spool_gb is held, so a freed "
                   "disk raises the recorded offer on the next tick; a "
                   "declaration refused at start offers no spool_gb until the "
                   "supervisor restarts or re-execs (#1190).\n")
    return notice


_ORIGIN_BATCHES_ANNOTATION = "produced_output_batches"


_ORIGIN_BATCHES_TEMPLATE_KEY = "produced_output_batches"


_ORIGIN_SLOTS_ANNOTATION = "produced_output_slots"


def require_declared_origin_batches(
    manifest: Mapping[str, object], *, transport: str, queue_root: Path,
) -> None:
    """Refuse a manifest whose declared batches are not what it lists (#912).

    A consumer declares committed origin-only batches by carrying their
    references under ``annotations.produced_output_batches``.  The references
    are what PB will later account the read against, so they must describe
    the manifest exactly: the manifest is derived again from the queue's own
    records (``produced_output.origin_batch_manifest``, which also refuses an
    uncommitted, reclaimed or changed batch) and its mount prefix, entries and
    references must be the declared ones.  A manifest without the annotation
    is not read here at all.

    A data_manifest.v2 declares them with its own entries, at the read
    phases ``annotations.produced_output_slots`` names (#946), and is checked
    by placing those slots again into its static part
    (``produced_output.verify_placed_origin_batches``). A v1 manifest has no
    read plan to place them in, so it may not carry slots.
    """

    annotations = manifest.get("annotations")
    if not isinstance(annotations, Mapping):
        return
    declared = annotations.get(_ORIGIN_BATCHES_ANNOTATION)
    slots = annotations.get(_ORIGIN_SLOTS_ANNOTATION)
    if declared is None and slots is None:
        return
    if transport != "pool":
        raise SubmissionRefused(
            f"pbrun: a data manifest that declares {_ORIGIN_BATCHES_ANNOTATION} "
            f"needs the pull queue, where those batches are filed; --transport "
            f"{transport} has none")
    from prismabuild import produced_output as produced_mod

    if manifest.get("schema") == pb.DATA_MANIFEST_SCHEMA_V2:
        if slots is None:
            raise SubmissionRefused(
                f"pbrun: a data_manifest.v2 declares its batches where it reads "
                f"them, under {_ORIGIN_SLOTS_ANNOTATION}; build it with "
                "produced_output.place_origin_batches")
        try:
            produced_mod.verify_placed_origin_batches(queue_root, manifest)
        except produced_mod.ProducedOutputError as exc:
            raise SubmissionRefused(
                f"pbrun: declared produced-output batch: {exc}") from None
        return
    if slots is not None:
        raise SubmissionRefused(
            f"pbrun: {_ORIGIN_SLOTS_ANNOTATION} places batches in a read plan, "
            "which only a data_manifest.v2 has")
    try:
        derived = produced_mod.origin_batch_manifest(queue_root, declared)
    except produced_mod.ProducedOutputError as exc:
        raise SubmissionRefused(f"pbrun: declared produced-output batch: {exc}") from None
    for field in ("mount_prefix", "entries"):
        if manifest.get(field) != derived[field]:
            raise SubmissionRefused(
                f"pbrun: the data manifest's {field} is not what its declared "
                f"{_ORIGIN_BATCHES_ANNOTATION} commit; build it with "
                "produced_output.origin_batch_manifest")
    if list(declared) != derived["annotations"][_ORIGIN_BATCHES_ANNOTATION]:
        raise SubmissionRefused(
            f"pbrun: the data manifest's {_ORIGIN_BATCHES_ANNOTATION} are not "
            "in canonical form")


def declare_origin_consumers(queue, refs: Sequence[Mapping[str, object]], *,
                             consumer_action_key: str) -> None:
    """File this consumer against each consumed batch it declares (#914).

    Called with the sealed key, before the consumer's row is published, so
    the retirement tick never sees a queued consumer it has no declaration
    for.  One batch at a time: each declaration takes that batch's
    output-prefix lock, and no caller may hold two of those at once.  A batch
    that has started to retire, or is gone, refuses the submission; a
    ``retain`` batch files nothing.
    """

    from prismabuild import produced_output as produced_mod

    for ref in refs:
        try:
            produced_mod.declare_origin_consumer(
                queue, ref, consumer_action_key=consumer_action_key)
        except produced_mod.ProducedOutputError as exc:
            raise SubmissionRefused(
                f"pbrun: cannot declare {consumer_action_key[:12]} as a consumer "
                f"of a produced-output batch: {exc}") from None


def submission_window(q, key: str, refs: Sequence[Mapping[str, object]]):
    """Hold ``key``'s transition lock from its declarations through its row (#945).

    A consumer declares its consumed batches before its row exists, so for a
    moment it holds a batch with no queue record at all. An operator's
    release of such an ``unpublished`` declaration
    (`produced_output.release_origin_consumer`) takes the same lock without
    waiting and refuses while it is held, so it can never release a key whose
    row is about to land. A consumer that declares nothing takes no lock.
    ``publish_consumer_row`` and ``PoolQueue.publish`` take the same lock
    again, which nests, and the declarations take their output-prefix locks
    inside it: transition, then ownership, as everywhere.
    """

    if not refs:
        return contextlib.nullcontext(True)
    return q._transition_locked(key)


def require_deployed_read_plan_storage(*, source_root: Path | None = None,
                                       published_root: Path | None = None, store: FleetStore) -> None:
    """Refuse a v2 row until the published storage reader has the same bytes.

    A source checkout can seal v2 before the fleet's storage role knows it.
    The old role would skip the manifest and the row would claim cold; compare
    the contract owners to the published, hashed generation first.
    The operator also verifies the DL380 role has reloaded that generation.
    """

    source_root = store.runtime_root if source_root is None else source_root
    published_root = store.root / "repo" if published_root is None else published_root
    try:
        receipt = json.loads((published_root / "RUNTIME_VERSION.json").read_text())
        files = receipt["files"]
        for member in ("src/prismabuild/core.py", "src/prismabuild/manifest_layout.py",
                       "tools/fleet/prewarm_loop.py"):
            expected = hashlib.sha256((source_root / member).read_bytes()).hexdigest()
            if (files.get(member) != expected or
                    hashlib.sha256((published_root / member).read_bytes()).hexdigest()
                    != expected):
                raise ValueError(f"{member} is not the compatible published bytecode")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise SubmissionRefused(
            f"pbrun: v2 read plan requires the compatible published storage "
            f"generation: {exc}") from None


def require_linear_read_plan_progress(manifest: Mapping[str, object],
                                      progress: Mapping[str, object] | None) -> None:
    """A read phase boundary matters only when the worker accepts that phase."""

    names = [phase["name"] for phase in manifest["read_plan"]["phases"]]
    reported = ([phase["name"] for phase in progress["phases"]]
                if isinstance(progress, dict) and not progress.get("cycle")
                else [])
    # One rule for both paths (#1332): the tier loop places a planner row's
    # progress in its read plan with the same contract.
    contract = residency_plan.progress_contract(names, reported)
    if contract in ("undeclared", "unnamed"):
        raise SubmissionRefused(
            "pbrun: v2 read plan requires linear progress reporting "
            "with every read phase named")
    if contract == "misordered":
        raise SubmissionRefused("pbrun: read plan phases must follow progress order")


def progress_contract_notice(
    queue,
    intent: Mapping[str, object],
    *,
    policy: Mapping[str, object] | None,
    requested_timeout_s: float | None = None,
    awaited_batch: Mapping[str, object] | None = None,
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
           f"with an explicit payload execution budget of {requested_timeout_s:g}s, "
           "not a wall-clock stop or resource-release guarantee."),
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
            raise SubmissionRefused(
                f"pbrun: no eligible worker offers {contracts.PROGRESS_CYCLE_TAG}; "
                "cyclic progress support is unknown. Update the fleet's "
                "published generation and wait for worker adoption.\n"
                + "\n".join(contracts.contract_remedies([contracts.PROGRESS_CYCLE_TAG])))
        lines.append(
            "pbrun: no worker offers on record; whether any box honours the "
            "contract is unknown.")
        return "\n".join(lines)
    unsupported = sorted(
        host for host, contracts in announced.items()
        if contracts is None or pb.PROGRESS_RECORD_SCHEMA_V1 not in contracts)
    if len(unsupported) == len(announced):
        raise SubmissionRefused(
            "pbrun: no eligible worker announces "
            f"{pb.PROGRESS_RECORD_SCHEMA_V1} ({', '.join(unsupported)}), so "
            "this action would be admitted under the progress contract and "
            "then killed by a whole-run ceiling it never asked for.  Update "
            "the fleet's published generation, or submit with --timeout-s.\n"
            + "\n".join(contracts.contract_remedies([contracts.PROGRESS_TAG])))
    if unsupported:
        lines.append(
            "pbrun: " + ", ".join(unsupported) + " do not announce "
            f"{pb.PROGRESS_RECORD_SCHEMA_V1}, so this action is not offered "
            f"to them: it requires the {contracts.PROGRESS_TAG} tag they do not "
            "publish.  It waits for a box that does rather than being killed "
            "by a ceiling it never asked for.")
    watchdog_hosts = set(announced) - set(unsupported)
    helper_intent = {
        **intent,
        "tags": [*list(intent.get("tags") or []), contracts.PROGRESS_TAG,
                 contracts.PROGRESS_HELPER_TAG],
    }
    helper_hosts = set(queue.placeable_hosts(helper_intent) or [])
    eligible = watchdog_hosts & helper_hosts
    helper_missing = sorted(watchdog_hosts - helper_hosts)
    if not eligible:
        raise SubmissionRefused(
            "pbrun: no eligible worker both announces "
            f"{pb.PROGRESS_RECORD_SCHEMA_V1} and offers "
            f"{contracts.PROGRESS_HELPER_TAG}"
            + (f" ({', '.join(helper_missing)} announce the watchdog but do not "
               "offer the helper)" if helper_missing else "")
            + "; this action uses the helper environment. Update the fleet's "
            "published generation, or submit an action that does not declare "
            "progress phases.\n"
            + "\n".join(contracts.contract_remedies([contracts.PROGRESS_HELPER_TAG])))
    if helper_missing:
        lines.append(
            "pbrun: " + ", ".join(helper_missing) + " announce "
            f"{pb.PROGRESS_RECORD_SCHEMA_V1} but do not offer "
            f"{contracts.PROGRESS_HELPER_TAG}, so this action is not offered to them: "
            f"it requires {contracts.PROGRESS_TAG} and {contracts.PROGRESS_HELPER_TAG}. "
            "It waits for a helper-capable box rather than starting without "
            "the documented reporting path.")
    if policy.get("cycle"):
        cycle_intent = {**helper_intent, "tags": [
            *helper_intent["tags"], contracts.PROGRESS_CYCLE_TAG]}
        cycle_hosts = set(queue.placeable_hosts(cycle_intent) or [])
        cycle_missing = sorted(eligible - cycle_hosts)
        eligible &= cycle_hosts
        if not eligible:
            raise SubmissionRefused(
                f"pbrun: no eligible worker offers {contracts.PROGRESS_CYCLE_TAG} "
                f"({', '.join(cycle_missing)}); update the fleet's published "
                "generation before submitting cyclic progress.\n"
                + "\n".join(contracts.contract_remedies([contracts.PROGRESS_CYCLE_TAG])))
        if cycle_missing:
            lines.append("pbrun: " + ", ".join(cycle_missing)
                         + f" do not offer {contracts.PROGRESS_CYCLE_TAG}; "
                         "this cyclic action waits for a capable worker.")
        lines.append(
            "pbrun: cyclic phases: each phase grants its allowance once "
            "between increases in cumulative committed units; "
            f"at most {total:g}s of quiet after the count stops increasing.")
        helper_intent = cycle_intent
    if awaited_batch is not None:
        awaited_intent = {**helper_intent, "tags": [
            *helper_intent["tags"], contracts.QUEUED_CHILD_TAG]}
        awaited_hosts = set(queue.placeable_hosts(awaited_intent) or [])
        awaited_missing = sorted(eligible - awaited_hosts)
        eligible &= awaited_hosts
        if not eligible:
            raise SubmissionRefused(
                f"pbrun: no eligible worker offers {contracts.QUEUED_CHILD_TAG} "
                f"({', '.join(awaited_missing)}); update the fleet's published "
                "generation before submitting a coordinator that awaits "
                "queued children.\n"
                + "\n".join(contracts.contract_remedies([contracts.QUEUED_CHILD_TAG])))
        if awaited_missing:
            lines.append("pbrun: " + ", ".join(awaited_missing)
                         + f" do not offer {contracts.QUEUED_CHILD_TAG}; "
                         "this coordinator waits for a capable worker.")
        lines.append(
            "pbrun: queued-child wait: the worker credits quiet while a "
            "verified awaited child is ready or claimed at both ends of "
            "the interval; a first sighting is a baseline and earns no "
            "credit.")
        helper_intent = awaited_intent
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
                    f"(requested {requested_grace:g}s); an explicit payload "
                    "execution budget is unchanged.")
    return "\n".join(lines)


def timeout_ceiling_notice(
    queue,
    intent: Mapping[str, object],
    *,
    requested: float | None,
) -> str:
    """Report eligible workers whose payload budgets cut ``--timeout-s`` short.

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
    7200. What was wrong was being told nothing. The smallest announced budget
    need not be the budget an unannounced eligible worker would enforce.
    Checkout, credited waits and settlement are not bounded by this notice.

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
            f"pbrun: --timeout-s {requested:g} exceeds the payload execution ceiling "
            f"{'every' if certain else 'some'} eligible worker announces "
            f"({named}); the lowest announced payload execution budget is {lowest:g}s, "
            "not a wall-clock stop or resource-release guarantee."
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
    a pin has one job and that was it. ``placement_contract`` no longer drops the
    host pin that way, but the reading rule is what keeps this correct
    whatever it returns.

    Exclusivity is claimed only where it is provable.  A tag naming this host
    cannot be claimed elsewhere; a tag that merely happens to match one live
    box today -- ``gb10``, ``sparklina`` -- is a fact about the fleet as
    announced at this instant, and the second box offering it can claim an
    action whose tree it does not have.  So that case is reported as the
    contingency it is rather than as "match only this box".

    The explicit ``--tag`` list is deliberately NOT a parameter here.  The
    only thing it decides is what ``placement_contract`` returned, and that is
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
    # A claim whose finish is in flight.  ``PoolQueue.finish`` moves
    # ``claimed/<key>.json`` aside to a finish tombstone before it releases
    # capacity and files ``done/``; the receipt is already in the CAS by then.
    # Reading only the two names above made that window look like nothing
    # outstanding, so ``pbwait`` answered ``cache_hit`` before the ending
    # existed and a caller reading ``done/`` found nothing (the 6d88c0b15b18
    # canary's leg 3).  The pool's own claim gate treats both suffixes as a
    # live claim (``already_claimed``); agree with it.
    suffixes = (pool.TOMBSTONE_SUFFIX, pool.LATE_FINISH_SUFFIX)
    try:
        names = os.listdir(q.dir(pool.CLAIMED))
    except OSError:
        names = []
    for name in names:
        if not (name.startswith(f"{key}.") and name.endswith(suffixes)):
            continue
        try:
            item = json.loads(
                (q.dir(pool.CLAIMED) / name).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            # Gone because the finish just filed its ending, or not a record:
            # either way the terminal read that follows answers.
            continue
        if not isinstance(item, dict) or item.get("action_key") != key:
            continue
        generation = item.get("published_unix")
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

    ``ActionAlreadyLiveError`` is not such a refusal and is re-raised
    unchanged: the queue is already carrying this key, which is an answer the
    caller acts on by attaching to the live generation (``publish_or_attach``
    below), not a reason to stop.
    """

    try:
        return q.publish(**publication)
    except pool.ActionAlreadyLiveError:
        raise
    except pool.PoolContractError as exc:
        raise SubmissionRefused(f"pbrun: {exc}") from exc


def publish_or_attach(q, publication: Mapping[str, object], *, key: str, display: DisplaySink = discard_display):
    """Submit this key, or report the live generation already running it.

    Returns ``(queued_path, generation)``.  ``queued_path`` is ``None`` when
    the queue refused the publication because it is already carrying the key;
    the caller then waits on ``generation`` rather than submitting a second
    copy of the same content-addressed work.

    This is the whole of the duplicate-submission answer, and it is the pool's
    to give.  ``live_submission``/``bounded_attachment`` ask the same question
    from outside the queue, which is right for ``--detach`` -- it has to decide
    what to print without publishing anything -- but between that read and the
    publication the state can change, and for identical submissions made at
    the same moment it reliably does (#812).  ``PoolQueue.publish`` reads and
    writes inside the key's transition lock, so its answer cannot be stale.

    ``generation`` may be ``None`` when the queue's own read-back finds no
    stamp, which keeps its historical meaning: wait for any ending for the
    key rather than pretend to know which run it belongs to.

    A queue from a runtime generation that predates ``refuse_if_live`` is
    asked in the way it understands, for the same reason ``publication_row``
    feature-detects ``retry_safe``: a checkout can advance just before the
    atomic runtime generation rolls, and the older behaviour -- restamp, then
    wait on whatever the read-back says -- is what this client had before.
    """

    if "refuse_if_live" in inspect.signature(q.publish).parameters:
        publication = {**publication, "refuse_if_live": True}
    try:
        queued_path = publish_or_refuse(q, publication)
    except pool.ActionAlreadyLiveError as exc:
        emit_display(display, f"pbrun: {key[:12]} is already {exc.state} on the pool; "
              f"attaching to that run rather than submitting a second copy",
              channel="stderr", flush=True)
        return None, exc.generation
    return queued_path, published_generation(q, key, queued_path)


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

    found = _live_submission_records(q, key, lane_root=lane_root)
    if found is not None and found[0] == "slurm":
        if not _slurm_submission_live(str(found[2]["job_id"]), **lane_commands):
            return None
    return found


def _slurm_submission_live(job_id: str, **lane_commands) -> bool:
    state = slurm_lane.query_state(job_id, **lane_commands)
    return state is not None and state[0] not in slurm_lane.TERMINAL_STATES


def _live_submission_records(q, key: str, *, lane_root=None):
    """Read shared liveness evidence; leave SLURM controller queries to the parent."""

    found = outstanding_submission(q, key, lane_root=lane_root)
    if found is None:
        return None
    transport, generation, submission = found
    if action_wait.await_ending(q, key, wait_s=0.0, generation=generation) is not None:
        return None
    if transport == "slurm":
        job_id = str(submission.get("job_id") or "")
        if not job_id:
            return None
        return found
    if q.item_path(pool.READY, key).exists():
        return found
    age = q.lease_age(key)
    if age is not None and age < pool.LEASE_TIMEOUT_S:
        return found
    return None


def _attachment_value(q, key: str, *, lane_root=None):
    found = _live_submission_records(q, key, lane_root=lane_root)
    if found is None:
        return None
    transport, generation, submission = found
    if transport == "slurm":
        directory = Path(str(submission.get("directory") or "."))
        record = slurm_lane.submission_record_path(
            directory, published_unix=generation,
            attempt=int(submission.get("attempt") or 1))
        job_id = str(submission.get("job_id") or "")
    else:
        ready = q.item_path(pool.READY, key)
        record = ready if ready.exists() else q.item_path(pool.CLAIMED, key)
        job_id = ""
    return {"transport": transport, "generation": generation,
            "job_id": job_id, "submission": str(record)}


def bounded_attachment(q, key: str, *, lane_root=None, **lane_commands):
    """Discover an attachment without letting a queue read park the submitter.

    The child only reads. Its reply includes the display path so rendering
    cannot re-read READY in the parent. Scheduler commands have their existing
    timeouts and run in the parent, keeping their process ownership unchanged.
    Existing tolerant record parsing and liveness rules remain in force.
    """

    value = action_wait.read_observation(
        "attachment discovery",
        lambda: _attachment_value(q, key, lane_root=lane_root),
        budget_s=ATTACHMENT_READ_TIMEOUT_S)
    if value is None:
        return None
    if (not isinstance(value, dict)
            or value.get("transport") not in {"pool", "slurm"}
            or not isinstance(value.get("generation"), (int, float))
            or isinstance(value.get("generation"), bool)
            or not math.isfinite(value["generation"])
            or not isinstance(value.get("job_id"), str)
            or not isinstance(value.get("submission"), str)
            or not value["submission"]):
        raise action_wait.OutcomeReadUnavailable("attachment discovery returned an invalid payload")
    if value["transport"] == "slurm":
        if not value["job_id"]:
            raise action_wait.OutcomeReadUnavailable("attachment discovery returned no SLURM job id")
        if not _slurm_submission_live(value["job_id"], **lane_commands):
            return None
    return value


def _report_stall(key: str, report, *, display: DisplaySink = discard_display) -> None:
    """Say that a running job has not moved.  Say it; do nothing about it.

    The lane decides *when* (``STALL_WINDOW_S`` of unchanged samples, repeated
    at ``STALL_REPORT_EVERY_S``); this decides the words.  The job is still
    running and keeps running: a stall is evidence for a person, and the only
    thing that ends it is that person's ``--withdraw`` or a ``--timeout-s``
    they asked for.  Wall-clock is never evidence of death.
    """

    minutes = int(report.stalled_for_s // 60)
    where = report.node or "an unknown node"
    emit_display(display, f"pbrun: {key[:12]} slurm job {report.job_id} has shown no progress "
          f"for {minutes} min on {where}; it is still running. Withdraw with "
          f"pbrun --withdraw {key[:12]} if it is dead.",
          channel="stderr", flush=True)


_QUERY_COMMANDS = frozenset({"sacct", "scontrol", "squeue"})


_RESUME_COMMANDS = frozenset({
    "poll_s", "sacct", "scontrol", "squeue", "sstat", "sleep", "clock",
    "on_stall", "on_notice",
})


SEALED_ARGV0 = movement_actions.SEALED_ARGV0


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


def require_disk_metadata_scope(demand: Mapping[str, object], *, transport: str) -> None:
    """Refuse a ``disk_metadata`` reservation on a transport with nothing to enforce it.

    ``disk_metadata`` (#1008 item 4) is a host-ledger consumable a pull-queue
    worker can opt into offering one of (default zero); a full-demand claim
    charges it at admission. This serializes declaring actions on the same
    host ledger, not ordinary work, tier egress or external I/O. The SLURM lane's
    translation (``slurm_lane.LaneResources.from_demand``) reads only ``cpu``,
    ``gpu`` and ``mem_gb`` -- it has no GRES for directory/file metadata
    throughput -- so a ``disk_metadata`` demand sealed there would never be
    enforced, and two such jobs could still land on the same node.  Sealing
    that would be worse than refusing it, the same reasoning
    :func:`require_progress_scope` states for the stall watchdog: the action
    would be admitted on a cooperative reservation that lane cannot enforce.
    """

    if demand.get("disk_metadata") and transport != "pool":
        raise SubmissionRefused(
            f"pbrun: --demand disk_metadata needs the pull queue: it is a "
            f"host-ledger reservation, and --transport {transport} has no "
            "way to enforce it")


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


def require_awaited_batch_scope(
        *, awaited: Mapping[str, object] | None,
        progress: Mapping[str, object] | None, transport: str) -> None:
    """Refuse an awaited batch without progress phases on pool transport (#1666).

    The credit moves the stall deadline, so without a progress policy it
    credits against no allowance. The stall watchdog is the pull-queue
    worker's, so the SLURM lane cannot enforce it either.
    """

    if awaited is None:
        return
    if progress is None:
        raise ValueError(
            "--awaited-batch requires --progress-phase: the credit moves the "
            "stall deadline, and without a policy it credits no allowance")
    if transport != "pool":
        raise ValueError(
            "--awaited-batch requires pool transport: the stall watchdog is "
            "the pull-queue worker's, and the SLURM lane can only enforce a "
            "total duration")


def require_host_class_scope(
    *, measurement: bool, host_class: str | None, transport: str, anywhere: bool = False
) -> None:
    """Refuse a scope the design cannot honour, before anything is sealed."""

    if measurement and transport == "pool" and anywhere:
        if host_class is not None:
            raise SubmissionRefused("pbrun: --host-class already scopes pool measurement placement; "
                             "drop --anywhere")
        raise SubmissionRefused(
            "pbrun: pool measurements run on the submitting host whose "
            "platform/toolchain is sealed; --anywhere contradicts that placement."
        )

    if measurement and host_class is None and transport != "pool":
        raise SubmissionRefused(
            "pbrun: --measurement requires --host-class CLASS.\n"
            "A measurement's numerics do not transfer across architectures, "
            "so its result is keyed on the host class that produced it "
            "(docs/design.md, \"Cache/action-key semantics\"); a portable "
            "measurement would let any box's KL stand in for another's."
        )
    if host_class is not None and transport != "slurm" and not (
        transport == "pool" and measurement
    ):
        raise SubmissionRefused(
            "pbrun: --host-class needs --transport slurm or pool --measurement.\n"
            "A host_class_keyed action is attested through the SLURM "
            "controller (docs/design.md, \"Worker preflight and execution "
            "attestation\"); a pull-queue worker refuses it at preflight, so "
            "submitting it there queues work that cannot run."
        )


TARGET_EVIDENCE_MAX_BYTES = 64 * 1024


_TARGET_EVIDENCE_CLASSES = {
    "gb10": lambda evidence, platform_key: (
        platform_key == "linux-aarch64-sm121"
        and all(row["name"] == "NVIDIA GB10" and row["compute_capability"] == "12.1"
                for row in evidence["accelerators"])),
}


class TargetEvidence(dict):
    """A vetted packet's evidence, plus the worker's argv[0] toolchain fields.

    It compares equal to the plain evidence mapping, so everything that reads
    the evidence is unchanged.  ``argv0`` is what the class seals for argv[0]:
    the WORKER's executable identity, never the submitting box's (#1598).
    """

    argv0: dict[str, str]
    recorder: dict[str, str] | None = None


def load_target_evidence(path: str, *, host_class: str) -> dict[str, object]:
    """Read and vet one target-evidence packet for ``host_class`` (#1598).

    ``pbevidence.py`` prints the packet on a worker of the class.  It replaces
    the local probe a class-scoped pool measurement makes, so a box without an
    accelerator can seal the class facts.  Nothing here is an attestation: each
    worker checks the declared facts against its own live facts before it
    runs, so a wrong packet fails closed there.  This function refuses the
    packets that could never seal a class -- not local, no accelerator, no
    device identity, mixed models, or a platform the class does not have --
    before anything is sealed.
    """

    try:
        with open(path, "rb") as handle:
            raw = handle.read(TARGET_EVIDENCE_MAX_BYTES + 1)
    except OSError as exc:
        raise SubmissionRefused(f"pbrun: cannot read --target-evidence {path}: {exc}") from None
    if len(raw) > TARGET_EVIDENCE_MAX_BYTES:
        raise SubmissionRefused(
            f"pbrun: --target-evidence {path} exceeds {TARGET_EVIDENCE_MAX_BYTES} "
            "bytes: a packet is one worker's facts, not a payload")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise SubmissionRefused(f"pbrun: --target-evidence {path} is not JSON: {exc}") from None
    try:
        evidence = target_evidence.vet(value)
    except target_evidence.PacketError as exc:
        raise SubmissionRefused(f"pbrun: --target-evidence {path}: {exc}") from None
    rule = _TARGET_EVIDENCE_CLASSES.get(host_class)
    if rule is None:
        raise SubmissionRefused(
            f"pbrun: --target-evidence {path}: no rule for class {host_class!r}; "
            f"a packet can stand for {', '.join(sorted(_TARGET_EVIDENCE_CLASSES))}")
    try:
        argv0 = target_evidence.argv0_contract(value)
        recorder = target_evidence.recorder_contract(value)
    except target_evidence.PacketError as exc:
        raise SubmissionRefused(f"pbrun: --target-evidence {path}: {exc}") from None
    try:
        platform_key = pb._platform_key_from_evidence(evidence)
    except pb.ActionContractError as exc:
        raise SubmissionRefused(f"pbrun: --target-evidence {path}: {exc}") from None
    if not rule(evidence, platform_key):
        raise SubmissionRefused(
            f"pbrun: --target-evidence {path} disagrees with class {host_class}: "
            f"it reports {platform_key}")
    vetted = TargetEvidence(evidence)
    vetted.argv0 = argv0
    vetted.recorder = recorder
    return vetted


def host_class_scope(
    host_class: str | None, *, measurement: bool = False, transport: str = "slurm",
    target_evidence: Mapping[str, object] | None = None,
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

    if target_evidence is not None and not (
            measurement and transport == "pool" and host_class is not None):
        raise ValueError(
            "target_evidence stands for a class-scoped pool measurement only")
    if measurement and transport == "pool":
        # A pool worker can attest its platform and executable/ABI directly.
        # A class is placement intent, not a forged SLURM attestation. The
        # actual platform, ABI, driver and device models constrain numerics;
        # physical UUIDs and the selected worker remain receipt provenance.
        # A vetted target-evidence packet stands in for the local probe when
        # the submitting box has no accelerator (#1598): the worker still
        # checks every declared fact against its own live facts.
        evidence = (target_evidence if target_evidence is not None else
                    pb._collect_worker_evidence(
                        **({"attest_accelerator_identity": True}
                           if host_class is not None else {})))
        # The class's executable identity is the WORKER's: a submitter of another
        # architecture has a different /bin/bash, and the worker refuses a
        # declared size that is not its own (#1598).
        if target_evidence is not None:
            argv0 = getattr(target_evidence, "argv0", None)
            if not argv0:
                raise ValueError(
                    "target_evidence carries no worker argv0 identity; "
                    "load it with load_target_evidence")
        else:
            argv0 = pb.executable_toolchain_contract(SEALED_ARGV0)
        toolchain = {
            **argv0,
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
display: DisplaySink = discard_display) -> int:
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

    emit_display(display, f"pbrun: {key[:12]} is already in the CAS; nothing submitted",
          channel="stderr", flush=True)
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
    emit_display(display, f"pbrun: cache_hit on {host} in 0s", channel="stderr", flush=True)
    return 0


def _unfiled_record(
    exc: BaseException,
    *,
    key: str,
    action,
    cas,
    record: str = "",
    tool: str = "pbrun",
    report=None,
display: DisplaySink = discard_display) -> int:
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
    emit_display(display, 
        f"{tool}: slurm took this action, but {tool} could not write its "
        f"record.\n"
        f"  slurm job: {job_id or '(none accepted)'}\n"
        f"{where}"
        f"  reason:    {reason}\n"
        f"{advice}",
        channel="stderr", flush=True)
    if report is not None:
        return report.finish(action_client.ClientUnavailable(key,
            getattr(report.identity, 'published_unix', None), action_wait.WaitEnd.UNRETRIED))
    return action_client.RECORD_WRITE_FAILED_EXIT


def _lane_io_failure(exc, *, key: str, action, cas, tool="pbrun", job_id="", report=None, display: DisplaySink = discard_display) -> int:
    """Separate stamped post-submission writes from other filesystem faults."""

    if getattr(exc, "job_id", None):
        return _unfiled_record(exc, key=key, action=action, cas=cas, tool=tool, report=report, display=display)
    path = str(getattr(exc, "filename", "") or "")
    reason = str(getattr(exc, "strerror", "") or exc)
    emit_display(display, 
        f"{tool}: filesystem access failed while processing slurm action {key[:12]}.\n"
        f"  slurm job: {job_id or '(not identified; acceptance is unknown)'}\n"
        f"  path:      {path or '(not supplied)'}\n"
        f"  reason:    {reason}\n"
        "Submission and receipt status could not be fully verified.\n"
        f"Restore filesystem access, then run `tools/fleet/pbwait.py {key[:12]}` "
        "to recover a recorded submission or ending; if nothing was submitted, "
        "retry the original command.",
        channel="stderr", flush=True,
    )
    if report is not None:
        return report.finish(action_client.ClientUnavailable(key,
            getattr(report.identity, 'published_unix', None), action_wait.WaitEnd.UNRETRIED))
    return action_client.RECORD_WRITE_FAILED_EXIT


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
    priority_reason: str | None = None,
    placement_notice: str = "",
    anywhere: bool = False,
    detach: bool = False,
    runtime_root: Path | None = None,
    lane_root=None,
    queue_root=None,
    report=None,
    store: FleetStore, display: DisplaySink = discard_display, **lane_commands,
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

    report = report if report is not None else action_client.ClientReport()
    runtime_root = store.runtime_root if runtime_root is None else runtime_root
    key = str(action["action_key"])
    if placement_notice:
        # How wide this action is, said before anything is submitted, exactly
        # as the pool path says it.  The pin a box-local checkout imposes is a
        # consequence of a path rather than of a flag, and a submitter that is
        # not told has narrowed the fleet to one box without knowing.
        emit_display(display, placement_notice, channel="stderr", flush=True)
    slots = int(demand.get("gpu", 0) or 0)
    if exclusive and slots > 1:
        # ``LaneResources.gres()`` answers ``gpu:1`` for an exclusive action
        # whatever the count says, because ``gpu:N`` and ``shard:N`` are
        # mutually exclusive requests against one device and exclusivity is the
        # first.  On today's one-device boxes that is right and the count is
        # redundant; on a two-GPU box it would silently hand back half of what
        # was asked for.  Refusing is the honest answer either way -- the pool
        # read the count off worker offers, and SLURM has no such thing here.
        raise SubmissionRefused(
            f"pbrun: --exclusive --gpu-capacity {slots} is not something this "
            f"transport can express.\n"
            "Under SLURM, exclusivity IS the whole device: the lane sends "
            "--gres=gpu:1, and a count above one would have to name that many "
            "whole devices, which nothing here derives or checks.\n"
            "Drop --gpu-capacity to take one device exclusively, or drop "
            "--exclusive and ask for --gpu-capacity slots (shards) instead."
        )
    resources = slurm_lane.LaneResources.from_demand(demand, exclusive=exclusive)
    lane_commands.setdefault("on_stall", lambda report: _report_stall(key, report, display=display))
    lane_commands.setdefault(
        "on_notice",
        lambda text: emit_display(display, f"pbrun: {text}", channel="stderr", flush=True))
    # Say that the slot has no device, every time, on the line that announces
    # the submission.  The mask is applied before the transport branch and it
    # is also a silent narrowing: a suite that used to run its CUDA tests now
    # skips them, and a skip that nobody announced reads as the same green.
    masked = "" if slots else "  [no GPU: CUDA_VISIBLE_DEVICES='']"
    queue = store.queue_root if queue_root is None else queue_root
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
        return _lane_io_failure(exc, key=key, action=action, cas=cas, report=report, display=display)
    if receipt is not None:
        digest = receipt['result']['sha256']
        report.publish_identity(action_client.CachedClientIdentity(key, Path(queue), Path(cas.root),
            action_client.ClientTransport.SLURM, digest))
        cached_outcome(
            key,
            receipt=receipt,
            queue_root=queue,
            resources=resources,
            tags=tags,
            retry_safe=retry_safe,
            max_attempts=max_attempts,
        display=display)
        return report.finish(action_client.ClientExecution(key, None, 'cache_hit', None,
            None, None, False, 0, action_client.CachedResult(digest), receipt_published=True))
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
        if report.identity is None:
            report.publish_identity(action_client.SlurmClientIdentity(key, job.published_unix,
                Path(queue), Path(cas.root), job.record_path, job.job_id,
                action_client.ClientArrival.SUBMITTED))
        emit_display(display, 
            f"pbrun: submitted {key[:12]} as slurm job {job.job_id} "
            f"(attempt {job.attempt}) tags={tags} demand={demand}{masked}",
            channel="stderr", flush=True)

    # sbatch's own refusal is this transport's capability gate: an unknown
    # Feature or an impossible GRES is rejected at submit time, which is the
    # moment the pool path's ``capability_verdict`` spoke.  So it reaches the
    # caller as the message SLURM wrote, in the shape that message had, rather
    # than as a traceback.
    try:
        if attached is not None:
            submission = attached[2]
            job_id = str(submission.get("job_id") or "")
            generation = _submission_generation(submission)
            record_path = slurm_lane.submission_record_path(
                Path(str(submission.get('directory') or '.')), published_unix=generation,
                attempt=int(submission.get('attempt') or 1))
            report.publish_identity(action_client.SlurmClientIdentity(key, generation,
                Path(queue), Path(cas.root), record_path, job_id, action_client.ClientArrival.ATTACHED))
            emit_display(display, f"pbrun: {key[:12]} is already running (slurm job {job_id}); "
                  f"attaching to it rather than submitting a second copy"
                  f"{masked}", channel="stderr", flush=True)
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
                priority_reason=priority_reason,
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
        emit_display(display, f"pbrun: the fate of this submission is unknown.\n  {exc}",
              channel="stderr", flush=True)
        return report.finish(action_client.ClientUnsettled(key,
            getattr(report.identity, 'published_unix', None), action_client.UnsettledReason.SUBMISSION_FATE))
    except slurm_lane.SlurmLaneError as exc:
        if getattr(exc, "job_id", None):
            # sbatch accepted this before the lane failed, so this is a record
            # that would not write and not a refusal.  Reported as a refusal it
            # told a submitter to fix the ``--tag`` of a job that was already
            # queued, which is both wrong and expensive to act on.
            return _unfiled_record(exc, key=key, action=action, cas=cas, report=report, display=display)
        raise SubmissionRefused(
            f"pbrun: slurm refused this action.\n"
            f"  required tags: {tags or '(any box)'}\n"
            f"  demand:        {demand}\n"
            f"  {exc}\n"
            f"Fix the --tag, or read `sinfo -N -l` for a node that offers it."
        ) from exc
    except OSError as exc:
        return _lane_io_failure(
            exc, key=key, action=action, cas=cas, report=report,
            job_id=(str(attached[2].get("job_id") or "") if attached else
                    str(submitted_job_ids[-1]) if submitted_job_ids else ""), display=display)

    last = result.last
    if last is None:                       # unreachable: run always submits
        emit_display(display, "pbrun: nothing was submitted", channel="stderr")
        return report.finish(action_client.ClientRefused(1, key))
    job, outcome = last
    if detach:
        emit_display(display, sorted_json(action_client.detached_submission_record(
            key,
            transport="slurm",
            status="submitted",
            queue_root=queue,
            published_unix=result.published_unix,
            job_id=job.job_id,
            submission=job.record_path,
        )), flush=True)
        return report.finish(action_client.ClientSubmitted(report.identity, job.record_path))

    def execution(code, status, replay=None):
        return report.finish(action_client.ClientExecution(key, result.published_unix, status,
            outcome.exit_code, None, None, outcome.state == 'TIMEOUT', code, replay,
            receipt_published=result.receipt is not None, transport_state=outcome.state,
            transport_signal=outcome.signal))

    total = len(result.attempts)
    for index, (attempted, reported) in enumerate(result.attempts, start=1):
        emit_display(display, f"pbrun: attempt {index}/{total} slurm job {attempted.job_id} "
              f"{reported.state}", channel="stderr")
        _echo(attempted.stdout_path, "stdout", display=display)
        _echo(attempted.stderr_path, "stderr", display=display)

    if result.receipt is not None:
        if slurm_lane.job_was_cache_hit(job):
            # The job started, read the receipt somebody else published, and
            # ran nothing.  Saying "executed" here would attribute that
            # somebody else's work to this job, with this job's elapsed time.
            emit_display(display, f"pbrun: cache_hit -- slurm job {job.job_id} found "
                  f"{key[:12]} already in the CAS and ran nothing",
                  channel="stderr")
            return execution(0, 'cache_hit',
                action_client.CachedResult(result.receipt['result']['sha256']))
        emit_display(display, f"pbrun: executed via slurm job {job.job_id} ({outcome.state})",
              channel="stderr")
        return execution(0, 'executed')
    if outcome.state == "CANCELLED":
        emit_display(display, f"pbrun: withdrawn -- slurm job {job.job_id} was cancelled",
              channel="stderr")
        return execution(action_client.WITHDRAWN_EXIT, 'withdrawn')
    if outcome.state == slurm_lane.WAIT_TIMEOUT_STATE:
        emit_display(display, f"pbrun: gave up waiting for {key[:12]}; slurm job "
              f"{job.job_id} is still queued or running "
              f"(pbrun --transport slurm --withdraw {key[:12]} stops it)",
              channel="stderr")
        return report.finish(action_client.ClientPatienceExpired(key, result.published_unix))
    if outcome.state == slurm_lane.UNKNOWN_STATE:
        # The controller answered that it knows no such job and there is no
        # receipt.  That is not a failure and it is not filed as one: the job
        # may have been purged past MinJobAge, or the controller's memory of
        # it went with a restart while it runs on.  Same exit as giving up,
        # because the truth is the same -- no verdict yet.
        emit_display(display, f"pbrun: no scheduler command can describe slurm job "
              f"{job.job_id} for {key[:12]} and it has published no receipt; "
              f"it may still be running as slurm job {job.job_id}, or have "
              f"been purged past MinJobAge; look under {job.directory}",
              channel="stderr")
        return report.finish(action_client.ClientUnsettled(key, result.published_unix,
            action_client.UnsettledReason.SCHEDULER_UNKNOWN))
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
        emit_display(display, f"pbrun: slurm job {job.job_id} exited 0 but published no "
              f"receipt for {key[:12]}; see {job.stdout_path} and "
              f"{job.stderr_path}", channel="stderr")
        return execution(1, 'failed')
    if outcome.state == "OUT_OF_MEMORY":
        # The state alone sends an operator to a job log that a SIGKILLed
        # process never got to write.  What decided the ending is the number
        # this submission declared, which the log does not carry and the
        # caller may not have typed at all -- `mem_gb` defaults to 4.
        declared = int(demand.get("mem_gb", 0) or 0)
        emit_display(display, f"pbrun: slurm job {job.job_id} exceeded the "
              f"{declared} GiB it declared; raise it with "
              f"--demand mem_gb={max(declared * 2, 8)} (or whatever the "
              f"action really needs)", channel="stderr")
    emit_display(display, f"pbrun: failed ({outcome.state}) after {total} attempt(s); "
          f"logs {job.stdout_path} and {job.stderr_path}", channel="stderr")
    if isinstance(outcome.exit_code, int) and outcome.exit_code:
        return execution(action_client.reported_exit(outcome.exit_code, key=key), 'failed')
    return execution(1, 'failed')


def _echo(path, stream, *, display: DisplaySink = discard_display) -> None:
    """Print a job log if it is there, and say nothing when it is not.

    An absent log is normal -- a job cancelled before it started never opened
    one -- and turning that into an error would replace the reason the action
    failed with a complaint about the file that would have explained it.
    """

    try:
        emit_display(display, Path(path).read_text(encoding="utf-8", errors="replace"), channel=stream, end="")
    except OSError:
        return


def withdraw_routed(
    prefixes, *, transport: str, reason: str = "", by: str = "",
    lane_root=None, queue_root=None, scancel: str = "scancel",
    squeue: str = "squeue", queue=None,
store: FleetStore, display: DisplaySink = discard_display) -> int:
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
        store=store, display=display))
    if pool_prefixes:
        if queue is None:
            root = store.queue_root if queue_root is None else Path(queue_root)
            queue = pool.PoolQueue(root)
        rc = max(rc, withdraw_main(queue, pool_prefixes, reason=reason, by=by, store=store, display=display))
    return rc


def withdraw_slurm_main(
    prefixes, *, reason: str = "", by: str = "", lane_root=None,
    queue_root=None, scancel: str = "scancel", squeue: str = "squeue",
store: FleetStore, display: DisplaySink = discard_display) -> int:
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
