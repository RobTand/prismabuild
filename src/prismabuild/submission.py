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
