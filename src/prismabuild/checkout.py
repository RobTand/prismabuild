"""Address an action by the tree it runs against, instead of by a path.

An action carries ``checkout_root``, an absolute path.  When that path is a
box-local worktree -- ``/home/rob/tmp/ts101``, which is what an agent naturally
makes -- the action must be tagged to the box that holds it or a worker
elsewhere claims a tree it cannot see.  So the action can only ever run on one
box, and the fleet is three boxes wide and one box deep.  Measured on the live
queue 2026-09-04 07:15: ``ready 22, 22 on exactly one box (sparky 22), 22 by a
box-local checkout, 0 on more than one`` -- with sparklina holding a free GPU
and dl380g10 eighty free cores.

This module is the other addressing.  The submitter names a **tree**; the
claiming worker materialises that tree under its own local scratch and runs
there.  Nothing about the work changes; what changes is that the *location*
stops being part of the request, so three boxes can each answer it.

Four things are deliberate.

*The identity is the tree, not the commit.*  ``git commit-tree`` bakes the
committer and the clock into its object, so two submits of an unchanged tree a
second apart are two different commits -- and binding a commit would make every
resubmit a CAS miss, which is the one property the action key exists to keep.
The tree sha is the content of the checkout and nothing else, so it is what the
key, the stamp and the closure bind.  The commit is transport: it is what
carries the tree through ``git`` and what names the ref.

*A dirty tree is the normal case, so the commit is synthesised.*  Agents submit
from dirty trees constantly and ``pbrun`` has a whole delta digest for it.
Requiring a clean tree would make this unusable, so the tree is written from
the working directory through a scratch index -- never the caller's index,
never a branch, never a stash (``git stash create`` drops untracked files, and
``pbrun`` learned the hard way that an untracked edit must move the action
key).

*Objects are shared; trees are not.*  ``/mnt/shared`` is NFSv4 mounted
``local_lock=none``: a dozen agents building in worktrees on it is slow and a
locking hazard, and ``TRITON_CACHE_DIR`` must stay box-local whatever else
moves.  Git objects are write-once and read-only afterwards, which is a very
different load, so the bare repository is shared and every working tree is
local to the box that built it.

*The closure keeps its teeth, and keeps them by derivation.*  The stamp the
worker verifies is not copied from the action: the materialiser recomputes the
tree sha **from the worktree it has just built** and writes that.  A worktree
that landed on the wrong tree therefore produces different bytes, and
``core.verify_code_closure`` refuses.  That is the difference between a check
and a receipt.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import time
import uuid

#: Announced by a worker whose bytes can materialise a tree, and read by
#: ``pbrun`` before it addresses an action by one.  This is the whole migration
#: mechanism: a box running older bytes announces nothing, the submitter sees
#: that and stays path-addressed, and the fleet converts itself one publish at
#: a time without anybody sequencing it by hand.  It is a capability the box
#: states about itself, not a version number a submitter interprets.
CHECKOUT_COMMIT_CAPABILITY = "checkout_commit"

#: The bare repositories every box can read.  Objects only -- see the module
#: docstring for why no working tree lives here.
SHARED_GIT_ROOT = Path(os.environ.get(
    "PRISMABUILD_SHARED_GIT_ROOT", "/mnt/shared/prismabuild-fleet/git"))
#: This box's own mirror of them.  ``git worktree add`` writes administrative
#: state into the repository it is run from, so pointing every box's worktrees
#: at the shared bare repo would put concurrent per-box writes back on NFS --
#: which is the thing being avoided.
MIRROR_ROOT = Path(os.environ.get(
    "PRISMABUILD_GIT_MIRROR_ROOT", "/home/rob/.cache/prismabuild/git"))
#: Where materialised working trees live.  Box-local by construction, and
#: disposable: results travel through the CAS, never through the tree.
TREES_ROOT = Path(os.environ.get(
    "PRISMABUILD_TREES_ROOT", "/home/rob/tmp/pb-trees"))

#: Dropped at the top of every tree this module creates.  The sweep removes
#: only trees carrying it -- ``worktrees under dq-runs are experiment pins`` is
#: the standing reminder that a removal sweep is the dangerous kind of tidy,
#: and the rule that came out of it is that a sweep may only remove what it can
#: prove it made.
TREE_MARKER = ".pbrun-materialised.json"

#: Sibling of a tree being built, held only between ``worktree add`` and the
#: marker landing inside it.  Its presence says "this box was interrupted
#: mid-build", which is the one case where a marker-less tree at the
#: deterministic path is safe to remove.
BUILDING_SUFFIX = ".building"

#: How many trees per repository the sweep keeps.  Small because a tree is a
#: full checkout and the box is at 86% of 1.8 TB with a 10% floor; larger than
#: one because a fan-out of shards at one tree must all reuse it.
DEFAULT_KEEP_TREES = 6

#: The upper bound on what a submit may add to the object store, in bytes.
#: A synthesised tree of a checkout holding a 90 GB cache is not a submission,
#: it is an accident, and the object store is shared.
DEFAULT_MAX_ADD_BYTES = 2 * 1024 ** 3

#: How long to wait for another loop on this box that is materialising the
#: same tree, and how old its lock must be before it is treated as abandoned.
LOCK_WAIT_S = 900.0
LOCK_STALE_S = 1800.0

#: How often a step with no local upper bound -- the first fetch, the worktree
#: build -- refreshes the lease while it runs.  Well under ``pool``'s 300 s
#: ``LEASE_TIMEOUT_S``, because the point is to never be silent for that long.
HEARTBEAT_EVERY_S = 20.0

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


class CheckoutError(RuntimeError):
    """A tree could not be named, published or materialised."""


def stamp_bytes(tree_sha: str) -> bytes:
    """The closure stamp's exact bytes for a tree.

    One serialiser, called by the submitter and by the materialiser, because
    the check IS a byte-for-byte comparison: an encoding that lived in two
    places would be a way for the two sides to disagree about a tree they
    agree on.  ``indent=1, sort_keys=True`` matches what ``pbrun`` has always
    written, so nothing about the stamp's shape is new.
    """

    if not _SHA_RE.match(str(tree_sha)):
        raise CheckoutError(f"tree sha must be 40 hex characters, got {tree_sha!r}")
    return json.dumps({"checkout_tree": str(tree_sha)},
                      indent=1, sort_keys=True).encode("utf-8")


def _run_while_beating(argv: Sequence[str], *, beat: Callable[[], None],
                       timeout: float, every: float = HEARTBEAT_EVERY_S) -> None:
    """Run one command that can take minutes, refreshing the lease while it runs.

    ``_git`` blocks inside ``subprocess.run`` and can call nothing while the
    child works.  Two steps here have no local upper bound -- the first fetch
    of a whole history across the shared mount, and the ``worktree add`` that
    writes it out -- and a lease that goes silent for ``LEASE_TIMEOUT_S`` is
    reaped, which requeues an action that is *running*: the same tree then
    materialises on a second box and the work runs twice.  Beating around the
    call, as a first version did, bounds nothing; the beat has to happen
    *during* it.

    The step this guards has never actually been slow on the repositories in
    hand -- tessera's whole history is 5.9 MB packed and clones in 0.46 s --
    so this is not a fix for an observed hang.  It is here because "has not
    been slow yet" is not a bound, and the failure it would buy is a double
    run rather than a slow one.
    """

    # Its own session, so the timeout below can end the whole tree of
    # processes: ``git fetch`` runs a transport child, and killing only the
    # one we spawned leaves that child alive, holding the pipes.  Measured,
    # because it is not obvious: ``kill()`` followed by ``communicate()`` on a
    # ``sh -c 'sleep 30'`` returned after 29.8 s -- the parent was dead within
    # microseconds and the read waited for the orphan.  A timeout that can
    # hang is not a timeout, so the group is signalled and the pipes are never
    # read again after it.
    proc = subprocess.Popen(list(argv), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True,
                            start_new_session=True)
    deadline = time.monotonic() + float(timeout)
    out = err = ""
    try:
        while True:
            try:
                out, err = proc.communicate(timeout=max(0.01, float(every)))
                break
            except subprocess.TimeoutExpired:
                if time.monotonic() > deadline:
                    raise CheckoutError(
                        f"{' '.join(argv)} did not finish within {timeout}s")
                beat()
    finally:
        if proc.poll() is None:
            _kill_group(proc)
    if proc.returncode != 0:
        raise CheckoutError(
            f"{' '.join(argv)} exited {proc.returncode}: "
            f"{(err or out).strip()[:600]}")


def _kill_group(proc: subprocess.Popen) -> None:
    """End a timed-out child and everything it started, without reading it."""

    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):        # pragma: no cover
        proc.kill()
    for stream in (proc.stdout, proc.stderr):
        if stream is not None:
            try:
                stream.close()
            except OSError:                              # pragma: no cover
                pass
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:                    # pragma: no cover
        pass


def _git_while_beating(*args: str, cwd: str | Path | None = None,
                       beat: Callable[[], None], timeout: float,
                       every: float = HEARTBEAT_EVERY_S) -> None:
    argv = ["git"]
    if cwd is not None:
        argv += ["-C", str(cwd)]
    _run_while_beating(argv + list(args), beat=beat, timeout=timeout, every=every)


def _git(*args: str, cwd: str | Path | None = None,
         env: Mapping[str, str] | None = None, timeout: float = 600.0,
         check: bool = True) -> str:
    argv = ["git"]
    if cwd is not None:
        argv += ["-C", str(cwd)]
    argv += list(args)
    environ = dict(os.environ)
    if env:
        environ.update(env)
    out = subprocess.run(argv, capture_output=True, text=True,
                         timeout=timeout, env=environ)
    if check and out.returncode != 0:
        raise CheckoutError(
            f"{' '.join(argv)} exited {out.returncode}: "
            f"{(out.stderr or out.stdout).strip()[:600]}")
    return out.stdout


# -- submit side ---------------------------------------------------------


def repo_identity(cwd: str | Path) -> dict[str, str] | None:
    """Name the repository ``cwd`` sits in, box-independently.  ``None`` if none.

    The name has to be the same string on every box, so it cannot contain any
    part of the checkout's path: ``/home/rob/tessera`` and
    ``/mnt/shared/tessera-x86`` are the same repository at two paths, and an
    agent's ``/home/rob/tmp/ts101`` is a third.  It is the **root commit**
    alone -- the one thing every clone of a repository shares and no two
    repositories share.  A first version put the basename in front of it for
    readability and a test caught what that cost: two worktrees of one
    repository produced two identities, two bare repositories and two action
    keys for one request, which is the exact defect this addressing exists to
    remove.  The readable name is kept where it costs nothing -- the bare
    repository's ``description`` and each tree's marker.

    One scope limit, recorded rather than papered over: a **shallow** clone has
    no root commit reachable, so ``rev-list --max-parents=0`` answers with its
    shallow boundary and such a checkout gets its own identity.  Agents make
    worktrees of full clones, so this has never been the case in hand; it is a
    limit on the claim, not a bug in it.

    ``prefix`` is the submitter's directory *within* the repository.  A submit
    from a subdirectory is ordinary, the tree is always written from the
    toplevel, and without carrying the prefix the worker would run the action
    one or more directories above where it was asked for.
    """

    try:
        toplevel = _git("rev-parse", "--show-toplevel", cwd=cwd,
                        timeout=30, check=False).strip()
    except (OSError, subprocess.SubprocessError):
        return None
    if not toplevel:
        return None
    head = _git("rev-parse", "HEAD", cwd=toplevel, timeout=30, check=False).strip()
    if not _SHA_RE.match(head):
        # An unborn HEAD.  There is no history to parent a synthesised commit
        # onto and no root commit to name the repository by, so this checkout
        # stays path-addressed and says so, rather than being given half an
        # identity.
        return None
    roots = [line for line in _git("rev-list", "--max-parents=0", "HEAD",
                                   cwd=toplevel, timeout=120,
                                   check=False).split()
             if _SHA_RE.match(line)]
    if not roots:
        return None
    # A repository may have several root commits (a grafted import, a merged
    # history).  Sorting makes the choice the same on every box, which is the
    # only property this needs.
    root = sorted(roots)[0]
    prefix = _git("rev-parse", "--show-prefix", cwd=cwd,
                  timeout=30, check=False).strip().strip("/")
    return {
        "toplevel": toplevel,
        "prefix": prefix,
        "root_commit": root,
        "head": head,
        "repo": root,
        "name": _NAME_RE.sub("-", Path(toplevel).name).strip("-") or "repo",
    }


def pending_add_bytes(toplevel: str | Path) -> int:
    """How many bytes a synthesised tree would add to the object store.

    Measured as what ``git add -A`` would actually stage -- untracked files git
    is not ignoring, plus modified tracked ones -- and deliberately not as the
    size of the directory.  The 90 GB cache this bound exists to catch is
    normally ``.gitignore``d and never enters a tree at all, so a ``du`` would
    refuse the submissions that are fine and say nothing about the one that is
    not.
    """

    total = 0
    root = Path(toplevel)
    listed = _git("ls-files", "--others", "--exclude-standard", "-z",
                  cwd=root, timeout=600, check=False)
    modified = _git("diff", "--name-only", "-z", "HEAD",
                    cwd=root, timeout=600, check=False)
    for name in {n for n in (listed + modified).split("\0") if n}:
        member = root / name
        try:
            if member.is_symlink() or not member.is_file():
                continue
            total += member.stat().st_size
        except OSError:
            continue
    return total


def synthesise_tree_commit(toplevel: str | Path, *, scratch: str | Path,
                           message: str = "pbrun") -> tuple[str, str]:
    """Write the working tree as a git tree, and a commit carrying it.

    Returns ``(tree_sha, commit_sha)``.  Touches no branch, no stash and not
    the caller's index: the index is a scratch file named by ``GIT_INDEX_FILE``
    and thrown away, so a submit is invisible to whatever the agent is doing in
    that checkout at the time.

    The index is **seeded from HEAD** before ``add -A``.  Starting from an
    empty one looks equivalent and is not: a tracked file that also matches an
    ignore rule is skipped by ``add`` and would silently vanish from the tree,
    which is a wrong tree that no later check can catch, because every later
    check compares against this one.

    Exclusions come from ``.git/info/exclude``, which ``pbrun`` writes before
    calling this, so the stamp and the result logs are skipped for free.  They
    have to be: a submit that staged its own droppings would produce a new tree
    every time and never hit the CAS again.
    """

    index = Path(scratch) / f"index.{os.getpid()}.{uuid.uuid4().hex[:8]}"
    index.parent.mkdir(parents=True, exist_ok=True)
    env = {"GIT_INDEX_FILE": str(index)}
    try:
        _git("read-tree", "HEAD", cwd=toplevel, env=env, timeout=600)
        _git("add", "-A", cwd=toplevel, env=env, timeout=1800)
        tree = _git("write-tree", cwd=toplevel, env=env, timeout=600).strip()
    finally:
        index.unlink(missing_ok=True)
    if not _SHA_RE.match(tree):
        raise CheckoutError(f"git write-tree produced {tree!r}")
    head = _git("rev-parse", "HEAD", cwd=toplevel, timeout=30).strip()
    # The commit is transport and nothing else -- the identity is the tree --
    # so its author, committer and dates are fixed rather than read from the
    # environment.  A varying commit for an unchanged tree would churn a ref
    # per submit in the shared repository for no gain.
    env = {
        "GIT_AUTHOR_NAME": "prismabuild", "GIT_AUTHOR_EMAIL": "pbrun@localhost",
        "GIT_COMMITTER_NAME": "prismabuild", "GIT_COMMITTER_EMAIL": "pbrun@localhost",
        "GIT_AUTHOR_DATE": "@0 +0000", "GIT_COMMITTER_DATE": "@0 +0000",
    }
    commit = _git("commit-tree", tree, "-p", head, "-m", f"{message} {tree}",
                  cwd=toplevel, env=env, timeout=120).strip()
    if not _SHA_RE.match(commit):
        raise CheckoutError(f"git commit-tree produced {commit!r}")
    return tree, commit


def ensure_shared_bare(repo: str, *, name: str = "",
                       root: str | Path | None = None) -> Path:
    """The shared bare repository for ``repo``, created once by whoever is first.

    Built in a private directory and moved into place, because ``git init`` is
    not atomic and two submitters racing it would each see a half-made
    repository.  ``rename`` onto a non-empty directory fails with ENOTEMPTY,
    which is exactly the signal "somebody else finished first" -- the same
    primitive the queue's claim is built on.

    Auto-gc is turned off on the receiving side.  Every submitter pushes a
    *different* ref name here, so the contended write is not the ref but the
    repacking a gc would start under it.
    """

    base = Path(root if root is not None else SHARED_GIT_ROOT)
    target = base / f"{repo}.git"
    if (target / "HEAD").exists():
        _describe(target, name)
        return target
    base.mkdir(parents=True, exist_ok=True)
    tmp = base / f".{repo}.{socket.gethostname()}.{os.getpid()}.{uuid.uuid4().hex[:8]}"
    try:
        _git("init", "--bare", "--quiet", str(tmp), timeout=120)
        _git("config", "gc.auto", "0", cwd=tmp, timeout=30)
        _git("config", "receive.autogc", "false", cwd=tmp, timeout=30)
        try:
            os.rename(tmp, target)
        except OSError:
            shutil.rmtree(tmp, ignore_errors=True)
    finally:
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
    if not (target / "HEAD").exists():
        raise CheckoutError(f"could not create the shared bare repository {target}")
    _describe(target, name)
    return target


def _describe(bare: Path, name: str) -> None:
    """Put a human name on a repository whose directory name is a root commit.

    ``description`` is git's own field for this and nothing reads it as data,
    which is exactly the property wanted: the identity is the root commit and
    must stay so, and a person listing the shared git root still gets to see
    what they are looking at.  Best effort, always -- a naming courtesy has no
    business failing a submission.
    """

    if not name:
        return
    try:
        target = bare / "description"
        if target.read_text(encoding="utf-8").strip() == name:
            return
        target.write_text(f"{name}\n", encoding="utf-8")
    except OSError:
        pass


def ref_for(commit: str) -> str:
    return f"refs/pbrun/{commit}"


def publish_tree_commit(toplevel: str | Path, commit: str, origin: str | Path) -> str:
    """Push one synthesised commit to the shared bare repository.

    Returns the ref it now lives under.  The ref name is the commit, so
    concurrent submitters never write the same one and the push is idempotent:
    re-pushing an unchanged tree is a no-op the other boxes cannot observe.
    """

    ref = ref_for(commit)
    _git("push", "--quiet", str(origin), f"{commit}:{ref}",
         cwd=toplevel, timeout=1800)
    return ref


# -- worker side ---------------------------------------------------------


def item_checkout_commit(item: Mapping[str, object]) -> str:
    return str(item.get("checkout_commit") or "")


def item_is_box_local(item: Mapping[str, object]) -> bool:
    """Does this item's checkout exist on exactly one box?

    The one rule, so the pin and the measurement of the pin cannot describe
    different fleets -- the reason ``pool.is_box_local_path`` exists in one
    place already.  A commit-addressed item answers False whatever its
    ``checkout_root`` says, because that field is the submitter's own path
    kept for diagnosis and is not what the worker runs against.
    """

    from . import pool                                   # local: pool imports us

    if item_checkout_commit(item):
        return False
    return pool.is_box_local_path(item.get("checkout_root"))


def _hold_lock(path: Path, *, wait_s: float, stale_s: float,
               heartbeat: Callable[[], None] | None) -> bool:
    """Take a per-tree lock, or report that somebody else has it.

    ``O_CREAT|O_EXCL`` because that is the primitive this fleet already trusts
    for minting a token.  A lock older than ``stale_s`` is taken over: the
    holder is a worker loop on this box, and a loop that died mid-fetch must
    not wedge every later action at that tree.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + wait_s
    while True:
        try:
            descriptor = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            with os.fdopen(descriptor, "w") as handle:
                handle.write(f"{socket.gethostname()}:{os.getpid()}\n")
            return True
        except FileExistsError:
            try:
                age = time.time() - path.stat().st_mtime
            except OSError:
                age = 0.0                      # it went away between the two
            if age > stale_s:
                path.unlink(missing_ok=True)
            if time.monotonic() > deadline:
                return False
            if heartbeat is not None:
                heartbeat()
            # Slept on EVERY retry, including the one where the lock vanished
            # between the open and the stat.  An early ``continue`` there was a
            # busy loop: the retry can lose the race again, fail the stat
            # again, and spin a core without ever reaching the deadline.
            time.sleep(1.0)


def _tree_at(worktree: Path) -> str:
    """The tree sha this worktree's HEAD carries, or "" if it is not one."""

    if not (worktree / ".git").exists():
        return ""
    try:
        return _git("rev-parse", "HEAD^{tree}", cwd=worktree,
                    timeout=60, check=False).strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _tracked_clean(worktree: Path) -> bool:
    """Are the tracked files still the ones the tree says?

    Tracked content is the code the action key pinned, so it must match.
    Untracked droppings are not checked and must not be: every action tees its
    output into the checkout, so a reused tree legitimately has files in it
    that the tree does not.
    """

    out = subprocess.run(["git", "-C", str(worktree), "diff", "--quiet", "HEAD"],
                         capture_output=True, text=True, timeout=300)
    return out.returncode == 0


def materialise(
    item: Mapping[str, object],
    *,
    mirror_root: str | Path | None = None,
    trees_root: str | Path | None = None,
    heartbeat: Callable[[], None] | None = None,
    live_commits: Callable[[], set[str]] | None = None,
) -> str:
    """Build (or reuse) this box's worktree for the item's tree, and return its root.

    ``heartbeat`` is called *during* every step that can take real time, not
    around it.  It is not decoration: the first fetch into an empty mirror
    pulls a repository's whole history across NFS, the lease expires after
    300 s, and a lease that expires under a fetch is requeued and run twice.
    A first version beat before and after each call, which bounds nothing --
    the process is blocked inside the call and can beat nothing while it is;
    the two unbounded steps run under ``_run_while_beating`` instead.

    ``live_commits`` answers which trees are executing on this box right now,
    and is the guard on the one destructive branch -- a worktree whose tracked
    files no longer match its own tree is rebuilt, and rebuilt only when
    nothing is running in it.
    """

    def beat() -> None:
        if heartbeat is not None:
            heartbeat()

    commit = item_checkout_commit(item)
    tree = str(item.get("checkout_tree") or "")
    repo = str(item.get("checkout_repo") or "")
    origin = str(item.get("checkout_origin") or "")
    prefix = str(item.get("checkout_prefix") or "").strip("/")
    stamp = str(item.get("checkout_stamp") or "")
    if not (_SHA_RE.match(commit) and _SHA_RE.match(tree) and repo and origin):
        raise CheckoutError(
            "a commit-addressed item needs checkout_commit, checkout_tree, "
            f"checkout_repo and checkout_origin; got {commit!r} {tree!r} "
            f"{repo!r} {origin!r}")

    mirrors = Path(mirror_root if mirror_root is not None else MIRROR_ROOT)
    trees = Path(trees_root if trees_root is not None else TREES_ROOT)
    mirror = mirrors / f"{repo}.git"
    worktree = trees / repo / commit
    checkout_root = worktree / prefix if prefix else worktree

    def _write_stamp() -> None:
        """Derive the stamp from the tree that is here, never from the item.

        This is what makes the closure a check rather than a receipt: the sha
        below is read out of the built worktree, so a tree that is not the one
        the action pinned writes different bytes and
        ``core.verify_code_closure`` refuses it.
        """

        if not stamp:
            return
        actual = _tree_at(worktree)
        if not actual:
            raise CheckoutError(f"{worktree} is not a git worktree")
        target = checkout_root / stamp
        target.parent.mkdir(parents=True, exist_ok=True)
        scratch = target.parent / f".{target.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}"
        with scratch.open("wb") as handle:
            handle.write(stamp_bytes(actual))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(scratch, target)

    def _reusable() -> bool:
        return _tree_at(worktree) == tree and _tracked_clean(worktree)

    if _reusable():
        # Touch the marker so the sweep's "least recently used" means used and
        # not merely made.
        marker = worktree / TREE_MARKER
        try:
            os.utime(marker, None)
        except OSError:
            pass
        _write_stamp()
        return str(checkout_root)

    lock = trees / repo / ".locks" / f"{commit}.lock"
    building = trees / repo / f"{commit}{BUILDING_SUFFIX}"
    if not _hold_lock(lock, wait_s=LOCK_WAIT_S, stale_s=LOCK_STALE_S,
                      heartbeat=heartbeat):
        # Somebody on this box has held the lock longer than a fetch should
        # take.  If they finished, use what they built; otherwise say so
        # rather than build a second copy beside theirs.
        if _reusable():
            _write_stamp()
            return str(checkout_root)
        raise CheckoutError(
            f"another loop on this box has held {lock} for over {LOCK_WAIT_S:.0f}s "
            f"and {worktree} is not usable")
    try:
        if not _reusable():
            if worktree.exists():
                running = live_commits() if live_commits is not None else set()
                if commit in running:
                    raise CheckoutError(
                        f"{worktree} does not match tree {tree[:12]} and an "
                        f"action is running in it; refusing to rebuild it "
                        f"under a live claim")
                if not (worktree / TREE_MARKER).exists() and not building.exists():
                    raise CheckoutError(
                        f"{worktree} exists and carries no {TREE_MARKER}: this "
                        f"module did not create it and will not remove it")
                _git("worktree", "remove", "--force", str(worktree),
                     cwd=mirror, timeout=600, check=False)
                shutil.rmtree(worktree, ignore_errors=True)
            beat()
            _ensure_mirror(mirror)
            beat()
            _ensure_commit(mirror, commit=commit, origin=origin, beat=beat)
            beat()
            worktree.parent.mkdir(parents=True, exist_ok=True)
            # ``prune`` first: a tree removed by the sweep or by a crash leaves
            # its administrative entry behind, and ``worktree add`` then
            # refuses the path as already registered.
            _git("worktree", "prune", cwd=mirror, timeout=300, check=False)
            # Claim the path *before* building it.  The marker can only be
            # written after ``worktree add`` returns -- ``add`` refuses a
            # non-empty path, so it cannot be placed first -- and a loop that
            # dies in between (an OOM kill, or a SIGTERM that no longer
            # reaches git now it has its own session) leaves a marker-less
            # directory at exactly the deterministic path the retry needs,
            # which the refusal above would then treat as somebody else's
            # forever.  This sibling says "ours, half-built": it is removed
            # once the marker lands, so its presence is only ever the crash.
            building.parent.mkdir(parents=True, exist_ok=True)
            building.write_text(f"{socket.gethostname()} {os.getpid()}\n",
                                encoding="utf-8")
            _git_while_beating("worktree", "add", "--detach", "--quiet",
                               str(worktree), commit, cwd=mirror, beat=beat,
                               timeout=1800)
            beat()
            actual = _tree_at(worktree)
            if actual != tree:
                raise CheckoutError(
                    f"materialised {worktree} carries tree {actual[:12] or '?'}, "
                    f"not the {tree[:12]} the action pinned")
            (worktree / TREE_MARKER).write_text(json.dumps({
                "schema": "prismaquant.prismabuild.materialised_tree.v1",
                "repo": repo, "commit": commit, "tree": tree,
                "name": str(item.get("checkout_name") or ""),
                "origin": origin, "host": socket.gethostname(),
                "created_unix": time.time(),
            }, indent=1, sort_keys=True), encoding="utf-8")
            building.unlink(missing_ok=True)
        _write_stamp()
    finally:
        lock.unlink(missing_ok=True)
    return str(checkout_root)


def _ensure_mirror(mirror: Path) -> None:
    if (mirror / "HEAD").exists():
        return
    mirror.parent.mkdir(parents=True, exist_ok=True)
    tmp = mirror.parent / f".{mirror.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}"
    try:
        _git("init", "--bare", "--quiet", str(tmp), timeout=120)
        _git("config", "gc.auto", "0", cwd=tmp, timeout=30)
        try:
            os.rename(tmp, mirror)
        except OSError:
            shutil.rmtree(tmp, ignore_errors=True)
    finally:
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
    if not (mirror / "HEAD").exists():
        raise CheckoutError(f"could not create the local mirror {mirror}")


def _ensure_commit(mirror: Path, *, commit: str, origin: str,
                   beat: Callable[[], None]) -> None:
    """Make sure this box's mirror holds the commit, fetching it if not.

    Fetched **by ref**, never by raw sha: ``git fetch <path> <sha>`` needs
    ``uploadpack.allowAnySHA1InWant`` on the serving side and is refused by
    default, while the ref the submitter pushed is always fetchable and names
    exactly one object.
    """

    have = subprocess.run(["git", "-C", str(mirror), "cat-file", "-e",
                           f"{commit}^{{commit}}"], capture_output=True,
                          text=True, timeout=120)
    if have.returncode == 0:
        return
    beat()
    ref = ref_for(commit)
    # Beaten *during*, not around: this is the one step with no local bound.
    _git_while_beating("fetch", "--quiet", "--no-tags", str(origin),
                       f"+{ref}:{ref}", cwd=mirror, beat=beat, timeout=3600)
    have = subprocess.run(["git", "-C", str(mirror), "cat-file", "-e",
                           f"{commit}^{{commit}}"], capture_output=True,
                          text=True, timeout=120)
    if have.returncode != 0:
        raise CheckoutError(
            f"fetched {ref} from {origin} and {mirror} still does not hold "
            f"{commit[:12]}")


def sweep(
    *,
    trees_root: str | Path | None = None,
    mirror_root: str | Path | None = None,
    keep: int = DEFAULT_KEEP_TREES,
    live_commits: set[str] | None = None,
    dry_run: bool = False,
) -> dict[str, object]:
    """Bound the materialised trees, and remove only trees this module made.

    Three guards, and each one is a lesson rather than a precaution.  A tree
    without ``TREE_MARKER`` is not ours and is never touched -- the 2026-09-03
    sweep that removed sixteen experiment checkouts is why that rule is
    absolute rather than heuristic.  A tree whose commit appears in a live
    claim is kept whatever its age, because the queue can be asked and
    guessing is not an option.  And what is kept is the most recently *used*,
    which is why ``materialise`` touches the marker on every reuse: a shard
    fan-out reuses one tree for hours without ever recreating it.
    """

    trees = Path(trees_root if trees_root is not None else TREES_ROOT)
    mirrors = Path(mirror_root if mirror_root is not None else MIRROR_ROOT)
    running = set(live_commits or ())
    removed: list[str] = []
    kept = 0
    skipped: list[str] = []
    if not trees.is_dir():
        return {"removed": removed, "kept": kept, "skipped": skipped}
    for repo_dir in sorted(p for p in trees.iterdir() if p.is_dir()):
        candidates: list[tuple[float, Path]] = []
        for worktree in sorted(p for p in repo_dir.iterdir() if p.is_dir()):
            marker = worktree / TREE_MARKER
            if not marker.exists():
                if worktree.name != ".locks":
                    skipped.append(str(worktree))
                continue
            if worktree.name in running:
                kept += 1
                continue
            try:
                candidates.append((marker.stat().st_mtime, worktree))
            except OSError:
                continue
        candidates.sort(reverse=True)
        kept += min(len(candidates), max(int(keep), 0))
        for _, worktree in candidates[max(int(keep), 0):]:
            removed.append(str(worktree))
            if dry_run:
                continue
            mirror = mirrors / f"{repo_dir.name}.git"
            _git("worktree", "remove", "--force", str(worktree),
                 cwd=mirror, timeout=600, check=False)
            shutil.rmtree(worktree, ignore_errors=True)
            _git("worktree", "prune", cwd=mirror, timeout=300, check=False)
    return {"removed": removed, "kept": kept, "skipped": skipped}
