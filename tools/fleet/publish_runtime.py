#!/usr/bin/env python3
"""Publish this checkout as the bytes the fleet executes, and say which commit.

Workers do not run this repository.  They run ``/mnt/shared/prismabuild-fleet/
repo`` -- a copy on the one filesystem every box mounts -- and until now that
copy was maintained by hand.  The failure that follows from a hand-copied
runtime is not subtle: a fix lands in the checkout, the fleet keeps executing
the old bytes, and the two disagree silently because nothing anywhere records
which commit the mirror is.  Thirty-two resubmissions died instantly on
``AttributeError: 'PoolQueue' object has no attribute 'placeable'`` -- a
method committed twenty minutes earlier, in a file the mirror did not have.

So publication becomes a complete immutable generation with a receipt.
``RUNTIME_VERSION.json`` records the commit, whether the tree was dirty, and
a sha256 per published file next to the bytes themselves; the generation is
copied, revalidated and import-probed off-line, then ``repo`` moves to it in
one namespace operation.  A caller therefore sees one complete generation,
never the member-by-member interval a receipt could not attest.  Worker
attestation already records the sha256 of what it loaded, so the two can be
compared after the fact and a mismatch is a fact rather than a suspicion.

The mirror keeps its scripts at the top of ``tools/`` while the checkout has
them under ``tools/fleet/``, because the hook and older command lines address
the flat path.  Both are written, from the same source, rather than left to
drift into two versions of one file -- which is exactly what happened to
``worker_loop.py``, where the flat copy was current and the nested one was
three days stale.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid

CHECKOUT = Path(__file__).resolve().parents[2]
MIRROR = Path("/mnt/shared/prismabuild-fleet/repo")
#: Published as ``tools/<name>`` *and* ``tools/fleet/<name>``.
FLEET_SCRIPTS = (
    "docker", "pbrun.py", "pbtest.py", "require_pool.py", "worker_loop.py", "worker.py",
    "render_identity.py", "seal_and_publish.py", "tessera_status.py",
    "dispatch_tessera_shards.py", "dispatch_tessera_ladder.py",
    "publish_runtime.py", "pool_reset.py", "runtime_paths.py", "supervise.py",
    # The SLURM lane's two halves: the shared submit every producer routes
    # through, and the job entry it names.  A runtime published without them
    # has producers importing a module that is not there and a batch script
    # execing a path that does not exist.
    "fleet_submit.py", "slurm_job.py",
    # The commands an operator runs on a box that has no checkout.  The
    # published pool_reset.py:582 tells them to run ``pbwait.py <key>``, and
    # neither dl380g10 nor sparklina has a checkout to run it from.
    # runtime_process_census.py is here for the same reason and is only
    # useful there: it reads /proc on the box it runs on.
    "pbstatus.py", "pbwait.py", "pbcampaign.py", "runtime_process_census.py",
)
#: Fleet tools deliberately left out of the generation, each with the reason.
#: Empty: every tool under ``tools/fleet`` is something a box with no checkout
#: may have to run. The tuple exists so that leaving one out is a decision
#: somebody wrote down rather than an omission nobody noticed.
EXCLUDED: tuple[tuple[str, str], ...] = ()

#: Not code, but read by published code: the supervisor on each box reads the
#: fleet's declared shape from here, so a runtime published without it starts
#: no workers at all.
FLEET_DATA = ("fleet_boxes.json",)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_result(*argv: str):
    return subprocess.run(
        ["git", "-C", str(CHECKOUT), *argv],
        capture_output=True, text=True, check=False,
    )


def _commit_identity() -> str:
    """The exact Git commit the published bytes claim, or refuse.

    A linked worktree under the shared mount can still point its ``.git`` file
    at a box-local control directory.  On another box ``git rev-parse`` then
    exits 128; treating its empty stdout as a commit published real bytes under
    ``commit: ""``.  A receipt with no identity is not an approximate receipt.
    """

    result = _git_result("rev-parse", "--verify", "HEAD")
    commit = result.stdout.strip()
    if result.returncode != 0 or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        detail = (result.stderr or result.stdout or "no output").strip()
        raise SystemExit(
            "cannot prove a 40-hex Git commit for runtime publication: "
            f"git rev-parse --verify HEAD exited {result.returncode}: {detail}"
        )
    return commit


def _working_tree_dirty() -> bool:
    # Every file under the published source/script/test roots is eligible for
    # the generation below, including a newly-created one Git does not yet
    # track.  Excluding untracked paths here could therefore put bytes absent
    # from ``commit`` into a receipt that claimed ``dirty: false``.
    result = _git_result("status", "--porcelain")
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "no output").strip()
        raise SystemExit(
            "cannot prove whether the runtime checkout is clean: "
            f"git status exited {result.returncode}: {detail}"
        )
    return bool(result.stdout.strip())


def _source_for(name: str) -> Path:
    if name.startswith("tools/fleet/"):
        base = name.rsplit("/", 1)[1]
        source = CHECKOUT / "tools" / "fleet" / base
        return source if source.is_file() else CHECKOUT / "tools" / base
    if name == "tools/prismabuild_worker.py":
        return CHECKOUT / "tools" / "prismabuild_worker.py"
    if name.startswith("tools/"):
        base = name.rsplit("/", 1)[1]
        source = CHECKOUT / "tools" / "fleet" / base
        return source if source.is_file() else CHECKOUT / "tools" / base
    return CHECKOUT / name


def _publication_manifest() -> dict[str, str]:
    published: dict[str, str] = {}
    for source in sorted((CHECKOUT / "src" / "prismabuild").glob("*.py")):
        published[f"src/prismabuild/{source.name}"] = _sha256(source)
    for name in FLEET_SCRIPTS:
        source = CHECKOUT / "tools" / "fleet" / name
        if not source.is_file():
            source = CHECKOUT / "tools" / name
        if not source.is_file():
            continue
        published[f"tools/{name}"] = _sha256(source)
        published[f"tools/fleet/{name}"] = published[f"tools/{name}"]
    for name in FLEET_DATA:
        source = CHECKOUT / "tools" / "fleet" / name
        if source.is_file():
            published[f"tools/{name}"] = _sha256(source)
            published[f"tools/fleet/{name}"] = published[f"tools/{name}"]
    worker = CHECKOUT / "tools" / "prismabuild_worker.py"
    if worker.is_file():
        published["tools/prismabuild_worker.py"] = _sha256(worker)
    for source in sorted((CHECKOUT / "tests").glob("*.py")):
        published[f"tests/{source.name}"] = _sha256(source)
    return published


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            # Some NFS servers decline directory fsync.  Every regular member
            # is still flushed before the namespace operation below.
            pass
    finally:
        os.close(descriptor)


def _write_receipt(path: Path, receipt: dict[str, object]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=1)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _probe(root: Path) -> None:
    probe = subprocess.run(
        [sys.executable, "-c",
         "import sys; sys.path.insert(0, r'%s'); "
         "from prismabuild import pool, core; "
         "assert hasattr(pool.PoolQueue, 'claim'); print('import ok')"
         % (root / "src")],
        capture_output=True, text=True, check=False,
    )
    print((probe.stdout or probe.stderr).strip())
    if probe.returncode != 0:
        raise SystemExit(
            f"refusing to activate a runtime that failed its import probe "
            f"with status {probe.returncode}"
        )


def _seal_generation(root: Path) -> None:
    """Make accidental mutation fail; generations are append-only history."""

    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_symlink():
            continue
        path.chmod(path.stat().st_mode & ~0o222)
    root.chmod(root.stat().st_mode & ~0o222)


def _unseal_tree(root: Path) -> None:
    """Undo ``_seal_generation`` on a tree that is still private to us."""

    for path in [root, *root.rglob("*")]:
        if path.is_symlink():
            continue
        path.chmod(path.stat().st_mode | 0o200)


def _remove_staging_tree(stage: Path) -> None:
    """Remove a private ``.staging`` tree even after it was sealed.

    ``_seal_generation`` runs before the rename, so a failure between the two
    leaves a read-only tree that plain ``rmtree`` cannot delete, and the leak
    sits under ``runtime-generations`` forever (issue #34).  Only the private
    staging target is ever unsealed here; a published generation is never
    touched.  This runs from a ``finally``: a cleanup failure is reported and
    swallowed so the publication error that caused it stays the exception the
    caller sees.
    """

    if stage.suffix != ".staging":
        raise ValueError(f"refusing to remove a non-staging tree: {stage}")
    try:
        _unseal_tree(stage)
        shutil.rmtree(stage)
    except OSError as exc:
        print(
            f"warning: staging tree survived failed publication: {stage}: {exc}",
            file=sys.stderr,
        )


def _activate(generation: Path, *, migrate_directory: bool) -> Path | None:
    """Expose ``generation`` at MIRROR in one namespace operation.

    Once MIRROR is a symlink, replacing a prepared sibling symlink is one
    atomic operation.  Linux ``RENAME_EXCHANGE`` would make the one-time move
    from the historical non-empty directory atomic too, but the fleet's NFS
    mount rejects that operation with ``EINVAL``.  That migration therefore
    requires an explicit maintenance flag: first retain the old directory,
    then install the complete symlink, rolling the old name back if install
    fails.  A caller can be refused in that narrow interval; it can never read
    a mixed generation.  Publication never deletes a prior generation.
    """

    MIRROR.parent.mkdir(parents=True, exist_ok=True)
    candidate = MIRROR.parent / f".{MIRROR.name}.activate-{uuid.uuid4().hex}"
    target = os.path.relpath(generation, MIRROR.parent)
    os.symlink(target, candidate)
    _fsync_directory(MIRROR.parent)
    legacy: Path | None = None
    try:
        if MIRROR.is_symlink() or not MIRROR.exists():
            os.replace(candidate, MIRROR)
        elif MIRROR.is_dir():
            if not migrate_directory:
                raise SystemExit(
                    f"{MIRROR} is the legacy directory runtime; refusing its "
                    "one-time maintenance handoff without --migrate-directory"
                )
            legacy = generation.parent / (
                f"legacy-{int(time.time())}-{uuid.uuid4().hex[:8]}"
            )
            os.replace(MIRROR, legacy)
            try:
                os.replace(candidate, MIRROR)
            except BaseException:
                if not MIRROR.exists() and not MIRROR.is_symlink():
                    os.replace(legacy, MIRROR)
                raise
        else:
            raise SystemExit(
                f"refusing to replace live runtime {MIRROR}: expected a "
                "directory or symlink"
            )
        _fsync_directory(MIRROR.parent)
    finally:
        if candidate.is_symlink():
            candidate.unlink()
    return legacy


def _activate_existing(name: str, *, dry_run: bool) -> int:
    """Point the live runtime at a generation that already exists.

    Rollback's whole job.  A generation is immutable and already carries a
    receipt proving its bytes, so restoring one is a namespace operation and
    nothing else -- no copy, no re-hash, no dependence on what the checkout
    happens to contain now.  Publication never deletes a generation, which is
    what makes this possible at all.

    The name is validated rather than trusted: it must be a direct child of the
    generation store, it must not be a dot-name, and it must carry a receipt
    that reads.  A path that escapes the store, a staging tree, or a directory
    that is not a published generation is refused before ``repo`` is touched.
    """

    store = MIRROR.parent / "runtime-generations"
    # A dot-name is never a generation, and one shape of it is dangerous.  A
    # publish stages at ``.<generation>.staging`` in this same store, writes
    # the receipt into it, and removes it on failure -- but that removal
    # reports an OSError and continues, so an interrupted publish can leave a
    # survivor that carries a receipt and passes every other check here.  Its
    # bytes were never sealed or probed.
    if "/" in name or name in ("", ".", "..") or name.startswith("."):
        raise SystemExit(
            f"not a generation name: {name!r}. A staging tree left behind by "
            "an interrupted publish is a dot-name and carries a receipt; it "
            "is not a generation."
        )
    generation = store / name
    if not (generation / "RUNTIME_VERSION.json").is_file():
        raise SystemExit(
            f"{generation} is not a published generation: no RUNTIME_VERSION.json"
        )
    # Rollback is the command that runs when everything else has failed, so a
    # short or damaged receipt has to be a sentence rather than a traceback.
    try:
        receipt = json.loads((generation / "RUNTIME_VERSION.json").read_text())
    except (OSError, ValueError) as exc:
        raise SystemExit(
            f"{generation}: receipt is not readable ({exc}); this is not a "
            "generation to point the live runtime at."
        ) from exc
    if not isinstance(receipt, dict):
        raise SystemExit(
            f"{generation}: receipt is not readable (not a JSON object); this "
            "is not a generation to point the live runtime at."
        )
    print(
        f"activating {name}: commit {str(receipt.get('commit', ''))[:12]}, "
        f"default transport {receipt.get('default_transport') or 'pool'}"
    )
    if dry_run:
        return 0
    _activate(generation, migrate_directory=False)
    print(f"activated {MIRROR} -> {generation}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--allow-dirty", action="store_true",
                    help="publish a tree with uncommitted changes")
    ap.add_argument(
        "--migrate-directory", action="store_true",
        help="perform the one-time fail-closed handoff from the legacy live directory",
    )
    ap.add_argument(
        "--default-transport", choices=("pool", "slurm"), default=None,
        help="the transport every pbrun and producer running THIS generation "
             "uses when nothing says otherwise; recorded in the receipt and "
             "read by fleet_submit.default_transport.  Omitted means the pull "
             "queue, which is what every generation published before the "
             "SLURM cutover means too.",
    )
    ap.add_argument(
        "--activate-generation", metavar="NAME", default=None,
        help="point the live runtime at an existing generation instead of "
             "publishing a new one; this is what rollback does, and it is "
             "deliberately not a re-publish -- the previous generation's bytes "
             "and receipt are already proved and a rebuild from a moved "
             "checkout would not be the same thing.",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.activate_generation is not None:
        return _activate_existing(args.activate_generation, dry_run=args.dry_run)

    # Identity is established before MIRROR is even enumerated, much less
    # touched.  Failure here is a refusal, never an empty field in a receipt.
    commit = _commit_identity()
    dirty = _working_tree_dirty()
    if dirty and not args.allow_dirty:
        raise SystemExit(
            "refusing to publish a dirty tree: the receipt would name a commit "
            "whose bytes are not the bytes published.  Commit, or pass "
            "--allow-dirty and accept that the version is only approximate."
        )

    published = _publication_manifest()

    print(f"publishing {len(published)} files from {commit[:12]}"
          f"{' (dirty)' if dirty else ''} to {MIRROR}")
    if args.dry_run:
        for name in sorted(published):
            print(f"  {name}")
        return 0

    store = MIRROR.parent / "runtime-generations"
    nonce = uuid.uuid4().hex[:12]
    generation_name = f"{commit[:12]}-{int(time.time())}-{nonce}"
    stage = store / f".{generation_name}.staging"
    generation = store / generation_name
    # The same refusal shape as the dirty-tree refusal above, and for the same
    # reason: these two directories are the first bytes written, so a store
    # this user cannot write is a fact to state before "publishing N files"
    # rather than a PermissionError traceback after it.  The live runtime is
    # untouched either way.
    try:
        store.mkdir(parents=True, exist_ok=True)
        stage.mkdir()
    except OSError as exc:
        raise SystemExit(
            f"cannot write the generation store {store}: {exc}.  Nothing was "
            "published and the live runtime still points where it did."
        ) from exc
    activated = False
    try:
        for name, expected in sorted(published.items()):
            source = _source_for(name)
            target = stage / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            _fsync_file(target)
            actual = _sha256(target)
            if actual != expected:
                raise SystemExit(
                    f"copied runtime member changed at {name}: expected "
                    f"{expected}, staged {actual}"
                )

        # A checkout can advance while a slow NFS copy is in progress.  Hash
        # the complete source set again and re-prove both Git facts before a
        # receipt names the staged bytes.
        if _commit_identity() != commit or _working_tree_dirty() != dirty:
            raise SystemExit(
                "runtime checkout identity moved while publication was staged"
            )
        if _publication_manifest() != published:
            raise SystemExit(
                "runtime source bytes moved while publication was staged"
            )

        receipt: dict[str, object] = {
            "schema": "prismaquant.prismabuild.runtime_version.v1",
            "commit": commit,
            # Optional, and absent means the pull queue: every generation
            # published before the SLURM cutover has no such field and must
            # keep behaving as it did.
            **({"default_transport": args.default_transport}
               if args.default_transport else {}),
            "dirty": dirty,
            "generation": generation_name,
            "published_unix": time.time(),
            "published_by": socket.gethostname(),
            "files": published,
        }
        _write_receipt(stage / "RUNTIME_VERSION.json", receipt)
        _probe(stage)
        _seal_generation(stage)
        _fsync_directory(stage)
        os.replace(stage, generation)
        _fsync_directory(store)
        legacy = _activate(
            generation, migrate_directory=args.migrate_directory
        )
        activated = True
        print(f"activated {MIRROR} -> {generation}")
        if legacy is not None:
            print(f"retained previous directory runtime at {legacy}")
        return 0
    finally:
        if not activated and stage.exists():
            _remove_staging_tree(stage)


if __name__ == "__main__":
    raise SystemExit(main())
