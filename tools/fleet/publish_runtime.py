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

So the copy becomes a step with a receipt.  ``RUNTIME_VERSION.json`` records
the commit, whether the tree was dirty, and a sha256 per published file, next
to the bytes themselves; a worker attestation already records the sha256 of
what it loaded, so the two can be compared after the fact and a mismatch is
a fact rather than a suspicion.

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
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time

CHECKOUT = Path(__file__).resolve().parents[2]
MIRROR = Path("/mnt/shared/prismabuild-fleet/repo")
#: Published as ``tools/<name>`` *and* ``tools/fleet/<name>``.
FLEET_SCRIPTS = (
    "docker", "pbrun.py", "pbtest.py", "require_pool.py", "worker_loop.py", "worker.py",
    "render_identity.py", "seal_and_publish.py", "tessera_status.py",
    "dispatch_tessera_shards.py", "dispatch_tessera_ladder.py",
    "publish_runtime.py", "pool_reset.py", "supervise.py",
)
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--allow-dirty", action="store_true",
                    help="publish a tree with uncommitted changes")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

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

    print(f"publishing {len(published)} files from {commit[:12]}"
          f"{' (dirty)' if dirty else ''} to {MIRROR}")
    if args.dry_run:
        for name in sorted(published):
            print(f"  {name}")
        return 0

    for name in sorted(published):
        if name.startswith("tools/fleet/"):
            base = name.rsplit("/", 1)[1]
            source = CHECKOUT / "tools" / "fleet" / base
            if not source.is_file():
                source = CHECKOUT / "tools" / base
        elif name == "tools/prismabuild_worker.py":
            source = CHECKOUT / "tools" / "prismabuild_worker.py"
        elif name.startswith("tools/"):
            base = name.rsplit("/", 1)[1]
            source = CHECKOUT / "tools" / "fleet" / base
            if not source.is_file():
                source = CHECKOUT / "tools" / base
        else:
            source = CHECKOUT / name
        target = MIRROR / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    receipt = {
        "schema": "prismaquant.prismabuild.runtime_version.v1",
        "commit": commit,
        "dirty": dirty,
        "published_unix": time.time(),
        "published_by": socket.gethostname(),
        "files": published,
    }
    (MIRROR / "RUNTIME_VERSION.json").write_text(json.dumps(receipt, indent=1))
    print(f"wrote {MIRROR / 'RUNTIME_VERSION.json'}")

    # A published runtime nobody can import is worse than a stale one: the
    # error surfaces on a worker, minutes later, as a failed action.  Import
    # it here, from the mirror, before anything is asked to run it.
    probe = subprocess.run(
        [sys.executable, "-c",
         "import sys; sys.path.insert(0, r'%s'); "
         "from prismabuild import pool, core; "
         "assert hasattr(pool.PoolQueue, 'claim'); print('import ok')"
         % (MIRROR / "src")],
        capture_output=True, text=True, check=False,
    )
    print((probe.stdout or probe.stderr).strip())
    return 0 if probe.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
