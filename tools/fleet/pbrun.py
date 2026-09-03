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

*The closure is the checkout's git identity.*  A code closure needs at least
one real file, and the honest identity of "this command against this tree" is
the commit plus whatever is dirty on top of it.  Binding that makes a cache
hit correct rather than lucky: change the code and the action key moves.

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
import hashlib
import json
import os
import shlex
import socket
import subprocess
import sys
import time
from pathlib import Path

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(SH / "repo" / "src"))
from prismabuild import core as pb, pool  # noqa: E402

POLL_S = 5.0
STAMP = ".pbrun-closure.json"


def _git_identity(cwd: Path) -> dict[str, str]:
    """Commit plus a digest of the working-tree delta.  Never raises."""

    def _git(*args: str) -> str:
        try:
            out = subprocess.run(
                ["git", "-C", str(cwd), *args],
                capture_output=True, text=True, timeout=30,
            )
            return out.stdout if out.returncode == 0 else ""
        except Exception:                                    # noqa: BLE001
            return ""

    head = _git("rev-parse", "HEAD").strip() or "no-git"
    # Content of the delta, not just its file list: a re-edit that restores
    # the same bytes is the same action, and a one-character change is not.
    # The stamp itself is filtered out: it is written into this tree by the
    # submit that is computing this very digest.
    porcelain = "\n".join(
        line for line in _git("status", "--porcelain").splitlines()
        if STAMP not in line
    )
    dirty = porcelain + _git("diff", "HEAD")
    return {
        "head": head,
        "dirty_sha256": hashlib.sha256(dirty.encode()).hexdigest(),
    }


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


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Submit one command to the PrismaBuild pool and wait for it."
    )
    ap.add_argument("--demand", default="",
                    help="resource demand, e.g. gpu=1,mem_gb=16")
    ap.add_argument("--gpu", action="store_true",
                    help="shorthand for gpu=1,mem_gb=16")
    ap.add_argument("--exclusive", action="store_true",
                    help="demand the whole GPU capacity of one box")
    ap.add_argument("--gpu-capacity", type=int, default=4,
                    help="slots one box declares; --exclusive demands all of them")
    ap.add_argument("--tag", action="append", default=[],
                    help="placement tag; defaults to this box's hostname")
    ap.add_argument("--anywhere", action="store_true",
                    help="let any box run it (only valid if the checkout is shared)")
    ap.add_argument("--cwd", default=os.getcwd())
    ap.add_argument("--deterministic", action="store_true",
                    help="declare byte-identical output; enables CAS reuse")
    ap.add_argument("--timeout-s", type=float, default=7200.0)
    ap.add_argument("--wait-s", type=float, default=86400.0,
                    help="give up waiting for a worker to pick this up")
    ap.add_argument("--priority", type=int, default=0)
    ap.add_argument("command", nargs=argparse.REMAINDER)
    args = ap.parse_args()

    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("nothing to run: pbrun [options] -- <command>")

    cwd = Path(args.cwd).resolve()
    if not cwd.is_dir():
        raise SystemExit(f"--cwd is not a directory: {cwd}")

    demand = _parse_demand(args.demand)
    if args.gpu:
        demand.setdefault("gpu", 1)
        demand.setdefault("mem_gb", 16)
    if args.exclusive:
        demand["gpu"] = args.gpu_capacity
        demand.setdefault("mem_gb", 16)
    demand.setdefault("mem_gb", 4)

    tags = list(args.tag)
    if not tags and not args.anywhere:
        # Honest default: an agent's worktree exists on one box only, so the
        # action is pinned there.  Cross-box placement is opt-in and is only
        # correct when the checkout is on shared storage.
        tags = [socket.gethostname()]

    # The closure member must be under checkout_root: that is where the
    # worker re-verifies it, on whichever box claimed the action.
    identity = _git_identity(cwd)
    (cwd / STAMP).write_text(
        json.dumps({"cwd": str(cwd), **identity}, indent=1, sort_keys=True),
        encoding="utf-8")
    exclude = cwd / ".git" / "info" / "exclude"
    try:
        if exclude.parent.is_dir() and STAMP not in exclude.read_text():
            with exclude.open("a", encoding="utf-8") as handle:
                handle.write(f"{STAMP}\n")
    except OSError:
        pass                       # a worktree without .git/info is not an error

    log_name = "pbrun_result.txt"
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "fleet/pbrun",
            "definition_version": "v1",
            "task_class": "generation",
            # A pytest or a timing run is not byte-reproducible and must not
            # claim to be: the CAS only enforces canonical equality on
            # "deterministic", so mislabelling one would be a false receipt.
            "determinism": "deterministic" if args.deterministic else "stochastic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": ["/bin/bash", "-lc",
                     f"{shlex.join(command)} 2>&1 | tee {shlex.quote(log_name)}; "
                     f"exit ${{PIPESTATUS[0]}}"],
            "working_directory": ".",
            "result_path": log_name,
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(cwd, [STAMP]),
        "params": {"command": command, "cwd": str(cwd), "demand": demand},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    }
    action = pb.seal_action(body)
    key = str(action["action_key"])

    cas = pb.PrismaBuildCAS(SH / "cas")
    cas.publish_action_request(action)

    q = pool.PoolQueue(SH / "pb-queue")
    q.publish(
        action_key=key,
        cas_root=str(SH / "cas"),
        checkout_root=str(cwd),
        worker_script=str(SH / "repo" / "tools" / "prismabuild_worker.py"),
        tags=tags,
        needs_gpu=bool(demand.get("gpu")),
        priority=args.priority,
        resources=demand,
    )
    print(f"pbrun: queued {key[:12]} tags={tags} demand={demand}",
          file=sys.stderr, flush=True)

    done = q.item_path("done", key)
    deadline = time.monotonic() + args.wait_s
    # Poll by readdir, not by stat.  The queue lives on NFS, where a stat of a
    # path that did not exist yet is negatively cached: the outcome landed and
    # a bare ``done.exists()`` kept answering False.  Listing the directory
    # revalidates it.
    def _landed() -> bool:
        try:
            return done.name in os.listdir(done.parent)
        except OSError:
            return False

    while not _landed():
        if time.monotonic() > deadline:
            print(f"pbrun: gave up waiting for {key[:12]}", file=sys.stderr)
            return 75
        time.sleep(POLL_S)

    outcome = json.loads(done.read_text(encoding="utf-8"))
    detail = outcome.get("detail") or {}
    sys.stdout.write(str(detail.get("stdout") or ""))
    sys.stderr.write(str(detail.get("stderr") or ""))
    status = str(outcome.get("status"))
    print(f"pbrun: {status} on {outcome.get('finished_host')} "
          f"in {detail.get('elapsed_s', 0):.0f}s", file=sys.stderr)
    if status == "cache_hit":
        return 0
    rc = detail.get("returncode")
    return int(rc) if isinstance(rc, int) else (0 if status == "executed" else 1)


if __name__ == "__main__":
    raise SystemExit(main())
