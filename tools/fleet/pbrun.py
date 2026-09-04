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
import uuid
from pathlib import Path

SH = Path("/mnt/shared/prismabuild-fleet")
#: Every box mounts this at the same path, so a checkout underneath it is
#: visible to all of them and an action that runs there can run anywhere.
#: A checkout outside it exists on exactly one box.  That is a *fact about
#: the path*, which is why placement below is derived from it rather than
#: asked of the submitter.
SHARED_ROOT = Path("/mnt/shared")
sys.path.insert(0, str(SH / "repo" / "src"))
from prismabuild import core as pb, pool  # noqa: E402

POLL_S = 5.0
#: One stamp per ACTION, not per checkout.  A single shared name looked
#: harmless because concurrent submits from one tree write the same bytes --
#: but the worker re-verifies the live stamp against the closure its action
#: pinned, and by then a later submit has replaced it with a *different*
#: identity, because the tree moved in between (pytest bytecode, result logs,
#: whatever a neighbouring shard did).  Hence "live code closure differs from
#: the action-pinned closure", ten of them in one fan-out.  Atomic writing
#: fixes torn reads and does nothing for this; separate files fix both.
STAMP_PREFIX = ".pbrun-closure."
#: Every action tees its output to a file inside the checkout, and the
#: worker refuses to start when that file already exists.  A fixed name
#: therefore lets the first submit from a tree poison every later one:
#: 19 of the queue's failures were exactly this, all reading "declared
#: result path must be absent before execution".  The name is derived
#: from what distinguishes the action, so two different commands get two
#: files while a resubmit of the same command still lands on the same
#: name and stays a CAS hit.
RESULT_PREFIX = "pbrun_result."


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
    # submit that is computing this very digest.  So are the result logs: a
    # leftover one is output *about* a previous action, not a change to the
    # code this action runs, and leaving it in moved the key on every submit
    # after the first -- a cache miss dressed up as a different action.
    porcelain = "\n".join(
        line for line in _git("status", "--porcelain").splitlines()
        if STAMP_PREFIX not in line and RESULT_PREFIX not in line
    )
    # `git diff HEAD` covers tracked edits.  It says nothing about an
    # UNTRACKED file, whose name appears in porcelain as "?? path" while its
    # bytes appear nowhere -- so editing an untracked script left the action
    # key unmoved and the CAS replayed the previous run's stdout.  That failure
    # is invisible from the outside: a stale result is indistinguishable from a
    # fresh one unless you notice the traceback points at a line the file no
    # longer has, which is exactly how it was caught.
    untracked = []
    for line in porcelain.splitlines():
        if not line.startswith("?? "):
            continue
        member = cwd / line[3:].strip().strip('"')
        if member.is_dir() or not member.exists():
            continue                 # a directory entry is expanded by git itself
        try:
            untracked.append(f"{line[3:]}:{_sha256_file(member)}")
        except OSError:
            untracked.append(f"{line[3:]}:unreadable")
    dirty = porcelain + _git("diff", "HEAD") + "\n".join(sorted(untracked))
    return {
        "head": head,
        "dirty_sha256": hashlib.sha256(dirty.encode()).hexdigest(),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def placement_tags(
    cwd: Path,
    *,
    explicit: list[str],
    here: bool,
    hostname: str,
) -> list[str]:
    """Return the placement tags for an action whose working directory is ``cwd``.

    Placement is PrismaBuild's decision, not the submitter's.  The submitter
    knows one thing the pool cannot infer -- an explicit ``--tag`` naming a
    hardware class the work requires -- and everything else follows from where
    the checkout lives:

    * A checkout under ``/mnt/shared`` is mounted at the same path on every
      box, so **any** worker that satisfies the demand can run the action and
      no host tag is added.  This is the case that used to need ``--anywhere``,
      and forgetting the flag was invisible: the work ran, correctly, on one
      box, while the others sat idle.  A default that has to be remembered to
      be right is not a default.
    * A checkout anywhere else exists on exactly one box, so the action is
      pinned to this host.  ``--here`` forces that pin even on shared storage,
      for the rare action that is genuinely about *this* machine.

    Nothing here decides *which* free box runs a shared-checkout action; the
    queue does, from the demand and what each worker offers.  That separation
    is the point.
    """

    if explicit:
        return list(explicit)
    if here:
        return [hostname]
    try:
        cwd.resolve().relative_to(SHARED_ROOT)
    except ValueError:
        return [hostname]
    return []


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
                    help="require a box offering this tag (e.g. a hardware class)")
    ap.add_argument("--anywhere", action="store_true",
                    help="accepted and ignored; a shared checkout is already free "
                         "to run anywhere")
    ap.add_argument("--here", action="store_true",
                    help="pin to this box even though the checkout is shared")
    ap.add_argument("--cwd", default=os.getcwd())
    ap.add_argument("--deterministic", action="store_true",
                    help="declare byte-identical output; enables CAS reuse")
    ap.add_argument("--timeout-s", type=float, default=7200.0)
    ap.add_argument("--wait-s", type=float, default=86400.0,
                    help="give up waiting for a worker to pick this up")
    ap.add_argument("--priority", type=int, default=0)
    ap.add_argument("--env", action="append", default=[],
                    help="K=V added to the action's environment (repeatable)")
    ap.add_argument("--no-default-env", action="store_true",
                    help="declare only --env, without the fleet defaults")
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

    tags = placement_tags(
        cwd,
        explicit=list(args.tag),
        here=args.here,
        hostname=socket.gethostname(),
    )
    if args.anywhere and args.here:
        raise SystemExit("--anywhere and --here contradict each other")

    # `run_local_action` builds the child's environment from *these* and
    # nothing else, so an empty dict is not "inherit the caller" -- it is an
    # empty environment, rescued only by `bash -lc` sourcing a profile.  That
    # is why TRITON_CACHE_DIR never reached a worker and 38 tests failed on a
    # root-owned cache; the fix is to declare the few the fleet actually needs.
    #
    # Every value here is deliberately the same string on every box, so the
    # action key stays box-independent.  TRITON_CACHE_DIR is the one to watch:
    # the path must be box-LOCAL (never /mnt/shared, where concurrent boxes
    # corrupt each other's cache), and it is local precisely because each box
    # has its own /home/rob -- same string, different disk.
    variables = {} if args.no_default_env else {
        "HOME": "/home/rob",
        "TMPDIR": "/home/rob/tmp",
        "TRITON_CACHE_DIR": "/home/rob/.triton-cache",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    }
    for entry in args.env:
        if "=" not in entry:
            raise SystemExit(f"--env expects K=V, got {entry!r}")
        key, value = entry.split("=", 1)
        variables[key] = value

    fingerprint = hashlib.sha256(
        json.dumps([command, str(cwd), demand, variables], sort_keys=True).encode()
    ).hexdigest()[:16]
    log_name = f"{RESULT_PREFIX}{fingerprint}.txt"
    # The closure member must be under checkout_root: that is where the
    # worker re-verifies it, on whichever box claimed the action.
    identity = _git_identity(cwd)
    # Written through a private temp file and renamed, because rename is the
    # one primitive this fleet trusts on NFS and a plain write is not atomic.
    # Concurrent submits from one checkout -- forty test shards, say -- all
    # write this same file, and a reader that catches a partial one gets
    # "cannot open code closure file as a regular file" or "live code closure
    # differs from the action-pinned closure".  The content is identical across
    # those submits, so atomicity is the whole fix; ordering does not matter.
    payload = json.dumps({"cwd": str(cwd), **identity}, indent=1, sort_keys=True)
    stamp_name = f"{STAMP_PREFIX}{fingerprint}.json"
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
    exclude = cwd / ".git" / "info" / "exclude"
    try:
        if exclude.parent.is_dir():
            current = exclude.read_text()
            with exclude.open("a", encoding="utf-8") as handle:
                if STAMP_PREFIX not in current:
                    handle.write(f"{STAMP_PREFIX}*\n")
                if RESULT_PREFIX not in current:
                    handle.write(f"{RESULT_PREFIX}*\n")
    except OSError:
        pass                       # a worktree without .git/info is not an error


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
        "code_closure": pb.build_code_closure(cwd, [stamp_name]),
        "params": {"command": command, "cwd": str(cwd), "demand": demand},
        "environment": {"variables": variables, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    }
    action = pb.seal_action(body)
    key = str(action["action_key"])

    cas = pb.PrismaBuildCAS(SH / "cas")
    cas.publish_action_request(action)

    q = pool.PoolQueue(SH / "pb-queue")

    # Refuse work the fleet cannot run, at the one moment the caller is still
    # watching.  A required tag no box offers is not a slow submission: the
    # item matches no worker's placement filter, so it sits in `ready` --
    # counted, reported as pending -- while every idle worker polls past it
    # until `--wait-s` expires a day later.  A suite submitted with
    # `--tag dl380` did exactly that in front of fifteen idle boxes offering
    # `x86`.  `placeable` answers None when no worker has announced at all,
    # and that stays a warning: a fleet whose loops predate the offer
    # registry must still be able to submit.
    intent = {"tags": tags, "needs_gpu": bool(demand.get("gpu")), "resources": demand}
    verdict = q.placeable(intent)
    if verdict is False:
        raise SystemExit(
            f"pbrun: no live worker can run this action.\n"
            f"  required tags: {tags or '(any box)'}\n"
            f"  demand:        {demand}\n"
            f"  offered now:   {q.offered_tags() or '(no worker has announced)'}\n"
            f"Fix the --tag, or start a worker on a box that offers it."
        )
    if verdict is None:
        print("pbrun: no worker offers on record; submitting unchecked",
              file=sys.stderr, flush=True)

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
