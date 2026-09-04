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


def result_and_stamp_names(command, cwd, demand, variables):
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

    The same command at the same commit still fingerprints identically, so a
    repeat submission can still be answered from the CAS -- which is the one
    case where sharing the path was safe all along.
    """

    identity = _git_identity(cwd)
    fingerprint = hashlib.sha256(
        json.dumps([command, str(cwd), demand, variables, identity],
                   sort_keys=True).encode()
    ).hexdigest()[:16]
    return (f"{RESULT_PREFIX}{fingerprint}.txt",
            f"{STAMP_PREFIX}{fingerprint}.json")


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


def await_outcome(q, key: str, *, wait_s: float) -> int:
    """Block until this action reaches a terminal directory, then report it.

    Split out of ``main`` so the outcome half can be tested without a
    submission: the bug this exists to prevent lived entirely in which
    directories the loop watched, which is exactly the part a live-queue
    test would have been least likely to reach.
    """

    # Watch BOTH terminal directories.  An action whose argv exits non-zero is
    # retried and then filed under ``failed``, never under ``done`` -- and this
    # loop used to watch ``done`` alone, so a caller whose suite legitimately
    # failed sat here until ``--wait-s`` expired (a DAY, by default) and then
    # got exit 75 and the words "gave up waiting".  The work had run, three
    # times, and said why each time; none of it reached the person waiting.
    # Sixty-six items sat in ``failed`` when this was found, and the agents who
    # submitted them reported the pool as having never scheduled their work.
    done = q.item_path("done", key)
    failed = q.item_path("failed", key)
    deadline = time.monotonic() + wait_s
    # Poll by readdir, not by stat.  The queue lives on NFS, where a stat of a
    # path that did not exist yet is negatively cached: the outcome landed and
    # a bare ``done.exists()`` kept answering False.  Listing the directory
    # revalidates it.
    def _landed(path) -> bool:
        try:
            return path.name in os.listdir(path.parent)
        except OSError:
            return False

    while True:
        if _landed(done):
            outcome_path = done
            break
        if _landed(failed):
            outcome_path = failed
            break
        if time.monotonic() > deadline:
            print(f"pbrun: gave up waiting for {key[:12]}", file=sys.stderr)
            return 75
        time.sleep(POLL_S)

    outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
    detail = outcome.get("detail") or {}
    sys.stdout.write(str(detail.get("stdout") or ""))
    sys.stderr.write(str(detail.get("stderr") or ""))
    status = str(outcome.get("status"))
    print(f"pbrun: {status} on {outcome.get('finished_host')} "
          f"in {detail.get('elapsed_s', 0):.0f}s", file=sys.stderr)
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
    tags = placement_tags(
        cwd,
        explicit=list(args.tag),
        here=args.here,
        hostname=socket.gethostname(),
    )
    if args.exclusive:
        # "All of one box" is a fact about the boxes, and guessing it does not
        # fail loudly -- it fails as an action nobody can ever claim.  The
        # default was 4 while sparky declares 2 and sparklina 1, so every
        # --exclusive submission asked for twice the slots that exist and sat
        # in ``ready`` forever.  Read it from what the fleet announces, which
        # needs the placement tags, so it happens after them.
        demand["gpu"] = args.gpu_capacity or exclusive_gpu_demand(
            pool.PoolQueue(SH / "pb-queue"), tags)
        demand["mem_gb"] = max(int(demand.get("mem_gb", 0)), 16)

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
        # Torch, numpy and OpenBLAS each size their thread pool from the
        # machine's core count, and the pool admits many actions per box, so
        # the default multiplies: dl380g10 ran a 24-worker pytest under 16
        # worker loops and reached a load average of **927** on 80 cores, with
        # every process fighting for a scheduler slot it did not need.  A
        # fleet gets its parallelism from running many actions, not from each
        # action taking the whole box, so the per-process share is small by
        # default.  An action that genuinely wants threads says so with
        # ``--env OMP_NUM_THREADS=N``, which overrides this.
        "OMP_NUM_THREADS": "4",
        "MKL_NUM_THREADS": "4",
        "OPENBLAS_NUM_THREADS": "4",
    }
    for entry in args.env:
        if "=" not in entry:
            raise SystemExit(f"--env expects K=V, got {entry!r}")
        key, value = entry.split("=", 1)
        variables[key] = value

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

    log_name, stamp_name = result_and_stamp_names(
        command, cwd, demand, variables)
    # The closure member must be under checkout_root: that is where the
    # worker re-verifies it, on whichever box claimed the action.
    identity = _git_identity(cwd)
    # Written through a private temp file and renamed, because rename is the
    # one primitive this fleet trusts on NFS and a plain write is not atomic.
    # Concurrent submits from one checkout -- forty test shards, say -- all
    # write this same file, and a reader that catches a partial one gets
    # "cannot open code closure file as a regular file" or "live code closure
    # differs from the action-pinned closure".  The content is identical across
    # those submits *because the commit is in the name*, so atomicity is the
    # whole fix and ordering does not matter.  It was not identical before
    # that: the name held the command and the content held the commit.
    payload = json.dumps({"cwd": str(cwd), **identity}, indent=1, sort_keys=True)
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
    # Say that the slot has no device, every time.  The mask is correct and it
    # is also a silent narrowing: a suite that used to run its CUDA tests now
    # skips them, and a skip that nobody announced reads as the same green.
    masked = "" if demand.get("gpu") else "  [no GPU: CUDA_VISIBLE_DEVICES='']"
    print(f"pbrun: queued {key[:12]} tags={tags} demand={demand}{masked}",
          file=sys.stderr, flush=True)

    return await_outcome(q, key, wait_s=args.wait_s)


if __name__ == "__main__":
    raise SystemExit(main())
