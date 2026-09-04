"""Progress of the GLM-5.3 Tessera fleet export, read from the queue and CAS.

The queue line has to say something different under each transport, because
the pull queue's ``ready`` and ``claimed`` directories only mean anything
while workers are draining them.  Under SLURM nothing writes to either one --
the pending work is in the scheduler -- so reporting their counts prints two
zeroes beside a controller holding forty jobs, and a zero that means "this
directory is unused" reads exactly like a zero that means "there is nothing to
do".  So SLURM's depth is asked of ``squeue``, which is where it is.

The terminal counts are the same under both, because both transports file
their endings in the same two directories on purpose.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
Q = SH / "pb-queue"
RES = SH / "checkout" / "results" / "glm53-tessera"
PARTS = Path("/mnt/shared/models/GLM-5.3-Flash-Tessera-E2M1K2-20260901-parts")

TRANSPORTS = ("pool", "slurm")
DEFAULT_TRANSPORT_ENV = "PRISMABUILD_TRANSPORT"

#: The pull queue's own directories, in the order a person reads them.
POOL_STATES = ("ready", "claimed")
TERMINAL_STATES = ("done", "failed", "withdrawn")

#: How long the controller gets to answer before the status screen gives up on
#: it.  A busy ``squeue`` must not become a hung status command.
SQUEUE_TIMEOUT_S = 20.0


def _keys(directory: Path) -> set[str]:
    """The action keys filed in one queue directory, top level only.

    ``withdrawn/superseded/`` holds retired decisions and is not a count of
    anything current, so the glob stays unrecursive.
    """

    if not directory.is_dir():
        return set()
    return {path.stem for path in directory.glob("*.json")}


def queue_counts(queue_root: Path = Q, *, transport: str = "pool") -> dict:
    """What the queue directories hold, counted once per action.

    A withdrawal is filed twice by design -- the marker under ``withdrawn/``
    that stops ``pool_reset`` re-submitting a decision, and the terminal record
    under ``failed/`` that ``merge_suite`` reads for an exit status -- so
    counting both makes one cancellation read as a cancellation plus a
    failure.  It is one action ending one way.
    """

    root = Path(queue_root)
    states = TERMINAL_STATES if transport == "slurm" else (
        POOL_STATES + TERMINAL_STATES)
    keys = {state: _keys(root / state) for state in states}
    withdrawn = keys.get("withdrawn", set())
    counts = {}
    for state in states:
        found = keys[state]
        if state == "failed":
            found = found - withdrawn
        counts[state] = len(found)
    return counts


def squeue_depth(*, squeue: str = "squeue") -> dict:
    """Pending and running job counts per partition, from the controller.

    Injectable so the tests can drive it with a fake binary: SLURM is not
    installed on any box in this fleet, and a status line nobody can test is
    how the ``ready``/``claimed`` line came to be wrong in the first place.
    """

    completed = subprocess.run(
        [squeue, "-h", "-o", "%P|%T"],
        capture_output=True, text=True, timeout=SQUEUE_TIMEOUT_S,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            (completed.stderr or completed.stdout).strip() or "squeue refused")
    depth: dict[str, dict[str, int]] = {}
    for line in completed.stdout.splitlines():
        if "|" not in line:
            continue
        partition, _, state = line.partition("|")
        partition = partition.strip().rstrip("*")
        state = state.strip().upper()
        # Every other state is a job on its way out -- COMPLETING, CANCELLED,
        # a failure the scheduler has not purged yet.  None of them is work
        # waiting for a node, which is the question this line answers.
        if state not in ("PENDING", "RUNNING"):
            depth.setdefault(partition, {"pending": 0, "running": 0})
            continue
        row = depth.setdefault(partition, {"pending": 0, "running": 0})
        row["pending" if state == "PENDING" else "running"] += 1
    return depth


def describe_squeue_depth(*, squeue: str = "squeue") -> str:
    """The depth line, or why it could not be read.

    A status script must never be the thing that fails, so an unreachable
    controller is reported as unavailable rather than raised -- the same
    contract ``width`` already keeps for an unreadable pool.
    """

    try:
        depth = squeue_depth(squeue=squeue)
    except Exception as exc:                       # noqa: BLE001 - diagnostic
        return f"unavailable ({type(exc).__name__})"
    if not depth:
        return "no jobs queued or running"
    return "; ".join(
        f"{partition} {row['pending']} pending {row['running']} running"
        for partition, row in sorted(depth.items())
    )


def width(queue_root: Path = Q) -> str:
    """How much of ``ready`` only one box can take.

    ``queue {'ready': 18}`` reads the same whether those items are spread
    across three boxes or queued behind one, and on 2026-09-04 they were
    queued behind one: every one of them pinned to sparky by a box-local
    checkout, while two boxes idled.  This is the one command a person runs
    to look at the queue, so the width belongs under the counts.

    A status script must never be the thing that fails, so an unreadable
    fleet is reported as unknown rather than raised.
    """

    try:
        sys.path.insert(0, str(RUNTIME_ROOT / "src"))
        from prismabuild import pool
        return pool.describe_placement_census(
            pool.PoolQueue(queue_root).placement_census())
    except Exception as exc:                       # noqa: BLE001 - diagnostic
        return f"unavailable ({type(exc).__name__})"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--transport", choices=TRANSPORTS,
        default=os.environ.get(DEFAULT_TRANSPORT_ENV) or "pool",
        help="which dispatcher this fleet is running (env "
             "PRISMABUILD_TRANSPORT); decides whether the depth is read from "
             "the pull queue or from squeue")
    ap.add_argument("--queue-root", default=str(Q), help=argparse.SUPPRESS)
    ap.add_argument("--results-root", default=str(RES), help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    queue_root = Path(args.queue_root)
    results = Path(args.results_root)

    manifests = sorted(results.glob("shard-*.json")) if results.is_dir() else []
    done_shards, total_bytes, qbytes, qparams = [], 0, 0, 0
    for m in manifests:
        d = json.loads(m.read_text())
        done_shards.append(d["shard"])
        total_bytes += d["total_bytes"]
        qbytes += d["quantized_bytes"]
        qparams += d["quantized_params"]
    missing = [n for n in range(1, 121) if n not in set(done_shards)]
    def gib(b):
        return b / 2 ** 30

    print(f"queue      {queue_counts(queue_root, transport=args.transport)}")
    if args.transport == "slurm":
        # The placement census describes worker offers, and under SLURM there
        # are none: the scheduler is the thing that knows what is waiting.
        print(f"squeue     {describe_squeue_depth()}")
    else:
        print(f"width      {width(queue_root)}")
    print(f"shards     {len(done_shards)}/120 encoded   missing {len(missing)}")
    if missing[:12]:
        print(f"  next     {missing[:12]}{'...' if len(missing) > 12 else ''}")
    if qparams:
        print(f"body       {gib(qbytes):.3f} GiB over {qparams:,} params "
              f"= {qbytes*8/qparams:.4f} bpp")
        print(f"on disk    {gib(total_bytes):.3f} GiB   (Mia 163.560 GiB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
