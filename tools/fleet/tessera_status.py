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

The shard count moved for the same kind of reason.  A shard's manifest is the
action's declared result, and under SLURM the action runs in a private
checkout the job removes when it ends, so the manifest exists exactly where it
always did -- in the CAS, as the receipt's verified result -- and nowhere under
the shared checkout.  Counting files in ``results/glm53-tessera`` therefore
reported ``0/120 encoded`` for an export that had encoded shards, while a
manifest left over from a previous plan could still decide the screen.  So the
count comes from the CAS receipts of the export this screen names, and the
shared directory is read only under the transport that wrote it.
"""
from __future__ import annotations

import argparse
import json
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

#: One reader of the fleet's default, published beside this file.  A status
#: screen that guessed "pool" after the cutover reported the depth of a queue
#: nobody drains and called it the fleet.
from fleet_submit import TRANSPORTS, default_transport  # noqa: E402

CAS = SH / "cas"

#: The action kind this screen counts.  A shard receipt is addressed by the
#: action key, and nothing indexes keys by export, so the requests the CAS
#: already publishes are what names them.
EXPORT_DEFINITION_ID = "tessera/glm53-export-shard"

#: The pull queue's own directories, in the order a person reads them.
POOL_STATES = ("ready", "claimed")
TERMINAL_STATES = ("done", "failed", "withdrawn")

#: How long the controller gets to answer before the status screen gives up on
#: it.  A busy ``squeue`` must not become a hung status command.
SQUEUE_TIMEOUT_S = 20.0

#: The prefix every "could not read it" line carries.  ``main`` reads its own
#: lines to settle the exit status, so the sentence an operator sees and the
#: code a script branches on cannot drift apart.
UNAVAILABLE = "unavailable ("

#: The fields the screen sums.  A manifest that parsed but was missing one of
#: them used to reach the totals and take the whole screen down with a
#: ``KeyError``, which is the failure a status command must not have: the
#: operator is already looking at it because something else is wrong.
MANIFEST_FIELDS = ("shard", "total_bytes", "quantized_bytes", "quantized_params")

#: What the exit status means.  A status screen that always exits 0 cannot be
#: branched on, and an operator who reads only the code is told the fleet is
#: fine no matter what the screen printed.
EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_NOTHING = 3

_EPILOG = """\
exit status:
  0  the screen read everything it consulted
  1  the screen printed, but part of what it consulted could not be read: a
     missing root, an entry that would not parse, a plan whose digest could
     not be taken, or a controller that did not answer
  2  the command line was wrong
  3  nothing to read: none of the roots this screen consults exists

3 takes precedence over 1.  A store that holds no shards yet is a complete
screen and exits 0; the export has not started, which is not a failure to
read it.
"""


def _manifest(text: str) -> dict:
    """One shard manifest, or a ``ValueError`` naming what is wrong with it.

    The readers already treat an entry they cannot parse as one skipped line
    rather than the end of the screen.  Validating here puts a manifest that
    parses but does not carry what the totals sum under that same handling,
    so ``main`` can add the fields up without guarding each one.
    """

    body = json.loads(text)
    if not isinstance(body, dict):
        raise ValueError("manifest is not an object")
    for field in MANIFEST_FIELDS:
        value = body.get(field)
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"manifest field {field!r} is not an integer")
    return body


def _keys(directory: Path) -> set[str]:
    """The action keys filed in one queue directory, top level only.

    ``withdrawn/superseded/`` holds retired decisions and is not a count of
    anything current, so the glob stays unrecursive.
    """

    if not directory.is_dir():
        return set()
    return {path.stem for path in directory.glob("*.json")}


def queue_counts(queue_root: Path | None = None, *, transport: str = "pool") -> dict:
    """What the queue directories hold, counted once per action.

    A withdrawal is filed twice by design -- the marker under ``withdrawn/``
    that stops ``pool_reset`` re-submitting a decision, and the terminal record
    under ``failed/`` that ``merge_suite`` reads for an exit status -- so
    counting both makes one cancellation read as a cancellation plus a
    failure.  It is one action ending one way.
    """

    if queue_root is None:
        # Resolved on the call, not bound at definition; see
        # ``fleet_submit.submit``.
        queue_root = Q

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
        return f"{UNAVAILABLE}{type(exc).__name__})"
    if not depth:
        return "no jobs queued or running"
    return "; ".join(
        f"{partition} {row['pending']} pending {row['running']} running"
        for partition, row in sorted(depth.items())
    )


def _core():
    """``prismabuild.core``, imported the way ``width`` imports the pool."""

    sys.path.insert(0, str(RUNTIME_ROOT / "src"))
    from prismabuild import core as pb
    return pb


def cas_shard_manifests(
    cas_root: Path | None = None,
    *,
    plan_sha256: str,
    definition_id: str = EXPORT_DEFINITION_ID,
) -> tuple[dict[int, dict], int, list[str]]:
    """Every completed shard of one export, from verified CAS receipts.

    An export is identified by the action kind, the digest of the allocation
    plan it encodes, and the shard count it was cut into.  The plan digest is
    what separates this export from a previous one: it is bound into every
    shard's action key on purpose, so a re-allocated plan is different work
    rather than a silent overwrite, and a receipt carrying another digest is
    another export's result no matter which directory its manifest reached.

    Only requests are enumerated, because only requests name the keys.  The
    lane's re-seal publishes a second request for the same shard, carrying the
    checkout snapshot, so one shard can have two keys and two receipts of
    identical bytes; the shard number deduplicates them.

    Args:
        cas_root: The store the fleet publishes into.
        plan_sha256: The digest of the plan this export encodes.
        definition_id: The action kind to count.

    Returns:
        The manifests by shard number, how many receipts belonged to another
        plan, and the path of every entry that could not be read.
    """

    if cas_root is None:
        # Resolved on the call, not bound at definition; see
        # ``fleet_submit.submit``.
        cas_root = SH / "cas"

    pb = _core()
    requests = Path(cas_root) / "requests"
    if not requests.is_dir():
        return {}, 0, []
    cas = pb.PrismaBuildCAS(cas_root)
    manifests: dict[int, dict] = {}
    other_plans = 0
    unreadable: list[str] = []
    for path in sorted(requests.glob("*/*.json")):
        try:
            raw = path.read_text(encoding="utf-8")
            # Cheap first: a store holds every action the fleet ever sealed,
            # and most of them are not export shards.
            if definition_id not in raw:
                continue
            body = json.loads(raw)
            task = body.get("task") or {}
            params = body.get("params") or {}
            if task.get("definition_id") != definition_id:
                continue
            if params.get("of_shards") != 120:
                continue
            if params.get("plan_sha256") != plan_sha256:
                # Counted, not silently dropped: a screen that reports 0/120
                # while the store holds a previous export's results should say
                # that is what it is looking at.
                if cas.lookup(body) is not None:
                    other_plans += 1
                continue
            shard = params.get("shard")
            if not isinstance(shard, int) or shard in manifests:
                continue
            receipt = cas.lookup(body)
            if receipt is None:
                continue
            manifest = _manifest(
                Path(cas.result_path(receipt, body)).read_text(encoding="utf-8"))
            if manifest["shard"] != shard:
                unreadable.append(str(path))
                continue
            manifests[shard] = manifest
        except Exception:                          # noqa: BLE001 - diagnostic
            # A status script must never be the thing that fails.  One
            # unreadable entry is named on its own line and costs no other
            # shard its line.
            unreadable.append(str(path))
    return manifests, other_plans, unreadable


def shared_shard_manifests(
    results_root: Path | None = None,
) -> tuple[dict[int, dict], list[str]]:
    """The manifests the pull queue's workers wrote into the shared checkout.

    Kept for the transport that wrote them.  These files carry no plan digest,
    so a leftover from a previous export cannot be told from a current one,
    which is the second half of why the count moved to the CAS.
    """

    if results_root is None:
        # Resolved on the call, not bound at definition; see
        # ``fleet_submit.submit``.
        results_root = RES

    results = Path(results_root)
    if not results.is_dir():
        return {}, []
    manifests: dict[int, dict] = {}
    unreadable: list[str] = []
    for path in sorted(results.glob("shard-*.json")):
        try:
            manifest = _manifest(path.read_text(encoding="utf-8"))
            manifests[manifest["shard"]] = manifest
        except Exception:                          # noqa: BLE001 - diagnostic
            unreadable.append(str(path))
    return manifests, unreadable


def width(queue_root: Path | None = None) -> str:
    """How much of ``ready`` only one box can take.

    ``queue {'ready': 18}`` reads the same whether those items are spread
    across three boxes or queued behind one, and on 2026-09-04 they were
    queued behind one: every one of them pinned to sparky by a box-local
    checkout, while two boxes idled.  This is the one command a person runs
    to look at the queue, so the width belongs under the counts.

    A status script must never be the thing that fails, so an unreadable
    fleet is reported as unknown rather than raised.
    """

    if queue_root is None:
        # Resolved on the call, not bound at definition; see
        # ``fleet_submit.submit``.
        queue_root = Q

    try:
        sys.path.insert(0, str(RUNTIME_ROOT / "src"))
        from prismabuild import pool
        return pool.describe_placement_census(
            pool.PoolQueue(queue_root).placement_census())
    except Exception as exc:                       # noqa: BLE001 - diagnostic
        return f"{UNAVAILABLE}{type(exc).__name__})"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--transport", choices=TRANSPORTS, default=default_transport(),
        help="which dispatcher this fleet is running (env "
             "PRISMABUILD_TRANSPORT, else the published runtime generation's "
             "default_transport); decides whether the depth is read from the "
             "pull queue or from squeue")
    ap.add_argument("--queue-root", default=str(Q), help=argparse.SUPPRESS)
    ap.add_argument("--results-root", default=str(RES), help=argparse.SUPPRESS)
    ap.add_argument("--cas-root", default=str(CAS), help=argparse.SUPPRESS)
    ap.add_argument("--plan", default=None, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    queue_root = Path(args.queue_root)
    results = Path(args.results_root)

    # Every root the screen reads, checked before it reads them.  A missing
    # queue directory counts as no keys and a missing results directory as no
    # manifests, so a screen pointed at the wrong store prints the same zeroes
    # as a fleet with nothing to do.  Naming the roots is also what lets the
    # exit status tell "nothing here" from "nothing yet".
    consulted = [("queue root", queue_root), ("CAS root", Path(args.cas_root))]
    if args.transport != "slurm":
        consulted.append(("results root", results))
    absent = [f"{name} {path}" for name, path in consulted if not path.is_dir()]
    for row in absent:
        print(f"missing    {row}")
    if len(absent) == len(consulted):
        print("nothing    no root this screen reads exists, so it read nothing")
        return EXIT_NOTHING

    # The plan digest names the export, so it is read from the same file the
    # dispatcher hands the exporter.  Imported here rather than at module
    # scope so a status screen costs nothing until it needs the constant.
    import dispatch_tessera_shards as dispatcher
    plan = args.plan if args.plan is not None else dispatcher.PLAN
    try:
        plan_sha256 = dispatcher.sha256_file(plan)
    except OSError:
        plan_sha256 = None

    if plan_sha256 is None:
        manifests, other_plans, unreadable = {}, 0, []
    else:
        manifests, other_plans, unreadable = cas_shard_manifests(
            Path(args.cas_root), plan_sha256=plan_sha256)
    from_cas = len(manifests)
    # Only under the transport that wrote them.  Under SLURM the manifest is
    # written inside a private checkout the job removes, so a file here is a
    # previous export's and carries no digest to say so.
    from_shared = 0
    if args.transport != "slurm":
        shared, shared_unreadable = shared_shard_manifests(results)
        unreadable += shared_unreadable
        from_shared = len([n for n in shared if n not in manifests])
        manifests = {**shared, **manifests}

    done_shards = sorted(manifests)
    total_bytes = sum(m["total_bytes"] for m in manifests.values())
    qbytes = sum(m["quantized_bytes"] for m in manifests.values())
    qparams = sum(m["quantized_params"] for m in manifests.values())
    missing = [n for n in range(1, 121) if n not in manifests]
    def gib(b):
        return b / 2 ** 30

    print(f"queue      {queue_counts(queue_root, transport=args.transport)}")
    if args.transport == "slurm":
        # The placement census describes worker offers, and under SLURM there
        # are none: the scheduler is the thing that knows what is waiting.
        depth = describe_squeue_depth()
        print(f"squeue     {depth}")
    else:
        depth = width(queue_root)
        print(f"width      {depth}")
    print(f"shards     {len(done_shards)}/120 encoded   missing {len(missing)}")
    if plan_sha256 is None:
        print(f"  plan     unreadable, so no receipt could be matched: {plan}")
    else:
        # The shared directory appears only under the transport that reads it,
        # so the line never reports a count for a place it did not consult.
        source = f"  read     {from_cas} from CAS receipts"
        if args.transport != "slurm":
            source += f", {from_shared} from {results}"
        print(f"{source}   plan {plan_sha256[:16]}")
    if other_plans:
        print(f"  stale    receipts encoding a different plan: {other_plans}")
    if unreadable:
        # Named, not just counted.  An operator reaches for this screen when
        # something is already wrong, and a bare count sends them looking for
        # which file it was.
        print(f"  skipped  entries that could not be read: {len(unreadable)}")
        for name in unreadable[:6]:
            print(f"           {name}")
        if len(unreadable) > 6:
            print(f"           and {len(unreadable) - 6} more")
    if missing[:12]:
        print(f"  next     {missing[:12]}{'...' if len(missing) > 12 else ''}")
    if qparams:
        print(f"body       {gib(qbytes):.3f} GiB over {qparams:,} params "
              f"= {qbytes*8/qparams:.4f} bpp")
        print(f"on disk    {gib(total_bytes):.3f} GiB   (Mia 163.560 GiB)")
    if absent or unreadable or plan_sha256 is None or depth.startswith(UNAVAILABLE):
        return EXIT_PARTIAL
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
