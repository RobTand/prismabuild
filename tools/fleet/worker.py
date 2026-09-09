"""Pull one action off the shared queue and run it. Runs on any box.

Everything that reaches the queue is inside ``main``. Importing this file
must not serve the queue: an importer is a reader, and this module's one
statement used to claim a real action off the live pool and execute it on
whatever box did the importing. A constant sweep, a documentation build or
an editor's auto-import is enough to trigger that, and the action it takes
is somebody else's work, run under the wrong identity at the wrong time.

Admission is the same admission the loop performs, so it answers to the same
two stops.  A one-shot that ignored them was a hole in both: the host-local
maintenance gate is what ``maintenance_begin`` writes and what every loop
obeys, and ``containment`` is what puts a claim in front of the broker's
draining refusal.  Serving one action rather than many changes how long this
process lives, not whose permission it needs.  The stops are imported from
``worker_loop`` rather than restated, so a fleet has one definition of
"drained" and not two that can drift.
"""
import sys, socket, json
from pathlib import Path
SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import pool
from worker_loop import (  # noqa: E402
    GENERATION_VERSION, RUNTIME_VERSION, _generation_at, maintenance_requested,
)


def refusal() -> str:
    """Why this box may not admit right now, or "" when it may.

    The generation check reads the same pair the loop compares at every idle
    poll: the receipt beside the bytes this process imported, and the receipt
    at the stable name the publisher moves.  A one-shot started from a path
    that has since been superseded would otherwise claim live work under
    retired code, which is the case a loop escapes by exiting.
    """

    if maintenance_requested():
        return "resource broker draining for maintenance; admission paused"
    loaded = _generation_at(GENERATION_VERSION)
    live = _generation_at(RUNTIME_VERSION)
    if loaded and live and loaded != live:
        return f"runtime moved {loaded} -> {live}; rerun from the live generation"
    return ""


def main() -> int:
    declined = refusal()
    if declined:
        print(json.dumps({
            "worker_host": socket.gethostname(),
            "declined": declined,
            "outcome": None,
        }, indent=1))
        return 0
    q = pool.PoolQueue(SH / "pb-queue")
    outcome = q.serve_once(
        tags=["gb10"], has_gpu=True, python="/usr/bin/python3", timeout_s=120,
        containment=True,
    )
    print(json.dumps({
        "worker_host": socket.gethostname(),
        "outcome": None if outcome is None else {
            k: outcome[k] for k in ("status", "returncode", "stdout", "elapsed_s")
        },
    }, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
