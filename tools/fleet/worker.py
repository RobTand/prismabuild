"""Pull one action off the shared queue and run it. Runs on any box.

Everything that reaches the queue is inside ``main``. Importing this file
must not serve the queue: an importer is a reader, and this module's one
statement used to claim a real action off the live pool and execute it on
whatever box did the importing. A constant sweep, a documentation build or
an editor's auto-import is enough to trigger that, and the action it takes
is somebody else's work, run under the wrong identity at the wrong time.
"""
import sys, socket, json
from pathlib import Path
SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import pool


def main() -> None:
    q = pool.PoolQueue(SH / "pb-queue")
    outcome = q.serve_once(
        tags=["gb10"], has_gpu=True, python="/usr/bin/python3", timeout_s=120
    )
    print(json.dumps({
        "worker_host": socket.gethostname(),
        "outcome": None if outcome is None else {
            k: outcome[k] for k in ("status", "returncode", "stdout", "elapsed_s")
        },
    }, indent=1))


if __name__ == "__main__":
    main()
