"""Pull one action off the shared queue and run it. Runs on any box."""
import sys, socket, json
from pathlib import Path
SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(SH / "repo" / "src"))
from prismabuild import pool

q = pool.PoolQueue(SH / "pb-queue")
outcome = q.serve_once(tags=["gb10"], has_gpu=True, python="/usr/bin/python3", timeout_s=120)
print(json.dumps({
    "worker_host": socket.gethostname(),
    "outcome": None if outcome is None else {
        k: outcome[k] for k in ("status", "returncode", "stdout", "elapsed_s")
    },
}, indent=1))
