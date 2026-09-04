"""Progress of the GLM-5.3 Tessera fleet export, read from the queue and CAS."""
import json, sys
from pathlib import Path
SH = Path("/mnt/shared/prismabuild-fleet")
Q = SH / "pb-queue"
RES = SH / "checkout" / "results" / "glm53-tessera"
PARTS = Path("/mnt/shared/models/GLM-5.3-Flash-Tessera-E2M1K2-20260901-parts")

counts = {d: len(list((Q / d).glob("*.json"))) for d in
          ("ready", "claimed", "done", "failed") if (Q / d).is_dir()}

def width() -> str:
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
        sys.path.insert(0, str(SH / "repo" / "src"))
        from prismabuild import pool
        return pool.describe_placement_census(pool.PoolQueue(Q).placement_census())
    except Exception as exc:                       # noqa: BLE001 - diagnostic only
        return f"unavailable ({type(exc).__name__})"
manifests = sorted(RES.glob("shard-*.json")) if RES.is_dir() else []
done_shards, total_bytes, qbytes, qparams = [], 0, 0, 0
for m in manifests:
    d = json.loads(m.read_text())
    done_shards.append(d["shard"])
    total_bytes += d["total_bytes"]; qbytes += d["quantized_bytes"]
    qparams += d["quantized_params"]
missing = [n for n in range(1, 121) if n not in set(done_shards)]
gib = lambda b: b / 2 ** 30
print(f"queue      {counts}")
print(f"width      {width()}")
print(f"shards     {len(done_shards)}/120 encoded   missing {len(missing)}")
if missing[:12]:
    print(f"  next     {missing[:12]}{'...' if len(missing) > 12 else ''}")
if qparams:
    print(f"body       {gib(qbytes):.3f} GiB over {qparams:,} params "
          f"= {qbytes*8/qparams:.4f} bpp")
    print(f"on disk    {gib(total_bytes):.3f} GiB   (Mia 163.560 GiB)")
