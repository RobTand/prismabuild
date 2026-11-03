"""Scratch detached-publication checkout and worker offer."""
from pathlib import Path
import subprocess
from prismabuild import pool

def _checkout(tmp_path: Path) -> Path:
    work = tmp_path / "work"
    work.mkdir()
    (work / "seed.txt").write_text("sealed\n", encoding="utf-8")
    for args in (
        ("init", "-q"),
        ("config", "user.email", "test@example.invalid"),
        ("config", "user.name", "PrismaBuild test"),
        ("add", "seed.txt"),
        ("commit", "-qm", "sealed tree"),
    ):
        completed = subprocess.run(
            ["git", "-C", str(work), *args], capture_output=True, text=True,
        )
        assert completed.returncode == 0, completed.stderr
    return work



def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host="sparky", tags=["sparky", "gb10"], has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    return queue

