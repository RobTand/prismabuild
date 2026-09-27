"""Surface missing declared inputs without pretending absence proves death (#1184)."""
from pathlib import Path

import pbstatus
from prewarm_fixture import Fleet
from prismabuild import pool


def test_ready_row_names_its_missing_prewarm_input(tmp_path):
    fleet = Fleet(tmp_path)
    path, size = fleet.file("lost.pt", 4096)
    key = fleet.action("missing-input", [(path, size)])
    Path(path).unlink()
    fleet.cycle(fleet.args(readers=1))
    proof = fleet.queue.prewarm(key)
    assert proof["entries_warmed"] == 0
    assert "[Errno 2]" in proof["errors"][0]
    row = next(row for row in pbstatus.read_pool(fleet.queue.root)["jobs"]
               if row["action_key"] == key)
    assert "prewarm input errors" in row["reason"]
    assert "lost.pt" in row["reason"]
    assert "permanence unproven" in row["reason"]
    # No producer/retry binding exists in this manifest: absence alone is not
    # authority to terminalize. A later legitimate landing may still cure it.
    assert fleet.queue.item_path(pool.READY, key).exists()
    Path(path).write_bytes(b"x" * size)
    fleet.cycle(fleet.args(readers=1))
    row = next(row for row in pbstatus.read_pool(fleet.queue.root)["jobs"]
               if row["action_key"] == key)
    assert "prewarm input errors" not in row["reason"]


def test_unreadable_prewarm_is_unknown_not_missing(tmp_path):
    fleet = Fleet(tmp_path)
    key = fleet.action("unknown-input", [fleet.file("input.pt", 4)])
    path = fleet.queue.prewarm_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not json")
    report = pbstatus.read_pool(fleet.queue.root)
    row = next(row for row in report["jobs"] if row["action_key"] == key)
    assert "prewarm evidence unreadable" in row["reason"]
    assert "missing input" not in row["reason"]
    assert report["queue"]["complete"] is False
