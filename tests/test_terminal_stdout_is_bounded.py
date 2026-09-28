"""#1203: a huge action stream never blanks the queue's readers.

Mutable ending records carry only a bounded tail of an action's stdout or
stderr; the immutable attempt log keeps every byte and the record names it.
No fleet queue, live mount, container, or other process is touched.
"""
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbstatus  # noqa: E402


KEY = "e" * 64
LEGACY_KEY = "d" * 64
# Comfortably past pbstatus's MAX_ENDING_RECORD_BYTES (8 MiB) so an unbounded
# record is unreadable, and past the inline tail so the cut path is real.
HUGE = ("chatter line %06d\n" * 500_000) % tuple(range(500_000)) + "FINAL-MARKER\n"


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    made = pool.PoolQueue(tmp_path / "pb-queue")
    made.ensure_layout()
    return made


def test_a_huge_stdout_stays_readable_in_the_ending(queue):
    queue.publish(action_key=KEY, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", max_attempts=1)
    claim = queue.claim()
    assert claim is not None and claim["action_key"] == KEY
    queue.finish(KEY, status="executed",
                 detail={"returncode": 0, "stdout": HUGE},
                 claim_snapshot=claim)

    rows = pbstatus.read_endings(queue.root)
    mine = next(row for row in rows if row.get("action_key") == KEY)
    assert not mine.get("unreadable"), mine.get("unreadable")

    record = json.loads(
        queue.item_path(pool.DONE, KEY).read_text(encoding="utf-8"))
    detail = record["detail"]
    assert len(detail["stdout"].encode("utf-8")) <= pool.TERMINAL_STREAM_TAIL_BYTES
    assert detail["stdout"].endswith("FINAL-MARKER\n")
    assert detail["stdout_bytes"] == len(HUGE.encode("utf-8"))
    assert detail["stdout_truncated"] is True
    # The full stream stays retrievable: the named log still holds every byte
    # at the recorded digest.
    log = detail["stdout_log"]
    raw = (queue.root / str(log["path"])).read_bytes()
    import hashlib
    assert len(raw) == detail["stdout_bytes"]
    assert hashlib.sha256(raw).hexdigest() == log["sha256"]
    assert raw.endswith(b"FINAL-MARKER\n")
    # Small streams keep their whole text with the same shape.
    assert detail["stderr"] == "" and detail["stderr_bytes"] == 0
    assert detail["stderr_truncated"] is False
    assert set(detail["stderr_log"]) == {"path", "bytes", "sha256"}


def test_pbstatus_degrades_one_legacy_oversized_record_per_row(tmp_path):
    root = tmp_path / "pb-queue"
    for name in ("ready", "claimed", "done", "failed", "withdrawn", "workers"):
        (root / name).mkdir(parents=True, exist_ok=True)
    small = {"schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": KEY,
             "status": "executed", "claimed_host": "sparky",
             "finished_host": "sparky", "published_unix": 1.0,
             "claimed_unix": 2.0, "finished_unix": 3.0,
             "detail": {"returncode": 0, "stdout": "fine\n"}}
    legacy = dict(small, action_key=LEGACY_KEY)
    legacy["detail"] = {"returncode": 0, "stdout": HUGE}
    (root / "done" / f"{KEY}.json").write_text(json.dumps(small))
    (root / "done" / f"{LEGACY_KEY}.json").write_text(json.dumps(legacy))

    rows = pbstatus.read_endings(root)
    assert len(rows) == 2
    good = next(row for row in rows if row.get("action_key") == KEY)
    bad = next(row for row in rows if row.get("action_key") == LEGACY_KEY)
    assert not good.get("unreadable")
    assert good.get("status") == "executed"
    assert "record exceeds 8388608-byte reader limit" in bad.get("unreadable", "")
    rendered = "\n".join(pbstatus.ending_lines(rows))
    assert "executed" in rendered
