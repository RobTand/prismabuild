"""A wait never reports a pool action done while its finish is still filing it.

``PoolQueue.finish`` moves ``claimed/<key>.json`` aside to a finish tombstone
first, then releases the claim's capacity, and only then writes
``done/<key>.json``.  The CAS receipt was published before any of that.  A
waiter that looked only at ``ready/`` and ``claimed/`` saw nothing outstanding
inside that window, found the receipt, and answered ``cache_hit`` -- exit 0 --
before the terminal record existed.  A caller that then read ``done/`` for the
worker's evidence found nothing: the 2026-09-22 publication canary's leg 3
failed with "no accepted-progress observation" exactly so, 1.23 s before the
record it wanted was written.

The pool's own claim gate already treats a finish tombstone as a live claim
(``already_claimed``); the waiter must agree with it.  Knowing the generation,
it then reads that generation's ending from the immutable attempt archive
(#817), which ``finish`` publishes before the claim moves.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbwait  # noqa: E402

from test_pbrun_detach import _checkout, _queue, _run_pbrun, _one_json_line  # noqa: E402


def test_a_wait_inside_the_finish_window_reports_the_archived_ending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    key = _one_json_line(capsys.readouterr())["action_key"]
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    inside: list[dict] = []
    real_release = pool.PoolQueue._release_reservation

    def release_then_observe(self, action_key, **kwargs):
        # This is the finish window: the claim is entombed, the ending is not
        # filed yet.  Observe it once, exactly as a waiter polling now would.
        if action_key == key and not inside:
            claimed = sorted(p.name for p in self.dir(pool.CLAIMED).iterdir())
            inside.append({
                "claimed": claimed,
                "done_exists": self.item_path(pool.DONE, key).exists(),
                "receipt": cas.lookup(pbwait.recorded_action(cas, key)) is not None,
                "found": pbrun.outstanding_submission(self, key),
                "rows": pbwait.wait_for_keys(self, [key], cas=cas, wait_s=0.0),
            })
        return real_release(self, action_key, **kwargs)

    monkeypatch.setattr(pool.PoolQueue, "_release_reservation", release_then_observe)
    served = queue.serve_once(
        tags=["sparky"], python=sys.executable, timeout_s=60.0,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    assert served is not None and served["status"] == "executed", served

    assert len(inside) == 1, "the finish window was never observed"
    window = inside[0]
    # The preconditions of the race, so the test cannot pass vacuously.
    assert window["receipt"] is True
    assert window["done_exists"] is False
    assert any(name.startswith(f"{key}.") and name.endswith(pool.TOMBSTONE_SUFFIX)
               for name in window["claimed"]), window["claimed"]
    # The fix: the entombed claim is still outstanding pool work, so the wait
    # names its generation and answers with that generation's real ending,
    # read from the immutable attempt archive (#817) that ``finish`` wrote
    # before the claim moved -- never ``cache_hit`` from the receipt alone.
    assert window["found"] is not None and window["found"][0] == "pool"
    rows = window["rows"]
    assert [row["status"] for row in rows] == ["executed"], rows
    assert rows[0]["transport"] == "pool"
    assert rows[0]["returncode"] == 0

    # Once the finish files the ending, the same wait reports it.
    after = pbwait.wait_for_keys(queue, [key], cas=cas, wait_s=5.0)
    assert [row["status"] for row in after] == ["executed"], after
    assert pbwait.verdict(after) == 0


def test_a_late_finish_record_is_outstanding_pool_work(tmp_path: Path) -> None:
    """``.late-finish`` is the other name the claim gate reads as claimed."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = "a" * 64
    record = {"action_key": key, "published_unix": 123.5}
    for suffix in (pool.TOMBSTONE_SUFFIX, pool.LATE_FINISH_SUFFIX):
        path = queue.dir(pool.CLAIMED) / f"{key}.1.sparky.7.abcdef01{suffix}"
        path.write_text(pbrun.json.dumps(record), encoding="utf-8")
        found = pbrun.outstanding_submission(queue, key)
        assert found is not None and found[:2] == ("pool", 123.5), found
        path.unlink()
    # Another key's tombstone and a malformed one are not this key's work.
    (queue.dir(pool.CLAIMED) / f"{'b' * 64}.1.h.1.x{pool.TOMBSTONE_SUFFIX}").write_text(
        pbrun.json.dumps({"action_key": "b" * 64, "published_unix": 9.0}),
        encoding="utf-8")
    (queue.dir(pool.CLAIMED) / f"{key}.2.h.1.y{pool.TOMBSTONE_SUFFIX}").write_text(
        "not json", encoding="utf-8")
    assert pbrun.outstanding_submission(queue, key) is None
