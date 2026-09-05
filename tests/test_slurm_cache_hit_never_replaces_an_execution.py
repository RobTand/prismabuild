"""A ``cache_hit`` ending never replaces the record of the run that did the work.

Row 15b of fleet/slurm/smoke (two ``pbrun``s of one key started together)
measured the losing order on 2026-09-05: the second submitter's job was held
behind the first, read the receipt the first had just published, and both
submitters polled on the same tick.  The hit's ``exists()`` check ran before
the execution's record landed and its rename then replaced that record, so
``done/`` held a ``cache_hit`` with ``elapsed_s=0`` for work that had been
done.  ``publish_outcome`` now files a hit link-first, which cannot replace
anything, whatever order the two writers arrive in.
"""

from __future__ import annotations

import json
from pathlib import Path

from prismabuild import pool, slurm_lane

KEY = "83412e188ca513f56b55eea83de568d6765b4776f6587c594638dcb5b688267b"


def _file(queue_root: Path, status: str, generation: float):
    return slurm_lane.publish_outcome(
        queue_root=queue_root, action_key=KEY, published_unix=generation,
        published_by="rob@test", status=status, attempts=1, max_attempts=1,
        retry_safe=None,
    )


def _done(queue_root: Path) -> dict:
    return json.loads((queue_root / pool.DONE / f"{KEY}.json").read_text(encoding="utf-8"))


def test_a_hit_filed_after_the_execution_leaves_the_execution_standing(tmp_path: Path) -> None:
    """The order row 15b took: the hit's writer arrives second, from another generation."""
    assert _file(tmp_path, "executed", 100.0) is not None

    assert _file(tmp_path, "cache_hit", 200.0) is None

    record = _done(tmp_path)
    assert record["status"] == "executed"
    assert record["published_unix"] == 100.0


def test_an_execution_filed_after_a_hit_replaces_it(tmp_path: Path) -> None:
    """The other order: the hit lands first and the execution's record takes over."""
    assert _file(tmp_path, "cache_hit", 200.0) is not None
    assert _done(tmp_path)["status"] == "cache_hit"

    assert _file(tmp_path, "executed", 100.0) is not None

    record = _done(tmp_path)
    assert record["status"] == "executed"
    assert record["published_unix"] == 100.0


def test_a_hit_with_no_ending_at_all_is_still_filed(tmp_path: Path) -> None:
    assert _file(tmp_path, "cache_hit", 200.0) is not None

    assert _done(tmp_path)["status"] == "cache_hit"


def test_the_writer_links_rather_than_renames_and_leaves_no_debris(tmp_path: Path) -> None:
    target = tmp_path / "done" / "record.json"

    assert slurm_lane._publish_json_if_absent(target, {"who": "first"}) is True
    assert slurm_lane._publish_json_if_absent(target, {"who": "second"}) is False

    assert json.loads(target.read_text(encoding="utf-8")) == {"who": "first"}
    assert sorted(p.name for p in target.parent.iterdir()) == ["record.json"]
