"""One validating CAS-request reader (issue #1313).

``PrismaBuildCAS.read_action_request`` owns the ``requests/`` read with
validation plus the key check; ``slurm_lane.recorded_action`` and
``pbwait.recorded_action`` delegate to it. A stale or invalid action reads
as absent (None), not present. tmp_path-only; no fleet.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import slurm_lane  # noqa: E402
import pbwait  # noqa: E402
from test_core import _action  # noqa: E402


def _checkout(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    (path / "task_code.py").write_text("RESULT = 1\n", encoding="utf-8")
    return path


def _store(cas_root: Path, key: str, value: object) -> None:
    path = cas_root / "requests" / key[:2] / f"{key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _readers(cas: pb.PrismaBuildCAS, key: str) -> list:
    return [
        cas.read_action_request(key),
        slurm_lane.recorded_action(cas, key),
        pbwait.recorded_action(cas, key),
    ]


def test_valid_request_reads_present(tmp_path):
    action = _action(_checkout(tmp_path / "checkout"))
    key = str(action["action_key"])
    _store(tmp_path / "cas", key, action)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    for found in _readers(cas, key):
        assert isinstance(found, dict) and found["action_key"] == key


def test_invalid_body_reads_absent(tmp_path):
    key = "a" * 64
    _store(tmp_path / "cas", key, {"not": "an action"})
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert _readers(cas, key) == [None, None, None]


def test_stale_key_reads_absent(tmp_path):
    action = _action(_checkout(tmp_path / "checkout"))
    _store(tmp_path / "cas", "b" * 64, action)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert _readers(cas, "b" * 64) == [None, None, None]


def test_missing_request_reads_absent(tmp_path):
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert _readers(cas, "c" * 64) == [None, None, None]
