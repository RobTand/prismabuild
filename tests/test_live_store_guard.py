"""The session guard in ``conftest.py`` tells a test's leak from the fleet's work."""
from __future__ import annotations

from pathlib import Path

import conftest


def _store(root: Path) -> Path:
    for rel in conftest.WATCHED:
        (root / rel).mkdir(parents=True, exist_ok=True)
    return root


def test_a_new_record_naming_the_basetemp_is_a_leak(tmp_path: Path) -> None:
    live = _store(tmp_path / "live")
    basetemp = str(tmp_path / "pytest-7")
    before = conftest.listing(live)
    (live / "pb-queue/done/ab.json").write_text(
        '{"detail": {"stdout_path": "' + basetemp + '/lane/ab/1.out"}}'
    )
    (live / "slurm/cd").mkdir()
    (live / "slurm/cd/latest.json").write_text('{"script": "' + basetemp + '/x"}')
    leaked, unattributed = conftest.leaked_entries(
        before, conftest.listing(live), live_root=live, basetemp=basetemp
    )
    assert leaked == ["pb-queue/done/ab.json", "slurm/cd"]
    assert unattributed == []


def test_a_new_record_from_the_fleet_is_reported_not_counted(tmp_path: Path) -> None:
    live = _store(tmp_path / "live")
    before = conftest.listing(live)
    (live / "pb-queue/done/ef.json").write_text(
        '{"detail": {"stdout_path": "/home/rob/tmp/lane/ef/9.out"}}'
    )
    leaked, unattributed = conftest.leaked_entries(
        before, conftest.listing(live), live_root=live,
        basetemp=str(tmp_path / "pytest-7"),
    )
    assert leaked == []
    assert unattributed == ["pb-queue/done/ef.json"]


def test_nothing_new_is_nothing(tmp_path: Path) -> None:
    live = _store(tmp_path / "live")
    before = conftest.listing(live)
    assert conftest.leaked_entries(
        before, conftest.listing(live), live_root=live, basetemp="/nowhere"
    ) == ([], [])


def test_a_missing_store_lists_empty(tmp_path: Path) -> None:
    assert conftest.listing(tmp_path / "absent") == {
        rel: set() for rel in conftest.WATCHED
    }
