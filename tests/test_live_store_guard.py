"""The session guard in ``conftest.py`` tells a test's leak from the fleet's work."""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import conftest
import pytest

# The pull queue's own materialization fixture, so this file proves the guard
# against the sequence a real test runs rather than a re-creation of it.
from test_pool import _materialization_item


#: The queue directories a terminal record is filed into, which the guard now
#: reaches by walking ``pb-queue`` rather than by naming each one.
QUEUE_STATES = ("ready", "claimed", "done", "failed", "withdrawn")


def _store(root: Path) -> Path:
    for rel in conftest.WATCHED:
        (root / rel).mkdir(parents=True, exist_ok=True)
    for state in QUEUE_STATES:
        (root / "pb-queue" / state).mkdir(parents=True, exist_ok=True)
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
    """A box without the mount gets a guard that reports nothing, not an error."""

    assert conftest.listing(tmp_path / "absent") == {
        "": set(), **{rel: set() for rel in conftest.WATCHED}
    }


def test_a_reachable_store_with_a_blocked_traversal_is_not_certified(
        tmp_path: Path, monkeypatch, capsys) -> None:
    """Session hooks bound a traversal that stalls after the root probe (#554)."""

    live = _store(tmp_path / "live")

    def blocked(_root: Path):
        time.sleep(30)
        return set(), True, []

    class Plugins:
        @staticmethod
        def get_plugin(_name: str):
            return None

    config = SimpleNamespace(pluginmanager=Plugins())
    session = SimpleNamespace(config=config)
    monkeypatch.setattr(conftest, "LIVE_ROOT", live)
    monkeypatch.setattr(conftest, "LIVE_CENSUS_TIMEOUT_S", 0.1)
    monkeypatch.setattr(conftest, "reachable", lambda _root: True)
    monkeypatch.setattr(conftest, "_walk", blocked)
    started = time.monotonic()
    conftest.pytest_sessionstart(session)
    conftest.pytest_sessionfinish(session, 0)
    elapsed = time.monotonic() - started

    assert elapsed < 10, f"the blocked census held pytest for {elapsed:.1f}s"
    assert config._pb_live_before is None
    assert "start census is unavailable (timed_out)" in capsys.readouterr().out


def test_blocked_leak_attribution_is_not_counted_as_a_clean_census(
        tmp_path: Path, monkeypatch) -> None:
    """Content reads happen under the same abandonable finish boundary (#554)."""

    live = _store(tmp_path / "live")
    before = conftest.listing(live)
    (live / "pb-queue/done/new.json").write_text('{"path": "new"}')

    def blocked(_path: Path, _needle: str):
        time.sleep(30)
        return False, True, []

    monkeypatch.setattr(conftest, "_names", blocked)
    started = time.monotonic()
    result = conftest.bounded_leaked_entries(
        before, live_root=live, basetemp=str(tmp_path / "pytest"), timeout_s=0.1,
    )
    assert time.monotonic() - started < 10
    assert result["status"] == "unavailable"
    assert result["reason"] == "timed_out"


def test_top_level_files_and_absent_optional_directories_are_complete(tmp_path: Path) -> None:
    """Only failed directory reads make an otherwise valid census partial."""

    live = tmp_path / "live"
    live.mkdir()
    (live / "fleet-note.json").write_text("{}")
    result = conftest.bounded_listing(live, timeout_s=1)
    assert result["status"] == "complete"
    assert result["listing"][""] == {"fleet-note.json"}
    assert set(conftest.WATCHED).issubset(result["listing"])


@pytest.mark.parametrize("timeout_s", [0, -1, float("inf"), float("nan")])
def test_census_timeout_cannot_select_an_unbounded_reader(timeout_s: float) -> None:
    with pytest.raises(ValueError, match="finite positive"):
        conftest.bounded_listing(timeout_s=timeout_s)


def test_xdist_workers_skip_the_full_history_census(monkeypatch) -> None:
    """The controller's shared basetemp owns one before/after inventory."""

    config = SimpleNamespace(workerinput={})
    session = SimpleNamespace(config=config)
    monkeypatch.setattr(
        conftest, "bounded_listing",
        lambda *_args, **_kwargs: pytest.fail("an xdist worker censused live history"),
    )
    conftest.pytest_sessionstart(session)
    assert config._pb_live_guard_worker is True


@pytest.mark.parametrize("workers", [0, 2])
def test_real_session_rejects_a_leak_from_its_test_worker(tmp_path: Path, workers: int) -> None:
    """The controller's before/after observations must catch worker basetemps."""

    live = _store(tmp_path / "scratch-live")
    suite = tmp_path / "suite"
    tests = suite / "tests"
    tests.mkdir(parents=True)
    (tests / "conftest.py").symlink_to(Path(conftest.__file__).resolve())
    (tests / "test_writer.py").write_text(
        "import os\nfrom pathlib import Path\n"
        "def test_write(tmp_path):\n"
        "    root = Path(os.environ['PRISMABUILD_TEST_LIVE_ROOT'])\n"
        "    (root / 'pb-queue/done/leak.json').write_text(str(tmp_path))\n"
    )
    env = dict(os.environ, PRISMABUILD_TEST_LIVE_ROOT=str(live),
               PRISMABUILD_TEST_LIVE_PROBE_TIMEOUT_S="5")
    command = [sys.executable, "-m", "pytest", "-q", "-p", "no:randomly",
               "--basetemp", str(tmp_path / "child-basetemp")]
    if workers:
        command += ["-n", str(workers)]
    result = subprocess.run(command + ["tests"], cwd=suite, env=env,
                            text=True, capture_output=True, timeout=45)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "1 passed" in result.stdout, result.stdout + result.stderr
    assert "were written by this test session" in result.stdout
    assert "did not certify" not in result.stdout


def test_a_traversal_error_is_partial_evidence(tmp_path: Path, monkeypatch) -> None:
    live = _store(tmp_path / "live")

    def denied(_root, *, onerror):
        onerror(PermissionError("injected unreadable directory"))
        return iter(())

    monkeypatch.setattr(conftest.os, "walk", denied)
    observation = conftest.bounded_listing(live, timeout_s=1)
    assert observation["status"] == "partial"
    assert "injected unreadable directory" in observation["detail"]
    assert "listing" not in observation


def test_a_positive_leak_in_partial_finish_evidence_still_fails_session(
        tmp_path: Path, monkeypatch, capsys) -> None:
    """A known leak remains a failure even when another finish read is short."""

    class Plugins:
        @staticmethod
        def get_plugin(_name: str):
            return None

    config = SimpleNamespace(
        pluginmanager=Plugins(),
        _pb_live_before={"": set()},
        _tmp_path_factory=SimpleNamespace(getbasetemp=lambda: tmp_path / "pytest"),
    )
    session = SimpleNamespace(config=config, exitstatus=0)
    monkeypatch.setattr(conftest, "bounded_leaked_entries", lambda *_args, **_kwargs: {
        "status": "partial", "reason": "traversal_error", "detail": "denied",
        "leaked": ["pb-queue/done/leak.json"], "abandoned": [],
    })
    conftest.pytest_sessionfinish(session, 0)
    assert session.exitstatus == 1
    assert "despite the partial census" in capsys.readouterr().out


def test_the_quarantine_is_not_watched(tmp_path: Path) -> None:
    """It holds records already moved out of the fleet's way."""

    live = _store(tmp_path / "live")
    (live / "quarantine").mkdir()
    before = conftest.listing(live)
    (live / "quarantine/moved.json").write_text('{"basetemp": "/anything"}')
    assert conftest.leaked_entries(
        before, conftest.listing(live), live_root=live, basetemp="/anything"
    ) == ([], [])


def test_an_entry_written_into_the_store_itself_is_seen(tmp_path: Path) -> None:
    """``pbrun`` and ``seal_and_publish`` address the store root directly."""

    live = _store(tmp_path / "live")
    basetemp = str(tmp_path / "pytest-7")
    before = conftest.listing(live)
    (live / "out_sparky.json").write_text('{"where": "' + basetemp + '/x"}')
    leaked, unattributed = conftest.leaked_entries(
        before, conftest.listing(live), live_root=live, basetemp=basetemp
    )
    assert leaked == ["out_sparky.json"]
    assert unattributed == []


def test_a_materialized_checkout_lands_under_the_guard_root(tmp_path: Path) -> None:
    """A test that materializes a sealed snapshot must not use the real root.

    ``materialize._execution_checkout`` resolves its base from
    ``materialize.LOCAL_CHECKOUT_ROOT`` when the caller names none, and the
    pull queue hands it ``pool.LOCAL_CHECKOUT_ROOT``. Both are module
    attributes bound at import from ``PRISMABUILD_LOCAL_CHECKOUT_ROOT``, so
    the guard has to repoint the attributes for a module it already imported
    and set the variable for a fresh import or a child process.

    Seven tests materialize without naming a root, so before the guard
    covered it they all built real checkouts under
    ``/home/rob/tmp/prismabuild-checkouts``.
    """

    from prismabuild import materialize, pool

    real = Path(materialize.DEFAULT_LOCAL_CHECKOUT_ROOT)
    for resolved in (materialize.LOCAL_CHECKOUT_ROOT, pool.LOCAL_CHECKOUT_ROOT):
        assert resolved != real
        assert tmp_path in resolved.parents, resolved
    # A fresh import, and a child process such as ``slurm_job``, read the
    # variable rather than the attribute.
    from_environment = Path(os.environ[materialize.LOCAL_CHECKOUT_ROOT_ENV])
    assert tmp_path in from_environment.parents, from_environment

    # And the resolution the materializer actually performs, end to end.
    item = _materialization_item(tmp_path)
    with pool._execution_checkout(item) as checkout:
        assert tmp_path in checkout.parents, checkout
        assert real not in checkout.parents


def test_a_cas_request_naming_the_basetemp_is_a_leak(tmp_path: Path) -> None:
    """The CAS is half the September leak, and it sits three levels down.

    145 requests and 6 receipts were filed into the live CAS between
    2026-09-04 and 2026-09-05. A guard that listed only the queue directories
    and the lane root saw none of them, and a guard that listed ``cas`` alone
    would have seen the four directory names that were already there.
    """

    live = _store(tmp_path / "live")
    (live / "cas/requests/ab").mkdir(parents=True)
    basetemp = str(tmp_path / "pytest-7")
    before = conftest.listing(live)
    (live / "cas/requests/ab/cd.json").write_text(
        '{"params": {"result_path": "' + basetemp + '/shard/result.txt"}}'
    )
    leaked, unattributed = conftest.leaked_entries(
        before, conftest.listing(live), live_root=live, basetemp=basetemp
    )
    assert leaked == ["cas/requests/ab/cd.json"]
    assert unattributed == []
