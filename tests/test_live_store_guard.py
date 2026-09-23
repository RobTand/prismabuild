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
    monkeypatch.setattr(conftest, "LIVE_CENSUS", True)
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
    # Marked, so the call-time hook lets the write through and the census has
    # something to find: this case is about the census, not the hook.
    (tests / "test_writer.py").write_text(
        "import os\nfrom pathlib import Path\nimport pytest\n"
        "@pytest.mark.live_store(reason='the census must still catch this')\n"
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


# --------------------------------------------------------------------------
# The call-time guard (#1019)
#
# Each case runs a child session against a scratch store named by
# ``PRISMABUILD_TEST_LIVE_ROOT``, so a guard that fails to refuse writes into
# ``tmp_path`` and never into the fleet's real store.
# --------------------------------------------------------------------------

#: The directory holding this file, so a child session can import the suite's
#: own helpers (``test_core._action``) and the fleet tools.
_TESTS = Path(__file__).resolve().parent


def _child_session(tmp_path: Path, source: str) -> tuple[Path, subprocess.CompletedProcess]:
    """Run ``source`` as a one-file suite whose live store is a scratch store."""

    live = _store(tmp_path / "scratch-live")
    suite = tmp_path / "suite"
    tests = suite / "tests"
    tests.mkdir(parents=True)
    (tests / "conftest.py").symlink_to(Path(conftest.__file__).resolve())
    (tests / "test_child.py").write_text(source)
    env = dict(os.environ, PRISMABUILD_TEST_LIVE_ROOT=str(live),
               PRISMABUILD_TEST_LIVE_PROBE_TIMEOUT_S="5")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:randomly",
         "--basetemp", str(tmp_path / "child-basetemp"), "tests"],
        cwd=suite, env=env, text=True, capture_output=True, timeout=120,
    )
    return live, result


def test_a_write_into_the_live_store_fails_at_the_call(tmp_path: Path) -> None:
    """The failing test is named, with the path, before any byte is written.

    Before #1019 this write succeeded and only the finish census could report
    it, after the fact, as a session failure that named no test.
    """

    live, result = _child_session(tmp_path, (
        "import os\nfrom pathlib import Path\n"
        "def test_write():\n"
        "    root = Path(os.environ['PRISMABUILD_TEST_LIVE_ROOT'])\n"
        "    with open(root / 'pb-queue/done/x.json', 'w') as handle:\n"
        "        handle.write('{}')\n"
    ))
    output = result.stdout + result.stderr
    target = live / "pb-queue/done/x.json"
    assert not target.exists(), output
    assert result.returncode == 1, output
    assert "1 failed" in result.stdout, output
    assert str(target) in result.stdout, output
    assert "live-store guard" in result.stdout, output


def test_a_cas_request_filed_through_an_unrepointed_default_fails_at_the_call(
        tmp_path: Path) -> None:
    """The September leak's shape: ``pbrun.SH`` left at the store, a CAS request.

    ``PrismaBuildCAS`` opens every directory through ``_open_directory_nofollow``,
    which starts at ``/`` and descends one ``dir_fd``-relative component at a
    time, so no audited call sees an absolute path under the store. The
    request must be refused anyway.
    """

    live, result = _child_session(tmp_path, (
        "import os, sys\nfrom pathlib import Path\n"
        f"sys.path.insert(0, {str(_TESTS)!r})\n"
        "import pbrun\n"
        "from prismabuild import core as pb\n"
        "from test_core import _action\n"
        "def test_file_a_request(tmp_path):\n"
        "    # The default the per-test fixture did not reach.\n"
        "    pbrun.SH = Path(os.environ['PRISMABUILD_TEST_LIVE_ROOT'])\n"
        "    checkout = tmp_path / 'checkout'\n"
        "    checkout.mkdir()\n"
        "    pb.PrismaBuildCAS(pbrun.SH / 'cas').publish_action_request(\n"
        "        _action(checkout))\n"
    ))
    output = result.stdout + result.stderr
    assert not (live / "cas/requests").exists(), output
    assert result.returncode == 1, output
    assert "1 failed" in result.stdout, output
    assert str(live / "cas") in result.stdout, output
    assert "live-store guard" in result.stdout, output


def test_the_census_is_opt_in(monkeypatch, capsys) -> None:
    """Without the opt-in, a session neither probes nor walks the store."""

    class Plugins:
        @staticmethod
        def get_plugin(_name: str):
            return None

    config = SimpleNamespace(pluginmanager=Plugins())
    session = SimpleNamespace(config=config, exitstatus=0)
    monkeypatch.setattr(conftest, "LIVE_CENSUS", False)
    monkeypatch.setattr(conftest, "reachable", lambda *_a, **_k: pytest.fail(
        "an opted-out session probed the store"))
    monkeypatch.setattr(conftest, "bounded_listing", lambda *_a, **_k: pytest.fail(
        "an opted-out session walked the store"))
    monkeypatch.setattr(conftest, "bounded_leaked_entries", lambda *_a, **_k: pytest.fail(
        "an opted-out session walked the store"))
    conftest.pytest_sessionstart(session)
    conftest.pytest_sessionfinish(session, 0)
    assert session.exitstatus == 0
    assert capsys.readouterr().out == ""


@pytest.fixture
def guarded(tmp_path: Path, monkeypatch) -> Path:
    """A scratch store the in-process hook guards in place of the real one."""

    live = _store(tmp_path / "guarded-live")
    (live / "pb-queue/done/old.json").write_text("{}")
    monkeypatch.setattr(conftest, "GUARDED", conftest._guarded_roots(live))
    yield live
    # The refusals below were expected; do not let the swallowed-refusal
    # check fail the test that asked for them.
    conftest.REFUSALS.clear()


_REFUSED_CALLS = {
    "open for writing": lambda live, out: open(live / "pb-queue/done/x.json", "w"),
    "open for reading": lambda live, out: open(live / "pb-queue/done/old.json"),
    "os.open of the directory": lambda live, out: os.open(live / "cas", os.O_RDONLY),
    "Path.write_text": lambda live, out: (live / "slurm/x.json").write_text("{}"),
    "os.listdir": lambda live, out: os.listdir(live / "pb-queue"),
    "os.scandir": lambda live, out: os.scandir(live),
    "os.walk": lambda live, out: next(os.walk(live)),
    "os.mkdir": lambda live, out: os.mkdir(live / "new"),
    "Path.mkdir(parents=True)": lambda live, out: (live / "cas/requests/ab").mkdir(
        parents=True, exist_ok=True),
    "os.rename into": lambda live, out: os.rename(out, live / "pb-queue/done/y.json"),
    "os.rename out of": lambda live, out: os.rename(live / "pb-queue/done/old.json", out),
    "os.replace into": lambda live, out: os.replace(out, live / "pb-queue/done/y.json"),
    "os.link into": lambda live, out: os.link(out, live / "pb-queue/done/y.json"),
    "os.symlink placed in": lambda live, out: os.symlink(out, live / "link"),
    "os.remove": lambda live, out: os.remove(live / "pb-queue/done/old.json"),
    "os.unlink": lambda live, out: os.unlink(live / "pb-queue/done/old.json"),
    "os.rmdir": lambda live, out: os.rmdir(live / "slurm"),
    "os.chmod": lambda live, out: os.chmod(live / "pb-queue/done/old.json", 0o600),
    "os.truncate": lambda live, out: os.truncate(live / "pb-queue/done/old.json", 0),
    "os.utime": lambda live, out: os.utime(live / "pb-queue/done/old.json"),
    "shutil.rmtree": lambda live, out: __import__("shutil").rmtree(live / "slurm"),
    "the store root itself": lambda live, out: os.listdir(live),
    "a path that leaves and re-enters the store": lambda live, out: open(
        live.parent / "elsewhere/../guarded-live/pb-queue/done/z.json", "w"),
}


@pytest.mark.parametrize("call", sorted(_REFUSED_CALLS))
def test_each_audited_call_under_the_store_is_refused_before_it_runs(
        guarded: Path, tmp_path: Path, call: str) -> None:
    outside = tmp_path / "outside.json"
    outside.write_text("{}")

    def inventory() -> list[str]:
        with conftest._LiveAccess():
            return sorted(str(p) for p in guarded.rglob("*"))

    before = inventory()
    with pytest.raises(RuntimeError, match="live-store guard") as refused:
        _REFUSED_CALLS[call](guarded, outside)
    assert str(guarded) in str(refused.value)
    assert outside.read_text() == "{}"
    conftest.REFUSALS.clear()
    assert inventory() == before


def test_a_relative_path_is_placed_against_the_working_directory(
        guarded: Path, monkeypatch) -> None:
    monkeypatch.chdir(guarded.parent)
    with pytest.raises(RuntimeError, match=str(guarded / "pb-queue/done/r.json")):
        open("guarded-live/pb-queue/done/r.json", "w")
    assert not (guarded / "pb-queue/done/r.json").exists()


def test_calls_outside_the_store_pass(guarded: Path, tmp_path: Path) -> None:
    """Including a link whose *target* is in the store: making it reads nothing."""

    (tmp_path / "sibling-of-guarded-live").mkdir()  # a shared prefix, not a child
    (tmp_path / "sibling-of-guarded-live/ok.json").write_text("{}")
    os.symlink(guarded / "pb-queue", tmp_path / "pointer")
    assert os.listdir(tmp_path / "sibling-of-guarded-live") == ["ok.json"]
    assert conftest.REFUSALS == []


def test_a_sibling_whose_name_extends_the_root_is_not_the_store(
        tmp_path: Path, monkeypatch) -> None:
    live = tmp_path / "live"
    live.mkdir()
    monkeypatch.setattr(conftest, "GUARDED", conftest._guarded_roots(live))
    (tmp_path / "live-2").mkdir()
    (tmp_path / "live-2/ok.json").write_text("{}")
    assert os.listdir(tmp_path / "live-2") == ["ok.json"]


@pytest.mark.live_store(reason="proves the marker lets a test through")
def test_a_marked_test_is_let_through(guarded: Path) -> None:
    assert "old.json" in os.listdir(guarded / "pb-queue/done")
    assert conftest.REFUSALS == []


def test_a_cas_directory_walk_is_refused_at_the_descent(guarded: Path) -> None:
    """The no-follow walk announces the absolute directory before descending."""

    from prismabuild import core as pb

    with pytest.raises(RuntimeError, match=str(guarded / "cas/requests/ab")):
        pb._open_directory_nofollow(guarded / "cas/requests/ab", where="test",
                                    create=True)
    assert not (guarded / "cas/requests").exists()


def test_a_slurm_lane_directory_walk_is_refused_at_the_descent(guarded: Path) -> None:
    from prismabuild import slurm

    with pytest.raises(RuntimeError, match=str(guarded / "slurm/jobs/x")):
        slurm._ensure_real_directory(guarded / "slurm/jobs/x",
                                     root=guarded / "slurm", where="test")
    with pytest.raises(RuntimeError, match=str(guarded / "slurm")):
        slurm._open_directory_nofollow(guarded / "slurm", where="test")
    assert not (guarded / "slurm/jobs").exists()


def test_a_caught_refusal_still_fails_the_test(tmp_path: Path) -> None:
    """Code that swallows exceptions cannot turn a refused call into a pass."""

    live, result = _child_session(tmp_path, (
        "import os\nfrom pathlib import Path\n"
        "def test_swallow():\n"
        "    root = Path(os.environ['PRISMABUILD_TEST_LIVE_ROOT'])\n"
        "    try:\n"
        "        (root / 'pb-queue/done/s.json').write_text('{}')\n"
        "    except Exception:\n"
        "        pass\n"
    ))
    output = result.stdout + result.stderr
    assert not (live / "pb-queue/done/s.json").exists(), output
    assert result.returncode == 1, output
    assert "the refusal was caught" in result.stdout, output
