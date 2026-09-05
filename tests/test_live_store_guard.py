"""The session guard in ``conftest.py`` tells a test's leak from the fleet's work."""
from __future__ import annotations

import os
from pathlib import Path

import conftest

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
