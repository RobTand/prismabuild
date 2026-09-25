"""``PoolQueue()`` follows ``pool.DEFAULT_POOL_ROOT`` as it is *now*.

The default used to be bound at definition time, so re-pointing the module
attribute (which ``tests/conftest.py`` does for every test, to keep the suite
out of the live store) never reached a bare ``PoolQueue()``: it still opened
``/mnt/shared/pb-queue``.  The guard could only report such a write after it
had happened.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import pool


def test_a_bare_queue_opens_the_root_the_module_names_now(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wanted = tmp_path / "repointed-queue"
    wanted.mkdir()
    monkeypatch.setattr(pool, "DEFAULT_POOL_ROOT", wanted)

    assert pool.PoolQueue().root == wanted


def test_an_explicit_root_still_wins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pool, "DEFAULT_POOL_ROOT", tmp_path / "ignored")
    chosen = tmp_path / "chosen"

    assert pool.PoolQueue(chosen).root == chosen


def test_a_relative_default_is_refused_like_a_relative_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pool, "DEFAULT_POOL_ROOT", Path("relative/queue"))

    with pytest.raises(pool.PoolContractError):
        pool.PoolQueue()


# --- #976: the default names the fleet's queue, and a missing one refuses ---

def test_a_missing_default_root_is_refused_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main: a bare ``PoolQueue()`` over a directory no box has read as an
    empty queue, so ``retire_worker`` said ``no worker record`` and pbtest's
    ceilings fell back to 7200 s for weeks (#939).

    branch: the default root is checked when it is the one used, and the
    refusal names the path it looked for.
    """

    absent = tmp_path / "no-such-queue"
    monkeypatch.setattr(pool, "DEFAULT_POOL_ROOT", absent)

    with pytest.raises(pool.PoolContractError, match=str(absent)):
        pool.PoolQueue()
    assert not absent.exists(), "refusing must not create the root it refused"


def test_an_explicit_missing_root_is_still_the_callers_to_create(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the default is checked: a caller that names a root owns it, and
    every fixture and bench builds its queue under a fresh directory."""

    monkeypatch.setattr(pool, "DEFAULT_POOL_ROOT", tmp_path / "also-absent")

    assert pool.PoolQueue(tmp_path / "fresh").root == tmp_path / "fresh"


def test_the_default_root_is_the_queue_pbrun_submits_to() -> None:
    """main: ``/mnt/shared/pb-queue``, one directory above the fleet's queue.

    Read in a child with ``PRISMABUILD_POOL_ROOT`` unset, because the conftest
    repoints both the attribute and the variable for every test here.  The
    child imports nothing that touches the store: ``pool`` and ``pbrun`` only
    build paths at import.
    """

    repository = Path(__file__).resolve().parents[1]
    environment = {name: value for name, value in os.environ.items()
                   if name != "PRISMABUILD_POOL_ROOT"}
    probe = (
        "import sys\n"
        f"sys.path[:0] = [{str(repository / 'src')!r}, "
        f"{str(repository / 'tools' / 'fleet')!r}]\n"
        "from prismabuild import pool\n"
        "import pbrun\n"
        "print(pool.DEFAULT_POOL_ROOT)\n"
        "print(pbrun.SH / 'pb-queue')\n"
    )
    shown = subprocess.run(
        [sys.executable, "-c", probe], env=environment, capture_output=True,
        text=True, check=True, timeout=120,
    ).stdout.split()

    assert shown == ["/mnt/shared/prismabuild-fleet/pb-queue"] * 2
