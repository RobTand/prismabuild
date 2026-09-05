"""``PoolQueue()`` follows ``pool.DEFAULT_POOL_ROOT`` as it is *now*.

The default used to be bound at definition time, so re-pointing the module
attribute (which ``tests/conftest.py`` does for every test, to keep the suite
out of the live store) never reached a bare ``PoolQueue()``: it still opened
``/mnt/shared/pb-queue``.  The guard could only report such a write after it
had happened.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from prismabuild import pool


def test_a_bare_queue_opens_the_root_the_module_names_now(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wanted = tmp_path / "repointed-queue"
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
