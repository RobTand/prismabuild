"""The launch context names the queue root; nobody derives it from a path (#961).

A consumer that needed its queue root read it off the residency map's path
shape (``<queue>/residency/<key>.json``, then ``parent.parent``), and PQ's
``launch_queue_root`` copied that derivation.  The queue layout was therefore
a convention two codebases each re-implemented: move the map and every copy
resolves the wrong root.

Now the pool launcher publishes ``PRISMABUILD_QUEUE_ROOT`` for every action it
runs, ``core`` forwards it to the action unsealed and refuses an action that
seals it, and ``reader_lease.launch_queue_root`` is the SDK's one reader.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, reader_lease  # noqa: E402

KEY = "c" * 64


def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (pb.QUEUE_ROOT_ENV, pb.RESIDENCY_MAP_ENV):
        monkeypatch.delenv(name, raising=False)


def test_the_launcher_names_its_queue_for_every_action(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()

    # An ordinary action -- no residency, no map -- still learns its queue:
    # an owner binding its produced output needs the root and reads no map.
    env = queue.launch_environment({"action_key": KEY})

    assert env == {"PRISMABUILD_QUEUE_ROOT": str(tmp_path / "pb-queue")}


def test_the_published_root_survives_a_relocated_map(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    moved = tmp_path / "elsewhere" / "maps" / "deep" / f"{KEY}.json"
    moved.parent.mkdir(parents=True)
    moved.write_text("{}")
    monkeypatch.setattr(pool.PoolQueue, "residency_map_path",
                        lambda self, key: moved)
    launch = queue.launch_environment(
        {"action_key": KEY, "residency": {"leads": ["d" * 64]}})
    assert launch[pb.RESIDENCY_MAP_ENV] == str(moved)

    # Through core, as the action sees it, and back through the SDK.
    _clear(monkeypatch)
    for name, value in launch.items():
        monkeypatch.setenv(name, value)
    action_env = pb._residency_environment({"action_key": KEY}, {})

    assert action_env[pb.QUEUE_ROOT_ENV] == str(tmp_path / "pb-queue")
    assert reader_lease.launch_queue_root(action_env) == tmp_path / "pb-queue"
    # The path-shape derivation would have named the wrong directory.
    assert moved.parent.parent != tmp_path / "pb-queue"


def test_an_action_may_not_seal_the_queue_root(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _clear(monkeypatch)
    with pytest.raises(pb.ActionContractError, match="PRISMABUILD_QUEUE_ROOT"):
        pb._residency_environment({"action_key": KEY},
                                  {"PRISMABUILD_QUEUE_ROOT": "/somewhere"})


def test_a_launch_without_a_queue_names_none(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _clear(monkeypatch)

    assert pb.QUEUE_ROOT_ENV not in pb._residency_environment(
        {"action_key": KEY}, {})
    assert reader_lease.launch_queue_root({}) is None


def test_an_older_launcher_still_resolves_through_its_map_layout(
        tmp_path: Path) -> None:
    """A pool generation before #961 publishes only the map, in its layout."""

    env = {pb.RESIDENCY_MAP_ENV: str(tmp_path / "q" / "residency" / f"{KEY}.json")}

    assert reader_lease.launch_queue_root(env) == tmp_path / "q"


def test_the_published_root_wins_over_the_map_shape(tmp_path: Path) -> None:
    env = {pb.QUEUE_ROOT_ENV: str(tmp_path / "q"),
           pb.RESIDENCY_MAP_ENV: str(tmp_path / "other" / "residency" / "m.json")}

    assert reader_lease.launch_queue_root(env) == tmp_path / "q"
    assert "launch_queue_root" in reader_lease.__all__
    assert "QUEUE_ROOT_ENV" in pb.__all__
