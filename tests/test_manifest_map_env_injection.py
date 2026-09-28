"""#1247 Option 1: the map reaches a manifest row that never sealed residency.

``residency_map_environment`` injects ``PRISMABUILD_RESIDENCY_MAP`` for a row
whose sealed residency block names leads (#583) and, new, for a row that
declares a ``pbcampaign.data-manifest`` input when a composed map for its
action key exists: the declaration is the opt-in, the map is the evidence,
and neither alone injects.  A row with no map launches byte-identically.
"""
from __future__ import annotations

import json
from pathlib import Path

import prismabuild.core as pb
import prismabuild.pool as pool


def _queue_at(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    return queue


def _item(key: str, cas_root: Path, *, residency=None) -> dict:
    item = {"action_key": key, "cas_root": str(cas_root)}
    if residency is not None:
        item["residency"] = residency
    return item


def _request(cas_root: Path, key: str, *, manifest: bool) -> None:
    entry = ({"id": "pbcampaign.data-manifest",
              "sha256": "0" * 64, "bytes": 8}
             if manifest else
             {"id": "pbrun.checkout-snapshot",
              "sha256": "1" * 64, "bytes": 8})
    (cas_root / "requests" / key[:2]).mkdir(parents=True, exist_ok=True)
    (cas_root / "requests" / key[:2] / f"{key}.json").write_text(
        json.dumps({"action_key": key, "inputs": [entry], "params": {}}))


def _map_for(queue: pool.PoolQueue, key: str) -> Path:
    path = queue.residency_map_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"schema": "prismaquant.prismabuild."
                                   "residency_map.v1"}))
    return path


def test_a_declaring_row_with_a_composed_map_is_injected(tmp_path):
    """The row never sealed residency; its declaration plus a map inject."""

    queue = _queue_at(tmp_path)
    key = "a" * 64
    cas_root = tmp_path / "cas"
    _request(cas_root, key, manifest=True)
    _map_for(queue, key)

    env = queue.residency_map_environment(_item(key, cas_root))

    assert env == {pb.RESIDENCY_MAP_ENV: str(queue.residency_map_path(key))}


def test_a_declaring_row_without_a_map_launches_byte_identically(tmp_path):
    """No map, no variable: the declaration alone is not an injection."""

    queue = _queue_at(tmp_path)
    key = "b" * 64
    cas_root = tmp_path / "cas"
    _request(cas_root, key, manifest=True)

    assert queue.residency_map_environment(_item(key, cas_root)) == {}


def test_a_map_without_a_declaration_is_not_injected(tmp_path):
    """Somebody else's map on a non-declaring row stays unset."""

    queue = _queue_at(tmp_path)
    key = "c" * 64
    cas_root = tmp_path / "cas"
    _request(cas_root, key, manifest=False)
    _map_for(queue, key)

    assert queue.residency_map_environment(_item(key, cas_root)) == {}


def test_an_unreadable_request_fails_closed(tmp_path):
    """A request that cannot be read answers nothing, never a guess."""

    queue = _queue_at(tmp_path)
    key = "d" * 64
    _map_for(queue, key)
    cas_root = tmp_path / "cas"          # no request file at all

    assert queue.residency_map_environment(
        _item(key, cas_root)) == {}


def test_a_sealed_residency_row_keeps_its_own_path(tmp_path):
    """#583's branch is unchanged: leads plus a present map inject."""

    queue = _queue_at(tmp_path)
    key = "e" * 64
    cas_root = tmp_path / "cas"
    _request(cas_root, key, manifest=False)   # would NOT inject alone
    _map_for(queue, key)

    env = queue.residency_map_environment(
        _item(key, cas_root,
              residency={"leads": ["f" * 64], "tier_id": "x"}))

    assert env == {pb.RESIDENCY_MAP_ENV: str(queue.residency_map_path(key))}
