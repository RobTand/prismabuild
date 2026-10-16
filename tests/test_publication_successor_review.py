"""Independent B16 successor-proof boundary counterexamples; execute through PB."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from prismabuild import core as pb, pool
import pbrun

from test_pool_requeue_preserves_newer_generation import (
    DEMAND, KEY, _end_claim, _publish, clock, queue,
)


def _collision(queue: pool.PoolQueue, clock: list[float]):
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    clock[0] += 1
    _publish(queue)
    ready = queue.records.item_path(pool.record_store.READY, KEY)
    successor = json.loads(ready.read_bytes())
    return original, ready, successor


def _assert_old_authority_retained(queue: pool.PoolQueue, generation: float):
    retained = [*queue.records.dir(pool.record_store.CLAIMED).glob(f"{KEY}.json"),
                *queue.records.dir(pool.record_store.CLAIMED).glob(f"{KEY}.*{pool.record_store.TOMBSTONE_SUFFIX}")]
    assert len(retained) == 1
    assert json.loads(retained[0].read_bytes())["published_unix"] == generation


@pytest.mark.parametrize("damage", [
    "missing", "empty_object", "empty_list", "all_zero", "tier_only", "malformed_tier_key",
])
def test_structurally_unusable_successor_demand_cannot_end_old_publication(
    queue: pool.PoolQueue, clock: list[float], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch, damage: str,
):
    original, ready, successor = _collision(queue, clock)
    if damage == "missing":
        successor.pop("resources")
    else:
        successor["resources"] = {
            "empty_object": {}, "empty_list": [],
            "all_zero": {"cpu": 0, "mem_gb": 0},
            "tier_only": {"stage_gib@prismabuild-stage:review": 1},
            "malformed_tier_key": {"cpu": 1, "stage_gib@@broken": 1},
        }[damage]
    raw = json.dumps(successor).encode()
    ready.write_bytes(raw)

    consumer = pool.PoolQueue(tmp_path / "consumer")
    consumer.records.ensure_layout()
    consumer.records.item_path(pool.record_store.READY, KEY).write_bytes(raw)
    denials = []

    def record_denial(item, reason, *args, **kwargs):
        denials.append(reason)

    with monkeypatch.context() as patch:
        patch.setattr(consumer, "record_denial", record_denial)
        assert consumer.claim(owner="consumer", capacity=DEMAND) is None
    required = ("malformed_tier_demand" if damage == "malformed_tier_key"
                else "admission_demand_required")
    assert required in denials

    try:
        _end_claim(queue, original, "finish_retry", clock)
    except (OSError, pb.PrismaBuildError):
        pass

    assert ready.read_bytes() == raw
    assert queue.publication_ending(KEY, generation=original["published_unix"]) is None
    assert pbrun.outcome_poll(queue, KEY, original["published_unix"]) == (
        None, original["published_unix"])
    _assert_old_authority_retained(queue, original["published_unix"])


@pytest.mark.parametrize("resources", [{"cpu": 1}, {"mem_gb": 1}])
def test_positive_host_demand_does_not_require_both_cpu_and_memory(
    queue: pool.PoolQueue, clock: list[float], resources: dict[str, int],
):
    original, ready, successor = _collision(queue, clock)
    successor["resources"] = resources
    ready.write_text(json.dumps(successor))

    _end_claim(queue, original, "finish_retry", clock)

    ending = queue.publication_ending(KEY, generation=original["published_unix"])
    assert isinstance(ending, pool.PublicationSupersession)
    assert ending.successor_published_unix == successor["published_unix"]
    claimed = queue.claim(owner="successor", capacity=DEMAND)
    assert claimed is not None
    assert claimed["published_unix"] == successor["published_unix"]


@pytest.mark.parametrize("identity", ["original", "successor"])
def test_public_decoder_normalizes_oversized_generation_to_contract_error(
    queue: pool.PoolQueue, clock: list[float], identity: str,
):
    original, ready, successor = _collision(queue, clock)
    raw = ready.read_bytes()
    address = queue.records.publication_ending_path(original)
    record = {"schema": pool.PUBLICATION_SUPERSEDED_SCHEMA_V1,
              "status": "superseded", "action_key": KEY,
              "published_unix": original["published_unix"], "successor": successor}
    if identity == "original":
        record["published_unix"] = 10 ** 400
    else:
        successor["published_unix"] = 10 ** 400

    with pytest.raises(pool.PoolContractError):
        queue.decode_publication_supersession(address, record)

    assert ready.read_bytes() == raw
    _assert_old_authority_retained(queue, original["published_unix"])
    assert not address.exists()
