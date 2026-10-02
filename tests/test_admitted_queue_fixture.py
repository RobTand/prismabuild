"""The opt-in fixture never turns invalid caller declarations into grants."""
import pytest

from admitted_queue_fixture import AdmittedQueueFixture
from prismabuild import pool

KEY = "b" * 64


def fixture(tmp_path):
    real = pool.PoolQueue(tmp_path / "queue")
    return AdmittedQueueFixture(real, capacity={"cpu": 2, "mem_gb": 4},
                                default_demand={"cpu": 1, "mem_gb": 1})


def publish(queue, **options):
    queue.publish(action_key=KEY, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
                  max_attempts=1, **options)


def test_fixed_simulated_inputs_take_real_tokens_and_release_real_owner(tmp_path):
    queue = fixture(tmp_path)
    publish(queue)
    claimed = queue.claim()
    assert claimed is not None and claimed["action_key"] == KEY
    assert queue.ledger().holder_tokens(KEY) == {"cpu": 1, "mem_gb": 1}
    assert queue.queue.item_path(pool.CLAIMED, KEY).exists()
    queue.finish(KEY, status="executed")
    assert not queue.ledger().held_keys()


@pytest.mark.parametrize("resources", [None, {}, {"cpu": 0, "mem_gb": 0}])
def test_explicit_invalid_demand_is_not_the_fixture_default(tmp_path, resources):
    queue = fixture(tmp_path)
    publish(queue, resources=resources)
    assert queue.claim() is None
    row = pool._read_json(queue.item_path(pool.READY, KEY))
    assert isinstance(row, dict)
    assert row["resources"] == (resources or {})
    assert not queue.ledger().held_keys()


def test_explicit_missing_capacity_is_not_the_fixture_default(tmp_path):
    queue = fixture(tmp_path)
    publish(queue)
    assert queue.claim(capacity=None) is None
    assert queue.item_path(pool.READY, KEY).exists()
    assert not queue.ledger().held_keys()


def test_withdrawal_remains_the_real_transition(tmp_path):
    queue = fixture(tmp_path)
    publish(queue)
    queue.withdraw(KEY, reason="fixture cancellation control")
    assert queue.claim() is None
    assert not queue.item_path(pool.READY, KEY).exists()
    assert not queue.ledger().held_keys()


def test_fixture_does_not_mock_private_transition_methods(tmp_path, monkeypatch):
    queue = fixture(tmp_path)
    publish(queue)
    seen = []
    original = queue.queue._write_claim_intent
    def intent(*args, **kwargs):
        seen.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(queue, "_write_claim_intent", intent)
    assert queue.claim() is not None
    assert seen == [True]
