"""Independent test roles share an explicit private queue and root."""

import pytest

from admitted_queue_fixture import AdmittedQueueFixture
from fixture_damage import Damage
from fleet_roles import Actions, Clock, Hosts, Stages
from output_producer_fixture import Broker, Outputs
from prismabuild import pool


@pytest.fixture
def queue(tmp_path):
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "pb-queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.records.ensure_layout()
    return queue


@pytest.fixture
def clock(monkeypatch):
    return Clock(monkeypatch)


@pytest.fixture
def hosts(queue, monkeypatch):
    return Hosts(queue, monkeypatch)


@pytest.fixture
def actions(queue, tmp_path):
    return Actions(queue, tmp_path)


@pytest.fixture
def damage(queue):
    return Damage(queue)


@pytest.fixture
def stages(queue, tmp_path, actions, damage):
    return Stages(queue, tmp_path, actions, damage)


@pytest.fixture
def broker(tmp_path, monkeypatch):
    broker = Broker(tmp_path, monkeypatch)
    try:
        yield broker
    finally:
        broker.close()


@pytest.fixture
def outputs(queue, tmp_path, broker):
    return Outputs(queue, tmp_path, broker)
