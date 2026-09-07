"""A lease missing its host cannot turn the sweeping box into its owner."""
import socket

import pytest

from prismabuild import pool


KEY = "c" * 64


def _widowed_queue(tmp_path, host):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    pool._write_json_atomic(queue.lease_path(KEY), {
        "action_key": KEY, "host": host,
        "heartbeat_unix": pool._now() - 10_000,
    })
    return queue


@pytest.mark.parametrize("host", [None, "", 17])
def test_missing_lease_host_uses_the_unique_reservation_owner(tmp_path, host):
    queue = _widowed_queue(tmp_path, host)
    foreign = queue.ledger("other-" + socket.gethostname())
    foreign.ensure_capacity({"cpu": 1})
    assert foreign.acquire(KEY, {"cpu": 1})
    local = queue.ledger()
    local.ensure_capacity({"cpu": 2})
    before_local = local.available()

    assert queue.sweep_widowed_leases(timeout_s=60) == [KEY]
    assert not queue.lease_path(KEY).exists()
    assert foreign.held() == {}, "the sweep lost the lease but stranded its reservation"
    assert foreign.available() == {"cpu": 1}
    assert local.available() == before_local


def test_ambiguous_widowed_lease_keeps_all_ownership_evidence(tmp_path, capsys):
    queue = _widowed_queue(tmp_path, None)
    local = queue.ledger()
    foreign = queue.ledger("other-" + socket.gethostname())
    for ledger in (local, foreign):
        ledger.ensure_capacity({"cpu": 1})
        assert ledger.acquire(KEY, {"cpu": 1})
    original = queue.lease_path(KEY).read_bytes()

    assert queue.sweep_widowed_leases(timeout_s=60) == []
    assert queue.lease_path(KEY).read_bytes() == original
    assert local.held() == foreign.held() == {"cpu": 1}
    assert "ambiguous claim holder" in capsys.readouterr().err

    # Once the conflicting evidence is resolved, recovery can complete.
    assert foreign.release(KEY) == 1
    assert queue.sweep_widowed_leases(timeout_s=60) == [KEY]
    assert local.held() == {}
