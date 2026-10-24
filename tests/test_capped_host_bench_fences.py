"""Both benchmark entrypoints must carry the real final claim fence."""
import time
from pathlib import Path

import pytest

from prismabuild import host_cap, pool
from test_capped_partial_host import capped

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("entry", ["claim_pass", "r13"])
@pytest.mark.parametrize("fault", [False, True])
def test_benchmark_entrypoints_reach_the_required_fence(capped, tmp_path, monkeypatch, entry, fault):
    worker, _, _, membership = capped
    monkeypatch.setattr(worker.cpu_topology, "inherited_tiers", lambda: {
        "preferred": [4, 5, 6, 7], "fallback": [12, 13, 14, 15]})
    seen, checked = [], []
    original_claim = pool.PoolQueue.claim
    original_commit = pool.PoolQueue._rename_and_commit_claim
    original_publish = pool.PoolQueue.publish

    def claim(queue, **parameters):
        host_cap.require_claim_fence(parameters.get("admission_open"), parameters["capacity"], queue)
        seen.append(queue)
        return original_claim(queue, **parameters, cpu_sampler=lambda: {
            "sampled_unix": time.time(), "busy_cpus": 0., "psi_some": 0.,
            "cpu_count": 8, "interval_s": 1.})

    def commit(queue, tiers, scan):
        checked.append(tiers.host.candidate.key)
        if fault:
            membership.write_text("0::/user.slice/worker\n")
        return original_commit(queue, tiers, scan)

    def publish(queue, **parameters):
        # The listing benchmark ordinarily publishes deliberately unplaceable
        # rows. Make its one row placeable so this test reaches real commit.
        parameters.pop("tags", None)
        return original_publish(queue, **parameters)

    monkeypatch.setattr(pool.PoolQueue, "claim", claim)
    monkeypatch.setattr(pool.PoolQueue, "_rename_and_commit_claim", commit)
    if entry == "claim_pass":
        import bench_claim_pass
        monkeypatch.setattr(pool.PoolQueue, "publish", publish)
        # Stop after the first poll, whose fault deliberately invalidates the
        # next producer's observation too.
        class PollFinished(Exception):
            pass

        def one_poll(queue, **parameters):
            result = claim(queue, **parameters)
            if fault:
                assert result is None
            else:
                assert result is not None
            raise PollFinished

        monkeypatch.setattr(pool.PoolQueue, "claim", one_poll)
        with pytest.raises(PollFinished):
            bench_claim_pass.build_and_poll(ROOT, tmp_path, 1, 0, 0)
    else:
        import bench_tier_cycle_r13
        queue = pool.PoolQueue(tmp_path / "queue")
        template = bench_tier_cycle_r13._write_only_template(tmp_path / "out")
        if fault:
            with pytest.raises(AssertionError):
                bench_tier_cycle_r13._bind_owner(queue, template, "a" * 64)
        else:
            assert bench_tier_cycle_r13._bind_owner(queue, template, "a" * 64)["owner_action_key"] == "a" * 64
    assert len(seen) == len(checked) == 1
    queue = seen[0]
    if fault:
        assert not queue.ledger().held_keys()
        key = checked[0]
        assert queue.records.item_path(pool.record_store.READY, key).exists()
        assert not queue.records.item_path(pool.record_store.INTENT, key).exists()
        assert not queue.records.item_path(pool.record_store.CLAIMED, key).exists()
    else:
        assert queue.ledger().held_keys() == checked
