"""Idle preference requires GPU evidence that survives restartability I/O (#1929)."""
import pytest

from prismabuild import box_capacity, pool
from test_gpu_evicts_cpu_guests import guest_fleet


@pytest.mark.parametrize('host', ['cpuhost', 'sparky'])
def test_idle_sample_must_remain_fresh_after_restartability_reads(guest_fleet, monkeypatch, host):
    fleet = guest_fleet
    fleet.queue.ledger('cpuhost').release('f' * 64, certificate=fleet.queue.ledger('cpuhost').certify_release('f' * 64))
    offer = pool.record_store.read_document(fleet.queue.root / pool.WORKERS / 'sparky.json')
    stamp = offer['observed_detail']['gpu_sampled_unix']
    clock = [stamp]
    monkeypatch.setattr(pool, '_now', lambda: clock[0])
    original_load = fleet.queue._opposite_resource_load

    def current_cpu_load(offer, *, gpu_job):
        # Isolate GPU expiry while the fitting CPU peer stays freshly observed.
        if offer.get('host') == 'cpuhost':
            offer = {**offer, 'observed_detail': {
                **offer['observed_detail'], 'observed_unix': clock[0]}}
        return original_load(offer, gpu_job=gpu_job)

    monkeypatch.setattr(fleet.queue, '_opposite_resource_load', current_cpu_load)
    original = fleet.queue._guest_restartable

    def delayed_proof(record):
        result = original(record)
        clock[0] = stamp + box_capacity.GPU_SAMPLE_MAX_AGE_S + 1
        return result

    monkeypatch.setattr(fleet.queue, '_guest_restartable', delayed_proof)
    key = fleet.publish('a')
    claimed = fleet.claim(host)
    assert clock[0] - stamp > box_capacity.GPU_SAMPLE_MAX_AGE_S
    assert not fleet.queue._idle_gpu_offer(offer)
    if host == 'cpuhost':
        assert claimed is not None and claimed['action_key'] == key, (
            'CPU host yielded to a GPU sample already stale at the decision')
    else:
        assert claimed is None, 'GPU host bypassed a fitting CPU host using expired idle proof'
