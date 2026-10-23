"""The lead's complete-attempt GPU judgment, including final observation failures."""
import json
import os

import pytest

from prismabuild import core as pb, pool
from test_gpu_review_r2 import generic_gpu  # noqa: F401


@pytest.mark.parametrize('uuids,failed,covered,final,state,reason', [
    (['GPU-earlier'], False, True, True, 'used', 'attributed_gpu_uuid'),
    (['GPU-final'], False, False, True, 'used', 'attributed_gpu_uuid'),
    (['GPU-earlier'], True, True, False, 'used', 'attributed_gpu_uuid'),
    ([], False, True, True, 'zero', 'successful_payload_sample'),
    ([], True, True, False, 'unobserved', 'final_observation_failed'),
    ([], True, True, True, 'unobserved', 'sampler_error'),
    ([], False, False, True, 'unobserved', 'no_sample_during_lifetime'),
    ([], False, False, False, 'unobserved', 'final_observation_failed'),
])
def test_complete_attempt_core_and_terminal_agree(generic_gpu, monkeypatch,
                                                 uuids, failed, covered, final, state, reason):
    queue, item, action, cas, checkout = generic_gpu
    telemetry = {'gpu_devices_used': len(uuids), 'gpu_uuids': uuids,
                 'gpu_evidence': {'sample_failed': failed, 'live_sample': covered,
                                  'final_succeeded': final}}
    monkeypatch.setattr(pb, '_gpu_broker_request', lambda *a, **k: {
        'resource_telemetry': telemetry, 'nonce': os.environ[pb.ACTION_NONCE_ENV],
        'scope_id': os.environ[pb.ACTION_SCOPE_ENV]})
    if state == 'zero':
        with pytest.raises(pb.LocalActionError, match='zero distinct GPU UUIDs'):
            pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                resource_demand=queue.demand_of(item))
        assert cas.lookup(action) is None
    else:
        result = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout,
                                     resource_demand=queue.demand_of(item))
        observed = result['receipt']['resource_telemetry']
        assert (observed['gpu_use'], observed['gpu_use_reason']) == (state, reason)
    monkeypatch.setattr(queue, 'cleanup_action_containers', lambda *a, **k: {
        'complete': True, 'resource_scope': {'telemetry': telemetry}})
    path = queue.finish(item['action_key'], status='executed', detail={'returncode': 0},
                        claim_snapshot=item)
    terminal = json.loads(path.read_text())
    assert terminal['status'] == ('failed' if state == 'zero' else 'executed')
    assert (terminal['detail']['gpu_use'], terminal['detail']['gpu_use_reason']) == (state, reason)
