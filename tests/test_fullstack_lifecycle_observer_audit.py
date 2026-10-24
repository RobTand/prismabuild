"""Audit #2100 callback provenance against the live queue, not event copies."""
from __future__ import annotations

from prismabuild import client, reader_lease, record_store, residency_plan, tier_cycle
from fullstack_lifecycle_harness import run_case
from test_fullstack_connected_lifecycle_acceptance import (
    _pb2100_finished, _pb2100_live, _pb2100_protect, _pb2100_sources,
)


def test_rejection_callbacks_inspect_their_own_published_sample(tmp_path):
    expected = {
        'repeated': (1, 'replayed'),
        'regressing': (2, 'replayed'),
        'stale_attempt': (3, 'foreign token'),
        'invalid': (4, 'units_completed is not a finite count'),
    }
    seen = []
    accepted = None

    def observe(name, context):
        nonlocal accepted
        queue = context['queue']
        lease = record_store.read_document(queue.records.lease_path(context['consumer']))
        observation = lease['progress_observation']
        phase = 'p1' if name == 'before_acceptance' else 'p2'
        consumed = next(c for c in tier_cycle.live_consumers(queue)
                        if c['action_key'] == context['consumer'])
        assert consumed['accepted_phase'] == phase
        remaining = residency_plan.remaining(
            residency_plan.read(queue, context['consumer']), consumed['accepted_phase'])
        assert remaining[0]['start_bytes'] == (0 if phase == 'p1' else 4096)
        _pb2100_live(context)
        _pb2100_protect(context)
        if name == 'accepted':
            accepted = observation['last_accepted']
        if name in expected:
            seen.append(name)
            count, reason = expected[name]
            assert observation['rejected_count'] == count, (name, observation)
            assert observation['last_rejection'] == reason, (name, observation)
            assert observation['accepted_count'] == 2
            assert observation['last_accepted'] == accepted

    result = run_case('progress', root=tmp_path,
                      sources=_pb2100_sources(tmp_path / 'sources'), observe=observe)
    assert seen == list(expected)
    _pb2100_finished(result, tmp_path)


def test_wrong_scope_probe_names_the_pinned_attempt_without_inventing_authority(tmp_path):
    seen = []

    def observe(name, context):
        seen.append(name)
        certificate = context['certificate']
        verdict = client.containment_certificate_ok(context['queue'], certificate)
        acquired = context['pins'][0]
        pin = reader_lease.validate_pin(acquired['pin'])
        attempt = pin['refs'][acquired['ref_id']]['attempt']
        assert certificate['action_key'] == pin['owner_action_key']
        assert certificate['nonce'] == attempt['nonce']
        if name == 'missing_scope':
            assert set(certificate) == {'action_key', 'nonce'}
            assert verdict == (False, 'certificate names no attempt')
        else:
            assert name == 'wrong_scope'
            assert set(certificate) == {'action_key', 'nonce', 'scope_id'}
            assert certificate['scope_id'] and certificate['scope_id'] != attempt['scope_id']
            # The portable lane has no broker attestation. This is an actual
            # authority read refusal, NOT a qualified attestation scope mismatch.
            assert verdict == (False, 'no-broker-attestation-retain')
            probe = context['scope_probe']
            assert probe['status'] == 'unqualified'
            assert probe['scope_mismatch_exercised'] is False
            assert probe['reason'] == verdict[1]
            assert probe['blocked_operation'] == 'authoritative scope mismatch verification'
        _pb2100_live(context)
        _pb2100_protect(context)

    result = run_case('recovery', root=tmp_path,
                      sources=_pb2100_sources(tmp_path / 'sources'), observe=observe)
    assert seen == ['missing_scope', 'wrong_scope']
    probe = result['recovery']['scope_probes']['wrong_scope']
    assert probe['status'] == 'unqualified'
    assert probe['scope_mismatch_exercised'] is False
    assert result['recovery']['adoptions'] == 0
    assert result['retained']['owners']
    assert result['retained']['ram_holdings']
    _pb2100_finished(result, tmp_path)
