"""Frozen broker records and kernel peer identities for CPU-only GPU fixtures."""
import os
import time
from pathlib import Path

from prismabuild import core as pb


def frozen_gpu(telemetry, *, action_key=None, nonce=None, deadline=None):
    nonce = nonce or os.environ.get(pb.ACTION_NONCE_ENV, 'a' * 32)
    action_key = action_key or 'a' * 64
    scope = 'prismabuild-job' + pb.raw_sha256((action_key + nonce).encode())[:32] + '.slice'
    return {'action_key': action_key, 'nonce': nonce, 'scope_id': scope,
            'deadline_monotonic': time.monotonic() + 10 if deadline is None else deadline,
            'resource_telemetry': {'gpu_uuids': list(telemetry.get('gpu_uuids', [])),
                'gpu_evidence': {**telemetry.get('gpu_evidence', {
                    'sample_failed': False, 'live_sample': False}),
                    'final_succeeded': telemetry.get('gpu_evidence', {}).get('final_succeeded') is True}}}


def gpu_reply(action, telemetry, **fields):
    record = frozen_gpu(telemetry, action_key=action['action_key'],
                        deadline=fields.get('deadline_monotonic'))
    return {'ok': True, 'nonce': record['nonce'], 'scope_id': record['scope_id'], 'gpu_record': record}


def worker_peer(authority, record, monkeypatch, pid=123456789):
    worker = {'pid': pid, 'starttime': '17', 'parent': 1}
    authority.records[record['scope_id']]['worker_process'] = worker
    original = authority._live_process
    monkeypatch.setattr(authority, '_live_process', lambda value: worker if value == pid else original(value))
    source = Path.read_text
    peer = Path(f'/proc/{pid}/cgroup')
    monkeypatch.setattr(Path, 'read_text', lambda path, *a, **kw:
        '0::/prismabuild.slice/' + record['scope_id'] + '/payload\n'
        if path == peer else source(path, *a, **kw))
    return pid
