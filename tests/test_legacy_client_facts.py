"""Tolerated legacy JSON values must not become false typed execution facts."""
import json

import pytest

from prismabuild import action_client as client, action_wait, client_channel as channel
from test_pbrun_bounded_pool_wait import KEY, Queue, _ending


@pytest.mark.parametrize('raw,expected', [
    pytest.param(False, 0, id='legacy-false'),
    pytest.param(True, 1, id='legacy-true'),
    pytest.param('7', 1, id='text-status'),
    pytest.param(1.0, 1, id='floating-status'),
    pytest.param([], 1, id='list-status'),
    pytest.param(None, 1, id='missing-status'),
])
def test_legacy_cli_exit_is_preserved_with_only_valid_numeric_facts(tmp_path, raw, expected):
    queue = Queue(tmp_path / 'queue')
    record = _ending('failed', raw)
    record['detail'].update(action_returncode='9', action_signal=False, receipt_published='true')
    queue.item_path('failed', KEY).write_text(json.dumps(record))
    outcome = client.await_pool_outcome(queue, KEY, patience=action_wait.ProbeOnce(0))
    assert outcome.ending.exit_code == expected
    assert type(outcome.ending.exit_code) is int
    verdict = outcome.ending.verdict
    assert verdict.transport_returncode is None
    assert verdict.action_returncode is None
    assert verdict.action_signal is None
    assert verdict.receipt_published is None
    identity = client.PoolClientIdentity(KEY, None, queue.root, tmp_path / 'cas', None,
        client.ClientArrival.ATTACHED)
    decoder = channel.ClientEventDecoder(queue_root=queue.root, cas_root=tmp_path / 'cas')
    decoder.feed(channel.encode_client_event(identity))
    decoder.feed(channel.encode_client_event(outcome.ending, identity=identity))
    assert decoder.finish(expected).problem is None
