"""Cached replay facts come from the adopted attempt without changing its verdict."""
import json
from pathlib import Path

import pytest

from prismabuild import action_client as client, action_result, action_wait, pool
from prismabuild import client_channel as channel
from test_client_channel import IDENTITY, KEY, _decoder
from test_verified_action_result import (
    _finish, _no_outer_launch_identity, _publish_result, _standard_action,
)


@pytest.mark.parametrize('kind', [
    pytest.param('cache_hit', id='cache-hit'),
    pytest.param('published', id='fresh-result'),
    pytest.param('missing', id='missing-announcement'),
    pytest.param('truncated', id='truncated-announcement'),
    pytest.param('duplicate', id='duplicate-announcement'),
    pytest.param('wrong-key', id='wrong-action-key'),
    pytest.param('bad-digest', id='malformed-result-digest'),
    pytest.param('fake-output', id='quoted-cache-prose'),
])
def test_real_immutable_attempt_metadata_preserves_its_summary_exit_and_output(
        tmp_path, capsys, kind):
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    action = _standard_action(checkout, command=['/bin/echo', 'cached'])
    cas, receipt = _publish_result(tmp_path, action, b'the cached test output\n')
    key = action['action_key']
    announcement = dict(status='published' if kind in {'published', 'fake-output'} else 'cache_hit',
                        receipt=json.loads(json.dumps(receipt)),
                        payload_path=str(cas.result_path(receipt, action)))
    if kind == 'wrong-key':
        announcement['receipt']['action_key'] = 'b' * 64
    elif kind == 'bad-digest':
        announcement['receipt']['result']['sha256'] = True
    stdout = json.dumps(announcement) + '\n'
    if kind == 'missing':
        stdout = 'ordinary legacy output\n'
    elif kind == 'truncated':
        stdout = stdout[:len(stdout) // 2]
    elif kind == 'duplicate':
        stdout += stdout
    elif kind == 'fake-output':
        stdout = '{"status": "cache_hit", "payload_path": "/unrelated/readable/file"}\n' + stdout
    queue, record = _finish(tmp_path, action, receipt, stdout=stdout)
    path = queue.records.item_path(pool.DONE, key)
    summary = client.outcome_summary(queue, path, record)
    rendered = client.bounded_outcome_render(queue, path, record, budget_s=5)
    assert rendered['summary'] == summary
    assert summary['status'] == 'executed'
    if kind == 'cache_hit':
        assert rendered['cached_replay'] == client.CachedResult(receipt['result']['sha256'])
    else:
        assert isinstance(rendered['cached_replay'], client.ReplayUnavailable)
        assert rendered['cached_replay'].reason is (
            client.ReplayDiagnostic.NOT_A_CACHE_HIT if kind in {'published', 'fake-output'}
            else client.ReplayDiagnostic.INVALID_ANNOUNCEMENT if kind in {'wrong-key', 'bad-digest'}
            else client.ReplayDiagnostic.NO_UNIQUE_ANNOUNCEMENT)
    capsys.readouterr()
    assert client.report_pool_outcome(queue, key, wait_s=0, generation=record['published_unix']) == 0
    out, err = capsys.readouterr()
    assert out == stdout
    assert err.endswith('pbrun: ' + client.outcome_headline(summary) + '\n')


def test_cached_metadata_uses_only_the_adopted_attempt():
    def announcement(digest):
        return json.dumps(dict(status='cache_hit', receipt={
            'receipt_sha256': 'c' * 64, 'action_key': KEY,
            'result': {'sha256': digest}})) + '\n'

    summary = dict(action_key=KEY, adopted={'attempt': 2}, detail={'stdout': announcement('0' * 64)})
    attempts = [{'attempt': 1, 'stdout': announcement('1' * 64)},
                {'attempt': 2, 'stdout': announcement('2' * 64)},
                {'attempt': 3, 'stdout': announcement('3' * 64)}]
    value = client._cached_replay_value(summary, attempts)
    assert client.decode_replay_metadata(value, action_key=KEY) == client.CachedResult('2' * 64)
    assert summary['detail']['stdout'] == announcement('0' * 64)


@pytest.mark.parametrize('damage', [
    pytest.param('digest-type', id='boolean-digest'),
    pytest.param('digest-case', id='noncanonical-digest'),
    pytest.param('wrong-key', id='different-action'),
    pytest.param('diagnostic', id='unknown-diagnostic'),
    pytest.param('both', id='present-and-refused'),
])
def test_corrupt_replay_ipc_cannot_change_an_observed_execution(monkeypatch, damage):
    value = dict(summary={'action_key': KEY, 'status': 'executed', 'detail': {'returncode': 0}},
                 attempts=[], cached_result_sha256='d' * 64,
                 cached_result_action_key=KEY, cached_result_diagnostic=None)
    if damage == 'digest-type':
        value['cached_result_sha256'] = True
    elif damage == 'digest-case':
        value['cached_result_sha256'] = 'D' * 64
    elif damage == 'wrong-key':
        value['cached_result_action_key'] = 'b' * 64
    elif damage == 'diagnostic':
        value.update(cached_result_sha256=None, cached_result_action_key=None,
                     cached_result_diagnostic='not-in-the-protocol')
    else:
        value['cached_result_diagnostic'] = client.ReplayDiagnostic.INVALID_ANNOUNCEMENT.value
    monkeypatch.setattr(action_wait._reader, 'bounded', lambda *_a, **_k: {'status': 'ok', 'value': value})
    rendered = client.bounded_outcome_render(object(), Path('/ending'),
        {'action_key': KEY, 'status': 'executed'}, budget_s=5)
    assert rendered['summary']['status'] == 'executed'
    assert rendered['summary']['detail']['returncode'] == 0
    assert rendered['cached_replay'] == client.ReplayUnavailable(client.ReplayDiagnostic.INVALID_IPC)


@pytest.mark.parametrize('replay', [
    pytest.param(client.CachedResult('d' * 64), id='cached-result'),
    pytest.param(client.ReplayUnavailable(client.ReplayDiagnostic.NO_UNIQUE_ANNOUNCEMENT), id='ambiguous'),
    pytest.param(client.ReplayUnavailable(client.ReplayDiagnostic.NOT_A_CACHE_HIT), id='fresh'),
])
def test_typed_execution_replay_facts_round_trip_without_changing_exit(replay):
    ending = client.ClientEnding(client.ClientExecution(KEY, 10, 'executed', 0, 0, None, False, 0, replay))
    decoder = _decoder()
    decoder.feed(channel.encode_client_event(IDENTITY))
    decoder.feed(channel.encode_client_event(ending, identity=IDENTITY))
    result = decoder.finish(0)
    assert result.problem is None
    assert result.ending == ending
    assert result.ending.exit_code == 0


def test_one_public_announcement_owner_preserves_the_existing_grammar():
    raw = json.dumps({'status': 'cache_hit', 'receipt': {'receipt_sha256': 'a' * 64}}).encode()
    assert action_result.decode_result_announcement(raw)['status'] == 'cache_hit'
    with pytest.raises(action_result.ActionResultError, match='exactly one'):
        action_result.decode_result_announcement(raw + b'\n' + raw)
