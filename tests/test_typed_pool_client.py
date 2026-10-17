"""A real private terminal read produces facts independently of its rendering."""
import json
from pathlib import Path

import pytest

from prismabuild import action_client as client, action_wait, client_channel as channel
from test_pbrun_bounded_pool_wait import KEY, Queue, _ending


@pytest.mark.parametrize('directory,status,returncode,expected,timed_out', [
    pytest.param('done', 'executed', 0, 0, False, id='success'),
    pytest.param('failed', 'failed', 9, 9, False, id='launcher-nine'),
    pytest.param('failed', 'failed', 74, 1, False, id='reserved-clamp'),
    pytest.param('failed', 'timeout', 1, 1, True, id='execution-timeout'),
    pytest.param('withdrawn', 'withdrawn', 143, 143, False, id='withdrawal'),
    pytest.param('done', 'cache_hit', 17, 0, False, id='cached-precedence'),
    pytest.param('failed', 'failed', -15, -15, False, id='negative-launcher'),
    pytest.param('done', 'executed', None, 0, False, id='legacy-no-returncode'),
])
def test_typed_wait_and_renderer_share_one_observation(
        tmp_path, capsys, monkeypatch, directory, status, returncode, expected, timed_out):
    queue = Queue(tmp_path / 'queue')
    queue.records.item_path(directory, KEY).write_text(json.dumps(_ending(status, returncode)))
    outcome = client.await_pool_outcome(queue, KEY, patience=action_wait.ProbeOnce(0))
    assert capsys.readouterr() == ('', '')
    assert isinstance(outcome.ending.verdict, client.ClientExecution)
    assert outcome.ending.exit_code == expected
    assert outcome.ending.verdict.transport_returncode == returncode
    assert outcome.ending.verdict.execution_timeout is timed_out

    def no_read(*_args, **_kwargs):
        pytest.fail('rendering read an already observed terminal again')

    monkeypatch.setattr(client, 'bounded_outcome_observation', no_read)
    monkeypatch.setattr(client, 'bounded_outcome_render', no_read)
    assert client.render_pool_outcome(outcome) is None
    rendered = capsys.readouterr()
    assert rendered.out == 'ok\\n'
    assert 'pbrun:' in rendered.err
    assert client.render_pool_outcome(outcome) is None
    assert capsys.readouterr() == rendered


def test_pool_wait_preserves_the_existing_client_lifetime(tmp_path):
    queue = Queue(tmp_path / 'queue')
    queue.records.item_path('done', KEY).write_text(json.dumps(_ending('executed', 0)))
    reader = action_wait.RetainedReader('earlier attachment', 123, 17, 1.0)
    with action_wait.client_readers() as history:
        history.record_abandoned([reader.to_record()])
        outcome = client.await_pool_outcome(queue, KEY, patience=action_wait.ProbeOnce(0))
        assert outcome.ending.readers == history.snapshot()
        assert outcome.ending.readers.abandoned_count == 1
    with action_wait.client_readers() as next_client:
        assert next_client.snapshot() == action_wait.ReaderHistory()


def test_unreadable_terminal_has_its_own_verdict(monkeypatch):
    unreadable = action_wait.Unreadable(action_wait.UnreadableTerminal(
        Path('/private/ending'), 'malformed ending'))
    monkeypatch.setattr(action_wait, 'await_action', lambda *_args, **_kwargs: unreadable)
    outcome = client.await_pool_outcome(object(), KEY, patience=action_wait.ProbeOnce(0))
    assert isinstance(outcome.ending.verdict, client.ClientUnreadable)
    assert outcome.ending.exit_code == 1
    identity = client.PoolClientIdentity(KEY, 10, Path('/queue'), Path('/cas'), None,
                                        client.ClientArrival.SUBMITTED)
    decoder = channel.ClientEventDecoder(queue_root=identity.queue_root, cas_root=identity.cas_root)
    decoder.feed(channel.encode_client_event(identity))
    decoder.feed(channel.encode_client_event(outcome.ending, identity=identity))
    assert decoder.finish(1).ending == outcome.ending
    assert decoder.problem is None


def test_an_abandoned_reader_cannot_forge_recovery_eligibility():
    identity = client.PoolClientIdentity(KEY, 10, Path('/queue'), Path('/cas'), None,
                                        client.ClientArrival.SUBMITTED)
    reader = action_wait.RetainedReader('earlier read', 123, 17, 1.0)
    ending = client.ClientEnding(client.ClientUnavailable(KEY, 10, action_wait.WaitEnd.FINAL_EMPTY),
        action_wait.ReaderHistory(1, (reader,), True))
    record = json.loads(channel.encode_client_event(ending, identity=identity))
    record['recovery_eligible'] = True
    decoder = channel.ClientEventDecoder(queue_root=identity.queue_root, cas_root=identity.cas_root)
    decoder.feed(channel.encode_client_event(identity))
    decoder.feed((json.dumps(record) + '\n').encode())
    result = decoder.finish(74)
    assert result.problem is not None
    assert not result.may_recover(deadline=12, now=11)
