"""Client recovery remembers abandoned readers after per-await cleanup."""
from concurrent.futures import ThreadPoolExecutor

import pytest

from prismabuild import action_client, action_wait


def _reader(pid=123):
    return dict(section='fixture observation', pid=pid, starttime_ticks=17,
                since_unix=1.0)


def _reply(monkeypatch, record, *, status):
    def bounded(*_args, abandoned, **_kwargs):
        abandoned.append(record)
        return dict(status=status, value='delivered', elapsed_s=0.01)

    monkeypatch.setattr(action_wait._reader, 'bounded', bounded)


def test_delivered_reader_remains_in_client_history_after_await_cleanup(monkeypatch):
    _reply(monkeypatch, _reader(), status='ok')
    reaped = []
    monkeypatch.setattr(action_wait, 'reap_retained',
                        lambda readers: reaped.extend(readers) or [])
    with action_wait.client_readers() as history:
        state = action_wait.AwaitState(1.0, action_wait.AwaitProfile.SYNCHRONOUS)
        with action_wait.reader_session(state):
            assert action_wait.read_observation('fixture', lambda: None, budget_s=1) == 'delivered'
            assert len(state.lingering) == 1
        assert len(reaped) == 1
        assert history.snapshot() == action_wait.ReaderHistory(
            1, (action_wait.RetainedReader.from_record(_reader()),), True)


@pytest.mark.parametrize('status', ['timed_out', 'failed'])
def test_refused_reader_is_counted_before_another_plain_failure(monkeypatch, status):
    _reply(monkeypatch, _reader(), status=status)
    with action_wait.client_readers() as history:
        with pytest.raises(action_wait.OutcomeReadUnavailable):
            action_wait.read_observation('fixture', lambda: None, budget_s=1)
        monkeypatch.setattr(action_wait._reader, 'bounded',
                            lambda *_a, **_k: dict(status='failed', error='plain failure'))
        with pytest.raises(action_wait.OutcomeReadUnavailable, match='plain failure'):
            action_wait.read_observation('fixture', lambda: None, budget_s=1)
        assert history.snapshot().abandoned_count == 1
        assert history.snapshot().identities[0].pid == 123
        assert history.snapshot().complete


@pytest.mark.parametrize('wait_s,expected_reads', [(0, 2), (1, 3)])
def test_final_reread_and_reaping_cannot_erase_the_first_abandoned_reader(
        monkeypatch, wait_s, expected_reads):
    calls = []

    def bounded(*_args, abandoned, **_kwargs):
        calls.append(1)
        if len(calls) == 1:
            abandoned.append(_reader())
            return dict(status='timed_out', elapsed_s=0.01)
        return dict(status='failed', error='plain failure')

    monkeypatch.setattr(action_wait._reader, 'bounded', bounded)
    monkeypatch.setattr(action_wait, 'wait_out_retained_readers',
                        lambda *_a, **_k: ([], 0.0))
    with action_wait.client_readers() as history:
        assert action_client.await_outcome(object(), 'a' * 64, wait_s=wait_s) == 74
        assert len(calls) == expected_reads
        assert history.snapshot().abandoned_count == 1
        assert history.snapshot().identities[0].pid == 123


def test_invalid_identity_still_blocks_client_recovery(monkeypatch):
    record = _reader()
    del record['since_unix']
    _reply(monkeypatch, record, status='timed_out')
    with action_wait.client_readers() as history:
        with pytest.raises(action_wait.RetainedIdentityUnavailable):
            action_wait.read_observation('fixture', lambda: None, budget_s=1)
        assert history.snapshot() == action_wait.ReaderHistory(1, (), False)


def test_identity_limit_keeps_the_full_abandoned_count(monkeypatch):
    with action_wait.client_readers() as history:
        for pid in range(1, 13):
            _reply(monkeypatch, _reader(pid), status='timed_out')
            with pytest.raises(action_wait.OutcomeReaderRetained):
                action_wait.read_observation('fixture', lambda: None, budget_s=1)
        snapshot = history.snapshot()
        assert snapshot.abandoned_count == 12
        assert [reader.pid for reader in snapshot.identities] == list(range(1, 9))
        assert not snapshot.complete


def test_nested_clients_restore_the_outer_history(monkeypatch):
    _reply(monkeypatch, _reader(), status='timed_out')
    with action_wait.client_readers() as outer:
        with action_wait.client_readers() as inner:
            with pytest.raises(action_wait.OutcomeReaderRetained):
                action_wait.read_observation('fixture', lambda: None, budget_s=1)
        assert outer.snapshot().abandoned_count == 0
        assert inner.snapshot().abandoned_count == 1
        with pytest.raises(action_wait.OutcomeReaderRetained):
            action_wait.read_observation('fixture', lambda: None, budget_s=1)
        assert outer.snapshot().abandoned_count == 1
        assert inner.snapshot().abandoned_count == 1


def test_concurrent_clients_do_not_share_reader_history():
    def collect(pid):
        with action_wait.client_readers() as history:
            history.record_abandoned([_reader(pid)])
            return history.snapshot()

    with ThreadPoolExecutor(max_workers=2) as workers:
        snapshots = list(workers.map(collect, [1, 2]))
    assert [snapshot.abandoned_count for snapshot in snapshots] == [1, 1]
    assert [[reader.pid for reader in snapshot.identities] for snapshot in snapshots] == [[1], [2]]
