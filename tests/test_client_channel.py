"""A private, bounded protocol supplies client facts and rejects contradictions."""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import action_client as client, action_wait, client_channel as channel

KEY = 'a' * 64
QUEUE = Path('/configured/queue')
CAS = Path('/configured/cas')
IDENTITY = client.PoolClientIdentity(KEY, 10.0, QUEUE, CAS,
    QUEUE / 'ready' / (KEY + '.json'), client.ClientArrival.SUBMITTED)


def _decoder(**kwargs):
    return channel.ClientEventDecoder(queue_root=QUEUE, cas_root=CAS, **kwargs)


def _unavailable(history=action_wait.ReaderHistory()):
    return client.ClientEnding(client.ClientUnavailable(KEY, 10.0, action_wait.WaitEnd.FINAL_EMPTY), history)


def _ending_bytes(ending):
    return channel.encode_client_event(ending, identity=IDENTITY)


def test_identity_is_delivered_before_the_ending_with_fragmented_input():
    seen = []
    decoder = _decoder(on_identity=seen.append)
    raw = channel.encode_client_event(IDENTITY)
    for byte in raw:
        decoder.feed(bytes([byte]))
    assert seen == [IDENTITY]
    assert decoder.ending is None
    decoder.feed(_ending_bytes(_unavailable()))
    result = decoder.finish(74)
    assert result.problem is None
    assert result.may_recover(deadline=12, now=11)
    assert not result.may_recover(deadline=12, now=12)


def test_wire_cannot_replace_the_parents_configured_store_roots():
    record = json.loads(channel.encode_client_event(IDENTITY))
    assert 'cas_root' not in record and 'queue_root' not in record
    record['cas_root'] = '/unrelated/store'
    decoder = _decoder()
    decoder.feed((json.dumps(record) + '\n').encode())
    assert decoder.finish(74).problem is not None


def test_a_later_reaped_reader_still_blocks_recovery():
    reader = action_wait.RetainedReader('old read', 123, 17, 1.0)
    history = action_wait.ReaderHistory(1, (reader,), True)
    decoder = _decoder()
    decoder.feed(channel.encode_client_event(IDENTITY))
    decoder.feed(_ending_bytes(_unavailable(history)))
    result = decoder.finish(74)
    assert result.problem is None
    assert result.ending.readers == history
    assert not result.may_recover(deadline=12, now=11)


def test_bounded_reader_detail_never_loses_the_full_abandoned_count():
    readers = tuple(action_wait.RetainedReader('long section ' * 90, pid, 17, 1.0)
                    for pid in range(1, 9))
    ending = _unavailable(action_wait.ReaderHistory(8, readers, True))
    raw = _ending_bytes(ending)
    assert len(raw) <= channel.MAX_EVENT_BYTES
    decoder = _decoder()
    decoder.feed(channel.encode_client_event(IDENTITY))
    decoder.feed(raw)
    result = decoder.finish(74)
    assert result.problem is None
    assert result.ending.readers.abandoned_count == 8
    assert not result.ending.readers.complete
    assert not result.may_recover(deadline=12, now=11)


def test_recovery_accepts_one_ending_and_refuses_a_new_identity():
    decoder = _decoder(expected_identity=IDENTITY)
    decoder.feed(_ending_bytes(_unavailable()))
    assert decoder.finish(74).problem is None
    decoder = _decoder(expected_identity=IDENTITY)
    decoder.feed(channel.encode_client_event(IDENTITY))
    decoder.feed(_ending_bytes(_unavailable()))
    assert decoder.finish(74).problem is not None


@pytest.mark.parametrize('damage', [
    pytest.param('truncated', id='truncated'),
    pytest.param('duplicate-identity', id='duplicate-identity'),
    pytest.param('identity-after-ending', id='identity-after-ending'),
    pytest.param('duplicate-ending', id='duplicate-ending'),
    pytest.param('wrong-key', id='wrong-key'),
    pytest.param('older-generation', id='older-generation'),
    pytest.param('wrong-exit', id='wrong-exit'),
    pytest.param('killed-client', id='killed-client'),
    pytest.param('oversized', id='oversized'),
    pytest.param('duplicate-json-key', id='duplicate-json-key'),
    pytest.param('nonfinite-generation', id='nonfinite-generation'),
    pytest.param('extra-field', id='extra-field'),
])
def test_invalid_client_events_cannot_authorize_recovery(damage):
    identity = channel.encode_client_event(IDENTITY)
    ending = _ending_bytes(_unavailable())
    returncode = 74
    if damage == 'truncated':
        raw = identity + ending[:-1]
    elif damage == 'duplicate-identity':
        raw = identity + identity + ending
    elif damage == 'identity-after-ending':
        raw = _ending_bytes(client.ClientEnding(client.ClientOfferTimeout())) + identity
    elif damage == 'duplicate-ending':
        raw = identity + ending + ending
    elif damage == 'wrong-key':
        row = json.loads(ending)
        row['action_key'] = 'b' * 64
        raw = identity + (json.dumps(row) + '\n').encode()
    elif damage == 'older-generation':
        row = json.loads(ending)
        row['published_unix'] = 9
        raw = identity + (json.dumps(row) + '\n').encode()
    elif damage == 'wrong-exit':
        raw, returncode = identity + ending, 0
    elif damage == 'killed-client':
        raw, returncode = identity + ending, -15
    elif damage == 'oversized':
        raw = b'x' * (channel.MAX_CLIENT_EVENTS * channel.MAX_EVENT_BYTES + 1)
    elif damage == 'duplicate-json-key':
        raw = identity.replace(b'"action_key":', b'"action_key":"' + KEY.encode() + b'","action_key":') + ending
    elif damage == 'nonfinite-generation':
        raw = identity.replace(b'10.0', b'NaN') + ending
    else:
        row = json.loads(ending)
        row['extra'] = 'not in the protocol'
        raw = identity + (json.dumps(row) + '\n').encode()
    decoder = _decoder()
    decoder.feed(raw)
    result = decoder.finish(returncode)
    assert result.problem is not None
    assert not result.may_recover(deadline=12, now=11)


@pytest.mark.parametrize('verdict,returncode', [
    pytest.param(client.ClientExecution(KEY, 10, 'executed', 7, 7, None, False, 7), 7, id='action-seven'),
    pytest.param(client.ClientExecution(KEY, 10, 'failed', 74, 74, None, False, 1), 1, id='reserved-clamped'),
    pytest.param(client.ClientExecution(KEY, 10, 'failed', -15, None, 15, False, -15), 241, id='negative-transport'),
    pytest.param(client.ClientExecution(KEY, 10, 'withdrawn', -15, None, 15, False, 143), 143, id='withdrawn'),
    pytest.param(client.ClientExecution(KEY, 10, 'cache_hit', None, None, None, False, 0), 0, id='cache-hit'),
    pytest.param(client.ClientUnavailable(KEY, 10, action_wait.WaitEnd.DEADLINE), 74, id='unavailable'),
    pytest.param(client.ClientPatienceExpired(KEY, 10), 75, id='patience'),
    pytest.param(client.ClientSuperseded(KEY, 10, 11), 76, id='superseded'),
    pytest.param(client.ClientOfferTimeout(), 1, id='offer-timeout'),
    pytest.param(client.ClientRefused(1), 1, id='text-system-exit'),
    pytest.param(client.ClientRefused(2), 2, id='argument-refusal'),
    pytest.param(client.ClientDeferredTimeout('c' * 64), 75, id='still-deferred'),
    pytest.param(client.ClientSubmitted(IDENTITY), 0, id='detached-submission'),
    pytest.param(client.ClientCommand(0), 0, id='non-submission-command'),
])
def test_client_ending_matches_its_existing_process_exit(verdict, returncode):
    ending = client.ClientEnding(verdict)
    decoder = _decoder()
    if isinstance(verdict, (client.ClientExecution, client.ClientUnavailable, client.ClientPatienceExpired,
                            client.ClientSuperseded, client.ClientSubmitted)):
        decoder.feed(channel.encode_client_event(IDENTITY))
    decoder.feed(_ending_bytes(ending))
    result = decoder.finish(returncode)
    assert result.problem is None
    assert result.ending == ending
    assert ending.exit_code & 255 == returncode
    program = ('raise SystemExit("refused")' if isinstance(verdict, client.ClientRefused)
               and verdict.returncode == 1 else 'import sys; raise SystemExit(int(sys.argv[1]))')
    process = subprocess.run([sys.executable, '-c', program, str(ending.exit_code)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.returncode == returncode


def test_pipe_writer_validates_direction_sets_cloexec_and_keeps_records_bounded():
    read_fd, write_fd = os.pipe()
    try:
        with pytest.raises(channel.ClientProtocolError):
            channel.ClientEventWriter(read_fd)
        os.set_inheritable(write_fd, True)
        writer = channel.ClientEventWriter(write_fd)
        assert fcntl.fcntl(write_fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
        assert not os.get_blocking(write_fd)
        writer.send(IDENTITY)
        writer.send(_unavailable())
        writer.close()
        raw = os.read(read_fd, channel.MAX_CLIENT_EVENTS * channel.MAX_EVENT_BYTES + 1)
        assert len(raw) <= 2 * channel.MAX_EVENT_BYTES
        decoder = _decoder()
        decoder.feed(raw)
        assert decoder.finish(74).problem is None
    finally:
        os.close(read_fd)
        try:
            os.close(write_fd)
        except OSError:
            pass


def test_a_closed_parent_pipe_leaves_the_writer_inert():
    read_fd, write_fd = os.pipe()
    writer = channel.ClientEventWriter(write_fd)
    os.close(read_fd)
    writer.send(IDENTITY)
    assert not writer.complete
    assert writer.fd is None
    writer.send(_unavailable())


def test_regular_file_is_refused_as_a_client_event_descriptor(tmp_path):
    path = tmp_path / 'not-a-pipe'
    with path.open('wb') as stream:
        with pytest.raises(channel.ClientProtocolError):
            channel.ClientEventWriter(stream.fileno())
    assert path.read_bytes() == b''
