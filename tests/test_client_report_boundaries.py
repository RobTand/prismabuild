"""Recovery requires a collected lifetime and report failures stay visible."""
import os
from pathlib import Path

from prismabuild import action_client as client, action_wait, client_channel
from pbtest_shard_output import ClientReply, ONE_PASS
from test_pbtest_reports_whether_pytest_ran import _one_shard


def test_a_report_without_a_reader_collector_cannot_authorize_recovery():
    read_fd, write_fd = os.pipe()
    writer = client_channel.ClientEventWriter(write_fd)
    identity = client.PoolClientIdentity('a' * 64, 1.0,
        Path('/configured/queue'), Path('/configured/cas'), None,
        client.ClientArrival.SUBMITTED)
    try:
        report = client.ClientReport(on_event=writer.send)
        report.publish_identity(identity)
        assert report.finish(client.ClientUnavailable(
            identity.action_key, identity.published_unix, action_wait.WaitEnd.UNRETRIED)) == 74
        writer.close()
        decoder = client_channel.ClientEventDecoder(
            queue_root=identity.queue_root, cas_root=identity.cas_root)
        decoder.feed(os.read(read_fd, 2048))
        events = decoder.finish(74)
        assert events.problem is None
        assert events.ending.readers.abandoned_count == 0
        assert events.ending.readers.identities == ()
        assert not events.ending.readers.complete
        assert not events.may_recover(deadline=float('inf'), now=0.0)
    finally:
        writer.close()
        os.close(read_fd)


def test_an_unusable_report_is_visible_once_without_changing_native_results(
        tmp_path, monkeypatch, capsys):
    expected, control = _one_shard(tmp_path, monkeypatch, ONE_PASS, 0,
        run='valid', with_exit=True)
    capsys.readouterr()
    actual, record = _one_shard(tmp_path, monkeypatch, ONE_PASS, 0,
        run='contradictory', with_exit=True,
        client_reply=ClientReply(ONE_PASS, client.ClientCommand(7)))
    output = capsys.readouterr().out
    assert expected == actual == 0
    assert record['returncode'] == control['returncode'] == 0
    assert record['summary'] == control['summary'] == '1 passed in 0.01s'
    assert output.count('client report unusable: client ending contradicts the process exit status') == 1
