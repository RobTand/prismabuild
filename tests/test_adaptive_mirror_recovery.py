"""Lost callbacks and unsafe comparison inputs cannot strand mirror publication."""
import json
import os
from pathlib import Path

import pytest

from prismabuild import adaptive_mirrors, adaptive_snapshot, immutable_files


def test_overflow_rebuilds_watches_after_a_hidden_source_directory_replacement(tmp_path, monkeypatch):
    local = tmp_path / 'local'
    local.mkdir()
    source = local / 'gpu-state.json'
    source.write_text('{"sampled_unix":10}')
    changes = adaptive_mirrors._LocalChanges(local)
    try:
        local.rename(tmp_path / 'old-local')
        local.mkdir()
        source.write_text('{"sampled_unix":11}')
        # Overflow can discard the rename, watch-loss and moved-in notifications.
        os.read(changes.fd, 65536)
        read = os.read
        overflow = [True]

        def lost_notifications(descriptor, count):
            if descriptor == changes.fd and overflow[0]:
                overflow[0] = False
                return adaptive_mirrors._HEADER.pack(-1, adaptive_mirrors._OVERFLOW, 0, 0)
            return read(descriptor, count)

        monkeypatch.setattr(adaptive_mirrors.os, 'read', lost_notifications)
        assert set(adaptive_snapshot.FILES).issubset(changes.consume())
        source.write_text('{"sampled_unix":12}')
        assert 'gpu-state.json' in changes.consume(), 'new source writes have no installed callback'
    finally:
        changes.close()


@pytest.mark.parametrize('kind', ['fifo', 'symlink', 'oversized'])
def test_shared_dedup_never_opens_an_unbounded_or_followed_leaf(tmp_path, monkeypatch, kind):
    local, shared = tmp_path / 'local', tmp_path / 'shared'
    local.mkdir()
    shared.mkdir()
    payload = {'sampled_unix': 10., 'capacity': 2}
    (local / 'gpu-state.json').write_text(json.dumps(payload))
    previous = shared / 'gpu-state.json'
    if kind == 'fifo':
        os.mkfifo(previous)
    elif kind == 'symlink':
        other = tmp_path / 'outside.json'
        other.write_text(json.dumps(payload))
        previous.symlink_to(other)
    else:
        previous.write_text(' ' * 65536 + json.dumps(payload))
    read_text = Path.read_text

    def reject_unbounded_read(path, *args, **kwargs):
        if path == previous:
            pytest.fail('shared dedup used unbounded, following Path.read_text')
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'read_text', reject_unbounded_read)
    if kind == 'oversized':
        adaptive_snapshot.copy_snapshot(local, shared)
        observed = json.loads(immutable_files.read_replaced_record(previous, where='test mirror'))
        assert observed['sampled_unix'] == payload['sampled_unix']
        assert previous.stat().st_size < 65536
    else:
        with pytest.raises(immutable_files.CASTamperError):
            adaptive_snapshot.copy_snapshot(local, shared)
