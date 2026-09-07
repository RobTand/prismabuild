"""A post-read identity-stat outage must not retire a stranded action."""
import errno
import json
import os
import pytest

from test_an_unverifiable_attempt_link_does_not_stop_the_reaper import (
    KEY, _stranded_tombstone,
)
from prismabuild import core as pb, pool


@pytest.mark.parametrize('error_number', [errno.ESTALE, errno.EIO, errno.EACCES])
def test_post_read_stat_outage_retains_tombstone(tmp_path, monkeypatch, error_number):
    queue, tombstone = _stranded_tombstone(tmp_path, with_history=False)
    record = json.loads(tombstone.read_text())
    record['attempt_history'] = queue.archive_attempt(
        record, attempt=1, status='failed', disposition='requeued', detail={},
    )
    record['attempts'] = 1
    pool._write_json_atomic(tombstone, record)
    outcome = queue.attempt_path(record, 1)
    # Prove that the immutable outcome and its history verify without the fault.
    assert len(queue.attempt_outcomes(record)) == 1
    original_stat = os.stat
    calls = 0

    def stat(path, *args, **kwargs):
        nonlocal calls
        if path == outcome.name and kwargs.get('dir_fd') is not None:
            calls += 1
            if calls == 2:  # opening stat succeeded; post-read identity check fails
                raise OSError(error_number, 'temporary identity stat failure')
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(pb.os, 'stat', stat)
    assert queue.sweep_finish_tombstones() == []
    assert calls == 2
    assert tombstone.exists()
    assert not list(queue.superseded_dir().glob(f'{KEY}.*.finish-tombstone.json'))
    # A later successful read restores the same action and verified outcome.
    assert queue.sweep_finish_tombstones() == [KEY]
    restored = json.loads(queue.item_path(pool.CLAIMED, KEY).read_text())
    assert restored == record
    assert len(queue.attempt_outcomes(restored)) == 1
