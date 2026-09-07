"""Taking a READY record aside must preserve recovery after crashes and I/O errors."""
import errno
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import pool

KEY = 'a' * 64


def queue_and_record(tmp_path):
    q = pool.PoolQueue(tmp_path / 'queue')
    q.publish(action_key=KEY, cas_root=q.root / 'cas', checkout_root=q.root / 'co',
              worker_script=q.root / 'worker.py', resources={'cpu': 1})
    ready = q.item_path(pool.READY, KEY)
    return q, ready, json.loads(ready.read_text())


def take(q, ready, kind):
    return q._take_orphan_stub(ready) if kind == 'orphan' else q._withdraw_ready(KEY)


@pytest.mark.parametrize('kind', ['orphan', 'withdrawal'])
@pytest.mark.parametrize('failure', ['crash', 'restore_error'])
def test_a_taken_valid_record_is_recovered_after_interruption(tmp_path, monkeypatch, kind, failure):
    q, ready, original = queue_and_record(tmp_path)
    real_rename, real_link = os.rename, os.link

    def rename(src, dst, *args, **kwargs):
        result = real_rename(src, dst, *args, **kwargs)
        if Path(src) == ready:
            raise KeyboardInterrupt('crash immediately after taking READY')
        return result

    def link(src, dst, *args, **kwargs):
        if Path(dst) == ready:
            raise OSError(errno.EIO, 'restore unavailable')
        return real_link(src, dst, *args, **kwargs)

    with monkeypatch.context() as patch:
        if failure == 'crash':
            patch.setattr(pool.os, 'rename', rename)
            with pytest.raises(KeyboardInterrupt):
                take(q, ready, kind)
        else:
            patch.setattr(pool.os, 'link', link)
            try:
                take(q, ready, kind)
            except OSError as exc:
                assert exc.errno == errno.EIO
    assert not ready.exists()
    later = pool._now() + pool.LEASE_TIMEOUT_S * 2
    monkeypatch.setattr(pool, '_now', lambda: later)
    q.reap_stale()
    assert ready.exists(), 'a valid record remains invisible after recovery'
    assert json.loads(ready.read_text()) == original
    assert not q.item_path(pool.FAILED, KEY).exists()


def test_an_orphan_capture_survives_failure_to_publish_its_ending(tmp_path, monkeypatch):
    q, ready, _ = queue_and_record(tmp_path)
    ready.write_text(json.dumps({'action_key': KEY}))
    real_publish = pool.pb._atomic_publish

    def publish(path, *args, **kwargs):
        if Path(path) == q.item_path(pool.FAILED, KEY):
            raise OSError(errno.ENOSPC, 'terminal publication interrupted')
        return real_publish(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(pool.pb, '_atomic_publish', publish)
        with pytest.raises(OSError):
            q.quarantine_orphans()
    later = pool._now() + pool.LEASE_TIMEOUT_S * 2
    monkeypatch.setattr(pool, '_now', lambda: later)
    q.reap_stale()
    ending = q.item_path(pool.FAILED, KEY)
    assert ending.exists(), 'the owned stub lost its observable ending on a crash'
    assert json.loads(ending.read_text())['status'] == 'orphaned_stub'


def stranded_capture(q, ready, monkeypatch):
    real_rename = os.rename

    def crash(src, dst, *args, **kwargs):
        result = real_rename(src, dst, *args, **kwargs)
        if Path(src) == ready:
            raise KeyboardInterrupt('captured but not diagnosed')
        return result

    with monkeypatch.context() as patch:
        patch.setattr(pool.os, 'rename', crash)
        with pytest.raises(KeyboardInterrupt):
            q._withdraw_ready(KEY)
    later = pool._now() + pool.LEASE_TIMEOUT_S * 2
    monkeypatch.setattr(pool, '_now', lambda: later)


def test_recovery_does_not_replace_a_new_publication(tmp_path, monkeypatch):
    q, ready, original = queue_and_record(tmp_path)
    stranded_capture(q, ready, monkeypatch)
    replacement = {**original, 'published_unix': original['published_unix'] + 1}
    pool._write_json_atomic(ready, replacement)
    q.sweep_ready_transitions()
    assert json.loads(ready.read_text()) == replacement
    assert not list((q.root / 'ready-transitions').glob('*.json'))
    assert list(q.superseded_dir().glob('*.ready-source'))


def test_an_old_terminal_does_not_retire_a_later_captured_request(tmp_path, monkeypatch):
    q, ready, original = queue_and_record(tmp_path)
    stranded_capture(q, ready, monkeypatch)
    prior = {**original, 'published_unix': original['published_unix'] - 1,
             'status': 'executed', 'schema': pool.POOL_OUTCOME_SCHEMA_V1}
    pool._write_json_atomic(q.item_path(pool.DONE, KEY), prior)
    q.sweep_ready_transitions()
    assert json.loads(ready.read_text()) == original
    assert json.loads(q.item_path(pool.DONE, KEY).read_text()) == prior


def test_recovery_retains_source_while_terminal_listing_is_unavailable(tmp_path, monkeypatch):
    q, ready, original = queue_and_record(tmp_path)
    stranded_capture(q, ready, monkeypatch)
    original_listdir = os.listdir

    def listdir(path):
        if Path(path) == q.dir(pool.DONE):
            raise OSError(errno.ESTALE, 'unavailable terminal census')
        return original_listdir(path)

    with monkeypatch.context() as patch:
        patch.setattr(pool.os, 'listdir', listdir)
        assert q.sweep_ready_transitions() == []
    assert not ready.exists()
    assert list((q.root / 'ready-transitions').glob('*.json'))
    assert q.sweep_ready_transitions() == [KEY]
    assert json.loads(ready.read_text()) == original


def test_recovery_defers_while_the_original_key_transition_is_active(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    q, ready, original = queue_and_record(tmp_path)
    stranded_capture(q, ready, monkeypatch)
    with q._transition_locked(KEY):
        with ThreadPoolExecutor(max_workers=1) as executor:
            assert executor.submit(q.sweep_ready_transitions).result(timeout=5) == []
    assert not ready.exists()
    assert q.sweep_ready_transitions() == [KEY]
    assert json.loads(ready.read_text()) == original


def test_empty_terminal_keeps_the_captured_record_for_recovery(tmp_path, monkeypatch):
    q, ready, original = queue_and_record(tmp_path)
    stranded_capture(q, ready, monkeypatch)
    ending = q.item_path(pool.DONE, KEY)
    ending.write_bytes(b'')
    assert q.sweep_ready_transitions() == []
    assert not ready.exists()
    ending.unlink()
    assert q.sweep_ready_transitions() == [KEY]
    assert json.loads(ready.read_text()) == original
