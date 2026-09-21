"""Independent review of damaged retained evidence. All fixtures are private."""
import builtins
import json

from test_pbmcp_withdrawn_attempt_history import KEY, withdrawn
import pbmcp
from prismabuild import pool


def _rewrite(path, value):
    path.chmod(0o600)
    path.write_text(json.dumps(value))
    path.chmod(0o444)


def _retained(queue):
    row_path = queue.item_path(pool.WITHDRAWN, KEY)
    row = json.loads(row_path.read_text())
    attempt_path = queue.root / row['attempt_history_before_withdrawal'][0]['outcome']
    return row_path, row, attempt_path, json.loads(attempt_path.read_text())


def test_an_untrusted_attempt_count_cannot_expand_work_by_its_value(withdrawn, monkeypatch):
    queue, session = withdrawn
    row_path, row, _path, _attempt = _retained(queue)
    row['attempts'] = 10**12
    _rewrite(row_path, row)

    def bounded_range(*args):
        # Fail before allocating anything, even on a broken candidate.
        assert all(abs(value) < 10000 for value in args), 'untrusted attempt count expands range'
        return builtins.range(*args)

    monkeypatch.setattr(pbmcp, 'range', bounded_range, raising=False)
    body = session.call('pb_action', {'key_prefix': KEY})
    assert body['complete'] is True, body.get('unavailable')
    assert body['attempts_history']['problems']


def test_an_attempt_for_another_action_is_not_served_as_retained_history(withdrawn):
    queue, session = withdrawn
    _row_path, _row, path, attempt = _retained(queue)
    attempt['action_key'] = '1' * 64
    _rewrite(path, attempt)
    body = session.call('pb_action', {'key_prefix': KEY})
    assert body['state'] == 'withdrawn'
    assert body['attempts_detail'][0].get('unreadable'), body['attempts_detail']
    assert not body.get('log_tail', {}).get('present')


def test_a_corrupt_retained_log_link_cannot_read_an_unrelated_file(withdrawn, tmp_path):
    queue, session = withdrawn
    _row_path, _row, path, attempt = _retained(queue)
    unrelated = tmp_path / 'unrelated-fixture.txt'
    unrelated.write_text('unrelated benign fixture bytes\n')
    attempt['logs']['stdout']['path'] = str(unrelated)
    _rewrite(path, attempt)
    body = session.call('pb_log', {'key_prefix': KEY})
    assert not (body.get('log') or {}).get('present'), body


def test_missing_retained_links_do_not_claim_no_execution_was_published(withdrawn):
    queue, session = withdrawn
    path, row, _attempt_path, _attempt = _retained(queue)
    del row['attempt_history_before_withdrawal']
    _rewrite(path, row)
    body = session.call('pb_log', {'key_prefix': KEY})
    assert body['state'] == 'withdrawn'
    assert body['attempts_history']['problems']
    assert 'no published attempt yet' not in body.get('reason', '')
