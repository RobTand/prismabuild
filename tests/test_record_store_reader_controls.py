"""Real cancellation census and MCP current-slot controls; PB execution only."""
import errno
import os
from pathlib import Path
import pytest
from prismabuild import pool
import pbmcp_fixture as fx
from test_pbmcp_tools import fleet, session

KEY = 'a' * 63 + '1'
OTHER = 'a' * 63 + '2'
PREFIX = 'a' * 12


@pytest.mark.parametrize('error', [errno.EACCES, errno.EIO, errno.ESTALE])
@pytest.mark.parametrize('location', ['state', 'decision-parent', 'decision-leaf'])
@pytest.mark.parametrize('visible', [False, True])
def test_cancel_never_certifies_a_partial_prefix_census(
    tmp_path,
    monkeypatch,
    error,
    location,
    visible,
):
    queue = pool.PoolQueue(tmp_path / 'queue')
    for key in [KEY, OTHER]:
        queue.publish(action_key=key, cas_root=tmp_path / 'cas',
                      checkout_root=tmp_path / 'checkout',
                      worker_script=tmp_path / 'worker.py', resources={'cpu': 1})
    if not visible:
        queue.records.item_path(pool.record_store.READY, KEY).unlink()
    if location == 'state':
        queue.records.item_path(pool.record_store.READY, OTHER).rename(
            queue.records.item_path(pool.record_store.CLAIMED, OTHER))
        target = queue.records.dir(pool.record_store.CLAIMED)
    else:
        row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, OTHER))
        queue.withdraw(OTHER, signal_child=False)
        queue.records.item_path(pool.record_store.WITHDRAWN, OTHER).unlink()
        decision = queue.records.withdrawal_decision_path(row)
        assert decision is not None and decision.is_file()
        target = decision.parent if location == 'decision-leaf' else decision.parent.parent
    native = os.scandir
    seen = []
    def scandir(path):
        if isinstance(path, (str, bytes, Path)) and Path(os.fsdecode(path)) == target:
            seen.append(str(path))
            raise OSError(error, 'actual incomplete cancellation namespace', str(path))
        return native(path)
    monkeypatch.setattr(os, 'scandir', scandir)
    refusal = None
    selected = None
    try:
        selected = queue.find_key(PREFIX)
    except (OSError, pool.PoolContractError) as exc:
        refusal = exc
    assert seen, 'the actual directory enumeration was not exercised'
    assert selected is None, 'a partial census certified the visible key as unique'
    assert refusal is not None
    assert (isinstance(refusal, OSError) and refusal.errno == error) or (
        getattr(refusal.__cause__, 'errno', None) == error), (
        'an unavailable namespace was reported as unique, ambiguous or absent', refusal)


@pytest.mark.parametrize('state', [pool.record_store.READY, pool.record_store.CLAIMED])
@pytest.mark.parametrize('raw', [b'{torn', b'[]'], ids=['invalid-json', 'non-object'])
def test_pb_action_reports_unknown_for_an_invalid_current_slot(session, fleet, state, raw):
    path = fleet.queue.records.item_path(state, fx.DONE_KEY)
    path.write_bytes(raw)
    body = session.call('pb_action', {'key_prefix': fx.DONE_KEY})
    assert body['complete'] is False
    assert body['found'] is None
    assert body['state'] is None
    assert 'outcome' not in body, 'the older successful ending answered past current uncertainty'
    assert body['unavailable'], 'the failure must be a normal bounded read section'
