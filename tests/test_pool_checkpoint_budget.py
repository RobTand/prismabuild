"""Shared checkpoint I/O must not exhaust the payload's execution budget."""
import json
import time
from types import SimpleNamespace

import pytest
from prismabuild import core as pb, pool
from test_pool_action_timeout import _claimed


@pytest.mark.parametrize('stage', ['first_lease', 'heartbeat', 'withdrawal', 'telemetry'])
def test_checkpoint_delay_preserves_remaining_budget(tmp_path, monkeypatch, stage):
    queue, item = _claimed(tmp_path, 5)
    offset = [0.0]
    monkeypatch.setattr(pool, 'time', SimpleNamespace(
        monotonic=lambda: time.monotonic() + offset[0],
        time=time.time, sleep=time.sleep))
    delayed = []

    def delay():
        if not delayed:
            delayed.append(stage)
            offset[0] += 10

    if stage in ('first_lease', 'heartbeat'):
        original = queue.write_lease
        calls = []

        def lease(*args, **kwargs):
            original(*args, **kwargs)
            calls.append(kwargs)
            if len(calls) == (1 if stage == 'first_lease' else 2):
                delay()

        monkeypatch.setattr(queue, 'write_lease', lease)
    elif stage == 'withdrawal':
        original = queue.withdrawal_covers
        calls = []

        def withdrawal(item):
            calls.append(item)
            if len(calls) == 2:  # after a real communicate timeout
                delay()
            return original(item)

        monkeypatch.setattr(queue, 'withdrawal_covers', withdrawal)
    else:
        # The outer PB action supplies real containment. This inner fixture
        # isolates the synchronous sampling call; it creates no broker scope.
        scope = SimpleNamespace(wrap_argv=lambda argv: argv,
                                terminate_owned=lambda reason: None)
        monkeypatch.setattr(queue, '_start_resource_scope', lambda item: scope)

        def sample(scope):
            delay()
            return {}

        monkeypatch.setattr(queue, '_sample_resource_scope', sample)

    outcome = queue.execute(item, containment=(stage == 'telemetry'),
                            heartbeat_s=0.05, timeout_grace_s=0.2)
    assert delayed == [stage]
    assert outcome['status'] == 'executed'
    assert outcome['returncode'] == 0
    cas = pb.PrismaBuildCAS(item['cas_root'])
    key = item['action_key']
    action = json.loads((cas.root / 'requests' / key[:2] / f'{key}.json').read_text())
    receipt = cas.lookup(action)
    assert receipt is not None
    assert cas.result_path(receipt, action).read_text() == 'ok'


def test_repeated_checkpoint_delays_do_not_reset_spent_budget(tmp_path, monkeypatch):
    queue, item = _claimed(tmp_path, 0.3)
    offset = [0.0]
    monkeypatch.setattr(pool, 'time', SimpleNamespace(
        monotonic=lambda: time.monotonic() + offset[0],
        time=time.time, sleep=time.sleep))
    original = queue.write_lease
    calls = []

    def lease(*args, **kwargs):
        original(*args, **kwargs)
        calls.append(kwargs)
        offset[0] += 10

    monkeypatch.setattr(queue, 'write_lease', lease)
    outcome = queue.execute(item, heartbeat_s=0.05, timeout_grace_s=0.2)
    assert len(calls) >= 3
    assert outcome['status'] == 'timeout'
    assert outcome['returncode'] is None
