"""A corrupt empty intent must become durable before the first unlink."""
from pathlib import Path
import hashlib
import json
import os
import socket
import sys
import tempfile

import pytest

from prismabuild import client as sdk
from prismabuild import produced_output as po
from test_consumed_origin_retirement import _commit, _queue, _template


@pytest.mark.parametrize("intent", [None, {}, {"reason": "orphan", "consumers": []}],
                         ids=["absent", "corrupt-empty", "filed"])
def test_the_delete_intent_is_durable_before_unlink(tmp_path, monkeypatch, capsys, intent):
    mapped = Path('/proc/self/exe').resolve()
    executable = mapped.read_bytes()
    with capsys.disabled():
        print('c13-f1-runtime: ' + json.dumps({
            'version_info': list(sys.version_info), 'executable': sys.executable,
            'resolved': str(Path(sys.executable).resolve()),
            'mapped_executable': str(mapped), 'bytes': len(executable),
            'sha256': hashlib.sha256(executable).hexdigest(),
            'host': socket.gethostname(), 'cpus': sorted(os.sched_getaffinity(0)),
            'tmpdir': tempfile.gettempdir(),
        }), flush=True)

    template = _template(tmp_path / 'canonical')
    queue = _queue(tmp_path)
    instance, origin, committed = _commit(queue, template, 'empty-intent')
    queue.finish(instance['owner_action_key'], status='failed')
    commitments = sdk.instance_dir(queue.root, instance) / 'commitments.json'
    record = json.loads(commitments.read_bytes())
    batch = record['batches']['b1']
    if intent is None:
        batch.pop('retiring', None)
    else:
        batch['retiring'] = intent
    commitments.write_text(json.dumps(record, sort_keys=True) + '\n')

    class PausedRetirement(BaseException):
        pass

    def pause_before_unlink(*args, **kwargs):
        raise PausedRetirement()

    monkeypatch.setattr(po, '_unlink_if_committed', pause_before_unlink)
    with pytest.raises(PausedRetirement):
        po.origin_retirement_tick(queue)

    assert origin.exists()
    observed = po.batch_record(queue, instance, template, batch_id='b1')
    with pytest.raises(po.ProducedOutputError, match='origin-batch-retiring'):
        po.declare_origin_consumer(queue, committed['ref'],
                                   consumer_action_key='e' * 64)
    assert observed['state'] == po.BATCH_STATE_RETIRING == 'retiring'
    assert observed['commitment']['retiring']
    assert set(observed['commitment']['retiring']) == {'reason', 'consumers'}
