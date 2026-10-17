"""The real main emits facts matching its process exit and unchanged stdout."""
import json
import os
from pathlib import Path
import sys

import pytest

from prismabuild import action_client as client, client_channel as channel, core, pool, slurm_lane
from test_verified_action_result import (
    _finish, _no_outer_launch_identity, _publish_result, _standard_action,
)


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = '''
import json, os, sys
from pathlib import Path
sys.path.insert(0, str(Path(os.environ['CLIENT_SOURCE']) / 'tools' / 'fleet'))
import pbrun
base = Path(os.environ['CLIENT_ROOT'])
pbrun.SH = base
cas = pbrun.pb.PrismaBuildCAS(base / 'cas')
action = cas.read_action_request(os.environ['CLIENT_KEY'])
mode = os.environ['CLIENT_MODE']
def prepare(args):
    if mode == 'refused':
        raise SystemExit('controlled refusal')
    if mode == 'offer_timeout':
        raise pbrun.OfferDiscoveryTimedOut('controlled offer timeout')
    if mode == 'interrupted':
        raise SystemExit(143)
    return dict(template=dict(cas=cas, params=dict(placement=dict(required_tags=[]),
        demand=dict(cpu=1, mem_gb=1))), cwd=base / 'checkout', portable_checkout=True,
        offer_queue=lambda: pbrun.pool.PoolQueue(base / 'pb-queue'))
pbrun.prepare_submission = prepare
pbrun.seal_action_from_template = lambda *args, **kwargs: action
pbrun.announce_placement = lambda *args, **kwargs: None
pbrun.d38_gate.judge_publication = lambda *args, **kwargs: None
pbrun.publish_consumer_row = lambda q, *args, **kwargs: (
    q.records.item_path('ready', action['action_key']) if mode != 'attached' else None, None)
fd = sys.argv[sys.argv.index('--client-events-fd') + 1]
sys.argv = ['pbrun.py', '--client-events-fd', fd, '--wait-s', '0', '--transport', 'pool',
    *(['--detach'] if mode == 'cached' else []), '--', '/bin/echo', 'hello']
raise SystemExit(pbrun.main())
'''


def _setup(tmp_path):
    (tmp_path / 'checkout').mkdir()
    action = _standard_action(tmp_path / 'checkout', command=['/bin/echo', 'hello'])
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / 'pb-queue')
    queue.records.ensure_layout()
    return action, cas, queue


def _run(base, action, *, mode):
    environment = dict(os.environ, CLIENT_SOURCE=str(ROOT), CLIENT_ROOT=str(base),
        CLIENT_KEY=action['action_key'], CLIENT_MODE=mode,
        PYTHONPATH=str(ROOT / 'src'))
    running = channel.ClientProcess.launch([sys.executable, '-c', BOOTSTRAP],
        queue_root=base / 'pb-queue', cas_root=base / 'cas', env=environment)
    output, _ = running.process.communicate(timeout=15)
    return running.process.returncode, output, running.wait()


@pytest.mark.parametrize('status,returncode,expected', [
    pytest.param('executed', 0, 0, id='executed-zero'),
    pytest.param('failed', 7, 7, id='launcher-seven'),
    pytest.param('failed', 74, 1, id='reserved-launcher-clamped'),
    pytest.param('failed', -15, 241, id='negative-launcher-exit-byte'),
    pytest.param('timeout', 1, 1, id='execution-timeout'),
    pytest.param('withdrawn', 143, 143, id='withdrawal'),
])
def test_main_execution_facts_match_the_actual_interpreter_exit(
        tmp_path, status, returncode, expected):
    action, _, queue = _setup(tmp_path)
    state = 'done' if status == 'executed' else 'withdrawn' if status == 'withdrawn' else 'failed'
    queue.records.item_path(state, action['action_key']).write_text(json.dumps(dict(
        action_key=action['action_key'], status=status, attempts=1, finished_host='fixture',
        detail=dict(returncode=returncode, stdout='unchanged payload\n', stderr=''))))
    actual, output, events = _run(tmp_path, action, mode='queued')
    assert actual == expected
    assert events.problem is None
    assert events.identity.action_key == action['action_key']
    assert isinstance(events.ending.verdict, client.ClientExecution)
    assert events.ending.verdict.transport_returncode == returncode
    assert events.ending.verdict.execution_timeout is (status == 'timeout')
    prefix = (f"pbrun: {action['action_key'][:12]} had been withdrawn by an operator; "
              "this submission supersedes that decision\n" if status == 'withdrawn' else '')
    assert output.startswith(prefix + 'pbrun: queued ' + action['action_key'])
    assert '\nunchanged payload\n' in output


@pytest.mark.parametrize('mode,kind,returncode', [
    pytest.param('refused', client.ClientRefused, 1, id='prepublication-refusal'),
    pytest.param('offer_timeout', client.ClientOfferTimeout, 1, id='unpublished-offer-timeout'),
])
def test_main_prepublication_exceptions_emit_one_ending(tmp_path, mode, kind, returncode):
    action, _, _ = _setup(tmp_path)
    actual, output, events = _run(tmp_path, action, mode=mode)
    assert actual == returncode
    assert events.problem is None
    assert events.identity is None
    assert isinstance(events.ending.verdict, kind)
    assert output.startswith('controlled ')


def test_an_unmodeled_interruption_cannot_be_reported_as_a_refusal(tmp_path):
    action, _, _ = _setup(tmp_path)
    actual, _, events = _run(tmp_path, action, mode='interrupted')
    assert actual == 143
    assert events.problem is not None
    assert events.ending is None
    assert not events.may_recover(deadline=float('inf'), now=0)


def test_main_cached_identity_replays_only_its_configured_verified_result(tmp_path):
    action, _, _ = _setup(tmp_path)
    cas, receipt = _publish_result(tmp_path, action, b'cached native output\n')
    actual, output, events = _run(tmp_path, action, mode='cached')
    assert actual == 0 and events.problem is None
    assert isinstance(events.identity, client.CachedClientIdentity)
    assert events.identity.result_sha256 == receipt['result']['sha256']
    assert json.loads(output.splitlines()[-1]) == client.reported_submission(
        client.ClientReport(identity=events.identity, ending=events.ending))
    assert client.read_cached_output(events.identity, events.ending) == client.CachedOutput(
        cas.result_path(receipt, action).read_text())


@pytest.mark.parametrize('transport', [
    pytest.param(client.ClientTransport.POOL, id='pool'),
    pytest.param(client.ClientTransport.SLURM, id='slurm'),
])
def test_a_different_cas_result_refuses_replay_without_changing_the_execution(tmp_path, transport):
    action, _, queue = _setup(tmp_path)
    _, receipt = _publish_result(tmp_path, action, b'the actual result\n')
    identity = client.CachedClientIdentity(action['action_key'], queue.root, tmp_path / 'cas',
        transport, 'f' * 64)
    ending = client.ClientEnding(client.ClientSubmitted(identity))
    replay = client.read_cached_output(identity, ending)
    assert isinstance(replay, client.CachedOutputUnavailable)
    assert 'differs from the announced digest' in replay.reason
    assert receipt['result']['sha256'] != identity.result_sha256
    assert ending.exit_code == 0


@pytest.mark.parametrize('announcement', [
    pytest.param('unique', id='one-cache-announcement'),
    pytest.param('duplicate', id='ambiguous-announcement'),
    pytest.param('forged', id='foreign-payload-path'),
])
def test_pool_worker_cache_output_uses_the_adopted_fact_and_verified_cas(tmp_path, announcement):
    action, _, _ = _setup(tmp_path)
    cas, receipt = _publish_result(tmp_path, action, b'the original cached output\n')
    stdout = json.dumps(dict(status='cache_hit', receipt=receipt,
        payload_path=str(cas.result_path(receipt, action)))) + '\n'
    if announcement == 'duplicate':
        stdout += stdout
    elif announcement == 'forged':
        stdout = '{"status":"cache_hit","payload_path":"/unrelated/readable/file"}\n'
    _finish(tmp_path, action, receipt, stdout=stdout, queue_root=tmp_path / 'pb-queue')
    actual, output, events = _run(tmp_path, action, mode='queued')
    assert actual == 0 and events.problem is None
    assert isinstance(events.identity, client.PoolClientIdentity)
    assert stdout in output
    replay = client.read_cached_output(events.identity, events.ending)
    if announcement == 'unique':
        assert replay == client.CachedOutput('the original cached output\n')
    else:
        assert isinstance(replay, client.CachedOutputUnavailable)
        assert isinstance(events.ending.verdict.replay, client.ReplayUnavailable)
    assert events.ending.exit_code == 0


@pytest.mark.parametrize('state,returncode,signal,receipted,expected', [
    pytest.param('FAILED', 9, None, True, 0, id='receipt-wins-over-scheduler-failure'),
    pytest.param('COMPLETED', 0, None, False, 1, id='scheduler-zero-without-receipt'),
    pytest.param('TIMEOUT', 0, 15, False, 1, id='scheduler-timeout'),
    pytest.param('CANCELLED', 0, 15, False, 143, id='scheduler-withdrawal'),
])
def test_slurm_reports_its_selected_client_exit_separately_from_scheduler_facts(
        tmp_path, monkeypatch, state, returncode, signal, receipted, expected):
    import pbrun
    from test_slurm_lane import _paper_action
    action = _paper_action(tmp_path, 'typed-client')
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    request = cas.publish_action_request(action)
    job = slurm_lane.SubmittedJob(action['action_key'], '123', 1, [], tmp_path / 'job.sh',
        tmp_path, tmp_path / 'stdout', tmp_path / 'stderr', tmp_path / 'submission', 100.0)
    outcome = slurm_lane.Outcome('123', state, returncode, signal,
        job.stdout_path, job.stderr_path)

    def run(*_args, **kwargs):
        kwargs['on_submit'](job)
        return slurm_lane.RunResult(action['action_key'], [(job, outcome)],
            {'result': {'sha256': 'b' * 64}} if receipted else None, 100.0)

    monkeypatch.setattr(slurm_lane, 'run', run)
    monkeypatch.setattr(slurm_lane, 'job_was_cache_hit', lambda _job: False)
    report = client.ClientReport()
    assert pbrun.slurm_outcome(action, cas=cas, request_path=request, tags=[], demand={},
        exclusive=False, timeout_s=1, wait_s=0, retry_safe=False, max_attempts=1,
        lane_root=tmp_path / 'lane', queue_root=tmp_path / 'queue', report=report) == expected
    ending = report.ending.verdict
    assert isinstance(ending, client.ClientExecution)
    assert ending.reported_returncode == expected
    assert ending.transport_returncode == returncode
    assert ending.transport_signal == signal
    assert ending.transport_state == state
    assert ending.receipt_published is receipted
    assert ending.action_returncode is None
    assert ending.execution_timeout is (state == 'TIMEOUT')


def test_private_descriptor_numbers_do_not_enter_the_sealed_action(tmp_path, monkeypatch, capsys):
    from test_pbrun_detach import _checkout, _queue, _run_pbrun
    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    identities, requests, descriptors = [], [], []
    held = []
    try:
        for _ in range(2):
            read_fd, write_fd = os.pipe()
            descriptors.append(write_fd)
            try:
                assert _run_pbrun(tmp_path, monkeypatch, work,
                    '--detach', '--client-events-fd', str(write_fd)) == 0
                decoder = channel.ClientEventDecoder(queue_root=queue.root, cas_root=tmp_path / 'cas')
                decoder.feed(os.read(read_fd, 2048))
                events = decoder.finish(0)
                assert events.problem is None
                identities.append(events.identity)
                key = events.identity.action_key
                requests.append((tmp_path / 'cas' / 'requests' / key[:2] / (key + '.json')).read_bytes())
            finally:
                os.close(read_fd)
                try:
                    os.close(write_fd)
                except OSError:
                    pass
            held.extend(os.pipe())
        assert descriptors[0] != descriptors[1]
        assert identities[0].action_key == identities[1].action_key
        assert requests[0] == requests[1]
        assert b'client-events-fd' not in requests[0]
    finally:
        for fd in held:
            os.close(fd)
