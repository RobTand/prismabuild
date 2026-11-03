"""Real CLI producers and their consumers do not depend on public display."""
from __future__ import annotations

import contextlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import pytest
import pbcanary
import pbcampaign
import pbrun
import pbwait
import pool_reset
from prismabuild import core, submission
from submission_support import patch_store
from test_pbrun_client_events import BOOTSTRAP, ROOT, _setup
from test_verified_action_result import _publish_result, _finish, _no_outer_launch_identity

PUBLIC = '{"schema":"public.view.v1","complete":false}\n'
REAL_RUN = subprocess.run


def _read(record):
    record.seek(0)
    raw = record.read()
    assert raw.endswith(b'\n'), 'private document was not committed'
    return json.loads(raw)


def _scripts(tmp_path, monkeypatch, mode='queued'):
    action, cas, queue = _setup(tmp_path)
    if mode == 'cached':
        _, receipt = _publish_result(tmp_path, action, b'private result bytes\n')
        _finish(tmp_path, action, receipt, stdout='private result bytes\n' + json.dumps(
            {'status': 'published', 'receipt': receipt}) + '\n', queue_root=queue.root)
    monkeypatch.setenv('CLIENT_SOURCE', str(ROOT))
    monkeypatch.setenv('CLIENT_ROOT', str(tmp_path))
    monkeypatch.setenv('CLIENT_KEY', action['action_key'])
    monkeypatch.setenv('CLIENT_MODE', mode)
    monkeypatch.setenv('PYTHONPATH', str(ROOT / 'src'))
    before = BOOTSTRAP.index("fd = sys.argv[")
    # Reuse the existing scratch queue/CAS fixture and actual pbrun pipeline.
    # Only preparation/sealing and publication placement are fixture seams.
    from queue_space_support import QUEUE_SPACE_BOOTSTRAP
    bootstrap = BOOTSTRAP[:before] + QUEUE_SPACE_BOOTSTRAP + '\nraise SystemExit(pbrun.main())\n'
    run_script = tmp_path / 'submit.py'
    run_script.write_text(bootstrap)
    wait_script = tmp_path / 'wait.py'
    wait_script.write_text(
        'import sys\nfrom pathlib import Path\n'
        f'sys.path.insert(0, {str(ROOT / "tools/fleet")!r})\n'
        'import pbwait\nfrom prismabuild.submission_frontend import store_for\n'
        f'store = store_for(pbwait.__file__, root=Path({str(tmp_path)!r}))\n'
        'pbwait.store_for = lambda *a, **kw: store\n'
        'raise SystemExit(pbwait.main())\n')
    return action, run_script, wait_script


@pytest.mark.parametrize('module', [pbrun, pbwait])
def test_capability_is_one_line_without_store_access(monkeypatch, capsys, module):
    def forbidden(*args, **kwargs):
        raise AssertionError('capability queried the store')
    monkeypatch.setattr(module, 'configured_store' if module is pbrun else 'store_for', forbidden)
    assert module.main(['--machine-record-capability']) == 0
    captured = capsys.readouterr()
    assert captured.err == ''
    assert captured.out == ('{"schema":"prismabuild.machine_record_capability.v1",'
                            '"machine_record_fd":1}\n')


@pytest.mark.parametrize('mode,status', [('queued', 'submitted'), ('attached', 'attached'),
                                        ('cached', 'cache_hit')])
def test_detach_private_document_is_complete_and_stdout_unchanged(tmp_path, monkeypatch, mode, status):
    action, script, _ = _scripts(tmp_path, monkeypatch, mode)
    with tempfile.TemporaryFile() as record:
        done = REAL_RUN([sys.executable, str(script), '--detach', '--transport', 'pool',
                         '--machine-record-fd', str(record.fileno()), '--', '/bin/echo', 'hello'],
                        pass_fds=(record.fileno(),), capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
        private = _read(record)
    assert private == json.loads(done.stdout)
    assert private['action_key'] == action['action_key']
    assert private['schema'] == 'prismabuild.submission.v1'
    assert private['status'] == status
    historical = pool_reset.announced_submission(json.dumps({**private, 'status': 'published'}))
    assert historical['status'] == 'published'
    assert private['request_sha256']
    assert {'phases', 'publication', 'submission', 'done', 'failed', 'withdrawn'} <= private.keys()


@pytest.mark.parametrize('mode,status', [('queued', 'submitted'), ('attached', 'attached'),
                                        ('cached', 'cache_hit')])
def test_pool_reset_reads_real_detach_even_with_public_stdout(tmp_path, monkeypatch, mode, status):
    action, script, _ = _scripts(tmp_path, monkeypatch, mode)
    # Replace rendering inside the producer, after acknowledgement adaptation.
    text = script.read_text().replace('raise SystemExit(pbrun.main())',
        'from dataclasses import replace\n'
        'original_render = pbrun.render_submission\n'
        f'pbrun.render_submission = lambda e: original_render(replace(e, text={PUBLIC.strip()!r}) '
        'if e.channel == "stdout" else e)\nraise SystemExit(pbrun.main())')
    script.write_text(text)
    process, log = pool_reset.start_resubmission(
        [sys.executable, str(script), '--detach', '--transport', 'pool', '--', '/bin/echo', 'hello'],
        queue_root=tmp_path / 'pb-queue', key=action['action_key'])
    answer = pool_reset.submission_outcome(process, log)
    assert answer['state'] == 'submitted', answer
    assert answer['line']['action_key'] == action['action_key']
    assert answer['line']['status'] == status
    assert pool_reset.announced_submission(json.dumps(answer['line'])) == answer['line']
    assert PUBLIC.strip() in log.read_text()


@pytest.mark.parametrize('options', [[], ['--json'], ['--endings']])
def test_wait_private_record_comes_from_real_cas_observation(tmp_path, monkeypatch, options):
    action, _, script = _scripts(tmp_path, monkeypatch, 'cached')
    with tempfile.TemporaryFile() as record:
        done = REAL_RUN([sys.executable, str(script), *options, '--wait-s', '0',
                         '--machine-record-fd', str(record.fileno()), action['action_key']],
                        pass_fds=(record.fileno(),), capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
        value = _read(record)
    row = value['endings'][0] if '--endings' in options else value[0]
    assert row['action_key'] == action['action_key']
    assert row['status'] == 'executed'
    if options:
        assert value == json.loads(done.stdout)
    else:
        assert 'key' in done.stdout and not done.stdout.lstrip().startswith('[')
    if '--endings' in options:
        assert value['schema'] == 'prismabuild.endings.v1' and value['complete']
        assert row['succeeded'] and row['exit_code'] == 0
        assert Path(row['result']['stdout']['path']).read_bytes().startswith(b'private result bytes\n')
        assert row['result']['verified'] is True


def _private_cli_run(command, **kwargs):
    done = REAL_RUN(command, **kwargs)
    return subprocess.CompletedProcess(done.args, done.returncode, PUBLIC, done.stderr)


def test_canary_submit_and_wait_read_private_records(tmp_path, monkeypatch):
    action, run_script, wait_script = _scripts(tmp_path, monkeypatch, 'cached')
    monkeypatch.setattr(pbcanary.subprocess, 'run', _private_cli_run)
    key, ack = pbcanary.submit_leg({'pbrun': run_script},
        {'name': 'leg-1', 'argv': ['/bin/echo', 'hello'], 'demand': {}}, tmp_path / 'checkout', 'r', -10, None)
    assert key == action['action_key'] and ack['status'] == 'cache_hit'
    waited = pbcanary.wait_leg({'pbwait': wait_script}, 'leg-1', key, 0)
    assert waited['record']['action_key'] == key
    assert waited['record']['status'] == 'executed'
    assert waited['returncode'] == 0
    assert waited['stdout'] == PUBLIC


def test_verify_reads_private_ack_and_endings(tmp_path, monkeypatch):
    action, run_script, wait_script = _scripts(tmp_path, monkeypatch, 'cached')
    spec = importlib.util.spec_from_file_location('private_verify', ROOT / 'tools/verify.py')
    verify = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verify)
    monkeypatch.setattr(verify, 'PBRUN', run_script)
    monkeypatch.setattr(verify, 'PBWAIT', wait_script)
    monkeypatch.setattr(verify, 'root_command', lambda root:
        [sys.executable, str(run_script), '--detach', '--transport', 'pool', '--', '/bin/echo', 'hello'])
    monkeypatch.setattr(verify, 'populations', lambda root: {'root': ['tests/example.py']})
    monkeypatch.setattr(verify.subprocess, 'run', _private_cli_run)
    row = verify.run_root_lane(tmp_path)
    assert row['action_key'] == action['action_key']
    assert row['output'].startswith('private result bytes\n')
    assert row['returncode'] == 0
    assert json.loads((tmp_path / 'root.endings.json').read_text())['complete']


def test_campaign_real_publication_ignores_display(tmp_path, monkeypatch):
    from test_pbrun_detach import _checkout, _queue
    work = _checkout(tmp_path)
    _queue(tmp_path)
    import socket
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    patch_store(monkeypatch, client=pbcampaign, root=tmp_path)
    monkeypatch.setattr(pbcampaign, 'render_submission', lambda e: None)
    reply = pbcampaign.submit_row({'cwd': str(work), 'argv': ['/bin/echo', 'hello']}, transport='pool')
    assert reply['status'] == 'submitted'
    assert core.PrismaBuildCAS(tmp_path / 'cas').read_action_request(reply['action_key'])
    assert Path(reply['submission']).is_file()


def test_pbtest_private_identity_survives_replaced_submission_stdout(tmp_path, monkeypatch):
    from dataclasses import replace
    from test_pbtest_identifies_its_checkout_once import _counted_run, _sealed, pbtest
    real_submit = submission.submit_action
    def submit(*args, display, **kwargs):
        return real_submit(*args, display=lambda e: display(
            replace(e, text=PUBLIC.strip()) if e.channel == 'stdout' else e), **kwargs)
    monkeypatch.setattr(submission, 'submit_action', submit)
    observe = pbtest.client_recovery.observe_command
    monkeypatch.setattr(pbtest.client_recovery, 'observe_command',
                        lambda identity, *, deadline: observe(identity, deadline=0))
    code, _, root = _counted_run(tmp_path, monkeypatch, shards=1)
    rows = json.loads((tmp_path / 'report.json').read_text())
    assert code == 1  # the real submitted shard remains unclaimed
    assert rows[0]['action_key'] == _sealed(root)[0]['action_key']
    assert 'public.view.v1' not in rows[0]['output']


@pytest.mark.parametrize('kind', ['standard', 'pipe', 'public', 'nonempty', 'readonly', 'stdout_alias'])
def test_invalid_descriptor_refuses_before_publication(tmp_path, monkeypatch, kind):
    action, script, _ = _scripts(tmp_path, monkeypatch)
    with contextlib.ExitStack() as stack:
        record = stack.enter_context(tempfile.TemporaryFile())
        stdout = subprocess.PIPE
        passed = ()
        if kind == 'standard':
            fd = 1
        elif kind == 'pipe':
            rd, fd = os.pipe()
            stack.callback(os.close, rd)
            stack.callback(os.close, fd)
            passed = (fd,)
        elif kind == 'readonly':
            path = tmp_path / 'readonly'
            path.touch(mode=0o600)
            fd = os.open(path, os.O_RDONLY)
            stack.callback(os.close, fd)
            passed = (fd,)
        else:
            fd = record.fileno()
            passed = (fd,)
            if kind == 'public':
                os.fchmod(fd, 0o644)
            elif kind == 'nonempty':
                record.write(b'old evidence\n')
                record.flush()
            elif kind == 'stdout_alias':
                stdout = record
        done = REAL_RUN([sys.executable, str(script), '--detach', '--machine-record-fd', str(fd),
                         '--', '/bin/echo', 'hello'], pass_fds=passed,
                        stdout=stdout, stderr=subprocess.PIPE, text=True)
    assert done.returncode == 2
    assert 'machine' in done.stderr
    assert not list((tmp_path / 'pb-queue' / 'ready').glob('*.json'))


def test_pool_reset_never_uses_stdout_as_private_fallback(tmp_path):
    script = tmp_path / 'obsolete.py'
    script.write_text('print(' + repr(json.dumps(dict(
        schema='prismabuild.submission.v1', action_key='a' * 64,
        transport='pool', status='submitted'))) + ')\n')
    process, log = pool_reset.start_resubmission([sys.executable, str(script)],
        queue_root=tmp_path / 'queue', key='a' * 64)
    assert pool_reset.submission_outcome(process, log)['state'] == 'unclear'


from test_consumer_surface import consumer_world, consumer_action


@pytest.mark.parametrize('status,exit_code', [('executed', 0), ('failed', 1), ('waiting', 75)])
def test_endings_keep_real_verdicts_and_order_on_private_channel(
        consumer_world, status, exit_code, capsys):
    key, _ = consumer_action(consumer_world, 'result', status)
    with tempfile.TemporaryFile() as record:
        assert pbwait.main(['--endings', '--wait-s', '0', '--machine-record-fd',
                            str(record.fileno()), key, key]) == exit_code
        value = _read(record)
    assert value == json.loads(capsys.readouterr().out)
    assert [row['action_key'] for row in value['endings']] == [key, key]
    assert [row['status'] for row in value['endings']] == [status, status]
    assert value['complete'] is (status != 'waiting')


def test_complete_records_larger_than_pipe_capacity_and_short_writes(tmp_path, monkeypatch):
    from prismabuild import machine_record
    write = os.write
    def short_write(fd, raw):
        return write(fd, raw[:31])
    monkeypatch.setattr(machine_record.os, 'write', short_write)
    value = {'result': 'private' * 65536}
    with tempfile.TemporaryFile() as record:
        machine_record.Writer(record.fileno()).write(value)
        assert _read(record) == value


def test_write_failure_does_not_publish_partial_or_private_diagnostics(monkeypatch, capsys):
    from prismabuild import machine_record
    write = os.write
    calls = 0
    def fail_commit(fd, raw):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError('controlled failure')
        return write(fd, raw)
    with tempfile.TemporaryFile() as record:
        writer = machine_record.Writer(record.fileno())
        monkeypatch.setattr(machine_record.os, 'write', fail_commit)
        writer.write({'private': 'never print this value'})
        record.seek(0)
        assert record.read() == b''
    diagnostic = capsys.readouterr()
    assert diagnostic.out == ''
    assert 'write failed' in diagnostic.err and 'never print' not in diagnostic.err


@pytest.mark.parametrize('raw', [b'{"status":"submitted"}', b'{}\n{}\n', b'{broken}\n',
    b'{"status":NaN}\n', b'{"status":1,"status":2}\n',
    '{"status":1}'.encode('utf-16') + b'\n'])
def test_partial_and_multiple_private_documents_fail_closed(raw):
    from prismabuild import machine_record
    with machine_record.Capture() as private:
        private.file.write(raw)
        with pytest.raises(machine_record.MachineRecordError):
            private.read()


@pytest.mark.parametrize('response', [
    (2, ''), (0, '{}\n'), (0, '{"schema":"prismabuild.machine_record_capability.v1","machine_record_fd":true}\n'),
])
def test_external_capability_check_has_typed_unsupported_result(monkeypatch, response):
    from prismabuild import machine_record
    code, stdout = response
    monkeypatch.setattr(machine_record.subprocess, 'run', lambda *a, **kw:
        subprocess.CompletedProcess(a[0], code, stdout, ''))
    with pytest.raises(machine_record.UnsupportedRuntime):
        machine_record.require_capability([sys.executable, '/unsupported/pbrun.py'])


@pytest.mark.parametrize('tool', ['pbrun', 'pbwait'])
def test_real_cli_capability_accepts_no_action_or_descriptor(tool):
    done = REAL_RUN([sys.executable, str(ROOT / 'tools/fleet' / f'{tool}.py'),
                     '--machine-record-capability'], capture_output=True, text=True)
    assert done.returncode == 0 and done.stderr == ''
    assert done.stdout == ('{"schema":"prismabuild.machine_record_capability.v1",'
                           '"machine_record_fd":1}\n')


def test_authorized_child_forwards_the_private_descriptor(tmp_path):
    import pbcanary_submission
    script = tmp_path / 'pbrun.py'
    script.write_text('''import os

def main(argv, *, publication_canary_intent, authorize_canary):
    authorize_canary({'fixture': 'sealed'})
    fd = int(argv[argv.index('--machine-record-fd') + 1])
    os.write(fd, b'private handoff probe\\n')
    return 0
''')
    seen = []
    with tempfile.TemporaryFile() as record:
        done = pbcanary_submission.submit(
            [sys.executable, str(script), '--machine-record-fd', str(record.fileno())],
            intent={}, authorize=lambda action, **kw: seen.append(action),
            pass_fds=(record.fileno(),))
        record.seek(0)
        assert record.read() == b'private handoff probe\n'
    assert done.returncode == 0 and seen == [{'fixture': 'sealed'}]


def test_unreadable_ending_preserves_exit_74_and_complete_document(consumer_world, capsys):
    key, _ = consumer_action(consumer_world, 'corrupt', 'failed')
    queue = consumer_world.queue
    terminal = json.loads(queue.records.item_path('failed', key).read_bytes())
    attempt = json.loads(queue.records.attempt_path(terminal, 1).read_bytes())
    log = queue.root / attempt['logs']['stdout']['path']
    log.chmod(0o644)
    log.write_bytes(b'corrupt private fixture log\n')
    log.chmod(0o444)
    with tempfile.TemporaryFile() as record:
        assert pbwait.main(['--endings', '--wait-s', '0', '--machine-record-fd',
                            str(record.fileno()), key]) == 74
        value = _read(record)
    assert value == json.loads(capsys.readouterr().out)
    assert value['complete'] is False
    assert value['endings'][0]['status'] == 'unreadable'
    assert value['endings'][0]['succeeded'] is False


def test_retained_private_evidence_is_0600_even_under_public_umask(tmp_path):
    import stat
    from prismabuild import machine_record
    path = tmp_path / 'private.json'
    path.write_text('old evidence\n')
    path.chmod(0o644)
    previous = os.umask(0)
    try:
        machine_record.save(path, {'action_key': 'a' * 64})
    finally:
        os.umask(previous)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert json.loads(path.read_text()) == {'action_key': 'a' * 64}
