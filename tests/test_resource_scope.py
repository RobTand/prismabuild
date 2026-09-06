"""Per-attempt telemetry and selective memory containment."""
from pathlib import Path
import pytest

from prismabuild.resource_scope import ResourceScope, ResourceUnavailable, read_cgroup


def _group(path, cpu=2000000, memory=1234):
    path.mkdir(parents=True)
    (path / 'cpu.stat').write_text(f'usage_usec {cpu}\n')
    (path / 'memory.current').write_text(str(memory))
    (path / 'memory.peak').write_text(str(memory + 1))
    (path / 'memory.events').write_text('oom 0\noom_kill 0\n')
    return path


def test_scope_identity_separates_attempts(tmp_path, monkeypatch):
    calls = []
    def request(self, op, **extra):
        calls.append((self.nonce, op, extra))
        import hashlib
        unit = 'prismabuild-job' + hashlib.sha256((self.action_key + self.nonce).encode()).hexdigest()[:32] + '.slice'
        return {'ok': True, 'scope_id': unit, 'token': 'b' * 64,
                'cgroup_path': '/sys/fs/cgroup/prismabuild.slice/' + unit}
    monkeypatch.setattr(ResourceScope, '_request', request)
    one = ResourceScope('a'*64, '1'*32, 1024**3, tmp_path / 'one.json')
    two = ResourceScope('a'*64, '2'*32, 1024**3, tmp_path / 'two.json')
    one.create()
    two.create()
    assert one.unit != two.unit
    assert calls[0] == ('1'*32, 'create', {'memory_max_bytes': 1024**3, 'recovery_protocol': 1})
    argv = one.wrap_argv(['/bin/true'])
    assert argv[-2:] == ['--', '/bin/true']
    assert argv[argv.index('--token')+1] == 'b'*64


def test_cgroup_reads_whole_tree_counters(tmp_path):
    result = read_cgroup(_group(tmp_path / 'cg'))
    assert result['cpu_seconds'] == 2
    assert result['memory_current_bytes'] == 1234
    assert result['memory_peak_bytes'] == 1235


def test_missing_scope_is_incomplete_not_idle(tmp_path):
    scope = ResourceScope('a'*64, '1'*32, 1024**3, tmp_path / 'one.json')
    scope.cgroup_path = tmp_path / 'missing'
    sample = scope.sample()
    assert not sample['complete']


def test_scope_rejects_invalid_identity(tmp_path):
    with pytest.raises(ValueError):
        ResourceScope('../elsewhere', '1'*32, 1024, tmp_path / 'x')


def test_complete_sample_is_persisted_and_incomplete_retains_last_counters(tmp_path):
    import json
    scope = ResourceScope('a'*64, '1'*32, 1024**3, tmp_path / 'sample.json')
    scope.cgroup_path = _group(tmp_path / 'cg')
    sample = scope.sample()
    assert sample['complete']
    assert json.loads(scope.telemetry_path.read_text()) == sample
    (scope.cgroup_path / 'cpu.stat').unlink()
    failed = scope.sample()
    assert not failed['complete']
    assert failed['cpu_seconds'] == sample['cpu_seconds']
    assert failed['memory_current_bytes'] == sample['memory_current_bytes']


def test_stop_and_release_use_exact_attempt_authentication(tmp_path, monkeypatch):
    import json
    import prismabuild.resource_scope as module
    requests = []
    def request(body, **kwargs):
        requests.append(body)
        return {'ok': True}
    monkeypatch.setattr(module, 'broker_request', request)
    scope = ResourceScope('a'*64, '1'*32, 1024**3, tmp_path / 'sample.json')
    scope.token = 'b'*64
    scope.terminate_owned('aggregate memory growing into host reserve')
    scope.release()
    assert requests == [
        {'op': 'stop', 'action_key': 'a'*64, 'nonce': '1'*32, 'token': 'b'*64,
         'reason': 'aggregate memory growing into host reserve'},
        {'op': 'release', 'action_key': 'a'*64, 'nonce': '1'*32, 'token': 'b'*64},
    ]
    assert 'aggregate memory growing' in json.loads(
        (tmp_path / 'sample.termination.json').read_text())['reason']


@pytest.mark.parametrize('response,expected', [(b'{"ok":false,"error":"no token"}\n', OSError),
    (b'{}\n', OSError), (b'x'*65537, OSError),
    (b'{"ok":false,"maintenance":true,"retryable":true}\n', ResourceUnavailable),
    (b'{"ok":false,"maintenance":true}\n', OSError),
    (b'{"ok":false,"retryable":true}\n', OSError)])
def test_broker_refusal_and_oversized_response_fail_closed(tmp_path, response, expected):
    import socket
    import threading
    from prismabuild.resource_scope import broker_request
    path = tmp_path / 'broker.sock'
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(path))
        server.listen(1)
        def serve():
            conn, _ = server.accept()
            with conn:
                conn.recv(65536)
                try:
                    conn.sendall(response)
                except BrokenPipeError:
                    pass
        worker = threading.Thread(target=serve)
        worker.start()
        try:
            with pytest.raises(expected) as caught:
                broker_request({'op': 'create'}, socket_path=path)
            assert type(caught.value) is expected
        finally:
            worker.join(timeout=5)
            assert not worker.is_alive()


@pytest.mark.parametrize('approved', [False, True])
def test_proxy_sends_only_stdio_fds_and_never_executes_payload(tmp_path, approved):
    import array
    import json
    import os
    import socket
    import struct
    import subprocess
    import sys
    import threading
    path = tmp_path / 'broker.sock'
    marker = tmp_path / 'payload-ran'
    seen = {}
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(path))
        server.listen(1)
        def serve():
            conn, _ = server.accept()
            with conn:
                seen['pid'], _, _ = struct.unpack('3i', conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                message, control, flags, _ = conn.recvmsg(65536, socket.CMSG_SPACE(12))
                assert not flags & socket.MSG_CTRUNC
                seen['request'] = json.loads(message)
                descriptors = array.array('i')
                for level, kind, raw in control:
                    assert level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS
                    descriptors.frombytes(raw)
                seen['fds'] = [os.readlink(f'/proc/self/fd/{fd}') for fd in descriptors]
                try:
                    if approved:
                        os.write(descriptors[1], b'broker payload stdout\n')
                    conn.sendall(json.dumps({'ok': approved, 'returncode': 7}).encode() + b'\n')
                finally:
                    for fd in descriptors:
                        os.close(fd)
        worker = threading.Thread(target=serve)
        worker.start()
        helper = Path(__file__).resolve().parents[1] / 'tools/fleet/resource_exec.py'
        process = subprocess.Popen([sys.executable, str(helper), '--socket', str(path),
            '--action-key', 'a'*64, '--nonce', '1'*32, '--token', 'b'*64, '--',
            sys.executable, '-c', 'import pathlib,sys; pathlib.Path(sys.argv[1]).touch()', str(marker)],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        output, error = process.communicate(timeout=10)
        worker.join(timeout=5)
        assert not worker.is_alive()
    assert seen['pid'] == process.pid
    assert seen['request']['op'] == 'run'
    assert seen['request']['cwd'] == os.getcwd()
    assert seen['request']['affinity'] == sorted(os.sched_getaffinity(0))
    assert seen['request']['umask'] == int(next(line.split()[1] for line in
        Path('/proc/self/status').read_text().splitlines() if line.startswith('Umask:')), 8)
    assert seen['request']['argv'][-1] == str(marker)
    assert len(seen['fds']) == 3
    assert seen['fds'][0] == '/dev/null'
    assert all(target.startswith('pipe:') for target in seen['fds'][1:])
    assert not marker.exists(), 'the unprivileged proxy must never execute payload itself'
    assert output == ('broker payload stdout\n' if approved else '')
    assert process.returncode == (7 if approved else 125), error


@pytest.mark.parametrize('published', [False, True])
def test_wrapper_and_proxy_support_source_and_published_layouts(tmp_path, monkeypatch, published):
    import json
    import shutil
    import socket
    import subprocess
    import sys
    import threading
    import prismabuild.resource_scope as module
    source = Path(__file__).resolve().parents[1]
    root = tmp_path / 'generation'
    module_dir = root / 'src/prismabuild'
    module_dir.mkdir(parents=True)
    shutil.copyfile(source / 'src/prismabuild/resource_scope.py', module_dir / 'resource_scope.py')
    (module_dir / '__init__.py').touch()
    tools = root / ('tools' if published else 'tools/fleet')
    tools.mkdir(parents=True)
    shutil.copyfile(source / 'tools/fleet/resource_exec.py', tools / 'resource_exec.py')
    monkeypatch.setattr(module, '__file__', str(module_dir / 'resource_scope.py'))
    scope = ResourceScope('a'*64, '1'*32, 1024**3, tmp_path / 'sample.json')
    scope.token = 'b'*64
    argv = scope.wrap_argv(['/bin/true'])
    assert argv[1] == str(tools / 'resource_exec.py')
    # --help imports the local resource client before parsing. Running the
    # copied helper proves its import path in the flattened published layout.
    result = subprocess.run([sys.executable, argv[1], '--help'],
                            env={'PATH': '/usr/bin:/bin'}, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_proxy_signal_stops_exact_scope_and_returns_signal_status(tmp_path):
    import array
    import json
    import os
    import signal
    import socket
    import subprocess
    import sys
    import threading
    endpoint = tmp_path / 'broker.sock'
    seen = []
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(endpoint))
        server.listen(2)
        def serve():
            run, _ = server.accept()
            with run:
                raw, control, _, _ = run.recvmsg(65536, socket.CMSG_SPACE(12))
                seen.append(json.loads(raw))
                fds = array.array('i')
                for _, _, data in control:
                    fds.frombytes(data)
                try:
                    os.write(fds[1], b'running\n')
                    stop, _ = server.accept()
                    with stop:
                        seen.append(json.loads(stop.recv(65536)))
                        stop.sendall(b'{"ok":true}\n')
                finally:
                    for fd in fds:
                        os.close(fd)
        worker = threading.Thread(target=serve)
        worker.start()
        helper = Path(__file__).resolve().parents[1] / 'tools/fleet/resource_exec.py'
        process = subprocess.Popen([sys.executable, str(helper), '--socket', str(endpoint),
            '--action-key', 'a'*64, '--nonce', '1'*32, '--token', 'b'*64, '--', '/bin/true'],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        assert process.stdout.readline() == 'running\n'
        process.send_signal(signal.SIGTERM)
        _, error = process.communicate(timeout=10)
        worker.join(timeout=5)
        assert not worker.is_alive()
    assert process.returncode == 143, error
    assert seen[1] == {'op': 'stop', 'action_key': 'a'*64, 'nonce': '1'*32,
                       'token': 'b'*64, 'reason': 'launcher received signal 15'}


def test_create_refuses_broker_scope_for_another_attempt(tmp_path, monkeypatch):
    unit = 'prismabuild-job' + 'e'*32 + '.slice'
    monkeypatch.setattr(ResourceScope, '_request', lambda *a, **kw: {
        'ok': True, 'scope_id': unit, 'token': 'b'*64,
        'cgroup_path': '/sys/fs/cgroup/prismabuild.slice/' + unit})
    scope = ResourceScope('a'*64, '1'*32, 1024**3, tmp_path / 'sample.json')
    with pytest.raises(OSError, match='invalid scope identity'):
        scope.create()


@pytest.mark.parametrize('tagged',[False,True])
def test_legacy_create_schema_refusal_only_defers_tagged_requests(tmp_path,tagged):
    import json
    import socket
    import threading
    from prismabuild.resource_scope import broker_request
    path=tmp_path/'legacy.sock';seen=[]
    with socket.socket(socket.AF_UNIX) as server:
        server.bind(str(path));server.listen(1)
        def respond():
            connection,_=server.accept()
            with connection:
                seen.append(json.loads(connection.recv(65536)))
                connection.sendall(b'{"ok":false,"error":"unknown request field"}\n')
        thread=threading.Thread(target=respond,daemon=True);thread.start()
        request={'op':'create','action_key':'a'*64,'nonce':'b'*32,'memory_max_bytes':1024}
        if tagged:request['recovery_protocol']=1
        with pytest.raises(ResourceUnavailable if tagged else OSError) as caught:
            broker_request(request,socket_path=path)
        if not tagged:assert not isinstance(caught.value,ResourceUnavailable)
        thread.join(timeout=5)
    assert seen==[request]
