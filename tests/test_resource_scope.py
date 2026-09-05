"""Per-attempt telemetry and selective memory containment."""
from pathlib import Path
import pytest

from prismabuild.resource_scope import ResourceScope, read_cgroup


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
        unit = 'prismabuild-job' + self.nonce + '.slice'
        return {'ok': True, 'scope_id': unit, 'token': 'b' * 64,
                'cgroup_path': '/sys/fs/cgroup/prismabuild.slice/' + unit}
    monkeypatch.setattr(ResourceScope, '_request', request)
    one = ResourceScope('a'*64, '1'*32, 1024**3, tmp_path / 'one.json')
    two = ResourceScope('a'*64, '2'*32, 1024**3, tmp_path / 'two.json')
    one.create()
    two.create()
    assert one.unit != two.unit
    assert calls[0] == ('1'*32, 'create', {'memory_max_bytes': 1024**3})
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


@pytest.mark.parametrize('response', [b'{"ok":false,"error":"no token"}\n',
                                    b'{}\n', b'x'*65537])
def test_broker_refusal_and_oversized_response_fail_closed(tmp_path, response):
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
            with pytest.raises(OSError):
                broker_request({'op': 'create'}, socket_path=path)
        finally:
            worker.join(timeout=5)
            assert not worker.is_alive()


@pytest.mark.parametrize('approved', [False, True])
def test_helper_attaches_own_pid_before_any_payload(tmp_path, approved):
    import json
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
                seen['request'] = json.loads(conn.recv(65536))
                seen['ran_before_attach'] = marker.exists()
                conn.sendall(json.dumps({'ok': approved}).encode() + b'\n')
        worker = threading.Thread(target=serve)
        worker.start()
        helper = Path(__file__).resolve().parents[1] / 'tools/fleet/resource_exec.py'
        process = subprocess.Popen([sys.executable, str(helper), '--socket', str(path),
            '--action-key', 'a'*64, '--nonce', '1'*32, '--token', 'b'*64, '--',
            sys.executable, '-c', 'import pathlib,sys; pathlib.Path(sys.argv[1]).touch()', str(marker)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        _, error = process.communicate(timeout=10)
        worker.join(timeout=5)
    assert seen['pid'] == process.pid
    assert seen['request']['op'] == 'attach'
    assert not seen['ran_before_attach']
    assert marker.exists() is approved
    assert process.returncode == (0 if approved else 125), error
