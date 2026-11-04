"""Synthetic capability transport across the real unprivileged proxy."""
import array
from concurrent.futures import ThreadPoolExecutor
import errno
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys

import pytest

import resource_exec
from prismabuild import resource_scope
from short_socket import short_socket_path


def _private_scope(tmp_path, *, socket_path=resource_scope.BROKER_SOCKET):
    scope = resource_scope.ResourceScope(
        'a' * 64, '1' * 32, 1024 ** 3, tmp_path / 'telemetry.json',
        socket_path=socket_path)
    scope.token = 'b' * 64
    scope.select_launch_transport('/worker.py')
    return scope


@pytest.mark.parametrize('frame', [b'b' * 64, b'b' * 63, b'b' * 65, b'z' * 64, b''])
def test_private_frame_is_exact_bounded_and_consumed(frame):
    reader, writer = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
    try:
        if frame:
            os.write(writer, frame)
        if frame == b'b' * 64:
            assert resource_exec.read_capability_fd(reader) == 'b' * 64
        else:
            with pytest.raises((OSError, ValueError)):
                resource_exec.read_capability_fd(reader)
        with pytest.raises(OSError) as closed:
            os.fstat(reader)
        assert closed.value.errno == errno.EBADF
    finally:
        os.close(writer)


def test_private_capability_refuses_a_regular_file_and_closes_it(tmp_path):
    path = tmp_path / 'not-a-private-pipe'
    path.write_bytes(b'b' * 64)
    descriptor = os.open(path, os.O_RDONLY)
    with pytest.raises(ValueError, match='descriptor'):
        resource_exec.read_capability_fd(descriptor)
    with pytest.raises(OSError) as closed:
        os.fstat(descriptor)
    assert closed.value.errno == errno.EBADF


@pytest.mark.parametrize('descriptor', [True, -1, 0, 1, 2, 2 ** 31])
def test_private_capability_rejects_invalid_descriptor_numbers(descriptor):
    with pytest.raises(ValueError, match='descriptor'):
        resource_exec.read_capability_fd(descriptor)


def test_prepared_private_pipe_closes_when_process_creation_fails(tmp_path):
    scope = _private_scope(tmp_path)
    with pytest.raises(OSError, match='synthetic process creation failure'):
        with scope.prepared_launch(['/bin/true'], worker_script='/worker.py') as launch:
            argv, descriptors, evidence = launch
            descriptor, = descriptors
            assert '--token-fd' in argv and '--token' not in argv
            assert 'b' * 64 not in json.dumps({'argv': argv, 'evidence': evidence})
            raise OSError('synthetic process creation failure')
    with pytest.raises(OSError) as closed:
        os.fstat(descriptor)
    assert closed.value.errno == errno.EBADF


@pytest.mark.parametrize('approved', [True, False])
def test_private_proxy_forwards_only_stdio_and_never_exposes_its_capability(tmp_path, approved):
    sentinel = 'b' * 64
    with short_socket_path() as endpoint, socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(endpoint))
        server.listen(1)
        server.settimeout(10)
        scope = _private_scope(tmp_path, socket_path=endpoint)

        def broker_round_trip():
            connection, _ = server.accept()
            with connection:
                message, ancillary, flags, _ = connection.recvmsg(65536, socket.CMSG_SPACE(12))
                assert not flags & socket.MSG_CTRUNC
                request = json.loads(message)
                received = array.array('i')
                for level, kind, raw in ancillary:
                    assert (level, kind) == (socket.SOL_SOCKET, socket.SCM_RIGHTS)
                    received.frombytes(raw)
                try:
                    assert request['token'] == sentinel
                    assert len(received) == 3
                    assert sentinel not in json.dumps(request['argv'])
                    assert all(sentinel not in value for value in request['env'].values())
                    if approved:
                        payload = subprocess.run(
                            request['argv'], env=request['env'], cwd=request['cwd'],
                            stdin=received[0], stdout=received[1], stderr=received[2],
                            close_fds=True, timeout=10)
                        answer = {'ok': True, 'returncode': payload.returncode}
                    else:
                        answer = {'ok': False, 'error': sentinel}
                    connection.sendall(json.dumps(answer).encode() + b'\n')
                finally:
                    for descriptor in received:
                        os.close(descriptor)

        with ThreadPoolExecutor(max_workers=1) as executor:
            response = executor.submit(broker_round_trip)
            with scope.prepared_launch(['/bin/true'], worker_script='/worker.py') as launch:
                argv, descriptors, evidence = launch
                descriptor, = descriptors
                program = (
                    'import json,os,sys\n'
                    'try:\n os.fstat(int(sys.argv[1])); inherited=True\n'
                    'except OSError:\n inherited=False\n'
                    'print(json.dumps({"private_fd_open":inherited,'
                    '"capability_in_env":any("b"*64 in value for value in os.environ.values())}))\n')
                argv[-1:] = [sys.executable, '-c', program, str(descriptor)]
                process = subprocess.Popen(
                    argv, pass_fds=descriptors, stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            with pytest.raises(OSError):
                os.fstat(descriptor)
            output, error = process.communicate(timeout=15)
            response.result(timeout=10)
    assert sentinel not in json.dumps(evidence) + output + error
    assert process.returncode == (0 if approved else 125)
    if approved:
        assert json.loads(output) == {'private_fd_open': False, 'capability_in_env': False}
        assert error == ''
    else:
        assert output == '' and 'resource execution refused' in error


def _sealed_proxy_generation(tmp_path, monkeypatch, proxy_source):
    store = tmp_path / 'runtime-generations'
    generation = store / 'retained-fixture'
    source = Path(__file__).resolve().parents[1]
    files = {}
    members = {
        'tools/prismabuild_worker.py': source / 'tools/prismabuild_worker.py',
        'tools/resource_broker.py': source / 'tools/fleet/resource_broker.py',
        'tools/runtime_paths.py': source / 'src/prismabuild/runtime_paths.py',
    }
    for name in ('__init__', 'resource_scope', 'core', 'progress',
                 'residency_map', 'storage_tiers', 'runtime_paths'):
        members[f'src/prismabuild/{name}.py'] = source / f'src/prismabuild/{name}.py'
    for relative, original in members.items():
        destination = generation / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(original.read_bytes())
    proxy = generation / 'tools/resource_exec.py'
    proxy.write_bytes(proxy_source)
    for path in generation.rglob('*'):
        if path.is_file():
            files[path.relative_to(generation).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    receipt = {'schema': 'prismaquant.prismabuild.runtime_version.v1',
               'generation': generation.name, 'commit': 'ab' * 20, 'files': files}
    (generation / 'RUNTIME_VERSION.json').write_text(json.dumps(receipt))
    for path in sorted(generation.rglob('*'), reverse=True):
        path.chmod(0o555 if path.is_dir() else 0o444)
    generation.chmod(0o555)
    monkeypatch.setattr(resource_scope, 'RETAINED_GENERATION_STORE', store)
    return generation, proxy


@pytest.mark.parametrize('source,expected', [
    (b"import argparse\np = argparse.ArgumentParser()\np.add_argument('--token', required=True)\n", 'legacy_argv'),
    (b'PRIVATE_CAPABILITY_FD_PROTOCOL = 1\n', 'private_fd'),
])
def test_verified_retained_proxy_selects_its_recorded_transport(tmp_path, monkeypatch, source, expected):
    generation, proxy = _sealed_proxy_generation(tmp_path, monkeypatch, source)
    scope = resource_scope.ResourceScope('a' * 64, '1' * 32, 1024, tmp_path / 'telemetry.json')
    scope.select_launch_transport(generation / 'tools/prismabuild_worker.py')
    assert scope.capability_transport == expected
    scope.token = 'b' * 64
    with scope.prepared_launch(['/bin/true'], worker_script=generation / 'tools/prismabuild_worker.py') as launch:
        argv, descriptors, evidence = launch
        assert argv[1] == str(proxy)
        assert evidence['capability_transport'] == expected
        if expected == 'private_fd':
            assert '--token-fd' in argv and '--token' not in argv
            assert 'b' * 64 not in json.dumps(evidence) + json.dumps(argv)
            assert len(descriptors) == 1
        else:
            assert argv[argv.index('--token') + 1] == 'b' * 64
            assert descriptors == ()


@pytest.mark.parametrize('source', [
    b'PRIVATE_CAPABILITY_FD_PROTOCOL = 2\n',
    b'PRIVATE_CAPABILITY_FD_PROTOCOL: int = 1\n',
    b'PRIVATE_CAPABILITY_FD_PROTOCOL = 1\nPRIVATE_CAPABILITY_FD_PROTOCOL = 1\n',
    b'PRIVATE_CAPABILITY_FD_PROTOCOL = 1 + 0\n',
    b"argument = '--token-fd'\n",
    b'PRIVATE_CAPABILITY_FD_PROTOCOL =\n',
])
def test_malformed_retained_protocol_cannot_fall_back_to_legacy(tmp_path, monkeypatch, source):
    generation, _ = _sealed_proxy_generation(tmp_path, monkeypatch, source)
    scope = resource_scope.ResourceScope('a' * 64, '1' * 32, 1024, tmp_path / 'telemetry.json')
    with pytest.raises(OSError, match='capability protocol'):
        scope.select_launch_transport(generation / 'tools/prismabuild_worker.py')
    assert scope.capability_transport is None


@pytest.mark.parametrize('fault', ['changed-proxy', 'foreign-store'])
def test_unverified_retained_proxy_cannot_use_legacy_fallback(tmp_path, monkeypatch, fault):
    generation, proxy = _sealed_proxy_generation(tmp_path, monkeypatch, b"argument = '--token'\n")
    if fault == 'changed-proxy':
        proxy.chmod(0o644)
        proxy.write_bytes(b'# changed after receipt\n')
        proxy.chmod(0o444)
    else:
        other_store = tmp_path / 'other-store'
        other_store.mkdir()
        monkeypatch.setattr(resource_scope, 'RETAINED_GENERATION_STORE', other_store)
    scope = resource_scope.ResourceScope('a' * 64, '1' * 32, 1024, tmp_path / 'telemetry.json')
    with pytest.raises(OSError):
        scope.select_launch_transport(generation / 'tools/prismabuild_worker.py')
    assert scope.capability_transport is None
