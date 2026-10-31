"""Owned process and filesystem barriers for profile contract tests."""
from __future__ import annotations

from contextlib import contextmanager
import ctypes
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import sys
import time
import threading


def profile_checkout_boundary(checkout: Path) -> None:
    """Do not adopt a Git marker in a runner's unrelated ancestor directory."""
    if not (checkout / '.git').exists():
        subprocess.run(['git', 'init', '-q', str(checkout)], check=True)


def wait_for_profile_file(path: Path, predicate=lambda text: bool(text), *, process=None):
    """Subscribe before reading, so a rename between the two cannot be lost."""
    libc = ctypes.CDLL(None, use_errno=True)
    descriptor = libc.inotify_init1(os.O_CLOEXEC | os.O_NONBLOCK)
    if descriptor < 0:
        raise OSError(ctypes.get_errno(), 'inotify_init1')
    pidfd = None
    try:
        if libc.inotify_add_watch(descriptor, os.fsencode(path.parent), 0x188) < 0:
            raise OSError(ctypes.get_errno(), 'inotify_add_watch', str(path.parent))
        if process is not None:
            pidfd = os.pidfd_open(process.pid)
        deadline = time.monotonic() + 60
        while True:
            try:
                text = path.read_text()
                if predicate(text):
                    return text
            except FileNotFoundError:
                pass
            remaining = deadline - time.monotonic()
            assert remaining > 0, f'no expected event at {path}'
            ready, _, _ = select.select(
                [descriptor] + ([] if pidfd is None else [pidfd]), [], [], remaining)
            assert ready, f'no expected event at {path}'
            if descriptor in ready:
                os.read(descriptor, 65536)
            elif pidfd in ready:
                raise AssertionError(f'owned process exited before publishing {path}')
    finally:
        os.close(descriptor)
        if pidfd is not None:
            os.close(pidfd)


def deadline_after_profile_ready(monkeypatch, checkout: Path) -> None:
    """Deliver TimeoutExpired after both payload readiness and relay launch."""
    spawn = subprocess.Popen
    launch = __import__('prismabuild.core', fromlist=['_ProfileSession'])
    original = launch._ProfileSession.launch_argv
    sessions = []

    def remember(session, argv):
        sessions.append(session)
        return original(session, argv)

    def start(*args, **kwargs):
        process = spawn(*args, **kwargs)
        if kwargs.get('cwd') == checkout:
            wait = process.wait
            first = True

            def expire(timeout=None):
                nonlocal first
                if first:
                    first = False
                    wait_for_profile_file(checkout / 'ready', process=process)
                    wait_for_profile_file(sessions[-1].exit_status_path,
                                          lambda text: json.loads(text)['phase'] == 'launched',
                                          process=process)
                    raise subprocess.TimeoutExpired('controlled execution deadline', 0)
                return wait(timeout=timeout)

            process.wait = expire
        return process

    monkeypatch.setattr(launch._ProfileSession, 'launch_argv', remember)
    monkeypatch.setattr(subprocess, 'Popen', start)


BLOCKED_PROFILE_WORK = (
    'import signal; from pathlib import Path; '
    'Path("ready").write_text("ready"); signal.pause()'
)


@contextmanager
def native_sampled_work(tmp_path: Path, frame_name: str):
    """Keep a busy workload frame alive through verification and a blocking ptrace sample."""
    directory = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    listener = socket.socket(socket.AF_UNIX)
    listener.settimeout(60)
    address = f'/proc/{os.getpid()}/fd/{directory}/native.sock'
    listener.bind(address)
    listener.listen(1)
    cancelled = threading.Event()
    errors = []

    def release_after_sample():
        try:
            with listener.accept()[0] as peer:
                peer.settimeout(60)
                with peer.makefile('rb') as stream:
                    pid = int(stream.readline())
                status = Path(f'/proc/{pid}/status')
                traced = False
                captures = 0
                deadline = time.monotonic() + 60
                try:
                    while not cancelled.is_set():
                        assert time.monotonic() < deadline, 'no blocking sampler attachment'
                        tracer = next(line for line in status.read_text().splitlines()
                                      if line.startswith('TracerPid:'))
                        attached = int(tracer.split()[1]) != 0
                        if traced and not attached:
                            captures += 1
                            # retry_new discards its verification stack before recording.
                            if captures == 2:
                                return
                        traced = attached
                finally:
                    peer.sendall(b'x')
        except BaseException as error:
            errors.append(error)

    observer = threading.Thread(target=release_after_sample)
    observer.start()
    work = (
        'import os,socket,select\n'
        f'def {frame_name}():\n'
        '    with socket.socket(socket.AF_UNIX) as channel:\n'
        f'        channel.connect({address!r})\n'
        '        channel.sendall(str(os.getpid()).encode()+b"\\n")\n'
        '        while not select.select([channel], [], [], 0)[0]:\n'
        '            pass\n'
        '        assert channel.recv(1) == b"x"\n'
        '    print("sampled")\n'
        f'{frame_name}()\n'
    )
    try:
        yield work
        observer.join(timeout=60)
        assert not observer.is_alive(), 'sampler observer did not finish'
        if errors:
            raise errors[0]
    finally:
        cancelled.set()
        listener.close()
        observer.join(timeout=60)
        os.close(directory)


def early_profile_command(argv, profile_path: Path, document: str):
    """Exit the profiler after a real relay record exists, even for fast actions."""
    code = (
        'import subprocess,sys; from pathlib import Path; '
        f'sys.path.insert(0, {str(Path(__file__).parent)!r}); '
        'from profile_test_support import wait_for_profile_file; '
        'child=subprocess.Popen(sys.argv[4:]); '
        'wait_for_profile_file(Path(sys.argv[3]), process=child); '
        'Path(sys.argv[1]).write_text(sys.argv[2])'
    )
    return [sys.executable, '-c', code, str(profile_path), document,
            str(profile_path.with_name('exit_status')), *argv]
