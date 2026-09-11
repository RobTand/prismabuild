"""A service stop drains owned workers and cannot be undone by cron ensure."""
from pathlib import Path
import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest

TOOLS = Path(__file__).resolve().parents[1] / 'tools' / 'fleet'
sys.path.insert(0, str(TOOLS))
import supervise


def wait_for(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, 'process lifecycle did not settle'
        time.sleep(.02)


def test_stop_waits_for_current_action_and_reaps_owned_workers(tmp_path):
    generation = tmp_path / 'runtime-generations' / 'one'
    scripts = generation / 'tools'
    scripts.mkdir(parents=True)
    (generation / 'RUNTIME_VERSION.json').write_text('{}')
    (tmp_path / 'repo').symlink_to(generation, target_is_directory=True)
    worker = scripts / 'worker_loop.py'
    worker.write_text(textwrap.dedent('''
        import pathlib, signal, sys, time
        root = pathlib.Path(sys.argv[1])
        stop = False
        def request(*args):
            global stop
            stop = True
            (root / 'stopping').touch()
        signal.signal(signal.SIGTERM, request)
        (root / 'ready').touch()
        while not stop or not (root / 'committed').exists():
            time.sleep(.02)
        (root / 'finished').touch()
    '''))
    harness = tmp_path / 'supervisor.py'
    harness.write_text(textwrap.dedent(f'''
        import os, pathlib, subprocess, sys
        sys.path.insert(0, {str(TOOLS)!r})
        import supervise
        root = pathlib.Path({str(tmp_path)!r})
        supervise.MIRROR = root
        supervise.CLAIM = root / 'claim'
        supervise.LOG_DIR = root
        supervise.declared_shape = lambda *a, **k: (1, [str(root)])
        supervise.declared_roles = lambda *a: []
        supervise._claim_holders = lambda: frozenset()
        supervise._ready_backlog = lambda: False
        supervise._loaded_published_generation = lambda: None
        supervise._reexec_if_published = lambda *a: False
        supervise._systemd_managed = lambda: False
        sys.argv = ['supervise', '--interval-s', '.02', '--loops', '1']
        raise SystemExit(supervise.main())
    '''))
    output = (tmp_path / 'supervisor.log').open('w')
    proc = subprocess.Popen([sys.executable, str(harness)], stdout=output, stderr=output)
    unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
    try:
        wait_for(lambda: (tmp_path / 'ready').exists())
        proc.send_signal(signal.SIGTERM)
        wait_for(lambda: (tmp_path / 'stopping').exists() or proc.poll() is not None)
        assert (tmp_path / 'stopping').exists(), 'supervisor exited without stopping its worker'
        assert proc.poll() is None, 'supervisor must wait for the current action'
        assert unrelated.poll() is None
        (tmp_path / 'committed').touch()
        assert proc.wait(timeout=5) == 0
        assert (tmp_path / 'finished').exists()
        assert unrelated.poll() is None
        assert 'shutdown complete' in (tmp_path / 'supervisor.log').read_text()
    finally:
        (tmp_path / 'committed').touch()
        # Private harness children only; never inspect or signal fleet workers.
        for entry in Path('/proc').iterdir():
            if not entry.name.isdigit():
                continue
            try:
                argv = (entry / 'cmdline').read_bytes().split(b'\0')
                if len(argv) > 1 and argv[1] == str(worker).encode():
                    os.kill(int(entry.name), signal.SIGTERM)
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                pass
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=5)
        unrelated.terminate()
        unrelated.wait(timeout=5)
        output.close()


def test_cron_ensure_defers_to_installed_systemd_owner(monkeypatch, capsys):
    monkeypatch.setattr(supervise, '_systemd_managed', lambda: True, raising=False)
    monkeypatch.setattr(sys, 'argv', ['supervise', '--ensure'])
    monkeypatch.setattr(supervise, 'declared_shape', lambda *a: pytest.fail('cron reached worker setup'))
    assert supervise.main() == 0
    assert 'systemd' in capsys.readouterr().out


def test_only_explicit_installed_unit_owns_startup(tmp_path, monkeypatch):
    unit = tmp_path / 'service'
    monkeypatch.setattr(supervise, 'SYSTEMD_UNIT', unit)
    assert supervise._systemd_managed() is False
    unit.write_text(supervise.SYSTEMD_EXEC.removesuffix(' --systemd') + '\n')
    assert supervise._systemd_managed() is False
    unit.write_text('[Service]\n' + supervise.SYSTEMD_EXEC + '\n')
    assert supervise._systemd_managed() is True


def test_shutdown_rechecks_ownership_after_opening_pidfd(monkeypatch):
    monkeypatch.setattr(supervise, '_proven_roots', lambda: [])
    monkeypatch.setattr(supervise, '_live_loops', lambda: [123])
    monkeypatch.setattr(supervise, '_live_role_loops', lambda _: [])
    monkeypatch.setattr(supervise.os, 'pidfd_open', lambda pid: 42)
    monkeypatch.setattr(supervise, '_is_fleet_loop', lambda *a, **k: False)
    monkeypatch.setattr(supervise.signal, 'pidfd_send_signal', lambda *a: pytest.fail('lost ownership'))
    closed = []
    monkeypatch.setattr(supervise.os, 'close', closed.append)
    monkeypatch.setattr(supervise, '_reap_children', lambda: 0)
    supervise._shutdown_workers()
    assert closed == [42]


def test_installer_declares_cooperative_stop_and_explicit_start_owner(tmp_path):
    # Shell installer runs for real, with isolated HOME and a recording systemctl.
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    command = bin_dir / 'systemctl'
    command.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$HOME/systemctl.calls"\n')
    command.chmod(0o755)
    result = subprocess.run(['/bin/bash', str(TOOLS / 'install_supervisor_unit.sh')],
                            env={**os.environ, 'HOME': str(tmp_path),
                                 'PATH': str(bin_dir) + ':' + os.environ['PATH']},
                            text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stderr
    unit = (tmp_path / '.config/systemd/user/prismabuild-supervisor.service').read_text()
    assert supervise.SYSTEMD_EXEC in unit.splitlines()
    assert 'KillMode=process' in unit
    assert 'TimeoutStopSec=infinity' in unit
    assert 'SendSIGKILL=no' in unit
    assert (tmp_path / 'systemctl.calls').read_text().splitlines() == [
        '--user daemon-reload', '--user enable prismabuild-supervisor.service']
