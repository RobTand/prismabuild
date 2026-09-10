"""Exited loop children remain the supervisor's responsibility across exec."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

TOOLS = Path(__file__).resolve().parents[1] / "tools" / "fleet"
sys.path.insert(0, str(TOOLS))
import supervise  # noqa: E402


@pytest.mark.parametrize("exit_before_exec", [True, False])
def test_tick_reaps_children_from_previous_process_image(tmp_path, exit_before_exec):
    # Keep the real fork/exec/wait lifecycle in a private supervisor process:
    # waitpid(-1) in a pytest worker must not consume pytest's own children.
    harness = tmp_path / "supervisor_harness.py"
    harness.write_text(textwrap.dedent(f"""
        import os, signal, subprocess, sys
        from pathlib import Path
        sys.path.insert(0, {str(TOOLS)!r})
        import supervise

        if len(sys.argv) == 1:
            read_fd, write_fd = os.pipe()
            child = subprocess.Popen([
                sys.executable, '-c',
                'import os, sys; os.read(int(sys.argv[1]), 1); sys.exit(7)',
                str(read_fd),
            ], pass_fds=(read_fd,), start_new_session=True)
            os.close(read_fd)
            if {exit_before_exec!r}:
                os.write(write_fd, b'x')
                os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT)
            os.set_inheritable(write_fd, True)
            os.execve(sys.executable, [sys.executable, __file__,
                      str(child.pid), str(write_fd), str(os.getpid())], os.environ)

        child_pid, write_fd, original_pid = map(int, sys.argv[1:])
        assert os.getpid() == original_pid
        assert not subprocess._active, 'exec must lose the old Popen registry'
        if not {exit_before_exec!r}:
            os.write(write_fd, b'x')
        os.close(write_fd)
        observed = os.waitid(os.P_PID, child_pid, os.WEXITED | os.WNOWAIT)
        assert observed.si_status == 7
        assert Path(f'/proc/{{child_pid}}/stat').read_text().split(') ')[1][0] == 'Z'

        # A second child stays live during the tick, then exposes its real
        # failure status. Reaping must neither wait for nor signal it.
        live_read, live_write = os.pipe()
        live = subprocess.Popen([
            sys.executable, '-c',
            'import os, sys; os.read(int(sys.argv[1]), 1); sys.exit(9)',
            str(live_read),
        ], pass_fds=(live_read,))
        os.close(live_read)
        supervise.CLAIM = Path({str(tmp_path / 'claim')!r})
        supervise.declared_shape = lambda *a, **k: (0, [])
        supervise._loaded_published_generation = lambda: None
        supervise._next_log_index = lambda: 0
        supervise._claim_holders = lambda: frozenset()
        supervise._ready_backlog = lambda: False
        supervise._live_loops = lambda: []
        checked = []
        def boundary(*args):
            assert not Path(f'/proc/{{child_pid}}').exists(), 'previous-image child remains a zombie at reexec boundary'
            assert live.poll() is None, 'live child must remain alive'
            checked.append(True)
            return False
        supervise._reexec_if_published = boundary
        class TickComplete(Exception):
            pass
        def stop(_seconds):
            raise TickComplete
        original_sleep = supervise.time.sleep
        supervise.time.sleep = stop
        sys.argv = ['supervise', '--loops', '1']
        supervise._live_loops = lambda: [live.pid]
        supervise.loop_args_of = lambda pid: []
        supervise._has_children = lambda pid: False
        try:
            try:
                supervise.main()
            except TickComplete:
                pass
            finally:
                supervise.time.sleep = original_sleep
            assert checked == [True]
            assert signal.getsignal(signal.SIGCHLD) != signal.SIG_IGN
            os.write(live_write, b'x')
            assert live.wait(timeout=5) == 9
            failed = subprocess.run([sys.executable, '-c', 'raise SystemExit(11)'])
            assert failed.returncode == 11
            print('reaped across exec; live child retained; failure statuses 7,9,11 preserved')
        finally:
            os.close(live_write)
            live.wait(timeout=5)
    """))
    result = subprocess.run([sys.executable, str(harness)], text=True,
                            capture_output=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "failure statuses 7,9,11 preserved" in result.stdout


def test_reap_tick_has_a_fixed_syscall_budget(monkeypatch):
    calls = []
    def waitpid(pid, flags):
        calls.append((pid, flags))
        return (123, 0)
    monkeypatch.setattr(supervise.os, "waitpid", waitpid)
    assert supervise._reap_children() == supervise.MAX_REAPS_PER_TICK
    assert calls == [(-1, supervise.os.WNOHANG)] * supervise.MAX_REAPS_PER_TICK


@pytest.mark.parametrize("ending", ["live", "no_children"])
def test_reap_stops_when_no_exited_child_is_available(monkeypatch, ending):
    calls = []
    def waitpid(pid, flags):
        calls.append((pid, flags))
        if len(calls) == 1:
            raise InterruptedError
        if len(calls) == 2:
            return (123, 0)
        if ending == "no_children":
            raise ChildProcessError
        return (0, 0)
    monkeypatch.setattr(supervise.os, "waitpid", waitpid)
    assert supervise._reap_children() == 1
    assert len(calls) == 3


def test_repeated_interruptions_are_bounded(monkeypatch):
    calls = []
    def interrupted(*args):
        calls.append(args)
        raise InterruptedError
    monkeypatch.setattr(supervise.os, "waitpid", interrupted)
    assert supervise._reap_children() == 0
    assert len(calls) == supervise.MAX_REAPS_PER_TICK
