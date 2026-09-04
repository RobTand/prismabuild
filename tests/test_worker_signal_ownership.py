"""A worker's interruption contract is its own, not its launcher's (issue #25)."""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402

from test_core import _action  # noqa: E402


def _launch_with_sigint_ignored(argv: list[str]) -> subprocess.Popen[bytes]:
    """Start ``argv`` the way ``nohup`` or a non-interactive ``&`` would."""

    return subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_IGN),
    )


def test_sigint_reaps_the_action_even_when_the_launcher_ignored_sigint(
    tmp_path: Path,
) -> None:
    """The fleet's dl380g10 loops inherit SIG_IGN for SIGINT.

    main: a worker exec'd with SIGINT ignored still unwinds on SIGINT, reaps
    its action group inside one grace period, and exits non-zero.
    Branch (pre-fix): Python never installs its KeyboardInterrupt handler over
    an inherited SIG_IGN, the signal is dropped, the worker sleeps on, and the
    test's teardown is what kills it -- which is exactly the ``returncode: -9``
    the issue recorded on the x86 population.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    task = (
        "import os,pathlib,time; "
        "f=open('starts.log','ab',buffering=0); "
        "f.write((str(os.getpid())+'\\n').encode()); os.fsync(f.fileno()); "
        "time.sleep(30); pathlib.Path('result.bin').write_bytes(b'result')"
    )
    action = _action(checkout, argv=[sys.executable, "-c", task])
    action_path = tmp_path / "action.json"
    action_path.write_text(json.dumps(action), encoding="utf-8")
    worker = Path(__file__).resolve().parents[1] / "tools" / "prismabuild_worker.py"
    argv = [
        sys.executable, str(worker), "run-local",
        "--action", str(action_path),
        "--cas-root", str(tmp_path / "cas"),
        "--checkout-root", str(checkout),
    ]
    process = _launch_with_sigint_ignored(argv)
    starts = checkout / "starts.log"
    action_pid: int | None = None
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if starts.exists() and starts.read_text(encoding="utf-8").strip():
                action_pid = int(starts.read_text(encoding="utf-8").split()[0])
                break
            time.sleep(0.02)
        assert action_pid is not None, "the action never started"

        process.send_signal(signal.SIGINT)
        assert process.wait(timeout=pb._PROCESS_GROUP_GRACE_SECONDS) != 0
        with pytest.raises(ProcessLookupError):
            os.kill(action_pid, 0)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2.0)
        if action_pid is not None:
            try:
                os.killpg(action_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
