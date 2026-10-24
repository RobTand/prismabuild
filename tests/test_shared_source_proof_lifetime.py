"""Real generation proof adoption and descriptor lifetime, not caller markers.

The fresh interpreter consumes the materializer's trusted host-local publication;
no serialized proof or caller-supplied "verified" flag is passed in the item.
The lifetime check complements the fake leases in the attempt-layout tests.
"""
from copy import deepcopy
import json
import os
import select
import signal
import subprocess
import sys
import time

import pytest

from test_producer_snapshot_is_proven import sealed  # noqa: F401 -- pytest fixture
from test_shared_snapshot_generations_2119 import _borrow, _record, _reap
from test_shared_pbtest_attempt_lifetime import _subreaper, _reap as _reap_child
from prismabuild import core, materialize


_REUSE = """import json, sys
from prismabuild import core, materialize
request = json.load(sys.stdin)
def unexpected_identity(*args, **kwargs):
    raise AssertionError('fresh interpreter reran generation Git proof')
core.git_checkout_identity = unexpected_identity
with materialize._shared_source_checkout(request['item'], local_checkout_root=request['base']) as lease:
    result = lease.preflight(request['action'])
    try:
        lease.preflight(request['bad'])
    except core.ActionContractError:
        pass
    else:
        raise AssertionError('warm proof skipped per-action validation')
    assert lease.preflight(request['action']) == result
    print(json.dumps([str(lease.checkout_root), str(lease.generation), result['action_key']]))
"""


def test_published_live_proof_is_reused_by_a_fresh_interpreter(sealed, tmp_path):
    action, cas, _ = sealed
    base = tmp_path / "local"
    item = _record(action, cas)
    bad = deepcopy(action)
    bad.pop("action_key")
    bad["inputs"] = []
    bad = core.seal_action(bad)
    with _borrow()(item, local_checkout_root=base) as lease:
        assert lease.preflight(action)["action_key"] == action["action_key"]
        # This is an exec, not a fork inheriting the process-local proof cache.
        child = subprocess.run(
            [sys.executable, "-c", _REUSE],
            input=json.dumps({"item": item, "base": str(base),
                              "action": action, "bad": bad}),
            capture_output=True, text=True, timeout=30)
        assert child.returncode == 0, child.stdout + child.stderr
        assert json.loads(child.stdout) == [
            str(lease.checkout_root), str(lease.generation), action["action_key"]]


_LAUNCHER = """import subprocess, sys
fds = tuple(map(int, sys.argv[1:]))
child = subprocess.Popen(
    [sys.executable, '-c', 'import os,sys; os.read(int(sys.argv[1]), 1)', str(fds[1])],
    pass_fds=fds, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
print(child.pid, flush=True)
sys.stdin.read(1)
"""


def test_real_reader_fd_pins_source_after_context_exit_and_launcher_sigkill(sealed, tmp_path):
    action, cas, _ = sealed
    base = tmp_path / "local"
    launcher, descendant = None, None
    with _subreaper():
        release_read, release_write = os.pipe()
        try:
            with _borrow()(_record(action, cas), local_checkout_root=base) as lease:
                lease.preflight(action)
                root = lease.checkout_root
                descriptor = lease.duplicate_reader_fd()
                try:
                    launcher = subprocess.Popen(
                        [sys.executable, "-c", _LAUNCHER, str(descriptor), str(release_read)],
                        pass_fds=(descriptor, release_read), stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
                finally:
                    os.close(descriptor)  # No test-owned duplicate can pin the tree.
                assert select.select([launcher.stdout], [], [], 10)[0], "launcher did not start"
                descendant = int(launcher.stdout.readline())
            # The real lease is closed, and SIGKILL closes the launcher's copy.
            # Only its one persistent descendant now owns a source descriptor.
            launcher.kill()
            assert launcher.wait(timeout=5) == -signal.SIGKILL
            _reap(base)
            _reap(base, now=time.time() + 366 * 86400)
            assert (root / "payload.txt").read_text() == "sealed by a producer\n"

            os.write(release_write, b"x")
            _reap_child(descendant)
            descendant = None
            # Bounded sweeps first notice idle, then retire after the grace.
            _reap(base)
            _reap(base, now=time.time() + 366 * 86400)
            assert not root.exists(), "source retirement did not converge after the child exited"
        finally:
            if launcher is not None and launcher.poll() is None:
                launcher.kill()
                launcher.wait(timeout=5)
            if descendant is not None:
                try:
                    os.kill(descendant, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                _reap_child(descendant)
            if launcher is not None:
                launcher.stdin.close()
                launcher.stdout.close()
            os.close(release_read)
            os.close(release_write)


def test_closed_lease_refuses_reader_fd_duplication(sealed, tmp_path):
    action, cas, _ = sealed
    with _borrow()(_record(action, cas), local_checkout_root=tmp_path / "local") as lease:
        pass
    with pytest.raises(materialize.MaterializationContractError, match="released"):
        lease.duplicate_reader_fd()
