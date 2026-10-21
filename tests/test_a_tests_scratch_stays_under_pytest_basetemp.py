"""A test's scratch lives under pytest's basetemp, never loose in TMPDIR.

On 2026-10-20 dl380g10's ``~/tmp``, the TMPDIR every action is given, held
thousands of directories the suite had made and never removed: 1,335 ``tmp*``
directories holding one ``policy.json``, 434 ``mergeq-refusal-*`` fixture
checkouts and 1,249 ``pbtest-xdist-roster-*`` plugin rosters.  The first two
were ``tempfile.mkdtemp`` calls that nothing removed.  The rosters belong to
inner shards the suite ends on purpose: SIGKILLed at a blocked exit hook
(#1530) or stopped by ``os._exit`` (#1455), so the roster's ``with`` exit never
runs.  The suite strips the action key from every test's environment, so those
shards get no attempt directory and put the roster straight into TMPDIR.
pytest removes its basetemp; nothing removes TMPDIR's entries.

Each case runs the real tests in a child pytest whose TMPDIR is an empty
directory of its own and whose basetemp is elsewhere, then requires that
TMPDIR to still be empty.

The record-compatibility helpers unpack old drivers once per process and
remove them at exit, which a killed shard never reaches: 29 ``compat-pre-c1*``
directories were left the same way.  Those live under the session's basetemp.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
import record_compat  # noqa: E402

CASES = {
    "ram-policy": ["tests/test_the_ram_promotion_is_chunked_at_window_quarters.py"],
    "mergeq-refusal": [
        "tests/test_pbmergeq.py::test_pin_runtime_refusal_is_not_retried_as_an_unobserved_test",
        "tests/test_pbmergeq_runtime.py::"
        "test_explicit_missing_worker_interpreter_is_blocked_not_generic_client_errors",
    ],
    "killed-xdist-shard": [
        "tests/test_pbtest_exit_completion_1530.py::test_xdist_completion_precedes_worker_exit_hooks",
    ],
    "abrupt-trace-shard": [
        "tests/test_pbtest_interrupted_trace_1455.py::test_current_test_survives_abrupt_controller_exit",
    ],
}


@pytest.mark.parametrize("case", sorted(CASES))
def test_the_tests_leave_their_tmpdir_empty(tmp_path, case):
    tmpdir = tmp_path / "tmpdir"
    tmpdir.mkdir()
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("PYTEST_", "PBTEST_"))}
    env.update(TMPDIR=str(tmpdir), PYTHONDONTWRITEBYTECODE="1")
    done = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
         f"--basetemp={tmp_path / 'basetemp'}", *CASES[case]],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=600)
    assert done.returncode == 0, done.stdout + done.stderr
    assert sorted(entry.name for entry in tmpdir.iterdir()) == []


def test_old_compat_drivers_unpack_under_the_sessions_basetemp(tmp_path_factory):
    basetemp = tmp_path_factory.getbasetemp()
    assert record_compat.pre_c13_drivers().is_relative_to(basetemp)
    assert record_compat.pre_c12_drivers().is_relative_to(basetemp)
