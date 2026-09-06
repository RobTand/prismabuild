"""What a shard's result says about whether the shard ran at all.

#208 cost 74 tests silently: a submission killed before it queued anything
printed the same blank where the summary goes as a shard that ran clean.  #211
replaced the blank with ``NO PYTEST SUMMARY`` and a ``ran`` flag, and derived
that flag from a summary matched by substring -- any line containing
``" passed"``, ``" failed"`` or ``" error"``.

That is the same defect one level in.  pytest's own usage failure prints
``python -m pytest: error: unrecognized arguments: ...`` and pbrun prints
``removed failed exchange probe ...``; either line was captured as the summary
and set ``ran=True`` for a shard that never started a case (#213).  So these
cases feed a mocked shard endings that are not summaries, and endings that
are, and assert the flag tells them apart.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest", ROOT / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]

#: The real ``Popen``, bound before any test replaces it.  ``pbtest`` calls it
#: through the ``subprocess`` module, which is the same module object this file
#: imported, so patching ``pbtest.subprocess.Popen`` patches it for everyone --
#: including the ``git init`` a later shard runs to build its own checkout.  The
#: stand-in below therefore answers for the submission and delegates the rest.
REAL_POPEN = subprocess.Popen


def _one_shard(tmp_path: Path, monkeypatch, output: str, returncode: int,
               *, run: str = "run") -> dict:
    """Run one shard whose pbrun produced ``output``, and read its record.

    The shard is mocked at ``Popen`` because the endings under test are
    endings a real submission only reaches by dying, and a test that has to
    kill a real shard to observe them tests the kill.
    """

    # One directory per call: a test that shards twice must not have the
    # second call trip over the first call's checkout.
    checkout = tmp_path / run / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    class FinishedProcess:
        def __init__(self) -> None:
            self.returncode = returncode

        def communicate(self):
            return output, None

    def popen(command, **kwargs):
        if str(pbtest.PBRUN) in [str(part) for part in command]:
            return FinishedProcess()
        return REAL_POPEN(command, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    report = tmp_path / run / "shards.json"
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
         "--shards", "1", "--json", str(report), "tests"],
    )

    pbtest.main()
    records = json.loads(report.read_text())
    assert len(records) == 1
    return records[0]


#: Endings that are not a terminal summary.  Each is a real line: pytest's
#: usage failure, its two-line form, a pbrun cleanup notice, a traceback
#: frame, and the empty output of a shard killed before it printed anything.
DID_NOT_RUN = [
    pytest.param("", 1, id="killed-before-printing-anything"),
    pytest.param(
        "ERROR: usage: __main__.py [options] [file_or_dir] [file_or_dir] [...]\n"
        "__main__.py: error: unrecognized arguments: --frobnicate\n",
        4, id="pytest-argparse-failure"),
    pytest.param(
        "removed failed exchange probe /mnt/shared/prismabuild-fleet/pb-exchange-zfefj1as\n",
        1, id="pbrun-cleanup-notice-naming-a-failure"),
    pytest.param(
        "Traceback (most recent call last):\n"
        '  File "/x/pbrun.py", line 9, in <module>\n'
        "    raise OSError(error, os.strerror(error))\n",
        1, id="traceback-mentioning-error"),
]

#: Endings that are a terminal summary, in the undecorated form ``-q`` prints
#: and the ``=``-wrapped form above ``-q``.
DID_RUN = [
    pytest.param("1 passed in 0.01s\n", 0, id="quiet-pass"),
    pytest.param("1 failed, 531 passed, 1 skipped in 17.82s\n", 1, id="quiet-mixed"),
    pytest.param("2 failed, 1 error in 12.34s\n", 1, id="quiet-error-count"),
    pytest.param("1 passed, 1 warning in 0.01s\n", 0, id="quiet-warning"),
    pytest.param("3 passed in 65.10s (0:01:05)\n", 0, id="duration-past-a-minute"),
    pytest.param("no tests ran in 0.01s\n", 5, id="no-tests-ran"),
    pytest.param(
        "============================== 1 passed in 0.02s ===============================\n",
        0, id="decorated-summary-above-q"),
]


@pytest.mark.parametrize("output, returncode", DID_NOT_RUN)
def test_a_shard_that_never_reached_a_case_does_not_read_as_having_run(
    tmp_path: Path, monkeypatch, output: str, returncode: int,
) -> None:
    """No terminal summary means ``ran`` is False, whatever words appeared.

    The argparse row is the precise case the substring test got wrong: its
    second line contains ``" error"``, so it was captured as the summary and
    the shard reported ``ran=True`` with zero cases executed.
    """

    record = _one_shard(tmp_path, monkeypatch, output, returncode)

    assert record["ran"] is False, record["summary"]
    assert record["summary"].startswith("NO PYTEST SUMMARY")
    # The count that did not run, not an absence for the reader to infer.
    assert "1 file(s) did not run" in record["summary"]


@pytest.mark.parametrize("output, returncode", DID_RUN)
def test_a_shard_that_reported_a_terminal_summary_keeps_it(
    tmp_path: Path, monkeypatch, output: str, returncode: int,
) -> None:
    """Narrowing the match must not lose a real ending.

    ``summary`` is display text: the shard line and the ``--json`` record both
    show it verbatim, so a real ending replaced by ``NO PYTEST SUMMARY`` would
    be this fix causing the reporting failure it was written to remove.
    """

    record = _one_shard(tmp_path, monkeypatch, output, returncode)

    assert record["ran"] is True
    assert record["summary"] == output.strip()


def test_a_shard_that_did_not_run_says_how_it_ended(
    tmp_path: Path, monkeypatch,
) -> None:
    """A signal, a timeout and a refused submission are different events.

    They all printed one sentence, and the reader had to go find the
    returncode elsewhere to tell a starved shard from a rejected one.
    """

    killed = _one_shard(tmp_path, monkeypatch, "", -9, run="killed")
    refused = _one_shard(tmp_path, monkeypatch, "", 2, run="refused")

    assert "signal 9" in killed["summary"]
    assert "rc=2" in refused["summary"]
