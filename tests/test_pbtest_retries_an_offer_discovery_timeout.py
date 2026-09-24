"""A shard whose submission hit a worker-offer discovery timeout is retried (#1102).

``pbrun`` refuses a submission when its worker-offer scan outlives
``SUBMISSION_OFFER_READ_TIMEOUT_S``.  That refusal publishes nothing, and it is
the one refusal that repeating the submission can clear
(``OfferDiscoveryTimedOut``).  ``pbcampaign --max-inflight`` retries it within
its wait budget (#560).  ``pbtest`` did not: on 2026-09-24, during an NFS
TEST_STATEID storm, three PQ #1176 runs each lost one shard to it and ended
rc 1 with "NO PYTEST SUMMARY", and someone reran them by hand.

``pbtest`` runs ``pbrun`` as a subprocess, so it sees the refusal only as
``pbrun``'s last line and exit 1.  These tests build that line with the real
``pbrun.bounded_offer_snapshot``, so the text ``pbtest`` recognizes cannot
drift from the text ``pbrun`` prints.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import types

import pytest

from pbtest_shard_output import ShardProcess, ONE_PASS, shard_output


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest", ROOT / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]
pbrun = pbtest.pbrun

REAL_POPEN = subprocess.Popen

TODAY = ("NO PYTEST SUMMARY -- 1 file(s) did not run "
         "(the shard ended rc=1, before or outside pytest)")


def _pbrun_refusal(monkeypatch, result: dict, *, retained: bool = False) -> str:
    """What ``pbrun`` prints when its offer read ends in ``result``.

    ``bounded_offer_snapshot`` builds the message and raises it as a
    ``SystemExit``; the interpreter prints that text on stderr, which the
    shard's pipe carries, and exits 1.
    """

    def bounded(section, read, *, deadline, abandoned, **kwargs):
        if retained:
            abandoned.append({"pid": 4242, "section": section})
        return dict(result)

    with monkeypatch.context() as patch:
        patch.setattr(pbrun.pbstatus, "bounded", bounded)
        with pytest.raises(SystemExit) as caught:
            pbrun.bounded_offer_snapshot(pbrun.pool.PoolQueue(Path("/nonexistent")))
    return f"{caught.value.code}\n"


def _timed_out(monkeypatch) -> str:
    """The retryable refusal, exactly as ``pbrun`` prints it."""

    return _pbrun_refusal(monkeypatch, {"status": "timed_out", "elapsed_s": 5.005})


class _Clock:
    """A monotonic clock that each submission and each sleep advance."""

    def __init__(self, submission_s: float) -> None:
        self.now = 1000.0
        self.submission_s = submission_s
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _shard(tmp_path: Path, monkeypatch, endings: list[tuple[str, int]],
           *, wait_s: float = 10800.0, submission_s: float = 5.0):
    """Run one shard whose successive ``pbrun`` attempts end as ``endings``.

    Returns the exit code, the shard's JSON record, the number of ``pbrun``
    launches, the clock, and what ``pbtest`` printed.
    """

    checkout = tmp_path / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    clock = _Clock(submission_s)
    launches: list[list[str]] = []

    class FinishedProcess(ShardProcess):
        def __init__(self, output: str, returncode: int) -> None:
            self.output = output
            self.returncode = returncode

        def communicate(self):
            return self.output, None

    def popen(command, **kwargs):
        if str(pbtest.PBRUN) not in [str(part) for part in command]:
            return REAL_POPEN(command, **kwargs)
        launches.append([str(part) for part in command])
        clock.now += clock.submission_s
        output, returncode = endings[min(len(launches), len(endings)) - 1]
        return FinishedProcess(output, returncode)

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    # ``raising=False``: the clock is read through ``pbtest.time``, which a
    # pbtest without the retry never imported.  The test then fails on what
    # the shard reports, not on the patch.
    monkeypatch.setattr(pbtest, "time", types.SimpleNamespace(
        monotonic=clock.monotonic, sleep=clock.sleep), raising=False)
    report = tmp_path / "shards.json"
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
        "--shards", "1", "--wait-s", str(wait_s), "--json", str(report), "tests",
    ])
    exit_code = pbtest.main()
    records = json.loads(report.read_text())
    assert len(records) == 1
    return exit_code, records[0], launches, clock


def _shard_line(out: str) -> str:
    lines = [line for line in out.splitlines()
             if line.startswith("shard   0 ") and "pbrun:" not in line
             and "pbtest:" not in line]
    assert lines, out
    return lines[-1]


def test_a_shard_whose_offer_read_timed_out_once_is_submitted_again(
    tmp_path, monkeypatch, capsys,
):
    """The first attempt is refused, the second is submitted and passes."""

    refusal = _timed_out(monkeypatch)
    exit_code, record, launches, clock = _shard(
        tmp_path, monkeypatch, [(refusal, 1), (ONE_PASS, 0)])
    out = capsys.readouterr().out

    assert len(launches) == 2
    assert launches[0] == launches[1]             # the same submission, byte for byte
    assert record.get("attempts") == 2
    assert record["returncode"] == 0
    assert record["ran"] is True
    assert record["summary"] == "1 passed in 0.01s"
    assert exit_code == 0
    assert clock.sleeps == [pbrun.POLL_S]         # the pace pbcampaign retries at
    # The retry is announced while it happens, and the shard's own line
    # carries it, so the receipt shows the run was not a first submission.
    assert any("worker-offer discovery timed out" in line and "attempt 2" in line
               for line in out.splitlines() if line.startswith("shard   0 pbtest:")), out
    shard_line = _shard_line(out)
    assert shard_line.startswith("shard   0 ok")
    assert "attempt 2" in shard_line


def test_a_shard_whose_offer_reads_all_time_out_fails_at_its_deadline(
    tmp_path, monkeypatch, capsys,
):
    """Every attempt is refused: the shard fails, with today's summary, once
    ``--wait-s`` is spent."""

    refusal = _timed_out(monkeypatch)
    # Each attempt costs 5 s and each pause POLL_S (5 s): attempts start at
    # 0 s and 10 s, and the third would start at 20 s, past the 12 s budget.
    exit_code, record, launches, clock = _shard(
        tmp_path, monkeypatch, [(refusal, 1)], wait_s=12.0, submission_s=5.0)
    out = capsys.readouterr().out

    assert len(launches) == 2
    assert record.get("attempts") == 2
    assert record["returncode"] == 1
    assert record["ran"] is False
    assert record["summary"] == TODAY
    assert exit_code == 1
    assert sum(clock.sleeps) <= 12.0
    shard_line = _shard_line(out)
    assert shard_line.startswith("shard   0 rc=1")
    assert TODAY in shard_line
    assert "attempt 2" in shard_line


def _not_retryable(monkeypatch):
    """Endings that must fail the shard on the first attempt."""

    return [
        # The reader survived cleanup: a retry would race it.
        ("retained-reader", _pbrun_refusal(
            monkeypatch, {"status": "timed_out", "elapsed_s": 5.005}, retained=True), 1),
        # The read failed or returned garbage: a retry cannot fix either.
        ("read-failed", _pbrun_refusal(
            monkeypatch, {"status": "error", "type": "OSError", "error": "EIO"}), 1),
        ("invalid-snapshot", _pbrun_refusal(
            monkeypatch, {"status": "ok", "value": "not a list"}), 1),
        # Any other refusal.
        ("no-worker", "pbrun: no recorded worker can run this action. "
                      "required tags: ['gb10']\n", 1),
        # A shard that ran and failed while its own output quotes the refusal:
        # the text is not pbrun's last line, so it is not pbrun's refusal.
        ("quoted-by-a-failing-test", shard_output(
            passed=1, prefix="----- Captured stderr call -----\n"
            + _timed_out(monkeypatch)).replace("1 passed", "1 failed"), 1),
    ]


@pytest.mark.parametrize("case", range(5))
def test_any_other_refusal_is_not_retried(tmp_path, monkeypatch, capsys, case):
    name, output, returncode = _not_retryable(monkeypatch)[case]
    exit_code, record, launches, clock = _shard(
        tmp_path, monkeypatch, [(output, returncode), (ONE_PASS, 0)])
    out = capsys.readouterr().out

    assert len(launches) == 1, name
    assert record.get("attempts") == 1, name
    assert record["returncode"] == 1, name
    assert clock.sleeps == [], name
    assert exit_code == 1, name
    assert "attempt 2" not in out, name
