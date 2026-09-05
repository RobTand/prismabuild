"""Each submission sends its own script, so no caller runs another's runtime.

The batch script used to be one mutable ``job.sh`` per action directory,
replaced by every submitter and handed to ``sbatch`` by path.  An action key is
a content hash, so two callers submitting one key is the ordinary case, and the
lane explicitly permits the deployment's runtime generation to roll between
them: caller B replaced ``job.sh`` before caller A's ``sbatch`` had read it, so
job A executed runtime B's request and CAS root while A's immutable submission
record still named A's.  The scheduler's singleton dependency orders the two
jobs after submission and cannot protect a file race before it.

Issue #68.  The script is now named by the digest of its own bytes, so two
submitters with the same bytes share one immutable file and different bytes are
different names.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import slurm_lane as sl  # noqa: E402

KEY = "d" * 64
ACTION = {"action_key": KEY, "params": {}}


def _completed(argv, stdout: str = ""):
    return subprocess.CompletedProcess(argv, 0, stdout, "")


def _submit(
    tmp_path: Path, name: str, sbatch, *, generation: float | None = None
) -> sl.SubmittedJob:
    return sl.submit(
        ACTION,
        cas=SimpleNamespace(root=tmp_path / f"cas-{name}"),
        request_path=tmp_path / f"request-{name}.json",
        resources=sl.LaneResources(), timeout_s=None,
        worker_script=tmp_path / f"worker-{name}.py",
        job_entry=tmp_path / f"entry-{name}.py",
        job_python=sys.executable, root=tmp_path / "lane",
        published_unix=(
            generation if generation is not None
            else (100.0 if name == "A" else 200.0)
        ),
        sbatch=sbatch,
    )


def test_the_script_sbatch_reads_is_the_one_this_caller_published(
    tmp_path: Path
) -> None:
    """Pre-fix: ``observed["executed"] == "runtime-A"`` failed with
    ``'runtime-B'``, while caller A's record named request-A and cas-A."""

    for name in ("A", "B"):
        (tmp_path / f"entry-{name}.py").write_text(
            f'print("runtime-{name}")\n', encoding="utf-8")
    observed: dict[str, str] = {}

    def sbatch_b(argv):
        observed["script_B"] = Path(argv[-1]).read_text(encoding="utf-8")
        return _completed(argv, "5002")

    def sbatch_a(argv):
        # Submission B lands after A published its script and before A's
        # ``sbatch`` reads it, which is the whole window.
        _submit(tmp_path, "B", sbatch_b)
        # What the scheduler copies is the file at the path it was handed.
        handed = Path(argv[-1])
        text = handed.read_text(encoding="utf-8")
        observed["script_A"] = text
        observed["handed_A"] = str(handed)
        copy = tmp_path / "accepted-A.sh"
        copy.write_text(text, encoding="utf-8")
        execution = subprocess.run(
            ["/bin/bash", str(copy)], capture_output=True, text=True)
        assert execution.returncode == 0, execution.stderr
        observed["executed"] = execution.stdout.strip()
        return _completed(argv, "5001")

    job = _submit(tmp_path, "A", sbatch_a)

    assert observed["executed"] == "runtime-A"
    assert observed["script_B"] != observed["script_A"]
    assert str(tmp_path / "request-A.json") in observed["script_A"]
    assert str(tmp_path / "request-B.json") in observed["script_B"]

    record = json.loads(job.record_path.read_text(encoding="utf-8"))
    assert record["request"] == str(tmp_path / "request-A.json")
    assert record["cas_root"] == str(tmp_path / f"cas-A")
    # The record names the bytes that were submitted, and the digest is the
    # check an operator can repeat against the file.
    assert record["script"] == observed["handed_A"]
    assert record["script_sha256"] == hashlib.sha256(
        observed["script_A"].encode("utf-8")).hexdigest()
    assert Path(record["script"]).name == f"{record['script_sha256']}.sh"


def test_two_submissions_of_the_same_bytes_share_one_immutable_script(
    tmp_path: Path
) -> None:
    """Content addressing, not a file per submission: the same script twice is
    the same file, and the bytes cannot have been substituted."""

    (tmp_path / "entry-A.py").write_text('print("runtime-A")\n',
                                         encoding="utf-8")
    first = _submit(tmp_path, "A", lambda argv: _completed(argv, "5001"),
                    generation=100.0)
    second = _submit(tmp_path, "A", lambda argv: _completed(argv, "5002"),
                     generation=300.0)

    assert first.script == second.script
    assert first.script.exists()
    scripts = sorted(
        (first.directory / sl.SCRIPT_DIRNAME).glob("*.sh"))
    assert scripts == [first.script]


def test_the_pointer_is_a_pointer_and_not_the_submitted_script(
    tmp_path: Path
) -> None:
    """``job.sh`` stays for a reader that wants one name, and the newest
    submission's bytes are what it holds.  Nothing is submitted by that path.
    """

    for name in ("A", "B"):
        (tmp_path / f"entry-{name}.py").write_text(
            f'print("runtime-{name}")\n', encoding="utf-8")
    handed: list[str] = []

    def sbatch(argv):
        handed.append(argv[-1])
        return _completed(argv, "5001")

    first = _submit(tmp_path, "A", sbatch)
    second = _submit(tmp_path, "B", sbatch)
    pointer = first.directory / "job.sh"

    assert handed == [str(first.script), str(second.script)]
    assert first.script != second.script
    assert pointer.read_bytes() == second.script.read_bytes()
    assert first.script.read_bytes() != second.script.read_bytes()
