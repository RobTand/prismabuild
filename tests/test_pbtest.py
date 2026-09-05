"""The suite dispatcher relies on pbrun's immutable checkout transport."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest", ROOT / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]


def test_box_local_git_checkout_is_dispatched_without_leaking_its_path(
    tmp_path: Path, monkeypatch,
) -> None:
    """A local source tree is portable; only pbrun's --cwd may name it."""

    checkout = tmp_path / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []

    class FinishedProcess:
        returncode = 0

        def communicate(self):
            return "1 passed in 0.01s\n", None

    def popen(command, **_kwargs):
        calls.append(command)
        return FinishedProcess()

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pbtest.py", "--checkout", str(checkout),
            "--python", "/target/python", "--shards", "1", "tests",
        ],
    )

    assert pbtest.main() == 0
    assert len(calls) == 1
    command = calls[0]
    separator = command.index("--")
    assert command[command.index("--cwd") + 1] == str(checkout.resolve())
    assert str(checkout.resolve()) not in " ".join(command[separator + 1:])
    assert "PYTHONPATH=src:experiments" in command


def _dispatch(tmp_path: Path, monkeypatch, extra: list[str]) -> list[str]:
    """Run one shard's worth of dispatch and return the pbrun argv it built."""

    checkout = tmp_path / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []

    class FinishedProcess:
        returncode = 0

        def communicate(self):
            return "1 passed in 0.01s\n", None

    def popen(command, **_kwargs):
        calls.append(command)
        return FinishedProcess()

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
         "--shards", "1", *extra, "tests"],
    )
    assert pbtest.main() == 0
    assert len(calls) == 1
    return calls[0]


def test_the_transport_reaches_pbrun(tmp_path: Path, monkeypatch) -> None:
    """A suite fanned out under SLURM must not shard into the pull queue.

    ``pbtest`` builds pbrun's argv itself, so a flag it does not forward is a
    flag the shards never see -- and twenty shards submitted to a queue with no
    workers wait out ``--wait-s`` before anybody learns which transport they
    were on.
    """

    monkeypatch.delenv("PRISMABUILD_TRANSPORT", raising=False)
    command = _dispatch(tmp_path, monkeypatch, ["--transport", "slurm"])
    assert command[command.index("--transport") + 1] == "slurm"
    # Before the payload, like every other pbrun flag.
    assert command.index("--transport") < command.index("--")


def test_a_checkout_with_no_receipt_stays_on_the_pull_queue(
    tmp_path: Path, monkeypatch,
) -> None:
    """The default is the fleet's, and a checkout is not a generation.

    This test used to say "the cutover is one environment variable", which
    described one shell and no part of a fleet whose agents start pbtest from
    cron, from systemd user units and from each other. The default rides in
    the published runtime generation's receipt; a checkout has none, so it
    keeps the pull queue until somebody says otherwise. The lane case is
    ``test_default_transport_travels_with_the_generation``.
    """

    monkeypatch.delenv("PRISMABUILD_TRANSPORT", raising=False)
    command = _dispatch(tmp_path, monkeypatch, [])
    assert command[command.index("--transport") + 1] == "pool"


def test_the_environment_can_carry_the_whole_suite_onto_the_lane(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setenv("PRISMABUILD_TRANSPORT", "slurm")
    command = _dispatch(tmp_path, monkeypatch, [])
    assert command[command.index("--transport") + 1] == "slurm"


def test_a_shard_states_its_placement_once(tmp_path: Path, monkeypatch) -> None:
    """Every shard carried ``--anywhere`` beside its class tag.

    ``pbrun`` refuses that pair: ``--anywhere`` asserts every eligible worker
    can run the action and ``--tag x86`` admits only the boxes offering the
    tag, so a suite fanned out this way would refuse at submission, shard by
    shard.  The tag is the claim that survives -- it owns the dependency the
    named interpreter is -- and dropping ``--anywhere`` moves neither the
    placement nor the action key.
    """

    command = _dispatch(tmp_path, monkeypatch, [])

    assert command[command.index("--tag") + 1] == "x86"
    assert "--anywhere" not in command
