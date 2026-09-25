"""Every shard ``pbtest`` builds carries a per-test bound it can name.

A suite fanned out over the fleet is the one place a hanging test costs a slot
rather than a developer's attention: action
``766d7ae5e0382b755a1189d4c1c3a42407d90fdb3cea56898b02b26855908489`` held a
63-file shard on dl380g10 for its whole 3600 s ceiling and the record named no
test, because the only bound in play was the pool's deadline (#600).  The
plugin that names the test reads ``PRISMABUILD_TEST_TIMEOUT_S``; a shard that
does not export it is a shard that runs unbounded, so the export belongs in
the command builder, not in the operator's memory.

The bound is derived rather than picked.  It fires one ``pool.HEARTBEAT_S``
before the shard's own ceiling, which is the margin that gets the handler's
stderr -- the node id -- into the lease's execution observation while the
action is still alive.  These cases read the derivation off the generated
``pbrun`` argv, which is the byte a shard actually runs under.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest

from pbtest_shard_output import ShardProcess, shard_output_for  # noqa: E402

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

_SPEC = importlib.util.spec_from_file_location(
    "pbtest", REPOSITORY / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbtest)                      # type: ignore[union-attr]

from prismabuild import pool, pytest_test_bound  # noqa: E402
from worker_loop import DEFAULT_EXECUTION_CEILING_S  # noqa: E402


class _FinishedProcess(ShardProcess):
    returncode = 0

    def __init__(self, command):
        self.output = shard_output_for(command)

    def communicate(self):
        return self.output, None


def _dispatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra):
    """One shard's dispatch, and the ``pbrun`` argv it built."""

    checkout = tmp_path / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []

    def _popen(command, **_kwargs):
        calls.append(list(command))
        return _FinishedProcess(command)

    monkeypatch.setattr(pbtest.subprocess, "Popen", _popen)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
         "--shards", "1", *extra, "tests"],
    )
    code = pbtest.main()
    return code, calls


def _exported_bound(command) -> str | None:
    """The bound the shard exports, read from the ``env`` prefix it runs under.

    After ``--`` the argv is ``env NAME=VALUE ... python -m pytest ...``, so a
    setting placed after the interpreter would be an argument to pytest rather
    than an environment variable.  Read only the prefix, so a misplaced export
    reads as absent instead of as present.
    """

    payload = command[command.index("--") + 1:]
    assert payload[0] == "env"
    prefix = []
    for word in payload[1:]:
        if "=" not in word:
            break
        prefix.append(word)
    for word in prefix:
        name, value = word.split("=", 1)
        if name == pytest_test_bound.TIMEOUT_ENV:
            return value
    return None


def test_a_shard_with_its_own_deadline_bounds_its_tests_inside_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """main: the exported bound is the shard's deadline less one heartbeat.

    branch: a test that outlives it is failed by pytest, which knows its name,
    a heartbeat before the pool's deadline ends the lease, which does not.
    """

    code, calls = _dispatch(tmp_path, monkeypatch, ["--timeout-s", "3600"])

    assert code == 0
    exported = _exported_bound(calls[0])
    assert exported is not None, (
        "a shard that exports no bound runs unbounded, which is the state "
        "#600 is about")
    assert float(exported) == pytest.approx(3600.0 - pool.HEARTBEAT_S)
    assert float(exported) < 3600.0


def test_a_shard_without_a_deadline_bounds_against_the_loops_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A shard that names no deadline still runs under the worker loop's.

    branch: the derivation uses that ceiling, so the common case -- no
    ``--timeout-s`` at all, which is how the hung shard was submitted -- is
    bounded too, rather than being the one case left unbounded.
    """

    code, calls = _dispatch(tmp_path, monkeypatch, [])

    assert code == 0
    exported = _exported_bound(calls[0])
    assert exported is not None
    assert float(exported) == pytest.approx(
        DEFAULT_EXECUTION_CEILING_S - pool.HEARTBEAT_S)


def test_a_measured_run_may_bound_far_tighter_than_its_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The derivation is a safe default, not a measurement.

    branch: a shard whose real duration is known -- the 63-file shard ran in
    69 s twice before it hung -- can be bounded on that number instead, and
    the explicit value is what reaches the shard.
    """

    code, calls = _dispatch(
        tmp_path, monkeypatch, ["--timeout-s", "3600", "--test-timeout-s", "300"])

    assert code == 0
    assert _exported_bound(calls[0]) == "300"


def test_the_bound_can_be_switched_off_for_a_run_that_wants_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Zero exports nothing rather than exporting a zero.

    branch: the plugin reads an absent variable as "no bound", so the shard
    argv of a disabled run is the argv it had before this change -- which is
    what makes an unbounded reproduction of an old receipt possible.
    """

    code, calls = _dispatch(tmp_path, monkeypatch, ["--test-timeout-s", "0"])

    assert code == 0
    assert _exported_bound(calls[0]) is None


def test_the_bound_follows_the_ceiling_the_boxes_announce() -> None:
    """main: the derivation takes the smallest ceiling a claimant announces.

    branch: dl380g10 announces 3600 s, so a bound sized against the published
    loop default of 7200 s would never have fired on the box whose shard hung.
    Reading the announcement is what makes the default bite there.  ``gpu``
    is explicit here: this is the shared announcement-vs-default derivation
    a ``--gpu`` shard also uses, not the further, non-GPU-only cap #1123
    added on top of it (covered separately, in the pbtest CPU-shard tests).
    """

    assert pbtest.per_test_bound(
        timeout_s=None, override_s=None, gpu=True,
        ceilings={"dl380g10": 3600.0, "sparky": 86400.0},
    ) == pytest.approx(3600.0 - pool.HEARTBEAT_S)


def test_a_box_that_announced_nothing_is_not_read_as_unbounded() -> None:
    """A silent ceiling is "did not say", and cannot lower the derivation.

    branch: the loops that starved #275 announced nothing; reading silence as
    a number would put this derivation in the same false confidence. With no
    announcement at all it falls back to the published default, which can be
    too generous to fire and never too tight.
    """

    assert pbtest.per_test_bound(
        timeout_s=None, override_s=None, ceilings={"quiet": None},
    ) == pytest.approx(DEFAULT_EXECUTION_CEILING_S - pool.HEARTBEAT_S)
    assert pbtest.per_test_bound(
        timeout_s=None, override_s=None, ceilings={"quiet": None, "loud": 600.0},
    ) == pytest.approx(600.0 - pool.HEARTBEAT_S)


def test_an_asked_deadline_shorter_than_the_ceiling_wins() -> None:
    """The pool applies ``min(requested, ceiling)``; so does this.

    branch: a shard that asks for less than its box allows is bounded by what
    it asked for, not by what the box would have permitted.
    """

    assert pbtest.per_test_bound(
        timeout_s=900.0, override_s=None, ceilings={"dl380g10": 3600.0},
    ) == pytest.approx(900.0 - pool.HEARTBEAT_S)


def test_the_derivation_never_returns_a_negative_bound() -> None:
    """A ceiling smaller than a heartbeat leaves no margin to derive from.

    branch: it yields no bound rather than a negative one, because a negative
    number reaching the plugin would disarm it by accident instead of by
    decision.
    """

    assert pbtest.per_test_bound(timeout_s=1.0, override_s=None) == 0.0
    assert pbtest.per_test_bound(timeout_s=None, override_s=-5.0) == 0.0
