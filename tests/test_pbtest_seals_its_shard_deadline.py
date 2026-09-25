"""Every pbtest shard seals the deadline it runs under (#939).

Admission judges each holder in a waiting item's way by what the holder
declared (``PoolQueue.holder_bound``, #924).  A pbtest shard declared nothing,
although pbtest derives the deadline it runs under for its per-test bound, so
admission could read a shard only by its age.  Each shard now seals that
deadline as ``execution_timeout_s``: the smaller of ``--timeout-s`` and the
smallest ceiling a box able to claim it announces, which is the ``min``
``pool._execution_timeout`` applies at claim.  The per-test bound stays one
heartbeat inside it.

The announcements are read from the queue ``pbrun`` submits to.  Until #939
pbtest read them from a bare ``pool.PoolQueue()``, whose default root no box
has, so it read none and every bound it derived was the fallback.  These
cases publish real offers into the queue ``pbrun.SH`` names, which
``conftest`` repoints under ``tmp_path``, and read the result off the ``pbrun``
argv each shard is submitted with.
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

import pbrun  # noqa: E402
from prismabuild import pool, pytest_test_bound  # noqa: E402
from worker_loop import DEFAULT_EXECUTION_CEILING_S  # noqa: E402


class _FinishedProcess(ShardProcess):
    returncode = 0

    def __init__(self, command):
        self.output = shard_output_for(command)

    def communicate(self):
        return self.output, None


def _announce(**ceilings: tuple[list[str], float | None]) -> Path:
    """Publish one live offer per host into the queue ``pbrun`` submits to."""

    root = pbrun.SH / "pb-queue"
    queue = pool.PoolQueue(root)
    queue.ensure_layout()
    for host, (tags, ceiling) in ceilings.items():
        queue.announce(host=host, tags=tags, has_gpu=False,
                       capacity={"cpu": 8, "mem_gb": 16},
                       timeout_ceiling_s=ceiling)
    return root


def _dispatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra, *, shards=2):
    """Every shard's ``pbrun`` argv, and what pbtest printed."""

    checkout = tmp_path / "checkout"
    for index in range(shards):
        test_file = checkout / "tests" / f"test_{index}.py"
        test_file.parent.mkdir(parents=True, exist_ok=True)
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
         "--shards", str(shards), *extra, "tests"],
    )
    assert pbtest.main() == 0
    assert len(calls) == shards
    return calls


def _sealed(command) -> float | None:
    """The ``--timeout-s`` a shard hands ``pbrun``, which seals it into params."""

    flags = command[:command.index("--")]
    if "--timeout-s" not in flags:
        return None
    return float(flags[flags.index("--timeout-s") + 1])


def _exported_bound(command) -> float | None:
    """The per-test bound in the ``env`` prefix the shard's pytest runs under."""

    payload = command[command.index("--") + 1:]
    assert payload[0] == "env"
    for word in payload[1:]:
        if "=" not in word:
            break
        name, value = word.split("=", 1)
        if name == pytest_test_bound.TIMEOUT_ENV:
            return float(value)
    return None


def test_the_ceilings_are_read_from_the_queue_pbrun_submits_to() -> None:
    """main: an offer in ``pbrun``'s queue is an offer pbtest reads.

    branch: the bare ``pool.PoolQueue()`` pbtest used names
    ``pool.DEFAULT_POOL_ROOT``, a different directory, so this read ``{}``
    and every bound fell back to the published loop default.
    """

    _announce(dl380g10=(["x86", "dl380g10"], 3600.0),
              sparky=(["gb10", "sparky"], 86400.0))

    assert pbtest.announced_ceilings(["x86"]) == {"dl380g10": 3600.0}
    assert pbtest.announced_ceilings(["sparky"]) == {"sparky": 86400.0}


def test_every_shard_seals_the_smallest_ceiling_a_claimant_announces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """main: the sealed deadline is the one the shard would be cut at anyway.

    branch: of the two x86 boxes that could claim it, the smaller ceiling is
    the one a claim would apply, so every shard seals 1200 s; the gb10 box
    cannot claim an x86 shard and does not enter the ``min``.  The per-test
    bound is one heartbeat inside the sealed deadline, on the same read.
    Both announced ceilings stay under pbtest's own non-GPU cap (#1123, twice
    ``pool.WITHHOLD_CEILING_S``) so this exercises the ``min``-over-claimants
    logic in isolation from that cap, which has its own coverage.
    """

    _announce(dl380g10=(["x86", "dl380g10"], 1200.0),
              other=(["x86", "other"], 1500.0),
              sparky=(["gb10", "sparky"], 600.0))

    calls = _dispatch(tmp_path, monkeypatch, ["--tag", "x86"])

    for command in calls:
        assert _sealed(command) == 1200.0
        assert _exported_bound(command) == pytest.approx(1200.0 - pool.HEARTBEAT_S)
    out = capsys.readouterr().out
    assert "execution_timeout_s=1200" in out
    assert "dl380g10 1200s" in out


def test_an_asked_deadline_inside_every_ceiling_is_sealed_as_asked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``--timeout-s`` under every announcement is what each shard seals.

    branch: it reaches ``pbrun`` spelled as before #939, so an explicit run's
    shard argv, and with it its action keys, is unchanged.
    """

    _announce(dl380g10=(["x86", "dl380g10"], 3600.0))

    calls = _dispatch(tmp_path, monkeypatch, ["--tag", "x86", "--timeout-s", "1800"])

    for command in calls:
        flags = command[:command.index("--")]
        assert flags[flags.index("--timeout-s") + 1] == str(1800.0)
        assert _exported_bound(command) == pytest.approx(1800.0 - pool.HEARTBEAT_S)


def test_an_asked_deadline_past_a_ceiling_is_sealed_at_the_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """A deadline longer than a claimant allows is sealed as the one it gets.

    branch: the box would cut the shard at its own ceiling whatever the
    request said, so sealing the request would declare an end the shard never
    reaches.  pbtest says it lowered the number rather than lowering it
    silently.
    """

    _announce(dl380g10=(["x86", "dl380g10"], 3600.0))

    calls = _dispatch(tmp_path, monkeypatch, ["--tag", "x86", "--timeout-s", "9000"])

    for command in calls:
        assert _sealed(command) == 3600.0
    out = capsys.readouterr().out
    assert "--timeout-s 9000 exceeds" in out


def test_with_nothing_announced_and_nothing_asked_no_deadline_is_sealed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No announcement and no ``--timeout-s`` seal nothing, as before #939.

    branch: the published loop default is a guess about boxes that said
    nothing; sealing it would give admission a declared end nobody declared.
    The per-test bound keeps its fallback to that default.
    """

    calls = _dispatch(tmp_path, monkeypatch, ["--tag", "x86"])

    for command in calls:
        assert _sealed(command) is None
        assert _exported_bound(command) == pytest.approx(
            DEFAULT_EXECUTION_CEILING_S - pool.HEARTBEAT_S)


def test_switching_the_per_test_bound_off_still_seals_the_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--test-timeout-s 0`` removes the per-test bound, not the shard's end.

    branch: the deadline is the box's, and a shard runs under it either way.
    The announced ceiling stays under pbtest's own non-GPU cap (#1123) so
    this exercises "0 does not touch the sealed end" without that cap
    intervening, which has its own coverage.
    """

    _announce(dl380g10=(["x86", "dl380g10"], 1200.0))

    calls = _dispatch(tmp_path, monkeypatch, ["--tag", "x86", "--test-timeout-s", "0"])

    for command in calls:
        assert _sealed(command) == 1200.0
        assert _exported_bound(command) is None


def test_the_per_test_bound_is_one_heartbeat_inside_the_sealed_deadline() -> None:
    """The two numbers come from one derivation and cannot drift apart.

    branch: sealing ``ceiling - HEARTBEAT_S`` instead would put the lease's
    deadline on the instant the per-test alarm fires, and the node id the
    alarm writes would no longer land in the record before the lease ends.
    """

    for timeout_s, ceilings in [
        (None, {"dl380g10": 3600.0, "sparky": 86400.0}),
        (1800.0, {"dl380g10": 3600.0}),
        (9000.0, {"dl380g10": 3600.0, "quiet": None}),
        (900.0, {}),
    ]:
        sealed = pbtest.shard_ceiling(timeout_s=timeout_s, ceilings=ceilings)
        assert pbtest.per_test_bound(
            timeout_s=timeout_s, override_s=None, ceilings=ceilings,
        ) == pytest.approx(sealed - pool.HEARTBEAT_S)
    assert pbtest.shard_ceiling(timeout_s=None, ceilings={"quiet": None}) is None
