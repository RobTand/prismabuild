"""A fleet tool must find its ``pbrun.py`` under both published layouts.

The publisher keeps every fleet entry point twice: ``tools/name.py`` for the
stable commands and ``tools/fleet/name.py`` for checkout-compatible imports,
with the same bytes under both names. A checkout has only the second. So a
launcher that spelled the flat path outright worked in a published generation
and not in a checkout, which is the invocation the operating guide shows.

``pool_reset --apply`` is where that cost the most: every path-addressed
failure was recovered correctly and then handed to a child that could not
start, so each one printed "can't open file" and stayed failed. The same
constant sits in ``pbtest``, and the same assumption sits in the path
``require_pool`` tells an operator to run.

The regression here drives ``pool_reset.main --apply`` end to end against a
temporary queue with its own CAS and checkout, with the real
``submit_command``: the refusal tests replace that function, so none of them
could see which launcher it names. Only the child process is stood in for,
because starting a real ``pbrun`` would submit work.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

import pool_reset  # noqa: E402
import require_pool  # noqa: E402
import runtime_paths  # noqa: E402

from test_pool_reset_refusal import KEY, fleet  # noqa: E402,F401

_SPEC = importlib.util.spec_from_file_location(
    "pbtest", REPOSITORY / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbtest)                      # type: ignore[union-attr]

#: Where a checkout keeps the submitter. The published flat copy has the same
#: bytes, so either answer runs the same code there; only a checkout can tell
#: the two apart.
IN_A_CHECKOUT = REPOSITORY / "tools" / "fleet" / "pbrun.py"


class _Stub:
    """A child that was started and is still running, as ``Popen`` looks."""

    pid = 4242

    def wait(self, timeout=None):
        raise pool_reset.subprocess.TimeoutExpired(cmd="pbrun.py",
                                                   timeout=timeout or 0.0)


def test_a_reset_from_a_checkout_starts_the_pbrun_beside_it(
    fleet: dict, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """The documented checkout invocation resubmits instead of exiting 2.

    main: the argv ``pool_reset`` would run names a ``pbrun.py`` that exists.
    branch: the whole ``--apply`` pass completes, so the recovered record is
    stamped ``reset`` and the command exits 0.
    """

    started: list[list[str]] = []

    def _capture(command, *, queue_root: Path, key: str):
        started.append(list(command))
        log = Path(queue_root) / pool_reset.RESETS / f"{key}.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text("", encoding="utf-8")
        return _Stub(), log

    monkeypatch.setattr(pool_reset, "start_resubmission", _capture)
    monkeypatch.setattr(pool_reset, "REFUSAL_WINDOW_S", 0.0, raising=False)

    code = pool_reset.main([
        "--apply", "--transport", "pool",
        "--queue-root", str(fleet["queue_root"]),
        "--cas-root", str(fleet["cas_root"]),
    ])
    capsys.readouterr()

    assert code == 0
    assert len(started) == 1
    launcher = Path(started[0][1])
    assert launcher.is_file(), started[0]
    assert launcher == IN_A_CHECKOUT
    record = json.loads(fleet["failed"].read_text(encoding="utf-8"))
    assert record["reset"]["reason"].startswith("re-submitted")


def test_the_same_layouts_answer_for_pbtest_and_for_the_hook(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """One resolver, so a second tool cannot keep the old assumption.

    branch: ``pbtest`` starts every shard through this path, and
    ``require_pool`` prints it in the refusal an agent is meant to copy.
    """

    assert pbtest.PBRUN == IN_A_CHECKOUT
    assert Path(require_pool.PBRUN) == IN_A_CHECKOUT
    assert runtime_paths.fleet_tool("pbrun.py", root=REPOSITORY) == IN_A_CHECKOUT


def test_a_runtime_with_no_pbrun_is_refused_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing launcher is said once, naming both places it was looked for.

    branch: without this, the same absence reached the operator as one child
    refusal per recovered record, each quoting the interpreter rather than the
    tool.
    """

    assert runtime_paths.fleet_tool("pbrun.py", root=tmp_path) is None
    monkeypatch.setattr(pool_reset, "RUNTIME_ROOT", tmp_path)
    plan = {"cwd": str(tmp_path), "argv": ["/usr/bin/true"], "tags": [],
            "demand": {}}

    with pytest.raises(SystemExit) as refused:
        pool_reset.submit_command(plan, transport="pool", pbrun=None)

    said = str(refused.value)
    assert "no pbrun.py" in said
    assert str(tmp_path / "tools" / "fleet" / "pbrun.py") in said
    assert str(tmp_path / "tools" / "pbrun.py") in said
