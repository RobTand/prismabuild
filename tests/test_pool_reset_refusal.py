"""A re-submission that refused must not read as work in flight.

``pool_reset --apply`` started each child ``pbrun`` with its output on
``/dev/null``, printed ``submitted`` from the fact that a process had been
created, stamped the failed record as re-submitted, and returned 0. A child
that refuses -- a closure that no longer matches, a checkout that is gone --
left the operator with a green run, a record claiming the work had been
re-submitted, and nothing queued anywhere.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pool_reset  # noqa: E402


KEY = "b" * 64

REFUSES = """import sys
sys.stderr.write("pbrun: live code closure differs from the action-pinned closure\\n")
raise SystemExit(2)
"""

WAITS = """import time
time.sleep(10)
"""


@pytest.fixture()
def fleet(tmp_path: Path) -> dict:
    """One pull-queue failure that ``pool_reset`` can recover and re-submit."""

    queue_root = tmp_path / "pb-queue"
    cas_root = tmp_path / "cas"
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    request = cas_root / "requests" / KEY[:2] / f"{KEY}.json"
    request.parent.mkdir(parents=True, exist_ok=True)
    request.write_text(json.dumps({
        "action_key": KEY,
        "params": {"command": ["/usr/bin/true"]},
        "task": {"working_directory": "."},
    }), encoding="utf-8")

    queue = pool.PoolQueue(queue_root)
    queue.ensure_layout()
    queue.publish(
        action_key=KEY,
        cas_root=str(cas_root),
        checkout_root=str(checkout),
        worker_script=str(tmp_path / "worker.py"),
        max_attempts=1,
    )
    assert queue.claim() is not None
    failed = queue.finish(
        KEY, status="failed",
        detail={"returncode": 1, "stdout": "", "stderr": "boom\n"},
    )
    assert failed == queue.item_path(pool.FAILED, KEY)
    return {"queue": queue, "queue_root": queue_root, "cas_root": cas_root,
            "failed": failed, "tmp_path": tmp_path}


def _run(fleet: dict, monkeypatch: pytest.MonkeyPatch, script: str) -> int:
    """``--apply`` against a stand-in ``pbrun`` that does what ``script`` says.

    The stand-in is bound through ``submit_command`` rather than through the
    module constant, because the constant is read at function-definition time
    and the real ``pbrun`` must never be started from a test.
    """

    fake = fleet["tmp_path"] / "fake_pbrun.py"
    fake.write_text(script, encoding="utf-8")
    built = pool_reset.submit_command

    def _with_the_stand_in(plan, **kwargs):
        kwargs.pop("pbrun", None)
        return built(plan, pbrun=fake, **kwargs)

    monkeypatch.setattr(pool_reset, "submit_command", _with_the_stand_in)
    monkeypatch.setattr(pool_reset, "REFUSAL_WINDOW_S", 2.0, raising=False)
    return pool_reset.main([
        "--apply", "--transport", "pool",
        "--queue-root", str(fleet["queue_root"]),
        "--cas-root", str(fleet["cas_root"]),
    ])


def test_a_child_that_refuses_is_reported_and_the_record_stands(
    fleet: dict, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """The operator hears the refusal, and the work is still resettable.

    main: ``main`` returns non-zero and prints what the child said.
    branch: the ending stays ``failed``, so the next run plans it again rather
    than skipping a record that says it was re-submitted.
    """

    code = _run(fleet, monkeypatch, REFUSES)
    printed = capsys.readouterr().out

    assert code != 0
    assert "refused" in printed
    assert "live code closure differs" in printed

    record = json.loads(fleet["failed"].read_text(encoding="utf-8"))
    assert record["status"] == "failed"
    assert "reset" not in record


def test_a_child_still_running_counts_as_submitted(
    fleet: dict, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """A submission is not refused merely because it has not returned.

    main: ``main`` returns 0 and the ending is stamped ``reset``.
    branch: the child's output is kept where an operator can read it, rather
    than sent to ``/dev/null``.
    """

    code = _run(fleet, monkeypatch, WAITS)
    printed = capsys.readouterr().out

    assert code == 0
    assert "submitted" in printed

    record = json.loads(fleet["failed"].read_text(encoding="utf-8"))
    assert record["status"] == "reset"

    logs = sorted((fleet["queue_root"] / "resets").glob(f"{KEY}*"))
    assert len(logs) == 1
