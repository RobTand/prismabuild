"""A withdrawal reports the decision, not the last failure before it.

``withdraw`` copies the live record into ``withdrawn/``, and a record a requeue
has touched carries the previous attempt's ``detail``: its returncode, its
stdout and its stderr. Under ``status: withdrawn`` that detail describes an
attempt that is not why the work stopped. ``pbrun`` writes the stderr to the
operator's terminal and only then says who withdrew it, and ``pbstatus`` shows
the failed attempt's returncode on the withdrawn row.

The two ``attempt_history`` keys were renamed for the same reason in PR #52.
This is the third field of that copy that describes an ending the record does
not have.
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
import pbrun  # noqa: E402

KEY = "f" * 64


@pytest.fixture()
def retried(tmp_path: Path) -> pool.PoolQueue:
    """One action that failed once, was requeued, and is claimed again."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.publish(
        action_key=KEY,
        cas_root=str(tmp_path / "cas"),
        checkout_root=str(tmp_path / "checkout"),
        worker_script=str(tmp_path / "worker.py"),
        max_attempts=2,
        retry_safe=True,
    )
    item = queue.claim()
    assert item is not None
    requeued = queue.finish(
        KEY, status="failed",
        detail={"returncode": 3, "stdout": "", "stderr": "boom\n"},
        claim_snapshot=item,
    )
    assert requeued == queue.item_path(pool.READY, KEY)
    assert queue.claim() is not None
    return queue


def test_the_withdrawn_record_does_not_carry_the_failed_attempts_detail(
    retried: pool.PoolQueue,
) -> None:
    """The evidence is kept, under a name that says what it is.

    main: ``detail`` is gone, so no reader adopts a returncode and a stderr the
    withdrawal did not produce.
    branch: the failed attempt's own detail is still there under
    ``detail_before_withdrawal``, because a cancellation is not a reason to
    lose what the run before it said.
    """

    retried.withdraw(KEY, by="rob", reason="not needed", signal_child=False)
    filed = json.loads(
        retried.item_path(pool.WITHDRAWN, KEY).read_text(encoding="utf-8")
    )

    assert filed["status"] == "withdrawn"
    assert "detail" not in filed
    assert filed["detail_before_withdrawal"]["returncode"] == 3
    assert filed["detail_before_withdrawal"]["stderr"] == "boom\n"


def test_pbrun_does_not_print_the_failed_attempts_stderr_for_a_withdrawal(
    retried: pool.PoolQueue, capsys: pytest.CaptureFixture
) -> None:
    """What the operator sees, which is where the defect was reported.

    main: the cancellation is reported and the earlier attempt's stderr is not
    written to the terminal as though it were the reason.
    """

    retried.withdraw(KEY, by="rob", reason="not needed", signal_child=False)

    code = pbrun.await_outcome(retried, KEY, wait_s=1.0)
    printed = capsys.readouterr()

    assert code == pbrun.WITHDRAWN_EXIT
    assert "withdrawn by rob" in printed.err
    assert "boom" not in printed.err
    assert "boom" not in printed.out
