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
import subprocess
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

ACK = {"schema": pool_reset.pbrun.DETACH_SCHEMA_V1, "action_key": "c" * 64,
       "transport": "pool", "status": "submitted", "published_unix": 123.0}
ACCEPTS = "import json; print(" + repr(json.dumps(ACK)) + ")\n"


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


@pytest.fixture()
def reaped(monkeypatch: pytest.MonkeyPatch):
    """Reap precisely the children this test starts, even after an assertion."""

    started: list[subprocess.Popen] = []
    begin = pool_reset.start_resubmission

    def _collected(*args, **kwargs):
        process, log = begin(*args, **kwargs)
        started.append(process)
        return process, log

    monkeypatch.setattr(pool_reset, "start_resubmission", _collected)
    yield started
    for process in started:
        if process.poll() is not None:
            continue
        process.kill()
        process.wait()


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
    monkeypatch.setattr(pool_reset, "POLL_S", 0.01)
    monkeypatch.setattr(pool_reset, "PROGRESS_INTERVAL_S", 0.02)
    return pool_reset.main([
        "--apply", "--transport", "pool",
        "--queue-root", str(fleet["queue_root"]),
        "--cas-root", str(fleet["cas_root"]),
    ])


def test_a_child_that_refuses_is_reported_and_the_record_stands(
    fleet: dict, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture,
    reaped: list,
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


def test_a_slow_submission_waits_for_its_acknowledgement(
    fleet, monkeypatch, capsys, reaped,
):
    code = _run(fleet, monkeypatch, "import time; time.sleep(0.1)\n" + ACCEPTS)
    printed = capsys.readouterr().out
    assert code == 0
    assert "still waiting" in printed
    record = json.loads(fleet["failed"].read_text())
    assert record["status"] == "reset"
    assert record["reset"]["submission"] == ACK
    assert all(p.poll() == 0 for p in reaped)


def test_a_slow_refusal_leaves_the_record_failed(fleet, monkeypatch, capsys, reaped):
    code = _run(fleet, monkeypatch, "import time; time.sleep(0.1)\n" + REFUSES)
    assert code == 1
    printed = capsys.readouterr().out
    assert "still waiting" in printed
    assert "submitted " not in printed
    assert json.loads(fleet["failed"].read_text())["status"] == "failed"
    assert all(p.poll() == 2 for p in reaped)


@pytest.mark.parametrize("reply", [None, {}, {**ACK, "status": "failed"},
    {**ACK, "action_key": ""}, {**ACK, "transport": "unknown"}])
def test_success_without_valid_acknowledgement_is_unclear(
    fleet, monkeypatch, capsys, reaped, reply,
):
    script = "pass" if reply is None else "print(" + repr(json.dumps(reply)) + ")"
    assert _run(fleet, monkeypatch, script) == 1
    assert "unclear" in capsys.readouterr().out
    assert json.loads(fleet["failed"].read_text())["status"] == "failed"


def test_noisy_submission_has_bounded_log_and_keeps_acknowledgement(
    fleet, monkeypatch, capsys, reaped,
):
    limit = pool_reset.LOG_LIMIT_BYTES
    script = "import sys; sys.stderr.write('x' * " + str(limit * 4) + " + '\\n')\n"
    assert _run(fleet, monkeypatch, script + ACCEPTS) == 0
    logs = list((fleet["queue_root"] / "resets").glob("*.log"))
    assert len(logs) == 1
    assert logs[0].stat().st_size <= limit
    assert pool_reset.announced_submission(logs[0].read_text()) == ACK


def test_resubmission_uses_detach(fleet):
    plans, _ = pool_reset.plan_resets(fleet["queue"], cas_root=fleet["cas_root"])
    assert "--detach" in pool_reset.submit_command(plans[0], transport="pool")
