"""Placement notices read the intent the placement verdict reads.

On 2026-11-06 a ``--progress-cycle`` submission whose command named an
absolute Python was refused fleet-wide: "no eligible worker offers
progress-cycle-v1; cyclic progress support is unknown".  Every live offer
carried the cyclic tag and answered no interpreter paths (``interpreters:
[]``).  The placement verdict drops an unknown interpreter from its probe,
because a first submission of any path is unknown by construction and the
claim-time stat guards each box (#1266).  The progress census, the pin width
and the ceiling warnings were asked of the original intent, which still
required the path, so they matched no offer.  The cyclic census refused; the
others went silent.  An interpreter that every eligible offer names absent is
still refused.
"""
from __future__ import annotations

import json
import sys

import pytest

from prismabuild import capability_eligibility, contracts
from prismabuild import core as pb
from prismabuild import pool

from pbrun_detach_support import _checkout, _one_json_line, _run_pbrun

PYTHON = sys.executable
CYCLE = ("--progress-cycle", "--progress", "encode=200", "--progress", "publish=5")


def _offer(queue, host="sparky", *, absent=(), ceiling=7200):
    """A live offer in the 2026-11-06 shape: capable, no path answered."""

    queue.announce(
        host=host,
        tags=["x86", host, contracts.INTERPRETER_TAG, contracts.PROGRESS_TAG,
              contracts.PROGRESS_HELPER_TAG, contracts.PROGRESS_CYCLE_TAG],
        has_gpu=False,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 0},
        timeout_ceiling_s=ceiling,
        progress_contracts=[pb.PROGRESS_RECORD_SCHEMA_V1],
        capabilities=capability_eligibility.OfferCapabilities(
            interpreters=capability_eligibility.InterpreterAnswers.from_observed(
                [], None)),
    )
    record_path = queue.root / "workers" / f"{host}.json"
    record = json.loads(record_path.read_text())
    assert record["interpreters"] == []
    if absent:
        record["interpreters_absent"] = sorted(absent)
        record_path.write_text(json.dumps(record))


def _submit(tmp_path, monkeypatch, *options):
    work = _checkout(tmp_path)
    return _run_pbrun(tmp_path, monkeypatch, work, "--detach", *options,
                      command=(PYTHON, "-c", "print(1)"))


def test_cyclic_progress_publishes_beside_an_unknown_interpreter(
        tmp_path, monkeypatch, capsys):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    _offer(queue)

    assert _submit(tmp_path, monkeypatch, *CYCLE) == 0

    captured = capsys.readouterr()
    assert "no worker has answered for the interpreter" in captured.err
    assert "cyclic phases" in captured.err
    item = json.loads(open(_one_json_line(captured)["submission"]).read())
    assert item["interpreter"] == PYTHON
    assert contracts.PROGRESS_CYCLE_TAG in item["tags"]
    assert contracts.INTERPRETER_TAG in item["tags"]


def test_an_interpreter_every_offer_names_absent_is_still_refused(
        tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    _offer(queue, absent=[PYTHON])

    with pytest.raises(SystemExit):
        _submit(tmp_path, monkeypatch, *CYCLE)

    assert queue.ready_items() == []


def test_the_ceiling_warning_survives_an_unknown_interpreter(
        tmp_path, monkeypatch, capsys):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    _offer(queue, ceiling=7200)

    assert _submit(tmp_path, monkeypatch, "--timeout-s", "9000") == 0

    assert "exceeds the payload execution ceiling" in capsys.readouterr().err
