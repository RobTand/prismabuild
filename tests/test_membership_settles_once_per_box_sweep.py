"""Membership settlement reads withdrawn/ once per box sweep, not every poll.

``fleet_membership.resume_owed`` finds this host's owed handoffs by reading
every ``withdrawn/*.json``: 4,092 records on 2026-10-20.  Every worker-loop
poll ran it, open gate or drain, and dl380g10 runs dozens of loops.  The
loops now settle membership in the box's sweep turn (``_sweep_due``: one
host-local marker shared by every loop and generation on the box, at most
once per ``HEARTBEAT_S``), the turn ``serve_once`` reaps in and a drain
retries its saved finishes in.  ``resign`` still settles on every round of
its own, so nothing waits on the loops' cadence.

Each poll here is the real ``worker_loop._run_loop`` with ``--once``, loaded
afresh as a separate loop would be, and listings of ``withdrawn/`` are
counted with the audit hook from
``test_a_waiting_client_reads_its_ending_by_name``.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Event

import pytest

from prismabuild import adaptive_cpu, pool
from prismabuild.record_store import WITHDRAWN

import test_a_waiting_client_reads_its_ending_by_name as waiting
from test_a_draining_worker_stays_visible import DRAIN_GATE, _load_loop

OPEN_GATE = {"draining": False, "changed_unix": 124.0}
OTHERS = 200


@pytest.fixture()
def queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> pool.PoolQueue:
    """A queue whose withdrawn/ holds other actions' operator withdrawals."""

    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmp_path / "box-state")
    made = pool.PoolQueue(tmp_path / "pb-queue")
    made.records.ensure_layout()
    for index in range(OTHERS):
        key = f"{index:064x}"
        made.records.item_path(WITHDRAWN, key).write_text(json.dumps({
            "schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": key,
            "status": "withdrawn", "published_unix": 1.0,
            "withdrawn_by": "rob", "withdrawn_host": "elsewhere",
        }), encoding="utf-8")
    return made


@pytest.fixture()
def withdrawn_listings(queue: pool.PoolQueue, tmp_path: Path):
    """How many times anything has listed withdrawn/ so far."""

    log = tmp_path / "withdrawn-listings.log"
    directory = queue.records.dir(WITHDRAWN)
    waiting._WATCHED = frozenset({os.path.normpath(os.path.abspath(directory)),
                                  os.path.realpath(directory)})
    waiting._LOG = str(log)
    try:
        yield lambda: (len(log.read_text(encoding="utf-8").splitlines())
                       if log.exists() else 0)
    finally:
        waiting._WATCHED, waiting._LOG = frozenset(), None


def _poll(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, gate) -> None:
    assert _load_loop(tmp_path, monkeypatch, gate)._run_loop(Event()) == 0


def test_loops_polling_inside_one_heartbeat_read_withdrawn_once(
    queue, withdrawn_listings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Four polls by four loops, open and drained: one census between them."""

    for gate in (OPEN_GATE, OPEN_GATE, DRAIN_GATE, DRAIN_GATE):
        _poll(tmp_path, monkeypatch, gate)

    assert withdrawn_listings() == 1


@pytest.mark.parametrize("gate", [OPEN_GATE, DRAIN_GATE], ids=["open", "drain"])
def test_the_next_heartbeat_settles_again(
    queue, withdrawn_listings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    gate,
) -> None:
    """The cadence delays settlement by at most one heartbeat; it never stops it."""

    _poll(tmp_path, monkeypatch, gate)
    assert withdrawn_listings() == 1
    marker = queue._sweep_marker()
    earlier = pool._now() - pool.HEARTBEAT_S - 1
    os.utime(marker, (earlier, earlier))
    _poll(tmp_path, monkeypatch, gate)

    assert withdrawn_listings() == 2
