"""A box's host-memory margin is a roster flag, not a module constant (#980).

A worker loop's live memory offer is ``min(--mem-gb, held + MemAvailable -
margin)``.  The margin was ``box_capacity.MEMORY_MARGIN_GB`` (8) on every
box: ``observe`` and ``CapacityObserver`` always took ``margin_gb``, but no
caller passed it.  Sparky's MemAvailable is about 110 GiB, because the agent
sessions run there, so it offered 102 GiB and a 104 GiB job never fit.

The arithmetic existed; the plumbing did not.  ``--mem-margin-gb`` now
travels from the roster ``args`` through the supervisor to the loop's
observer, and the join qualification reads the same flag.
"""
from __future__ import annotations

import os
from pathlib import Path
import socket
import sys

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tools" / "fleet"))

from prismabuild import box_capacity as bc  # noqa: E402
import fleet_membership as fm  # noqa: E402
import supervise  # noqa: E402
import worker_loop  # noqa: E402
from test_fleet_membership_busy_resign import (  # noqa: E402
    _busy_queue, _busy_roster, _busy_runtime, _healthy_broker, _incarnation,
)


def test_the_margin_decides_whether_104_fits_at_110_available() -> None:
    """The observer's own arithmetic, which main already had."""

    assert bc.observe({"mem_gb": 104}, {}, margin_gb=4, mem_gb=110,
                      load1=0).capacity == {"mem_gb": 104}
    assert bc.observe({"mem_gb": 104}, {}, mem_gb=110,
                      load1=0).capacity == {"mem_gb": 102}


def test_the_worker_loop_takes_the_flag() -> None:
    """Red on main: the loop refused ``--mem-margin-gb`` as unrecognized."""

    ap = worker_loop.build_parser()
    args = ap.parse_args(["--mem-margin-gb", "4"])
    worker_loop.validate_args(ap, args)
    assert args.mem_margin_gb == 4
    assert ap.parse_args([]).mem_margin_gb == bc.MEMORY_MARGIN_GB
    with pytest.raises(SystemExit):
        worker_loop.validate_args(ap, ap.parse_args(["--mem-margin-gb", "-1"]))


def _roster_args(monkeypatch, tmp_path: Path, host: str) -> list[str]:
    """The loop argv the supervisor builds for ``host`` from the checked-in roster."""

    monkeypatch.setattr(supervise, "CONFIG", REPO / "tools/fleet/fleet_boxes.json")
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    # sparky measures its --spool-gb auto from the disk; hold the reading still.
    reading = os.statvfs("/")
    monkeypatch.setattr(supervise.os, "statvfs", lambda path: reading)
    monkeypatch.setattr(supervise, "_SPOOL_VERDICTS", {})
    return supervise.declared_shape(host, 0)[1]


def _published_offer(args, mem_available: int) -> dict[str, int]:
    observer = worker_loop.capacity_observer(args, gpu_capable=True,
                                             ledger_total={})
    assert observer is not None
    for _ in range(observer.samples):
        offer = observer.offer({"mem_gb": args.mem_gb}, {},
                               mem_gb=mem_available, load1=0)
    return offer


@pytest.mark.parametrize("host", ["sparky", "sparklina"])
def test_a_spark_offers_its_104_from_the_supervisors_argv(
        monkeypatch, tmp_path: Path, host: str) -> None:
    argv = _roster_args(monkeypatch, tmp_path, host)
    args = worker_loop.build_parser().parse_args(argv)

    assert (args.mem_gb, args.mem_margin_gb) == (104, 4)
    assert _published_offer(args, 110) == {"mem_gb": 104}
    assert _published_offer(args, 106) == {"mem_gb": 102}


def test_the_file_server_keeps_the_default_margin(monkeypatch, tmp_path: Path) -> None:
    argv = _roster_args(monkeypatch, tmp_path, "dl380g10")
    args = worker_loop.build_parser().parse_args(argv)

    assert "--mem-margin-gb" not in argv
    assert args.mem_margin_gb == bc.MEMORY_MARGIN_GB
    assert _published_offer(args, 100) == {"mem_gb": 92}


def test_the_join_qualification_reads_the_same_margin(
        tmp_path: Path, monkeypatch) -> None:
    host = socket.gethostname()
    _incarnation(monkeypatch)
    roster = _busy_roster(tmp_path, host, [
        "--class", "x86", "--mem-gb", "96", "--mem-margin-gb", "4"], monkeypatch)
    checks = fm.qualify_host(host, queue_root=_busy_queue(monkeypatch, tmp_path),
                             roster_path=roster, broker_call=_healthy_broker(),
                             runtime_root=_busy_runtime(tmp_path),
                             held={}, mem_gb=10)

    assert checks["ok"] is True, checks
    assert checks["checks"]["offer"]["capacity"]["mem_gb"] == 6
