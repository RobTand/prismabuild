"""Each GB10 declares its local spool budget in the roster (#910).

#747 made a produced-output producer's spool window a ``spool_gb`` host
reservation, charged against the budget a box declares with
``worker_loop.py --spool-gb``.  No box passed the flag, so an opted-in
producer was queued and never claimed.  The budget is now declared the way
``--mem-gb`` is: in the box's ``args`` in ``fleet_boxes.json``.  The roster
also names the filesystem the budget is carved from (``local_disk``) and the
fleet's free floor (``local_disk_free_floor_percent``), and the supervisor
refuses a declaration the disk cannot cover.  A refusal drops the flag rather
than exiting, because the supervisor runs under ``Restart=always`` and an exit
would take every loop on the box with it.

These tests follow a declaration from the roster file through the
supervisor's arguments, the worker's declared kinds and its observed offer,
to a claim on a ``tmp_path`` queue.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

from prismabuild import box_capacity, pool
from prismabuild import produced_spool as ps

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import supervise  # noqa: E402
import worker_loop  # noqa: E402

GIB = 1 << 30
KIND = "spool_gb"
REPO = Path(__file__).resolve().parents[1]
ROSTER = REPO / "tools" / "fleet" / "fleet_boxes.json"
BASE_ARGS = ["--class", "gb10", "--gpu", "--mem-gb", "8", "--all-cores"]


class _Statvfs:
    """A filesystem of ``size_gib`` with ``free_gib`` available, counted."""

    def __init__(self, size_gib: int, free_gib: int) -> None:
        self.f_frsize = 4096
        self.f_blocks = size_gib * GIB // 4096
        self.f_bavail = free_gib * GIB // 4096
        self.calls: list[str] = []

    def __call__(self, path):
        self.calls.append(str(path))
        return self


def _refuse_statvfs(path):
    raise AssertionError(f"statvfs({path!r}) was read for a box with no --spool-gb")


@pytest.fixture(autouse=True)
def _fresh_verdicts(monkeypatch):
    # Verdicts are per process by design; each test is its own "start".
    monkeypatch.setattr(supervise, "_SPOOL_VERDICTS", {})


@pytest.fixture
def roster(tmp_path, monkeypatch):
    """Write a one-box roster and point the supervisor at it."""

    path = tmp_path / "fleet_boxes.json"
    monkeypatch.setattr(supervise, "CONFIG", path)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    # The local-disk check reads /proc/self/mountinfo, and a test's tmp_path
    # may sit on tmpfs.  Its refusal is pinned separately below.
    monkeypatch.setattr(ps, "_local_disk", lambda path: None)

    def write(*, args, local_disk=str(tmp_path), floor=5, extra=None):
        entry = {"loops": 2, "args": list(args)}
        if local_disk is not None:
            entry["local_disk"] = local_disk
        document = {"boxes": {"boxa": entry}}
        if floor is not None:
            document["local_disk_free_floor_percent"] = floor
        document.update(extra or {})
        path.write_text(json.dumps(document))
        return path

    return write


def _stat(monkeypatch, size_gib, free_gib):
    stat = _Statvfs(size_gib, free_gib)
    monkeypatch.setattr(supervise.os, "statvfs", stat)
    return stat


def _worker_offer(args):
    """What a worker started with ``args`` offers a claim, as the loop builds it."""

    parser = worker_loop.build_parser()
    parsed = parser.parse_args(args)
    worker_loop.validate_args(parser, parsed)
    declared = worker_loop.declared_host_capacity(parsed, cores=2)
    return box_capacity.observe(declared, {}, gpu_sample=None, mem_gb=None,
                                load1=None).capacity


def _producer(queue, key):
    """A HOST_WINDOW producer with R13's 32 GiB window, as pbrun derives it."""

    window = ps.host_window_terms({ps.HOST_WINDOW_ENV: "1",
                                   ps.MAX_ENV: str(32 * GIB)})
    assert window == {KIND: 32}
    resources = {"cpu": 1, "mem_gb": 1, **window}
    queue.publish(action_key=key, cas_root="/cas",
                  worker_script=str(REPO / "tools" / "prismabuild_worker.py"),
                  checkout_root=str(Path(queue.root).parent / "checkout"),
                  resources=resources)
    return resources


def _denials(queue):
    local = pool.cpu_admission.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    return list(pool.cpu_admission.read_json(local)["records"].values())


# -- off: a box that declares no spool budget -------------------------------


def test_a_box_without_spool_gb_keeps_its_args_byte_for_byte(roster, monkeypatch):
    roster(args=BASE_ARGS, local_disk=None, floor=None)
    monkeypatch.setattr(supervise.os, "statvfs", _refuse_statvfs)
    assert supervise.declared_shape("boxa", 0) == (2, BASE_ARGS)
    assert _worker_offer(BASE_ARGS) == {"mem_gb": 8, "cpu": 2}


def test_the_checked_in_boxes_without_spool_gb_read_exactly_their_file_args(
        tmp_path, monkeypatch):
    """dl380g10 declares no spool budget: its arguments are the file's."""

    monkeypatch.setattr(supervise, "CONFIG", ROSTER)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    monkeypatch.setattr(supervise.os, "statvfs", _refuse_statvfs)
    boxes = json.loads(ROSTER.read_text())["boxes"]
    assert "--spool-gb" not in boxes["dl380g10"]["args"]
    loops, args = supervise.declared_shape("dl380g10", 0)
    assert (loops, args) == (boxes["dl380g10"]["loops"],
                             [str(a) for a in boxes["dl380g10"]["args"]])


# -- on: the roster value reaches the worker and the claim -------------------


def test_a_covered_declaration_reaches_the_worker_and_its_producer_is_claimed(
        roster, monkeypatch, tmp_path):
    roster(args=[*BASE_ARGS, "--spool-gb", "32"])
    # 1000 GiB disk, 100 GiB free: the 5% floor is 50 GiB, leaving 50 GiB.
    stat = _stat(monkeypatch, 1000, 100)
    loops, args = supervise.declared_shape("boxa", 0)
    assert args == [*BASE_ARGS, "--spool-gb", "32"]
    assert stat.calls == [str(tmp_path)]
    offer = _worker_offer(args)
    assert offer == {"mem_gb": 8, "cpu": 2, KIND: 32}

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = "a" * 64
    _producer(queue, key)
    claimed = queue.claim(owner="w", capacity=offer)
    assert claimed is not None and claimed["action_key"] == key
    assert claimed["resources"][KIND] == 32
    assert queue.ledger().available().get(KIND, 0) == 0


def test_a_box_that_declares_too_little_leaves_the_producer_queued_with_a_reason(
        roster, monkeypatch, tmp_path):
    roster(args=[*BASE_ARGS, "--spool-gb", "16"])
    _stat(monkeypatch, 1000, 100)
    _, args = supervise.declared_shape("boxa", 0)
    offer = _worker_offer(args)
    assert offer[KIND] == 16

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = "b" * 64
    _producer(queue, key)
    assert queue.claim(owner="w", capacity=offer) is None
    assert (queue.dir(pool.READY) / f"{key}.json").exists()
    [denial] = _denials(queue)
    assert denial["reason"] == "never_fits_capacity"
    assert denial["evidence"]["demand"][KIND] == 32
    assert denial["evidence"]["capacity_total"][KIND] == 16
    # A box that names the smaller budget is also refused at submission.
    queue.announce(host="boxa", tags=["boxa"], has_gpu=False, capacity=offer)
    assert queue.placeable({"tags": [], "needs_gpu": False,
                            "resources": {"cpu": 1, "mem_gb": 1, KIND: 32}}) is False


def test_a_declaration_the_disk_cannot_cover_is_dropped_not_fatal(
        roster, monkeypatch, tmp_path, capsys):
    roster(args=[*BASE_ARGS, "--spool-gb", "32"])
    # 1000 GiB disk, 70 GiB free: 70 - 50 floor = 20 GiB of room for 32.
    _stat(monkeypatch, 1000, 70)
    loops, args = supervise.declared_shape("boxa", 0)
    assert (loops, args) == (2, BASE_ARGS)
    line = capsys.readouterr().out
    assert "--spool-gb declaration refused" in line
    assert str(32 * GIB) in line and str(20 * GIB) in line and "5% floor" in line
    offer = _worker_offer(args)
    assert KIND not in offer

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = "c" * 64
    _producer(queue, key)
    assert queue.claim(owner="w", capacity=offer) is None
    [denial] = _denials(queue)
    assert denial["reason"] == "never_fits_capacity"
    assert KIND not in denial["evidence"]["capacity_total"]


def test_the_boundary_is_free_minus_the_floor(roster, monkeypatch):
    roster(args=[*BASE_ARGS, "--spool-gb", "20"])
    _stat(monkeypatch, 1000, 70)                    # exactly 20 GiB of room
    assert supervise.declared_shape("boxa", 0)[1][-2:] == ["--spool-gb", "20"]


# -- every other refusal also drops only the flag ----------------------------


@pytest.mark.parametrize("case", [
    "no-local-disk", "relative-local-disk", "no-floor", "float-floor",
    "floor-100", "equals-form", "twice", "no-value", "not-a-number",
])
def test_an_unverifiable_declaration_is_dropped_with_its_reason(
        case, roster, monkeypatch, tmp_path, capsys):
    args = [*BASE_ARGS, "--spool-gb", "32"]
    kwargs = {}
    if case == "no-local-disk":
        kwargs["local_disk"] = None
    elif case == "relative-local-disk":
        kwargs["local_disk"] = "home/rob"
    elif case == "no-floor":
        kwargs["floor"] = None
    elif case == "float-floor":
        kwargs["floor"] = 5.0
    elif case == "floor-100":
        kwargs["floor"] = 100
    elif case == "equals-form":
        args = [*BASE_ARGS, "--spool-gb=32"]
    elif case == "twice":
        args = [*BASE_ARGS, "--spool-gb", "32", "--spool-gb", "32"]
    elif case == "no-value":
        args = [*BASE_ARGS, "--spool-gb"]
    elif case == "not-a-number":
        args = [*BASE_ARGS, "--spool-gb", "lots"]
    roster(args=args, **kwargs)
    _stat(monkeypatch, 1000, 900)                   # the disk is never the reason
    assert supervise.declared_shape("boxa", 0) == (2, BASE_ARGS)
    assert "--spool-gb declaration refused" in capsys.readouterr().out


def test_a_local_disk_that_is_not_a_local_filesystem_is_refused(
        roster, monkeypatch, capsys):
    roster(args=[*BASE_ARGS, "--spool-gb", "32"])
    _stat(monkeypatch, 1000, 900)

    def nfs(path):
        raise ps.SpoolError("spool root is not a known local disk filesystem")

    monkeypatch.setattr(ps, "_local_disk", nfs)
    assert supervise.declared_shape("boxa", 0) == (2, BASE_ARGS)
    assert "not a usable local disk" in capsys.readouterr().out


def test_an_unreadable_filesystem_is_refused(roster, monkeypatch, capsys):
    roster(args=[*BASE_ARGS, "--spool-gb", "32"])

    def stale(path):
        raise OSError(116, "Stale file handle")

    monkeypatch.setattr(supervise.os, "statvfs", stale)
    assert supervise.declared_shape("boxa", 0) == (2, BASE_ARGS)
    assert "cannot read free space" in capsys.readouterr().out


def test_a_zero_declaration_needs_no_disk(roster, monkeypatch):
    roster(args=[*BASE_ARGS, "--spool-gb", "0"], local_disk=None, floor=None)
    monkeypatch.setattr(supervise.os, "statvfs", _refuse_statvfs)
    args = supervise.declared_shape("boxa", 0)[1]
    assert args == [*BASE_ARGS, "--spool-gb", "0"]
    assert _worker_offer(args) == {"mem_gb": 8, "cpu": 2}


# -- checked once per start, not every tick ----------------------------------


def test_the_check_runs_once_per_declaration_not_every_tick(roster, monkeypatch):
    """A filling spool lowers free space; the offer must not flap with it."""

    roster(args=[*BASE_ARGS, "--spool-gb", "32"])
    stat = _stat(monkeypatch, 1000, 100)
    first = supervise.declared_shape("boxa", 0)
    stat.f_bavail = 60 * GIB // 4096                # the box's own producer spools
    for _ in range(3):
        assert supervise.declared_shape("boxa", 0, first) == first
    assert len(stat.calls) == 1
    # A new declaration is a new question.
    roster(args=[*BASE_ARGS, "--spool-gb", "8"])
    assert supervise.declared_shape("boxa", 0, first)[1][-2:] == ["--spool-gb", "8"]
    assert len(stat.calls) == 2
    # A new process -- a restart or a publish's re-exec -- asks again.
    monkeypatch.setattr(supervise, "_SPOOL_VERDICTS", {})
    roster(args=[*BASE_ARGS, "--spool-gb", "32"])
    assert supervise.declared_shape("boxa", 0)[1] == BASE_ARGS
    assert len(stat.calls) == 3


def test_room_is_f_bavail_minus_a_rounded_up_floor():
    stat = _Statvfs(1000, 100)
    room = supervise.local_disk_room("/x", 5, statvfs=stat)
    assert room == {"size_bytes": 1000 * GIB, "free_bytes": 100 * GIB,
                    "floor_bytes": 50 * GIB, "room_bytes": 50 * GIB}
    odd = _Statvfs(1, 1)
    odd.f_blocks, odd.f_frsize = 3, 1               # 5% of 3 B rounds up to 1 B
    assert supervise.local_disk_room("/x", 5, statvfs=odd)["floor_bytes"] == 1


# -- the checked-in roster ---------------------------------------------------


def test_both_gb10s_declare_a_spool_budget_on_a_named_local_disk():
    document = json.loads(ROSTER.read_text())
    assert document["local_disk_free_floor_percent"] == 5
    boxes = document["boxes"]
    for host in ("sparky", "gx10-6b77"):
        entry = boxes[host]
        args = [str(a) for a in entry["args"]]
        assert supervise._spool_gb_of(args) == 32
        assert os.path.isabs(entry["local_disk"])
    for host in ("dl380g10", "wsl-gpu"):
        assert "--spool-gb" not in boxes[host]["args"]
        assert "local_disk" not in boxes[host]


def test_sparklina_resolves_the_same_spool_budget_as_its_roster_key(
        tmp_path, monkeypatch):
    monkeypatch.setattr(supervise, "CONFIG", ROSTER)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    monkeypatch.setattr(ps, "_local_disk", lambda path: None)
    _stat(monkeypatch, 1000, 900)
    canonical = supervise.declared_shape("gx10-6b77", 0)
    assert supervise.declared_shape("sparklina", 0) == canonical
    assert canonical[1][canonical[1].index("--spool-gb") + 1] == "32"
