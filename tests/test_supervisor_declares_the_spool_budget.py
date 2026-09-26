"""Each GB10 declares its local disk budget in the roster (#910, #911).

#747 made a produced-output producer's spool window a ``spool_gb`` host
reservation, charged against the budget a box declares with
``worker_loop.py --spool-gb``.  No box passed the flag, so an opted-in
producer was queued and never claimed.  The budget is now declared the way
``--mem-gb`` is: in the box's ``args`` in ``fleet_boxes.json``.  The roster
also names the filesystem the budget is carved from (``local_disk``) and the
fleet's free floor (``local_disk_free_floor_percent``).

The supervisor measures the budget at start (#911): ``f_bavail`` minus the
floor, in whole GiB, capped when the roster gives a number instead of
``auto``.  That first value is the loops' argument for the life of the
process; every later tick measures again and records the value in a
host-local offer file, which each loop declares in place of its argument
(#1190).  It measures only while the host ledger shows no ``spool_gb``
held, because a holder's written bytes cannot be told from other use of the
disk; until then the offer stays what it was.  A refusal at start drops the
flag rather than exiting, because the supervisor runs under
``Restart=always`` and an exit would take every loop on the box with it.

These tests follow a declaration from the roster file through the
supervisor's arguments, the worker's declared kinds and its observed offer,
to a claim on a ``tmp_path`` queue.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
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
    monkeypatch.setattr(supervise, "_SPOOL_WAITING", {})
    monkeypatch.setattr(supervise, "_SPOOL_LIVE", {})


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


def _worker_offer(args, ledger=None):
    """What a worker started with ``args`` offers a claim, as the loop builds it.

    With ``ledger``, the offer includes the loop's per-poll read of the live
    ``spool_gb`` measurement the supervisor records for that ledger (#1190).
    """

    parser = worker_loop.build_parser()
    parsed = parser.parse_args(args)
    worker_loop.validate_args(parser, parsed)
    declared = worker_loop.declared_host_capacity(parsed, cores=2)
    if ledger is not None:
        declared = worker_loop.live_host_capacity(declared, ledger)
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


def test_a_number_the_disk_cannot_cover_is_lowered_to_the_room(
        roster, monkeypatch, tmp_path, capsys):
    roster(args=[*BASE_ARGS, "--spool-gb", "32"])
    # 1000 GiB disk, 70 GiB free: 70 - 50 floor = 20 GiB of room for 32.
    _stat(monkeypatch, 1000, 70)
    loops, args = supervise.declared_shape("boxa", 0)
    assert (loops, args) == (2, [*BASE_ARGS, "--spool-gb", "20"])
    line = capsys.readouterr().out
    assert "--spool-gb 32 measured 20 GiB" in line
    assert str(70 * GIB) in line and str(50 * GIB) in line and "(5%) floor" in line
    offer = _worker_offer(args)
    assert offer[KIND] == 20

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = "c" * 64
    _producer(queue, key)
    assert queue.claim(owner="w", capacity=offer) is None
    [denial] = _denials(queue)
    assert denial["reason"] == "never_fits_capacity"
    assert denial["evidence"]["capacity_total"][KIND] == 20


@pytest.mark.parametrize("free_gib", [50, 40])
def test_no_room_above_the_floor_drops_the_flag_not_the_box(
        free_gib, roster, monkeypatch, capsys):
    """At or under the floor the box offers no disk, and keeps its loops."""

    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    _stat(monkeypatch, 1000, free_gib)              # the floor is 50 GiB
    assert supervise.declared_shape("boxa", 0) == (2, BASE_ARGS)
    out = capsys.readouterr().out
    assert "--spool-gb declaration refused" in out and "measures 0 GiB" in out


def test_the_boundary_is_free_minus_the_floor(roster, monkeypatch):
    roster(args=[*BASE_ARGS, "--spool-gb", "20"])
    _stat(monkeypatch, 1000, 70)                    # exactly 20 GiB of room
    assert supervise.declared_shape("boxa", 0)[1][-2:] == ["--spool-gb", "20"]


# -- every other refusal also drops only the flag ----------------------------


@pytest.mark.parametrize("case", [
    "no-local-disk", "relative-local-disk", "no-floor", "float-floor",
    "floor-100", "equals-form", "twice", "no-value", "not-a-number",
    "auto-spelled-otherwise",
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
    elif case == "auto-spelled-otherwise":
        args = [*BASE_ARGS, "--spool-gb", "AUTO"]
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


# -- arguments settle once; the disk is measured every tick (#1190) ---------


def test_the_arguments_settle_once_and_the_disk_is_measured_every_tick(
        roster, monkeypatch, tmp_path):
    """The loops' arguments do not follow the disk; their offer does.

    Changing a loop's arguments cycles it, so they carry the first
    measurement for the life of the process.  With nothing held, each tick
    measures the disk again and the loops declare that value instead.
    """

    roster(args=[*BASE_ARGS, "--spool-gb", "32"])
    queue = _holder_queue(tmp_path, monkeypatch)
    stat = _stat(monkeypatch, 1000, 100)            # 50 GiB of room, capped at 32
    first = supervise.declared_shape("boxa", 0)
    assert first[1][-2:] == ["--spool-gb", "32"]
    assert _worker_offer(first[1], queue.ledger())[KIND] == 32
    stat.f_bavail = 60 * GIB // 4096                # another writer: 10 GiB of room
    for _ in range(3):
        assert supervise.declared_shape("boxa", 0, first) == first
    assert len(stat.calls) == 4
    assert _worker_offer(first[1], queue.ledger())[KIND] == 10
    # A new declaration is a new question, and its arguments change once.
    roster(args=[*BASE_ARGS, "--spool-gb", "8"])
    assert supervise.declared_shape("boxa", 0, first)[1][-2:] == ["--spool-gb", "8"]
    assert len(stat.calls) == 5
    # A new process -- a restart or a publish's re-exec -- starts its
    # arguments from what it measures: 10 GiB above the floor.
    monkeypatch.setattr(supervise, "_SPOOL_VERDICTS", {})
    roster(args=[*BASE_ARGS, "--spool-gb", "32"])
    assert supervise.declared_shape("boxa", 0)[1][-2:] == ["--spool-gb", "10"]


def test_a_holder_freezes_the_live_offer_until_it_is_released(
        roster, monkeypatch, tmp_path):
    """The live measurement never raises the offer under a held reservation.

    A 150 GiB holder runs on a 200 GiB offer.  Another writer frees 100 GiB
    while it runs: the disk now reads 300 GiB of room, but part of what the
    holder has not yet written is in it, so the tick does not read the disk
    and the offer stays 200.  A second 100 GiB holder is refused.  Once the
    first releases, the next tick measures, and the second is admitted.
    """

    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    queue = _holder_queue(tmp_path, monkeypatch)
    stat = _stat(monkeypatch, 1000, 250)            # 200 GiB of room
    shape = supervise.declared_shape("boxa", 0)
    first, second = "h" * 64, "i" * 64
    _scratch_holder(queue, first, 150)
    assert queue.claim(owner="w1", capacity=_worker_offer(
        shape[1], queue.ledger()))["action_key"] == first

    calls = len(stat.calls)
    stat.f_bavail = 350 * GIB // 4096
    for _ in range(2):
        assert supervise.declared_shape("boxa", 0, shape) == shape
    assert len(stat.calls) == calls                 # the disk was not read
    offer = _worker_offer(shape[1], queue.ledger())
    assert offer[KIND] == 200
    _scratch_holder(queue, second, 100)
    assert queue.claim(owner="w2", capacity=offer) is None

    queue.ledger().release(first)
    assert supervise.declared_shape("boxa", 0, shape) == shape
    assert len(stat.calls) == calls + 1
    offer = _worker_offer(shape[1], queue.ledger())
    assert offer[KIND] == 300
    assert queue.claim(owner="w2", capacity=offer)["action_key"] == second


@pytest.mark.parametrize("how", ["full", "unreadable"])
def test_a_disk_with_no_room_on_a_tick_offers_zero_and_keeps_the_flag(
        how, roster, monkeypatch, tmp_path):
    """A later tick with no room lowers the offer to 0; the loops keep running.

    Dropping the flag, as a first measurement of 0 does, would change the
    loops' arguments and cycle them.  The placeable declaration keeps the
    loop's starting value, so a submission is queued rather than refused
    while the disk is full for now.
    """

    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    queue = _holder_queue(tmp_path, monkeypatch)
    stat = _stat(monkeypatch, 1000, 250)            # 200 GiB of room
    shape = supervise.declared_shape("boxa", 0)
    if how == "full":
        stat.f_bavail = 40 * GIB // 4096            # under the 50 GiB floor
    else:
        def stale(path):
            raise OSError(116, "Stale file handle")

        monkeypatch.setattr(supervise.os, "statvfs", stale)
    assert supervise.declared_shape("boxa", 0, shape) == shape
    live = worker_loop.live_host_capacity({"mem_gb": 8, KIND: 200}, queue.ledger())
    assert live == {"mem_gb": 8, KIND: 0}
    assert _worker_offer(shape[1], queue.ledger())[KIND] == 0
    assert worker_loop.stable_host_capacity(
        live, {"mem_gb": 8, KIND: 200}) == {"mem_gb": 8, KIND: 200}


def test_the_placeable_declaration_rises_with_a_freed_disk():
    started = {"mem_gb": 8, "cpu": 2, KIND: 241}
    assert worker_loop.stable_host_capacity(
        {"mem_gb": 8, "cpu": 2, KIND: 302}, started)[KIND] == 302
    assert worker_loop.stable_host_capacity(
        {"mem_gb": 8, "cpu": 2}, {"mem_gb": 8, "cpu": 2}) == {"mem_gb": 8, "cpu": 2}


def test_a_missing_live_offer_is_capped_at_the_ledger_total(
        roster, monkeypatch, tmp_path):
    """A missing offer file is no information, and it does not raise the offer.

    The loops' 200 GiB argument was measured at start; the disk has since
    filled and the ledger was retired to 40.  With the file gone, declaring
    the argument would mint 160 GiB the disk does not have.
    """

    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    queue = _holder_queue(tmp_path, monkeypatch)
    stat = _stat(monkeypatch, 1000, 250)            # 200 GiB of room
    shape = supervise.declared_shape("boxa", 0)
    ledger = queue.ledger()
    ledger.ensure_capacity(_worker_offer(shape[1], ledger))
    stat.f_bavail = 90 * GIB // 4096                # 40 GiB of room
    supervise.declared_shape("boxa", 0, shape)
    offer = _worker_offer(shape[1], ledger)
    assert offer[KIND] == 40
    ledger.retire_free_capacity(offer)

    from prismabuild import local_scratch

    local_scratch.clear_spool_offer(ledger.base)
    assert local_scratch.read_spool_offer(ledger.base) is None
    assert _worker_offer(shape[1], ledger)[KIND] == 40
    # A ledger that has never held the kind gives no room either.
    fresh = pool.PoolQueue(tmp_path / "other-queue").ledger("boxa")
    assert _worker_offer(shape[1], fresh)[KIND] == 0


def test_a_first_measurement_that_waits_removes_an_earlier_offer(
        roster, monkeypatch, tmp_path):
    """A value this process did not measure is not left for the loops."""

    from prismabuild import local_scratch

    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    queue = _holder_queue(tmp_path, monkeypatch)
    ledger = queue.ledger()
    local_scratch.write_spool_offer(ledger.base, 500, "an earlier process")
    ledger.ensure_capacity({KIND: 64})
    ledger.acquire("j" * 64, {KIND: 8})
    monkeypatch.setattr(supervise.os, "statvfs", _refuse_statvfs)
    args = supervise.declared_shape("boxa", 0)[1]
    assert args[-2:] == ["--spool-gb", "64"]
    assert local_scratch.read_spool_offer(ledger.base) is None
    assert _worker_offer(args, ledger)[KIND] == 64


def test_only_the_owning_supervisor_writes_the_live_offer(
        roster, monkeypatch, tmp_path):
    """A start before the ownership claim, or a one-shot, leaves the offer alone.

    ``_run_supervisor`` reads the shape before it knows it owns the box, and
    ``--cycle-stale --once`` reads it without ever owning it.  Neither may
    write or remove the file the owner's loops read.
    """

    from prismabuild import local_scratch

    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    queue = _holder_queue(tmp_path, monkeypatch)
    ledger = queue.ledger()
    stat = _stat(monkeypatch, 1000, 250)            # 200 GiB of room
    shape = supervise.declared_shape("boxa", 0, record=False)
    assert shape[1][-2:] == ["--spool-gb", "200"]
    assert local_scratch.read_spool_offer(ledger.base) is None
    calls = len(stat.calls)
    stat.f_bavail = 350 * GIB // 4096
    assert supervise.declared_shape("boxa", 0, shape, record=False) == shape
    assert len(stat.calls) == calls
    assert local_scratch.read_spool_offer(ledger.base) is None
    # The owner's first tick measures and records.
    assert supervise.declared_shape("boxa", 0, shape) == shape
    assert local_scratch.read_spool_offer(ledger.base) == 300


def test_an_owner_whose_first_tick_waits_removes_an_earlier_offer(
        roster, monkeypatch, tmp_path):
    """Settled before the claim, then a holder arrived: the old file goes."""

    from prismabuild import local_scratch

    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    queue = _holder_queue(tmp_path, monkeypatch)
    ledger = queue.ledger()
    local_scratch.write_spool_offer(ledger.base, 500, "an earlier process")
    _stat(monkeypatch, 1000, 250)                   # 200 GiB of room
    shape = supervise.declared_shape("boxa", 0, record=False)
    assert local_scratch.read_spool_offer(ledger.base) == 500
    ledger.ensure_capacity({KIND: 200})
    ledger.acquire("k" * 64, {KIND: 8})
    assert supervise.declared_shape("boxa", 0, shape) == shape
    assert local_scratch.read_spool_offer(ledger.base) is None
    assert _worker_offer(shape[1], ledger)[KIND] == 200


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
        # Measured, not written down: no number in the file can go stale as
        # the disk fills or is freed.
        assert supervise._spool_gb_of(args) is None
        assert args[args.index("--spool-gb") + 1] == "auto"
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
    # 900 GiB free on a 1000 GiB disk, less the 50 GiB floor.
    assert canonical[1][canonical[1].index("--spool-gb") + 1] == "850"


# -- auto: the budget is measured, never written down (#911) -----------------


def test_auto_offers_the_whole_room_in_whole_gib(roster, monkeypatch, capsys):
    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    stat = _stat(monkeypatch, 1000, 100)
    # 100 GiB and 99 blocks free: the room is 50 GiB plus a fraction.
    stat.f_bavail += 99
    assert supervise.declared_shape("boxa", 0)[1] == [*BASE_ARGS, "--spool-gb", "50"]
    assert "--spool-gb auto measured 50 GiB" in capsys.readouterr().out
    assert _worker_offer([*BASE_ARGS, "--spool-gb", "50"])[KIND] == 50


def test_a_number_caps_the_measured_room(roster, monkeypatch):
    roster(args=[*BASE_ARGS, "--spool-gb", "40"])
    _stat(monkeypatch, 1000, 500)                   # 450 GiB of room
    assert supervise.declared_shape("boxa", 0)[1][-2:] == ["--spool-gb", "40"]


def _holder_queue(tmp_path, monkeypatch):
    """A queue at the supervisor's own root, claiming as host ``boxa``."""

    monkeypatch.setattr(supervise, "MIRROR", tmp_path)
    monkeypatch.setattr(socket, "gethostname", lambda: "boxa")
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


def _scratch_holder(queue, key, gib):
    queue.publish(action_key=key, cas_root="/cas",
                  worker_script=str(REPO / "tools" / "prismabuild_worker.py"),
                  checkout_root=str(Path(queue.root).parent / "checkout"),
                  resources={"cpu": 1, "mem_gb": 1, KIND: gib})


def test_a_running_holder_is_neither_charged_twice_nor_credited_twice(
        roster, monkeypatch, tmp_path, capsys):
    """The measurement waits for the box to hold no disk, and the ledger keeps its total.

    A 200 GiB box admits one 166 GiB scratch holder, which writes 10 GiB.
    The supervisor then restarts.  Reading the disk now gives 190 GiB of
    room: offering that would charge the holder's 10 written GiB a second
    time, and adding the holder's 166 GiB back (the mem_gb clamp) would
    offer 356, letting a second 166 GiB holder in beside the first -- 332
    GiB promised on a disk with 200 above its floor.  The loops keep the
    ledger's 200 instead, the second holder is refused, and the disk is
    measured again once nothing is held.
    """

    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    queue = _holder_queue(tmp_path, monkeypatch)
    stat = _stat(monkeypatch, 1000, 250)            # 200 GiB above the floor
    args = supervise.declared_shape("boxa", 0)[1]
    assert args[-2:] == ["--spool-gb", "200"]
    offer = _worker_offer(args)
    first, second = "d" * 64, "e" * 64
    _scratch_holder(queue, first, 166)
    assert queue.claim(owner="w1", capacity=offer)["action_key"] == first

    stat.f_bavail = 240 * GIB // 4096               # the holder wrote 10 GiB
    monkeypatch.setattr(supervise, "_SPOOL_VERDICTS", {})   # a restart
    calls = len(stat.calls)
    args = supervise.declared_shape("boxa", 0)[1]
    assert args[-2:] == ["--spool-gb", "200"]
    assert len(stat.calls) == calls                 # the disk was not read
    out = capsys.readouterr().out
    assert "measurement waits" in out and "hold 166 GiB" in out
    assert "current 200 GiB" in out

    _scratch_holder(queue, second, 166)
    assert _worker_offer(args, queue.ledger())[KIND] == 200
    assert queue.claim(owner="w2", capacity=_worker_offer(args, queue.ledger())) is None
    assert queue.ledger().capacity()[KIND] == 200
    assert (queue.dir(pool.READY) / f"{second}.json").exists()

    # Still waiting on the next tick; the line is not repeated.
    assert supervise.declared_shape("boxa", 0)[1][-2:] == ["--spool-gb", "200"]
    assert "measurement waits" not in capsys.readouterr().out

    # The holder finishes and removes its scratch; the next tick measures.
    queue.ledger().release(first)
    stat.f_bavail = 250 * GIB // 4096
    assert supervise.declared_shape("boxa", 0)[1][-2:] == ["--spool-gb", "200"]
    assert len(stat.calls) == calls + 1
    assert "--spool-gb auto measured 200 GiB" in capsys.readouterr().out


def test_freed_disk_reaches_the_offer_on_the_next_tick(roster, monkeypatch, tmp_path):
    """A disk freed while nothing is held reaches the loops' offer (#1190).

    sparky, 2026-09-26: its loops were started with 241 GiB above the floor.
    Two agents then freed their scratch, leaving 352 GB free, and a 250 GiB
    measurement row was still refused on the start-time 241. With nothing
    held, the next supervisor tick measures the disk again, and the loops
    declare what it found on their next poll. The row is admitted without a
    restart or a publish. The loops' arguments do not change, so no loop is
    cycled for it.
    """

    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    queue = _holder_queue(tmp_path, monkeypatch)
    stat = _stat(monkeypatch, 1000, 291)            # 241 GiB above the floor
    shape = supervise.declared_shape("boxa", 0)
    assert shape[1][-2:] == ["--spool-gb", "241"]
    row = "a" * 64
    _scratch_holder(queue, row, 250)
    assert queue.claim(owner="w1", capacity=_worker_offer(shape[1], queue.ledger())) is None

    stat.f_bavail = 352 * GIB // 4096               # the scratch is pruned
    assert supervise.declared_shape("boxa", 0, shape) == shape
    offer = _worker_offer(shape[1], queue.ledger())
    assert offer[KIND] == 302
    assert queue.claim(owner="w1", capacity=offer)["action_key"] == row


def test_a_holder_that_claims_during_the_reading_defers_it(
        roster, monkeypatch, tmp_path):
    """Held is read on both sides of statvfs."""

    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    queue = _holder_queue(tmp_path, monkeypatch)
    queue.ledger().ensure_capacity({KIND: 64})
    stat = _Statvfs(1000, 250)

    def claim_then_read(path):
        queue.ledger().acquire("f" * 64, {KIND: 8})
        return stat(path)

    monkeypatch.setattr(supervise.os, "statvfs", claim_then_read)
    args = supervise.declared_shape("boxa", 0)[1]
    assert args[-2:] == ["--spool-gb", "64"]        # the ledger's total
    assert supervise._SPOOL_VERDICTS == {}          # nothing settled


def test_a_waiting_number_still_caps_the_ledger_total(
        roster, monkeypatch, tmp_path):
    roster(args=[*BASE_ARGS, "--spool-gb", "40"])
    queue = _holder_queue(tmp_path, monkeypatch)
    queue.ledger().ensure_capacity({KIND: 200})
    queue.ledger().acquire("g" * 64, {KIND: 8})
    monkeypatch.setattr(supervise.os, "statvfs", _refuse_statvfs)
    assert supervise.declared_shape("boxa", 0)[1][-2:] == ["--spool-gb", "40"]


def test_an_unreadable_ledger_waits_rather_than_measuring(
        roster, monkeypatch, tmp_path, capsys):
    roster(args=[*BASE_ARGS, "--spool-gb", "auto"])
    monkeypatch.setattr(supervise, "MIRROR", tmp_path)

    def unreadable(ledger):
        raise PermissionError(13, "Permission denied", str(ledger.held_dir))

    monkeypatch.setattr(supervise, "_held_spool", unreadable)
    monkeypatch.setattr(supervise.os, "statvfs", _refuse_statvfs)
    # No ledger total to fall back on either: the loops offer no disk for now.
    assert supervise.declared_shape("boxa", 0)[1] == BASE_ARGS
    assert "cannot read the host ledger" in capsys.readouterr().out
    assert supervise._SPOOL_VERDICTS == {}


def test_a_held_census_that_cannot_list_a_holder_raises(tmp_path, monkeypatch):
    """``Path.glob`` hides EACCES as an empty listing; this census must not."""

    if os.geteuid() == 0:
        pytest.skip("root reads a mode-000 directory, so there is no EACCES to see")
    monkeypatch.setattr(socket, "gethostname", lambda: "boxa")
    ledger = pool.PoolQueue(tmp_path / "pb-queue").ledger()
    ledger.ensure_capacity({KIND: 8})
    assert ledger.acquire("h" * 64, {KIND: 8})
    assert supervise._held_spool(ledger) == 8
    holder = ledger.held_dir / ("h" * 64)
    holder.chmod(0)
    try:
        with pytest.raises(PermissionError):
            supervise._held_spool(ledger)
    finally:
        holder.chmod(0o755)
