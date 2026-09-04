"""A box busy with work the pool did not schedule must look busy to the pool.

Measured on the live fleet 2026-09-04 01:06.  sparklina published
``--gpu-slots 1 --mem-gb 40``; the ledger agreed with that offer exactly and
reported every token free::

    gx10-6b77 offer {'cpu': 10, 'gpu': 1, 'mem_gb': 40}  ledger free 51  held 0

    $ nvidia-smi --query-compute-apps=pid,used_memory --format=csv
    794915, 1145 MiB     805673, 1543 MiB
    831002, 1477 MiB     1242245, 1321 MiB

Four GPU processes, six hours into a campaign started by
``ssh sparklina 'setsid nohup ...'``.  The accounting was exactly right about
what the *pool* had scheduled and exactly blind to everything else, so one GPU
action placed there would have stacked on top -- the shape of the 2026-09-03
sparklina OOM.

These drive a real ``worker_loop.main()`` against a private pool root, with the
box's readings supplied rather than read, so the assertions are about the
loop's behaviour and not about whatever this machine happens to be doing.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import socket
import sys
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import box_capacity, pool  # noqa: E402

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"

#: The four out-of-pool encodes, as ``nvidia-smi`` reported them.
FOREIGN = [(794915, 1145), (805673, 1543), (831002, 1477), (1242245, 1321)]

SPARKLINA = ["--class", "gb10", "--tag", "sparklina", "--gpu-slots", "1",
             "--mem-gb", "40", "--cpu-slots", "10", "--all-cores",
             "--poll-s", "0"]


def _worker_loop():
    spec = importlib.util.spec_from_file_location("wl_under_test", WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(tmp_path: Path, argv: list[str], *, apps=(), mem_gb=100, load1=0.0):
    """One worker start against a private pool root, with the box's readings given."""

    wl = _worker_loop()
    with mock.patch.object(wl, "SH", tmp_path), \
         mock.patch.object(wl.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(wl, "published_commit", return_value="deadbeef"), \
         mock.patch.object(box_capacity, "gpu_compute_apps",
                           return_value=None if apps is None else list(apps)), \
         mock.patch.object(box_capacity, "mem_available_gb", return_value=mem_gb), \
         mock.patch.object(box_capacity, "run_queue", return_value=load1), \
         mock.patch.object(sys, "argv", ["worker_loop.py", *argv]):
        assert wl.main() == 0
    return pool.PoolQueue(tmp_path / "pb-queue")


def _offer(queue: pool.PoolQueue, host: str) -> dict:
    return json.loads((queue.root / "workers" / f"{host}.json").read_text())


def test_the_gpu_the_pool_did_not_schedule_comes_off_the_ledger(
    tmp_path: Path,
) -> None:
    host = socket.gethostname()

    queue = _run(tmp_path, [*SPARKLINA, "--max-idle", "3"], apps=FOREIGN)

    # The card is occupied, so the box holds no free GPU token to admit against.
    assert queue.ledger(host).capacity().get("gpu", 0) == 0
    assert queue.ledger(host).available().get("gpu", 0) == 0


def test_the_offer_file_says_what_fell_and_why(tmp_path: Path) -> None:
    host = socket.gethostname()

    queue = _run(tmp_path, [*SPARKLINA, "--max-idle", "3"], apps=FOREIGN,
                 mem_gb=100, load1=4.72)
    offer = _offer(queue, host)

    # The declaration is what the box CAN do, and is what a submitter is
    # answered from; the observation is what it can do now.
    assert offer["capacity"] == {"cpu": 10, "gpu": 1, "mem_gb": 40}
    assert offer["observed_capacity"]["gpu"] == 0
    assert offer["foreign"]["gpu"] == 4
    assert offer["observed_detail"]["gpu_compute_apps"] == 4
    assert offer["observed_detail"]["gpu_used_mib"] == 5486


def test_a_busy_box_is_a_slow_submission_not_a_refused_one(tmp_path: Path) -> None:
    """``pbrun`` raises ``SystemExit`` when ``placeable`` answers ``False``.

    So the live figure must not be what ``placeable`` reads: a GPU box fully
    occupied by someone else's encode would refuse every GPU submission on the
    fleet with "no live worker can run this action.  Fix the --tag", which is
    both wrong and unactionable.  The item should queue and wait.
    """

    queue = _run(tmp_path, [*SPARKLINA, "--max-idle", "3"], apps=FOREIGN)

    assert queue.placeable({"tags": ["sparklina"], "resources": {"gpu": 1}}) is True


def test_one_bad_reading_does_not_starve_the_box(tmp_path: Path) -> None:
    """A retire outlives the loop that made it for the length of an action.

    A worker spends the whole of an action inside ``serve_once`` -- up to two
    hours -- and does not poll while it is there, so a single unlucky reading
    taken just before one would starve the box until it finished.
    """

    host = socket.gethostname()

    queue = _run(tmp_path, [*SPARKLINA, "--max-idle", "1"], apps=FOREIGN)

    assert queue.ledger(host).capacity()["gpu"] == 1


def test_the_offer_recovers_when_the_foreign_work_exits(tmp_path: Path) -> None:
    """Out-of-pool work ends without telling anyone; the box must notice."""

    host = socket.gethostname()
    _run(tmp_path, [*SPARKLINA, "--max-idle", "3"], apps=FOREIGN)
    assert pool.PoolQueue(tmp_path / "pb-queue").ledger(host).capacity().get("gpu", 0) == 0

    queue = _run(tmp_path, [*SPARKLINA, "--max-idle", "1"], apps=[])

    assert queue.ledger(host).capacity()["gpu"] == 1
    assert queue.ledger(host).available()["gpu"] == 1


def test_a_running_action_keeps_what_it_is_executing_under(tmp_path: Path) -> None:
    """The retire is blunt, so it must stay safe under the pool's own work.

    And the action's own GPU process must not be counted against the box a
    second time: on this fleet it is a docker container's child, invisible to
    any ancestry walk, which is why the pool's own consumption is read from the
    ledger instead.
    """

    host = socket.gethostname()
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.ledger(host).ensure_capacity({"gpu": 1, "mem_gb": 40, "cpu": 10})
    assert queue.ledger(host).acquire("b" * 64, {"gpu": 1, "mem_gb": 16}) is True

    # One compute app, and the pool holds one GPU token: nothing foreign.
    queue = _run(tmp_path, [*SPARKLINA, "--max-idle", "3"],
                 apps=[(261917, 34765)], mem_gb=100)

    ledger = queue.ledger(host)
    assert ledger.held_keys() == ["b" * 64]
    assert ledger.capacity()["gpu"] == 1
    assert ledger.capacity()["mem_gb"] == 40
    assert _offer(queue, host)["foreign"] == {}


def test_memory_is_clamped_to_what_the_box_actually_has(tmp_path: Path) -> None:
    host = socket.gethostname()

    queue = _run(tmp_path, [*SPARKLINA, "--max-idle", "3"], apps=[], mem_gb=20)

    assert queue.ledger(host).capacity()["mem_gb"] == 12    # 20 free, 8 margin


def test_assume_idle_offers_the_declaration_unobserved(tmp_path: Path) -> None:
    """The debug path: no box is asked what else is running on it."""

    host = socket.gethostname()

    def refuse():
        raise AssertionError("--assume-idle must not read the box")

    wl = _worker_loop()
    with mock.patch.object(wl, "SH", tmp_path), \
         mock.patch.object(wl.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(wl, "published_commit", return_value="deadbeef"), \
         mock.patch.object(box_capacity, "gpu_compute_apps", side_effect=refuse), \
         mock.patch.object(box_capacity, "mem_available_gb", side_effect=refuse), \
         mock.patch.object(box_capacity, "run_queue", side_effect=refuse), \
         mock.patch.object(sys, "argv", ["worker_loop.py", *SPARKLINA,
                                         "--max-idle", "3", "--assume-idle"]):
        assert wl.main() == 0

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    assert queue.ledger(host).capacity()["gpu"] == 1
    assert _offer(queue, host)["observed_capacity"] == {
        "cpu": 10, "gpu": 1, "mem_gb": 40}


def test_an_unreadable_box_keeps_its_declaration(tmp_path: Path) -> None:
    """A wedged nvidia-smi is a missing reading, not an idle GPU nor a busy one."""

    host = socket.gethostname()

    queue = _run(tmp_path, [*SPARKLINA, "--max-idle", "3"], apps=None,
                 mem_gb=None, load1=None)

    assert queue.ledger(host).capacity()["gpu"] == 1
    assert _offer(queue, host)["foreign"] == {}


def test_a_loop_that_starts_on_a_busy_box_does_not_re_mint(tmp_path: Path) -> None:
    """The restart must inherit the box's verdict, not re-assert the declaration.

    A loop exits on ``--max-idle`` and the supervisor replaces it, so on a box
    running three to five of them one restarts every half hour or so.  If a
    starting loop primed its window with what it was *declared*, it would offer
    the declaration for the length of that window and ``ensure_capacity`` would
    re-mint every free token the other loops had retired -- and ``acquire``
    reads the free directory, not any loop's window, so any loop on the box
    could then take one.  That is this whole blindness, reopened on a timer.
    """

    host = socket.gethostname()
    _run(tmp_path, [*SPARKLINA, "--max-idle", "3"], apps=FOREIGN)
    assert pool.PoolQueue(tmp_path / "pb-queue").ledger(host).capacity().get("gpu", 0) == 0

    # A fresh loop, one poll, the foreign work still there.  One poll is the
    # worst case: the window is at its most optimistic on the first reading.
    queue = _run(tmp_path, [*SPARKLINA, "--max-idle", "1"], apps=FOREIGN)

    assert queue.ledger(host).capacity().get("gpu", 0) == 0
    assert queue.ledger(host).available().get("gpu", 0) == 0
    assert _offer(queue, host)["observed_capacity"]["gpu"] == 0


def test_a_first_start_on_an_unknown_host_still_offers_its_declaration(tmp_path: Path) -> None:
    """An empty ledger means "never seen", which is not the same as zero.

    The seed is read from the ledger's standing total, so it must distinguish a
    host it has never heard of -- which has no verdict to inherit -- from one
    whose gpu total it has already retired to zero.
    """

    host = socket.gethostname()
    queue = _run(tmp_path, [*SPARKLINA, "--max-idle", "1"], apps=[], mem_gb=100)

    assert queue.ledger(host).capacity()["gpu"] == 1
    assert _offer(queue, host)["observed_capacity"]["gpu"] == 1
