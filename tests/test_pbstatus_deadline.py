"""pbstatus returns at a deadline instead of joining a blocked mount read.

The defect (#350): on 2026-09-07 fifteen ``pbstatus`` processes sat 33-49
minutes each in ``__nfs_lookup_revalidate`` on sparky.  They were ``hard``
mount waits, not driver wedges, and every one of them eventually returned the
right answer -- long after the operator had needed it, and after inflating the
box's load average to ~14.7 at 0.0% CPU for the whole window.

These tests block the queue-root read rather than a real mount: the read path
is monkeypatched with a sleep well past the deadline, which reproduces the
property that matters (the caller cannot bound this read in-process) without
needing a sick NFS server.  The branch where ``SIGKILL`` does not collect the
child is reached the same way, by injecting the failure rather than the fault:
with ``os.kill`` made a no-op the parent takes its retained-child path exactly
as it would over a task in ``D``, and what the retained child still owns can
then be read straight out of ``/proc``.  Proved below: the caller returns on
time, says what it could not read, exits distinctly, reaps the child it can
reap, and hands an abandoned one none of the caller's descriptors.
"""
import json
from pathlib import Path
import os
import signal
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))
from prismabuild import pool
import pbstatus

#: What the run must exit with when the queue root did not answer.  Spelled
#: literally so this file states the contract rather than reading it back out
#: of the module under test.
EXIT_INCOMPLETE = 3

#: Longer than any deadline under test, so the read cannot finish on its own.
BLOCKED_S = 30.0


@pytest.fixture
def blocked_queue_root(tmp_path, monkeypatch):
    """A queue root whose pool census never returns inside a deadline."""
    monkeypatch.setenv("PRISMABUILD_TRANSPORT", "pool")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()

    def never(*args, **kwargs):
        time.sleep(BLOCKED_S)
        return {"nodes": [], "jobs": [], "notes": [],
                "queue": {"ready": 0, "claimed": 0, "empty": True,
                          "complete": True, "live_workers": 0,
                          "sampled_unix": time.time()}}

    monkeypatch.setattr(pbstatus, "read_pool", never)
    monkeypatch.setattr(pbstatus, "read_endings", never)
    return queue


def test_a_blocked_queue_root_returns_at_the_default_deadline(
        blocked_queue_root, capsys):
    """The whole run is bounded by default, with no flag from the caller."""
    started = time.monotonic()
    code = pbstatus.main(["--json", "--queue-root", str(blocked_queue_root.root)])
    elapsed = time.monotonic() - started

    assert elapsed < BLOCKED_S - 10, (
        f"pbstatus joined the blocked read for {elapsed:.1f}s; a diagnostic "
        "that waits for a hard mount is the defect in #350")
    assert code == EXIT_INCOMPLETE
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["complete"] is False
    assert "pool" in result["timed_out_sections"]
    assert "pbstatus: incomplete -- queue root did not answer within" in captured.err
    assert "pending)" in captured.err


def test_the_deadline_is_the_whole_run_not_each_section(
        blocked_queue_root, capsys):
    """A second blocked section does not buy itself another full deadline."""
    started = time.monotonic()
    code = pbstatus.main(["--json", "--timeout-s", "2", "--recent", "5",
                          "--queue-root", str(blocked_queue_root.root)])
    elapsed = time.monotonic() - started
    assert code == EXIT_INCOMPLETE
    assert elapsed < 8, f"two blocked sections took {elapsed:.1f}s against a 2s budget"
    result = json.loads(capsys.readouterr().out)
    assert result["timed_out_sections"] == ["pool", "endings"]


def test_text_mode_marks_the_partial_output_rather_than_printing_an_empty_fleet(
        blocked_queue_root, capsys):
    """An operator must not read a truncated census as a quiet queue."""
    code = pbstatus.main(["--timeout-s", "1", "--queue-root",
                          str(blocked_queue_root.root)])
    captured = capsys.readouterr()
    assert code == EXIT_INCOMPLETE
    assert "pool: incomplete -- queue root did not answer" in captured.out
    assert "endings: incomplete -- queue root did not answer" in captured.out
    assert "no jobs ready or claimed" not in captured.out
    assert "no endings filed" not in captured.out


def test_a_killable_blocked_child_is_reaped_and_not_left_behind(
        blocked_queue_root, capsys):
    """The deadline abandons only what it cannot reap.

    A child asleep in ``time.sleep`` answers ``SIGKILL``, so this run must
    leave nothing behind.  An empty ``abandoned_children`` is the proof and not
    merely the absence of one: the list is written only when ``waitpid`` has
    failed to collect the child inside the grace, so empty means the reader was
    reaped, not that nobody looked.  A child in uninterruptible sleep would be
    retained instead; that branch is covered by
    ``test_an_abandoned_child_owns_no_inherited_descriptor`` below, which
    injects a failed termination instead of waiting for a sick mount to
    supply one.
    """
    assert pbstatus.main(["--json", "--timeout-s", "1", "--queue-root",
                          str(blocked_queue_root.root)]) == EXIT_INCOMPLETE
    result = json.loads(capsys.readouterr().out)
    assert result["timed_out_sections"], "the fixture must have blocked a read"
    assert result["abandoned_children"] == []


def test_an_abandoned_child_owns_no_inherited_descriptor(tmp_path, monkeypatch):
    """What a retained reader still holds, read out of its ``/proc`` entry.

    The review of #358 reproduced the two consequences -- a wrapper left
    blocked on captured output, and a caller's ``flock`` still held after the
    caller closed it.  This asserts the cause underneath both: a descriptor is
    not a private copy, so an abandoned child that kept the caller's table
    keeps the caller's resources alive.  After the fix its table is
    ``/dev/null`` on the three standard streams and its own answer pipe, and
    nothing else.

    Termination is made to fail on purpose.  A child in ``time.sleep`` answers
    ``SIGKILL`` and would be reaped, and a child in ``D`` cannot be produced
    here; a no-op ``os.kill`` puts the parent on its retained-child path
    without needing either.
    """
    inherited = os.open(tmp_path / "caller.txt", os.O_CREAT | os.O_RDWR, 0o600)
    abandoned: list[dict] = []
    real_kill = os.kill
    monkeypatch.setattr(pbstatus.os, "kill", lambda pid, sig: None)
    try:
        result = pbstatus.bounded("pool", lambda: time.sleep(BLOCKED_S),
                                  deadline=pbstatus.Deadline(0.05),
                                  abandoned=abandoned)
        assert result["status"] == "timed_out"
        assert len(abandoned) == 1, "a no-op kill must leave the child retained"
        pid = abandoned[0]["pid"]
        held = {}
        for name in os.listdir(f"/proc/{pid}/fd"):
            try:
                held[int(name)] = os.readlink(f"/proc/{pid}/fd/{name}")
            except OSError:                        # closed under the listing
                continue
        assert {held.get(fd) for fd in (0, 1, 2)} == {os.devnull}, held
        other = {fd: link for fd, link in held.items() if fd > 2}
        assert len(other) == 1, f"the child kept more than its own pipe: {other}"
        assert next(iter(other.values())).startswith("pipe:"), other
        assert not any(str(tmp_path) in link for link in held.values()), held
    finally:
        os.close(inherited)
        for child in abandoned:
            real_kill(child["pid"], signal.SIGKILL)
            os.waitpid(child["pid"], 0)


def test_timeout_zero_restores_the_unbounded_in_process_read(tmp_path, monkeypatch, capsys):
    """``--timeout-s 0`` is the pre-#350 path: no deadline and no child.

    Proved by a side effect the parent can see.  A forked reader's writes to
    this list would die with the child, so an empty list here would mean the
    read had been moved off the caller's process even when the caller asked
    for the old behaviour.
    """
    monkeypatch.setenv("PRISMABUILD_TRANSPORT", "pool")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    ran: list[str] = []

    def census(*args, **kwargs):
        ran.append("pool")
        return {"nodes": [], "jobs": [], "notes": [],
                "queue": {"ready": 0, "claimed": 0, "empty": True,
                          "complete": True, "live_workers": 0,
                          "sampled_unix": time.time()}}

    monkeypatch.setattr(pbstatus, "read_pool", census)
    code = pbstatus.main(["--json", "--timeout-s", "0", "--recent", "0",
                          "--queue-root", str(queue.root)])
    assert code == 0
    assert ran == ["pool"], "the unbounded path must run in the caller's process"
    result = json.loads(capsys.readouterr().out)
    assert result["complete"] is True and result["timed_out_sections"] == []


def test_a_healthy_run_is_complete_and_exits_zero(tmp_path, monkeypatch, capsys):
    """The added keys do not change the verdict on a queue that answers."""
    monkeypatch.setenv("PRISMABUILD_TRANSPORT", "pool")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    code = pbstatus.main(["--json", "--recent", "0", "--queue-root", str(queue.root)])
    assert code == 0
    result = json.loads(capsys.readouterr().out)
    assert result["complete"] is True
    assert result["timed_out_sections"] == [] and result["abandoned_children"] == []
    assert result["pool"]["empty"] is True


def test_a_negative_timeout_is_refused(capsys):
    with pytest.raises(SystemExit) as raised:
        pbstatus.main(["--timeout-s", "-1"])
    assert raised.value.code == 2
    assert "--timeout-s cannot be negative" in capsys.readouterr().err


def _fake_proc(root: Path, entries, uptime_s: float = 10_000.0) -> Path:
    """A ``/proc`` with the two fields the peer scan reads.

    ``stat`` is written with a command name holding a space and a bracket,
    because that is the field the real parser has to split around.
    """
    root.mkdir(parents=True, exist_ok=True)
    (root / "uptime").write_text(f"{uptime_s} {uptime_s}\n")
    for pid, state, argv, starttime_ticks in entries:
        directory = root / str(pid)
        directory.mkdir()
        fields = ["0"] * 50
        fields[0] = state
        fields[19] = str(starttime_ticks)        # field 22, zero-based from 3
        (directory / "stat").write_text(
            f"{pid} (python3 (x)) " + " ".join(fields) + "\n")
        (directory / "cmdline").write_bytes(b"\0".join(argv) + b"\0")
    return root


def test_wedged_peers_counts_only_uninterruptible_pbstatus_processes(tmp_path):
    ticks = os.sysconf("SC_CLK_TCK")
    proc = _fake_proc(tmp_path / "proc", [
        (11, "D", [b"python3", b"/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbstatus.py",
                   b"--json"], 100 * ticks),
        (12, "D", [b"python3", b"/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbstatus.py"],
         9000 * ticks),
        # Uninterruptible, but somebody else's process.
        (13, "D", [b"python3", b"/home/rob/prismabuild/tools/fleet/pbrun.py"], 100 * ticks),
        # A pytest run whose command line merely names the test file: matching
        # on a substring rather than a whole path component would count this.
        (14, "D", [b"pytest", b"tests/test_pbstatus_pool.py"], 100 * ticks),
        # This command, running normally.
        (15, "S", [b"python3", b"tools/fleet/pbstatus.py"], 100 * ticks),
    ])
    result = pbstatus.wedged_peers(proc=proc, self_pid=999)
    assert [peer["pid"] for peer in result["peers"]] == [11, 12]
    # Oldest first: pid 11 started 100 ticks-seconds into a 10000 s uptime.
    assert result["peers"][0]["age_s"] == pytest.approx(9900.0)
    assert result["peers"][1]["age_s"] == pytest.approx(1000.0)
    assert result["truncated"] is False


def test_the_peer_scan_excludes_this_process_and_bounds_itself(tmp_path):
    ticks = os.sysconf("SC_CLK_TCK")
    entries = [(pid, "D", [b"python3", b"tools/fleet/pbstatus.py"], 100 * ticks)
               for pid in range(20, 40)]
    proc = _fake_proc(tmp_path / "proc", entries)
    assert 20 not in [peer["pid"] for peer in
                      pbstatus.wedged_peers(proc=proc, self_pid=20)["peers"]]
    limited = pbstatus.wedged_peers(proc=proc, self_pid=999, limit=5)
    assert limited["scanned"] == 5 and limited["truncated"] is True


def test_wedged_peers_are_warned_about_on_stderr_without_refusing_to_run(
        tmp_path, monkeypatch, capsys):
    """The count is the signal; the refusal would hide the fleet."""
    monkeypatch.setenv("PRISMABUILD_TRANSPORT", "pool")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    monkeypatch.setattr(pbstatus, "wedged_peers",
                        lambda **kw: {"peers": [{"pid": 3427158, "age_s": 2961.0}],
                                      "scanned": 3, "truncated": False, "note": None})
    code = pbstatus.main(["--json", "--recent", "0", "--queue-root", str(queue.root)])
    captured = capsys.readouterr()
    assert code == 0, "a wedged peer is a warning, never a refusal to report"
    assert "1 other pbstatus process" in captured.err
    assert "uninterruptible sleep" in captured.err
    assert "2961s" in captured.err and "3427158" in captured.err
    assert json.loads(captured.out)["complete"] is True


def test_a_truncated_peer_scan_says_its_count_is_a_floor(
        tmp_path, monkeypatch, capsys):
    """Fifteen corpses reported as three is the same lie as an empty listing.

    The scan is bounded by a PID count and a wall clock, so it can stop
    before the end of ``/proc``.  When it does, the warning has to say that
    the number it printed is a lower bound; an operator who reads "1" and
    believes it is the whole answer is the reader #350 exists to protect.
    """
    monkeypatch.setenv("PRISMABUILD_TRANSPORT", "pool")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    monkeypatch.setattr(pbstatus, "wedged_peers",
                        lambda **kw: {"peers": [{"pid": 3427158, "age_s": 2961.0}],
                                      "scanned": 5, "truncated": True, "note": None})
    code = pbstatus.main(["--json", "--recent", "0", "--queue-root", str(queue.root)])
    captured = capsys.readouterr()
    assert code == 0
    assert "scan stopped after 5 pids" in captured.err
    assert "floor" in captured.err
