"""Lock files of terminal keys are retired, and a racing holder keeps exclusion (#995).

``transition-locks/`` held 46,506 files against 37,962 terminal keys, and
``residency/`` held 363 empty consumer directories out of 429: nothing ever
removed either.  ``pb_gc --queue-root`` now retires both.

A lock file cannot simply be unlinked.  A process that opened the old inode
before the unlink and locks it afterwards holds a lock on an inode no name
reaches; the next process creates a new inode at the name and locks that.
Both proceed.  ``test_a_waiter_on_a_retired_inode_does_not_share_the_lock``
builds exactly that interleaving with real processes, and on main both
holders get in.  With the fix, the retirer tombstones the inode before it
unlinks it, and ``posix_lock.held`` lets go of a tombstoned inode and opens
the name again.
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import time


from prismabuild import pool, posix_lock
import pb_gc

SRC = Path(__file__).resolve().parents[1] / "src"
ENV = {**os.environ, "PYTHONPATH": str(SRC)}


def _key(tag: str) -> str:
    return hashlib.sha256(tag.encode()).hexdigest()


def _probe(path: Path) -> bool:
    """Whether a fresh process can take the lock without waiting."""

    result = subprocess.run(
        [sys.executable, "-c", "from pathlib import Path; import sys; "
         "from prismabuild.posix_lock import held; "
         "exec('with held(Path(sys.argv[1]), blocking=False) as ok:\\n print(ok)')",
         str(path)],
        env=ENV, capture_output=True, text=True, timeout=30, check=True)
    return result.stdout.strip() == "True"


def _blocked_on(pid: int, path: Path) -> bool:
    """Whether ``pid`` is queued behind a POSIX lock on ``path``'s inode."""

    inode = os.stat(path).st_ino
    for line in Path("/proc/locks").read_text().splitlines():
        fields = line.split()
        if "->" in fields and "POSIX" in fields:
            rest = fields[fields.index("->") + 1:]
            # ``POSIX ADVISORY WRITE <pid> <maj:min:inode> <start> <end>``
            if len(rest) >= 5 and rest[3] == str(pid) and rest[4].endswith(f":{inode}"):
                return True
    return False


def test_a_waiter_on_a_retired_inode_does_not_share_the_lock(tmp_path):
    """The retirer holds the lock, a waiter queues, the name is retired.

    The waiter is granted the old inode when the retirer lets go.  A second
    process then takes the name.  Only one of them may hold the lock.
    """

    path = tmp_path / "transition-locks" / "key.lock"
    path.parent.mkdir()
    retirer = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    import fcntl
    fcntl.lockf(retirer, fcntl.LOCK_EX)
    waiter = subprocess.Popen(
        [sys.executable, "-c", "from pathlib import Path; import sys; "
         "from prismabuild.posix_lock import held; "
         "exec('with held(Path(sys.argv[1])):\\n print(\"in\", flush=True)\\n "
         "sys.stdin.readline()')", str(path)],
        env=ENV, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 30
        while not _blocked_on(waiter.pid, path):
            assert time.monotonic() < deadline, "the waiter never queued on the lock"
            time.sleep(0.02)
        # The retirement, under the lock: the protocol's tombstone, then the
        # unlink, then the release.
        os.pwrite(retirer, getattr(posix_lock, "TOMBSTONE", b"retired\n"), 0)
        os.fsync(retirer)
        os.unlink(path)
        fcntl.lockf(retirer, fcntl.LOCK_UN)
        os.close(retirer)
        assert waiter.stdout.readline().strip() == "in"
        # The waiter believes it holds the lock.  A newcomer must not get in.
        assert _probe(path) is False, (
            "a newcomer took the lock while the waiter held it: the waiter "
            "was holding a retired inode")
    finally:
        waiter.stdin.write("\n")
        waiter.stdin.flush()
        waiter.wait(timeout=30)


def test_a_retirement_between_open_and_lock_is_noticed(tmp_path, monkeypatch):
    """The seam: the name is retired after ``held`` opened it, before it locked."""

    path = tmp_path / "key.lock"
    path.touch()
    real = posix_lock._lockf
    calls = []
    retiring = []

    def retire_then_lock(descriptor, blocking):
        if retiring:
            # The retirer's own lock, taken inside ``retire`` below.
            return real(descriptor, blocking)
        if not calls:
            retiring.append(True)
            assert posix_lock.retire(path) == ""
            retiring.clear()
        calls.append(os.fstat(descriptor).st_ino)
        return real(descriptor, blocking)

    monkeypatch.setattr(posix_lock, "_lockf", retire_then_lock)
    with posix_lock.held(path) as ok:
        assert ok
        assert len(calls) == 2, "held must open the name again after a retirement"
        assert calls[-1] == os.stat(path).st_ino
        assert _probe(path) is False
    assert _probe(path) is True


def test_retire_keeps_a_held_lock_and_retires_a_free_one(tmp_path):
    path = tmp_path / "key.lock"
    with posix_lock.held(path):
        assert posix_lock.retire(path) == "held by this thread"
    child = subprocess.Popen(
        [sys.executable, "-c", "from pathlib import Path; import sys,time; "
         "from prismabuild.posix_lock import held; "
         "exec('with held(Path(sys.argv[1])):\\n print(\"ready\", flush=True)\\n "
         "time.sleep(30)')", str(path)],
        env=ENV, stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "ready"
        assert posix_lock.retire(path) == "held"
        assert path.exists()
    finally:
        child.kill()
        child.wait(timeout=10)
    assert posix_lock.retire(path) == ""
    assert not path.exists()
    assert posix_lock.retire(path) == "absent"


def _cut_off_retirement(path: Path, monkeypatch) -> None:
    """Run the real ``retire`` and fail its ``unlink``: the tombstone stays linked.

    That is the residue of a retirer killed between its tombstone and its
    unlink, or of an unlink the server refused.
    """

    def refused(*_args, **_kwargs):
        raise OSError(5, "unlink refused")

    with monkeypatch.context() as patch:
        patch.setattr(posix_lock.os, "unlink", refused)
        assert posix_lock.retire(path).startswith("cannot retire")
    assert path.read_bytes() == posix_lock.TOMBSTONE
    assert os.stat(path).st_nlink == 1


def test_a_retirement_cut_off_before_its_unlink_does_not_wedge_the_lock(tmp_path, monkeypatch):
    """A holder finishes a tombstoned inode the name still names, then takes a fresh one.

    Before the fix every opener of the name met the same tombstoned inode,
    let it go and opened it again, without end.
    """

    path = tmp_path / "key.lock"
    with posix_lock.held(path):
        pass
    _cut_off_retirement(path, monkeypatch)
    assert _probe(path)
    assert path.read_bytes() == b""


def test_retire_finishes_a_retirement_cut_off_before_its_unlink(tmp_path, monkeypatch):
    path = tmp_path / "key.lock"
    with posix_lock.held(path):
        pass
    _cut_off_retirement(path, monkeypatch)
    assert posix_lock.retire(path) == ""
    assert not path.exists()


# --------------------------------------------------------------------------
# pb_gc --queue-root
# --------------------------------------------------------------------------


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


def _file(queue: pool.PoolQueue, state: str, key: str, *, age_s: float) -> None:
    path = queue.item_path(state, key)
    path.write_text(json.dumps({"action_key": key, "published_unix": 1.0}))
    stamp = time.time() - age_s
    os.utime(path, (stamp, stamp))


def _lock_names(queue: pool.PoolQueue) -> set[str]:
    return {entry.name for entry in os.scandir(Path(queue.root) / "transition-locks")}


def test_one_gc_leaves_exactly_the_live_lock_files(tmp_path):
    """1,000 terminal keys and 10 live ones: one gc leaves exactly the 10."""

    queue = _queue(tmp_path)
    old = pool.LEASE_TIMEOUT_S + 60
    terminal = [_key(f"terminal-{index}") for index in range(1000)]
    live = [_key(f"live-{index}") for index in range(10)]
    states = (pool.DONE, pool.FAILED, pool.WITHDRAWN)
    for index, key in enumerate(terminal):
        _file(queue, states[index % 3], key, age_s=old)
    for index, key in enumerate(live):
        # Five queued again after an ending, five running: both are live.
        _file(queue, pool.DONE, key, age_s=old)
        _file(queue, pool.READY if index < 5 else pool.CLAIMED, key, age_s=old)
    for key in terminal + live:
        with queue._transition_locked(key):
            pass
    assert len(_lock_names(queue)) == 1010
    assert _lock_names(queue) == {pb_gc.transition_lock_name(key)
                                  for key in terminal + live}

    receipt_path = tmp_path / "receipt.json"
    assert pb_gc.main(["--queue-root", str(queue.root), "--apply",
                       "--all-lock-takers-verify", "--summary",
                       "--receipt", str(receipt_path)]) == 0

    assert _lock_names(queue) == {pb_gc.transition_lock_name(key) for key in live}
    receipt = json.loads(receipt_path.read_text())
    locks = receipt["kinds"][pb_gc.KIND_TRANSITION_LOCK]
    assert receipt["schema"] == pb_gc.GC_RECEIPT_SCHEMA and receipt["applied"]
    assert (locks["scanned"], locks["candidates"], locks["removed"]) == (1010, 1000, 1000)
    assert locks["kept"] == {"its key is ready or claimed": 10}
    assert receipt["survey_s"] >= 0 and receipt["sweep_s"] >= 0


def test_a_young_ending_keeps_its_lock_and_an_apply_needs_the_acknowledgement(tmp_path):
    queue = _queue(tmp_path)
    young, old = _key("young"), _key("old")
    _file(queue, pool.DONE, young, age_s=1)
    _file(queue, pool.DONE, old, age_s=pool.LEASE_TIMEOUT_S + 60)
    for key in (young, old):
        with queue._transition_locked(key):
            pass
    assert pb_gc.main(["--queue-root", str(queue.root), "--apply"]) == 2
    assert len(_lock_names(queue)) == 2
    assert pb_gc.main(["--queue-root", str(queue.root), "--apply",
                       "--all-lock-takers-verify"]) == 0
    assert _lock_names(queue) == {pb_gc.transition_lock_name(young)}
    receipts = list((Path(queue.root) / "gc-receipts").iterdir())
    assert len(receipts) == 1
    kept = json.loads(receipts[0].read_text())["kinds"][pb_gc.KIND_TRANSITION_LOCK]["kept"]
    assert kept == {"terminal for less than the lease timeout": 1}


def test_empty_residency_namespaces_of_terminal_consumers_are_retired(tmp_path):
    """Empty namespaces, their maps and landing records of ended consumers go.

    The landing record (#989) is retired with a terminal consumer whether or
    not its fragments remain; a namespace only when its directory is empty.
    """

    queue = _queue(tmp_path)
    old = pool.LEASE_TIMEOUT_S + 60
    residency = queue.residency_fragment_root()
    residency.mkdir(parents=True, exist_ok=True)
    dead = [_key(f"dead-{index}") for index in range(20)]
    for index, key in enumerate(dead):
        _file(queue, pool.FAILED, key, age_s=old)
        (residency / key).mkdir()
        if index < 5:
            # A finished consumer's map and landing record, beside it.
            (residency / f"{key}.map.json").write_text("{}")
            (residency / f"{key}.landing.json").write_text("{}")
    running = _key("running")
    _file(queue, pool.CLAIMED, running, age_s=old)
    (residency / running).mkdir()
    (residency / f"{running}.map.json").write_text("{}")
    (residency / f"{running}.landing.json").write_text("{}")
    holding = _key("holding")
    _file(queue, pool.DONE, holding, age_s=old)
    (residency / holding).mkdir()
    (residency / holding / f"{_key('mover')}.json").write_text("{}")
    (residency / f"{holding}.map.json").write_text("{}")
    (residency / f"{holding}.landing.json").write_text("{}")
    landing_only = _key("landing-only")
    _file(queue, pool.DONE, landing_only, age_s=old)
    (residency / f"{landing_only}.landing.json").write_text("{}")
    unknown = _key("never-queued")
    (residency / unknown).mkdir()
    other = residency / "produced-output-scopes"
    other.mkdir()

    receipt_path = tmp_path / "receipt.json"
    assert pb_gc.main(["--queue-root", str(queue.root), "--apply",
                       "--all-lock-takers-verify", "--receipt", str(receipt_path)]) == 0
    left = {entry.name for entry in os.scandir(residency)}
    assert left == {running, f"{running}.map.json", f"{running}.landing.json",
                    holding, f"{holding}.map.json", unknown, "produced-output-scopes"}
    receipt = json.loads(receipt_path.read_text())
    namespaces = receipt["kinds"][pb_gc.KIND_RESIDENCY_NAMESPACE]
    assert (namespaces["scanned"], namespaces["removed"]) == (23, 20)
    assert namespaces["kept"] == {"its consumer is ready or claimed": 1,
                                  "holds fragments": 1,
                                  "no terminal record names its consumer": 1}
    landings = receipt["kinds"][pb_gc.KIND_LANDING_RECORD]
    assert (landings["scanned"], landings["removed"]) == (8, 7)
    assert landings["kept"] == {"its consumer is ready or claimed": 1}
    # Taking a dead consumer's lock to remove its namespace made its lock
    # file; the same run retired it, so no lock file is left behind.
    assert receipt["own_locks_retired"] == 22
    assert _lock_names(queue) == set()


def test_a_tombstone_after_the_survey_keeps_the_namespace_as_the_survey_would(tmp_path):
    """The re-check under the lock uses the survey's liveness rule, not the row alone."""

    queue = _queue(tmp_path)
    residency = queue.residency_fragment_root()
    residency.mkdir(parents=True, exist_ok=True)
    key = _key("mid-transition")
    _file(queue, pool.DONE, key, age_s=pool.LEASE_TIMEOUT_S + 60)
    (residency / key).mkdir()
    (residency / f"{key}.map.json").write_text("{}")
    plan = pb_gc.survey_queue(Path(queue.root))
    assert len(plan["remove"]) == 1
    # A finisher moved a claim of the key aside after the survey.
    tombstone = queue.dir(pool.CLAIMED) / f"{key}.1.host.1.abcdef01{pool.TOMBSTONE_SUFFIX}"
    tombstone.write_text("{}")
    outcome = pb_gc.sweep_queue(plan, lock_takers_verify=True)
    assert outcome["removed"] == []
    assert {why for _row, why in outcome["skipped"]} == {"its consumer was queued again"}
    assert {entry.name for entry in os.scandir(residency)} == {key, f"{key}.map.json"}


def test_a_consumer_queued_again_after_the_survey_keeps_its_namespace(tmp_path):
    """The removal re-reads the consumer under its transition lock."""

    queue = _queue(tmp_path)
    residency = queue.residency_fragment_root()
    residency.mkdir(parents=True, exist_ok=True)
    key = _key("resubmitted")
    _file(queue, pool.DONE, key, age_s=pool.LEASE_TIMEOUT_S + 60)
    (residency / key).mkdir()
    (residency / f"{key}.map.json").write_text("{}")
    (residency / f"{key}.landing.json").write_text("{}")
    plan = pb_gc.survey_queue(Path(queue.root))
    assert len(plan["remove"]) == 2
    _file(queue, pool.READY, key, age_s=0)      # queued again after the survey
    outcome = pb_gc.sweep_queue(plan, lock_takers_verify=True)
    assert outcome["removed"] == []
    assert {why for _row, why in outcome["skipped"]} == {"its consumer was queued again"}
    assert {entry.name for entry in os.scandir(residency)} == {
        key, f"{key}.map.json", f"{key}.landing.json"}

    # A consumer whose lock is held when the sweep reaches it is left alone.
    queue.item_path(pool.READY, key).unlink()
    plan = pb_gc.survey_queue(Path(queue.root))
    with queue._transition_locked(key):
        context = multiprocessing.get_context("fork")
        results = context.Queue()
        worker = context.Process(target=_sweep_in_child, args=(plan, results))
        worker.start()
        skipped = results.get(timeout=60)
        worker.join(timeout=60)
    # Both residency rows, and the key's own lock file ("held").
    assert sorted(skipped) == ["held", "its consumer's transition lock is held",
                               "its consumer's transition lock is held"]
    assert {entry.name for entry in os.scandir(residency)} == {
        key, f"{key}.map.json", f"{key}.landing.json"}


def _sweep_in_child(plan, results) -> None:
    outcome = pb_gc.sweep_queue(plan, lock_takers_verify=True)
    results.put([why for _row, why in outcome["skipped"]])


def test_an_unsafe_lock_inode_is_refused_and_released(tmp_path):
    """The post-lock check refuses a hard-linked lock file and lets it go."""

    path = tmp_path / "locks" / "unsafe.lock"
    path.parent.mkdir()
    path.touch()
    os.link(path, tmp_path / "locks" / "second-name")
    try:
        with posix_lock.held(path):
            raise AssertionError("an unsafe lock file was held")
    except OSError as exc:
        assert "unsafe POSIX transition lock" in str(exc)
    os.unlink(tmp_path / "locks" / "second-name")
    assert _probe(path), "the refused acquisition kept the lock"


def _hold_repeatedly(root: str, keys: list[str], marks: str, stop_at: float,
                     results) -> None:
    queue = pool.PoolQueue(root)
    violations = entries = 0
    while time.monotonic() < stop_at:
        for key in keys:
            with queue._transition_locked(key):
                mark = Path(marks) / key
                try:
                    fd = os.open(mark, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                except FileExistsError:
                    violations += 1
                    continue
                os.close(fd)
                entries += 1
                time.sleep(0.0005)
                os.unlink(mark)
    results.put((violations, entries))


def test_a_holder_racing_the_gc_keeps_mutual_exclusion(tmp_path):
    """Four processes take terminal keys' locks while the gc retires them."""

    queue = _queue(tmp_path)
    keys = [_key(f"race-{index}") for index in range(8)]
    for key in keys:
        _file(queue, pool.DONE, key, age_s=pool.LEASE_TIMEOUT_S + 60)
        with queue._transition_locked(key):
            pass
    marks = tmp_path / "marks"
    marks.mkdir()
    context = multiprocessing.get_context("fork")
    results = context.Queue()
    stop_at = time.monotonic() + 4.0
    holders = [context.Process(target=_hold_repeatedly,
                               args=(str(queue.root), keys, str(marks), stop_at, results))
               for _ in range(4)]
    for holder in holders:
        holder.start()
    retired = 0
    while time.monotonic() < stop_at:
        plan = pb_gc.survey_queue(Path(queue.root))
        retired += len(pb_gc.sweep_queue(plan, lock_takers_verify=True)["removed"])
    outcomes = [results.get(timeout=60) for _ in holders]
    for holder in holders:
        holder.join(timeout=60)
        assert holder.exitcode == 0
    assert sum(violations for violations, _ in outcomes) == 0
    assert all(entries > 0 for _, entries in outcomes)
    assert retired > 0, "the gc never retired a lock, so nothing raced"
