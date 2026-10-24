"""#2204 research acceptance: installed watches, never publisher callbacks.

The queue-local residency topic includes leases and produced-output fragments.
Scopes are absolute default namespace roots; record contents are opaque to bell.
"""
from contextlib import contextmanager
import errno
import inspect
import os
from pathlib import Path
import select
import subprocess
import sys
from types import SimpleNamespace

import pytest

from prismabuild import queue_bell, queue_bell_events, queue_notifier
from test_queue_bell import bell
from test_queue_bell_v2 import _v2_get, _v2_poll


CONSUMER, MOVER, DIGEST = "a" * 64, "b" * 64, "c" * 64
TOPICS = ("residency", "residency-plans", "stage-ownership-holders",
          "released-origin-consumers")
LAYOUTS = (
    ("residency", f"{CONSUMER}/{MOVER}.json"),
    ("residency", f"{CONSUMER}.map.json"),
    ("residency", f"leases/{CONSUMER}/pin.lease.json"),
    ("residency", f"leases/{CONSUMER}/{MOVER}.retiring.json"),
    ("residency", f"produced-output-fragments/{CONSUMER}/{MOVER}.json"),
    ("residency-plans", f"{CONSUMER}.json"),
    ("residency-plans", f"shared/ranges/{DIGEST}.json"),
    ("residency-plans", f"shared/movers/{MOVER}.json"),
    ("residency-plans", f"superseded/{CONSUMER}.{DIGEST}.superseded.json"),
    ("stage-ownership-holders", f"{DIGEST}.json"),
    ("released-origin-consumers", f"{CONSUMER}.{DIGEST}.json"),
)


def _root(tmp_path):
    root = tmp_path / "queue"
    for name in (*queue_bell.REQUIRED_STATE_DIRS, *TOPICS):
        (root / name).mkdir(parents=True, exist_ok=True)
    return root


def _write(path, text='{"revision": 1}'):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _drain(watch, ring):
    # Real kernel packets plus bounded recovery turns, including packets
    # generated during recursive installation. No background reader competes.
    for _ in range(128):
        ready = select.select([watch.fd], [], [], 0.01)[0]
        if ready:
            watch.consume(os.read(watch.fd, 65536), ring)
        recover = getattr(watch, "recover", None)
        if callable(recover):
            recover(ring)
        progress = ring.now_v2().get("diagnostics", {}).get("residency_recovery", {})
        if not ready and not any(item.get("pending", 0) for item in progress.values()):
            return
    pytest.fail("installed-watch recovery did not quiesce within 128 turns")


@contextmanager
def _unstarted_service(root, **options):
    service = queue_notifier.BellService(root, [("127.0.0.1", 0)], **options)
    try:
        yield service
    finally:
        # No thread was started. BellService.__exit__ joins its reader, so close
        # precisely the resources this constructor acquired instead.
        service.ring.close()
        for server in service.servers:
            server.server_close()
        service.watch.close()
        service.reader.close()
        service.writer.close()


@contextmanager
def _installed(root):
    # Exercise the owner's real publication path, not a test-created proof
    # inferred solely from topic names. Do not start its reader thread: these
    # installed-watch tests deterministically consume the actual kernel packets.
    with _unstarted_service(root) as service:
        yield service.watch, service.ring


def _complete(snapshot, root, topic):
    coverage = snapshot.get("coverage", {})
    assert topic in coverage.get("topics", ()), snapshot
    scope = coverage.get("scopes", {}).get(str(root / topic), {})
    assert scope.get("topic") == topic, snapshot
    assert scope.get("complete") is True, snapshot
    assert isinstance(snapshot.get("epoch"), str) and snapshot["epoch"], snapshot
    assert isinstance(snapshot.get("seq"), int) and snapshot["seq"] >= 0, snapshot
    assert isinstance(coverage.get("version"), int) and coverage["version"] > 0
    return snapshot


def _before(ring, root, topic):
    return _complete(ring.now_v2(), root, topic)


def _observe(watch, ring, root, topic, before):
    _drain(watch, ring)
    reply = ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                         {topic}, set(), 0, before["coverage"]["version"])
    assert reply is not None
    assert reply["epoch"] == before["epoch"]
    assert reply["seq"] >= before["seq"]
    assert reply["coverage"]["version"] >= before["coverage"]["version"]
    assert reply.get("reset") or reply.get("events"), reply
    if reply.get("reset"):
        # A new observation boundary may be complete after installation/replay,
        # but the failed old-cursor subscription must not carry a positive proof.
        scope = reply["coverage"].get("scopes", {}).get(str(root / topic), {})
        assert scope.get("complete") is False, reply
        assert (reply["coverage"]["version"] > before["coverage"]["version"]
                or (reply.get("reason") == "history_lost"
                    and reply["seq"] > before["seq"])), reply
        _complete(ring.now_v2(), root, topic)
    else:
        _complete(reply, root, topic)
        events = reply["events"]
        assert all(event["topic"] == topic for event in events), reply
        seqs = [event["seq"] for event in events]
        assert seqs == sorted(set(seqs)), reply
        assert before["seq"] < seqs[0] <= seqs[-1] <= reply["seq"], reply


@pytest.mark.parametrize("topic,relative", LAYOUTS)
@pytest.mark.parametrize("mutation", ["add", "replace", "unlink", "open-write", "chmod", "utime"])
def test_external_record_mutation(tmp_path, topic, relative, mutation):
    root = _root(tmp_path)
    path = root / topic / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    if mutation != "add":
        _write(path)
    with _installed(root) as (watch, ring):
        before = _before(ring, root, topic)
        if mutation == "open-write":
            # Observe while the raw writer is STILL OPEN: close-write is too late.
            with path.open("r+b", buffering=0) as writer:
                writer.write(b'{"revision": 2}')
                _observe(watch, ring, root, topic, before)
            return
        if mutation == "add":
            _write(path)
        elif mutation == "replace":
            replacement = tmp_path / "replacement.json"
            _write(replacement, '{"revision": 2}')
            os.replace(replacement, path)
        elif mutation == "unlink":
            path.unlink()
        elif mutation == "chmod":
            path.chmod(path.stat().st_mode ^ 0o100)
        else:
            stat = path.stat()
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
        _observe(watch, ring, root, topic, before)


@pytest.mark.parametrize("topic,relative", [LAYOUTS[i] for i in (0, 2, 4, 6, 8)])
@pytest.mark.parametrize("arrival", ["mkdir", "populated-move"])
def test_nested_directory_arrival_installs_descendants(tmp_path, topic, relative, arrival):
    root = _root(tmp_path)
    target = root / topic / relative
    with _installed(root) as (watch, ring):
        before = _before(ring, root, topic)
        if arrival == "mkdir":
            _write(target)
        else:
            incoming = tmp_path / "incoming"
            parts = Path(relative).parts
            _write(incoming.joinpath(*parts[1:]))
            incoming.rename(root / topic / parts[0])
        _observe(watch, ring, root, topic, before)
        assert target.parent.relative_to(root) in watch.directories.values()
        # A reset for arrival alone cannot hide failure to install the leaf watch.
        before = _before(ring, root, topic)
        target.unlink()
        _observe(watch, ring, root, topic, before)


@pytest.mark.parametrize("topic", TOPICS)
def test_namespace_root_replacement_rebinds_default_scope(tmp_path, topic):
    root = _root(tmp_path)
    relative = next(relative for name, relative in LAYOUTS if name == topic)
    _write(root / topic / relative)
    incoming = tmp_path / "incoming"
    _write(incoming / relative, '{"revision": 2}')
    with _installed(root) as (watch, ring):
        before = _before(ring, root, topic)
        (root / topic).rename(tmp_path / "displaced")
        incoming.rename(root / topic)
        _observe(watch, ring, root, topic, before)
        before = _before(ring, root, topic)
        (root / topic / relative).unlink()
        _observe(watch, ring, root, topic, before)


def test_queue_root_replacement_rebinds_or_revokes_residency(tmp_path):
    root = _root(tmp_path)
    incoming = _root(tmp_path / "replacement")
    relative = f"leases/{CONSUMER}/pin.lease.json"
    _write(root / "residency" / relative)
    _write(incoming / "residency" / relative)
    with _installed(root) as (watch, ring):
        before = _before(ring, root, "residency")
        root.rename(tmp_path / "displaced")
        incoming.rename(root)
        try:
            _observe(watch, ring, root, "residency", before)
        except OSError:
            # Existing BellService retires on queue-root loss and closes the
            # ring in its listener's finally block. Preserve that safe option.
            ring.close()
            _revoked(ring.now_v2(), root)
            reply = _old_subscription(ring, before)
            assert reply.get("reset") is True, reply
            _revoked(reply, root)
            return
        before = _before(ring, root, "residency")
        (root / "residency" / relative).unlink()
        _observe(watch, ring, root, "residency", before)


def _identity(path_or_fd):
    info = os.fstat(path_or_fd) if isinstance(path_or_fd, int) else os.stat(path_or_fd)
    return info.st_dev, info.st_ino


def test_residency_watch_install_precedes_same_inode_scandir(tmp_path, monkeypatch):
    root = _root(tmp_path)
    for topic, relative in LAYOUTS:
        _write(root / topic / relative)
    relatives = {Path("residency")}
    for topic, relative in LAYOUTS:
        if topic == "residency":
            relatives.update(parent for parent in (Path(topic) / relative).parents
                             if parent != Path())
    expected = {_identity(root / relative) for relative in relatives}
    installed, scanned, violations = set(), set(), []
    original_add = queue_bell_events.InotifyQueue._add
    original_scandir = os.scandir

    def add(self, path, relative):
        # Existing _add receives a pinned /proc/self/fd/N path, not a new API.
        identity = _identity(path)
        result = original_add(self, path, relative)
        if relative in relatives:
            installed.add(identity)
        return result

    def scandir(path):
        identity = _identity(path)
        if identity in expected:
            scanned.add(identity)
            if identity not in installed:
                violations.append(identity)
        return original_scandir(path)

    monkeypatch.setattr(queue_bell_events.InotifyQueue, "_add", add)
    monkeypatch.setattr(os, "scandir", scandir)
    with _installed(root) as (watch, ring):
        before = _before(ring, root, "residency")
        assert scanned == expected and not violations, (scanned, expected, violations)
        (root / "residency" / CONSUMER / f"{MOVER}.json").unlink()
        _observe(watch, ring, root, "residency", before)


def test_bootstrap_child_after_scan_snapshot_is_replayed_before_complete(tmp_path, monkeypatch):
    root = _root(tmp_path)
    namespace = root / "residency"
    namespace_identity = _identity(namespace)
    child = namespace / "produced-output-fragments" / CONSUMER
    leaf = child / f"{MOVER}.json"
    original_scandir = os.scandir
    injected = []

    class Snapshot:
        def __init__(self, entries):
            self.entries = iter(entries)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()
            return False

        def close(self):
            pass  # The real iterator was already closed after snapshotting.

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.entries)

    def scandir(path):
        if _identity(path) != namespace_identity or injected:
            return original_scandir(path)
        with original_scandir(path) as entries:
            snapshot = list(entries)
        injected.append(True)
        _write(leaf)  # Absent from snapshot, but the namespace watch must see it.
        return Snapshot(snapshot)

    monkeypatch.setattr(os, "scandir", scandir)
    with _installed(root) as (watch, ring):
        initial = ring.now_v2()
        scope = initial.get("coverage", {}).get("scopes", {}).get(str(namespace), {})
        if scope.get("complete") is True:
            assert child.relative_to(root) in watch.directories.values(), initial
            assert not select.select([watch.fd], [], [], 0)[0], (
                "bootstrap published completeness before replaying buffered mutations")
            assert initial["seq"] > 0, "bootstrap mutation missing from transport history"
        _drain(watch, ring)
        before = _before(ring, root, "residency")
        assert injected and child.relative_to(root) in watch.directories.values()
        leaf.unlink()
        _observe(watch, ring, root, "residency", before)


def _revoked(snapshot, root, topic="residency"):
    coverage = snapshot.get("coverage", {})
    # Keep the existing topic-capability semantics for older v2 consumers.
    # Completeness is the new scope-qualified proof, never a topic name alone.
    scope = coverage.get("scopes", {}).get(str(root / topic), {})
    assert scope.get("topic") == topic, snapshot
    assert scope.get("complete") is False, snapshot


def _old_subscription(ring, before, topic="residency"):
    return ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                        {topic}, set(), 0, before["coverage"]["version"])


@pytest.mark.parametrize("loss", [queue_bell_events.IN_IGNORED,
    queue_bell_events.IN_DELETE_SELF, queue_bell_events.IN_MOVE_SELF])
def test_installed_watch_loss_revokes_scope_and_cursor(tmp_path, monkeypatch, loss):
    root = _root(tmp_path)
    leaf = root / "residency" / "leases" / CONSUMER
    _write(leaf / "pin.lease.json")
    with _installed(root) as (watch, ring):
        before = _before(ring, root, "residency")
        wd = next(wd for wd, path in watch.directories.items()
                  if path == leaf.relative_to(root))
        original_add = watch._add

        def cannot_restore(path, relative):
            if relative.parts and relative.parts[0] == "residency":
                raise OSError(errno.ENOSPC, "injected lost-watch recovery refusal")
            return original_add(path, relative)

        monkeypatch.setattr(watch, "_add", cannot_restore)
        try:
            watch.consume(queue_bell_events.EVENT_HEADER.pack(wd, loss, 0, 0), ring)
        except OSError:
            pass  # Stopping is safe only if the already-published proof is gone.
        _revoked(ring.now_v2(), root)
        reply = _old_subscription(ring, before)
        assert reply.get("reset") is True, reply
        _revoked(reply, root)


@pytest.mark.parametrize("boundary,error", [
    ("watch", errno.ENOSPC), ("watch", errno.EACCES),
    ("watch", errno.EOPNOTSUPP), ("watch", errno.ENOSYS),
    ("scan", errno.EACCES),
])
def test_loss_on_populated_installation_never_leaves_positive_proof(
        tmp_path, monkeypatch, boundary, error):
    root = _root(tmp_path)
    with _installed(root) as (watch, ring):
        before = _before(ring, root, "residency")
        incoming = tmp_path / "incoming"
        _write(incoming / CONSUMER / f"{MOVER}.json")
        target = root / "residency" / "produced-output-fragments"
        if boundary == "watch":
            original_add = watch._add

            def fail_install(path, relative):
                if relative == target.relative_to(root):
                    raise OSError(error, "injected unavailable residency watch")
                return original_add(path, relative)

            monkeypatch.setattr(watch, "_add", fail_install)
        else:
            identity = _identity(incoming)
            original_scandir = os.scandir

            def fail_scan(path):
                if _identity(path) == identity:
                    raise OSError(error, "injected inaccessible residency scope")
                return original_scandir(path)

            monkeypatch.setattr(os, "scandir", fail_scan)
        incoming.rename(target)
        try:
            _drain(watch, ring)
        except OSError:
            pass
        _revoked(ring.now_v2(), root)
        reply = _old_subscription(ring, before)
        assert reply.get("reset") is True, reply
        _revoked(reply, root)


@pytest.mark.parametrize("gap", ["ring-history", "epoch", "coverage-version", "future"])
def test_replay_gap_reply_is_not_a_positive_residency_coverage_proof(tmp_path, gap):
    root = _root(tmp_path)
    with _installed(root) as (watch, ring):
        before = _before(ring, root, "residency")
        cursor = queue_bell.Cursor(before["epoch"], before["seq"])
        version = before["coverage"]["version"]
        if gap == "ring-history":
            for _ in range(ring.events.maxlen + 1):
                ring.append("residency", CONSUMER)
        elif gap == "epoch":
            cursor = queue_bell.Cursor("0" * 32, before["seq"])
        elif gap == "coverage-version":
            ring.reset_v2()
        else:
            cursor = queue_bell.Cursor(before["epoch"], before["seq"] + 1)
        reply = ring.wait_v2(cursor, {"residency"}, set(), 0, version)
        assert reply.get("reset") is True, reply
        assert reply.get("reason"), reply
        _revoked(reply, root)
        # A caller must acquire a new valid observation boundary after the gap.
        fresh = _before(ring, root, "residency")
        renewed = _old_subscription(ring, fresh)
        assert renewed.get("reset") is not True, renewed
        assert renewed["events"] == [], renewed
        _complete(renewed, root, "residency")


def test_overflow_and_second_loss_during_rebuild_leave_no_scope_complete(tmp_path, monkeypatch):
    root = _root(tmp_path)
    _write(root / "residency" / CONSUMER / f"{MOVER}.json")
    with _unstarted_service(root) as service:
        before = _before(service.ring, root, "residency")
        original_add = queue_bell_events.InotifyQueue._add
        boundary_proofs = []

        def lose_again(self, path, relative):
            if relative == Path("residency"):
                boundary_proofs.append(service.ring.now_v2())
                raise OSError(errno.ENOSPC, "injected second loss during recovery")
            return original_add(self, path, relative)

        monkeypatch.setattr(queue_bell_events.InotifyQueue, "_add", lose_again)
        try:
            service.watch.consume(queue_bell_events.EVENT_HEADER.pack(
                -1, queue_bell_events.IN_Q_OVERFLOW, 0, 0), service.ring)
            _drain(service.watch, service.ring)
        except OSError:
            pass
        assert boundary_proofs, "overflow recovery never attempted residency installation"
        for proof in [*boundary_proofs, service.ring.now_v2()]:
            _revoked(proof, root)
        reply = _old_subscription(service.ring, before)
        assert reply.get("reset") is True, reply
        _revoked(reply, root)


@pytest.mark.parametrize("budget", ["entries", "time"])
def test_recovery_is_bounded_progressing_and_scope_local(tmp_path, monkeypatch, budget):
    # This small private service seam is also plumbed through pbbell's ordinary
    # argparse configuration. No namespace-lane record capture is done here.
    parameters = inspect.signature(queue_notifier.BellService).parameters
    assert {"recovery_entries", "recovery_budget_s"} <= set(parameters), (
        "residency recovery needs configurable per-turn entry and time limits")
    root = _root(tmp_path)
    unrelated = root / "residency-plans"
    _write(unrelated / f"{CONSUMER}.json")
    entry_limit = 4 if budget == "entries" else 1000
    with _unstarted_service(root, recovery_entries=entry_limit, recovery_budget_s=0.05) as service:
        recover = getattr(service.watch, "recover", None)
        assert callable(recover), "the installed watcher needs a bounded recovery turn"
        _before(service.ring, root, "residency")
        _before(service.ring, root, "residency-plans")
        stable_watches = {path: wd for wd, path in service.watch.directories.items()
                          if path.parts and path.parts[0] == "residency-plans"}
        incoming = tmp_path / "incoming"
        for number in range(40):
            _write(incoming / CONSUMER / f"fragment-{number}.json")
        target = root / "residency" / "produced-output-fragments"
        incoming.rename(target)
        original_scandir = os.scandir
        visits = []
        clock = [100.0]
        if budget == "time":
            monkeypatch.setattr(queue_bell_events.time, "monotonic", lambda: clock[0])

        class CountedScan:
            def __init__(self, iterator):
                self.iterator = iterator

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.iterator.close()

            def __iter__(self):
                return self

            def __next__(self):
                item = next(self.iterator)
                visits.append(item.name)
                if budget == "time":
                    clock[0] += 0.03  # deterministic slow-entry cost, no sleeps
                return item

            def close(self):
                self.iterator.close()

        def counted_scan(path):
            return CountedScan(original_scandir(path))

        monkeypatch.setattr(os, "scandir", counted_scan)
        progress, incomplete = [], False
        for turn in range(128):
            count = len(visits)
            if select.select([service.watch.fd], [], [], 0)[0]:
                service.watch.consume(os.read(service.watch.fd, 65536), service.ring)
            recover(service.ring)  # one bounded recovery turn, no reader thread
            max_visits = 4 if budget == "entries" else 2
            assert len(visits) - count <= max_visits, "unbounded enumeration in one recovery turn"
            proof = service.ring.now_v2()
            _complete(proof, root, "residency-plans")
            assert stable_watches == {
                path: wd for wd, path in service.watch.directories.items()
                if path.parts and path.parts[0] == "residency-plans"}
            pending = proof["diagnostics"].get("residency_recovery", {}).get(str(root / "residency"))
            assert isinstance(pending, dict), "recovery progress is not observable"
            assert isinstance(pending.get("scanned"), int), pending
            assert isinstance(pending.get("pending"), int), pending
            assert isinstance(pending.get("generation"), int), pending
            progress.append((pending["generation"], pending["scanned"]))
            scope = proof["coverage"].get("scopes", {}).get(str(root / "residency"), {})
            if scope.get("complete") is True:
                break
            incomplete = True
            _revoked(proof, root)
            # Churn in this scope must not restart unrelated scope recovery.
            if turn < 3:
                _write(target / CONSUMER / f"late-{turn}.json")
        else:
            pytest.fail("bounded recovery made no terminal progress within 128 turns")
        assert incomplete, "populated reconstruction was asserted complete in one unbounded turn"
        assert progress[-1][1] >= 40, progress
        for previous, current in zip(progress, progress[1:]):
            assert current[0] >= previous[0], progress
            if current[0] == previous[0]:
                assert current[1] >= previous[1], progress
        before = _before(service.ring, root, "residency")
        (target / CONSUMER / "late-0.json").unlink()
        _observe(service.watch, service.ring, root, "residency", before)


@pytest.mark.parametrize("topic,relative", LAYOUTS[:4], ids=[item[0] for item in LAYOUTS[:4]])
def test_in_place_mutation_is_visible_before_writer_closes(tmp_path, topic, relative):
    root = _root(tmp_path)
    leaf = root / topic / relative
    _write(leaf)
    with _installed(root) as (watch, ring):
        before = _before(ring, root, topic)
        with leaf.open("r+") as writer:
            writer.write('{"version":2}\n')
            writer.flush()
            os.fsync(writer.fileno())
            # CLOSE_WRITE alone cannot cover a long-lived external writer.
            _observe(watch, ring, root, topic, before)


def test_published_transport_carries_scope_proof_and_isolates_v1_outcomes(bell):
    before = _v2_get(bell, "/v2/now")
    _complete(before, bell.root, "residency")
    legacy = bell.client.now()
    _write(bell.root / "residency" / CONSUMER / f"{MOVER}.json")
    reply = _v2_poll(bell, before, topics="residency", ids="", timeout=1)
    assert reply.get("reset") or reply.get("events"), reply
    if reply.get("reset"):
        _revoked(reply, bell.root)
    else:
        _complete(reply, bell.root, "residency")
        assert {event["topic"] for event in reply["events"]} == {"residency"}
    unaffected = bell.client.wait(legacy, ("done",), (CONSUMER,), 0.01)
    assert unaffected.get("reset") is not True, unaffected
    assert unaffected["events"] == [], unaffected


def test_reconnect_to_restarted_owner_rejects_old_positive_scope_proof(tmp_path):
    root = _root(tmp_path)

    def endpoint(service):
        return SimpleNamespace(root=root, client=SimpleNamespace(
            port=service.servers[0].server_address[1]))

    with queue_notifier.BellService(root, [("127.0.0.1", 0)]) as first:
        before = _v2_get(endpoint(first), "/v2/now")
        _complete(before, root, "residency")
    with queue_notifier.BellService(root, [("127.0.0.1", 0)]) as second:
        reply = _v2_poll(endpoint(second), before, topics="residency", ids="", timeout=0)
        assert reply.get("reset") is True and reply.get("reason") == "epoch_mismatch", reply
        _revoked(reply, root)
        fresh = _v2_get(endpoint(second), "/v2/now")
        _complete(fresh, root, "residency")
        assert fresh["epoch"] != before["epoch"]


def test_unsupported_filesystem_cannot_gain_completeness_from_successful_add_watch(
        tmp_path, monkeypatch):
    # Some remote filesystems accept inotify_add_watch without observing remote
    # writers. A successful syscall or a publisher callback is not a capability
    # proof. Keep a small private, injectable filesystem-support predicate.
    predicate = getattr(queue_bell_events.InotifyQueue, "_residency_observable", None)
    assert callable(predicate), "residency coverage needs a filesystem capability check"
    root = _root(tmp_path)
    monkeypatch.setattr(queue_bell_events.InotifyQueue, "_residency_observable",
                        lambda self, *args, **kwargs: False)
    with _installed(root) as (watch, ring):
        for topic in TOPICS:
            _revoked(ring.now_v2(), root, topic)
        assert "done" in ring.now_v2()["coverage"]["topics"]
        _write(root / "residency" / CONSUMER / f"{MOVER}.json")
        _drain(watch, ring)
        # Even observed local events do not establish remote-writer coverage.
        _revoked(ring.now_v2(), root)


def test_recovery_limits_use_the_existing_pbbell_configuration_entrypoint():
    script = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbbell.py"
    result = subprocess.run([sys.executable, str(script), "--help"],
        text=True, capture_output=True, timeout=15, check=False)
    assert result.returncode == 0, result.stderr
    assert "--recovery-entries" in result.stdout, "pbbell lacks configurable recovery entry bound"
    assert "--recovery-budget-s" in result.stdout, "pbbell lacks configurable recovery time bound"
