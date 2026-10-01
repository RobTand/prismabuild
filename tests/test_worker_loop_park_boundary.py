"""The real idle loop records its stop before announcing, sleeping or admitting.

A parked loop leaves its park marker before anything else it does that poll,
and it must never reach admission.  Since #1204 the drain branch also
republishes an advisory offer -- a queue *write* that reserves nothing -- and
since #1403 it retries this box's own saved finishes -- a narrow, claim-free
pass over ``claimed/``.  The fixture allows exactly those two bounded
operations (as stubs, so the bounded writer's helper processes and their
sleeps never enter this loop's poll count) and still fails the test on any
other queue operation.

The three orderings this pins, in one poll:

* park before announce: the marker is on disk when the advisory record would
  be published;
* park before sleep: the marker is on disk when the loop sleeps;
* the loop's own sleeps are the whole poll count, asserted exactly, with a
  bounded stop predicate so an unexpected extra sleep can only fail the test,
  never spin it.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys

import pytest


@pytest.mark.parametrize("contents,stamp,make_root", [
    ('{"draining": true, "changed_unix": 100.5}', "100.5", True),
    ('{}', "unknown", True),
    ('{broken', "unknown", True),
    (None, "unknown", True),  # read_text on a directory raises OSError
    ('absent', "unknown", True),
    ('{"draining": true, "changed_unix": 100.5}', "100.5", False),
])
def test_park_precedes_announce_sleep_and_queue_access(tmp_path, monkeypatch,
                                                       contents, stamp, make_root):
    gate = tmp_path / "maintenance.json"
    if contents == 'absent':
        pass
    elif contents is None:
        gate.mkdir()
    else:
        gate.write_text(contents)
    parked = tmp_path / "rollout" / "parked"
    if make_root:
        parked.mkdir(parents=True)
    monkeypatch.setenv("PRISMABUILD_MAINTENANCE_GATE", str(gate))
    source = Path(__file__).resolve().parents[1] / "tools/fleet/worker_loop.py"
    spec = importlib.util.spec_from_file_location("park_boundary_loop", source)
    loop = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loop)
    # Also point the pre-feature baseline at the private gate, so its failure
    # proves missing evidence, rather than a missing environment override.
    monkeypatch.setattr(loop, "MAINTENANCE_GATE", gate)
    monkeypatch.setattr(loop, "SH", tmp_path)
    monkeypatch.setattr(loop, "loaded_runtime_commit", lambda: "test")
    monkeypatch.setattr(loop, "published_commit", lambda: "test")
    monkeypatch.setattr(loop, "_generation_at", lambda path: "test")
    monkeypatch.setattr(loop.cpu_topology, "inherited_tiers", lambda: None)
    # Capability probes are not this fixture's subject; make them inert so
    # nothing they do can be mistaken for a poll.
    monkeypatch.setattr(loop.container_images.InventoryCache, "get",
                        lambda self, *a, **k: None)
    monkeypatch.setattr(loop.box_capacity, "ipv4_addresses", lambda: None)

    class UntouchableQueue:
        def __init__(self, root):
            assert root == tmp_path / "pb-queue"

        def dir(self, *args, **kwargs):
            # Drain-path membership reconciliation may census the
            # withdrawn/ decisions (read-only, no admission). Report no
            # queue dir so the census is empty and nothing publishes;
            # any actual admission still fails below.
            return tmp_path / "no-such-queue-dir"

        def retry_own_pending_finishes(self):
            # The drain branch's second bounded queue access (#1403): it
            # retries this box's saved finishes and claims nothing. The
            # fixture substitutes an empty verdict for the same reason it
            # stubs the announce -- the retry's own behaviour is covered by
            # its own tests, and its sweep must not enter this poll count.
            return []

        def __getattr__(self, name):
            pytest.fail(f"parked loop reached queue operation {name}")

    monkeypatch.setattr(loop.pool, "PoolQueue", UntouchableQueue)
    starttime = Path("/proc/self/stat").read_text().rpartition(")")[2].split()[19]
    expected = parked / f"{os.getpid()}-{starttime}-{stamp}"
    polls = []
    announces = []

    def publish(announce, **kwargs):
        # The deliberate advisory announce is allowed (#1204), and it must
        # follow the park.  The real bounded writer forks helper processes
        # whose sleeps and locks are not this loop's polls, so the fixture
        # substitutes its verdict rather than running them.
        if make_root:
            assert expected.is_file(), "loop announced before leaving park evidence"
        announces.append(announce)
        return loop.PublicationResult("published", 0.0, None, "")

    monkeypatch.setattr(loop, "publish_offer", publish)

    def sleeping(seconds):
        if make_root:
            assert expected.is_file(), "loop slept without leaving park evidence"
            assert expected.read_bytes() == b""
            assert list(parked.iterdir()) == [expected]
            polls.append(expected.stat().st_mtime_ns)
        else:
            assert not parked.exists()
            polls.append(None)

    monkeypatch.setattr(loop.time, "sleep", sleeping)
    monkeypatch.setattr(sys, "argv", ["worker_loop.py", "--all-cores",
                                     "--cpu-slots", "1", "--poll-s", "0"])
    assert loop._run_loop(lambda: len(polls) >= 2) == 0
    assert len(polls) == 2, "the loop took exactly its own two poll sleeps"
    assert len(announces) == 2, "one advisory announce per parked poll"
    assert polls[0] == polls[1], "the second poll rewrote the marker"
