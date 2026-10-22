#!/usr/bin/env python3
"""PB-admitted private bell host-input watch/connection demand probe (#1940).

Uses configured temporary storage and an ephemeral port. No live bell access.
Run through published pbrun with two CPUs, four GiB and native threads one.
"""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild import queue_bell, queue_notifier


def prove_host_input_transport() -> int:
    with tempfile.TemporaryDirectory(prefix="pbbell-hosts-") as scratch:
        root = Path(scratch) / "queue"
        for directory in queue_bell.REQUIRED_STATE_DIRS:
            (root / directory).mkdir(parents=True)
        (root / "workers").mkdir()
        for host in ("first.lab", "second.lab"):
            for directory in ("free", "held", "minted"):
                (root / "reservations" / host / directory).mkdir(parents=True)
        with queue_notifier.BellService(root, [("127.0.0.1", 0)]) as service:
            port = service.servers[0].server_address[1]
            assert port != queue_bell.DEFAULT_PORT
            client = queue_bell.Client(root, host="127.0.0.1", port=port)
            topics = ("workers", "reservations")
            before = client.now_v2(topics)
            legacy_cursor = client.now()
            initial_watches = len(service.watch.directories)
            entered = threading.Event()
            original = service.ring.wait_v2

            def announce_host_subscription(*args):
                entered.set()
                return original(*args)

            service.ring.wait_v2 = announce_host_subscription
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(client.wait_v2,
                    queue_bell.Cursor(before["epoch"], before["seq"]),
                    before["coverage"]["version"], topics, ("first.lab",), 10)
                assert entered.wait(2)
                census = client.now_v2(topics)
                (root / "workers" / "first.lab.json").write_text("old writer, unparsed")
                delivered = pending.result(3)
            assert delivered["events"][0]["topic"] == "workers"
            incoming = Path(scratch) / "incoming"
            incoming.mkdir()
            (incoming / "cpu-0").touch()
            installed = threading.Event()
            consume = service.watch.consume

            def announce_holder_installation(data, ring):
                consume(data, ring)
                if Path("reservations/first.lab/held/legacy") in service.watch.directories.values():
                    installed.set()

            service.watch.consume = announce_holder_installation
            holder = root / "reservations" / "first.lab" / "held" / "legacy"
            incoming.rename(holder)
            assert installed.wait(3)
            before = client.now_v2(topics)
            (holder / "cpu-0").rename(root / "reservations" / "first.lab" / "free" / "cpu-0")
            released = client.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                before["coverage"]["version"], ("reservations",), ("first.lab",), 3)
            assert released["events"] and not released.get("reset")
            after = client.now_v2(topics)
            assert client.wait(legacy_cursor, (), (), 0.05)["events"] == []
            assert all(not list((root / state).iterdir()) for state in ("ready", "done", "failed"))
            print(json.dumps(dict(proof="scratch-host-inputs", host=socket.gethostname(),
                port=port, initial_watches=initial_watches,
                populated_watches=len(service.watch.directories),
                concurrent_diagnostics=census["diagnostics"], ending_diagnostics=after["diagnostics"],
                covered_topics=after["coverage"]["topics"],
                offer_events=len(delivered["events"]), release_events=len(released["events"]),
                affinity=sorted(os.sched_getaffinity(0)))), flush=True)
        assert not service.workers.active and not service.ring.waiters
    return 0


if __name__ == "__main__":
    raise SystemExit(prove_host_input_transport())
