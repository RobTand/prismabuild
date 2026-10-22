#!/usr/bin/env python3
"""PB-admitted scratch v2 transport proof and connection/watch census (#1939).

Run with pbrun on the queue host, two CPUs and four GiB, native threads one.
Uses only configured temporary scratch and an ephemeral private port, never 9470.
Counts describe this workload, not fleet-wide capacity or claim latency.
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


def prove_v2_transport() -> int:
    if socket.gethostname() != "dl380g10":
        raise SystemExit("this probe requires dl380g10's local queue-host runtime")
    with tempfile.TemporaryDirectory(prefix="pbbell-v2-") as scratch:
        root = Path(scratch) / "queue"
        for directory in queue_bell.STATE_DIRS:
            (root / directory).mkdir(parents=True)
        with queue_notifier.BellService(root, [("127.0.0.1", 0)]) as service:
            port = service.servers[0].server_address[1]
            assert port != queue_bell.DEFAULT_PORT
            client = queue_bell.Client(root, host="127.0.0.1", port=port)
            before = client.now_v2(("done", "attempts"))
            initial_watches = len(service.watch.directories)
            entered = threading.Event()
            original = service.ring.wait_v2

            def announce_v2_subscription(*args):
                entered.set()
                return original(*args)

            service.ring.wait_v2 = announce_v2_subscription
            key = "a" * 64
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(client.wait_v2,
                    queue_bell.Cursor(before["epoch"], before["seq"]),
                    before["coverage"]["version"], ("done",), (key,), 10)
                assert entered.wait(2)
                census = client.now_v2(("done",))
                temporary = root / "done" / ".new"
                temporary.write_text("unparsed body")
                os.replace(temporary, root / "done" / f"{key}.json")
                delivered = pending.result(3)
            assert delivered["events"][0]["id"] == key
            populated = root / "incoming"
            (populated / "generation").mkdir(parents=True)
            (populated / "generation" / "00000001.json").write_text("unparsed")
            before = client.now_v2(("attempts",))
            populated.rename(root / "attempts" / key)
            moved = client.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                before["coverage"]["version"], ("attempts",), (key,), 3)
            assert moved["events"]
            after = client.now_v2(("done", "attempts"))
            assert client.now().epoch == after["epoch"]
            print(json.dumps({"proof": "scratch-v2", "host": socket.gethostname(),
                "port": port, "coverage": after["coverage"],
                "initial_watches": initial_watches, "populated_watches": len(service.watch.directories),
                "concurrent_diagnostics": census["diagnostics"],
                "ending_diagnostics": after["diagnostics"],
                "delivered_events": len(delivered["events"]), "moved_events": len(moved["events"]),
                "affinity": sorted(os.sched_getaffinity(0))}), flush=True)
        assert not service.workers.active and not service.ring.waiters
    return 0


if __name__ == "__main__":
    raise SystemExit(prove_v2_transport())
