#!/usr/bin/env python3
"""Run INSIDE a PB action on dl380g10: scratch NFS rename and private-link proof.

No live queue records or roles are used. SSH children only perform the named
scratch write or a read-only HTTP query. Reserve 2 CPUs, 1 GiB, no GPU; bound
native threads to one. The lead can rerun this before activating #1894.
"""
from concurrent.futures import ThreadPoolExecutor
import argparse
import json
from pathlib import Path
import socket
import shlex
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild import queue_bell, queue_notifier


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--celestia-host", default="celestia",
                        help="SSH name or address of the LAN submitting host")
    args = parser.parse_args()
    if socket.gethostname() != "dl380g10":
        raise SystemExit("this proof requires the queue's local filesystem on dl380g10")
    with tempfile.TemporaryDirectory(prefix="pbbell-1894-",
                                      dir="/mnt/shared/prismabuild-fleet") as scratch:
        root = Path(scratch)
        for directory in queue_bell.STATE_DIRS:
            (root / directory).mkdir()
        # A private port and loopback-only listener for the NFS rename proof.
        with queue_notifier.BellService(root, [("127.0.0.1", 19470)]) as service:
            client = queue_bell.Client(root, host="127.0.0.1", port=19470)
            cursor = client.now()
            key = "a" * 64
            writer = ("import os,pathlib; p=pathlib.Path(" + repr(scratch)
                      + ")/'done'; t=p/'.nfs-write'; t.write_text('{}'); "
                      + "os.replace(t,p/" + repr(key + ".json") + "); print('NFS rename committed')")
            with ThreadPoolExecutor() as executor:
                pending = executor.submit(client.wait, cursor, ("done",), (key,), 30)
                result = subprocess.run(["ssh", "-o", "BatchMode=yes", "sparky",
                                         "python3", "-c", shlex.quote(writer)],
                                        text=True, capture_output=True, timeout=20, check=True)
                reply = pending.result(35)
            assert any(event["key"] == key for event in reply["events"]), reply
            print(json.dumps({"proof": "sparky-nfs-rename", "since": str(cursor),
                              "reply": reply, "writer": result.stdout.strip()}), flush=True)
        # Check the actual proposed port and all remote mount links without changing
        # host configuration. These listeners live only for this admitted action.
        addresses = [("10.100.98.3", queue_bell.DEFAULT_PORT),
                     ("10.100.99.3", queue_bell.DEFAULT_PORT),
                     ("192.168.1.107", queue_bell.DEFAULT_PORT)]
        with queue_notifier.BellService(root, addresses):
            for host, address in zip(("sparky", "sparklina", args.celestia_host), addresses):
                program = ("import http.client; c=http.client.HTTPConnection("
                           + repr(address[0]) + ",9470,timeout=5); "
                           + "c.request('GET','/v1/now'); r=c.getresponse(); "
                           + "assert r.status==200; print(r.read().decode())")
                result = subprocess.run(["ssh", "-o", "BatchMode=yes", host,
                                         "python3", "-c", shlex.quote(program)],
                                        text=True, capture_output=True, timeout=15, check=True)
                print(json.dumps({"proof": "private-link", "host": host,
                                  "address": address, "reply": json.loads(result.stdout)}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
