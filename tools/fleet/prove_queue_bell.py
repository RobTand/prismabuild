#!/usr/bin/env python3
"""Run INSIDE a PB action on dl380g10: scratch NFS rename and private-link proof.

No live queue records or roles are used. SSH children only perform the named
scratch write or a read-only HTTP query. Reserve 2 CPUs, 1 GiB, no GPU; bound
native threads to one. The lead can rerun this before activating #1894.
"""
from concurrent.futures import ThreadPoolExecutor
import argparse
import json
import os
from pathlib import Path
import socket
import shlex
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild import action_wait, endings, pool, queue_bell, queue_notifier


from worker_loop import capped_simulation_parameters


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
        # Drive the actual supersession/requeue writer against a private queue.
        queue = pool.PoolQueue(root)
        demand = {"cpu": 1, "mem_gb": 1}
        queue.announce(host=socket.gethostname(), tags=(), has_gpu=False,
                       capacity=demand, cpu_tiers={"preferred": sorted(os.sched_getaffinity(0))[:1],
                                                  "fallback": []})
        key = "f" * 64
        publish = dict(action_key=key, cas_root=root / "cas", checkout_root=root,
                       worker_script=Path(__file__), resources=demand,
                       max_attempts=2, retry_safe=True)
        queue.publish(**publish)
        original = queue.claim(owner="bell-scratch-proof", **capped_simulation_parameters(queue, demand))
        assert original is not None, "scratch claim refused"
        queue.publish(**publish)
        with queue_notifier.BellService(root, [("127.0.0.1", 19470)]) as service:
            client = queue_bell.Client(root, host="127.0.0.1", port=19470)
            # Waiting only on the immutable decision proves the missing namespace,
            # independently of any earlier attempt or claimed-directory wakeup.
            cursor = client.now()
            with ThreadPoolExecutor() as executor:
                pending = executor.submit(client.wait, cursor, ("publication-endings",), (key,), 30)
                queue.finish(key, status="failed", detail={"returncode": 17},
                             claim_snapshot=original)
                reply = pending.result(35)
            assert any(event["key"] == key for event in reply["events"]), reply
            decision, generation = action_wait.outcome_poll(queue, key, original["published_unix"])
            assert isinstance(decision, endings.PublicationSupersession)
            assert generation == original["published_unix"]
            assert not queue.ledger().held()
            print(json.dumps({"proof": "supersession-requeue-ending", "since": str(cursor),
                "reply": reply, "generation": generation,
                "successor": decision.successor_published_unix}), flush=True)

        # A scratch role uses the real entrypoint. Repoint only its private link.
        # Run the same role rollback fixture without importing pytest's live guards.
        import shutil
        import select
        fleet = root / "role-fleet"
        generation = fleet / "runtime-generations" / "new"
        previous = fleet / "runtime-generations" / "previous"
        previous.mkdir(parents=True)
        (generation / "tools" / "fleet").mkdir(parents=True)
        checkout = Path(__file__).resolve().parents[2]
        (generation / "src").symlink_to(checkout / "src", target_is_directory=True)
        entrypoint = generation / "tools" / "fleet" / "pbbell.py"
        shutil.copyfile(checkout / "tools" / "fleet" / "pbbell.py", entrypoint)
        (fleet / "repo").symlink_to(generation, target_is_directory=True)
        reader, writer = os.pipe()
        program = """
import os,runpy,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from prismabuild import queue_notifier,role_session
role_session.ROLE_LOCK_ROOT=Path(sys.argv[2])
original=queue_notifier.BellService.__enter__
ready_fd=int(sys.argv[3])
def ready(self):
    result=original(self)
    os.write(ready_fd,b'R')
    os.close(ready_fd)
    return result
queue_notifier.BellService.__enter__=ready
entrypoint,root=sys.argv[4:]
sys.argv=[entrypoint,'--queue-root',root,'--port','19470']
runpy.run_path(entrypoint,run_name='__main__')
"""
        process = subprocess.Popen([sys.executable, "-c", program, str(checkout / "src"),
            str(root / "role-locks"), str(writer), str(entrypoint), str(root)],
            pass_fds=(writer,), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        os.close(writer)
        try:
            assert select.select([reader], [], [], 30)[0] and os.read(reader, 1) == b"R"
            client = queue_bell.Client(root, host="127.0.0.1", port=19470)
            assert client.now().epoch
            replacement = fleet / "next-repo"
            replacement.symlink_to(previous, target_is_directory=True)
            replacement.replace(fleet / "repo")
            output, errors = process.communicate(timeout=10)
            assert process.returncode == 0, errors.decode()
            print(json.dumps({"proof": "role-rollback", "exit": process.returncode,
                              "retired": str(generation), "active": str(previous)}), flush=True)
        finally:
            os.close(reader)
            if process.poll() is None:
                process.terminate()
                process.communicate(timeout=10)

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
