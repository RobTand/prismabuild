"""Submit through pbrun --measurement on dl380g10; compare interleaved scratch IO.

This bounded file-create/read/unlink workload records load per arm. It measures
the existing mounts, not stable-storage durability or whole-suite throughput.
"""
import json
import os
import pathlib
import socket
import subprocess
import tempfile
import time

def load():
    return {"loadavg": pathlib.Path("/proc/loadavg").read_text().strip(),
            "cpu": pathlib.Path("/proc/stat").read_text().splitlines()[0],
            "cpu_pressure": pathlib.Path("/proc/pressure/cpu").read_text().strip(),
            "io_pressure": pathlib.Path("/proc/pressure/io").read_text().strip()}

payload = os.urandom(4096)
for repeat in range(3):
    for arm, root in [("before", "/home/rob/tmp"), ("after", "/home/rob/pbt17")]:
        start_load = load()
        start = time.monotonic()
        with tempfile.TemporaryDirectory(prefix="pb1914-", dir=root) as directory:
            paths = [pathlib.Path(directory) / str(i) for i in range(256)]
            for path in paths:
                with path.open("wb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
            for path in paths:
                assert path.read_bytes() == payload
                path.unlink()
        print(json.dumps({"arm": arm, "repeat": repeat, "root": root,
            "host": socket.gethostname(), "files": 256, "bytes_per_file": len(payload),
            "seconds": time.monotonic() - start,
            "mount": subprocess.check_output(["findmnt", "-n", "-o", "SOURCE,FSTYPE,TARGET", "-T", root], text=True).strip(),
            "start_load": start_load, "end_load": load()}), flush=True)
