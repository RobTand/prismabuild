"""Real profile marker and embedded relay writers with supplied clock/process facts."""
from __future__ import annotations

import base64
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    core = load("prismabuild.core")
    writing = request["op"] == "write"
    session = core._ProfileSession(mode="sample", backend=SimpleNamespace(container_routes_unsupported=True), directory=store / "profile")
    session.directory.mkdir()
    if family == "profile-route":
        path = session.container_route_path
        if writing:
            session._arm_container_route()
            if variant == "docker":
                assert load("docker")._record_sample_route(str(path), "docker run")
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = session.container_route()
            assert result == ("docker run" if variant == "docker" else None)
            return {"route": result}
    else:
        path = session.exit_status_path
        if writing:
            argv = core.profile_exit_status_relay(["/compat/missing"] if variant == "launch-failed" else ["/bin/true"], path)
            source = argv[2]
            assert source == core.PROFILE_RELAY_SOURCE
            # Execute the exact embedded production program. Only OS input
            # observations are supplied; old consumer code is never replaced.
            bootstrap = '''import builtins, io, os, subprocess, sys, time
source, status, variant = sys.argv[1:]
time.monotonic = lambda: 100.0
real_open = builtins.open
def observed_open(path, *args, **kwargs):
    if path == "/proc/%d/stat" % os.getpid():
        return io.BytesIO(b"3 (compat) " + b" ".join([b"3"] * 22))
    return real_open(path, *args, **kwargs)
builtins.open = observed_open
if variant == "launched":
    class SuppliedChild:
        pid = 4
        def wait(self):
            raise RuntimeError("fixture captured in-flight phase")
    subprocess.Popen = lambda *args, **kwargs: SuppliedChild()
sys.argv = ["relay", status, "/compat/missing" if variant == "launch-failed" else "/bin/true"]
try:
    exec(compile(source, "<attested-profile-relay>", "exec"), {"__name__": "__main__"})
except RuntimeError:
    if variant != "launched": raise
'''
            completed = subprocess.run([sys.executable, "-I", "-S", "-c", bootstrap, source, str(path), variant],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
            assert completed.returncode == (1 if variant == "launch-failed" else 0), completed.stderr
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = session.exit_status()
            assert result["phase"] == {"launch-failed": "launch_failed", "launched": "launched", "ended": "ended"}[variant]
            return result
    return {"raw": base64.b64encode(path.read_bytes()).decode(),
            "fixture_facts": ["supplied monotonic/process observations for embedded writer; no process lifetime or containment proof"]}
