#!/usr/bin/env python3
"""Real container-stop proof for the Docker shim relay (#1599).

Run INSIDE an admitted PrismaBuild action on a box with Docker:

    pbrun.py --tag <box> ... -- python3 tools/fleet/qualify_docker_stop.py

It starts the checkout's own shim (``tools/fleet/docker``, the file beside this
one) around a real container, sends TERM to the SHIM ONLY, and then asks the
real daemon what happened.  Two workloads:

* ``term_ignoring``: a container whose main process traps and ignores TERM,
  INT and HUP.  The shim's relay cannot stop it, so after the grace it must
  kill the exact container it created.  This is the case a guard that waits on
  its launcher cannot survive.
* ``term_honoring``: a container that exits on TERM.  The shim must stop it
  without killing anything.

Each case checks, against the daemon and not against the shim's own word:
the shim returned 128+TERM; its receipt names the container the daemon says
this call created; the container is not running afterwards; and no container
of this action is left running.  The two outcomes differ as they should.

Exit 0: both cases proved.  Exit 1: a case failed.  Exit 2: nothing was tested
(not admitted, no Docker, or no image).  "Did not test" is never "passed".
The JSON report is printed to stdout, so the action's retained log carries it.
Only containers this run created, found by their own call's label, are ever
removed, and only if a case left one behind.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
SHIM = HERE / "docker"
DOCKER = "/usr/bin/docker"
IMAGE = os.environ.get("PBDOCKER_STOP_IMAGE", "alpine:3.22")
OWNER = os.environ.get("PRISMABUILD_CONTAINER_OWNER", "")
MARKER = os.environ.get("PRISMABUILD_CONTAINER_MARKER", "")
OWNER_LABEL = "prismabuild.action"
SHIM_LABEL = "prismabuild.shim"
START_WAIT_S = 60.0
STOP_WAIT_S = 60.0

CASES = {
    "term_ignoring": "trap '' TERM INT HUP; echo ready; while :; do sleep 1; done",
    "term_honoring": "trap 'exit 0' TERM; echo ready; sleep 300 & wait $!",
}


def refuse(reason: str) -> int:
    print(json.dumps({"verdict": "did_not_test", "reason": reason}))
    return 2


def docker(*args: str, timeout: float = 15.0):
    done = subprocess.run([DOCKER, *args], capture_output=True, text=True,
                          timeout=timeout, check=False)
    return done.returncode, done.stdout.strip()


def owner_containers(*, running_only: bool) -> set[str]:
    code, out = docker("ps", *([] if running_only else ["-a"]), "-q", "--no-trunc",
                       "--filter", f"label={OWNER_LABEL}={OWNER}")
    if code != 0:
        raise RuntimeError("docker ps failed: not an empty answer")
    return set(out.split())


def run_case(name: str, script: str) -> dict:
    before = owner_containers(running_only=False)
    log = Path(MARKER).with_name(f"{Path(MARKER).name}.{name}.shim.log")
    with log.open("w") as sink:
        shim = subprocess.Popen(
            [sys.executable, str(SHIM), "run", "--rm", IMAGE, "sh", "-c", script],
            stdout=sink, stderr=subprocess.STDOUT)
        case = {"case": name, "shim_pid": shim.pid, "ok": False, "failures": []}
        cid = nonce = None
        deadline = time.monotonic() + START_WAIT_S
        while time.monotonic() < deadline and shim.poll() is None:
            new = owner_containers(running_only=True) - before
            if len(new) == 1:
                cid = next(iter(new))
                code, out = docker("inspect", "-f",
                                   '{{index .Config.Labels "%s"}}' % SHIM_LABEL, cid)
                nonce = out if code == 0 and out else None
                break
            time.sleep(0.2)
        case["container_id"], case["nonce"] = cid, nonce
        if cid is not None:
            # What the daemon says about the production path: the scope label
            # and parent are present only when the action had a resource scope.
            code, out = docker("inspect", "-f",
                               '{{index .Config.Labels "prismabuild.scope"}}|'
                               '{{.HostConfig.CgroupParent}}', cid)
            case["scope_label_and_cgroup_parent"] = out if code == 0 else None
        if cid is None or nonce is None:
            shim.kill()
            case["failures"].append("the container never started under the shim")
            return case
        sent = time.time()
        shim.send_signal(signal.SIGTERM)
        try:
            code = shim.wait(timeout=STOP_WAIT_S)
        except subprocess.TimeoutExpired:
            shim.kill()
            shim.wait()
            code = None
            case["failures"].append(f"the shim outlived {STOP_WAIT_S} s")
    case["elapsed_s"] = round(time.time() - sent, 3)
    case["shim_exit_code"] = code
    if code != 128 + int(signal.SIGTERM):
        case["failures"].append(f"shim exit {code}, expected {128 + int(signal.SIGTERM)}")
    receipts = sorted(Path(MARKER).parent.glob(f"{Path(MARKER).name}.stop-{nonce[:12]}.json"))
    receipt = json.loads(receipts[-1].read_text()) if receipts else None
    case["receipt"] = receipt
    if receipt is None:
        case["failures"].append("no receipt was written")
    else:
        if receipt.get("container_id") != cid:
            case["failures"].append(
                f"receipt names {receipt.get('container_id')}, the daemon says {cid}")
        expected = "killed" if name == "term_ignoring" else "stopped"
        if receipt.get("outcome") != expected:
            case["failures"].append(f"outcome {receipt.get('outcome')}, expected {expected}")
        killed = (receipt.get("escalation") or {}).get("container_killed")
        if killed is not (name == "term_ignoring"):
            case["failures"].append(f"container_killed is {killed}")
    still = {c for c in owner_containers(running_only=True) if c == cid}
    case["container_running_after"] = bool(still)
    if still:
        case["failures"].append("the container is still running")
    case["ok"] = not case["failures"]
    return case


def main() -> int:
    if not OWNER or not MARKER:
        return refuse("not an admitted action: no container owner or marker in the environment")
    if not SHIM.is_file():
        return refuse(f"no shim beside this script: {SHIM}")
    try:
        code, _ = docker("image", "inspect", IMAGE)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return refuse(f"docker is not usable: {exc!r}")
    if code != 0:
        return refuse(f"image {IMAGE} is not present on this box")
    report = {"schema": "prismabuild.docker_stop_qualification.v1",
              "image": IMAGE, "owner": OWNER, "host": os.uname().nodename,
              "cases": []}
    try:
        for name, script in CASES.items():
            report["cases"].append(run_case(name, script))
        leftover = owner_containers(running_only=True)
        report["owner_containers_running_at_end"] = sorted(leftover)
        report["verdict"] = ("proved" if all(c["ok"] for c in report["cases"]) and not leftover
                             else "failed")
    except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError) as exc:
        report["verdict"] = "failed"
        report["error"] = repr(exc)
    print(json.dumps(report, indent=1, sort_keys=True))
    return 0 if report["verdict"] == "proved" else 1


if __name__ == "__main__":
    raise SystemExit(main())
