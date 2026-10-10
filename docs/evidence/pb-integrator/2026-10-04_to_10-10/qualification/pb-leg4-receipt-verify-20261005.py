#!/usr/bin/env python3
"""Receipt-only verification of the original canary leg-4 pair (no submission, read-only).

Replays pbcanary._execute_side's envelope shape and calls the unchanged leg4.verify.
"""
import hashlib, json, sys
from pathlib import Path
SRC = "/mnt/shared/prismabuild-fleet/repo/src"           # published prismabuild.core (live generation)
TOOLS = "/home/rob/prismabuild-wt/pbint-sched/tools/fleet"  # unchanged leg4 + common (hash checked below)
CAS = Path("/mnt/shared/prismabuild-fleet/cas")
KEYS = {"sparky": "132a2eaed565c62839980b30f61fb68afa1d65fc8944dee4060e6dc30e5e6405",
        "sparklina": "bd437d7754c27bea4e6e972525beef1052959898b59c12fc5bf16e068c3ac759"}
EXPECTED = {"input_digest": "884c2b9e2c9309a13cbc07c24e14695333ced791fd816df205661faf9570d1fc"}
sys.path.insert(0, SRC); sys.path.insert(0, TOOLS)
leg4_path = Path(TOOLS) / "pbcanary_legs" / "leg4.py"
print("leg4.py sha256:", hashlib.sha256(leg4_path.read_bytes()).hexdigest()[:16], "(main and every checkout: 936dae29e6e3e648)")
from prismabuild import core as pb
from pbcanary_legs import leg4
cas = pb.PrismaBuildCAS(CAS)
envs = []
for side, key in KEYS.items():
    action = json.loads((CAS / "requests" / key[:2] / f"{key}.json").read_text())
    receipt = cas.lookup(action)
    if receipt is None:
        print(side, key[:12], "CAS lookup EMPTY"); sys.exit(2)
    blob = Path(cas.result_path(receipt, action)).read_bytes()
    artifact = blob.decode("utf-8")
    print(side, key[:12], "receipt ok; result blob %d bytes sha256 %s" % (len(blob), hashlib.sha256(blob).hexdigest()[:16]))
    envs.append({"action_key": key, "receipt": receipt, "artifact": artifact, "stdout": artifact,
                 "progress_observation": None, "progress_attempt": None})
ok, reason = leg4.verify(envs[0], envs[1], EXPECTED)
print("leg4.verify ->", ok, "|", reason)
sys.exit(0 if ok else 1)
