"""Capture a read-only census or validate an explicitly designated rollback window.

Capture is source/data inspection. Run validation through published PrismaBuild;
it reads actual runtime sources and imports the candidate window contract.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def immutable(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    with path.open("xb") as stream:
        stream.write(raw)
    path.chmod(0o444)


def capture(out):
    out.mkdir(parents=True, mode=0o700, exist_ok=False)
    started = time.time()
    result = subprocess.run([sys.executable, "/mnt/shared/prismabuild-fleet/repo/tools/pbstatus.py",
                             "--json", "--transport", "pool", "--recent", "0", "--timeout-s", "30"],
                            capture_output=True, timeout=45)
    path = out / "status.json"
    with path.open("xb") as stream:
        stream.write(result.stdout)
    path.chmod(0o400)
    (out / "capture-stderr.log").write_bytes(result.stderr)
    status = json.loads(result.stdout)
    envelope = {"captured_unix": started, "finished_unix": time.time(),
                "captured_at": datetime.fromtimestamp(started, timezone.utc).isoformat(),
                "status_file": path.name, "status_sha256": sha(result.stdout),
                "status_exit": result.returncode, "complete": status.get("complete") is True,
                "capture_tool": "/mnt/shared/prismabuild-fleet/repo/tools/pbstatus.py"}
    immutable(out / "capture.json", envelope)
    print(json.dumps(envelope, sort_keys=True))
    if result.returncode or not envelope["complete"]:
        raise SystemExit(2)


def validate(args):
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root / "src"))
    from prismabuild.compatibility_window import (
        WindowError, require_generation, require_predeploy, require_reachable,
    )
    envelope = json.loads(args.census.read_bytes())
    path = args.census.parent / envelope["status_file"]
    raw = path.read_bytes()
    if sha(raw) != envelope["status_sha256"] or envelope["status_exit"] != 0:
        raise WindowError("census provenance or completion changed")
    status = json.loads(raw)
    window = json.loads(args.window.read_bytes())
    designation = json.loads(args.designation.read_bytes())
    rollback = designation["rollback"]
    if any(window["rollback"][key] != rollback[key] for key in ("generation", "commit")):
        raise WindowError("captured window relabeled the operator's rollback target")
    hosts = require_predeploy(window, status, captured_unix=envelope["captured_unix"])
    target = args.generations / rollback["generation"]
    require_generation(target, rollback)
    facts = require_reachable(args.candidate, target)
    if designation.get("requires_no_proof_member") and "park_proof.py" in facts["candidate"]["present"]:
        raise WindowError("a174 designation does not authorize a B09 proof-member deployment")
    immutable(args.out, {"hosts": hosts, "rollback": rollback, "protocols": facts,
                         "census_sha256": sha(args.census.read_bytes()),
                         "window_sha256": sha(args.window.read_bytes()),
                         "designation_sha256": sha(args.designation.read_bytes()),
                         "result": "source-window validation only; no publication or physical rollback"})
    print(f"validated {len(hosts)} live hosts and the designated rollback bytes")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    census = commands.add_parser("capture")
    census.add_argument("--out", type=Path, required=True)
    check = commands.add_parser("validate")
    for name in ("census", "window", "designation", "generations", "candidate", "out"):
        check.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    if args.command == "capture":
        capture(args.out)
    else:
        validate(args)


if __name__ == "__main__":
    main()
