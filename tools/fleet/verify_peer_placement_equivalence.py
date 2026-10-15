"""Compare fixed-input claim histories against a retained Git base, inside PB."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def replay(source, history):
    environment = dict(os.environ, PYTHONPATH=os.pathsep.join(
        (str(source / "src"), str(source / "tests"))))
    result = subprocess.run([sys.executable, str(history)], cwd=source, env=environment,
                            capture_output=True, text=True)
    if result.returncode:
        print(f"peer replay failed for {source}", file=sys.stderr)
        print(result.stdout, file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        result.check_returncode()
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="1abcd5db95bb37fd33d6f72902108b539a51eb0d")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[2]
    history = source / "tests" / "test_peer_placement_histories.py"
    with tempfile.TemporaryDirectory(prefix="pb-peer-base-") as temporary:
        base = Path(temporary)
        archive = base / "base.tar"
        subprocess.run(["git", "archive", "--format=tar", "--output", str(archive), args.base],
                       cwd=source, check=True)
        subprocess.run(["tar", "-xf", str(archive), "-C", str(base)], check=True)
        before = replay(base, history)
        after = replay(source, history)
    differences = {name: {"base": before.get(name), "candidate": after.get(name)}
                   for name in sorted(before.keys() | after.keys())
                   if before.get(name) != after.get(name)}
    report = {"base": args.base, "histories": len(before), "differences": differences,
              "scope": "fixed observations plus explicit scratch replacement; no concurrent equivalence claim"}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, sort_keys=True))
    return bool(differences)


if __name__ == "__main__":
    raise SystemExit(main())
