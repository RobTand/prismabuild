#!/usr/bin/env python3
"""Check the pack install repair in disposable admitted-action scratch."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import urllib.parse
import venv

SNAPSHOT = "b50b6763de3467c45efb4de65349ed9836a3ee71"
SOURCE = "027103d9a8417e06c7f13356e58779a313cd7088"
BUNDLE_SHA256 = "8621e09b82c8a19c622b6fe279d6ac2f0d9f8472661ac1c4178c10c53b212ebc"
BUNDLE = Path("/mnt/shared/prismabuild-fleet/cas/blobs/86") / BUNDLE_SHA256


def run(argv: list[str], *, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True)
    print(json.dumps({"argv": argv, "returncode": result.returncode,
                      "stdout": result.stdout, "stderr": result.stderr}), flush=True)
    return result


def bootstrap_overlay(overlay: Path, *, cwd: Path, env: dict[str, str]) -> str:
    python = str(overlay / "bin" / "python")
    try:
        venv.create(overlay, with_pip=True)
        result = run([python, "-I", "-c", "import pip; print(pip.__version__)"],
                     cwd=cwd, env=env)
        result.check_returncode()
    except (subprocess.CalledProcessError, OSError) as error:
        raise SystemExit("reproduction requires pip 24.0; ensurepip/pip unavailable") from error
    if result.stdout.strip() != "24.0":
        raise SystemExit(f"reproduction requires pip 24.0; observed {result.stdout.strip()!r}")
    return python


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-requirement", action="store_true",
                        help="reproduce the original pip 24 rejection")
    args = parser.parse_args()
    if sys.version_info[:2] != (3, 12) or platform.machine() != "aarch64":
        raise SystemExit("reproduction requires Python 3.12 aarch64")
    if hashlib.sha256(BUNDLE.read_bytes()).hexdigest() != BUNDLE_SHA256:
        raise SystemExit("historical snapshot digest mismatch")
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    with tempfile.TemporaryDirectory(prefix="pri140-install-") as temporary:
        scratch = Path(temporary)
        checkout = scratch / "source"
        run(["git", "clone", "--branch", "prismabuild-snapshot", str(BUNDLE),
             str(checkout)], cwd=scratch, env=env).check_returncode()
        observed = subprocess.check_output(["git", "rev-parse", "HEAD"],
                                           cwd=checkout, env=env, text=True).strip()
        if observed != SNAPSHOT:
            raise SystemExit("historical checkout differs")
        overlay = scratch / "overlay"
        wheels = checkout / "_pb1485" / "wheels"
        python = bootstrap_overlay(overlay, cwd=scratch, env=env)
        run([python, "-m", "pip", "install", "--no-index", "--no-deps",
             "--find-links", str(wheels), "setuptools"], cwd=scratch,
            env=env).check_returncode()
        vcs_url = f"git+file://{urllib.parse.quote(str(checkout))}@{SOURCE}"
        url = (f"prismabuild @ {vcs_url}" if args.legacy_requirement
               else f"{vcs_url}#egg=prismabuild")
        result = run([python, "-m", "pip", "install", "--no-cache-dir",
                      "--no-build-isolation", "--no-deps", url],
                     cwd=scratch, env=env)
        if result.returncode:
            return result.returncode
        verification = r'''
import json, pathlib, runpy, sys
import prismabuild, prismabuild.client
evidence = runpy.run_path(sys.argv[1])["verify_install"]("prismabuild", sys.argv[2])
path = pathlib.Path(prismabuild.__file__).resolve()
if not path.is_relative_to(pathlib.Path(sys.prefix)):
    raise SystemExit("installed package escaped disposable interpreter")
if prismabuild.client.SDK_VERSION != 5:
    raise SystemExit("installed package is not SDK5")
evidence.update(sdk=prismabuild.client.SDK_VERSION, imported_file=str(path))
print(json.dumps(evidence, sort_keys=True))
'''
        verified = run([python, "-I", "-c", verification,
                        str(checkout / "tools" / "fleet" / "pbtest_pins.py"), SOURCE],
                       cwd=scratch, env=env)
        verified.check_returncode()
        print(json.dumps({"scope": "scratch install only; no pack produced",
                          "python": sys.version, "machine": platform.machine(),
                          "source": SOURCE, "snapshot": SNAPSHOT,
                          "install_returncode": result.returncode,
                          "guard": "historical snapshot verify_install; not both-host pack qualification"}),
              flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
