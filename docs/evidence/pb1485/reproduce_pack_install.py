#!/usr/bin/env python3
"""Replay only the failed pack's install in disposable admitted-action scratch."""
from __future__ import annotations

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
import zipfile

SNAPSHOT = "b50b6763de3467c45efb4de65349ed9836a3ee71"
SOURCE = "027103d9a8417e06c7f13356e58779a313cd7088"
BUNDLE_SHA256 = "8621e09b82c8a19c622b6fe279d6ac2f0d9f8472661ac1c4178c10c53b212ebc"
BUNDLE = Path("/mnt/shared/prismabuild-fleet/cas/blobs/86") / BUNDLE_SHA256


def run(argv: list[str], *, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True)
    print(json.dumps({"argv": argv, "returncode": result.returncode,
                      "stdout": result.stdout, "stderr": result.stderr}), flush=True)
    return result


def main() -> int:
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
        python = str(overlay / "bin" / "python")
        try:
            venv.create(overlay, with_pip=True)
            run([python, "-m", "pip", "--version"], cwd=scratch,
                env=env).check_returncode()
        except (subprocess.CalledProcessError, OSError) as error:
            print(json.dumps({"bootstrap_fallback": str(error)}), flush=True)
            venv.create(overlay, with_pip=False)
            with zipfile.ZipFile(wheels / "pip-26.2.1-py3-none-any.whl") as archive:
                archive.extractall(overlay / "lib" / "python3.12" / "site-packages")
        run([python, "-m", "pip", "install", "--no-index", "--no-deps",
             "--find-links", str(wheels), "setuptools"], cwd=scratch,
            env=env).check_returncode()
        url = f"prismabuild @ git+file://{urllib.parse.quote(str(checkout))}@{SOURCE}"
        result = run([python, "-m", "pip", "install", "--no-cache-dir",
                      "--no-build-isolation", "--no-deps", url],
                     cwd=scratch, env=env)
        print(json.dumps({"scope": "install reproduction only; no pack produced",
                          "python": sys.version, "machine": platform.machine(),
                          "source": SOURCE, "snapshot": SNAPSHOT,
                          "install_returncode": result.returncode}), flush=True)
        return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
