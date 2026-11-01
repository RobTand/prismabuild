#!/usr/bin/env python3
"""Compare repeated test runs with a CPU-bound sibling inside one PB action."""
from __future__ import annotations

import argparse
import io
import json
import multiprocessing
import os
from pathlib import Path
import platform
import re
import runpy
import subprocess
import sys
import tarfile
import tempfile
import time
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild.digest_primitives import raw_sha256
from prismabuild.pytest_temp import pytest_basetemp


def _load(stop, ready):
    ready.set()
    value = b"test-time-isolation"
    while not stop.is_set():
        value = raw_sha256(value).encode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref")
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--xdist", type=int, default=0)
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    if not os.environ.get("PRISMABUILD_ACTION_KEY"):
        parser.error("submit this qualification through the published pbrun.py")
    if args.repeats < 1 or args.xdist < 0 or args.xdist > 4:
        parser.error("positive repeats and zero to four xdist workers are required")
    print(json.dumps({"kind": "environment", "python": sys.version,
                      "host": platform.node(), "machine": platform.machine(),
                      "affinity": sorted(os.sched_getaffinity(0)),
                      "native_threads": {key: os.environ.get(key) for key in (
                          "OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS")}}),
          flush=True)
    helper = os.environ.get("PRISMABUILD_ACTION_PROGRESS_HELPER")
    commit = runpy.run_path(helper)["commit"] if helper else lambda *_: None
    root = Path(__file__).resolve().parents[1]
    evidence = root / "test-time-isolation-results"
    evidence.mkdir()
    with tempfile.TemporaryDirectory(prefix="pb-time-isolation-") as scratch:
        arms = []
        refs = [("before", args.baseline_ref)] if args.baseline_ref else []
        refs.append(("after", "HEAD"))
        for label, ref in refs:
            tree = Path(scratch) / label
            tree.mkdir()
            archive = subprocess.check_output(["git", "archive", ref], cwd=root)
            with tarfile.open(fileobj=io.BytesIO(archive)) as contents:
                contents.extractall(tree, filter="data")
            arms.append((label, tree))
        context = multiprocessing.get_context("spawn")
        stop, ready = context.Event(), context.Event()
        sibling = context.Process(target=_load, args=(stop, ready))
        sibling.start()
        failures = 0
        units = 0
        try:
            ready.wait()
            for repeat in range(1, args.repeats + 1):
                for label, tree in arms:
                    junit = evidence / f"{label}-{repeat}.xml"
                    command = [sys.executable, "-m", "pytest", "-q",
                               f"--junitxml={junit}", *args.files]
                    if args.xdist:
                        command.extend(["-n", str(args.xdist)])
                    environment = dict(os.environ, PYTHONPATH=str(tree / "src"),
                                       PYTEST_ADDOPTS="")
                    load_before = os.getloadavg()
                    started = time.monotonic() if args.baseline_ref else None
                    with pytest_basetemp() as base:
                        command.append(f"--basetemp={base}")
                        result = subprocess.run(command, cwd=tree, env=environment,
                                                text=True, stdout=subprocess.PIPE,
                                                stderr=subprocess.STDOUT)
                    seconds = time.monotonic() - started if started is not None else None
                    log = evidence / f"{label}-{repeat}.log"
                    with log.open("w") as output:
                        output.write(result.stdout)
                        output.flush()
                        os.fsync(output.fileno())
                    cases = list(ET.parse(junit).iter("testcase")) if junit.exists() else []
                    counts = {name: sum(case.find(name) is not None for case in cases)
                              for name in ("failure", "error", "skipped")}
                    qualified = (result.returncode == 0 and bool(cases) and sibling.is_alive()
                                 and not any(counts.values())
                                 and not re.search(r"\b\d+ xpassed\b", result.stdout))
                    failures += not qualified
                    summary = [line for line in result.stdout.splitlines()
                               if " passed" in line or " failed" in line or "ERROR" in line]
                    if junit.exists():
                        with junit.open("rb") as outcome:
                            os.fsync(outcome.fileno())
                    record = {"kind": "run", "arm": label, "repeat": repeat,
                              "returncode": result.returncode, "seconds": seconds,
                              "load_before": load_before, "load_after": os.getloadavg(),
                              "sibling_alive": sibling.is_alive(), "summary": summary,
                              "tests": len(cases), **counts, "qualified": qualified,
                              "stdout_sha256": raw_sha256(log.read_bytes())}
                    with (evidence / "outcomes.jsonl").open("a") as outcomes:
                        outcomes.write(json.dumps(record) + "\n")
                        outcomes.flush()
                        os.fsync(outcomes.fileno())
                    print(json.dumps(record), flush=True)
                    for line in result.stdout.splitlines():
                        if line.startswith("prismabuild-test-runtime:"):
                            print(line, flush=True)
                    if not qualified:
                        print(result.stdout, flush=True)
                    units += 1
                    commit(units)
        finally:
            stop.set()
            sibling.join()
        print(json.dumps({"kind": "complete", "runs": units, "failures": failures,
                          "sibling_exitcode": sibling.exitcode}), flush=True)
        return int(bool(failures) or sibling.exitcode != 0)


if __name__ == "__main__":
    raise SystemExit(main())
