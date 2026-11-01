#!/usr/bin/env python3
"""Repeat the seven historical #1711 selectors with complete assertion output.

Run through pbrun with --progress-phase repeat=180. The helper commits a
completed repeat only after its stdout, JUnit XML and summary reach disk.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

from prismabuild.digest_primitives import raw_sha256
from prismabuild.pytest_temp import pytest_basetemp


SELECTORS = (
    "test_archive_is_a_complete_stable_nofollow_copy_not_merely_a_digest[symlink]",
    "test_authentic_banked_ending_is_planned_then_removed_only_by_cleanup_owner[root-lease_lost]",
    "test_apply_revalidates_every_plan_identity_and_lifetime_before_deletion[directory-replaced]",
    "test_apply_revalidates_every_plan_identity_and_lifetime_before_deletion[ready]",
    "test_real_kernel_mount_census_cannot_hide_external_banks_or_processes[unreadable]",
    "test_anonymous_inode_label_rejects_every_ascii_control[ascii-14]",
    "test_real_operator_open_fd_is_live_even_when_it_belongs_to_this_pid",
)
TEST_PATH = "tests/test_checkout_recovery_1465.py"


def _commit_repeat_summary(path: Path, value: object) -> None:
    with path.open("w") as target:
        json.dump(value, target, indent=2)
        target.write("\n")
        target.flush()
        os.fsync(target.fileno())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--label", required=True)
    parser.add_argument("--workers", type=int, default=0)
    arguments = parser.parse_args()
    if arguments.repeats < 1:
        parser.error("--repeats must be positive")
    if not 0 <= arguments.workers <= 4:
        parser.error("--workers must be between zero and four")
    commit = runpy.run_path(os.environ["PRISMABUILD_ACTION_PROGRESS_HELPER"])["commit"]
    output = Path(tempfile.mkdtemp(prefix="pb1711-repeats-"))
    metadata = {
        "label": arguments.label,
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "test_sha256": raw_sha256(Path(TEST_PATH).read_bytes()),
        "selectors": list(SELECTORS),
        "repeats_requested": arguments.repeats,
        "workers": arguments.workers,
        "python": sys.version,
        "results": [],
    }
    print("PB1711_SOURCE " + json.dumps({key: value for key, value in metadata.items() if key != "results"}), flush=True)
    for index in range(1, arguments.repeats + 1):
        junit = output / f"repeat-{index:02d}.xml"
        stdout = output / f"repeat-{index:02d}.stdout"
        command = [sys.executable, "-m", "pytest", "-vv", "--tb=long", "-rs", *[
            f"{TEST_PATH}::{name}" for name in SELECTORS], f"--junitxml={junit}"]
        if arguments.workers:
            command[3:3] = ["-n", str(arguments.workers)]
        with pytest_basetemp() as base:
            command.append(f"--basetemp={base}")
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        with stdout.open("wb") as target:
            target.write(result.stdout)
            target.flush()
            os.fsync(target.fileno())
        print(f"PB1711_BEGIN_REPEAT {index}", flush=True)
        sys.stdout.buffer.write(result.stdout)
        sys.stdout.flush()
        cases = []
        if junit.exists():
            with junit.open("rb") as source:
                os.fsync(source.fileno())
            for case in ET.parse(junit).iter("testcase"):
                verdict = "passed"
                for name in ("failure", "error", "skipped"):
                    if case.find(name) is not None:
                        verdict = name
                        break
                cases.append({"name": case.attrib["name"], "verdict": verdict})
        row = {"repeat": index, "returncode": result.returncode, "cases": cases,
               "stdout_sha256": raw_sha256(result.stdout)}
        metadata["results"].append(row)
        _commit_repeat_summary(output / "summary.json", metadata)
        print("PB1711_REPEAT " + json.dumps(row), flush=True)
        commit(index, "repeat")
    totals = {name: sum(case["verdict"] == name for row in metadata["results"] for case in row["cases"])
              for name in ("passed", "failure", "error", "skipped")}
    totals["completed_repeats"] = len(metadata["results"])
    totals["invalid_case_count_repeats"] = sum(len(row["cases"]) != len(SELECTORS) for row in metadata["results"])
    metadata["totals"] = totals
    _commit_repeat_summary(output / "summary.json", metadata)
    print("PB1711_TOTAL " + json.dumps(totals), flush=True)
    print("PB1711_SUMMARY " + json.dumps(metadata), flush=True)
    return int(any(row["returncode"] for row in metadata["results"]) or bool(totals["invalid_case_count_repeats"]))


if __name__ == "__main__":
    raise SystemExit(main())
