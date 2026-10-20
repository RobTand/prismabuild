"""Standalone pytest plugin for external consumer release actions (stdlib + pytest)."""
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import sys

import pytest

PREFIX = "consumer-gate-outcomes: "
_collected = []
_deselected = []
_reports = []
_errors = []


def pytest_itemcollected(item):
    # Observe the original population before collection-modification hooks can
    # silently remove items. Every collected item must later have full reports.
    _collected.append(item.nodeid)


def pytest_deselected(items):
    _deselected.extend(item.nodeid for item in items)


def pytest_collectreport(report):
    if not report.passed:
        _errors.append({"nodeid": report.nodeid, "outcome": report.outcome})


def pytest_runtest_logreport(report):
    _reports.append({"nodeid": report.nodeid, "when": report.when,
                     "outcome": report.outcome,
                     "wasxfail": hasattr(report, "wasxfail")})


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    root = Path(os.environ["PB_TOOLS_ROOT"]).parent
    executable = Path(sys.executable)
    record = {"schema": "prismabuild.consumer_gate.outcomes.v1",
              "generation": root.name,
              "receipt_sha256": hashlib.sha256((root / "RUNTIME_VERSION.json").read_bytes()).hexdigest(),
              "consumer_commit": os.environ["PB_CONSUMER_COMMIT"],
              "runtime": os.environ["PB_CONSUMER_RUNTIME"],
              "python": str(executable), "python_realpath": str(executable.resolve()),
              "python_sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
              "executable_path_matches_running": executable.read_bytes() == Path("/proc/self/exe").read_bytes(),
              "capture_path": str(Path(__file__).resolve()),
              "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "python_version": platform.python_version(), "machine": platform.machine(),
              "host": socket.gethostname(), "args": list(session.config.invocation_params.args),
              "collected": _collected, "deselected": _deselected,
              "collection_errors": _errors, "reports": _reports,
              "exitstatus": int(exitstatus)}
    print("\n" + PREFIX + json.dumps(record, sort_keys=True), flush=True)
