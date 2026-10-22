"""Classify a missing record-compat proc namespace before any tests run."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


PROC_NAMESPACE_REFUSAL_EXIT = 78
PROC_NAMESPACE_REFUSAL = "record-compat infrastructure refusal [bwrap_proc_mount]"


class RecordCompatNamespacePreflight:
    def pytest_collection_finish(self, session):
        if session.config.getoption("collectonly"):
            return
        if not any(item.path.name.startswith("test_record_compatibility")
                   or item.path.name in {"test_funding_wire_compatibility.py",
                                         "test_current_receipt_compat_composition.py"}
                   for item in session.items):
            return
        support = str(Path(__file__).resolve().parent / "compat/support")
        if support not in sys.path:
            sys.path.insert(0, support)
        from compat_namespace_probe import PROC_MOUNT_ERROR, compat_proc_diagnostics, probe_compat_proc

        bwrap = shutil.which("bwrap")
        if bwrap is None:
            return  # Preserve the harness's existing missing-tool failure.
        try:
            probe = probe_compat_proc(bwrap)
        except (OSError, subprocess.TimeoutExpired):
            return  # Only the completed proc-mount refusal changes the session verdict.
        if probe.returncode and PROC_MOUNT_ERROR in probe.stderr:
            pytest.exit(PROC_NAMESPACE_REFUSAL + ": " + json.dumps(compat_proc_diagnostics(probe)),
                        returncode=PROC_NAMESPACE_REFUSAL_EXIT)
