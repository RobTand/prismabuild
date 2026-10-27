"""Measure replay stages inside a PB-admitted action, using private driver copies."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import platform
import shutil
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests/compat/support")]
import record_compat as replay


STAGE_FIELDS = {"compile_source", "profile_hook", "profile_calls", "setup",
                "adapter_with_attestation_hooks", "final_attestation"}


@contextmanager
def profiling_drivers(work, roots):
    original_here, original_driver_for = replay.HERE, replay.driver_for
    selected = {root: original_driver_for(root) for root in roots}
    sources = dict.fromkeys([original_here, *selected.values()])
    copies = {}
    for index, source in enumerate(sources):
        driver = work / f"driver-{index}"
        shutil.copytree(source, driver, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        isolate = driver / "compat_isolate.py"
        isolate.write_text(instrument_driver(isolate.read_text()))
        copies[source] = driver

    def driver_for(root):
        return copies[selected[root]]

    try:
        replay.HERE = copies[original_here]
        replay.driver_for = driver_for
        yield
    finally:
        replay.HERE, replay.driver_for = original_here, original_driver_for


 def_placeholder
