"""The exporter publishes the same resource facts a receipt carries.

Peak memory and I/O bytes come from the live attempt telemetry, which is the
record the sampler is already writing; the box window comes from the endings,
because it is only summarised once the action has stopped.  Every one of them
is absent, not zero, on a record generated before Tier 0.

The fixture is built here rather than by subclassing ``MetricsFixture``: a
subclass would re-run that class's twenty-two tests under a second name, and a
suite whose count grew for that reason is a suite nobody can read.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbmetrics  # noqa: E402
from test_pbmetrics import NOW, _ending, _item, _offer, _samples, _write  # noqa: E402

MIB = 1024 ** 2
KEY = "f6" * 32
NONCE = "9" * 32
DONE_KEY = "d4" * 32
OTHER_DONE_KEY = "e5" * 32


class ResourceProfileMetrics(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.queue = Path(self.temporary.name) / "pb-queue"
        for name in ("ready", "claimed", "done", "failed", "withdrawn", "workers"):
            (self.queue / name).mkdir(parents=True)
        for patcher in (
            mock.patch.object(pbmetrics.time, "time", return_value=NOW),
            mock.patch.object(pbmetrics.pbstatus.time, "time", return_value=NOW),
            mock.patch.object(pool, "_now", return_value=NOW),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

        _write(self.queue / "workers" / "sparky.json", _offer("sparky", NOW - 2, gpu=True))
        _write(self.queue / "workers" / "dl380.json", _offer("dl380", NOW - 3, gpu=False))
        _write(self.queue / "claimed" / f"{KEY}.json",
               _item(KEY, host="sparky", gpu=True, published=NOW - 30,
                     claimed=NOW - 20, nonce=NONCE))
        _write(self.queue / "claimed" / f"{KEY}.lease", {
            "schema": pool.POOL_LEASE_SCHEMA_V1,
            "action_key": KEY, "heartbeat_unix": NOW - 1})
        self.telemetry = (self.queue / "reservations" / "sparky"
                          / "telemetry" / f"{KEY}.json")
        _write(self.telemetry, {
            "action_key": KEY, "nonce": NONCE, "sampled_unix": NOW - 1,
            "complete": True, "cpu_seconds": 12.0, "wall_seconds": 10.0,
            "memory_current_bytes": 3 * 1024 ** 3,
        })
        _ending(self.queue, DONE_KEY, "executed", "sparky", NOW - 50, NOW - 40, NOW - 10)
        _ending(self.queue, OTHER_DONE_KEY, "executed", "dl380", NOW - 45, NOW - 35, NOW - 5)

    def collect(self, **kwargs: object) -> str:
        return pbmetrics.collect_metrics(self.queue, now=NOW, **kwargs)

    def _amend_telemetry(self, **extra: object) -> None:
        record = json.loads(self.telemetry.read_text())
        record.update(extra)
        _write(self.telemetry, record)

    def test_live_peaks_and_io_are_exported_per_host(self) -> None:
        self._amend_telemetry(
            memory_peak_bytes=5 * 1024 ** 3,
            process_io={"source": "proc_io", "rchar": 7, "wchar": 9,
                        "read_bytes": 2 * MIB, "write_bytes": 64 * MIB,
                        "processes_observed": 3, "processes_live": 1},
        )
        text = self.collect(terminal_limit=20)
        self.assertIn(
            'prismabuild_attempt_peak_resources{host="sparky",'
            'resource="memory_peak_bytes"} 5368709120', text)
        self.assertIn(
            'prismabuild_attempt_peak_resources{host="sparky",'
            'resource="io_write_bytes"} 67108864', text)
        self.assertIn(
            'prismabuild_attempt_peak_resources{host="sparky",'
            'resource="io_read_bytes"} 2097152', text)

    def test_a_telemetry_record_without_the_profile_exports_nothing_for_it(self) -> None:
        text = self.collect(terminal_limit=20)
        self.assertFalse(
            _samples(text, "prismabuild_attempt_peak_resources"),
            "a record with no peak must not read as a host peak of zero")

    def test_the_endings_box_window_is_exported(self) -> None:
        path = self.queue / "done" / f"{DONE_KEY}.json"
        record = json.loads(path.read_text())
        record["detail"]["resource_profile"] = {
            "schema": "prismabuild.resource_profile.v1",
            "box_window": {"source": "pqteld", "gpu": {
                "source": "pqteld", "power_w_peak": 42.0,
                "power_reference_w": 140.0,
                "power_peak_fraction_of_reference": 0.3}},
        }
        _write(path, record)
        text = self.collect(terminal_limit=20)
        self.assertIn(
            'prismabuild_terminal_box_window{host="sparky",'
            'metric="gpu_power_peak_watts"} 42', text)
        self.assertIn(
            'prismabuild_terminal_box_window{host="sparky",'
            'metric="gpu_power_peak_fraction"} 0.3', text)
        self.assertFalse(
            [line for line in _samples(text, "prismabuild_terminal_box_window")
             if 'host="dl380"' in line],
            "an ending with no window must not read as a window of zero")


if __name__ == "__main__":
    unittest.main()
