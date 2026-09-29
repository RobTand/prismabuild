"""``pbtest --shards N`` must not keep N pbrun clients resident (#1348).

Each shard is a ``pbrun`` client of about 80 MB that lives for the shard's
whole wait, so an unbounded fan-out made the submitting box's RAM grow with
the shard count.  The submitter now holds at most ``--max-clients`` at once
and starts the next shard as a slot frees.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys
import threading
import time

from pbtest_shard_output import ShardProcess, shard_output_for  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest", ROOT / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]

PUBLISHED_RUNTIME = Path(
    "/mnt/shared/prismabuild-fleet/runtime-generations/test-generation")


class Tracker:
    """Counts stand-in clients that have started and not yet exited."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.live = 0
        self.peak = 0
        self.started = 0

    def start(self) -> None:
        with self.lock:
            self.live += 1
            self.started += 1
            self.peak = max(self.peak, self.live)

    def stop(self) -> None:
        with self.lock:
            self.live -= 1


def run_suite(tmp_path: Path, monkeypatch, tracker: Tracker, shards: int,
              extra: list[str]) -> int:
    checkout = tmp_path / "checkout"
    (checkout / "tests").mkdir(parents=True)
    for number in range(shards):
        (checkout / "tests" / f"test_f{number}.py").write_text(
            "def test_one():\n    assert True\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    class Client(ShardProcess):
        returncode = 0

        def __init__(self, command) -> None:
            self.command = command
            tracker.start()

        def communicate(self):
            return shard_output_for(self.command), None

        def wait(self, timeout=None):
            # A client stays resident while its shard runs.
            time.sleep(0.05)
            tracker.stop()
            return 0

    monkeypatch.setattr(
        pbtest.subprocess, "Popen", lambda command, **_kw: Client(command))
    monkeypatch.setattr(pbtest, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
         "--shards", str(shards), *extra, "tests"])
    return pbtest.main()


def test_default_fan_out_keeps_a_bounded_number_of_clients_resident(
    tmp_path: Path, monkeypatch,
) -> None:
    tracker = Tracker()
    assert run_suite(tmp_path, monkeypatch, tracker, 24, []) == 0
    assert tracker.started == 24
    assert tracker.peak <= pbtest.DEFAULT_MAX_CLIENTS


def test_max_clients_flag_sets_the_bound(tmp_path: Path, monkeypatch) -> None:
    tracker = Tracker()
    assert run_suite(tmp_path, monkeypatch, tracker, 6, ["--max-clients", "2"]) == 0
    assert tracker.started == 6
    assert tracker.peak <= 2
