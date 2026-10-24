"""Keep the normal steady-tier regression out of campaign benchmark scale.

#1421's retained action proves a 4 GiB OOM and records 54.1 GB of process
write_bytes, but contains no test IDs. This check does NOT attribute that
historical write count. It prevents one concrete, avoidable suite workload:
four function-scoped fixtures rebuilding tens of thousands of terminal rows
and hundreds of thousands of fragment entries each.

Capture the real fixture's builder arguments BEFORE it allocates that forest.
There are no timing/RSS thresholds, real payload copies, or pytest hook calls.
The small shape already in the regression fits these deliberately loose bounds;
the campaign-width benchmark remains available through bench_tier_cycle.py.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pytest

import test_a_steady_tier_cycle_rereads_only_what_changed as steady


class _CapturedShape(Exception):
    def __init__(self, shape: argparse.Namespace):
        super().__init__("captured before campaign fixture construction")
        self.shape = shape


def test_the_ordinary_steady_fixture_has_a_unit_test_sized_forest(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def capture(queue, stage, shape):
        raise _CapturedShape(shape)

    # The subject is shape selection, not mount trust. Keep even the initial
    # queue directories in this action's pytest scratch, never in /dev/shm.
    root = tmp_path / "steady-fixture"
    root.mkdir()
    monkeypatch.setattr(steady, "_clock_root", lambda _path: root)
    monkeypatch.setattr(steady.bench, "build_queue", capture)
    fixture = steady.loop.__wrapped__(tmp_path)
    try:
        with pytest.raises(_CapturedShape) as caught:
            next(fixture)
    finally:
        fixture.close()

    shape = caught.value.shape
    big = [int(value) for value in shape.big_fragments.split(",") if value]
    record_width = sum(getattr(shape, field) for field in (
        "done", "failed", "withdrawn", "receipts", "passes",
        "ready_noise", "claimed_noise"))
    namespace_width = (shape.empty_dirs + shape.small_dirs + len(big)
                       + shape.produced_fragment_dirs)
    fragment_entries = sum(big) + sum(
        5 * (1 + index % 9) for index in range(shape.small_dirs))
    live_entries = shape.live_consumers * shape.phases * shape.files_per_range
    footprint = dict(records=record_width, namespaces=namespace_width,
                     fragment_entries=fragment_entries, live_entries=live_entries)
    print(f"steady-tier fixture construction widths: {footprint}")
    assert record_width <= 128, footprint
    assert namespace_width <= 32, footprint
    assert fragment_entries <= 128, footprint
    assert live_entries <= 64, footprint

    # Scaling down must retain the kinds of evidence the regression needs,
    # not turn its real builder into an empty queue or a mocked cycle.
    assert all(getattr(shape, field) > 0 for field in (
        "done", "failed", "withdrawn", "receipts", "passes",
        "empty_dirs", "small_dirs", "produced_fragment_dirs",
        "ready_noise", "claimed_noise")), footprint
    assert big and min(big) > 0, footprint
    assert shape.live_consumers >= 2 and shape.phases >= 3, footprint
    assert shape.files_per_range >= 2, footprint
