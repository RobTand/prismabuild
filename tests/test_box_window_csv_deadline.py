"""A recorder read may spend the budget; it must not authorize another read."""
import io

import pytest

from prismabuild import box_window


class Recorder:
    name = "pqteld-test-20231114.s2.csv"

    def __init__(self, clock, *, open_cost=0, read_cost=0):
        self.clock = clock
        self.open_cost = open_cost
        self.read_cost = read_cost
        self.opens = 0
        self.reads = 0

    def open(self, *args, **kwargs):
        self.opens += 1
        self.clock[0] += self.open_cost
        recorder = self

        class Stream(io.BytesIO):
            def readline(self, *args):
                recorder.reads += 1
                line = super().readline(*args)
                recorder.clock[0] += recorder.read_cost
                return line

            def read(self, *args):
                recorder.reads += 1
                data = super().read(*args)
                recorder.clock[0] += recorder.read_cost
                return data

        return Stream(b"epoch_ms,power_draw_w\n1000,10\n2000,90\n3000,20\n")


@pytest.mark.parametrize("now", [10, 11])
def test_expired_window_does_not_discover_files(monkeypatch, tmp_path, now):
    monkeypatch.setattr(box_window.time, "monotonic", lambda: now)
    discoveries = []
    monkeypatch.setattr(box_window, "_csv_files",
                        lambda *args: discoveries.append(args) or [])
    _, errors = box_window._pqteld_series(tmp_path, "test", 1, 3, expires=10)
    assert not discoveries
    assert any("deadline" in error for error in errors)


def test_discovery_that_spends_the_budget_does_not_open(monkeypatch, tmp_path):
    clock = [0]
    recorder = Recorder(clock)
    monkeypatch.setattr(box_window.time, "monotonic", lambda: clock[0])

    def discover(*args):
        clock[0] = 10
        return [recorder]

    monkeypatch.setattr(box_window, "_csv_files", discover)
    _, errors = box_window._pqteld_series(tmp_path, "test", 1, 3, expires=10)
    assert recorder.opens == 0
    assert any("deadline" in error for error in errors)


def test_open_that_spends_the_budget_does_not_read_header(monkeypatch, tmp_path):
    clock = [0]
    recorder = Recorder(clock, open_cost=10)
    monkeypatch.setattr(box_window.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(box_window, "_csv_files", lambda *args: [recorder])
    _, errors = box_window._pqteld_series(tmp_path, "test", 1, 3, expires=10)
    assert recorder.opens == 1
    assert recorder.reads == 0
    assert any("deadline" in error for error in errors)


def test_row_that_spends_budget_keeps_sample_but_starts_no_next_row(monkeypatch, tmp_path):
    clock = [0]
    recorder = Recorder(clock, read_cost=1)
    monkeypatch.setattr(box_window.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(box_window, "_csv_files", lambda *args: [recorder])
    window = box_window.read_window(1, 3, host="test", csv_dir=tmp_path,
                                    netdata_url=None, deadline_s=2)
    assert recorder.reads == 2  # header and one tail block
    assert window["gpu"]["samples"] == 1
    assert window["gpu"]["power_w_peak"] == 20
    assert any("deadline" in error for error in window["errors"])


def test_unexpired_scan_keeps_rows_after_clock_reversal(monkeypatch, tmp_path):
    path = tmp_path / "pqteld-test-19700101.s2.csv"
    path.write_text("epoch_ms,power_draw_w\n1000,10\n9000,999\n2000,90\n")
    monkeypatch.setattr(box_window, "_csv_files", lambda *args: [path])
    monkeypatch.setattr(box_window.time, "monotonic", lambda: 0)
    series, errors = box_window._pqteld_series(tmp_path, "test", 1, 3, expires=10)
    assert not errors
    assert series["power_draw_w"].count == 2
    assert series["power_draw_w"].high == 90
