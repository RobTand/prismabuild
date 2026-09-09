"""A finish budget must reach recent recorder samples before old day rows."""
import io

import pytest

from prismabuild import box_window


def test_recent_window_survives_a_budget_spent_on_old_rows(monkeypatch, tmp_path):
    path = tmp_path / 'day.csv'
    path.write_text('epoch_ms,power_draw_w\n' + '1000,1\n' * 1000
                    + '20000,30\n21000,50\n')
    monkeypatch.setattr(box_window, '_csv_files', lambda *args: [path])
    clock = [0.0]
    monkeypatch.setattr(box_window.time, 'monotonic', lambda: clock[0])
    number = box_window._number

    def parse(cell):
        clock[0] += 0.01
        return number(cell)

    monkeypatch.setattr(box_window, '_number', parse)
    series, errors = box_window._pqteld_series(tmp_path, 'test', 20, 21, expires=0.1)
    assert series['power_draw_w'].count == 2
    assert series['power_draw_w'].mean == 40
    assert any('deadline' in error for error in errors)


def test_latest_file_gets_the_budget_first(monkeypatch, tmp_path):
    older, newer = tmp_path / 'older.csv', tmp_path / 'newer.csv'
    older.write_text('epoch_ms,power_draw_w\n' + '1000,1\n' * 1000)
    newer.write_text('epoch_ms,power_draw_w\n20000,30\n')
    monkeypatch.setattr(box_window, '_csv_files', lambda *args: [older, newer])
    clock = [0.0]
    monkeypatch.setattr(box_window.time, 'monotonic', lambda: clock[0])
    number = box_window._number

    def parse(cell):
        clock[0] += 0.01
        return number(cell)

    monkeypatch.setattr(box_window, '_number', parse)
    series, errors = box_window._pqteld_series(tmp_path, 'test', 20, 21, expires=0.1)
    assert series['power_draw_w'].count == 1
    assert series['power_draw_w'].mean == 30
    assert errors


def test_clock_reversals_and_block_boundaries_preserve_all_aggregates(monkeypatch, tmp_path):
    # Tiny blocks cut both rows and CRLF pairs. A late out-of-window row must
    # not hide earlier matching rows, nor may an early one stop the scan.
    monkeypatch.setattr(box_window, 'CSV_READ_BLOCK_BYTES', 7, raising=False)
    path = tmp_path / 'day.csv'
    path.write_bytes(b'epoch_ms,power_draw_w,MemTotal\r\n'
                     b'1000,10,100\r\n9000,999,900\r\n'
                     b'2000,30,200\r\n500,999,500\r\n3000,20,300\r\n')
    monkeypatch.setattr(box_window, '_csv_files', lambda *args: [path])
    series, errors = box_window._pqteld_series(tmp_path, 'test', 1, 3, expires=float('inf'))
    assert not errors
    power = series['power_draw_w']
    assert (power.count, power.mean, power.low, power.high, power.last) == (3, 20, 10, 30, 20)
    assert series['MemTotal'].last == 300


def test_last_measured_cell_stays_last_across_files(monkeypatch, tmp_path):
    older, newer = tmp_path / 'older.csv', tmp_path / 'newer.csv'
    older.write_text('epoch_ms,power_draw_w,MemTotal\n1000,10,100\n2000,20,200\n')
    newer.write_text('epoch_ms,power_draw_w,MemTotal\n3000,30,300\n2000,40,\n')
    monkeypatch.setattr(box_window, '_csv_files', lambda *args: [older, newer])
    series, errors = box_window._pqteld_series(tmp_path, 'test', 1, 3, expires=float('inf'))
    assert not errors
    assert series['MemTotal'].last == 300
    assert series['power_draw_w'].last == 40
    assert series['power_draw_w'].count == 4


@pytest.mark.parametrize('block_size', [1, 2, 7, 65536])
@pytest.mark.parametrize('ending', [b'', b'\n', b'\r\n'])
def test_long_and_ragged_rows_do_not_hide_adjacent_samples(monkeypatch, tmp_path, block_size, ending):
    monkeypatch.setattr(box_window, 'CSV_READ_BLOCK_BYTES', block_size)
    path = tmp_path / 'day.csv'
    path.write_bytes(b'epoch_ms,power_draw_w,unused\n1000,10,' + b'x' * 100003
                     + b'\n2000,99\n2000,20,ok' + ending)
    monkeypatch.setattr(box_window, '_csv_files', lambda *args: [path])
    series, errors = box_window._pqteld_series(tmp_path, 'test', 1, 3, expires=float('inf'))
    assert not errors
    assert series['power_draw_w'].count == 2
    assert series['power_draw_w'].mean == 15
    assert series['power_draw_w'].last == 20


@pytest.mark.parametrize('content', [b'', b'epoch_ms,power_draw_w', b'epoch_ms,power_draw_w\n'])
def test_empty_file_or_header_has_no_data(monkeypatch, tmp_path, content):
    path = tmp_path / 'day.csv'
    path.write_bytes(content)
    monkeypatch.setattr(box_window, '_csv_files', lambda *args: [path])
    series, _ = box_window._pqteld_series(tmp_path, 'test', 1, 3, expires=float('inf'))
    assert series['power_draw_w'].count == 0


def test_append_after_eof_capture_is_left_for_a_later_read(monkeypatch, tmp_path):
    class Stream(io.BytesIO):
        def read(self, size):
            position = self.tell()
            super().seek(0, 2)
            self.write(b'2000,999\n')
            super().seek(position)
            return super().read(size)

    class Recorder:
        name = 'day.csv'

        def open(self, *args):
            return Stream(b'epoch_ms,power_draw_w\n1000,10\n')

    monkeypatch.setattr(box_window, '_csv_files', lambda *args: [Recorder()])
    series, errors = box_window._pqteld_series(tmp_path, 'test', 1, 3, expires=float('inf'))
    assert not errors
    assert series['power_draw_w'].count == 1
    assert series['power_draw_w'].high == 10


def test_truncated_read_records_unavailable_instead_of_stitching_rows(monkeypatch, tmp_path):
    class Stream(io.BytesIO):
        def read(self, size):
            self.truncate(self.tell() + 4)
            return super().read(size)

    class Recorder:
        name = 'day.csv'

        def open(self, *args):
            return Stream(b'epoch_ms,power_draw_w\n1000,10\n')

    monkeypatch.setattr(box_window, '_csv_files', lambda *args: [Recorder()])
    window = box_window.read_window(1, 3, host='test', csv_dir=tmp_path, netdata_url=None)
    assert window['source'] == 'unavailable'
    assert 'OSError' in window['reason']
