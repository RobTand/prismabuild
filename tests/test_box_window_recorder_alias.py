"""Recorder names follow declared machine identity across an OS rename."""
import json
import time

import pytest

from prismabuild import box_window


START = 1700000100


def recorder(directory, host, power):
    day = time.strftime("%Y%m%d", time.localtime(START))
    path = directory / f"pqteld-{host}-{day}.s2.csv"
    path.write_text(f"epoch_ms,power_draw_w\n{START * 1000},{power}\n")
    return path


def config(monkeypatch, tmp_path, boxes):
    path = tmp_path / "fleet_boxes.json"
    path.write_text(json.dumps({"boxes": boxes}))
    monkeypatch.setattr(box_window, "_FLEET_CONFIG_PATHS", (path,), raising=False)


@pytest.mark.parametrize("host", ["sparklina", "gx10-6b77"])
def test_both_explicit_names_cover_the_same_recorders(monkeypatch, tmp_path, host):
    config(monkeypatch, tmp_path, {"gx10-6b77": {"_alias": "sparklina"}})
    old = recorder(tmp_path, "gx10-6b77", 10)
    new = recorder(tmp_path, "sparklina", 20)
    recorder(tmp_path, "unrelated", 999)
    files = box_window._csv_files(tmp_path, host, START, START + 1)
    assert set(files) == {old, new}
    window = box_window.read_window(START, START + 1, host=host,
                                    csv_dir=tmp_path, netdata_url=None)
    assert window["host"] == host
    assert window["gpu"]["samples"] == 2
    assert window["gpu"]["power_w_mean"] == 15
    assert window["gpu"]["power_w_peak"] == 20


def test_no_config_keeps_only_the_exact_hostname(monkeypatch, tmp_path):
    monkeypatch.setattr(box_window, "_FLEET_CONFIG_PATHS",
                        (tmp_path / "missing",), raising=False)
    expected = recorder(tmp_path, "sparklina", 10)
    recorder(tmp_path, "gx10-6b77", 999)
    assert box_window._csv_files(tmp_path, "sparklina", START, START) == [expected]


@pytest.mark.parametrize("boxes", [
    {"old1": {"_alias": "new"}, "old2": {"_alias": "new"}},
    {"old": {"_alias": "new"}, "new": {}},
])
def test_ambiguous_names_do_not_merge_machine_evidence(monkeypatch, tmp_path, boxes):
    config(monkeypatch, tmp_path, boxes)
    for host in boxes:
        recorder(tmp_path, host, 999)
    window = box_window.read_window(START, START + 1, host="new",
                                    csv_dir=tmp_path, netdata_url=None)
    assert window["source"] == "unavailable"
    assert "ambiguous" in window["reason"]


def test_alias_is_not_a_filename_glob(monkeypatch, tmp_path):
    config(monkeypatch, tmp_path, {"test": {"_alias": "*"}})
    recorder(tmp_path, "unrelated", 999)
    window = box_window.read_window(START, START + 1, host="test",
                                    csv_dir=tmp_path, netdata_url=None)
    assert window["source"] == "unavailable"
    assert "hostname" in window["reason"]
