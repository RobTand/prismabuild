"""Actual diagnostic messages and hint filenames retain their prior bytes."""

import hashlib
import json
from pathlib import Path

import pytest

import prismabuild._bounded_reader as reader
from test_pbmergeq_runtime import _duration_history_two_trees, mq


@pytest.mark.parametrize("owned", [False, True])
def test_retained_reader_message_keeps_default_sorted_json_bytes(monkeypatch, capsys, owned):
    abandoned = []
    token = reader._ANNOUNCE_RETAINED.set(True)
    try:
        if owned:
            ownership = reader.ReaderOwnership(
                123, 456, "café", "12345678-1234-1234-1234-123456789abc",
                "private-pool", "section-λ")
            reader._retain_owned_reader(123, "section-λ", reader.time.monotonic(),
                                        abandoned, ownership, "private-pool")
        else:
            monkeypatch.setattr(reader.os, "kill", lambda *_: None)
            monkeypatch.setattr(reader, "_reap_within", lambda *_: False)
            monkeypatch.setattr(reader, "_starttime_ticks", lambda _: 456)
            reader._stop_reader(123, "section-λ", reader.time.monotonic(), abandoned)
    finally:
        reader._ANNOUNCE_RETAINED.reset(token)
    captured = capsys.readouterr()
    assert captured.out == ""
    expected = "pbstatus: retained reader " + json.dumps(abandoned[0], sort_keys=True) + "\n"
    assert captured.err.encode("utf-8") == expected.encode("utf-8")
    assert "\\u03bb" in captured.err, "wire spelling retains default ASCII escaping"


def test_duration_hint_payload_and_digest_name_keep_the_prior_report_bytes(tmp_path):
    cfg, store, _, current, report, rows = _duration_history_two_trees(tmp_path)
    rows[0]["note"] = "café λ"
    report.write_text(json.dumps(rows))
    original = report.read_bytes()
    hints = mq.Runner(cfg, store).history(current)
    assert hints and hints[0] == "--history"
    expected = json.dumps([rows[0]], sort_keys=True).encode("utf-8")
    path = Path(hints[1])
    assert path.name == "duration-hints-" + hashlib.sha256(expected).hexdigest() + ".json"
    assert path.read_bytes() == expected
    assert report.read_bytes() == original
