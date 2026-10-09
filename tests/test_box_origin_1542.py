"""Digest origin records name each new entry's writer once (#1542).

``box_state`` files ``<digest>.origin.json`` with ``O_EXCL`` when it first
creates an entry. The record names the resolved queue root, hostname, pid,
``argv[0]`` and creation time. A second call for the same root keeps the
first record. Entries without a record are legacy entries.
"""
from __future__ import annotations

import json
import os
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_cpu


def test_two_roots_file_two_origins_naming_base_host_pid_and_argv(tmp_path, monkeypatch):
    root = tmp_path / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    first = tmp_path / "queue-a" / "reservations" / "h"
    second = tmp_path / "queue-b" / "reservations" / "h"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    _, first_digest = adaptive_cpu.box_state(first)
    _, second_digest = adaptive_cpu.box_state(second)
    assert first_digest != second_digest
    first_record = adaptive_cpu.read_box_origin(root, first_digest)
    second_record = adaptive_cpu.read_box_origin(root, second_digest)
    for record, base in ((first_record, first), (second_record, second)):
        assert record is not None
        assert record["schema"] == "prismabuild.box_origin.v1"
        assert record["queue_root"] == str(base.resolve())
        assert record["hostname"] == socket.gethostname()
        assert record["pid"] == os.getpid()
        assert record["argv0"] and isinstance(record["argv0"], str)
        assert record["created_unix"] > 0
    origins, legacy = adaptive_cpu.census_box_origins(root)
    assert legacy == []
    assert sorted(sum(origins.values(), [])) == sorted([first_digest, second_digest])


def test_a_second_call_for_the_same_root_keeps_the_first_record(tmp_path, monkeypatch):
    root = tmp_path / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    base = tmp_path / "queue" / "reservations" / "h"
    base.mkdir(parents=True)
    _, digest = adaptive_cpu.box_state(base)
    path = root / (digest + ".origin.json")
    before = json.loads(path.read_text())
    _, again = adaptive_cpu.box_state(base)
    assert again == digest
    assert json.loads(path.read_text()) == before


def test_an_entry_without_a_record_is_a_legacy_entry(tmp_path, monkeypatch):
    root = tmp_path / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    base = tmp_path / "queue" / "reservations" / "h"
    base.mkdir(parents=True)
    directory, digest = adaptive_cpu.box_state(base)
    (directory / (digest + ".lock")).touch()
    (root / (digest + ".origin.json")).unlink()
    assert adaptive_cpu.read_box_origin(root, digest) is None
    origins, legacy = adaptive_cpu.census_box_origins(root)
    assert origins == {}
    assert legacy == [digest]
