"""Portable environment qualification controls, not historical-byte acceptance."""
from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path
import platform
import sys

import pytest

from record_compat_contract import HISTORY, generations
from record_compat import ArchivalRuntime, invoke
import record_compat


@pytest.fixture
def supplied_runtime():
    running = Path('/proc/self/exe').read_bytes()
    assert Path(sys.executable).read_bytes() == running
    return ArchivalRuntime(
        path=sys.executable, resolved_path=str(Path('/proc/self/exe').resolve()),
        sha256=hashlib.sha256(running).hexdigest(), python=sys.version,
        machine=platform.machine(), bytes=len(running), elf_class=running[4],
        elf_data=running[5], elf_machine=int.from_bytes(
            running[18:20], 'little' if running[5] == 1 else 'big'))


def invoke_shape(generations, tmp_path, runtime):
    case = 'shape-table/unhashed'
    generation, root, manifest = next(row for row in generations if (row[0], case) in HISTORY)
    return invoke(root, case, tmp_path / 'writer', manifest=manifest,
                  archival_runtime=runtime)


@pytest.mark.parametrize('directory', ['first-admitted', 'second-admitted'])
def test_archival_qualification_binds_selected_tmpdir(
        generations, tmp_path, monkeypatch, supplied_runtime, directory):
    selected = str(tmp_path / directory)
    Path(selected).mkdir()
    monkeypatch.setenv('TMPDIR', selected)
    observed = invoke_shape(generations, tmp_path, supplied_runtime)
    binding = observed['archival_runtime']
    assert binding['selected']['environment_before_driver']['TMPDIR'] == selected
    assert binding['observed'] == binding['selected']
    assert binding['error'] is None and observed['returncode'] == 0, observed
    assert observed['compiled_source_code_verified']
    assert observed['catalogued_entry_verified']
    assert base64.b64decode(observed['result']['raw'])
    assert binding['driver_tmpdir']['value'] == str(tmp_path / 'writer' / 'tmp')


def test_archival_tmpdir_snapshot_survives_later_ambient_change(
        generations, tmp_path, monkeypatch, supplied_runtime):
    selected = str(tmp_path / 'selected')
    Path(selected).mkdir()
    monkeypatch.setenv('TMPDIR', selected)
    run = record_compat.subprocess.run

    def change_ambient(command, **kwargs):
        monkeypatch.setenv('TMPDIR', str(tmp_path / 'later-ambient'))
        return run(command, **kwargs)

    monkeypatch.setattr(record_compat.subprocess, 'run', change_ambient)
    observed = invoke_shape(generations, tmp_path, supplied_runtime)
    binding = observed['archival_runtime']
    assert binding['selected']['environment_before_driver']['TMPDIR'] == selected
    assert binding['observed'] == binding['selected']
    assert binding['error'] is None and observed['returncode'] == 0, observed
    assert observed['compiled_source_code_verified']
    assert os.environ['TMPDIR'] != selected


def test_archival_qualification_rejects_changed_child_tmpdir(
        generations, tmp_path, monkeypatch, supplied_runtime):
    selected, changed = (str(tmp_path / name) for name in ('selected', 'changed-child'))
    Path(selected).mkdir()
    Path(changed).mkdir()
    monkeypatch.setenv('TMPDIR', selected)
    run = record_compat.subprocess.run

    def change_child(command, **kwargs):
        return run(command, **{**kwargs, 'env': {**kwargs['env'], 'TMPDIR': changed}})

    monkeypatch.setattr(record_compat.subprocess, 'run', change_child)
    observed = invoke_shape(generations, tmp_path, supplied_runtime)
    binding = observed['archival_runtime']
    assert binding['selected']['environment_before_driver']['TMPDIR'] == selected
    assert binding['observed']['environment_before_driver']['TMPDIR'] == changed
    assert binding['observed']['environment_digest'] != binding['selected']['environment_digest']
    assert observed['returncode'] == 2 and observed['process_returncode'] != 0
    assert 'explicit qualification' in binding['error']
    assert 'result' not in observed and not (tmp_path / 'writer').exists()
