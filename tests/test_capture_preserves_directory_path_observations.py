"""A capture must qualify the directory the consumer actually scans."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from kept_read_clock import after_paths
from prismabuild import pool
import stage_move
from test_a_remounted_device_is_not_trusted import table  # noqa: F401


@pytest.mark.parametrize('spelling', ['plain', 'symlink-parent'])
def test_partial_names_use_the_scanned_directory_observation(
        tmp_path, monkeypatch, table, spelling):
    nominal = tmp_path / 'nominal'
    nominal.mkdir()
    actual = tmp_path / 'actual'
    (actual / 'deep').mkdir(parents=True)
    (actual / 'range').mkdir()
    (nominal / 'range').mkdir()
    (nominal / 'link').symlink_to(actual / 'deep', target_is_directory=True)
    directory = (actual / 'range' if spelling == 'plain' else
                 nominal / 'link' / '..' / 'range')
    normalized = Path(os.path.normpath(str(directory)))
    destination = directory / 'shard.safetensors'
    destination.write_bytes(b'existing staged bytes')
    publisher = stage_move._StagedPublisher(
        queue=pool.PoolQueue(tmp_path / 'queue'), stage_root=directory,
        residency_root=tmp_path / 'residency', mover_action_key='a' * 64,
        manifest_sha256='b' * 64, tier_id='test-stage', cas_root=tmp_path / 'cas')
    after_paths(monkeypatch, directory, normalized, destination)
    if spelling == 'symlink-parent':
        assert os.stat(directory).st_ino != os.stat(normalized).st_ino
    normalized_before = os.stat(normalized)
    actual_before = os.stat(directory)
    real_scan = os.scandir
    scans = []

    def scan(path='.'):
        if not isinstance(path, int) and os.fsdecode(path) == str(directory):
            scans.append(str(path))
        return real_scan(path)

    monkeypatch.setattr(os, 'scandir', scan)
    assert publisher._inflight_partials(destination) == []
    assert scans == [str(directory)]
    assert publisher._inflight_partials(destination) == []
    assert scans == [str(directory)], 'a valid unchanged capture must still hit'

    foreign = directory / f'.{destination.name}.{"c" * 16}.partial'
    foreign.write_bytes(b'foreign copy in flight')
    # Make the changed directory observation deterministic even when the
    # filesystem assigns both real writes one coarse timestamp.
    os.utime(directory, ns=(actual_before.st_atime_ns, actual_before.st_mtime_ns + 1))
    assert foreign.read_bytes() == b'foreign copy in flight'
    assert os.stat(directory).st_mtime_ns != actual_before.st_mtime_ns
    if spelling == 'symlink-parent':
        assert os.stat(normalized) == normalized_before

    result = publisher._inflight_partials(destination)
    assert result is None or result == [foreign.name], (
        'a real foreign partial must be reported or refused; cached empty is unsafe')
    if spelling == 'plain':
        assert result == [foreign.name]
