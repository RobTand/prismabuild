"""The box's admission directory is bounded by the box itself (#1542 item 1).

``box_state`` files a digest per ledger-and-host identity, and each digest owns a
``.lock``, an ``.adaptive-cpu-v1`` directory, a ``.sweep`` marker and, for the
loops that use them, a ``.measurement-reader-v1``, ``.guard`` and ``.preemption``
file.  Nothing removed them: on 2026-10-05 dl380g10 held 57,210 entries (about
183,600 inodes) and a full inode table stopped every CPU action there for 37
minutes.  Removing a digest is safe by the module's own rule, a missing file is
"no information", with one exception that the lock test of
``test_box_state_stays_out_of_the_boxs_own_directory`` names: a lock a live loop
holds is never unlinked.

Every test uses its own directory and its own clock.  Nothing touches the fleet's.
"""
from __future__ import annotations

import fcntl
import hashlib
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import adaptive_cpu

NOW = 1_800_000_000.0
DAY = 86400.0
SUFFIXES = ('.lock', '.sweep', '.guard', '.preemption', '.measurement-reader-v1')


def _digest(name: str) -> str:
    return hashlib.sha256(name.encode()).hexdigest()


def _age(path: Path, seconds: float) -> None:
    stamp = NOW - seconds
    os.utime(path, (stamp, stamp), follow_symlinks=False)


def _digest_files(directory: Path, digest: str, age_s: float) -> list[Path]:
    """One box identity's state, every file and the adaptive directory aged."""
    made = []
    for suffix in SUFFIXES:
        path = directory / (digest + suffix)
        path.write_text('x')
        made.append(path)
    state = directory / (digest + '.adaptive-cpu-v1')
    (state / 'telemetry').mkdir(parents=True)
    (state / 'cpu-sample.json').write_text('{}')
    (state / 'telemetry' / ('a' * 64 + '.json')).write_text('{}')
    made += [state / 'telemetry' / ('a' * 64 + '.json'), state / 'cpu-sample.json',
             state / 'telemetry', state]
    for path in made:
        _age(path, age_s)
    return made + [state]


def _names(directory: Path) -> set[str]:
    return {entry.name[:64] for entry in directory.iterdir() if len(entry.name) > 64}


@pytest.fixture
def directory(tmp_path):
    root = tmp_path / 'admission'
    root.mkdir(mode=0o700)
    return root


def _prune(directory, keep=None, **kw):
    return adaptive_cpu.prune_box_state(directory, keep or _digest('keep'), now=NOW, **kw)


def test_a_digest_idle_past_the_age_bound_is_removed_whole(directory):
    old = _digest('old')
    _digest_files(directory, old, 8 * DAY)
    assert _prune(directory) == 1
    assert old not in _names(directory)
    assert not [e for e in directory.iterdir() if e.name.startswith(old)], "a part of it stayed"


def test_a_digest_written_inside_the_bound_is_kept(directory):
    young = _digest('young')
    _digest_files(directory, young, 6 * DAY)
    assert _prune(directory) == 0
    assert young in _names(directory)


def test_a_recent_write_deep_in_the_adaptive_directory_keeps_the_digest(directory):
    """A live loop rewrites ``cpu-sample.json``; the directory entry itself stays old."""
    busy = _digest('busy')
    files = _digest_files(directory, busy, 30 * DAY)
    sample = directory / (busy + '.adaptive-cpu-v1') / 'cpu-sample.json'
    _age(sample, 60)
    assert sample in files
    assert _prune(directory) == 0
    assert busy in _names(directory)


def test_the_callers_own_digest_is_never_removed(directory):
    keep = _digest('keep')
    _digest_files(directory, keep, 90 * DAY)
    assert _prune(directory, keep) == 0
    assert keep in _names(directory)


def test_a_lock_a_loop_holds_keeps_its_digest(directory):
    held = _digest('held')
    _digest_files(directory, held, 90 * DAY)
    descriptor = os.open(directory / (held + '.lock'), os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert _prune(directory) == 0
        assert held in _names(directory)
    finally:
        os.close(descriptor)
    assert _prune(directory) == 1, "the control: released, the same digest goes"


def test_past_the_count_bound_the_oldest_idle_digests_go_first(directory):
    ages = {_digest(f'd{i}'): (2 + i) * 3600.0 for i in range(6)}     # 2h .. 7h
    for digest, age in ages.items():
        _digest_files(directory, digest, age)
    assert _prune(directory, max_digests=3) == 3
    left = _names(directory)
    assert left == set(sorted(ages, key=ages.get)[:3]), "not the oldest three"


def test_the_count_bound_spares_what_is_younger_than_the_floor(directory):
    fresh = [_digest(f'f{i}') for i in range(5)]
    for digest in fresh:
        _digest_files(directory, digest, 120.0)
    assert _prune(directory, max_digests=2) == 0
    assert _names(directory) == set(fresh)


def test_one_pass_removes_at_most_its_batch(directory):
    for i in range(5):
        _digest_files(directory, _digest(f'b{i}'), 20 * DAY)
    assert _prune(directory, limit=2) == 2
    assert len(_names(directory)) == 3
    assert _prune(directory, limit=2) == 2
    assert _prune(directory, limit=2) == 1


def test_names_that_are_not_a_digest_and_links_are_left_alone(directory, tmp_path):
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'precious').write_text('x')
    link = directory / (_digest('link') + '.adaptive-cpu-v1')
    link.symlink_to(outside)
    stray = directory / 'notes.txt'
    stray.write_text('x')
    short = directory / 'abc.lock'
    short.write_text('x')
    for path in (stray, short):
        _age(path, 90 * DAY)
    _age(link, 90 * DAY)
    _prune(directory)
    assert (outside / 'precious').exists(), "followed a symlink out of the directory"
    assert stray.exists() and short.exists()


def test_a_second_pass_inside_the_interval_does_nothing(directory, monkeypatch):
    monkeypatch.setattr(adaptive_cpu, 'BOX_STATE_ROOT', directory)
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: NOW)
    _digest_files(directory, _digest('first'), 20 * DAY)
    assert adaptive_cpu.prune_box_state_if_due(directory, _digest('keep')) == 1
    _digest_files(directory, _digest('second'), 20 * DAY)
    assert adaptive_cpu.prune_box_state_if_due(directory, _digest('keep')) == 0
    assert _digest('second') in _names(directory)
    monkeypatch.setattr(adaptive_cpu.time, 'time',
                        lambda: NOW + adaptive_cpu.PRUNE_INTERVAL_S + 1)
    assert adaptive_cpu.prune_box_state_if_due(directory, _digest('keep')) == 1


def test_box_state_itself_prunes_so_no_caller_has_to(directory, monkeypatch, tmp_path):
    monkeypatch.setattr(adaptive_cpu, 'BOX_STATE_ROOT', directory)
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: NOW)
    stale = _digest('stale')
    _digest_files(directory, stale, 20 * DAY)
    base = tmp_path / 'queue'
    base.mkdir()
    got_directory, digest = adaptive_cpu.box_state(base)
    assert got_directory == directory
    assert stale not in _names(directory)
    assert digest not in {stale}
