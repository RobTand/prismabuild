"""Maintenance-only prune removes idle digests and keeps live ones (#1542).

The dry run lists only stale entries and changes nothing. Apply
removes only the stale ones, honours the per-pass bound, keeps an
entry whose digest belongs to a served queue, keeps a held admission
or preemption lock, a partial census publication, and any entry whose
evidence is unreadable. An old unheld sweep marker and a released
preemption lock never keep an entry alone. Apply refuses without the
maintenance acknowledgement, and refuses when the box is not provably
quiet (a held lock, a live loop, an unresolved claim, a live scope,
incomplete queue evidence, or unreadable evidence). A missing queue
never reads as an empty queue. Deletion while workers run stays
refused.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_cpu


def _quiet_proc(tmp_path: Path) -> Path:
    proc = tmp_path / "proc-quiet"
    proc.mkdir(exist_ok=True)
    return proc


def _entry(root: Path, base: Path, name: str, *, age_s: float) -> str:
    directory, digest = adaptive_cpu.box_state(base)
    assert directory == root
    (root / (digest + ".lock")).touch()
    state = root / (digest + ".adaptive-cpu-v1")
    state.mkdir(mode=0o700, exist_ok=True)
    (state / "cpu-sample.json").write_text("{}")
    stamp = time.time() - age_s
    candidates = [root / (digest + ".lock"), state, state / "cpu-sample.json"]
    origin = root / (digest + ".origin.json")
    if origin.exists():
        candidates.append(origin)
    for path in candidates:
        os.utime(path, (stamp, stamp))
    return digest


def _fixture(monkeypatch, tmp_path: Path):
    root = tmp_path / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    queue = tmp_path / "live-queue"
    (queue / "reservations" / "live-host").mkdir(parents=True)
    (queue / "ready").mkdir(parents=True)
    (queue / "claimed").mkdir(parents=True)
    live_base = queue / "reservations" / "live-host"
    _, live_digest = adaptive_cpu.box_state(live_base)
    (root / (live_digest + ".lock")).touch()
    stale = _entry(root, tmp_path / "old" / "reservations" / "h", "stale", age_s=8 * 24 * 3600)
    fresh = _entry(root, tmp_path / "new" / "reservations" / "h", "fresh", age_s=60)
    return root, queue, live_base, live_digest, stale, fresh


def test_dry_run_lists_only_stale_and_changes_nothing(tmp_path, monkeypatch):
    root, queue, live_base, live_digest, stale, fresh = _fixture(monkeypatch, tmp_path)
    before = sorted(path.name for path in root.iterdir())
    report = adaptive_cpu.prune_box_state(queue_roots=[queue])
    assert report["dry_run"] is True
    assert report["candidates"] == [stale]
    assert report["kept"][live_digest] == "served queue root"
    assert report["kept"][fresh] == "fresh activity"
    assert sorted(path.name for path in root.iterdir()) == before


def test_apply_removes_only_stale_and_honours_the_pass_bound(tmp_path, monkeypatch):
    root, queue, live_base, live_digest, stale, fresh = _fixture(monkeypatch, tmp_path)
    second = _entry(root, tmp_path / "older" / "reservations" / "h", "second",
                    age_s=9 * 24 * 3600)
    report = adaptive_cpu.prune_box_state(queue_roots=[queue], max_entries_per_pass=1,
                                          apply=True, maintenance_held=True,
                                          proc_root=_quiet_proc(tmp_path))
    assert report["dry_run"] is False
    assert report["removed"] == [second]
    assert adaptive_cpu._box_state_digest_of(second + ".lock") == second
    assert not (root / (second + ".lock")).exists()
    assert (root / (stale + ".lock")).exists()
    assert (root / (live_digest + ".lock")).exists()
    assert (root / (fresh + ".lock")).exists()


def test_a_held_lock_refuses_apply_and_keeps_everything(tmp_path, monkeypatch):
    root, queue, live_base, live_digest, stale, fresh = _fixture(monkeypatch, tmp_path)
    old = time.time() - 8 * 24 * 3600
    for path in (root / (live_digest + ".lock"), root / (live_digest + ".origin.json")):
        if path.exists():
            os.utime(path, (old, old))
    directory, held_digest = adaptive_cpu.box_state(tmp_path / "held" / "reservations" / "h")
    held_lock = directory / (held_digest + ".lock")
    held_lock.touch()
    import fcntl
    descriptor = os.open(held_lock, os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = root / (held_digest + ".adaptive-cpu-v1")
        state.mkdir(mode=0o700, exist_ok=True)
        (state / "cpu-sample.json").write_text("{}")
        for path in (held_lock, state / "cpu-sample.json"):
            os.utime(path, (old, old))
        survey = adaptive_cpu.survey_box_state(root, queue_roots=[queue])
        assert survey["kept"][held_digest] == "held admission lock"
        with pytest.raises(adaptive_cpu.PruneRefused):
            adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                         maintenance_held=True,
                                         proc_root=_quiet_proc(tmp_path))
        assert (root / (stale + ".lock")).exists()
        assert (root / (held_digest + ".lock")).exists()
    finally:
        os.close(descriptor)


def test_apply_keeps_a_partial_reader_but_not_idle_markers(tmp_path, monkeypatch):
    root, queue, _live_base, _live, _stale, _fresh = _fixture(monkeypatch, tmp_path)
    old = time.time() - 8 * 24 * 3600
    _, partial = adaptive_cpu.box_state(tmp_path / "partial" / "reservations" / "h")
    state = root / (partial + ".adaptive-cpu-v1")
    state.mkdir(mode=0o700, exist_ok=True)
    (state / "cpu-sample.json").write_text("{}")
    (root / (partial + ".measurement-reader-v1.writing")).write_text("partial")
    _, retired = adaptive_cpu.box_state(tmp_path / "retired" / "reservations" / "h")
    retired_state = root / (retired + ".adaptive-cpu-v1")
    retired_state.mkdir(mode=0o700, exist_ok=True)
    (retired_state / "cpu-sample.json").write_text("{}")
    (root / (retired + ".lock")).touch()
    (root / (retired + ".sweep")).touch()
    (root / (retired + ".preemption")).touch()
    for path in list(root.iterdir()) + [state, state / "cpu-sample.json",
                                        retired_state, retired_state / "cpu-sample.json"]:
        try:
            os.utime(path, (old, old))
        except OSError:
            pass
    survey = adaptive_cpu.survey_box_state(root, queue_roots=[queue])
    assert survey["kept"][partial] == "unresolved census reader"
    assert partial not in survey["candidates"]
    assert retired in survey["candidates"]


def test_a_held_preemption_lock_keeps_its_entry(tmp_path, monkeypatch):
    import fcntl
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    old = time.time() - 8 * 24 * 3600
    _, digest = adaptive_cpu.box_state(tmp_path / "handoff" / "reservations" / "h")
    state = root / (digest + ".adaptive-cpu-v1")
    state.mkdir(mode=0o700, exist_ok=True)
    (state / "cpu-sample.json").write_text("{}")
    (root / (digest + ".lock")).touch()
    slot = root / (digest + ".preemption")
    slot.touch()
    for path in list(root.iterdir()) + [state, state / "cpu-sample.json"]:
        try:
            os.utime(path, (old, old))
        except OSError:
            pass
    descriptor = os.open(slot, os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        survey = adaptive_cpu.survey_box_state(root, queue_roots=[queue])
        assert survey["kept"][digest] == "held preemption lock"
        assert digest not in survey["candidates"]
        with pytest.raises(adaptive_cpu.PruneRefused):
            adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                         maintenance_held=True,
                                         proc_root=_quiet_proc(tmp_path))
        assert (root / (stale + ".lock")).exists()
    finally:
        os.close(descriptor)


def test_apply_removes_a_retired_entry_with_its_markers(tmp_path, monkeypatch):
    root, queue, _live_base, live_digest, stale, fresh = _fixture(monkeypatch, tmp_path)
    old = time.time() - 8 * 24 * 3600
    _, retired = adaptive_cpu.box_state(tmp_path / "retired" / "reservations" / "h")
    retired_state = root / (retired + ".adaptive-cpu-v1")
    retired_state.mkdir(mode=0o700, exist_ok=True)
    (retired_state / "cpu-sample.json").write_text("{}")
    (root / (retired + ".lock")).touch()
    (root / (retired + ".sweep")).touch()
    (root / (retired + ".preemption")).touch()
    for path in list(root.iterdir()) + [retired_state, retired_state / "cpu-sample.json"]:
        try:
            os.utime(path, (old, old))
        except OSError:
            pass
    live_stamp = time.time()
    for path in (root / (live_digest + ".lock"), root / (fresh + ".lock")):
        if path.exists():
            os.utime(path, (live_stamp, live_stamp))
    report = adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                          maintenance_held=True,
                                          proc_root=_quiet_proc(tmp_path))
    assert retired in report["removed"]
    assert stale in report["removed"]
    assert not (root / (retired + ".sweep")).exists()
    assert not (root / (retired + ".preemption")).exists()
    assert not (root / (retired + ".lock")).exists()
    assert (root / (live_digest + ".lock")).exists()
    assert (root / (fresh + ".lock")).exists()


def test_apply_protects_a_bare_queue_root_entry(tmp_path, monkeypatch):
    root = tmp_path / "box-state"
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    queue = tmp_path / "pb-queue"
    (queue / "reservations" / "HOSTX").mkdir(parents=True)
    (queue / "ready").mkdir(parents=True)
    (queue / "claimed").mkdir(parents=True)
    ledger_base = queue / "reservations" / "HOSTX"
    _, digest = adaptive_cpu.box_state(ledger_base)
    old = time.time() - 8 * 24 * 3600
    state = root / (digest + ".adaptive-cpu-v1")
    state.mkdir(mode=0o700, exist_ok=True)
    (state / "cpu-sample.json").write_text("{}")
    for path in list(root.iterdir()) + [state, state / "cpu-sample.json"]:
        path.touch(exist_ok=True)
        os.utime(path, (old, old))
    survey = adaptive_cpu.survey_box_state(root, queue_roots=[queue])
    assert survey["kept"][digest] == "served queue root"
    assert digest not in survey["candidates"]


def test_apply_without_the_hold_refuses(tmp_path, monkeypatch):
    root, queue, live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                     maintenance_held=False,
                                     proc_root=_quiet_proc(tmp_path))
    assert (root / (stale + ".lock")).exists()


def test_apply_refuses_when_a_lock_is_held(tmp_path, monkeypatch):
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    import fcntl
    descriptor = os.open(root / (stale + ".lock"), os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(adaptive_cpu.PruneRefused):
            adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                         maintenance_held=True,
                                         proc_root=_quiet_proc(tmp_path))
        assert (root / (stale + ".lock")).exists()
    finally:
        os.close(descriptor)


def test_apply_refuses_with_an_unresolved_claim(tmp_path, monkeypatch):
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    (queue / "claimed" / ("ab12" * 16 + ".json")).write_text("{}")
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                     maintenance_held=True,
                                     proc_root=_quiet_proc(tmp_path))
    assert (root / (stale + ".lock")).exists()


def test_apply_refuses_with_a_live_loop_in_proc(tmp_path, monkeypatch):
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    proc = tmp_path / "proc-busy"
    pid_dir = proc / "4242"
    pid_dir.mkdir(parents=True)
    (pid_dir / "cmdline").write_bytes(b"python3\x00worker_loop.py\x00--once\x00")
    (pid_dir / "cgroup").write_text("0::/user.slice\n")
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                     maintenance_held=True, proc_root=proc)
    assert (root / (stale + ".lock")).exists()


def test_apply_refuses_when_evidence_is_unreadable(tmp_path, monkeypatch):
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                     maintenance_held=True,
                                     proc_root=tmp_path / "no-such-proc")
    assert (root / (stale + ".lock")).exists()


def test_a_missing_queue_keeps_everything_and_refuses_apply(tmp_path, monkeypatch):
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    gone = tmp_path / "no-such-queue"
    survey = adaptive_cpu.survey_box_state(root, queue_roots=[queue, gone])
    assert survey["queue_evidence"]["complete"] is False
    assert survey["queue_evidence"]["errors"]
    assert survey["candidates"] == []
    assert survey["kept"][stale] == "unresolved queue evidence"
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[queue, gone], apply=True,
                                     maintenance_held=True,
                                     proc_root=_quiet_proc(tmp_path))
    assert (root / (stale + ".lock")).exists()


def test_a_queue_without_state_directories_refuses_apply(tmp_path, monkeypatch):
    root, _queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    bare = tmp_path / "bare-queue"
    (bare / "reservations" / "live-host").mkdir(parents=True)
    proof = adaptive_cpu.prove_box_quiescent(root, queue_roots=[bare],
                                             proc_root=_quiet_proc(tmp_path))
    assert proof["quiet"] is False
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[bare], apply=True,
                                     maintenance_held=True,
                                     proc_root=_quiet_proc(tmp_path))
    assert (root / (stale + ".lock")).exists()


def test_an_unreadable_reservations_census_keeps_everything(tmp_path, monkeypatch):
    root, queue, _live_base, _live, stale, _fresh = _fixture(monkeypatch, tmp_path)
    real_scandir = os.scandir

    def fail_reservations(path, *args, **kwargs):
        if str(path) == str(queue / "reservations"):
            raise OSError("lost mount")
        return real_scandir(path, *args, **kwargs)

    monkeypatch.setattr(os, "scandir", fail_reservations)
    survey = adaptive_cpu.survey_box_state(root, queue_roots=[queue])
    assert survey["queue_evidence"]["complete"] is False
    assert survey["candidates"] == []
    with pytest.raises(adaptive_cpu.PruneRefused):
        adaptive_cpu.prune_box_state(queue_roots=[queue], apply=True,
                                     maintenance_held=True,
                                     proc_root=_quiet_proc(tmp_path))
    assert (root / (stale + ".lock")).exists()


@pytest.mark.parametrize("suffix", [".lock", ".sweep", ".preemption", ".other-state"])
def test_nonadaptive_entries_are_candidates_but_served_locks_stay(tmp_path, monkeypatch, suffix):
    root = tmp_path / "box-state"
    root.mkdir()
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root)
    queue = tmp_path / "served"
    for name in ("reservations/host", "ready", "claimed"):
        (queue / name).mkdir(parents=True)
    live = adaptive_cpu.box_identity(queue / "reservations" / "host")
    obsolete = adaptive_cpu.box_identity(tmp_path / "unserved" / "reservations" / "host")
    live_path = root / (live + ".lock")
    obsolete_path = root / (obsolete + suffix)
    stamp = time.time() - 8 * 24 * 3600
    for path in (live_path, obsolete_path):
        path.write_text("legacy")
        os.utime(path, (stamp, stamp))
    before = {path.name: path.stat().st_ino for path in root.iterdir()}
    dry_run = adaptive_cpu.prune_box_state(directory=root, queue_roots=[queue])
    assert dry_run["candidates"] == [obsolete]
    assert dry_run["kept"][live] == "served queue root"
    assert {path.name: path.stat().st_ino for path in root.iterdir()} == before
    applied = adaptive_cpu.prune_box_state(
        directory=root, queue_roots=[queue], apply=True, maintenance_held=True,
        proc_root=_quiet_proc(tmp_path))
    assert applied["removed"] == [obsolete]
    assert not obsolete_path.exists()
    assert live_path.read_text() == "legacy"
    assert live_path.stat().st_ino == before[live_path.name]


@pytest.mark.parametrize("roots", [None, [], ()])
def test_absent_queue_roots_refuse_apply_without_any_changes(tmp_path, monkeypatch, roots):
    root, _queue, _base, _live, _stale, _fresh = _fixture(monkeypatch, tmp_path)
    before = {path.name: path.lstat().st_ino for path in root.iterdir()}
    with pytest.raises(adaptive_cpu.PruneRefused, match="queue evidence"):
        adaptive_cpu.prune_box_state(
            directory=root, queue_roots=roots, apply=True, maintenance_held=True,
            proc_root=_quiet_proc(tmp_path))
    survey = adaptive_cpu.prune_box_state(directory=root, queue_roots=roots)
    assert survey["queue_evidence"]["complete"] is False
    assert survey["candidates"] == []
    assert {path.name: path.lstat().st_ino for path in root.iterdir()} == before


def test_a_live_resource_scope_refuses_apply_without_any_changes(tmp_path, monkeypatch):
    root, queue, _base, _live, _stale, _fresh = _fixture(monkeypatch, tmp_path)
    before = {path.name: path.lstat().st_ino for path in root.iterdir()}
    proc = _quiet_proc(tmp_path)
    member = proc / "4242"
    member.mkdir()
    (member / "cmdline").write_bytes(b"python3\0payload.py\0")
    (member / "cgroup").write_text("0::/prismabuild.slice/prismabuild-job-test.slice\n")
    with pytest.raises(adaptive_cpu.PruneRefused, match="live resource scope"):
        adaptive_cpu.prune_box_state(
            directory=root, queue_roots=[queue], apply=True, maintenance_held=True,
            proc_root=proc)
    assert {path.name: path.lstat().st_ino for path in root.iterdir()} == before
