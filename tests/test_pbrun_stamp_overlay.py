"""Closure stamps live in sealed snapshots, never in submitting worktrees."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
import pbrun
from prismabuild import core as pb
from test_pbrun_detach import _checkout, _queue, _run_pbrun


def test_submission_leaves_no_stamp_or_scratch_in_the_source(tmp_path, monkeypatch):
    checkout = _checkout(tmp_path)
    queue = _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, checkout, "--detach") == 0
    assert not list(checkout.glob(f"{pbrun.STAMP_PREFIX}*"))
    assert len(queue.ready_items()) == 1


@pytest.mark.parametrize("subdirectory", [".", "package"])
def test_overlay_preserves_the_existing_bundle_and_closure_identity(tmp_path, subdirectory):
    checkout = _checkout(tmp_path)
    cwd = checkout if subdirectory == "." else checkout / subdirectory
    cwd.mkdir(exist_ok=True)
    pbrun.keep_droppings_out_of_git(cwd)
    stamp = f"{pbrun.STAMP_PREFIX}{'a' * 16}.json"
    payload = json.dumps({"cwd": subdirectory, **pbrun._git_identity(cwd)},
                         indent=1, sort_keys=True)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    (cwd / stamp).write_text(payload)
    legacy_closure = pb.build_code_closure(cwd, [stamp])
    legacy = pbrun.build_git_checkout_snapshot(cwd, stamp_name=stamp, cas=cas)
    (cwd / stamp).unlink()
    overlay = pbrun.build_git_checkout_snapshot(
        cwd, stamp_name=stamp, stamp_payload=payload, cas=cas)
    assert overlay == legacy
    assert pbrun.build_stamp_closure(stamp, payload) == legacy_closure
    assert not (cwd / stamp).exists()
    materialized = tmp_path / "materialized"
    subprocess.run(["git", "clone", "-q", "--branch", pb.PBRUN_CHECKOUT_SNAPSHOT_REF_NAME,
                    str(cas.input_path(overlay["input"])),
                    str(materialized)], check=True, capture_output=True)
    assert (materialized / subdirectory / stamp).read_text() == payload


def test_concurrent_overlays_need_no_shared_stamp_path(tmp_path):
    checkout = _checkout(tmp_path)
    pbrun.keep_droppings_out_of_git(checkout)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    stamp = f"{pbrun.STAMP_PREFIX}{'b' * 16}.json"
    identity = pbrun._git_identity(checkout)
    payload = json.dumps({"cwd": ".", **identity}, indent=1, sort_keys=True)
    def seal(_):
        return pbrun.build_git_checkout_snapshot(
            checkout, stamp_name=stamp, stamp_payload=payload, cas=cas,
            expected_identity=identity)
    with ThreadPoolExecutor(max_workers=8) as workers:
        snapshots = list(workers.map(seal, range(16)))
    assert all(snapshot == snapshots[0] for snapshot in snapshots)
    assert not list(checkout.glob(f"{pbrun.STAMP_PREFIX}*"))
    assert pbrun._git_identity(checkout) == identity


def test_overlay_cannot_escape_the_checkout(tmp_path):
    checkout = _checkout(tmp_path)
    with pytest.raises(SystemExit, match="stamp name"):
        pbrun.build_git_checkout_snapshot(
            checkout, stamp_name="../outside", stamp_payload="{}",
            cas=pb.PrismaBuildCAS(tmp_path / "cas"))


def test_overlay_is_included_in_the_pre_hash_size_bound(tmp_path):
    checkout = _checkout(tmp_path)
    with pytest.raises(SystemExit, match="working tree plus closure stamp"):
        pbrun.build_git_checkout_snapshot(
            checkout, stamp_name=f"{pbrun.STAMP_PREFIX}{'c' * 16}.json",
            stamp_payload="x" * 32, max_bytes=32,
            cas=pb.PrismaBuildCAS(tmp_path / "cas"))


def test_overlay_still_refuses_git_content_transforms(tmp_path):
    checkout = _checkout(tmp_path)
    (checkout / ".gitattributes").write_text(".pbrun-closure.* text\n")
    with pytest.raises(SystemExit, match="content transform"):
        pbrun.build_git_checkout_snapshot(
            checkout, stamp_name=f"{pbrun.STAMP_PREFIX}{'d' * 16}.json",
            stamp_payload="{}", cas=pb.PrismaBuildCAS(tmp_path / "cas"))


def test_full_concurrent_submissions_leave_no_transient_stamp_names(tmp_path):
    checkout = _checkout(tmp_path)
    queue = _queue(tmp_path)
    pbrun.keep_droppings_out_of_git(checkout)
    code = """
import socket, sys
from pathlib import Path
import pbrun
socket.gethostname = lambda: 'sparky'
pbrun.SH = Path(sys.argv[1])
sys.argv = ['pbrun', '--cwd', sys.argv[2], '--detach', '--transport', 'pool',
            '--', '/bin/true']
raise SystemExit(pbrun.main())
"""
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(
        [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]),
        OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", MKL_NUM_THREADS="1")
    def submit(_):
        result = subprocess.run(
            [sys.executable, "-c", code, str(tmp_path), str(checkout)],
            env=env, text=True, capture_output=True, timeout=60)
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)["action_key"]
    with ThreadPoolExecutor(max_workers=8) as workers:
        keys = list(workers.map(submit, range(16)))
    assert len(set(keys)) == 1
    assert [item["action_key"] for item in queue.ready_items()] == [keys[0]]
    assert not list(checkout.glob(f"{pbrun.STAMP_PREFIX}*"))
