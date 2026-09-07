"""Permanent POSIX locks exclude threads and processes without stale ownership."""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import posix_lock


def probe(path):
    result = subprocess.run(
        [sys.executable, "-c", "from pathlib import Path; import sys; "
         "from prismabuild.posix_lock import held; "
         "exec('with held(Path(sys.argv[1]), blocking=False) as ok:\\n print(ok)')", str(path)],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
        capture_output=True, text=True, timeout=10, check=True)
    return result.stdout.strip() == "True"


def test_nested_lock_retains_process_exclusion(tmp_path):
    path = tmp_path / "key.lock"
    with posix_lock.held(path):
        assert not probe(path)
        with posix_lock.held(path) as nested:
            assert nested
        assert not probe(path), "closing a nested descriptor must not release the outer lock"
    assert probe(path)
    assert path.exists(), "the inode must remain stable between owners"


def test_busy_thread_skips_one_key_and_can_take_another(tmp_path):
    path = tmp_path / "key.lock"
    def attempt(candidate):
        with posix_lock.held(candidate, blocking=False) as acquired:
            return acquired
    with ThreadPoolExecutor(max_workers=1) as executor:
        with posix_lock.held(path):
            assert executor.submit(attempt, path).result(timeout=5) is False
            assert executor.submit(attempt, tmp_path / "other.lock").result(timeout=5) is True


def test_exception_releases_lock(tmp_path):
    path = tmp_path / "key.lock"
    with pytest.raises(ValueError):
        with posix_lock.held(path):
            raise ValueError("interrupted transition")
    assert probe(path)


def test_process_death_releases_lock(tmp_path):
    path = tmp_path / "key.lock"
    child = subprocess.Popen(
        [sys.executable, "-c", "from pathlib import Path; import sys,time; "
         "from prismabuild.posix_lock import held; "
         "exec('with held(Path(sys.argv[1])):\\n print(\"ready\", flush=True)\\n time.sleep(30)')", str(path)],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
        stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "ready"
        assert not probe(path)
    finally:
        child.kill()
        child.wait(timeout=5)
    assert probe(path)


def test_refuses_lock_inode_aliases(tmp_path):
    path = tmp_path / "key.lock"
    target = tmp_path / "target"
    target.touch()
    path.symlink_to(target)
    with pytest.raises(OSError):
        with posix_lock.held(path):
            pass
    path.unlink()
    os.link(target, path)
    with pytest.raises(OSError):
        with posix_lock.held(path):
            pass
