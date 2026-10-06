"""The per-test resource tracer must not read /proc through test-patched globals (#1550).

A legitimate test may patch ``Path.read_text`` (or any stat reader) and assert
it only ever sees its own fake pid.  The tracer samples the real worker inside
``pytest_runtest_makereport`` while that patch is live; a read dispatched
through the patched global raised inside the hook, which pytest reports as an
INTERNALERROR and which aborts the whole shard.
"""
import inspect
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbtest  # noqa: E402
from prismabuild import resource_scope  # noqa: E402

PATCHING_TESTS = '''\
import os
from pathlib import Path


def _only_the_fake_pid(self, *args, **kwargs):
    assert str(self) == "/proc/424242/stat", f"unexpected read of {self}"
    return "424242 (x) S 1 " + " ".join(["0"] * 30)


def test_patches_the_stat_reader(monkeypatch):
    monkeypatch.setattr(Path, "read_text", _only_the_fake_pid)
    assert Path("/proc/424242/stat").read_text().startswith("424242")


def test_patches_the_raw_os_readers(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("test-patched reader reached")
    monkeypatch.setattr(Path, "read_text", refuse)
    monkeypatch.setattr(Path, "open", refuse)
    monkeypatch.setattr("builtins.open", refuse)
    assert True


def test_after_the_patches_are_undone():
    assert Path("/proc/self/status").read_text()
'''


def _run(tmp_path: Path, extra=()):
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    (tmp_path / "test_fixture.py").write_text(PATCHING_TESTS)
    options = {"trace": True} if "trace" in inspect.signature(
        pbtest.shard_entry).parameters else {}
    entry = pbtest.shard_entry(sys.executable, tmp_path, **options)
    command = [*entry, "-q", "-p", "no:cacheprovider", *extra,
               "--pbtest-trace", "test_fixture.py"]
    return subprocess.run(command, cwd=tmp_path, text=True, capture_output=True,
                          timeout=120)


def _events(output: str) -> list[dict]:
    prefix = "pbtest-trace: "
    return [json.loads(line[len(prefix):]) for line in output.splitlines()
            if line.startswith(prefix)]


@pytest.mark.parametrize("workers", [0, 2])
def test_a_test_that_patches_the_stat_reader_does_not_abort_the_shard(tmp_path, workers):
    result = _run(tmp_path, ["-n", str(workers)] if workers else [])
    combined = result.stdout + result.stderr
    assert "INTERNALERROR" not in combined, combined
    assert result.returncode == 0, combined
    done = [e for e in _events(result.stdout) if e.get("when") == "call"]
    assert len(done) == 3, combined


def test_the_samples_taken_under_a_patch_are_still_real(tmp_path):
    result = _run(tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    patched = [e for e in _events(result.stdout)
               if e.get("when") == "call" and "patches" in e["nodeid"]]
    assert patched, result.stdout
    for event in patched:
        after = event["resources"]["after"]
        assert after["process_io"] is not None, event
        assert after["rss_bytes"] is not None, event
        assert after["errors"] == [], event


def test_the_default_reader_still_honours_a_patched_path_reader(monkeypatch):
    """Only the tracer pins its reader; the scope sampler's tests rely on the patch."""
    seen = []
    original = Path.read_text

    def read(path, *args, **kwargs):
        seen.append(str(path))
        if str(path) == "/proc/4242/stat":
            return "4242 (x) S 1 " + " ".join(["0"] * 17) + " 100 0"
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    assert resource_scope.read_process_io(4242) == ("4242:100", 1, None)
    assert "/proc/4242/stat" in seen


def test_an_injected_reader_is_used_for_every_procfs_read():
    seen = []
    stat = "7 (a b) S 3 " + " ".join(["0"] * 17) + " 55 " + " ".join(["0"] * 5)
    io = "\n".join(f"{name}: 1" for name in resource_scope.IO_COUNTERS)

    def reader(path):
        seen.append(str(path))
        return stat if path.endswith("/stat") else io

    identity, parent, counters = resource_scope.read_process_io(7, read_text=reader)
    assert (identity, parent) == ("7:55", 3)
    assert set(counters) == set(resource_scope.IO_COUNTERS)
    assert seen == ["/proc/7/stat", "/proc/7/io", "/proc/7/stat"]
