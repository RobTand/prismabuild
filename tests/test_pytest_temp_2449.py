"""Raw pytest callers own scratch even when their tests fail."""
from contextlib import nullcontext
import os
import signal
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild.pytest_temp import pytest_basetemp


@pytest.mark.parametrize("failure", [False, True])
def test_attempt_scratch_is_removed_after_any_ending(tmp_path, failure):
    root = tmp_path / "scratch"
    root.mkdir()
    compiler = root / "compiler-cache"
    compiler.mkdir()
    (compiler / "sentinel").write_text("keep")
    with pytest.raises(RuntimeError) if failure else nullcontext():
        with pytest_basetemp(root) as base:
            base.mkdir()
            (base / "marker").write_text("owned")
            readonly = base / "readonly"
            readonly.mkdir()
            (readonly / "data").write_text("owned")
            readonly.chmod(0o500)
            if failure:
                raise RuntimeError("test failure")
    assert not base.parent.exists(), "attempt scratch survived its ending"
    assert (compiler / "sentinel").read_text() == "keep"


def test_failed_raw_pytest_removes_tmp_path(tmp_path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    (checkout / "test_one.py").write_text(
        "def test_one(tmp_path):\n"
        "    (tmp_path / 'marker').write_text('owned')\n"
        "    assert False, 'expected regression fixture failure'\n")
    with pytest_basetemp(scratch) as base:
        run = subprocess.run([sys.executable, "-m", "pytest", "-q",
                              "-p", "no:cacheprovider", f"--basetemp={base}"],
                             cwd=checkout, env={**os.environ, "PYTEST_ADDOPTS": ""},
                             text=True, capture_output=True, timeout=120)
        assert run.returncode == 1, run.stdout + run.stderr
        assert "1 failed" in run.stdout
    assert list(scratch.iterdir()) == [], "failed raw pytest retained its scratch"


def test_sigterm_unwinds_private_scratch(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    program = (
        "import signal; from prismabuild.pytest_temp import pytest_basetemp\n"
        f"with pytest_basetemp({str(scratch)!r}) as base:\n"
        "    base.mkdir(); (base / 'marker').write_text('owned')\n"
        "    print(base, flush=True)\n"
        "    signal.pause()\n"
    )
    environment = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    with subprocess.Popen([sys.executable, "-c", program], env=environment,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
        base = Path(process.stdout.readline().strip())
        try:
            assert (base / "marker").is_file()
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=120) == 143, process.stderr.read()
            assert list(scratch.iterdir()) == []
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=120)


def test_helper_refuses_scratch_inside_a_git_checkout(tmp_path):
    checkout = tmp_path / "checkout"
    (checkout / ".git").mkdir(parents=True)
    scratch = checkout / "scratch"
    scratch.mkdir()
    with pytest.raises(ValueError, match="outside the checkout"):
        with pytest_basetemp(scratch):
            pytest.fail("in-checkout scratch was accepted")
    assert list(scratch.iterdir()) == []
