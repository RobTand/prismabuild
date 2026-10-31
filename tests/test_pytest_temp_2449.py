"""Raw pytest callers own scratch even when their tests fail."""
import os
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
    with pytest.raises(RuntimeError) if failure else __import__("contextlib").nullcontext():
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
