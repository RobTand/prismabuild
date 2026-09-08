"""Every explicitly requested test path must exist before fanout (#396)."""
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools/fleet"))
import pbtest


@pytest.mark.parametrize("missing", ["tests/test_typo.py", "misspelled_suite"])
def test_missing_path_refuses_before_submitting_a_valid_subset(tmp_path, monkeypatch, capsys, missing):
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_real.py").write_text("def test_real(): pass\n")
    submitted = []

    def no_submit(*args, **kwargs):
        submitted.append(args)
        pytest.fail("submitted a subset despite a missing explicit path")

    monkeypatch.setattr(pbtest.subprocess, "Popen", no_submit)
    monkeypatch.setattr(sys, "argv", ["pbtest.py", "--checkout", str(tmp_path),
                                    "--python", sys.executable,
                                    "tests/test_real.py", missing])
    assert pbtest.main() == 2
    assert missing in capsys.readouterr().err
    assert submitted == []


def test_valid_directory_and_explicit_file_are_deduplicated(tmp_path):
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_a.py").write_text("pass\n")
    (tests / "test_b.py").write_text("pass\n")
    assert pbtest.discover(tmp_path, ["tests", "tests/test_a.py"]) == [
        "tests/test_a.py", "tests/test_b.py"]
