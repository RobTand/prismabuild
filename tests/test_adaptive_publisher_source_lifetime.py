"""Publishers finish before pytest removes a passing test’s private source."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap


def _suite(tmp_path: Path) -> tuple[Path, Path]:
    suite = tmp_path / "suite"
    tests = suite / "tests"
    tests.mkdir(parents=True)
    (tests / "conftest.py").symlink_to(Path(__file__).with_name("conftest.py"))
    return suite, tests


def _run(suite: Path) -> subprocess.CompletedProcess[str]:
    root = Path(__file__).resolve().parents[1]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join([
        str(root / "src"), str(root / "tests"), str(root / "tools" / "fleet"),
        environment.get("PYTHONPATH", ""),
    ])
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:randomly",
         "-o", "tmp_path_retention_policy=failed", "tests"],
        cwd=suite, env=environment, text=True, capture_output=True, timeout=45,
    )


def test_publisher_is_reaped_by_its_source_owning_test(tmp_path: Path) -> None:
    suite, tests = _suite(tmp_path)
    (tests / "test_01_source_owner.py").write_text(textwrap.dedent("""
        import json
        import os
        from pathlib import Path
        from prismabuild import adaptive_snapshot

        def test_start_publisher(tmp_path, monkeypatch):
            suite = Path(__file__).resolve().parents[1]
            local = Path(os.environ["PRISMABUILD_BOX_STATE_ROOT"]) / "adaptive"
            shared = tmp_path / "shared"
            local.mkdir(parents=True)
            (local / "cpu-sample.json").write_text(json.dumps({"sampled_unix": 42.}))
            (suite / "source-path.txt").write_text(str(local))
            gate_read, gate_write = os.pipe()
            entry = suite / "gated_publisher.py"
            entry.write_text(
                "import os, sys\\n"
                f"assert os.read({gate_read}, 1) == b'x'\\n"
                f"os.close({gate_read})\\n"
                "from prismabuild.adaptive_snapshot import main\\n"
                "raise SystemExit(main(sys.argv[1:]))\\n"
            )
            popen = adaptive_snapshot.subprocess.Popen
            def start_gated(arguments, **options):
                options["pass_fds"] = (*options["pass_fds"], gate_read)
                return popen(arguments, **options)
            monkeypatch.setattr(adaptive_snapshot.subprocess, "Popen", start_gated)
            monkeypatch.setattr(adaptive_snapshot, "_ENTRY", entry)
            try:
                child = adaptive_snapshot.publish(local, shared)
            finally:
                os.close(gate_read)
            assert child is not None
            wait = child.wait
            released = False
            def release_and_wait(timeout=None):
                nonlocal released
                if not released:
                    released = True
                    os.write(gate_write, b"x")
                    os.close(gate_write)
                returncode = wait(timeout=timeout)
                (suite / "publisher-result.json").write_bytes(
                    (local / "publisher-result.json").read_bytes())
                if returncode == 0:
                    (suite / "copied.json").write_bytes((shared / "cpu-sample.json").read_bytes())
                return returncode
            child.wait = release_and_wait
    """))
    (tests / "test_02_next_owner.py").write_text(textwrap.dedent("""
        from pathlib import Path
        import pytest
        from prismabuild import adaptive_snapshot

        @pytest.fixture(autouse=True)
        def reap_publishers():
            yield
            for child in adaptive_snapshot._children:
                assert child.wait(timeout=10) == 0
            adaptive_snapshot._children.clear()

        def test_previous_source_has_been_removed():
            suite = Path(__file__).resolve().parents[1]
            source = Path((suite / "source-path.txt").read_text())
            assert not source.exists()
    """))

    result = _run(suite)

    diagnostics = suite / "publisher-result.json"
    detail = diagnostics.read_text() if diagnostics.exists() else "No publisher result"
    assert result.returncode == 0, result.stdout + result.stderr + detail
    copied = json.loads((suite / "copied.json").read_text())
    assert copied["sampled_unix"] == 42.
    assert copied["_snapshot"]["source"] == "host-local"


def test_source_owner_does_not_hide_a_failed_publisher(tmp_path: Path) -> None:
    suite, tests = _suite(tmp_path)
    (tests / "test_failed_publisher.py").write_text(textwrap.dedent("""
        import subprocess
        import sys
        from prismabuild import adaptive_snapshot

        def test_start_failed_publisher():
            adaptive_snapshot._children.append(subprocess.Popen(
                [sys.executable, "-c", "raise SystemExit(7)"]))
    """))

    result = _run(suite)

    assert result.returncode == 1, result.stdout + result.stderr
    assert "ERROR at teardown of test_start_failed_publisher" in result.stdout
    assert "assert 7 == 0" in result.stdout
