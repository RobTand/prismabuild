"""The allowance evidence must not depend on the runner's scratch filesystem."""
from pathlib import Path
import subprocess
import sys
import textwrap
import xml.etree.ElementTree as ET


REPO = Path(__file__).resolve().parents[1]


def test_allowance_suite_runs_on_tmpfs_without_relaxing_production_guard(tmp_path):
    """Run the original assertions with a deterministic tmpfs mount view.

    This is a fixture-isolation regression, not permission for production
    spools to use tmpfs.  Use a separate interpreter so the synthetic mount
    table cannot leak into unrelated tests.  Only mount discovery is faked:
    spool records, payloads, sealed requests and queue claims remain real.
    """
    report = tmp_path / "allowance-results.xml"
    script = textwrap.dedent('''
        from pathlib import Path
        import sys
        import pytest
        from prismabuild import produced_spool as ps

        class SpoolMountPath(type(Path())):
            def read_text(self, *args, **kwargs):
                if self == Path("/proc/self/mountinfo"):
                    return "1 0 0:1 / / rw - tmpfs tmpfs rw\\n"
                return super().read_text(*args, **kwargs)

        # Change only the spool's mount view; queue admission has its own
        # unrelated filesystem sampling and must retain the real mount table.
        ps.Path = SpoolMountPath

        def assert_production_guard():
            try:
                ps._local_disk(Path(sys.argv[1]))
            except ps.SpoolError as exc:
                assert "not a known local disk filesystem" in str(exc)
            else:
                raise AssertionError("production spool validation accepted tmpfs")

        assert_production_guard()
        status = pytest.main([
            "tests/test_a_producers_exports_run_on_its_allowance.py", "-q", "--tb=short",
            "--basetemp=" + sys.argv[2], "--junitxml=" + sys.argv[3],
        ])
        # A test-only concession must be undone after the allowance suite.
        assert_production_guard()
        raise SystemExit(status)
    ''')
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path),
         str(tmp_path / "allowance-pytest"), str(report)],
        cwd=REPO, capture_output=True, text=True, timeout=90,
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[:2500]
    cases = ET.parse(report).findall(".//testcase")
    assert len(cases) >= 10, "the existing allowance cases must still be collected"
    assert all(case.find("skipped") is None for case in cases), (
        "skipping the allowance evidence does not fix its filesystem dependency"
    )
