"""A coverage change must detect the mixed-holder election regression (#2182).

The existing orphan-only and mirror-race tests already pass. Run their suite
with one process-local mutation: ignore an unclaimed holder *only* beside a
claimed incumbent. The new steady-state regression must reject that mutant.
No repository source, queue outside the fixture, or pytest hooks are changed.
"""
from pathlib import Path
import subprocess
import sys
import textwrap
import xml.etree.ElementTree as ET


REPO = Path(__file__).resolve().parents[1]
REGRESSION = "test_a_raw_holder_beside_a_claimed_incumbent_does_not_elect_the_host"


def test_undeclared_suite_detects_mixed_holder_election_mutation(tmp_path):
    report = tmp_path / "mixed-holder-mutant.xml"
    script = textwrap.dedent('''
        import inspect
        from pathlib import Path
        import sys
        import pytest

        root = Path.cwd()
        sys.path[:0] = [str(root / "src"), str(root / "tests")]
        from prismabuild import _measurement_reservation as reservation

        source = "from __future__ import annotations\\n" + inspect.getsource(reservation.elect)
        needle = "        for key in holders:\\n"
        assert source.count(needle) == 1, "election loop changed; review the causal mutant"
        replacement = needle + (
            '            if key not in census["opportunities"] and any(\\n'
            '                    census["opportunities"].get(holder, {}).get("host")\\n'
            '                    == ledger.base.name for holder in holders):\\n'
            '                continue\\n'
        )
        namespace = dict(vars(reservation))
        exec(compile(source.replace(needle, replacement),
                     reservation.__file__, "exec"), namespace)
        reservation.elect = namespace["elect"]
        raise SystemExit(pytest.main([
            "tests/test_measurement_reservation_undeclared_incumbent_1419.py",
            "-q", "--tb=short", "--basetemp=" + sys.argv[1],
            "--junitxml=" + sys.argv[2],
        ]))
    ''')
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path / "mutant-pytest"), str(report)],
        cwd=REPO, capture_output=True, text=True, timeout=120,
    )
    output = result.stdout + result.stderr
    assert report.exists(), output
    cases = ET.parse(report).findall(".//testcase")
    assert cases and not any(case.find("error") is not None for case in cases), output
    failed = {case.get("name") for case in cases if case.find("failure") is not None}
    assert result.returncode == 1 and REGRESSION in failed, (
        "The undeclared-incumbent suite must include " + REGRESSION
        + " and fail it when a claimed incumbent masks an unclaimed holder. "
        + "Existing adjacent coverage must not stand in for the mixed case.\n" + output
    )
