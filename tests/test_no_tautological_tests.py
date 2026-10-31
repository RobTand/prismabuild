from pathlib import Path

from prismabuild.tautology import scan


def test_suite_has_no_tautologies():
    findings = scan(Path(__file__).resolve().parent)
    details = "\n".join(
        f"{finding.path}:{finding.line}: {finding.test}: {finding.rule}"
        for finding in findings
    )
    if findings:
        raise AssertionError(f"Tautological tests (D-32):\n{details}")
