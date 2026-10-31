#!/usr/bin/env python3
"""Regenerate tests/timing_allowlist.txt from source, refusing any growth.

This source generator runs no tests. Validate the resulting edit via pbtest.
"""
from pathlib import Path
import runpy


def main():
    repository = Path(__file__).resolve().parents[2]
    guard = runpy.run_path(str(repository / "tests/test_no_timing_sensitive_tests.py"))
    path = repository / "tests/timing_allowlist.txt"
    counts = guard["scan_timing_tests"](repository).findings
    rendered = guard["render_shrunk_timing_allowlist"](counts, path.read_text(encoding="utf-8"))
    temporary = path.with_suffix(".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)
    print(f"timing allowlist: {len(counts)} identities, {sum(counts.values())} sites")


if __name__ == "__main__":
    main()
