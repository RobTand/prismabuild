"""Run one named corpus negative control inside an admitted PB action."""
import argparse
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mutant", choices=(
        "missing-family", "duplicate-declaration", "duplicate-family",
        "missing-corpus", "missing-source-pin",
    ))
    args = parser.parse_args()
    import pytest
    from prismabuild import installed_bootstrap

    target = ROOT / "tests/test_record_compatibility_controller_broker.py"
    source = target.read_text()
    original_read = Path.read_text
    original_glob = Path.glob
    relative = target.relative_to(ROOT).as_posix()

    def read_text(path, *positional, **keywords):
        if path == target:
            if args.mutant == "missing-family":
                return source.replace('family_group = "broker"', 'family_group = "absent"')
            if args.mutant == "duplicate-declaration":
                return source + '\nfamily_group = "broker"\n'
        return original_read(path, *positional, **keywords)

    def glob(path, pattern, *positional, **keywords):
        yield from original_glob(path, pattern, *positional, **keywords)
        if args.mutant == "duplicate-family" and pattern == "tests/test_record_compatibility_controller*.py":
            yield target

    if args.mutant == "missing-corpus":
        installed_bootstrap.CORPUS_FILES = tuple(
            name for name in installed_bootstrap.CORPUS_FILES if name != relative)
    if args.mutant == "missing-source-pin":
        installed_bootstrap.QUALIFICATION_SOURCES = tuple(
            name for name in installed_bootstrap.QUALIFICATION_SOURCES if name != relative)
    with patch.object(Path, "read_text", read_text), patch.object(Path, "glob", glob):
        return pytest.main([
            "-q", "--tb=short",
            "tests/test_bootstrap_qualification.py::test_controller_partitions_are_complete_and_bootstrap_pinned",
        ])


if __name__ == "__main__":
    raise SystemExit(main())
