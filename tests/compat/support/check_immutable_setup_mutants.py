"""Run one named negative control inside an already-admitted PB action."""
from __future__ import annotations

import argparse
from functools import lru_cache
from pathlib import Path
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests/compat/support")]


def run_setup_mutant():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mutant", choices=("path-cache", "no-sharing", "no-cleanup", "missing-definitions"))
    args = parser.parse_args()
    import compat_references
    import compat_session
    import pytest

    if args.mutant == "path-cache":
        compat_references._has_definition = lru_cache(maxsize=128)(compat_references._has_definition)
    elif args.mutant == "missing-definitions":
        compat_references.source_definitions = lambda raw: frozenset()
    else:
        original = compat_session.retained_for_session

        def broken_lifetime(config, directory, *, history_control=False):
            if args.mutant == "no-sharing" and compat_session._SOURCES in config.stash:
                del config.stash[compat_session._SOURCES]
            if args.mutant == "no-cleanup":
                config = SimpleNamespace(stash=config.stash, add_cleanup=lambda callback: None)
            return original(config, directory, history_control=history_control)

        compat_session.retained_for_session = broken_lifetime
    return pytest.main(["-q", "--durations=0", "tests/test_compat_definition_cache.py"])


if __name__ == "__main__":
    raise SystemExit(run_setup_mutant())
