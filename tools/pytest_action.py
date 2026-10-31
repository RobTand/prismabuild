#!/usr/bin/env python3
"""Run pytest with private external scratch inside an admitted PB action."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild.pytest_temp import main

if __name__ == "__main__":
    raise SystemExit(main())
