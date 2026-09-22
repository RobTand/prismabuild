#!/usr/bin/env python3
"""PB worker payload for one producer-local precommit group export."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild.produced_spool import main
if __name__ == "__main__":
    raise SystemExit(main())
