#!/usr/bin/env python3
"""PB worker payload for one producer-local precommit group export."""
import sys
from pathlib import Path
_PB_ENTRYPOINT = Path(__file__).resolve()
sys.path.insert(0, str(_PB_ENTRYPOINT.parents[
    2 if _PB_ENTRYPOINT.parent.name == "fleet" else 1] / "src"))
from prismabuild.produced_spool import main
if __name__ == "__main__":
    raise SystemExit(main())
