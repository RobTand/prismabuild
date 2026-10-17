#!/usr/bin/env python3
"""Run the packaged tier role from this generation's physical entry point."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
_PB_ENTRYPOINT = Path(__file__).resolve(strict=True)
sys.path.insert(0, str(_PB_ENTRYPOINT.parents[
    2 if _PB_ENTRYPOINT.parent.name == "fleet" else 1] / "src"))
from prismabuild.tier_cycle import main

if __name__ == "__main__":
    raise SystemExit(main(entrypoint=__file__))
