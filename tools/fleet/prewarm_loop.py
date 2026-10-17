#!/usr/bin/env python3
"""Run the packaged prewarm_loop entry point in this runtime generation."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root

sys.path.insert(0, str(generation_root(__file__) / "src"))
from prismabuild.prewarm_loop import main

if __name__ == "__main__":
    raise SystemExit(main())
