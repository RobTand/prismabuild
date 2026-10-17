"""Adopt narrowly defined current controller wires from an actual qualified capture."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from refresh_compat_runtime import CONTROLLER_PROFILES as PROFILES, refresh


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--profile", choices=tuple(PROFILES), required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--primary-audit", type=Path, required=True)
    parser.add_argument("--preceding-adoption-report", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    refresh(parser.parse_args())


if __name__ == "__main__":
    main()
