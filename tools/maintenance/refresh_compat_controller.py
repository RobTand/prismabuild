"""Adopt narrowly defined current controller wires from an actual qualified capture."""
from __future__ import annotations

import argparse
import json
import shutil
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
    parser.add_argument("--export", type=Path,
                        help="copy adopted files and the preserved originals to this new evidence directory")
    args = parser.parse_args()
    refresh(args)
    if args.export is not None:
        root = args.root.resolve()
        result = json.loads(args.report.read_bytes())
        suffix = PROFILES[args.profile][0]
        paths = [Path('tests/compat') / f'{name}-{suffix}.{extension}' for name, extension in
                 (('goldens', 'jsonl'), ('capture', 'json'))]
        archive = Path(result['archive'])
        if not (root / archive).resolve().is_relative_to(root):
            raise ValueError('adoption archive escaped the checkout')
        paths.extend(path.relative_to(root) for path in (root / archive).rglob('*') if path.is_file())
        args.export.mkdir(parents=True, exist_ok=False)
        for relative in paths:
            output = args.export / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / relative, output)
            if output.read_bytes() != (root / relative).read_bytes():
                raise ValueError('adoption export failed exact readback')


if __name__ == "__main__":
    main()
