#!/usr/bin/env python3
"""Replay C29's source relocation and caller migration from the exact C05 base.

This is a source editor. It never imports product code or executes tests.
Use --root with a private checkout. Evidence and execution plans stay outside it.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess

BASE = "82f28e4ce36c3de222cc7818a5fb1c8837bb4f7d"
MODULES = ("prewarm_loop", "stage_move", "stage_release", "ram_promote", "stage_rounding")
MOVES = {f"tools/fleet/{name}.py": f"src/prismabuild/{name}.py" for name in MODULES}
BOOTSTRAP = '''sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))
'''
WRAPPER = '''#!/usr/bin/env python3
"""Run the packaged {name} entry point in this runtime generation."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root

sys.path.insert(0, str(generation_root(__file__) / "src"))
from prismabuild.{name} import main

if __name__ == "__main__":
    raise SystemExit(main())
'''


def migrate_imports(text: str) -> str:
    text = text.replace('import prewarm_loop, stage_move', 'from prismabuild import prewarm_loop, stage_move')
    for name in MODULES:
        text = re.sub(rf"(?m)^(\s*)import {name}(?=\s*(?:as\b|#|$))",
                      rf"\1from prismabuild import {name}", text)
        text = re.sub(rf"\bfrom {name} import\b", f"from prismabuild.{name} import", text)
        # Quoted imports and patch targets also occur inside embedded Python.
        text = re.sub(rf'(["\x27]){name}(?=[.](?!py["\x27])[A-Za-z_])', rf"\1prismabuild.{name}", text)
        text = re.sub(rf'(import_module\(["\x27]){name}(["\x27])',
                      rf"\1prismabuild.{name}\2", text)
        text = text.replace(f'("{name}", "SH",', f'("prismabuild.{name}", "SH",')
    return text


def produced_output(text: str) -> str:
    start = text.index("        stage_release = None\n        if in_process:\n")
    end = text.index("        if not in_process and tier_record is None:", start)
    text = text[:start] + '''        stage_release = None
        if in_process:
            from prismabuild import stage_release
''' + text[end:]
    old = '''    try:
        import stage_release  # type: ignore[import-not-found]
    except ImportError:
        for batch_id in selections:
            answers[batch_id] = {"ok": False,
                                 "refusal": "stage-release-unimportable"}
        return answers
'''
    text = text.replace(old, '    from prismabuild import stage_release\n')
    old = '''    try:
        import stage_release  # type: ignore[import-not-found]  # noqa: F401
    except ImportError:
        return {"event": report.refused("stage-release-unimportable",
                                        mover_key=mover)}
'''
    return text.replace(old, '')


def migrate(root: Path) -> list[str]:
    changed = []
    for old, new in MOVES.items():
        target = root / new
        if target.exists():
            continue
        text = (root / old).read_text()
        if old.endswith("prewarm_loop.py"):
            start = text.index('# The publisher writes this program twice,')
            text = text[:start] + text[text.index(BOOTSTRAP, start):]
        text = text.replace(BOOTSTRAP, '')
        if text.startswith('#!/usr/bin/env python3\n'):
            text = text.split('\n', 1)[1]
        target.write_text(migrate_imports(text))
        changed.append(new)
        if old.endswith("stage_rounding.py"):
            (root / old).unlink()
        else:
            (root / old).write_text(WRAPPER.format(name=Path(old).stem))
        changed.append(old)
    paths = subprocess.check_output(['git', '-C', str(root), 'ls-files'], text=True).splitlines()
    for path in paths:
        file = root / path
        if not file.exists() or file.suffix != '.py' or path in MOVES or path.startswith('refactor/') or path == 'tools/maintenance/verify_manifest_refactor.py':
            continue
        before = file.read_text()
        after = produced_output(before) if path == 'src/prismabuild/produced_output.py' and 'stage-release-unimportable' in before else before
        after = migrate_imports(after)
        # These assertions name the current owner, not the entry wrapper.
        if path in {'tests/test_prewarm_receipt_recovery.py', 'tests/test_prewarm_requires_known_receipts.py'}:
            after = after.replace('repository / "tools" / "fleet" / "prewarm_loop.py"',
                                  'repository / "src" / "prismabuild" / "prewarm_loop.py"')
        if path == 'tests/test_prewarm_rollout.py':
            start = after.index('def load(name):') if 'def load(name):' in after else -1
            if start >= 0:
                end = after.index('@pytest.fixture', start)
                after = after[:start] + after[end:]
                after = after.replace('import importlib.util\n', '')
                after = after.replace('    loop = load("prewarm_loop")', '    from prismabuild import prewarm_loop as loop')
        if path == 'tools/fleet/publish_runtime.py':
            after = after.replace('    # The tier role imports this helper for stage byte and token reports (#1555).\n    "stage_rounding.py",\n', '')
        if before != after:
            file.write_text(after)
            changed.append(path)
    path = root / 'tests/fixtures/duplication_baseline.json'
    before = path.read_text()
    after = before
    for old, new in MOVES.items():
        after = after.replace(old, new)
    if after != before:
        path.write_text(after)
        changed.append(str(path.relative_to(root)))
    return sorted(changed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--report', required=True, type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    head = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()
    subprocess.run(['git', '-C', str(root), 'merge-base', '--is-ancestor', BASE, head], check=True)
    changes = migrate(root)
    args.report.write_text(json.dumps({'base': BASE, 'root': str(root), 'head_before': head,
                                     'moves': MOVES, 'changed': changes}, indent=2) + '\n')


if __name__ == '__main__':
    main()
