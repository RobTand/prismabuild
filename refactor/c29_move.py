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
    text = re.sub(r'(?m)^(\s*)import prewarm_loop, stage_move\b', r'\1from prismabuild import prewarm_loop, stage_move', text)
    for name in MODULES:
        text = re.sub(rf"(?m)^(\s*)import {name}(?=\s*(?:as\b|#|$))",
                      rf"\1from prismabuild import {name}", text)
        text = re.sub(rf"\bfrom {name} import\b", f"from prismabuild.{name} import", text)
        # Quoted imports and patch targets also occur inside embedded Python.
        text = re.sub(rf'(import_module\(["\x27]){name}(["\x27])',
                      rf"\1prismabuild.{name}\2", text)
        text = text.replace(f'("{name}", "SH",', f'("prismabuild.{name}", "SH",')
    return text



def movement_body(text: str) -> str:
    """Remove exactly the obsolete dependency-unavailable paths."""
    text = migrate_imports(text)
    old = """        try:
            from prismabuild.stage_release import _unended_owner
        except ImportError as exc:
            return [{"consumer_action_key": consumer,
                     "mover_action_key": mover, "state": "uncertain",
                     "why": f"the ending proof is unavailable: {exc}"}
                    for consumer, mover in sorted(pairs)]
"""
    text = text.replace(old, '        from prismabuild.stage_release import _unended_owner\n')
    old = """        try:
            from prismabuild.stage_release import _claimed_paths
        except ImportError as exc:
            return None, f"claim authority unavailable: {exc}"
"""
    text = text.replace(old, '        from prismabuild.stage_release import _claimed_paths\n')
    old = """    try:
        from prismabuild.produced_output import PRODUCED_OUTPUT_REF_SCHEMA_V1
    except ImportError:
        return False
"""
    text = text.replace(old, '    from prismabuild.produced_output import PRODUCED_OUTPUT_REF_SCHEMA_V1\n')
    return text.replace('(ImportError, OSError, ValueError, pool.PoolContractError)',
                        '(OSError, ValueError, pool.PoolContractError)').replace(
                            '(ImportError, OSError, ValueError, pb.PrismaBuildError)',
                            '(OSError, ValueError, pb.PrismaBuildError)')


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
        text = movement_body(text)
        if old.endswith('stage_release.py'):
            text = text.replace('RANGE_SUFFIX, _is_action_key, paths_named_once,', 'RANGE_SUFFIX, paths_named_once,')
        target.write_text(text)
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
        if path == 'tests/test_pbtest.py':
            after = after.replace('    tests.mkdir(parents=True)\n    worker_path =',
                                  '    tests.mkdir(parents=True)\n    (checkout / "pytest.ini").write_text("[pytest]\\n")\n    worker_path =')
        if path == 'tests/test_pbtest_sealed_basetemp_1469.py':
            after = after.replace('    tests.mkdir(parents=True, exist_ok=True)\n    (tests /',
                                  '    tests.mkdir(parents=True, exist_ok=True)\n    (checkout / "pytest.ini").write_text("[pytest]\\n")\n    (tests /')
            after = after.replace('    tests.mkdir(parents=True)\n    for name in',
                                  '    tests.mkdir(parents=True)\n    (checkout / "pytest.ini").write_text("[pytest]\\n")\n    for name in')
        if path == 'tests/test_a_nested_prefix_successors_adopted_file_survives_retirement.py':
            after = after.replace('    po._UNFILED_REPORTS.clear()\n    yield\n    po._UNFILED_REPORTS.clear()',
                                  '    po._UNFILED_REPORTS.clear()\n    po._TEMPLATE_BODIES.clear()\n    yield\n    po._UNFILED_REPORTS.clear()\n    po._TEMPLATE_BODIES.clear()')
        if path == 'tests/test_record_store_reader_controls.py':
            after = after.replace('    native = os.scandir\n    seen = []',
                                  '    native = os.scandir\n    native_listdir = os.listdir\n    seen = []')
            original = "    monkeypatch.setattr(os, 'scandir', scandir)\n"
            replacement = """    def listdir(path):
        if isinstance(path, (str, bytes, Path)) and Path(os.fsdecode(path)) == target:
            seen.append(str(path))
            raise OSError(error, 'actual incomplete cancellation namespace', str(path))
        return native_listdir(path)
    monkeypatch.setattr(os, 'scandir', scandir)
    monkeypatch.setattr(os, 'listdir', listdir)
"""
            if "monkeypatch.setattr(os, 'listdir', listdir)" not in after:
                after = after.replace(original, replacement)
        if path in {'tests/test_tools_do_not_run_on_import.py', 'tests/test_fleet_tool_flags_have_help.py'}:
            original = 'TOOLS = sorted((Path(__file__).resolve().parents[1] / "tools" / "fleet").glob("*.py"))'
            replacement = 'REPO = Path(__file__).resolve().parents[1]\nTOOLS = sorted([*(REPO / "tools" / "fleet").glob("*.py"),\n                *(REPO / "src" / "prismabuild" / (name + ".py")\n                  for name in '+repr(MODULES)+')])'
            after = after.replace(original, replacement)
        if path in {'tests/test_a_stalled_mover_ends_no_progress.py',
                    'tests/test_a_wedged_egress_ends_no_progress.py'}:
            after = after.replace('sys.path.insert(0, {tools!r})', 'sys.path.insert(0, {source!r})')
            after = after.replace('tools=str(ROOT / "tools" / "fleet"),',
                                  'source=str(ROOT / "src"),')
        if path == 'tests/test_produced_output_tier_host_egress.py':
            start = after.index('def test_an_owner_without_the_fleet_tool_retires_through_the_action_anywhere(')
            end = after.index('\ndef test_', start + 1)
            case = after[start:end]
            doc_start = case.index('    """')
            doc_end = case.index('    """', doc_start + 7) + 7
            case = case[:doc_start] + '    """The remote tier announcement selects an action without fleet imports.\n\n    The original native ID and assertions remain. Its former import-failure\n    setup now uses the explicit remote-host ownership boundary.\n    """' + case[doc_end:]
            case = case.replace('here = socket.gethostname()', 'here = ELSEWHERE')
            case = case.replace('world = restage._World(tmp_path, window_gib=1, gib=4)',
                                'world = _world_with_the_stage_elsewhere(tmp_path, window_gib=1, gib=4)')
            after = after[:start] + case + after[end:]
        if path == 'tests/test_prewarm_rollout.py':
            start = after.index('def load(name):') if 'def load(name):' in after else -1
            if start >= 0:
                end = after.index('@pytest.fixture', start)
                after = after[:start] + after[end:]
                after = after.replace('import importlib.util\n', '')
                after = after.replace('    loop = load("prewarm_loop")', '    from prismabuild import prewarm_loop as loop')
            after = after.replace("upgrade = load('upgrade_client')", 'import upgrade_client as upgrade')
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
