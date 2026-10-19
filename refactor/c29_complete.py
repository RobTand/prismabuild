#!/usr/bin/env python3
"""Replay the remaining C29 owner and binding moves without importing product code."""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
import re
import subprocess

BASE = "fe416690b87aba99ad30c7df4f95ac8da47055d5"
LIBRARIES = (
    "submission_frontend", "pbtest_collection", "pbtest_capabilities",
    "runtime_paths", "role_log_identity", "deferred_release",
    "manifest_promotion", "fleet_submit", "pbmergeq_runtime", "prelaunch_tier",
)
OWNERS = {name: name for name in LIBRARIES} | {"tier_loop": "tier_cycle"}
MOVES = {f"tools/fleet/{old}.py": f"src/prismabuild/{new}.py"
         for old, new in OWNERS.items()}
PROTECTED = (
    "tests/compat/", "src/prismabuild/record_kinds.py", "tests/fleet_world.py",
    "src/prismabuild/core.py", "src/prismabuild/digest_primitives.py",
    "src/prismabuild/immutable_files.py", "src/prismabuild/local_scratch.py",
    "src/prismabuild/kept_reads.py", "tools/fleet/local_scratch_profile.py",
)
BOOTSTRAP = '''_PB_ENTRYPOINT = Path(__file__).resolve(strict=True)
sys.path.insert(0, str(_PB_ENTRYPOINT.parents[
    2 if _PB_ENTRYPOINT.parent.name == "fleet" else 1] / "src"))
'''
ENTRY = '''#!/usr/bin/env python3
"""Run the packaged tier role from this generation's physical entry point."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
''' + BOOTSTRAP + '''from prismabuild.tier_cycle import main

if __name__ == "__main__":
    raise SystemExit(main(entrypoint=_PB_ENTRYPOINT))
'''


def module_literals(source: str) -> str:
    """Retarget literal module addresses only in import and patch calls."""
    raw = source.encode()
    lines = raw.splitlines(keepends=True)
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line))
    edits = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        if ast.unparse(node.func) not in {
                'import_module', 'importlib.import_module',
                'patch', 'mock.patch', 'unittest.mock.patch',
                'monkeypatch.setattr'}:
            continue
        literal = node.args[0]
        if not isinstance(literal, ast.Constant) or not isinstance(literal.value, str):
            continue
        old = literal.value.split('.')[0]
        if old not in OWNERS:
            continue
        start = offsets[literal.lineno - 1] + literal.col_offset
        end = offsets[literal.end_lineno - 1] + literal.end_col_offset
        value = raw[start:end].replace(old.encode(), ('prismabuild.' + OWNERS[old]).encode(), 1)
        edits.append((start, end, value))
    for start, end, value in sorted(edits, reverse=True):
        raw = raw[:start] + value + raw[end:]
    return raw.decode()


def bindings(source: str) -> str:
    """Retarget actual imports, embedded imports and module patch strings."""
    source = module_literals(source)
    for old, new in OWNERS.items():
        alias = f" as {old}" if new != old else ""
        source = re.sub(
            rf"(?m)^(\s*)import {old}(?=\s*(?:as\b|#|$))([^\n]*)",
            lambda m: m[1] + f"from prismabuild import {new}"
            + (alias if not m[2].lstrip().startswith("as ") else "") + m[2], source)
        source = re.sub(rf"\bfrom {old} import\b", f"from prismabuild.{new} import", source)
        for quote in ('"', "'"):
            source = source.replace(quote + f'import {old}\\n',
                                    quote + f'from prismabuild import {new}{alias}\\n')
        # The conftest table names modules explicitly; data filenames do not.
        source = source.replace(f'("{old}", "SH",', f'("prismabuild.{new}", "SH",')
    return source


def owner_source(source: str, old: str) -> str:
    """Remove script bootstrap and bind the original tool-directory questions."""
    source = bindings(source)
    if source.startswith("#!/"):
        source = source.split("\n", 1)[1]
    source = re.sub(r'(?m)^sys\.path\.insert\(0, str\(Path\(__file__\)\.resolve\((?:strict=True)?\)\.parent\)\)\n', '', source)
    source = re.sub(r'(?m)^from prismabuild\.runtime_paths import generation_root[^\n]*\n', '', source)
    source = re.sub(r'(?m)^sys\.path\.insert\(0, str\(generation_root\(__file__\) / "src"\)\)\n', '', source)
    source = source.replace('sys.path.insert(0, str(RUNTIME_ROOT / "src"))\n', '')
    name = Path(old).stem
    if name == 'submission_frontend':
        source = source.replace('import sys\n', 'import sys\nfrom prismabuild.runtime_paths import generation_root\n')
    if name in {'tier_loop', 'fleet_submit', 'deferred_release', 'manifest_promotion'}:
        target = 'slurm_job.py' if name == 'fleet_submit' else 'tier_loop.py'
        anchor = 'from pathlib import Path\n'
        source = source.replace(anchor, anchor + '\nENTRYPOINT = (Path(__file__).resolve(strict=True).parents[2]\n'
                                f'              / "tools" / "fleet" / "{target}")\n', 1)
        if name == 'fleet_submit':
            source = source.replace('RUNTIME_ROOT = generation_root(__file__)',
                                    'RUNTIME_ROOT = ENTRYPOINT.parents[2]')
            source = source.replace('Path(__file__).resolve(strict=True).parent / "slurm_job.py"',
                                    'ENTRYPOINT')
        source = source.replace('store_for(__file__', 'store_for(ENTRYPOINT')
        if name == 'tier_loop':
            source = source.replace('Path(__file__).resolve().parent', 'ENTRYPOINT.parent')
            source = source.replace('Path(__file__)', 'Path(ENTRYPOINT)')
            # The source-file binding above must keep naming this package file.
            source = source.replace('ENTRYPOINT = (Path(ENTRYPOINT)', 'ENTRYPOINT = (Path(__file__)')
            source = source.replace(
                'def main(argv: list[str] | None = None) -> int:',
                'def main(argv: list[str] | None = None, *, entrypoint: str | Path | None = None) -> int:')
            source = source.replace('    args = _parser().parse_args(argv)\n',
                                    '    global ENTRYPOINT, MOVER_TOOLS_ROOT\n'
                                    '    if entrypoint is not None:\n'
                                    '        ENTRYPOINT = Path(entrypoint)\n'
                                    '        MOVER_TOOLS_ROOT = str(ENTRYPOINT.parent)\n'
                                    '    args = _parser().parse_args(argv)\n', 1)
            source = source.replace('\n\nif __name__ == "__main__":\n    raise SystemExit(main())\n', '\n')
            source = source.rstrip() + '\n'
    return source


def script_bootstrap(source: str) -> str:
    if re.search(r'(?m)^from runtime_paths import', source):
        point = re.search(r'(?m)^from runtime_paths import', source).start()
        if not re.search(r'(?m)^from pathlib import .*\bPath\b', source[:point]):
            source = source[:point] + 'from pathlib import Path\n' + source[point:]
            point += len('from pathlib import Path\n')
        source = source[:point] + BOOTSTRAP + source[point:]
    return bindings(source)


def publish_source(source: str) -> str:
    if 'STANDALONE_PACKAGE_SOURCES' in source:
        return source
    # Delete library-only rows and their obsolete shipping rationale.
    source = re.sub(r'    # \.\.\.and deferred_release\.py,[\s\S]*?    "manifest_promotion.py",\n', '', source)
    source = re.sub(r'    # tier_loop\.py imports prelaunch_tier[\s\S]*?    "prelaunch_tier.py",\n', '', source)
    for name in LIBRARIES:
        source = source.replace(f'"{name}.py", ', '').replace(f', "{name}.py"', '')
    source = source.replace('    "role_log_identity.py", ', '    ')
    source = source.replace('    # The SLURM lane\'s two halves: the shared submit every producer routes\n'
                            '    # through, and the job entry it names.  A runtime published without them\n'
                            '    # has producers importing a module that is not there and a batch script\n'
                            '    # execing a path that does not exist.\n',
                            '    # The retained SLURM lane job entry. Submission is a package owner.\n')
    marker = '#: Fleet tools deliberately left out of the generation, each with the reason.\n'
    source = source.replace(marker, '# The retained proxy verifies the same stdlib layout helper beside itself.\n'
                            'STANDALONE_PACKAGE_SOURCES = {\n'
                            '    "runtime_paths.py": "src/prismabuild/runtime_paths.py",\n'
                            '}\n\n' + marker)
    anchor = 'def _source_for(name: str) -> Path:\n'
    source = source.replace(anchor, anchor + '    parts = name.split("/")\n'
                            '    if (parts[:-1] in (["tools"], ["tools", "fleet"])\n'
                            '            and parts[-1] in STANDALONE_PACKAGE_SOURCES):\n'
                            '        return CHECKOUT / STANDALONE_PACKAGE_SOURCES[parts[-1]]\n')
    anchor = '    for name in FLEET_SCRIPTS:\n'
    source = source.replace(anchor, '    for name, owner in STANDALONE_PACKAGE_SOURCES.items():\n'
                            '        if owner not in published:\n'
                            '            continue\n'
                            '        published[f"tools/{name}"] = published[owner]\n'
                            '        published[f"tools/fleet/{name}"] = published[owner]\n' + anchor, 1)
    # Relocate the existing missing-library check to the actual package owner.
    source = source.replace('names.update(alias.name.split(".")[0] for alias in node.names)',
                            'names.update(alias.name.split(".")[0] for alias in node.names)\n'
                            '            names.update(alias.name for alias in node.names\n'
                            '                         if alias.name.startswith("prismabuild."))')
    source = source.replace('            names.add(node.module.split(".")[0])\n',
                            '            names.add(node.module.split(".")[0])\n'
                            '            if node.module == "prismabuild":\n'
                            '                names.update("prismabuild." + alias.name for alias in node.names)\n'
                            '            elif node.module.startswith("prismabuild."):\n'
                            '                names.add(node.module)\n')
    source = source.replace('        for name in sorted(_imported_modules(source)):\n',
                            '        for name in sorted(_imported_modules(source)):\n'
                            '            if name.startswith("prismabuild."):\n'
                            '                package_member = "src/" + name.replace(".", "/") + ".py"\n'
                            '                if (CHECKOUT / package_member).is_file() and package_member not in published:\n'
                            '                    problems.append(f"{member} imports {name}, which this "\n'
                            '                                    "publication does not carry")\n')
    return source


def current_source(source: str, path: str) -> str:
    if path.startswith('tools/fleet/'):
        source = script_bootstrap(source)
    else:
        source = bindings(source)
    if path == 'tools/fleet/publish_runtime.py':
        source = publish_source(source)
    if path == 'tools/fleet/admission_shared_io.py' and '    try:\n        # Lazy and guarded:' in source:
        start = source.index('    try:\n        # Lazy and guarded:')
        end = source.index('    return completed.stdout.strip()', start)
        source = source[:start] + ('    from prismabuild import core\n'
                                  "    completed = core._git_run(os.getcwd(), 'rev-parse', 'HEAD')\n") + source[end:]
    if path == 'tools/fleet/fleet_membership.py' and '#: Reader-lease containment identifiers' in source:
        source = source.replace('    try:\n        from prismabuild import reader_lease as module  # noqa: E402\n'
                                '    except ImportError:\n        return None\n',
                                '    from prismabuild import reader_lease as module\n')
        source = source.replace('"""The reader-lease module when its lane has merged, else None.\n\n'
                                '    No vendoring, no parallel reimplementation: enumeration, validation,\n'
                                '    and release run through the owning module whenever it is importable.\n'
                                '    """', '"""Load the required reader-lease owner at the original call boundary."""')
        source = source.replace('from prismabuild import fleet_roster, pool as pool_module',
                                'from prismabuild import fleet_roster, reader_lease, pool as pool_module')
        start = source.index('#: Reader-lease containment identifiers')
        end = source.index('def reader_refs_census(', start)
        source = source[:start] + '\n' + source[end:]
        start = source.index('    """This host\'s reader refs and how the answer was established.')
        end = source.index('\n\ndef reader_refs_gate(', start)
        source = source[:start] + '''    """Enumerate reader refs through the required owner; unreadable stays unknown."""
    try:
        refs = reader_lease.refs_for_holder(queue, host)
    except (OSError, ValueError) as exc:
        return [], f"unknown-refs-unreadable: {exc}"
    return ([dict(r) for r in refs], "enumerated")
''' + source[end:]
        source = source.replace('    if state == "drained-absent":\n        return True, state, {"refs": []}\n', '')
        source = source.replace('the proof. Drained when the leases namespace is provably absent, or\n'
                                '    when an enumerated census names no refs for this host. Anything else —\n',
                                'the proof. The required owner must enumerate no refs for this host,\n'
                                '    including an absent namespace. Anything else retains the fence:\n')
    if path == 'tools/fleet/pbtest.py':
        source = source.replace('here.with_name("pbtest_collection.py").read_text()',
                                'Path(pbtest_collection.__file__).read_text()')
        source = source.replace('from prismabuild import dependency_digest  # noqa: E402',
                                'from prismabuild import dependency_digest, pbtest_collection  # noqa: E402')
        source = source.replace('# Only shipped helper imports bind to the private owner. A later package\n'
                                '# import loads its real on-disk module, preserving core\'s source identity.',
                                '# Only shipped helper imports bind to their captured owners. A later package\n'
                                '# import loads its real on-disk module, preserving that package\'s identity.')
        if '        return shipped_collection\n' not in source:
            source = source.replace('        return shipped_owner\n',
                                    '        return shipped_owner\n'
                                    '    if name == "prismabuild.pbtest_collection" and fromlist and level == 0:\n'
                                    '        return shipped_collection\n', 1)
        source = source.replace('\nload("pbtest_collection")\n',
                                '\nshipped_collection = load("pbtest_collection")\n')
    if path == 'tests/submission_support.py':
        source = source.replace("        if Path(filename).parent.name not in {'fleet', 'tools'}:\n",
                                "        if (Path(filename).parent.name not in {'fleet', 'tools'}\n"
                                "                and getattr(module, '__name__', '') not in {\n"
                                "                    'prismabuild.submission_frontend',\n"
                                "                    'prismabuild.deferred_release',\n"
                                "                    'prismabuild.manifest_promotion',\n"
                                "                    'prismabuild.fleet_submit',\n"
                                "                }):\n")
    if path == 'tools/fleet/seal_and_publish.py' and 'job_entry=Path(__file__)' not in source:
        source = source.replace('            transport=args.transport,\n',
                                '            transport=args.transport,\n'
                                '            job_entry=Path(__file__).resolve(strict=True).with_name("slurm_job.py"),\n')
    if path == 'src/prismabuild/resource_scope.py':
        source = source.replace('                    (directory.relative_to(root) / dependency).as_posix(),\n',
                                '                    ("src/prismabuild/runtime_paths.py"\n'
                                '                     if dependency == "runtime_paths.py"\n'
                                '                     and "src/prismabuild/runtime_paths.py" in files\n'
                                '                     else (directory.relative_to(root) / dependency).as_posix()),\n')
    if path in {'tests/tier_receipt_fixture.py', 'tests/test_tier_requires_known_receipts.py'}:
        source = source.replace('repository / "tools" / "fleet" / "tier_loop.py"',
                                'repository / "src" / "prismabuild" / "tier_cycle.py"')
    if path == 'tests/test_movement_tool_roots_1659.py':
        source = source.replace('Path(tier_loop.__file__).resolve().parent', 'tier_loop.ENTRYPOINT.parent')
    if path == 'tests/test_fleet_submit.py':
        source = source.replace('fleet_submit.__file__).resolve().parent', 'fleet_submit.ENTRYPOINT).resolve().parent')
    if path == 'tests/test_submission_review_lifetimes.py':
        source = source.replace('deferred.dr.store_for(deferred.dr.__file__)',
                                'deferred.dr.store_for(deferred.dr.ENTRYPOINT)')
    if path in {'tests/test_tools_do_not_run_on_import.py', 'tests/test_fleet_tool_flags_have_help.py'}:
        source = source.replace("'stage_rounding'))", "'stage_rounding', "
                                + ', '.join(repr(n) for n in OWNERS.values()) + '))')
    if path in {'tests/test_retained_worker_proxy.py', 'tests/test_install_ram_mount_unit.py',
                'tests/test_captured_fleet_roster_consumers.py', 'tests/test_needrestart_broker_restart_exclusion.py'}:
        source = source.replace('_CHECKOUT / "tools" / "fleet" / "runtime_paths.py"',
                                '_CHECKOUT / "src" / "prismabuild" / "runtime_paths.py"')
    if path == 'tests/run_proof_container.py':
        source = source.replace('/source/tools/fleet/pbtest_collection.py',
                                '/source/src/prismabuild/pbtest_collection.py')
    if path == 'tests/test_needrestart_broker_restart_exclusion.py':
        source = source.replace('shutil.copy2(FLEET / name, directory / name)',
                                'shutil.copy2(ROOT / "src/prismabuild/runtime_paths.py"\n'
                                '                     if name == "runtime_paths.py" else FLEET / name, directory / name)')
    if path == 'tests/test_captured_fleet_roster_consumers.py':
        source = source.replace('"digest_primitives.py", "fleet_roster.py")',
                                '"digest_primitives.py", "fleet_roster.py", "runtime_paths.py")')
        source = source.replace('shutil.copyfile(ROOT / "tools/fleet" / name, tools / name)',
                                'shutil.copyfile(ROOT / "src/prismabuild/runtime_paths.py"\n'
                                '                        if name == "runtime_paths.py" else ROOT / "tools/fleet" / name, tools / name)')
    if path == 'tests/test_retained_worker_proxy.py':
        source = source.replace('"residency_map", "storage_tiers")',
                                '"residency_map", "storage_tiers", "runtime_paths")')
    if path == 'tests/test_ram_fill_host_tokens_follow_the_fence.py':
        source = source.replace('"tools/fleet/tier_loop.py", "tools/fleet/stage_release.py"',
                                '"src/prismabuild/tier_cycle.py", "src/prismabuild/stage_release.py"')
    if path == 'tests/test_publish_runtime.py':
        source = source.replace('(fleet / "manifest_promotion.py").write_text("")',
                                '(checkout / "src/prismabuild/manifest_promotion.py").write_text("")')
        source = source.replace('match="tier_loop.py imports manifest_promotion"',
                                'match="tier_loop.py imports prismabuild.manifest_promotion"')
        start = source.index('def test_publish_refuses_a_script_whose_import_it_does_not_carry(')
        end = source.index('\n\ndef test_', start + 1)
        case = source[start:end]
        case = case.replace('from prismabuild import manifest_promotion\\n', 'import pbtest_outcomes\\n')
        case = case.replace('checkout / "src/prismabuild/manifest_promotion.py"', 'fleet / "pbtest_outcomes.py"')
        case = case.replace('"manifest_promotion.py"', '"pbtest_outcomes.py"')
        case = case.replace('imports prismabuild.manifest_promotion', 'imports pbtest_outcomes')
        source = source[:start] + case + source[end:]
    if path == 'tests/test_reader_refs_holder_unknown.py':
        source = source.replace('module._reader_lease() is reader_lease', 'module.reader_lease is reader_lease')
    if path == 'tests/test_role_log_migration_generation_integrity.py':
        source = source.replace('        assert name in publish_runtime.FLEET_SCRIPTS\n',
                                '        assert (f"src/prismabuild/{name}" in published\n'
                                '                if name == "role_log_identity.py" else name in publish_runtime.FLEET_SCRIPTS)\n')
        source = source.replace('        source = ROOT / "tools/fleet" / name\n',
                                '        source = ROOT / ("src/prismabuild" if name == "role_log_identity.py"\n'
                                '                         else "tools/fleet") / name\n')
        source = source.replace('        for member in (f"tools/{name}", f"tools/fleet/{name}"):\n',
                                '        for member in ((f"src/prismabuild/{name}",) if name == "role_log_identity.py"\n'
                                '                       else (f"tools/{name}", f"tools/fleet/{name}")):\n')
        source = source.replace('for name in ("role_log_identity", "supervise", "publish_runtime"):',
                                'for name in ("prismabuild.role_log_identity", "supervise", "publish_runtime"):')
        source = source.replace('        assert f"tools/{name}.py" in published\n',
                                '        assert ("src/" + name.replace(".", "/") + ".py"\n'
                                '                if name.startswith("prismabuild.") else f"tools/{name}.py") in published\n')
        source = source.replace('del incomplete["tools/role_log_identity.py"]',
                                'del incomplete["src/prismabuild/role_log_identity.py"]')
        source = source.replace('imports role_log_identity,', 'imports prismabuild.role_log_identity,')
    return source


def migrate(root: Path) -> list[str]:
    changed = []
    paths = subprocess.check_output(['git', '-C', str(root), 'ls-files'], text=True).splitlines()
    for old, new in MOVES.items():
        target = root / new
        if target.exists():
            continue
        target.write_text(owner_source((root / old).read_text(), old))
        changed.append(new)
        if old.endswith('/tier_loop.py'):
            (root / old).write_text(ENTRY)
        else:
            (root / old).unlink()
        changed.append(old)
    for path in paths:
        file = root / path
        if (not file.exists() or not path.endswith('.py') or path in MOVES
                or path.startswith(PROTECTED + ('refactor/', 'tools/maintenance/'))):
            continue
        before = file.read_text()
        after = current_source(before, path)
        if before != after:
            file.write_text(after)
            changed.append(path)
    path = root / 'tests/fixtures/duplication_baseline.json'
    before = path.read_text()
    after = before
    for old, new in MOVES.items():
        after = after.replace(old, new)
    if before != after:
        path.write_text(after)
        changed.append(str(path.relative_to(root)))
    return sorted(changed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    changes = migrate(args.root.resolve())
    args.report.write_text(json.dumps({'base': BASE, 'moves': MOVES, 'changed': changes}, indent=2) + '\n')
    print(f'c29-complete: {len(changes)} source paths changed')


if __name__ == '__main__':
    main()
