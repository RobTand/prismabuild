"""Port only CURRENT compatibility drivers to the TierLedger owner."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import subprocess
import tarfile


@dataclass(frozen=True)
class Replacement:
    file: str
    before: str
    after: str
    count: int = 1


REPLACEMENTS = (
    Replacement('compat_floor_adapters.py', 'queue.ensure_layout()', 'queue.records.ensure_layout()'),
    Replacement('compat_egress_writer_adapters.py', 'queue.ensure_layout()', 'queue.records.ensure_layout()'),
    Replacement('compat_prelaunch_adapters.py', 'queue.ensure_layout()', 'queue.records.ensure_layout()'),
    Replacement('compat_batch_adapters.py', 'queue.ensure_layout()', 'queue.records.ensure_layout()'),
    Replacement('compat_spool_adapters.py', 'queue.ensure_layout()', 'queue.records.ensure_layout()'),
    Replacement('compat_floor_adapters.py',
                'queue.tier_ledger("compat-stage").ensure_capacity',
                'queue.tier("compat-stage").mint'),
    Replacement('compat_floor_adapters.py',
                'queue.tier_ledger("compat-stage").acquire',
                'queue.tier("compat-stage").take'),
    Replacement('compat_egress_writer_adapters.py',
                'queue.mint_tier_capacity(TIER, {"stage_gib": 1})',
                'queue.tier(TIER).mint({"stage_gib": 1})'),
    Replacement('compat_egress_writer_adapters.py',
                'queue.tier_ledger(TIER).acquire', 'queue.tier(TIER).take'),
    Replacement('compat_prelaunch_adapters.py',
                'queue.tier_ledger(TIER).begin_acquire', 'queue.tier(TIER).begin_take'),
    Replacement('compat_prelaunch_adapters.py',
                'queue.tier_ledger(TIER).commit_acquire', 'queue.tier(TIER).commit_take'),
    Replacement('compat_prelaunch_adapters.py',
                'queue.mint_tier_capacity(TIER, {"stage_gib": 2})',
                'queue.tier(TIER).mint({"stage_gib": 2})'),
    Replacement('compat_prelaunch_adapters.py',
                'queue.tier_ledger(TIER).release(holder)',
                'queue.tier(TIER).release_unspent(holder, authority=load("prismabuild.tier_holdings").Unspent.NEVER_STARTED)'),
    Replacement('compat_tier_adapters.py',
                'queue.mint_tier_capacity(name, {"stage_gib": 1})',
                'queue.tier(name).mint({"stage_gib": 1})'),
    Replacement('compat_builder_adapters.py',
                'queue.tier_ledger(tier).acquire', 'queue.tier(tier).take'),
    Replacement('compat_queue_adapters.py',
                'queue.mint_tier_capacity("stage:compat", {"stage_gib": 1})',
                'queue.tier("stage:compat").mint({"stage_gib": 1})'),
    Replacement('compat_spool_adapters.py',
                'queue.mint_tier_capacity(TIER, {"stage_gib": 2})',
                'queue.tier(TIER).mint({"stage_gib": 2})'),
    Replacement('compat_batch_adapters.py',
                'queue.mint_tier_capacity(TIER, {"stage_gib": 2})',
                'queue.tier(TIER).mint({"stage_gib": 2})'),
    Replacement('compat_resident_adapters.py',
                'queue.mint_tier_capacity("local:compat-host", {"local_gib": 1})',
                'queue.tier("local:compat-host").mint({"local_gib": 1})'),
)


def syntax_records(text, rules=()):
    records = Counter()

    def visit(node, owner=''):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            owner += '.' + node.name
            records['definition', owner, ast.dump(node.args) if hasattr(node, 'args') else '',
                    tuple(ast.dump(item) for item in node.decorator_list)] += 1
        if isinstance(node, (ast.Assert, ast.Raise)):
            source = ast.get_source_segment(text, node)
            for rule in rules:
                source = source.replace(rule.before, rule.after)
            records[type(node).__name__, owner, ast.dump(ast.parse(source).body[0])] += 1
        for child in ast.iter_child_nodes(node):
            visit(child, owner)

    visit(ast.parse(text))
    return records


def transformed(text, rules):
    result = text
    for rule in rules:
        old, new = result.count(rule.before), result.count(rule.after)
        if (old, new) == (rule.count, 0):
            result = result.replace(rule.before, rule.after)
        elif (old, new) != (0, rule.count):
            raise ValueError(f'{rule.file}: ambiguous preimage {rule.before!r}: {old=}, {new=}')
    ast.parse(result)
    return result


def require_retained_selector(root):
    driver = root / 'tests/compat/support/record_compat.py'
    tree = ast.parse(driver.read_bytes())
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    if not {'pre_c13_drivers', 'driver_for', 'invoke'} <= functions.keys():
        raise ValueError('Compose the qualified C13 retained/current driver selector first')
    calls = [node for node in ast.walk(functions['invoke']) if isinstance(node, ast.Call)]
    selector_calls = [node for node in ast.walk(functions['driver_for']) if isinstance(node, ast.Call)]
    if (not any(isinstance(node.func, ast.Name) and node.func.id == 'driver_for' for node in calls)
            or not any(isinstance(node.func, ast.Name) and node.func.id == 'pre_c13_drivers'
                       for node in selector_calls)):
        raise ValueError('invoke does not select the retained driver')
    with tarfile.open(root / 'tests/compat/drivers-before-c13.tar.gz') as archive:
        names = [member.name for member in archive.getmembers()]
        if len(names) != len(set(names)):
            raise ValueError('duplicate retained driver archive members')
        for rule in REPLACEMENTS:
            member = archive.getmember('tests/compat/support/' + rule.file)
            if not member.isfile():
                raise ValueError('retained driver member is not a regular file')
            text = archive.extractfile(member).read().decode()
            if text.count(rule.before) != rule.count or rule.after in text:
                raise ValueError(f'{rule.file}: retained preimage changed')


def port(root, *, destination=None):
    """Validate every preimage before staging or changing any driver."""
    root = Path(root).resolve()
    support = root / 'tests/compat/support'
    if destination is None:
        require_retained_selector(root)
    else:
        destination = Path(destination).resolve()
        if destination == support or destination.is_relative_to(support):
            raise ValueError('private driver destination must be outside the source drivers')
        if destination.exists():
            raise ValueError('private driver destination already exists')
    prepared, rows = {}, []
    for name in sorted({rule.file for rule in REPLACEMENTS}):
        rules = [rule for rule in REPLACEMENTS if rule.file == name]
        original = (support / name).read_text()
        updated = transformed(original, rules)
        if syntax_records(updated) != syntax_records(original, rules):
            raise ValueError(f'{name}: definition/assertion/raise occurrences changed')
        prepared[name] = updated
        rows.append(dict(file=name, changed=updated != original,
                         definitions_assertions_raises=sum(syntax_records(updated).values()),
                         replacements=[dict(before=r.before, after=r.after, count=r.count) for r in rules]))
    if destination is not None:
        shutil.copytree(support, destination, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    target = destination or support
    for name, text in prepared.items():
        path = target / name
        if path.read_text() != text:
            path.write_text(text)
    return dict(files=rows, root=str(root), driver=str(target),
                historical_driver_changed=False, current_only=True,
                scope='TierLedger port only; final owner composition and runtime captures require separate qualification')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--stage-driver', type=Path)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.report.exists():
        parser.error('report exists; retain it and choose a new output')
    report = port(args.root, destination=args.stage_driver)
    report['head'] = subprocess.check_output(
        ['git', '-C', str(args.root), 'rev-parse', 'HEAD'], text=True).strip()
    with args.report.open('x') as stream:
        json.dump(report, stream, indent=2)
        stream.write('\n')


if __name__ == '__main__':
    main()
