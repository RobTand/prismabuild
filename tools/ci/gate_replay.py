#!/usr/bin/env python3
"""Capture merged PRs through REST or replay their files against the current population."""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import importlib.util
import json
import math
from pathlib import Path
import subprocess

SPEC = importlib.util.spec_from_file_location('gate_select', Path(__file__).with_name('gate_select.py'))
select = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(select)


def summarize(rows: list[dict]) -> dict:
    def quantiles(key):
        values = [row[key] for row in rows]
        if not values or any(value is None for value in values):
            return {'p50': None, 'p90': None}
        values.sort()
        return {label: values[math.ceil(fraction * len(values)) - 1]
                for label, fraction in (('p50', 0.5), ('p90', 0.9))}
    return {'prs': len(rows), 'selected_files': quantiles('selected_files'),
            'wall_seconds': quantiles('wall_seconds'),
            'distribution': {'over_300_files': sum(row['selected_files'] > 300 for row in rows),
                             'over_600_files': sum(row['selected_files'] > 600 for row in rows)},
            'unknown_cost_prs': [row['number'] for row in rows if row['wall_seconds'] is None]}



def changed_functions(before: str, after: str) -> dict:
    """AST-only diagnostic. Includes enclosing functions when nested bodies change."""
    def definitions(source):
        found = {}
        def visit(node, prefix=''):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    name = prefix + child.name
                    if not isinstance(child, ast.ClassDef):
                        found.setdefault(name, []).append(ast.dump(child, include_attributes=False))
                    visit(child, name + '.')
                else:
                    visit(child, prefix)
        visit(ast.parse(source))
        return found
    old, new = definitions(before), definitions(after)
    changed = sorted(name for name in old.keys() & new.keys() if old[name] != new[name])
    added, removed = sorted(new.keys() - old.keys()), sorted(old.keys() - new.keys())
    return {'changed': changed, 'added': added, 'removed': removed,
            'count': len(changed) + len(added) + len(removed),
            'before_functions': len(old), 'after_functions': len(new)}


def pool_function_report(captured: dict, root: Path | None = None) -> dict:
    rows = []
    for pull in captured['pulls']:
        def source(side):
            if side in pull:
                return pull[side]
            sha = pull['base_sha' if side == 'before' else 'head_sha']
            return subprocess.check_output(['git', 'show', sha + ':src/prismabuild/pool.py'],
                                           cwd=root, text=True)
        diff = changed_functions(source('before'), source('after'))
        rows.append({key: value for key, value in pull.items() if key not in {'before', 'after'}} | diff)
    counts = sorted(row['count'] for row in rows)
    return {'schema': 'prismabuild.pool_changed_functions.v1',
            'source': {key: value for key, value in captured.items() if key != 'pulls'},
            'method': 'AST function and method definitions, including enclosing functions for nested-body changes; '
                      'ignores formatting and comments; diagnostic only, no function-level test selection',
            'prs': len(rows), 'counts': {'min': min(counts) if counts else None,
                'p50': counts[math.ceil(len(counts) * .5) - 1] if counts else None,
                'p90': counts[math.ceil(len(counts) * .9) - 1] if counts else None,
                'max': max(counts) if counts else None}, 'pulls': rows}

def api(endpoint: str):
    """Only GET endpoints in this repository. Never load credentials as data."""
    return json.loads(subprocess.check_output(
        ['gh', 'api', 'repos/RobTand/prismabuild/' + endpoint], text=True))


def capture_history(destination: Path, count: int = 150) -> Path:
    destination.mkdir(exist_ok=False, parents=True)
    merged = {}
    for page in range(1, 101):
        pulls = api(f'pulls?state=closed&sort=updated&direction=desc&per_page=100&page={page}')
        for pull in pulls:
            if pull['merged_at']:
                merged[pull['number']] = pull
        ordered = sorted(merged.values(), key=lambda row: (row['merged_at'], row['number']), reverse=True)
        # updated_at >= merged_at. Older pages cannot hide a newer merged PR
        # once this boundary has crossed the oldest retained merge timestamp.
        if not pulls or (len(ordered) >= count and pulls[-1]['updated_at'] <= ordered[count - 1]['merged_at']):
            break
    else:
        raise ValueError('cannot prove latest-merged boundary within 100 REST pages')
    if len(ordered) < count:
        raise ValueError(f'only {len(ordered)} merged PRs found, need {count}')
    rows = []
    for pull in ordered[:count]:
        files = []
        for file_page in range(1, 31):
            batch = api(f"pulls/{pull['number']}/files?per_page=100&page={file_page}")
            files.extend(batch)
            if len(batch) < 100:
                break
        else:
            raise ValueError(f"PR {pull['number']} reached the REST 3000-file limit")
        row = {key: pull[key] for key in ('number', 'merged_at', 'merge_commit_sha', 'html_url')}
        row.update(files_count=len(files), changed_files=sorted({
            path for file in files for path in (file['filename'], file.get('previous_filename', file['filename']))}))
        (destination / f"pr-{pull['number']}.json").write_text(json.dumps(row, sort_keys=True) + '\n')
        rows.append(row)
    output = destination / 'history.json'
    output.write_text(json.dumps({'repository': 'RobTand/prismabuild',
        'captured_at': datetime.now(timezone.utc).isoformat(),
        'source': 'GitHub REST pulls and pull files', 'boundary_pages': page, 'pulls': rows}, indent=2) + '\n')
    return output


def replay(root: Path, history: dict) -> dict:
    index = select.dependency_index(root)
    durations = select.read_json(root / 'tools/ci/gate_durations.json')
    quarantine = select.read_json(root / 'tools/ci/gate_quarantine.json')
    def plan(paths):
        return select.create_plan(root, paths, durations=durations, quarantine=quarantine, index=index)
    rows = []
    for pull in history['pulls']:
        selected = plan(pull['changed_files'])
        rows.append({'number': pull['number'], 'merged_at': pull['merged_at'],
                     'changed_files': len(pull['changed_files']), 'selected_files': len(selected['selected']),
                     'wall_seconds': selected['estimate']['wall_seconds'],
                     'missing_duration_files': len(selected['estimate']['missing_files'])})
    population = plan([])['population']
    modules = {}
    for name in ('pool', 'core'):
        path = f'src/prismabuild/{name}.py'
        selected = plan([path])
        modules[path] = {'selected_files': len(selected['selected']),
                         'estimate': selected['estimate']}
    return {'schema': 'prismabuild.gate_replay.v1',
            'source': {key: value for key, value in history.items() if key != 'pulls'},
            'population': population,
            'method': 'latest merged PR files against current population, nearest-rank percentiles, '
                      'five-worker file-level longest-first packing, excluding pickup and setup',
            'summary': summarize(rows), 'large_modules': modules, 'pulls': rows}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument('--fetch-history', type=Path, help='fresh empty destination, REST GET only')
    inputs.add_argument('--history', type=Path, help='captured history.json to replay')
    inputs.add_argument('--pool-functions', type=Path, help='captured base/head pool sources for AST diagnostics')
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    if args.fetch_history:
        print(capture_history(args.fetch_history))
        return 0
    report = (pool_function_report(json.loads(args.pool_functions.read_text()), args.root.resolve()) if args.pool_functions
              else replay(args.root.resolve(), json.loads(args.history.read_text())))
    rendered = json.dumps(report, sort_keys=True, indent=2) + '\n'
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end='')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
