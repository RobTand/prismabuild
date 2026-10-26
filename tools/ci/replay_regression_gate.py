"""Replay saved GitHub PR metadata; optional declared-fix probes read immutable blobs."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import gzip
import json
from pathlib import Path
import subprocess

from check_regression_test import change_type, check_pull_request, is_fix, is_test_file


def git(*args: str) -> str:
    return subprocess.check_output(['git', *args], text=True)


def replay(corpus: dict, declared_fixes: list[int]) -> dict:
    rows = []
    for entry in corpus['pulls']:
        pull = dict(entry['pull'])
        labels_unavailable = pull.get('labels') is None
        pull['labels'] = pull.get('labels') or []
        files = [dict(file) for file in entry['files']]
        probe = pull['number'] in declared_fixes
        if probe:
            pull['body'] = (pull['body'] or '') + '\nChange type: fix\n'
            base = git('merge-base', pull['base']['sha'], pull['head']['sha']).strip()
            for file in files:
                if not is_test_file(file['filename']) or file['status'] not in {'added', 'modified', 'renamed', 'removed'}:
                    continue
                file['before'] = '' if file['status'] == 'added' else git(
                    'show', base + ':' + file.get('previous_filename', file['filename']))
                file['after'] = '' if file['status'] == 'removed' else git('show', pull['head']['sha'] + ':' + file['filename'])
        declaration = change_type(pull)
        rows.append({'number': pull['number'], 'title': pull['title'],
                     'url': pull['html_url'], 'head': pull['head']['sha'],
                     'linked_issues': entry['linked_issues'],
                     'declaration_probe': probe, 'change_type': declaration,
                     'labels_unavailable': labels_unavailable,
                     'fix_signal': is_fix(pull),
                     'classified_fix': declaration is not None and is_fix(pull),
                     **asdict(check_pull_request(pull, files))})
    return {'pulls': len(rows), 'classified_fixes': sum(r['classified_fix'] for r in rows),
            'fix_signals': sum(r['fix_signal'] for r in rows),
            'missing_declarations': sum(r['change_type'] is None for r in rows),
            'labels_unavailable': sum(r['labels_unavailable'] for r in rows),
            'rejected': sum(not r['allowed'] for r in rows),
            'declaration_probes': declared_fixes, 'live_rollout_observations': 0, 'rows': rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('corpus', type=Path)
    parser.add_argument('--declare-fix', type=int, action='append', default=[],
                        help='counterfactual declaration probe, never a live rollout observation')
    args = parser.parse_args()
    opener = gzip.open if args.corpus.suffix == '.gz' else open
    with opener(args.corpus, 'rt') as stream:
        result = replay(json.load(stream), args.declare_fix)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
