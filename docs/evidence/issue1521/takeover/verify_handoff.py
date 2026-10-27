"""Check packaged evidence and the evidence-only tail; never execute tests."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    prefix = 'docs/evidence/issue1521/takeover/'
    summary = json.loads((root / 'validation.json').read_text())
    for name, expected in json.loads((root / 'file-hashes.json').read_text()).items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, name
    sources = [json.loads((root / name).read_text())
               for name in ('qualification-audit.json', 'integrated-audit.json')]
    keys = [entry['action_key'] for audit in sources for entry in audit['entries']]
    assert len(keys) == len(set(keys)) == summary['total_distinct_actions'] == 47
    assert sum(audit['passing_executions'] for audit in sources) == summary['total_passing_native_executions'] == 1033
    for audit, expected in zip(sources, summary['qualifications']):
        assert audit['head'] == expected['head']
        assert audit['actions'] == expected['actions'] == len(audit['entries'])
        assert audit['passing_executions'] == expected['passing_executions']
        assert len(audit['source_bundles']) == expected['source_bundles']
    original = set(summary['integrated_original_six_ids'])
    assert len(original) == 6
    assert original <= {node for entry in sources[1]['entries'] for node in entry['nodeids']}
    changes = subprocess.check_output([
        'git', '-C', str(args.repo), 'diff', '--name-only',
        summary['integrated_head'], 'HEAD',
    ]).decode().splitlines()
    assert all(path.startswith(prefix) for path in changes), changes
    print(json.dumps(dict(checks_passed=True, evidence_only_tail=changes,
                          actions=47, passing_native_executions=1033)))


if __name__ == '__main__':
    main()
