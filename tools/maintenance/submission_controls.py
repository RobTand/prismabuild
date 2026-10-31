"""Compare exact-base and candidate fixtures in one admitted worker action."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

BASE = 'f0bcb21e195378e7fb6e85e1bbfde24c45672e6b'
CONTROLS = ['tests/test_pbtest_sealed_basetemp_1469.py',
    'tests/test_a_range_decision_takes_its_censuses_once.py::test_a_partial_created_during_a_scan_is_reported_and_not_kept']
FOCUS = ['tests/test_pbgang_member_options_1517.py',
    'tests/test_pbtest_seals_its_shard_deadline.py',
    'tests/test_require_pool_interpreters_are_configured.py']


def compare_submission_sources(repo: Path, output: Path):
    output.mkdir(parents=True, exist_ok=True)
    results = {}
    with tempfile.TemporaryDirectory(prefix='c14-controls-') as temporary:
        baseline = Path(temporary) / 'base'
        subprocess.run(['git', 'clone', '--no-hardlinks', '--quiet', '--no-checkout',
                        str(repo), str(baseline)], check=True)
        subprocess.run(['git', '-C', str(baseline), 'checkout', '--quiet', '--detach', BASE], check=True)
        for name, checkout, selected in [('base', baseline, CONTROLS),
                                         ('candidate', repo, CONTROLS), ('focus', repo, FOCUS)]:
            environment = dict(os.environ, PYTHONPATH=os.pathsep.join(
                str(checkout / path) for path in ('src', 'tools/fleet', 'tests')))
            command = [sys.executable, '-c',
                'import pbtest_outcomes; raise SystemExit(pbtest_outcomes.main())',
                '-q', '--tb=short', *selected]
            run = subprocess.run(command, cwd=checkout, env=environment, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, text=True)
            (output / f'controls-{name}.log').write_text(run.stdout)
            if 'pbtest-outcomes: ' not in run.stdout or 'pbtest-completion: ' not in run.stdout:
                raise RuntimeError(f'{name}: control produced no native outcome or completion record')
            results[name] = {'returncode': run.returncode, 'command': command,
                'failed': [line for line in run.stdout.splitlines() if line.startswith(('FAILED ', 'ERROR '))]}
    (output / 'controls-result.json').write_text(json.dumps(results, indent=2) + '\n')
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(compare_submission_sources(Path.cwd(), args.output)))
    # Successful comparison execution is not a claim that either test run passed.
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
