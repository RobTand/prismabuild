"""Run inside an admitted PB action to prove #2270's refusals detect defects."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile

MUTANTS = {
    'no-debt': 'test_refusal_records_debt_and_holds_capacity_until_verified',
    'missing-evidence': 'test_refusal_records_debt_and_holds_capacity_until_verified',
    'nonce': 'test_reconciliation_refuses_wrong_or_unproved_identity[nonce]',
    'attempt': 'test_reconciliation_refuses_wrong_or_unproved_identity[attempt]',
    'scope': 'test_reconciliation_refuses_wrong_or_unproved_identity[scope]',
    'stale': 'test_reconciliation_refuses_wrong_or_unproved_identity[stale]',
    'permit-bypass': 'test_permits_are_single_use_and_refuse_forgery_and_changed_owners',
}


def check_cleanup_mutants():
    root = Path(__file__).resolve().parents[2]
    destination = Path(tempfile.mkdtemp(prefix='cleanup-mutants-2270-'))
    for mutant, node in MUTANTS.items():
        result = subprocess.run([sys.executable, '-m', 'pytest', '-q', '--tb=short',
            'tests/test_cleanup_obligation_2270.py::' + node], cwd=root,
            env={**os.environ, 'PB2270_MUTANT': mutant},
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        (destination / (mutant + '.log')).write_text(result.stdout)
        if (result.returncode != 1 or '1 failed' not in result.stdout
                or ('KeyError' if mutant in ('no-debt', 'missing-evidence') else 'DID NOT RAISE') not in result.stdout):
            print(f'mutant {mutant}: NOT KILLED, exit={result.returncode}', flush=True)
            return 1
        print(f'mutant {mutant}: killed, exit=1', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(check_cleanup_mutants())
