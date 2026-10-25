"""Stage the pinned root inputs in this admitted checkout, then run the root adapter.

``tools/verify.py integration`` submits this file as its root lane's single
action. The worker materialized a private checkout for the action, so staging
here never changes the submitter's sealed source. The steps are those in
docs/proof_test_execution.md: prepare, then ``run_proof_container.py`` over
every declared root file.
"""
from pathlib import Path
import subprocess
import sys

root = Path.cwd().resolve()
prepared = subprocess.run([sys.executable, str(root / 'tests/prepare_proof_execution.py'),
                           '--checkout', str(root), '--prepare'], check=False)
if prepared.returncode:
    raise SystemExit(f'root lane: input preparation failed with {prepared.returncode}')
raise SystemExit(subprocess.run([sys.executable, str(root / 'tests/run_proof_container.py')],
                                check=False).returncode)
