"""Capture the adopted main outcomes once inside PB, never during pytest.

The caller explicitly seals pb2156-reference-main in the PB source bundle.
The permanent tests consume the resulting SHA-pinned JSON instead of Git.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

MAIN = "43234d0fbd42d6ed76da38658cb9f447ed19a606"
ROOT = Path(__file__).resolve().parents[3]
REVIEW = ROOT / "tests/compat/support/cpu_lending_review_cap0_fixture.py"
BUILDER = ROOT / "tests/compat/support/cpu_lending_cap0_fixture.py"

with tempfile.TemporaryDirectory() as temporary:
    scratch = Path(temporary)
    archive = subprocess.check_output(["git", "archive", MAIN, "src"], cwd=ROOT)
    with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
        bundle.extractall(scratch / "main", filter="data")
    import ast
    scenarios = sorted(node.name for node in ast.parse(REVIEW.read_bytes()).body
                       if isinstance(node, ast.FunctionDef) and node.name[0] in "sq"
                       and node.name[1:3].isdigit())
    fixtures = [(name, REVIEW, name) for name in scenarios]
    fixtures += [("builder_" + name, BUILDER, name) for name in
                 ("ordinary", "wide", "busy", "unknown", "measurement")]
    outcomes = {}
    for name, fixture, argument in fixtures:
        target = scratch / name
        target.mkdir()
        result = subprocess.run([sys.executable, str(fixture), str(target), argument],
            cwd=target, env=dict(os.environ, PYTHONPATH=str(scratch / "main/src"),
                                PRISMABUILD_BOX_STATE_ROOT=str(target / "box-state")),
            text=True, capture_output=True, timeout=600, check=True)
        value = json.loads(result.stdout)
        outcomes[name] = value["steps"] if fixture == REVIEW else value
    assert len(outcomes) == 27
    print("CPU_MAIN_REFERENCE " + json.dumps({
        "main_commit": MAIN,
        "main_src_tree": subprocess.check_output(["git", "rev-parse", MAIN + ":src"],
                                                 cwd=ROOT, text=True).strip(),
        "fixture_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                           for path in (REVIEW, BUILDER)},
        "outcomes": outcomes}, sort_keys=True))
