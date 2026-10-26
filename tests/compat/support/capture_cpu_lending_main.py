"""Capture the adopted main outcomes from verified committed source inputs.

The adopted source bytes and tree identity are committed history inputs.
The permanent tests consume the original PB-captured SHA-pinned JSON.
"""
import ast
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

MAIN = "43234d0fbd42d6ed76da38658cb9f447ed19a606"
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "tests"))
from history_inputs import history_manifest, materialize_sources

REVIEW = ROOT / "tests/compat/support/cpu_lending_review_cap0_fixture.py"
BUILDER = ROOT / "tests/compat/support/cpu_lending_cap0_fixture.py"


def capture_reference():
    with tempfile.TemporaryDirectory() as temporary:
        scratch = Path(temporary)
        materialize_sources(MAIN, scratch / "main")
        adopted = history_manifest()["revisions"][MAIN]
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
        return {
            "main_commit": adopted["commit"],
            "main_src_tree": adopted["trees"]["src"],
            "fixture_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                               for path in (REVIEW, BUILDER)},
            "outcomes": outcomes}


if __name__ == "__main__":
    print("CPU_MAIN_REFERENCE " + json.dumps(capture_reference(), sort_keys=True))
