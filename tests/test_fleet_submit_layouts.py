"""Physical smoke producers retain the SLURM job entry beside their own script."""
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

import pytest

import publish_runtime
from test_slurm_lane import fleet  # noqa: F401


@pytest.mark.parametrize("layout", ["tools", "tools/fleet"])
def test_published_smoke_caller_keeps_its_exact_job_entry(tmp_path, fleet, layout):
    generation = tmp_path / "generation"
    manifest = publish_runtime._publication_manifest()
    for member in manifest:
        target = generation / member
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(publish_runtime._source_for(member), target)
    assert manifest["tools/slurm_job.py"] == manifest["tools/fleet/slurm_job.py"]
    script = generation / layout / "seal_and_publish.py"
    store = tmp_path / "private-fleet"
    program = '''
import pathlib, runpy, sys
script, store = map(pathlib.Path, sys.argv[1:])
namespace = runpy.run_path(str(script), run_name="physical_smoke_entry")
from prismabuild import fleet_submit
root = namespace["RUNTIME_ROOT"]
assert pathlib.Path(fleet_submit.__file__).resolve() == root / "src/prismabuild/fleet_submit.py"
assert fleet_submit.JOB_ENTRY == root / "tools/fleet/slurm_job.py"
fleet_submit.SH = store
main = namespace["main"]
main.__globals__["SH"] = store
raise SystemExit(main(["--transport", "slurm"]))
'''
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", program, str(script), str(store)],
        cwd=tmp_path, env=dict(os.environ), capture_output=True, text=True, timeout=120)
    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    assert report["transport"] == "slurm"
    record = json.loads(Path(report["where"]).read_text())
    submitted_script = Path(record["script"]).read_bytes()
    assert hashlib.sha256(submitted_script).hexdigest() == record["script_sha256"]
    commands = [shlex.split(line) for line in submitted_script.decode().splitlines()
                if line.startswith("exec ")]
    expected = script.with_name("slurm_job.py")
    assert len(commands) == 1
    assert str(expected) in commands[0]
    assert expected.read_bytes() == (generation / "tools/fleet/slurm_job.py").read_bytes()
    rows = (fleet / "submissions.jsonl").read_text().splitlines()
    assert len(rows) == 1
    assert json.loads(rows[0])["argv"][-1] == record["script"]
