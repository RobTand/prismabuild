"""Run declared root proof files inside an already admitted action's affinity."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("tests", nargs="*")
args = parser.parse_args()
root = Path.cwd().resolve()
profile = json.loads((root / "tests/proof_execution.json").read_bytes())
if profile["schema"] != "prismabuild.proof_test_execution.v1":
    raise ValueError("unsupported proof execution population")
required = profile["root"]
tests = args.tests or list(required["files"])
if len(tests) != len(set(tests)) or any(path not in required["files"] for path in tests):
    raise ValueError("root container accepts only the declared root proof files, once each")
manifest = json.loads((root / "qualification_dependencies.json").read_bytes())
for name, digest in manifest["files"].items():
    actual = hashlib.sha256((root / "qualification_deps" / name).read_bytes()).hexdigest()
    if actual != digest:
        raise RuntimeError(f"Pinned test dependency differs: {name}")
for artifact in profile["artifacts"]:
    actual = hashlib.sha256((root / "qualification_artifacts" / artifact["name"]).read_bytes()).hexdigest()
    if actual != artifact["sha256"]:
        raise RuntimeError(f"Pinned historical source differs: {artifact['name']}")
image = required["image"]
actual_image = subprocess.check_output([
    "docker", "image", "inspect", image, "--format", "{{.Id}}"], text=True).strip()
if actual_image != image:
    raise RuntimeError("The required local Python image differs")
affinity = sorted(os.sched_getaffinity(0))
print(json.dumps({"image": image, "affinity": affinity,
                  "dependency_files_verified": len(manifest["files"]),
                  "source": str(root), "tests": tests}), flush=True)
command = ["docker", "run", "--rm", "--network", "none", "--user", "0:0",
           "--entrypoint", required["entrypoint"],
           "--cpuset-cpus", ",".join(map(str, affinity)),
           "--memory", f"{required['container_memory_gib']}g",
           "--pids-limit", "256", "--mount", f"type=bind,src={root},dst=/source,readonly",
           "--workdir", "/source"]
for key, value in {
    "PYTHONPATH": "/source/qualification_deps:/source/src:/source/tools/fleet",
    "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1", "PYTHONDONTWRITEBYTECODE": "1",
    "OMP_NUM_THREADS": str(required["native_threads"]),
    "MKL_NUM_THREADS": str(required["native_threads"]),
    "OPENBLAS_NUM_THREADS": str(required["native_threads"]),
    "PRISMABUILD_PROOF_EXPECTED_AFFINITY": ",".join(map(str, affinity)),
    "PRISMABUILD_PROOF_ARTIFACT_ROOT": "/source/qualification_artifacts",
}.items():
    command.extend(["--env", f"{key}={value}"])
bootstrap = (
    "import os, runpy; "
    "expected={int(x) for x in os.environ['PRISMABUILD_PROOF_EXPECTED_AFFINITY'].split(',')}; "
    "assert os.geteuid() == 0 and os.getegid() == 0, 'real container UID/GID 0 required'; "
    "assert os.sched_getaffinity(0) == expected, 'container changed PB CPU affinity'; "
    "runpy.run_module('pytest', run_name='__main__', alter_sys=True)"
)
command.extend([image, "-B", "-c", bootstrap, "-q", "-rA",
                "-o", "addopts=", "-p", "no:cacheprovider",
                "--basetemp=/var/lib/pb-proof-qualification", *tests])
raise SystemExit(subprocess.run(command, check=False).returncode)
