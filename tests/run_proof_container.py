"""Run declared root proof files inside an already admitted action's affinity."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile



def load_population(root):
    profile = json.loads((root / "tests/proof_execution.json").read_bytes())
    if profile["schema"] != "prismabuild.proof_test_execution.v1":
        raise ValueError("unsupported proof execution population")
    required = profile["root"]
    receipt_path = root / 'tests/receipt_execution.json'
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_bytes())
        if receipt['schema'] != 'prismabuild.receipt_test_execution.v1' or receipt['image'] != required['image']:
            raise ValueError('unsupported receipt execution population or image')
        required['files'].update(receipt['root_files'])
    return profile, required


def observe_action_runtime():
    running = Path('/proc/self/exe').read_bytes()
    print('c15-action-runtime: ' + json.dumps({
        'complete': True, 'version': list(sys.version_info[:3]),
        'executable': {'path': sys.executable, 'resolved_path': str(Path(sys.executable).resolve()),
                       'bytes': len(running), 'sha256': hashlib.sha256(running).hexdigest()},
        'executable_path_matches_running': Path(sys.executable).read_bytes() == running,
        'affinity': sorted(os.sched_getaffinity(0)),
        'native_threads': {name: os.environ.get(name) for name in (
            'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS')},
    }), flush=True)


def verify_inputs(root, profile):
    manifest = json.loads((root / "qualification_dependencies.json").read_bytes())
    for name, digest in manifest["files"].items():
        actual = hashlib.sha256((root / "qualification_deps" / name).read_bytes()).hexdigest()
        if actual != digest:
            raise RuntimeError(f"Pinned test dependency differs: {name}")
    for artifact in profile["artifacts"]:
        actual = hashlib.sha256((root / "qualification_artifacts" / artifact["name"]).read_bytes()).hexdigest()
        if actual != artifact["sha256"]:
            raise RuntimeError(f"Pinned historical source differs: {artifact['name']}")
    return manifest


def container_command(root, required, tests, affinity):
    image = required["image"]
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
        "TMPDIR": "/home/rob/pbt17",
    }.items():
        command.extend(["--env", f"{key}={value}"])
    bootstrap = (
        "import os, runpy, sys; "
        "expected={int(x) for x in os.environ['PRISMABUILD_PROOF_EXPECTED_AFFINITY'].split(',')}; "
        "assert os.geteuid() == 0 and os.getegid() == 0, 'real container UID/GID 0 required'; "
        "assert os.sched_getaffinity(0) == expected, 'container changed PB CPU affinity'; "
        "os.makedirs('/home/rob/pbt17', exist_ok=True); "
        "owner=runpy.run_path('/source/tools/fleet/pbtest_outcomes.py'); "
        "raise SystemExit(owner['main'](sys.argv[1:], collection_spec=" + repr(
            {'files': tests, 'roots': [[path, 'file'] for path in tests]}) + ", "
        "collection_source=open('/source/src/prismabuild/pbtest_collection.py').read()))"
    )
    command.extend([image, "-B", "-c", bootstrap, "-q", "-rA",
                    "-o", "addopts=", "-p", "no:cacheprovider",
                    "--basetemp=/var/lib/pb-proof-qualification", *tests])
    return command


def run_profile(root, required, tests, affinity, path, collect_only):
    from proof_container_runtime import (
        ATTESTATION_SCHEMA, canonical, digest, load_profile, observe_runtime,
        require_identity, require_profile_kernel, source_digests, verify_closure,
    )

    runtime = load_profile(path, root)
    outer = observe_runtime()
    require_identity(outer, runtime.primary)
    require_profile_kernel(runtime, outer)
    if outer['affinity'] != affinity or outer['native_threads'] != {
            name: '1' for name in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS')}:
        raise ValueError('outer runtime changed PB affinity or native thread bounds')
    outer_closure = verify_closure(runtime, ('host',))
    inspected = json.loads(subprocess.check_output([
        'docker', 'image', 'inspect', runtime.image, '--format',
        '{"id":{{json .Id}},"architecture":{{json .Architecture}}}'], text=True))
    expected_architecture = {62: 'amd64', 183: 'arm64'}[runtime.primary['elf_machine']]
    if inspected != {'id': runtime.image, 'architecture': expected_architecture}:
        raise ValueError('actual local image differs from the runtime profile')
    image = dict(expected=runtime.image, actual=inspected['id'], architecture=inspected['architecture'])
    output = Path(tempfile.mkdtemp(prefix='pb-root-runtime-', dir=os.environ['TMPDIR']))
    attestation = output / 'attestation.json'
    inner_attestation = output / 'inner-attestation.json'
    mode = 'collection' if collect_only else 'execution'
    arguments = (str(Path('/source') / runtime.path.relative_to(root)), tests, mode,
                 outer, outer_closure, image, affinity,
                 '/qualification-output/inner-attestation.json', str(inner_attestation))
    bootstrap = (
        "import runpy; "
        "owner=runpy.run_path('/source/tests/proof_container_runtime.py'); "
        "raise SystemExit(owner['run'](*" + repr(arguments) + '))'
    )
    command = container_command(root, required, tests, affinity)
    image_index = command.index(required['image'])
    command = command[:image_index]
    command[command.index('--entrypoint') + 1] = runtime.primary['executable']['path']
    for captured in runtime.closure['roots']:
        if captured['origin'] == 'host':
            target = captured['path']
            command.extend(['--mount', f'type=bind,src={target},dst={target},readonly'])
    command.extend(['--mount', f'type=bind,src={output},dst=/qualification-output',
                    runtime.image, '-B', '-S', '-c', bootstrap])
    result = subprocess.run(command, check=False)
    inner_descriptor = None
    try:
        inner_raw = inner_attestation.read_bytes()
        inner_descriptor = dict(path=str(inner_attestation), bytes=len(inner_raw),
                                sha256=digest(inner_raw), raw_json=inner_raw.decode())
        record = json.loads(inner_raw)
        if record['profile_sha256'] != runtime.sha256 or record['mode'] != mode:
            raise ValueError('inner attestation differs from the selected profile or mode')
        if record['exitstatus'] != result.returncode:
            raise ValueError('inner status differs from actual Docker termination')
        record['closure']['outer'] = verify_closure(runtime, ('host',))
    except (OSError, ValueError, KeyError) as exc:
        record = dict(schema=ATTESTATION_SCHEMA, mode=mode, complete=False,
            profile_sha256=runtime.sha256, image=image, outer=outer, inner=None, installed_reader=None,
            closure=dict(sha256=runtime.closure['sha256'], outer=outer_closure, inner=None,
                         mapped_native_providers={}), source_sha256=source_digests(root, tests, runtime),
            exitstatus=result.returncode or 1, error=f'{type(exc).__name__}: {exc}',
            attestation_path=str(attestation))
    record['inner_attestation'] = inner_descriptor
    record['attestation_path'] = str(attestation)
    with attestation.open('xb') as handle:
        handle.write(canonical(record) + b'\n')
    print('prismabuild-root-runtime: ' + canonical(record).decode(), flush=True)
    return record['exitstatus'] if record['complete'] else record['exitstatus'] or 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-profile', type=Path)
    parser.add_argument('--collect-only', action='store_true')
    parser.add_argument("tests", nargs="*")
    args = parser.parse_args(argv)
    root = Path.cwd().resolve()
    profile, required = load_population(root)
    observe_action_runtime()
    tests = args.tests or list(required["files"])
    if len(tests) != len(set(tests)) or any(path not in required["files"] for path in tests):
        raise ValueError("root container accepts only the declared root proof files, once each")
    manifest = verify_inputs(root, profile)
    if args.runtime_profile is not None:
        return run_profile(root, required, tests, sorted(os.sched_getaffinity(0)),
                           args.runtime_profile, args.collect_only)
    if args.collect_only:
        raise ValueError('collection mode requires an explicit runtime profile')
    image = required["image"]
    actual_image = subprocess.check_output([
        "docker", "image", "inspect", image, "--format", "{{.Id}}"], text=True).strip()
    if actual_image != image:
        raise RuntimeError("The required local Python image differs")
    affinity = sorted(os.sched_getaffinity(0))
    print(json.dumps({"image": image, "affinity": affinity,
                      "dependency_files_verified": len(manifest["files"]),
                      "source": str(root), "tests": tests}), flush=True)
    command = container_command(root, required, tests, affinity)
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
