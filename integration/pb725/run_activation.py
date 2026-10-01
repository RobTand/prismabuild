"""Admitted-worker-only opt-in PB #725 launcher; no install or resolver.

Run with the reviewed interpreter's -I flag. Source inputs are Git refs sealed
by pbrun --snapshot-ref, never coordinator paths or copied activation payloads.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import runpy
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])


def extract_ref(selection, destination):
    ref = "refs/heads/" + selection["ref"]
    observed = git("rev-parse", "--verify", ref + "^{commit}").decode().strip()
    if observed != selection["commit"]:
        raise RuntimeError(f"source closure refused: {ref} at {observed}")
    raw = git("archive", "--format=tar", observed, "--", *selection["archive_paths"])
    destination.mkdir(mode=0o700)
    inventory = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        for member in archive:
            name = PurePosixPath(member.name)
            selected = any(name == PurePosixPath(prefix) or
                           PurePosixPath(prefix) in name.parents
                           for prefix in selection["archive_paths"])
            # Git includes container directories such as tools/ when only
            # tools/fleet/ is selected. Permit those directories, not files.
            ancestor_directory = member.isdir() and any(
                name in PurePosixPath(prefix).parents
                for prefix in selection["archive_paths"])
            if (name.is_absolute() or ".." in name.parts or
                    not (selected or ancestor_directory)):
                raise RuntimeError(f"unsafe source archive member: {member.name}")
            path = destination.joinpath(*name.parts)
            if member.isdir():
                path.mkdir(parents=True, exist_ok=True, mode=0o700)
            elif member.isfile():
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                source = archive.extractfile(member)
                if source is None:
                    raise RuntimeError(f"missing regular archive bytes: {member.name}")
                with source, path.open("xb") as target:
                    data = source.read()
                    target.write(data)
                inventory[member.name] = hashlib.sha256(data).hexdigest()
            else:
                raise RuntimeError(f"nonregular source archive member: {member.name}")
    return {"ref": ref, "commit": observed,
            "tree": git("rev-parse", observed + "^{tree}").decode().strip(),
            "archive_sha256": hashlib.sha256(raw).hexdigest(),
            "root": str(destination), "files_sha256": inventory}


def assert_origins(package_root, pq_root, tools_root):
    origins = {}
    for name, module in list(sys.modules.items()):
        if name == "prismabuild" or name.startswith("prismabuild."):
            expected = package_root
        elif name == "prismaquant" or name.startswith("prismaquant."):
            expected = pq_root / "prismaquant"
        else:
            continue
        location = Path(getattr(module, "__file__", "") or "").resolve()
        if not location.is_relative_to(expected):
            raise RuntimeError(f"divergent behavioral import: {name}: {location}")
        origins[name] = str(location)
    # Pinned fleet scripts import sibling tools by bare name. Attribute all
    # such modules, including transitive mover dependencies, separately.
    for name, module in list(sys.modules.items()):
        filename = getattr(module, "__file__", None)
        if ((tools_root / "tools/fleet" / (name.split(".")[0] + ".py")).is_file()
                or (filename and Path(filename).resolve().is_relative_to(tools_root))):
            location = Path(filename or "").resolve()
            if not location.is_relative_to(tools_root):
                raise RuntimeError(f"divergent pinned tool import: {name}: {location}")
            origins[name] = str(location)
    return origins


def main():
    if not sys.flags.isolated:
        raise RuntimeError("launcher requires interpreter -I; ambient PYTHONPATH is not a closure")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-path", choices=(
        "integration/pb725/test_activation_reader.py",
        "integration/pb725/test_source_render_reader.py"),
        default="integration/pb725/test_activation_reader.py")
    selected = parser.parse_args().test_path
    test_path = ROOT / selected
    binding_path = HERE / ("activation_binding.json" if test_path.name ==
                           "test_activation_reader.py" else "source_render_binding.json")
    binding = json.loads(binding_path.read_text())
    if Path(sys.executable).absolute() != Path(binding["python"]).absolute():
        raise RuntimeError("use the explicitly selected PQ interpreter")
    if not os.environ.get("PRISMABUILD_ACTION_KEY"):
        raise RuntimeError("opt-in launcher must execute inside an admitted PB action")
    if any(name == "prismabuild" or name.startswith("prismabuild.") for name in sys.modules):
        raise RuntimeError("behavioral PB was imported before install verification")
    verifier = ROOT / "tools/fleet/pbtest_pins.py"
    if digest(verifier) != binding["verifier_sha256"]:
        raise RuntimeError("existing verifier differs from reviewed harness source")
    verify_install = runpy.run_path(str(verifier))["verify_install"]
    # This is the existing verifier, not a new provenance implementation.
    installed = verify_install("prismabuild", binding["pb_installed_commit"])
    print("PB725_INSTALL " + json.dumps(installed, sort_keys=True), flush=True)
    package_root = Path(installed["origin"]).resolve().parent
    parent = Path(os.environ["TMPDIR"]).resolve(strict=True)
    private = Path(tempfile.mkdtemp(prefix="pb725-activation-", dir=parent))
    # Retained private artifacts, no recursive deletion or mutable source use.
    pq_root, tools_root = private / "pq", private / "pb-tools"
    evidence = {
        "binding": binding,
        "harness_snapshot_commit": git("rev-parse", "HEAD").decode().strip(),
        "integration_files_sha256": {p.name: digest(p) for p in (
            binding_path, test_path, HERE / "pb725_scaffold.py", Path(__file__), verifier)},
        "installed": installed,
        "pq_source": extract_ref(binding["pq"], pq_root),
        "pb_tool_source": extract_ref(binding["pb_tools"], tools_root),
        "private_root": str(private),
    }
    # Tools prepend generation_root/src themselves. It MUST stay absent:
    # every behavioral PB import resolves to the verified installed package.
    assert not (tools_root / "src").exists()
    sys.path.insert(0, str(HERE))  # Only the explicit integration scaffold, never PB src/tests.
    sys.path.insert(0, str(pq_root))
    sys.path.insert(0, str(tools_root / "tools/fleet"))
    os.environ["PB725_PRIVATE_ROOT"] = str(private)
    os.environ["PB725_BINDING_PATH"] = str(binding_path)
    os.environ["PB725_PACKAGE_ROOT"] = str(package_root)
    os.environ["PB725_PQ_ROOT"] = str(pq_root)
    os.environ["PB725_TOOLS_ROOT"] = str(tools_root)
    os.environ["TMPDIR"] = str(private)
    tempfile.tempdir = str(private)
    os.environ["TRITON_CACHE_DIR"] = str(private / "triton")
    os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    os.environ.pop("PYTEST_ADDOPTS", None)
    sys.dont_write_bytecode = True
    path = private / "source-evidence.json"
    path.write_text(json.dumps(evidence, sort_keys=True, indent=2))
    print("PB725_SOURCE " + json.dumps(evidence, sort_keys=True), flush=True)
    import pytest
    try:
        # No root conftest/pyproject from the SDK3 harness may add its src.
        status = pytest.main(["-c", "/dev/null", "--noconftest", "--import-mode=importlib",
                              "-p", "no:cacheprovider", "-v", "-s", "--tb=short",
                              "-o", "faulthandler_timeout=30",
                              "--basetemp", str(private / "pytest"),
                              str(test_path)])
    finally:
        evidence["final_import_origins"] = assert_origins(package_root, pq_root, tools_root)
        path.write_text(json.dumps(evidence, sort_keys=True, indent=2))
        print("PB725_IMPORTS " + json.dumps(evidence["final_import_origins"], sort_keys=True),
              flush=True)
    return int(status)


if __name__ == "__main__":
    raise SystemExit(main())
