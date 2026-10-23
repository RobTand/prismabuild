#!/usr/bin/env python3
"""Check or build the reviewed pool CPU test environment, without editing live venvs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile


CPU_DEFINITIONS = Path(__file__).resolve().with_name("pb_cpu")
CPU_INVENTORY = """
import importlib.metadata as m, json, platform, sys
print(json.dumps({
    'implementation': platform.python_implementation(),
    'machine': platform.machine(),
    'python': list(sys.version_info[:2]),
    'distributions': [{'name': d.metadata['Name'], 'version': d.version}
                      for d in m.distributions()]}))
"""


def cpu_package_name(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def cpu_observe(python, run=subprocess.run):
    completed = run([str(python), "-I", "-c", CPU_INVENTORY],
                    check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def cpu_read_definition(observed, directory=CPU_DEFINITIONS):
    machine = observed["machine"]
    minor = observed["python"]
    if observed["implementation"] != "CPython" or (machine, minor) not in (
            ("x86_64", [3, 14]), ("aarch64", [3, 12])):
        raise ValueError(f"unsupported interpreter: {observed['implementation']} {machine} {minor}")
    path = directory / f"{machine}-cp{minor[0]}{minor[1]}.json"
    definition = json.loads(path.read_text())
    if (definition["machine"], definition["python"]) != (machine, minor):
        raise ValueError(f"definition does not describe the interpreter: {path}")
    names = set()
    for package in definition["packages"]:
        name = package["name"]
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name):
            raise ValueError(f"invalid package name: {name}")
        if not re.fullmatch(r"[A-Za-z0-9.!+_-]+", package["version"]):
            raise ValueError(f"invalid package version: {name}")
        normalized = cpu_package_name(name)
        if normalized in names or not package["reason"].strip():
            raise ValueError(f"duplicate or unexplained package: {name}")
        names.add(normalized)
        if not package["sha256"] or any(
                not re.fullmatch(r"[0-9a-f]{64}", digest) for digest in package["sha256"]):
            raise ValueError(f"missing or invalid wheel hashes: {name}")
    return path, definition


def cpu_compare(definition, observed):
    expected = {cpu_package_name(p["name"]): p["version"] for p in definition["packages"]}
    installed = {}
    duplicates = []
    for package in observed["distributions"]:
        name = cpu_package_name(package["name"])
        if name in installed:
            duplicates.append(name)
        installed[name] = package["version"]
    missing = {name: expected[name] for name in sorted(expected.keys() - installed.keys())}
    extra = {name: installed[name] for name in sorted(installed.keys() - expected.keys())}
    different = {name: {"expected": expected[name], "installed": installed[name]}
                 for name in sorted(expected.keys() & installed.keys())
                 if expected[name] != installed[name]}
    return {"ok": not (missing or extra or different or duplicates),
            "missing": missing, "extra": extra, "different": different,
            "duplicates": sorted(duplicates)}


def cpu_requirements(definition):
    return "\n".join(
        f"{p['name']}=={p['version']} " + " ".join(f"--hash=sha256:{h}" for h in p["sha256"])
        for p in definition["packages"]) + "\n"


def cpu_build_destination(destination):
    destination = destination.resolve()
    live = Path("/home/rob/venvs/pb-cpu").resolve()
    if destination == live or live in destination.parents:
        raise ValueError("build must never write into the live pb-cpu environment")
    if destination.exists():
        raise ValueError(f"build destination already exists: {destination}")
    return destination


def cpu_build(python, destination, definition, run=subprocess.run):
    destination = cpu_build_destination(destination)
    destination.mkdir(parents=True)
    run([str(python), "-I", "-m", "venv", str(destination)], check=True,
        stdout=sys.stderr)
    target = destination / "bin" / "python"
    with tempfile.TemporaryDirectory(prefix="pb-cpu-requirements-") as scratch:
        requirements = Path(scratch) / "requirements.txt"
        requirements.write_text(cpu_requirements(definition))
        run([str(target), "-I", "-m", "pip", "--isolated", "install",
             "--no-cache-dir", "--only-binary=:all:", "--require-hashes", "--no-deps",
             "--index-url", "https://pypi.org/simple",
             "--extra-index-url", "https://download.pytorch.org/whl/cpu",
             "-r", str(requirements)], check=True, stdout=sys.stderr)
    return cpu_compare(definition, cpu_observe(target, run))


def cpu_main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", required=True, type=Path,
                        help="interpreter to inspect, or base interpreter for a fresh build")
    commands = parser.add_subparsers(dest="operation", required=True)
    commands.add_parser("check", help="report exact installed-distribution drift as JSON")
    build = commands.add_parser("build", help="create and verify a fresh environment")
    build.add_argument("destination", type=Path, help="new venv directory, which must not exist")
    args = parser.parse_args(argv)
    try:
        observed = cpu_observe(args.python)
        path, definition = cpu_read_definition(observed)
        report = (cpu_compare(definition, observed) if args.operation == "check" else
                  cpu_build(args.python, args.destination, definition))
        report.update(operation=args.operation, definition=path.name,
                      machine=observed["machine"], python=observed["python"])
        print(json.dumps(report))
        return 0 if report["ok"] else 1
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(cpu_main())
