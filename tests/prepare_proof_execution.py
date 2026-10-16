"""Inspect source populations and copy pinned inputs; never import or run tests."""
import argparse
import ast
import base64
import csv
import hashlib
from io import BytesIO
import json
from pathlib import Path
import tarfile


def sha(data):
    return hashlib.sha256(data).hexdigest()


def syntax_data(node):
    if isinstance(node, ast.AST):
        return [type(node).__name__, [[name, syntax_data(value)] for name, value in ast.iter_fields(node)]]
    return [syntax_data(value) for value in node] if isinstance(node, list) else node


def inspect_population(root):
    profile = json.loads((root / "tests/proof_execution.json").read_bytes())
    if profile["schema"] != "prismabuild.proof_test_execution.v1":
        raise ValueError("unsupported proof execution population")
    files = sorted(str(p.relative_to(root)) for p in (root / "tests").rglob("test_*.py"))
    roots = set(profile["root"]["files"])
    native = set(files) - roots
    assert roots <= set(files) and set(profile["native_family"]["files"]) <= native
    assert not roots & native and roots | native == set(files)
    modules = {}

    def module(name):
        if name not in modules:
            path = root / "tests" / f"{name}.py"
            modules[name] = ast.parse(path.read_bytes(), feature_version=(3, 12)) if path.exists() else None
        return modules[name]

    def resolve(requesting, name):
        tree = module(requesting)
        if tree is not None:
            for node in tree.body:
                if isinstance(node, ast.FunctionDef) and node.name == name:
                    return node
                if isinstance(node, ast.ImportFrom) and node.module:
                    for alias in node.names:
                        if (alias.asname or alias.name) == name:
                            return resolve(node.module, alias.name)
        return None

    def call_name(node):
        return ast.unparse(node.func if isinstance(node, ast.Call) else node)

    def case_count(requesting, function):
        seen, pending, count = set(), [a.arg for a in function.args.args], 1
        while pending:
            name = pending.pop()
            if name in seen:
                continue
            seen.add(name)
            fixture = resolve(requesting, name)
            if fixture is None or not any(call_name(d) == "pytest.fixture" for d in fixture.decorator_list):
                continue
            for decorator in fixture.decorator_list:
                if isinstance(decorator, ast.Call) and call_name(decorator) == "pytest.fixture":
                    for keyword in decorator.keywords:
                        if keyword.arg == "params":
                            assert isinstance(keyword.value, (ast.List, ast.Tuple))
                            count *= len(keyword.value.elts)
            pending.extend(a.arg for a in fixture.args.args)
        for decorator in function.decorator_list:
            if isinstance(decorator, ast.Call) and call_name(decorator) == "pytest.mark.parametrize":
                assert isinstance(decorator.args[1], (ast.List, ast.Tuple))
                count *= len(decorator.args[1].elts)
        return count

    actual_root_importers = {"tests/test_client_proof_prior_trust.py"}
    for filename in files:
        tree = ast.parse((root / filename).read_bytes(), feature_version=(3, 12))
        for node in tree.body:
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.module == "test_client_proof_prior_trust" and any(a.name == "tmp_path" for a in node.names):
                actual_root_importers.add(filename)
            if node.module == "test_client_proof_prior_trust" and any(a.name == "root_publication" for a in node.names):
                assert filename in roots, f"unclassified actual-root publication requester: {filename}"
            if node.module == "test_park_proof_real_writer" and any(a.name == "parked_role" for a in node.names):
                assert filename in roots, f"unclassified actual-root writer requester: {filename}"
    assert actual_root_importers == roots, (actual_root_importers, roots)
    counts = {}
    for filename, expected in {**profile["root"]["files"], **profile["native_family"]["files"]}.items():
        requesting = Path(filename).stem
        functions = [n for n in module(requesting).body
                     if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")]
        counts[filename] = sum(case_count(requesting, n) for n in functions)
        assert counts[filename] == expected, (filename, counts[filename], expected)
        if filename in roots:
            provider = resolve(requesting, "tmp_path")
            assert provider is not None and "TemporaryDirectory" in ast.unparse(provider), filename
    for name, move in profile["process_test_moves"].items():
        origin = module(Path(move["from"]).stem)
        assert not any(isinstance(n, ast.FunctionDef) and n.name == name for n in origin.body)
        moved = resolve(Path(move["to"]).stem, name)
        assert moved is not None and sha(json.dumps(syntax_data(moved)).encode()) == move["ast_sha256"]
        assert case_count(Path(move["to"]).stem, moved) == move["cases"]
    facts = {"source_inspection_only": True, "root_cases": sum(profile["root"]["files"].values()),
             "native_family_cases": sum(profile["native_family"]["files"].values()),
             "root_files": sorted(roots), "native_family_files": sorted(profile["native_family"]["files"]),
             "native_suite_files": sorted(native), "case_counts": counts}
    return profile, facts


def stage_bytes(data, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.read_bytes() != data:
        raise ValueError(f"staged input differs: {target}")
    target.write_bytes(data)


def prepare_inputs(root, profile, artifact_source, site_packages):
    if artifact_source is not None:
        sources = {entry["name"]: (artifact_source / entry["name"]).read_bytes()
                   for entry in profile["artifacts"]}
    else:
        corpus = profile["artifact_corpus"]
        data = (root / corpus["path"]).read_bytes()
        if len(data) != corpus["bytes"] or sha(data) != corpus["sha256"]:
            raise ValueError("bundled historical source archive differs")
        declared = {entry["name"]: entry for entry in profile["artifacts"]}
        with tarfile.open(fileobj=BytesIO(data), mode="r:gz") as archive:
            members = archive.getmembers()
            if (len(members) != len(declared) or {member.name for member in members} != set(declared)
                    or any(not member.isfile() or member.size != declared[member.name]["bytes"]
                           for member in members)):
                raise ValueError("bundled historical source members differ")
            sources = {}
            for name, entry in declared.items():
                with archive.extractfile(name) as stream:
                    sources[name] = stream.read(entry["bytes"] + 1)
    for entry in profile["artifacts"]:
        data = sources[entry["name"]]
        if len(data) != entry["bytes"] or sha(data) != entry["sha256"]:
            raise ValueError(f"historical source bytes differ: {entry['name']}")
    for entry in profile["artifacts"]:
        stage_bytes(sources[entry["name"]], root / "qualification_artifacts" / entry["name"])
    tooling = profile["root"]["tooling"]
    expected_files = set()
    target = root / "qualification_deps"
    external = {"../../../bin/py.test", "../../../bin/pytest", "../../../bin/pygmentize"}
    for package in tooling["packages"]:
        with (site_packages / package / "RECORD").open(newline="") as stream:
            for relative, expected_hash, expected_size in csv.reader(stream):
                path = Path(relative)
                if ".." in path.parts:
                    if relative not in external:
                        raise ValueError(f"unexpected external tooling file: {relative}")
                    continue
                if path.suffix == ".pyc":
                    continue
                source = site_packages / path
                data = source.read_bytes()
                if expected_hash:
                    algorithm, expected = expected_hash.split("=", 1)
                    actual = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
                    assert algorithm == "sha256" and actual == expected, relative
                if expected_size:
                    assert len(data) == int(expected_size), relative
                stage_bytes(data, target / path)
                expected_files.add(relative)
    files = {str(p.relative_to(target)): sha(p.read_bytes())
             for p in sorted(target.rglob("*")) if p.is_file()}
    assert set(files) == expected_files and len(files) == tooling["files"]
    manifest = {"source": str(site_packages), "packages": tooling["packages"], "files": files}
    (root / "qualification_dependencies.json").write_text(json.dumps(manifest, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", required=True, type=Path)
    parser.add_argument("--paths", choices=("root", "native-family", "native-suite"))
    parser.add_argument("--prepare", action="store_true", help="stage bundled source artifacts and pinned test tooling")
    parser.add_argument("--prepare-from", help="use only this explicit directory of pinned historical byte artifacts")
    parser.add_argument("--site-packages", type=Path)
    args = parser.parse_args()
    root = args.checkout.resolve()
    profile, facts = inspect_population(root)
    if args.prepare_from is not None and not args.prepare_from:
        parser.error("supplied historical source directory is empty")
    prepare = args.prepare or args.prepare_from is not None
    if args.paths and prepare:
        parser.error("file-list output and input preparation are separate source operations")
    if prepare:
        site = args.site_packages or Path(profile["root"]["tooling"]["site_packages"])
        source = Path(args.prepare_from).resolve() if args.prepare_from is not None else None
        prepare_inputs(root, profile, source, site)
    if args.paths:
        key = {"root": "root_files", "native-family": "native_family_files", "native-suite": "native_suite_files"}[args.paths]
        print("\n".join(facts[key]))
    else:
        print(json.dumps(facts, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
