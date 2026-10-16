"""Read-only source inventory and supported-generation capture. No product imports."""
from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile


WIRE = re.compile(r"^(?:(?:prismaquant\.)?prismabuild|fleet)\.[\w.-]+\.v\d+$")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def document(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def put(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != raw:
            raise ValueError(f"refusing to replace captured bytes: {path}")
        return
    with path.open("xb") as stream:
        stream.write(raw)
    path.chmod(0o444)


def inventory(root):
    """Include embedded Python strings and extensionless production launchers."""
    rows = defaultdict(lambda: {"occurrences": [], "constructions": []})
    for base in ("src", "tools"):
        for path in sorted((root / base).rglob("*")):
            if not path.is_file() or path.suffix not in (".py", ""):
                continue
            try:
                source = path.read_text()
                tree = ast.parse(source)
            except (SyntaxError, UnicodeError):
                continue
            relative = str(path.relative_to(root))
            bindings = {}
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            bindings[target.id] = node.value.value

            def scan(node, owner="<module>", embedded=False):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    owner = owner.removeprefix("<module>.") + "." + node.name
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    if WIRE.fullmatch(node.value):
                        rows[node.value]["occurrences"].append({
                            "path": relative, "line": node.lineno, "owner": owner,
                            "embedded": embedded})
                    elif "schema" in node.value and "\n" in node.value:
                        try:
                            scan(ast.parse(node.value), owner + ":embedded", True)
                        except SyntaxError:
                            pass
                if isinstance(node, ast.Dict):
                    for key, value in zip(node.keys, node.values):
                        if isinstance(key, ast.Constant) and key.value == "schema":
                            wire = (value.value if isinstance(value, ast.Constant)
                                    else bindings.get(value.id) if isinstance(value, ast.Name)
                                    else None)
                            if isinstance(wire, str) and WIRE.fullmatch(wire):
                                rows[wire]["constructions"].append({
                                    "path": relative, "line": node.lineno, "owner": owner,
                                    "fields": sorted(k.value for k in node.keys
                                                     if isinstance(k, ast.Constant)
                                                     and isinstance(k.value, str)),
                                    "expansion": any(k is None for k in node.keys),
                                    "embedded": embedded})
                for child in ast.iter_child_nodes(node):
                    scan(child, owner, embedded)

            scan(tree)
    return [{"wire_id": wire, **row} for wire, row in sorted(rows.items())]


def git_provenance(root, commit, files):
    found = subprocess.run(["git", "-C", str(root), "cat-file", "-e", commit + "^{commit}"],
                           capture_output=True).returncode == 0
    if not found:
        return {"commit_available": False, "verified_paths": [],
                "note": "Git object absent; published receipt hashes bind captured bytes"}
    verified = []
    for name, expected in files.items():
        result = subprocess.run(["git", "-C", str(root), "show", commit + ":" + name],
                                capture_output=True)
        if result.returncode == 0:
            if digest(result.stdout) != expected:
                raise ValueError(f"Git and published generation disagree: {commit}:{name}")
            verified.append(name)
    return {"commit_available": True, "verified_paths": verified}


def source_equivalence(root, receipt):
    """Verify the qualified source commit, without inventing the deployed Git object."""
    commit = receipt["shape_gate"]["snapshot_parent"]
    paths = subprocess.check_output(["git", "-C", str(root), "ls-tree", "-r", "--name-only", commit], text=True).splitlines()
    paths = set(paths)
    mapping = {}
    for published in receipt["files"]:
        candidates = [published]
        if published.startswith("tools/"):
            candidates.append("tools/fleet/" + published.removeprefix("tools/"))
        source = next((path for path in candidates if path in paths), None)
        if source is None:
            raise ValueError(f"qualified source has no published file: {published}")
        mapping[published] = source
    names = list(mapping)
    query = "".join(commit + ":" + mapping[name] + "\n" for name in names)
    raw = subprocess.check_output(["git", "-C", str(root), "cat-file", "--batch"], input=query.encode())
    offset, matches = 0, []
    for published in names:
        end = raw.index(b"\n", offset)
        header = raw[offset:end].split()
        if len(header) != 3 or header[1] != b"blob":
            raise ValueError("qualified source is not an immutable blob")
        start = end + 1
        content = raw[start:start + int(header[2])]
        offset = start + len(content) + 1
        if digest(content) != receipt["files"][published]:
            raise ValueError(f"qualified source differs from published bytes: {published}")
        matches.append({"published": published, "source": mapping[published],
                        "git_blob": header[0].decode(), "sha256": digest(content)})
    return {"declared_generation_commit": receipt["commit"], "source_commit": commit,
            "source_tree": subprocess.check_output(["git", "-C", str(root), "rev-parse", commit + "^{tree}"], text=True).strip(),
            "verified_paths": matches, "note": "byte equivalence; not an available deployed-commit Git object"}


def capture_prepared(root, commit, out, name):
    original = subprocess.check_output(["git", "-C", str(root), "archive", commit, "src", "tools", "fleet"])
    contents = {}
    with tarfile.open(fileobj=io.BytesIO(original)) as source:
        for entry in source.getmembers():
            if entry.isfile():
                contents[entry.name] = source.extractfile(entry).read()
            elif not entry.isdir():
                raise ValueError(f"non-regular prepared source: {entry.name}")
    source_receipt = {"generation": name, "commit": commit, "kind": "unpublished Git source control",
                      "tree": subprocess.check_output(["git", "-C", str(root), "rev-parse", commit + "^{tree}"], text=True).strip(),
                      "files": {path: digest(raw) for path, raw in contents.items()}}
    receipt = document(source_receipt)
    archive = io.BytesIO()
    with gzip.GzipFile(fileobj=archive, mode="wb", mtime=0) as zipped:
        with tarfile.open(fileobj=zipped, mode="w") as tar:
            for path, content in sorted({**contents, "COMPAT_SOURCE.json": receipt}.items()):
                info = tarfile.TarInfo(path)
                info.size, info.mode = len(content), 0o444
                tar.addfile(info, io.BytesIO(content))
    packed = archive.getvalue()
    record = {**source_receipt, "receipt_sha256": digest(receipt), "archive_sha256": digest(packed),
              "provenance_file": "COMPAT_SOURCE.json", "designated_rollback": False, "published": False}
    put(out / (name + ".tar.gz"), packed)
    put(out / (name + ".json"), json.dumps(record, sort_keys=True).encode() + b"\n")
    return {key: record[key] for key in ("generation", "commit", "receipt_sha256", "archive_sha256", "provenance_file")}


def capture_generation(directory, out, git_root):
    raw = (directory / "RUNTIME_VERSION.json").read_bytes()
    receipt = json.loads(raw)
    if receipt["generation"] != directory.name or receipt.get("dirty") is not False:
        raise ValueError(f"unsealed or misnamed generation: {directory}")
    files = {}
    archive = io.BytesIO()
    with gzip.GzipFile(fileobj=archive, mode="wb", mtime=0) as zipped:
        with tarfile.open(fileobj=zipped, mode="w") as tar:
            for name, expected in sorted(receipt["files"].items()):
                if not name.startswith(("src/", "tools/", "fleet/")):
                    continue
                path = Path(name)
                if path.is_absolute() or ".." in path.parts or (directory / path).is_symlink():
                    raise ValueError(f"unsafe generation member: {name}")
                content = (directory / path).read_bytes()
                if digest(content) != expected:
                    raise ValueError(f"published hash mismatch: {directory.name}/{name}")
                files[name] = expected
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(content), 0o444
                tar.addfile(info, io.BytesIO(content))
            info = tarfile.TarInfo("RUNTIME_VERSION.json")
            info.size, info.mode = len(raw), 0o444
            tar.addfile(info, io.BytesIO(raw))
    packed = archive.getvalue()
    generation = receipt["generation"]
    record = {"generation": generation, "commit": receipt["commit"],
              "receipt_sha256": digest(raw), "archive_sha256": digest(packed),
              "files": files, "closure": "all receipt-sealed src, tools and fleet files; dynamic siblings included",
              "git": git_provenance(git_root, receipt["commit"], files)}
    put(out / (generation + ".tar.gz"), packed)
    put(out / (generation + ".json"), json.dumps(record, sort_keys=True).encode() + b"\n")
    return record


def capture_window(status_path, generations, rollback, rollback_commit, out, git_root):
    status_raw = status_path.read_bytes()
    status = json.loads(status_raw)
    if (status.get("complete") is not True or status.get("timed_out_sections")
            or status.get("unavailable_sections") or not isinstance(status.get("nodes"), list)):
        raise ValueError("incomplete live census cannot designate a supported window")
    target = json.loads((generations / rollback / "RUNTIME_VERSION.json").read_bytes())
    if target.get("commit") != rollback_commit or target.get("generation") != rollback:
        raise ValueError("explicit rollback designation disagrees with immutable receipt")
    live = [node for node in status["nodes"] if node.get("state") == "live"]
    if not live or any(not re.fullmatch("[0-9a-f]{40}", str(n.get("runtime_commit")))
                       for n in live):
        raise ValueError("live hosts need full commit identities")
    commits = {node["runtime_commit"] for node in live}
    by_commit = defaultdict(list)
    for path in sorted(generations.glob("*/RUNTIME_VERSION.json")):
        value = json.loads(path.read_bytes())
        if value.get("commit") in commits:
            by_commit[value["commit"]].append(path.parent.name)
    supported = {rollback}
    for commit in commits:
        matches = by_commit[commit]
        if len(matches) != 1:
            raise ValueError(f"live commit needs unambiguous generation evidence: {commit}: {matches}")
        supported.update(matches)
    records = [capture_generation(generations / name, out, git_root)
               for name in sorted(supported)]
    captured = datetime.now(timezone.utc).isoformat()
    window = {"policy": "live worker generations plus designated rollback target",
              "live_identity_basis": "full live commit plus unique immutable generation receipt; ambiguity refuses",
              "captured_at": captured, "status_mtime_unix": status_path.stat().st_mtime,
              "status_sha256": digest(status_raw), "complete": True,
              "rollback": {"generation": rollback, "commit": rollback_commit,
                           "authority": "Rob, implementation brief 2026-10-16"},
              "live": [{key: node.get(key) for key in
                        ("node", "runtime_commit", "age_s", "healthy", "loops")} for node in live],
              "excluded": [{"node": node.get("node"), "state": node.get("state")}
                           for node in status["nodes"] if node not in live],
              "generations": [{key: record[key] for key in
                               ("generation", "commit", "archive_sha256", "receipt_sha256")}
                              for record in records]}
    put(out / "window.json", document(window))
    return window


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("inventory")
    listing.add_argument("--root", type=Path, required=True)
    listing.add_argument("--out", type=Path, required=True)
    capture = commands.add_parser("capture")
    capture.add_argument("--status", type=Path, required=True)
    capture.add_argument("--generations", type=Path, required=True)
    capture.add_argument("--rollback-generation", required=True)
    capture.add_argument("--rollback-commit", required=True)
    capture.add_argument("--git-root", type=Path, required=True)
    capture.add_argument("--out", type=Path, required=True)
    equivalent = commands.add_parser("source-equivalence")
    equivalent.add_argument("--root", type=Path, required=True)
    equivalent.add_argument("--receipt", type=Path, required=True)
    equivalent.add_argument("--out", type=Path, required=True)
    prepared = commands.add_parser("prepared")
    prepared.add_argument("--root", type=Path, required=True)
    prepared.add_argument("--commit", required=True)
    prepared.add_argument("--name", required=True)
    prepared.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "inventory":
        rows = inventory(args.root)
        put(args.out, b"".join(json.dumps(row, sort_keys=True).encode() + b"\n" for row in rows))
        print(f"inventoried {len(rows)} wire IDs")
    elif args.command == "capture":
        window = capture_window(args.status, args.generations, args.rollback_generation,
                                args.rollback_commit, args.out, args.git_root)
        print(json.dumps({"captured_at": window["captured_at"],
                          "generations": [r["generation"] for r in window["generations"]],
                          "live_hosts": len(window["live"])}))
    elif args.command == "source-equivalence":
        value = source_equivalence(args.root, json.loads(args.receipt.read_bytes()))
        put(args.out, json.dumps(value, sort_keys=True).encode() + b"\n")
        print(f"verified {len(value['verified_paths'])} published files against {value['source_commit']}")
    else:
        row = capture_prepared(args.root, args.commit, args.out, args.name)
        put(args.out / "prepared.json", document([row]))
        print(json.dumps(row, sort_keys=True))


if __name__ == "__main__":
    main()
