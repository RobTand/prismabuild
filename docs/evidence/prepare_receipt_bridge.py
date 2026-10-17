#!/usr/bin/env python3
"""Prepare an unpublished standalone C15 reader bridge from exact historical bytes.

Source/Git operations only. Never imports product code, executes a bridge,
publishes a generation, or changes a live pointer.
"""
import argparse
import ast
import gzip
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile


BASE = "a962ed3fb2767ce6805110a18124f3cf01a166a3"
PATH = "tools/fleet/upgrade_client.py"
BRANCH = "sol/c15-prepared-reader-bridge-r2-20261017"
LEAVES = {
    "digest_primitives.py": "src/prismabuild/digest_primitives.py",
    "generation_receipt.py": "src/prismabuild/generation_receipt.py",
}
DECLARATIONS = """
CLIENT_UPGRADE_RECEIPT_MEMBER_PROTOCOL = 1
RECEIPT_READER_MEMBERS = frozenset({'digest_primitives.py', 'generation_receipt.py'})


def bridge_protocol(source, name):
    tree = ast.parse(source)
    bindings = [node for node in ast.walk(tree)
                if isinstance(node, ast.Name) and node.id == name
                and isinstance(node.ctx, (ast.Store, ast.Del))]
    declarations = [node for node in tree.body
                    if (isinstance(node, ast.Assign) and len(node.targets) == 1
                        and isinstance(node.targets[0], ast.Name) and node.targets[0].id == name)
                    or (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                        and node.target.id == name)]
    other = [node for node in ast.walk(tree)
             if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name == name)
             or (isinstance(node, ast.arg) and node.arg == name)
             or (isinstance(node, ast.alias) and (node.asname or node.name) == name)
             or (isinstance(node, ast.ExceptHandler) and node.name == name)]
    if not bindings and not other:
        return None
    if len(bindings) != 1 or len(declarations) != 1 or other:
        raise ValueError('ambiguous receipt capability declaration: ' + name)
    value = ast.literal_eval(declarations[0].value)
    if type(value) is not int or value < 1:
        raise ValueError('invalid receipt capability declaration: ' + name)
    return value


def bridge_receipt_closure(version, blobs):
    present = set(version['files']) & RECEIPT_READER_MEMBERS
    if present and present != RECEIPT_READER_MEMBERS:
        raise ValueError('incomplete receipt reader dependency closure')
    if (bridge_protocol(blobs['upgrade_client.py'], 'CLIENT_UPGRADE_RECEIPT_READER_PROTOCOL') == 1
            and present != RECEIPT_READER_MEMBERS):
        raise ValueError('receipt reader requires both installed dependencies')

"""
FENCE = """        bridge_receipt_closure(version, blobs)
        if set(names) & RECEIPT_READER_MEMBERS:
            for code in (blobs['upgrade_client.py'], installed_bytes['upgrade_client.py']):
                if bridge_protocol(code, 'CLIENT_UPGRADE_RECEIPT_MEMBER_PROTOCOL') != 1:
                    raise ValueError('receipt client transaction requires receipt-member recovery protocol 1')
"""


def git(root, *args, data=None, env=None):
    return subprocess.check_output(["git", "-C", str(root), *args], input=data, env=env).strip()


def replace_once(source, old, new):
    if source.count(old) != 1:
        raise ValueError("bridge source anchor is not exact: " + old[:100])
    return source.replace(old, new)


def prepare(source):
    text = source.decode()
    text = replace_once(text, "import argparse\n", "import argparse\nimport ast\n")
    text = replace_once(text, "OPTIONAL_MEMBERS = {\n", "OPTIONAL_MEMBERS = {\n" + "".join(
        f"    {name!r}: {member!r},\n" for name, member in LEAVES.items()))
    text = replace_once(text, "UPDATER_ONLY_MEMBERS = frozenset({'upgrade_client.py', 'park_proof.py'})",
                        "UPDATER_ONLY_MEMBERS = frozenset({'upgrade_client.py', 'park_proof.py', "
                        "'digest_primitives.py', 'generation_receipt.py'})")
    text = replace_once(text, "\ndef digest(data):", DECLARATIONS + "\ndef digest(data):")
    text = replace_once(text, "        previous = {name: installed.get(name) for name in names}\n",
                        FENCE + "        previous = {name: installed.get(name) for name in names}\n")
    text = replace_once(text, "        for name, expected in files.items():\n            target = self.install / name\n",
                        "        ordered = sorted(files, key=lambda name: (files[name] is None, "
                        "name == 'upgrade_client.py', name))\n        for name in ordered:\n"
                        "            expected = files[name]\n            target = self.install / name\n")
    ast.parse(text)
    return text.encode()


def corpus(root, output, bridge):
    entries = {"standalone-receipt-bridge.py": bridge}
    for relative in (PATH, "src/prismabuild/generation_receipt.py",
                     "src/prismabuild/runtime_publication.py", "src/prismabuild/resource_scope.py",
                     "tools/fleet/publish_runtime.py", "tools/fleet/supervise.py"):
        entries["base/" + relative] = subprocess.check_output(
            ["git", "-C", str(root), "show", BASE + ":" + relative])
    buffer = BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, data in sorted(entries.items()):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o444
            archive.addfile(info, BytesIO(data))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(gzip.compress(buffer.getvalue(), mtime=0))
    return {name: {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
            for name, data in entries.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--create-branch", action="store_true")
    args = parser.parse_args()
    root = args.checkout.resolve()
    source = subprocess.check_output(["git", "-C", str(root), "show", BASE + ":" + PATH])
    bridge = prepare(source)
    report = {"base": BASE, "base_updater_sha256": hashlib.sha256(source).hexdigest(),
              "bridge_sha256": hashlib.sha256(bridge).hexdigest(), "published": False,
              "leaves": LEAVES, "corpus": corpus(root, args.output, bridge)}
    if args.create_branch:
        with tempfile.TemporaryDirectory() as directory:
            environment = dict(os.environ, GIT_INDEX_FILE=str(Path(directory) / "index"),
                               GIT_AUTHOR_DATE="2026-10-17T13:45:00Z",
                               GIT_COMMITTER_DATE="2026-10-17T13:45:00Z")
            git(root, "read-tree", BASE, env=environment)
            blob = git(root, "hash-object", "-w", "--stdin", data=bridge).decode()
            git(root, "update-index", "--add", "--cacheinfo", "100644," + blob + "," + PATH, env=environment)
            tree = git(root, "write-tree", env=environment).decode()
            commit = git(root, "commit-tree", tree, "-p", BASE,
                         data=b"source: prepare standalone C15 receipt reader bridge\n", env=environment).decode()
            existing = subprocess.run(["git", "-C", str(root), "rev-parse", "--verify", "refs/heads/" + BRANCH],
                                      capture_output=True)
            if existing.returncode == 0 and existing.stdout.decode().strip() != commit:
                raise ValueError("refusing to replace an existing different prepared bridge branch")
            git(root, "update-ref", "refs/heads/" + BRANCH, commit)
            report.update(branch=BRANCH, commit=commit, tree=tree)
    metadata = args.output.with_suffix(".json")
    metadata.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
