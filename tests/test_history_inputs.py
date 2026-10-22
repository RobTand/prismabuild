"""Collected tests must get retained source from the committed tree (#1999)."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

import history_inputs

HISTORY_TOOLS = {
    "compat/support/compat_sources.py": "Maintenance capture deliberately resolves historical compatibility sources.",
    "compat/support/rebind_source_reviews.py": "Maintenance rebinding compares reviewed sources across explicit revisions.",
    "compat/support/record_compat.py": "Compatibility capture records the selected historical source identity.",
    "c07_review_audit.py": "Standalone source audit compares the reviewed C07 baseline with current sources.",
    "lifecycle_source_audit.py": "Standalone lifecycle audit compares explicit historical source trees.",
}
HISTORY_VERBS = {"show", "archive", "cat-file", "ls-tree", "rev-list", "log", "diff"}


def history_read_sites(source):
    """Inspect argv syntax, including -C, interpolations and revision defaults.

    Unresolved revisions are refused too. A ref obtained from a repository's
    own HEAD can be used for a diff of that repository's snapshot.
    """
    tree = ast.parse(source)
    bindings, local_heads = {}, set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    bindings[target.id] = node.value
                    if any(isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
                           and call.func.id in {"git", "_git"}
                           and {"rev-parse", "HEAD"} <= {
                               arg.value for arg in call.args
                               if isinstance(arg, ast.Constant) and isinstance(arg.value, str)}
                           for call in ast.walk(node.value)):
                        local_heads.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            bindings[node.target.id] = node.value
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            arguments = node.args.posonlyargs + node.args.args
            for argument, default in zip(arguments[-len(node.args.defaults):], node.args.defaults):
                bindings[argument.arg] = default
            for argument, default in zip(node.args.kwonlyargs, node.args.kw_defaults):
                if default is not None:
                    bindings[argument.arg] = default

    def spelling(node, seen=frozenset()):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name) and node.id in bindings and node.id not in seen:
            return spelling(bindings[node.id], seen | {node.id})
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return spelling(node.left, seen) + spelling(node.right, seen)
        if isinstance(node, ast.JoinedStr):
            return "".join(spelling(part.value if isinstance(part, ast.FormattedValue) else part, seen)
                           for part in node.values)
        return "?"

    found = []
    for node in ast.walk(tree):
        direct = isinstance(node, (ast.List, ast.Tuple)) and bool(node.elts) and spelling(node.elts[0]) == "git"
        wrapper = (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                   and node.func.id in {"git", "_git", "lifecycle_audit_git"})
        if not (direct or wrapper):
            continue
        arguments = [spelling(arg) for arg in (node.elts[1:] if direct else node.args)]
        if wrapper and arguments and arguments[0] not in HISTORY_VERBS:
            arguments = arguments[1:]  # _git(root, verb, ...)
        while arguments and arguments[0].startswith("-"):
            option = arguments.pop(0)
            if option in {"-C", "-c", "--git-dir", "--work-tree"} and arguments:
                arguments.pop(0)
        if not arguments or arguments[0] not in HISTORY_VERBS:
            continue
        verb, *operands = arguments
        if verb == "cat-file" and operands and operands[0] in {"blob", "tree", "commit", "tag"}:
            operands.pop(0)
        revisions = []
        takes_value = {"-n", "--max-count", "--format", "--pretty", "--output", "--prefix"}
        skip = False
        for operand in operands:
            if skip:
                skip = False
                continue
            if operand == "--":
                break
            if operand.startswith("-"):
                skip = operand in takes_value
                continue
            revisions.append(operand.split(":", 1)[0])
            if verb in {"archive", "cat-file", "ls-tree"}:
                break
        names = {part.id for part in ast.walk(node) if isinstance(part, ast.Name)}
        local_diff = (wrapper and verb == "diff" and bool(names & local_heads)
                      and all(revision == "HEAD" or revision == "?...HEAD" for revision in revisions))
        if not local_diff and any(revision != "HEAD" for revision in revisions):
            found.append((node.lineno, verb, revisions))
    return sorted(found)


def history_tree_reads(root):
    return {str(path.relative_to(root)): sites
            for path in sorted((root / "tests").rglob("*.py"))
            if path.relative_to(root / "tests").as_posix() not in HISTORY_TOOLS
            and (sites := history_read_sites(path.read_bytes()))}


def test_historical_inputs_come_from_the_committed_tree():
    assert all(reason.strip() for reason in HISTORY_TOOLS.values())
    assert not history_tree_reads(Path(__file__).resolve().parents[1])


@pytest.mark.parametrize("verb", sorted(HISTORY_VERBS))
@pytest.mark.parametrize("operand", ["'deadbeef:source'", '"main:source"',
    'f"{PIN}:{path}"', 'PIN + ":" + path', 'f"{revision}:{path}"'])
@pytest.mark.parametrize("prefix", ['"git", ', "'git', '-C', root, "])
def test_history_guard_catches_retained_revisions(verb, operand, prefix):
    source = ('PIN = "deadbeef"\ndef read(revision=PIN):\n'
              f'    return subprocess.check_output([{prefix}{verb!r}, {operand}])\n')
    assert history_read_sites(source)


@pytest.mark.parametrize("verb", sorted(HISTORY_VERBS))
def test_history_guard_allows_head(verb):
    assert not history_read_sites(f'subprocess.run(["git", "-C", root, {verb!r}, "HEAD"])')


def test_history_guard_checks_pinned_git_wrappers():
    assert history_read_sites('PIN = "deadbeef"\n_git(root, "show", PIN + ":file")')
    assert history_read_sites('_git(root, "show", f"{revision}:file")')
    assert not history_read_sites('source_head = _git(root, "rev-parse", "HEAD").stdout.strip()\n'
                                 '_git(root, "diff", f"{source_head}...HEAD")')
    assert not history_read_sites('subprocess.run(["git", "cat-file", "blob", "HEAD:file"])')
    assert history_read_sites('subprocess.run(["git", "show", "HEAD", "main"])')


def test_every_committed_history_input_has_a_verified_blob():
    pins = set()
    for revision, row in history_inputs.history_manifest()["revisions"].items():
        for path, pin in row["files"].items():
            history_inputs.source_bytes(revision, path)
            pins.add(pin["sha256"])
    assert pins == history_inputs.history_blobs().keys()


@pytest.mark.parametrize("damage", ["bytes", "sha256"])
def test_history_inputs_refuse_damaged_bytes(monkeypatch, damage):
    revision, row = next(iter(history_inputs.history_manifest()["revisions"].items()))
    path, pin = next(iter(row["files"].items()))
    raw = history_inputs.source_bytes(revision, path)
    damaged = raw + b"!" if damage == "bytes" else bytes([raw[0] ^ 1]) + raw[1:]
    monkeypatch.setattr(history_inputs, "history_blobs", lambda: {pin["sha256"]: damaged})
    with pytest.raises(AssertionError, match="byte count differs" if damage == "bytes" else "digest differs"):
        history_inputs.source_bytes(revision, path)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    reads = history_tree_reads(parser.parse_args().root.resolve())
    for path, sites in reads.items():
        print(path, sites)
    raise SystemExit(bool(reads))
