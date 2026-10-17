#!/usr/bin/env python3
"""Resolve the conservative transitive importer scope for a frozen native roster."""
import argparse
import ast
from collections import defaultdict
import json
from pathlib import Path
import subprocess


def module_name(path):
    for prefix in ("src/", "tools/fleet/", "tests/"):
        if path.startswith(prefix):
            path = path[len(prefix):]
            break
    return path.removesuffix(".py").replace("/", ".").removesuffix(".__init__")


def imports(tree, module):
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                base = ".".join([*module.split(".")[:-node.level], *([base] if base else [])])
            found.add(base)
            found.update(".".join(filter(None, (base, alias.name))) for alias in node.names)
        elif (isinstance(node, ast.Call) and node.args
              and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)):
            name = node.func.id if isinstance(node.func, ast.Name) else (
                node.func.attr if isinstance(node.func, ast.Attribute) else "")
            if name in {"import_module", "__import__"}:
                found.add(node.args[0].value)
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--base", default="bfc6efce3dd700bffa08f70cc502fa7382dd4fb0")
    parser.add_argument("--population", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    def git(*words):
        return subprocess.check_output(["git", *words], cwd=args.root, text=True).strip()
    paths = [path for path in git("ls-files", "src", "tools", "tests").splitlines()
             if path.endswith(".py")]
    modules, basenames = {}, defaultdict(set)
    for path in paths:
        modules[module_name(path)] = path
        basenames[Path(path).stem].add(path)
    reverse, edges = defaultdict(set), []
    for path in paths:
        tree = ast.parse((args.root / path).read_text(), filename=path)
        names = imports(tree, module_name(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            if "import " in node.value:
                try:
                    names.update(imports(ast.parse(node.value), module_name(path)))
                except SyntaxError:
                    pass
        dependencies = set()
        for name in names:
            if name in modules:
                dependencies.add(modules[name])
            elif name.split(".")[0] in basenames:
                dependencies.update(basenames[name.split(".")[0]])
        if path.startswith("tests/"):
            parent = Path(path).parent
            while parent != Path("."):
                conftest = str(parent / "conftest.py")
                if conftest in paths and conftest != path:
                    dependencies.add(conftest)
                parent = parent.parent
        for dependency in sorted(dependencies):
            reverse[dependency].add(path)
            edges.append((path, dependency))
    changed = git("diff", "--name-only", args.base, "HEAD", "--", "src", "tools").splitlines()
    roots = set(changed) & set(paths)
    closure, pending = set(roots), list(roots)
    while pending:
        for importer in reverse[pending.pop()] - closure:
            closure.add(importer)
            pending.append(importer)
    nodes = args.population.read_text().splitlines()
    assert len(nodes) == len(set(nodes))
    files = {node.split("::", 1)[0] for node in nodes}
    report = dict(source_only=True, head=git("rev-parse", "HEAD"), tree=git("rev-parse", "HEAD^{tree}"),
                  roots=sorted(roots), importer_closure=sorted(closure),
                  native_importer_files=sorted(files & closure),
                  conservative_whole_suite_files=sorted(files), native_population=len(nodes),
                  outside_importer_closure=sorted(files - closure), edges=edges,
                  implicit_pytest_dependency="every native test file imports its ancestral conftest files")
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: (len(report[key]) if isinstance(report[key], list) else report[key])
                      for key in ("roots", "importer_closure", "native_importer_files",
                                  "conservative_whole_suite_files", "native_population")}))


if __name__ == "__main__":
    main()
