"""List whole test files affected by changed current test-side adapters."""
import argparse
import ast
from collections import defaultdict
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--base", default="b26d6bc94abb3c977bbf04650ac27d0d6535277a")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    def git(*words):
        return subprocess.check_output(["git", "-C", str(args.root), *words], text=True)

    paths = [p for p in git("ls-files", "tests").splitlines()
             if p.endswith(".py") and Path(p).parent == Path("tests")]
    modules = {Path(p).stem: p for p in paths}
    changed = set(git("diff", "--name-only", args.base, "HEAD", "--", "tests").splitlines())
    seeds = changed.intersection(paths)
    readers = defaultdict(set)
    references = []
    for path in paths:
        source = (args.root / path).read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            names = set()
            kind = "import"
            if isinstance(node, ast.ImportFrom):
                names.add((node.module or "").removeprefix("tests.").split(".")[0])
            elif isinstance(node, ast.Import):
                names.update(a.name.removeprefix("tests.").split(".")[0] for a in node.names)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                kind = "literal reference or embedded Python"
                for name in modules:
                    if ("tests/" + name + ".py" in node.value
                            or name + ":" in node.value or name + ".py" in node.value):
                        names.add(name)
                if "import " in node.value:
                    try:
                        embedded = ast.parse(node.value)
                    except SyntaxError:
                        embedded = None
                    if embedded is not None:
                        for inner in ast.walk(embedded):
                            if isinstance(inner, ast.ImportFrom):
                                names.add((inner.module or "").removeprefix("tests.").split(".")[0])
                            elif isinstance(inner, ast.Import):
                                names.update(a.name.removeprefix("tests.").split(".")[0]
                                             for a in inner.names)
            for name in sorted(names.intersection(modules)):
                dependency = modules[name]
                if dependency != path:
                    readers[dependency].add(path)
                    references.append(dict(reader=path, dependency=dependency,
                                           line=node.lineno, kind=kind))
    closure = set(seeds)
    while True:
        expanded = closure | {reader for dependency in closure for reader in readers[dependency]}
        if expanded == closure:
            break
        closure = expanded
    selected = sorted(p for p in closure if Path(p).name.startswith("test_"))
    for path in ["tests/test_duplication_baseline.py", "tests/test_admitted_queue_fixture.py"]:
        if path not in selected:
            selected.append(path)
    report = dict(source_only=True, base=args.base, pin=git("rev-parse", "HEAD").strip(),
                  current_top_level_python_files=len(paths), changed_seeds=sorted(seeds),
                  source_closure=sorted(closure), whole_files=selected,
                  edges=[r for r in references if r["reader"] in closure
                         and r["dependency"] in closure],
                  scope="Current top-level test/support imports and explicit file/entry/embedded-code references. Historical captured generations are immutable, not callers of current helpers. Includes complete architecture and explicit-admission controls.")
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"pin": report["pin"], "changed_seeds": len(seeds),
                      "source_closure": len(closure), "whole_files": len(selected)}))


if __name__ == "__main__":
    main()
