"""Map every original production/tool assertion through tier caller migration."""
import argparse
import ast
from collections import Counter, defaultdict
import hashlib
import io
import json
from pathlib import Path
import runpy
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--base", default="578e54d0788c0f3b138f5dcf217e980322bb7726")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    def git(*words, stdin=None):
        return subprocess.check_output(["git", *words], cwd=args.root, input=stdin)

    def assertions(tree):
        return [node for node in ast.walk(tree) if isinstance(node, ast.Assert)]

    def key(node):
        return ast.dump(node, include_attributes=False)

    paths = [p for p in git("ls-tree", "-r", "--name-only", args.base, "src", "tools").decode().splitlines()
             if p.endswith(".py")]
    data = git("cat-file", "--batch", stdin="".join(args.base + ":" + p + "\n" for p in paths).encode())
    stream = io.BytesIO(data)
    migration = runpy.run_path(str(args.root / "docs/evidence/c10/migrate_source.py"))
    old, new = Counter(), Counter()
    origins, destinations = defaultdict(list), defaultdict(list)
    changes = []
    for path in paths:
        header = stream.readline().decode().split()
        tree = ast.parse(stream.read(int(header[2])).decode())
        assert stream.read(1) == b"\n"
        before = {node.lineno: key(node) for node in assertions(tree)}
        scopes, aliases = migration["caller_aliases"](tree)
        normalized = migration["CallerMove"](scopes, aliases).visit(tree)
        for node in assertions(normalized):
            predicate = key(node)
            old[predicate] += 1
            origins[predicate].append(dict(path=path, line=node.lineno))
            if before[node.lineno] != predicate:
                changes.append(dict(path=path, line=node.lineno,
                                    original=before[node.lineno], current=predicate))
    for path in git("ls-files", "src", "tools").decode().splitlines():
        if not path.endswith(".py"):
            continue
        for node in assertions(ast.parse((args.root / path).read_bytes())):
            predicate = key(node)
            new[predicate] += 1
            destinations[predicate].append(dict(path=path, line=node.lineno))
    missing = old - new
    report = dict(source_only=True, base=args.base,
        head=git("rev-parse", "HEAD").decode().strip(), tree=git("rev-parse", "HEAD^{tree}").decode().strip(),
        original_production_tool_assertions=old.total(), mapped_retained_assertions=(old & new).total(),
        missing=[dict(predicate=predicate, count=count, original=origins[predicate])
                 for predicate, count in missing.items()], exact_tier_owner_substitutions=changes,
        mappings=[dict(predicate_sha256=hashlib.sha256(predicate.encode()).hexdigest(),
                       original=origins[predicate], current_candidates=destinations[predicate],
                       original_count=count, retained_count=min(count, new[predicate]))
                  for predicate, count in old.items()])
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    assert not missing, report["missing"]
    print(json.dumps({key: report[key] for key in
        ("head", "original_production_tool_assertions", "mapped_retained_assertions")}))


if __name__ == "__main__":
    main()
