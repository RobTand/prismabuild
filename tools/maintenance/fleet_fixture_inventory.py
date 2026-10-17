"""Source-only C30 occurrence census, check preservation and importer graph."""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import csv
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile
import textwrap

BASE = "fe416690b87aba99ad30c7df4f95ac8da47055d5"
HELPERS = {"_publish", "_queue", "_item"}


def fixture_blob(source):
    return subprocess.check_output(["git", "hash-object", "--stdin"], input=source.encode()).decode().strip()


def fixture_sources(root, ref=None):
    if ref is None:
        return {p.relative_to(root).as_posix(): p.read_text()
                for p in sorted((root / "tests").rglob("*.py"))}
    raw = subprocess.check_output(["git", "-C", str(root), "archive", ref, "tests"])
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        return {m.name: archive.extractfile(m).read().decode()
                for m in archive.getmembers() if m.isfile() and m.name.endswith(".py")}


def fixture_nodes(tree):
    """Qualify each definition occurrence, including overwritten definitions."""
    counts = Counter()

    def descend(node, scope):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            key = scope, node.name
            counts[key] += 1
            scope += (f"{node.name}#{counts[key]}",)
        yield node, scope
        for child in ast.iter_child_nodes(node):
            yield from descend(child, scope)

    yield from descend(tree, ())


def fixture_checks(source, *, embedded=True):
    result = []
    counters = Counter()
    for node, scope in fixture_nodes(ast.parse(source)):
        kind = None
        if isinstance(node, ast.Assert):
            kind = "assert"
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr.startswith("assert") or node.func.attr == "raises":
                kind = "assert-call"
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            for index, decorator in enumerate(node.decorator_list):
                result.append(dict(scope=scope, kind="decorator", occurrence=index,
                                   line=decorator.lineno, form=ast.dump(decorator)))
            if node.name.startswith("test"):
                kind = "native-definition"
        if kind:
            counters[scope, kind] += 1
            form = node.name if kind == "native-definition" else ast.dump(node)
            result.append(dict(scope=scope, kind=kind,
                               occurrence=counters[scope, kind], line=node.lineno, form=form))
        if embedded and isinstance(node, ast.Constant) and isinstance(node.value, str):
            if not re.search(r"\b(assert|def test|unittest)\b", node.value):
                continue
            try:
                nested = fixture_checks(textwrap.dedent(node.value), embedded=False)
            except (SyntaxError, ValueError):
                continue
            for check in nested:
                check["scope"] = (*scope, f"embedded@{node.lineno}", *check["scope"])
                result.append(check)
    return result


def fixture_inventory(sources):
    rows = []
    for path, source in sorted(sources.items()):
        tree = ast.parse(source)
        numbered = list(fixture_nodes(tree))
        calls = [(n, scope) for n, scope in numbered if isinstance(n, ast.Call)]
        counters = Counter()
        for node, scope in numbered:
            kind = None
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in HELPERS:
                kind = "helper"
            elif isinstance(node, ast.Call):
                target = ast.unparse(node.func)
                if ((target.endswith(".setattr") or target.endswith("patch.object"))
                        and any(isinstance(v, ast.Constant) and v.value == "_now"
                                for v in node.args)):
                    kind = "clock-patch"
                elif (target.endswith((".write_text", ".write_bytes"))
                      and any(isinstance(v, ast.Call) and ast.unparse(v.func) == "json.dumps"
                              for v in ast.walk(node))) or target == "json.dump":
                    kind = "raw-write"
            if kind is None:
                continue
            counters[scope, kind] += 1
            consumers = [dict(line=n.lineno, call=ast.unparse(n.func))
                         for n, call_scope in calls if call_scope == scope
                         and n.lineno >= node.lineno and n is not node][:12]
            if kind == "helper":
                consumers = [dict(line=n.lineno, scope=call_scope, call=ast.unparse(n))
                             for n, call_scope in calls
                             if isinstance(n.func, ast.Name) and n.func.id == node.name
                             and call_scope != scope]
            rows.append(dict(id=f"{path}::{'.'.join(scope)}::{kind}#{counters[scope, kind]}",
                             path=path, scope=scope, kind=kind, line=node.lineno,
                             source=ast.unparse(node), consumers=consumers,
                             source_git_blob=fixture_blob(source)))
        # The review's 79-file lexical upper bound also includes clock reads and
        # embedded programs, which must not disappear from the occurrence map.
        for index, line in enumerate(source.splitlines(), 1):
            if re.search(r"pool\._now|['\"]_now['\"]", line):
                rows.append(dict(id=f"{path}::clock-text@{index}", path=path, line=index,
                                 kind="clock-text", source=line.strip(), scope=(), consumers=[]))
    return rows


def fixture_graph(sources):
    modules = {path.removesuffix(".py").replace("/", "."): path for path in sources}
    for module, path in list(modules.items()):
        if module.startswith("tests."):
            modules[module.removeprefix("tests.")] = path
    edges = defaultdict(set)
    for path, source in sources.items():
        for node in ast.walk(ast.parse(source)):
            names = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                names = [module, *(module + "." + alias.name for alias in node.names)]
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                # importlib/path-based fixtures are part of the exported graph.
                names = [node.value, *re.findall(
                    r"(?:tests/)?((?:test_|[a-z_]+fixture)[a-z_0-9]+)\.py", node.value)]
            edges[path].update(modules[name] for name in names if name in modules and modules[name] != path)
    return {path: sorted(edges[path]) for path in sorted(sources)}


def fixture_preservation(before, after, paths):
    result = []
    for path in sorted(paths):
        old = fixture_checks(before.get(path, ""))
        new = fixture_checks(after.get(path, ""))
        # Embedded scope identities use the string's content hash, not its
        # physical line after a helper deletion shifts the enclosing program.
        def key(row):
            scope = tuple(re.sub(r"embedded@\d+", "embedded", s) for s in row["scope"])
            return scope, row["kind"], row["form"]
        remaining = defaultdict(list)
        for row in new:
            remaining[key(row)].append(row)
        mappings = []
        for row in old:
            matches = remaining[key(row)]
            mappings.append(dict(original=row, current=matches.pop(0) if matches else None))
        result.append(dict(path=path, original=len(old), current=len(new), mappings=mappings,
                           lost=sum(r["current"] is None for r in mappings),
                           added=[r for values in remaining.values() for r in values]))
    return dict(paths=result, lost=sum(row["lost"] for row in result),
                original=sum(row["original"] for row in result),
                current=sum(row["current"] for row in result))


def fixture_definition_pins(sources, paths):
    return {path: [dict(scope=scope, line=node.lineno, form=ast.dump(node))
                   for node, scope in fixture_nodes(ast.parse(sources.get(path, "")))
                   if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
            for path in sorted(paths)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--base", default=BASE)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--classifications", type=Path)
    args = parser.parse_args()
    before = fixture_sources(args.root, args.base)
    after = fixture_sources(args.root)
    rows = fixture_inventory(before)
    changed = [p for p in before if before[p] != after.get(p)]
    graph = fixture_graph(after)
    affected = set(changed)
    while True:
        grown = affected | {p for p, imports in graph.items() if affected.intersection(imports)}
        if grown == affected:
            break
        affected = grown
    report = dict(base=args.base, source_only=True, product_imports=False,
                  counts=dict(Counter(row["kind"] for row in rows)),
                  files={kind: len({r["path"] for r in rows if r["kind"] == kind})
                         for kind in {r["kind"] for r in rows}},
                  original_sources={p: fixture_blob(s) for p, s in before.items()},
                  original_occurrences=rows, current_occurrences=fixture_inventory(after),
                  checks=fixture_preservation(before, after, changed),
                  original_definition_pins=fixture_definition_pins(before, changed),
                  current_definition_pins=fixture_definition_pins(after, changed),
                  changed_files=changed, importer_graph=graph, original_importer_graph=fixture_graph(before),
                  transitive_affected=sorted(affected))
    if args.classifications:
        with args.classifications.open() as stream:
            classified = list(csv.DictReader(stream, delimiter="\t"))
        roster = {row["id"] for row in rows}
        actual = [row["id"] for row in classified]
        if len(actual) != len(set(actual)) or set(actual) != roster:
            raise ValueError("classification does not cover each original occurrence exactly once")
        if any(not row["reason"] or not row["consumer"] for row in classified):
            raise ValueError("classification lacks a concrete reason or consumer")
        report["classifications"] = dict(path=str(args.classifications),
            git_blob=fixture_blob(args.classifications.read_text()),
            counts=dict(Counter(row["disposition"] for row in classified)), occurrences=classified)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("base", "counts", "files", "changed_files")},
                     separators=(",", ":")))
    return bool(report["checks"]["lost"])


if __name__ == "__main__":
    raise SystemExit(main())
