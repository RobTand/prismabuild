"""C09's source-only move recipe, pinned before the ownership extraction.

Run with --apply only on the exact source pin. Audits may run locally. Runtime
controls and collection must run inside an admitted PrismaBuild action.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import io
import json
from pathlib import Path
import re
import subprocess
import tokenize

STAGED_BASE = "a8ba9c0c7beeeb379908924a60c6933e997c8df6"
STAGED_OWNER = "src/prismabuild/staged_path.py"
STAGED_MODULES = {
    "stage_release": "src/prismabuild/stage_release.py",
    "stage_move": "src/prismabuild/stage_move.py",
    "tier_loop": "tools/fleet/tier_loop.py",
    "produced_output": "src/prismabuild/produced_output.py",
}
STAGED_RANGES = {
    "stage_release": ((138, 187), (239, 239), (253, 376), (408, 3413),
                      (3470, 3515), (4496, 4529), (4576, 4577), (4812, 6239)),
    "stage_move": ((83, 83), (91, 176), (343, 2944), (5097, 5115)),
    "tier_loop": ((2530, 2802),),
    "produced_output": ((142, 142), (186, 189)),
}
STAGED_PUBLIC = {
    "_StagedPublisher": "StagedPublisher", "_PublicationRefused": "PublicationRefused",
    "_PhaseClock": "PhaseClock", "_claimed_paths": "claimed_paths",
    "_claimed_paths_attributed": "claimed_paths_attributed",
    "_claimed_source_paths": "claimed_source_paths", "_fragment_owners": "fragment_owners",
    "_fragment_census": "fragment_census", "_ownership_census": "ownership_census",
    "_unended_owner": "unended_owner", "_drop_vouch": "withdraw_vouch",
    "_stage_ownership": "ownership", "_locked_parse_record": "locked_parse_record",
    "_hold_record": "hold_record", "_attributed_census": "attributed_census",
    "_cached_manifest_layout": "manifest_layout_for", "_prune_empty": "prune_empty",
    "_bounded_int": "bounded_int", "_validated_sealed_request": "validated_sealed_request",
    "_EgressReporter": "EgressReporter", "_EGRESS_RUN": "egress_run",
    "_is_action_key": "is_action_key",
}


def staged_sources(root, ref):
    paths = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", ref], cwd=root, text=True).splitlines()
    return {p: subprocess.check_output(["git", "show", f"{ref}:{p}"], cwd=root, text=True)
            for p in paths if p.endswith(".py") and not p.startswith("tests/compat/")}


def staged_symbols(sources):
    selected = {}
    for module, path in STAGED_MODULES.items():
        chosen = set()
        for node in ast.parse(sources[path]).body:
            if not any(lo <= node.lineno <= hi for lo, hi in STAGED_RANGES[module]):
                continue
            if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)):
                chosen.add(node.name)
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                chosen.update(n.id for t in targets for n in ast.walk(t) if isinstance(n, ast.Name))
        selected[module] = chosen
    return selected


def staged_destination(module):
    return "output_vocabulary" if module == "produced_output" else "staged_path"


def staged_exports(sources):
    """Resolve imported test-helper modules without loading any product code."""
    exports = {Path(path).stem: {} for path in sources}
    exports["prismabuild"] = {name: name for name in exports}
    pending = []
    for path, source in sources.items():
        own = exports[Path(path).stem]
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.name.rsplit(".", 1)[-1]
                    if name in exports:
                        own[alias.asname or alias.name] = name
            elif isinstance(node, ast.ImportFrom):
                module = (node.module or "").rsplit(".", 1)[-1]
                if module in exports:
                    pending.append((own, module, node.names))
    while True:
        changed = False
        for own, module, names in pending:
            for alias in names:
                target = exports[module].get(alias.name)
                key = alias.asname or alias.name
                if target and key not in own:
                    own[key] = target
                    changed = True
                elif target and own.get(key) not in (None, "", target):
                    own[key] = ""
                    changed = True
        if not changed:
            return exports


def staged_reference_module(node, aliases, exports):
    if isinstance(node, ast.Name):
        return aliases.get(node.id)
    if isinstance(node, ast.Attribute):
        parent = staged_reference_module(node.value, aliases, exports)
        return exports.get(parent, {}).get(node.attr)
    return None


def staged_alias_bindings(tree, selected, exports):
    aliases = {name: name for name in selected}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                module = alias.name.rsplit(".", 1)[-1]
                if module in exports or module in selected:
                    aliases[alias.asname or alias.name] = module
                    if alias.asname is None and "." in alias.name:
                        aliases[alias.name.split(".")[0]] = alias.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom):
            module = (node.module or "").split(".")[-1]
            if module in exports:
                aliases.update({a.asname or a.name: exports[module][a.name]
                                for a in node.names if a.name in exports[module]})
    return aliases


def staged_references(source, selected, *, local=None, exports=None):
    """Rewrite imports, calls, fault seams and embedded programs without reformatting."""
    tree = ast.parse(source)
    docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                  if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                  and n.body and isinstance(n.body[0], ast.Expr)
                  and isinstance(n.body[0].value, ast.Constant)
                  and isinstance(n.body[0].value.value, str)}
    annotation_strings = {id(value) for node in ast.walk(tree)
                          for annotation in (getattr(node, "annotation", None),
                                             getattr(node, "returns", None))
                          if annotation is not None for value in ast.walk(annotation)
                          if isinstance(value, ast.Constant) and isinstance(value.value, str)}
    lines = source.splitlines(keepends=True)
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line))
    edits = []
    needs = set()
    exports = exports or {"prismabuild": {m: m for m in selected}}
    aliases = staged_alias_bindings(tree, selected, exports)
    bare = {}
    dotted_patterns = {m: re.compile(rf"\b{m}\.({'|'.join(map(re.escape, sorted(names, key=len, reverse=True)))})\b")
                       for m, names in selected.items() if names}

    def edit(node, text):
        edits.append((offsets[node.lineno - 1] + node.col_offset,
                      offsets[node.end_lineno - 1] + node.end_col_offset, text))

    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        module = (node.module or "").split(".")[-1]
        if module not in selected:
            continue
        keep, move = [], []
        for alias in node.names:
            if alias.name in selected[module]:
                destination = staged_destination(module)
                name = STAGED_PUBLIC.get(alias.name, alias.name)
                bare[alias.asname or alias.name] = f"{destination}.{name}"
                needs.add(destination)
            else:
                keep.append(ast.unparse(alias))
        if len(keep) != len(node.names):
            edit(node, f"from {node.module} import {', '.join(keep)}" if keep else "")
    if local is not None:
        for name in selected[local]:
            bare[name] = f"{staged_destination(local)}.{STAGED_PUBLIC.get(name, name)}"
    covered = [(a, b) for a, b, _ in edits]
    for node in ast.walk(tree):
        a = offsets[getattr(node, "lineno", 1) - 1] + getattr(node, "col_offset", 0)
        if any(lo <= a < hi for lo, hi in covered):
            continue
        if isinstance(node, ast.Attribute):
            module = staged_reference_module(node.value, aliases, exports)
            if module in selected and node.attr in selected[module]:
                destination = staged_destination(module)
                needs.add(destination)
                edit(node, f"{destination}.{STAGED_PUBLIC.get(node.attr, node.attr)}")
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id in bare:
            edit(node, bare[node.id]); needs.add(bare[node.id].split(".")[0])
        elif isinstance(node, ast.Call) and len(node.args) >= 2:
            target, key = node.args[:2]
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                module = staged_reference_module(target, aliases, exports)
                if module in selected and key.value in selected[module]:
                    destination = staged_destination(module)
                    edit(target, destination); edit(key, repr(STAGED_PUBLIC.get(key.value, key.value)))
                    needs.add(destination)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
            value = node.value
            if id(node) in annotation_strings:
                tokens = []
                for token in tokenize.generate_tokens(io.StringIO(value).readline):
                    if token.type == tokenize.NAME and token.string in bare:
                        needs.add(bare[token.string].split(".")[0])
                        token = token._replace(string=bare[token.string])
                    tokens.append(token)
                value = tokenize.untokenize(tokens)
            for module, pattern in dotted_patterns.items():
                if module + "." in value:
                    value = pattern.sub(lambda match: f"{staged_destination(module)}.{STAGED_PUBLIC.get(match[1], match[1])}", value)
            if "from prismabuild" in value and "\n" in value:
                try:
                    value = staged_references(value, selected, exports=exports)
                except (SyntaxError, IndentationError):
                    pass
            if value != node.value:
                edit(node, repr(value))
    unique = {(a, b): text for a, b, text in edits}
    for (a, b), text in sorted(unique.items(), reverse=True):
        source = source[:a] + text + source[b:]
    present = {a.name for n in tree.body if isinstance(n, ast.ImportFrom) and n.module == "prismabuild"
               for a in n.names if a.asname in (None, a.name)}
    needs -= present
    if needs:
        tree = ast.parse(source)
        at = 1
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                at = node.end_lineno + 1
        lines = source.splitlines(keepends=True)
        lines.insert(at - 1, f"from prismabuild import {', '.join(sorted(needs))}\n")
        source = "".join(lines)
    return source


def staged_extract(source, module):
    lines = source.splitlines(keepends=True)
    remaining, moved = [], []
    before = 0
    for node in ast.parse(source).body:
        end = node.end_lineno
        text = "".join(lines[before:end])
        selected = any(lo <= node.lineno <= hi for lo, hi in STAGED_RANGES[module])
        if selected and module == "stage_release" and getattr(node, "name", None) == "_is_action_key":
            before = end
            continue
        (moved if selected else remaining).append(text)
        before = end
    remaining.extend(lines[before:])
    return "".join(remaining), "".join(moved)


def staged_lower_progress(root):
    """Move the exact accepted-lease read below both role orchestrators."""
    path = root / "src/prismabuild/prewarm_loop.py"
    source = path.read_text()
    node = next((n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef)
                 and n.name == "progress_phase"), None)
    if node is None:
        return
    lines = source.splitlines(keepends=True)
    method = "".join(lines[node.lineno - 1:node.end_lineno])
    method = method.replace("def progress_phase(queue: pool.PoolQueue,", "def accepted_progress_phase(self,")
    method = method.replace("queue.records.lease_path", "self.lease_path")
    method = method.replace("    path = self.lease_path", "    from . import core as pb, progress as progress_v1\n\n    path = self.lease_path")
    store = root / "src/prismabuild/record_store.py"
    store_source = store.read_text()
    at = next(n for n in ast.walk(ast.parse(store_source)) if isinstance(n, ast.FunctionDef) and n.name == "scratch_owner").lineno - 1
    store_lines = store_source.splitlines(keepends=True)
    store_lines.insert(at, "".join("    " + line if line.strip() else line for line in method.splitlines(keepends=True)) + "\n\n")
    store.write_text("".join(store_lines))
    path.write_text("".join(lines[:node.lineno - 1] + lines[node.end_lineno:]))
    files = list((root / "src/prismabuild").glob("*.py")) + list((root / "tools/fleet").glob("*.py")) + list((root / "tests").glob("*.py"))
    for path in files:
        source = path.read_text()
        if "progress_phase" not in source:
            continue
        lines = source.splitlines(keepends=True)
        offsets = [0]
        for line in lines:
            offsets.append(offsets[-1] + len(line))
        edits = []
        for call in ast.walk(ast.parse(source)):
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            phase_call = (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name)
                          and func.value.id == "prewarm_loop" and func.attr == "progress_phase")
            local_call = (path.name == "prewarm_loop.py" and isinstance(func, ast.Name)
                          and func.id == "progress_phase")
            new = None
            if (phase_call or local_call) and len(call.args) >= 3:
                receiver = ast.unparse(call.args[0])
                new = f"{receiver}.records.accepted_progress_phase({', '.join(ast.unparse(a) for a in call.args[1:])})"
            elif (isinstance(func, ast.Attribute) and func.attr == "setattr" and len(call.args) >= 2
                  and isinstance(call.args[0], ast.Name) and call.args[0].id == "prewarm_loop"
                  and isinstance(call.args[1], ast.Constant) and call.args[1].value == "progress_phase"):
                call.args[0] = ast.parse("pool.record_store.RecordStore", mode="eval").body
                call.args[1] = ast.Constant("accepted_progress_phase")
                new = ast.unparse(call)
            if new is not None:
                edits.append((offsets[call.lineno - 1] + call.col_offset,
                              offsets[call.end_lineno - 1] + call.end_col_offset, new))
        for a, b, new in sorted(edits, reverse=True):
            source = source[:a] + new + source[b:]
        if edits:
            path.write_text(source)
    owner = root / STAGED_OWNER
    source = owner.read_text().replace("import prewarm_loop, reader_lease", "import reader_lease")
    source = source.replace("prewarm_loop.BLOCK", "reader_lease.CONTENT_BLOCK_BYTES")
    owner.write_text(source)


def staged_inventory(path, source):
    tree = ast.parse(source, filename=path)
    rows = []
    parents = {c: n for n in ast.walk(tree) for c in ast.iter_child_nodes(n)}
    occurrence = Counter()
    for node in sorted(ast.walk(tree), key=lambda n: (getattr(n, "lineno", 0), getattr(n, "col_offset", 0))):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Call, ast.Assert,
                                 ast.With, ast.If, ast.Try, ast.Raise, ast.Constant)):
            continue
        if isinstance(node, ast.Constant) and not isinstance(node.value, str):
            continue
        scope, guards = [], []
        parent = parents.get(node)
        while parent is not None:
            if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                scope.append(parent.name)
            if isinstance(parent, (ast.If, ast.While, ast.For, ast.With, ast.Try, ast.ExceptHandler, ast.IfExp)):
                guards.append(ast.unparse(parent.test) if hasattr(parent, "test") else type(parent).__name__)
            parent = parents.get(parent)
        kind = type(node).__name__
        name = getattr(node, "name", "")
        occurrence[(tuple(scope), kind, name)] += 1
        row = {"kind": kind, "scope": list(reversed(scope)), "name": name,
               "occurrence": occurrence[(tuple(scope), kind, name)],
               "line": node.lineno, "column": node.col_offset, "guards": list(reversed(guards))}
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            row["decorators"] = [ast.dump(d) for d in node.decorator_list]
        else:
            row["ast"] = ast.dump(node, include_attributes=False)
        rows.append(row)
    return {"rows": rows, "ast": ast.dump(tree, include_attributes=False)}


def staged_population(sources):
    members = set(STAGED_MODULES.values()) | {"src/prismabuild/ram_promote.py", "src/prismabuild/reader_lease.py",
        "tools/fleet/stage_reclaim.py", "src/prismabuild/pool.py", "tests/residency_publication.py"}
    words = {p: set(re.findall(r"\b[A-Za-z_]\w*\b", s)) for p, s in sources.items()}
    while True:
        names = {Path(p).stem for p in members}
        more = {p for p, identifiers in words.items() if names & identifiers}
        if more <= members:
            return sorted(members)
        members |= more


def staged_cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("freeze", "apply", "audit", "lower-progress"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "lower-progress":
        staged_lower_progress(args.root)
        args.output.write_text(json.dumps({"owner": "RecordStore.accepted_progress_phase", "retired": "prewarm_loop.progress_phase"}) + "\n")
        return
    sources = staged_sources(args.root, STAGED_BASE)
    selected = staged_symbols(sources)
    exports = staged_exports(sources)
    members = staged_population(sources)
    result = {"base": STAGED_BASE, "root": str(args.root), "symbols": {k: sorted(v) for k, v in selected.items()},
              "population": members, "whole_test_files": [p for p in members if p.startswith("tests/test_")],
              "scope": "source and AST only; collection and execution require PB"}
    if args.mode == "freeze":
        result["files"] = {p: staged_inventory(p, sources[p]) for p in members if p in sources}
    elif args.mode == "apply":
        for path in STAGED_MODULES.values():
            if (args.root / path).read_text() != sources[path]:
                raise SystemExit(f"apply requires the exact source pin for {path}")
        owner = []
        for module, path in STAGED_MODULES.items():
            sources[path], chunk = staged_extract(sources[path], module)
            if module == "produced_output":
                (args.root / "src/prismabuild/output_vocabulary.py").write_text('"""Produced-output physical namespaces, shared below orchestration."""\n' + chunk)
            else:
                owner.append(chunk)
        header = '''"""Staged-copy publication, ownership census and containment acts."""
from __future__ import annotations
import array
import bisect
from collections.abc import Callable, Mapping, Sequence
from contextlib import ExitStack, contextmanager, nullcontext, suppress
import contextvars
import errno
import hashlib
import json
import os
from pathlib import Path
import socket
import stat as statmod
import struct
import sys
import threading
import time
from prismabuild import core as pb, kept_reads, manifest_layout, movement_actions, pool
from prismabuild import prewarm_loop, reader_lease, residency_map, residency_plan, storage_tiers, tier_holdings
from prismabuild import output_vocabulary
from prismabuild import progress as pb_progress
from . import data_manifest as manifest_codec
from prismabuild.stage_marks import STAGE_MOVER_XATTR, STAGE_SOURCE_XATTR, source_identity
'''
        joined = header + "\n".join(owner)
        joined = re.sub(r"        from prismabuild.stage_release import (_claimed_paths|_unended_owner)\n", "", joined)
        joined = re.sub(r"    from prismabuild.produced_output import PRODUCED_OUTPUT_REF_SCHEMA_V1\n", "", joined)
        joined = joined.replace('PRODUCED_OUTPUT_REF_SCHEMA_V1', 'output_vocabulary.PRODUCED_OUTPUT_REF_SCHEMA_V1')
        for module, names in selected.items():
            for name in names:
                if module == "produced_output":
                    joined = joined.replace(f"produced_output.{name}", f"output_vocabulary.{name}")
        tokens = []
        for token in tokenize.generate_tokens(io.StringIO(joined).readline):
            if token.type == tokenize.NAME and token.string in STAGED_PUBLIC:
                token = token._replace(string=STAGED_PUBLIC[token.string])
            tokens.append(token)
        joined = tokenize.untokenize(tokens)
        seen = {}
        duplicate_lines = set()
        for node in ast.parse(joined).body:
            if isinstance(node, ast.FunctionDef) and node.name == "own_action_key":
                form = ast.dump(node, include_attributes=False)
                if node.name in seen:
                    if seen[node.name] != form:
                        raise SystemExit("the two own_action_key bodies differ")
                    duplicate_lines.update(range(node.lineno - 1, node.end_lineno))
                seen[node.name] = form
        joined = "".join(line for i, line in enumerate(joined.splitlines(keepends=True)) if i not in duplicate_lines)
        (args.root / STAGED_OWNER).write_text(joined)
        changed = [STAGED_OWNER, "src/prismabuild/output_vocabulary.py"]
        for path, source in sources.items():
            if path.startswith("evidence/") or path.startswith("tests/compat/"):
                continue
            local = next((m for m, p in STAGED_MODULES.items() if p == path), None)
            new = staged_references(source, selected, local=local, exports=exports)
            if new != (args.root / path).read_text():
                (args.root / path).write_text(new)
                changed.append(path)
        result["changed"] = changed
    else:
        result["files"] = {p: staged_inventory(p, (args.root / p).read_text()) for p in members if (args.root / p).exists()}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"mode": args.mode, "files": len(members), "output": str(args.output)}))


if __name__ == "__main__":
    staged_cli()
