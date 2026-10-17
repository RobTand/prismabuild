#!/usr/bin/env python3
"""Follow tier owner arguments and fixture returns across source module imports."""
import argparse
import ast
from collections import defaultdict
import json
from pathlib import Path
import runpy
import subprocess


def module_name(path):
    for prefix in ("src/", "tools/fleet/", "tests/"):
        if path.startswith(prefix):
            path = path[len(prefix):]
            break
    name = path.removesuffix(".py").replace("/", ".")
    return name.removesuffix(".__init__")


def imported_names(tree, module):
    names = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names[alias.asname or alias.name.split(".")[0]] = (
                    alias.name if alias.asname else alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                parent = module.split(".")[:-node.level]
                base = ".".join([*parent, *([base] if base else [])])
            for alias in node.names:
                names[alias.asname or alias.name] = ".".join(filter(None, (base, alias.name)))
    return names


def resolve(node, names, module):
    if isinstance(node, ast.Name):
        return names.get(node.id, module + "." + node.id)
    if isinstance(node, ast.Attribute):
        parent = resolve(node.value, names, module)
        return parent + "." + node.attr if parent else None
    return None


def exported_shape(shape, fields, seen=frozenset()):
    if isinstance(shape, dict):
        if "@class" in shape:
            name = shape["@class"]
            if name in seen:
                return False
            found = exported_shape(fields.get(name, {}), fields, seen | {name})
            return {"@fields": found} if found else False
        found = {key: exported_shape(value, fields, seen) for key, value in shape.items()}
        return {key: value for key, value in found.items() if value}
    if isinstance(shape, tuple):
        return tuple(exported_shape(value, fields, seen) for value in shape)
    return shape


def analyze(root):
    move = runpy.run_path(str(root / "docs/evidence/c10/migrate_source.py"))
    paths = subprocess.check_output(
        ["git", "ls-files", "src", "tools", "tests"], cwd=root, text=True).splitlines()
    units, targets = {}, {}
    for path in paths:
        if not path.endswith(".py"):
            continue
        tree = ast.parse((root / path).read_text(), filename=path)
        module = module_name(path)
        unit = dict(tree=tree, module=module, names=imported_names(tree, module),
                    seeds=defaultdict(set), external_returns={}, generation=0)
        units[path] = unit
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                targets[module + "." + node.name] = (path, node, None)
            elif isinstance(node, ast.ClassDef):
                for method in node.body:
                    if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        targets[module + "." + node.name + "." + method.name] = (path, method, None)
                        if method.name == "__init__":
                            targets[module + "." + node.name] = (path, method, node.name)
    bindings, passes = {}, 0
    changed = True
    while changed:
        changed, passes = False, passes + 1
        if passes > len(units):
            raise RuntimeError("tier argument propagation did not converge")
        for unit in units.values():
            stamp = (tuple((name, tuple(sorted(values))) for name, values in sorted(unit["seeds"].items())),
                     repr(unit["external_returns"]))
            if unit.get("stamp") == stamp:
                continue
            unit["scopes"], unit["aliases"], unit["returns"], unit["fields"] = move["caller_aliases"](
                unit["tree"], unit["seeds"], unit["external_returns"], details=True)
            unit["stamp"] = stamp
        for path, unit in units.items():
            scopes, aliases = unit["scopes"], unit["aliases"]
            for call in ast.walk(unit["tree"]):
                if not isinstance(call, ast.Call):
                    continue
                target_name = resolve(call.func, unit["names"], unit["module"])
                target = targets.get(target_name)
                if target is None:
                    continue
                target_path, function, constructed = target
                target_unit = units[target_path]
                returned = ({"@class": constructed} if constructed else
                            target_unit["returns"].get(function.name, False))
                returned = exported_shape(returned, target_unit["fields"])
                spelling = ast.unparse(call.func)
                if returned and unit["external_returns"].get(spelling) != returned:
                    unit["external_returns"][spelling] = returned
                    changed = True
                params = [*function.args.posonlyargs, *function.args.args]
                if params and params[0].arg in {"self", "cls"}:
                    params = params[1:]
                paired = [(parameter.arg, argument)
                          for parameter, argument in zip(params, call.args)]
                paired.extend((keyword.arg, keyword.value) for keyword in call.keywords
                              if keyword.arg is not None)
                valid = {parameter.arg for parameter in
                         (*params, *function.args.kwonlyargs)}
                for parameter, argument in paired:
                    tier = move["bound_tier"](argument, aliases[scopes[call]])
                    if not tier or parameter not in valid:
                        continue
                    key = (path, call.lineno, target_name, parameter)
                    bindings[key] = dict(path=path, line=call.lineno,
                                         target=target_name, parameter=parameter)
                    seeded = target_unit["seeds"][function.name]
                    if parameter not in seeded:
                        seeded.add(parameter)
                        changed = True
    owner = units["src/prismabuild/tier_holdings.py"]["tree"]
    owner = next(n for n in owner.body if isinstance(n, ast.ClassDef) and n.name == "TierLedger")
    known = {n.name for n in owner.body if isinstance(n, ast.FunctionDef)}
    known.update(n.attr for n in ast.walk(owner)
                 if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)
                 and n.value.id == "self" and isinstance(n.ctx, ast.Store))
    unknown, aliases = [], {}
    for path, unit in units.items():
        aliases[path] = {getattr(scope, "name", "module"): sorted(values)
                         for scope, values in unit["aliases"].items() if values}
        if path == "src/prismabuild/tier_holdings.py":
            continue
        for node in ast.walk(unit["tree"]):
            if not isinstance(node, ast.Attribute):
                continue
            bound = move["bound_tier"](node.value, unit["aliases"][unit["scopes"][node]])
            if bound and node.attr not in known:
                unknown.append(dict(path=path, line=node.lineno, name=node.attr))
    return dict(source_only=True, passes=passes, files=len(units),
                bindings=list(bindings.values()), unknown_tier_attributes=unknown,
                aliases=aliases,
                seeds={path: {name: sorted(values) for name, values in unit["seeds"].items()}
                       for path, unit in units.items() if unit["seeds"]},
                external_returns={path: unit["external_returns"] for path, unit in units.items()
                                  if unit["external_returns"]})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = analyze(args.root)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({key: report[key] for key in ("passes", "files", "unknown_tier_attributes")}))


if __name__ == "__main__":
    main()
