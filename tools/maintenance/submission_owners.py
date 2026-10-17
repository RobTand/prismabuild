"""Read-only locator migration for C14's moved source owners."""
import json
from pathlib import Path

REGISTRY = Path(__file__).with_suffix(".json")


def relocated_locator(value):
    moves = json.loads(REGISTRY.read_text())["moves"]
    for old, new in moves.items():
        if value == old or value.endswith(":" + old):
            return value[:-len(old)] + new
    return value


def baseline_at_submission_owners(value):
    """Move exact locators without rewriting the immutable baseline artifact."""
    result = dict(value)
    groups = {}
    for name, modules in value["same_name_helpers"].items():
        for module in modules:
            path, new_name = relocated_locator(module + "::" + name).split("::", 1)
            groups.setdefault(new_name, []).append(path)
    result["same_name_helpers"] = {name: sorted(set(paths)) for name, paths in groups.items()}
    result["same_name_distinct"] = []
    for row in value["same_name_distinct"]:
        by_name = {}
        for module in row["modules"]:
            path, name = relocated_locator(module + "::" + row["name"]).split("::", 1)
            by_name.setdefault(name, []).append(path)
        result["same_name_distinct"].extend({**row, "name": name, "modules": sorted(paths)}
            for name, paths in by_name.items())
    registered = {}
    for row in result["same_name_distinct"]:
        registered.setdefault(row["name"], set()).update(row["modules"])
    result["same_name_helpers"] = {name: paths for name, paths in result["same_name_helpers"].items()
        if len(set(paths) | registered.get(name, set())) >= 2}
    result["primitive_digest_sites"] = sorted(relocated_locator(item) for item in value["primitive_digest_sites"])
    result["near_duplicates"] = sorted(sorted(relocated_locator(item) for item in pair)
                                       for pair in value["near_duplicates"])
    result["must_differ"] = [{**row, "pair": sorted(relocated_locator(item) for item in row["pair"])}
                             for row in value["must_differ"]]
    return result


def quoted_owner(token, quoted):
    """Retarget a documented function and its quoted definition spelling."""
    module, _, symbol = token.partition(".")
    old = f"tools/fleet/{module}.py::{symbol}"
    moved = relocated_locator(old)
    if moved == old:
        return None, quoted
    path, new_name = moved.split("::", 1)
    quoted = quoted.replace("def " + symbol + "(", "def " + new_name + "(")
    return path, quoted
