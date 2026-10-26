"""Pytest collection policy, separate from counted outcome recording."""
from __future__ import annotations

from pathlib import Path
import base64
import json
import math
import os
import posixpath
import statistics
import zlib


SELECTION_ENCODING_PREFIX = "pbtest-selection-z85-v1:"


def encode_selection(spec: dict) -> str:
    """Compress node maps while retaining ordinary selection JSON bytes."""
    if "node_groups" not in spec:
        return json.dumps(spec)
    from prismabuild.digest_primitives import compact_ascii_json_bytes
    raw = compact_ascii_json_bytes(spec)
    return SELECTION_ENCODING_PREFIX + base64.b85encode(zlib.compress(raw, 9)).decode("ascii")


def decode_selection(value: str) -> dict:
    """Read current encoded selections and retained JSON selections."""
    if value.startswith(SELECTION_ENCODING_PREFIX):
        encoded = value[len(SELECTION_ENCODING_PREFIX):]
        value = zlib.decompress(base64.b85decode(encoded)).decode("utf-8")
    return json.loads(value)


def ignored_named_paths(config) -> set[str]:
    """The positional arguments a loaded conftest's collect_ignore excludes."""

    import fnmatch

    conftests = []
    for plugin in config.pluginmanager.get_plugins():
        source = getattr(plugin, "__file__", None)
        if source and Path(source).name == "conftest.py":
            conftests.append((Path(source).parent, plugin))
    ignored: set[str] = set()
    for arg in config.args:
        name = str(arg).split("::", 1)[0]
        path = (config.invocation_params.dir / name).resolve()
        for folder, module in conftests:
            if folder != path.parent and folder not in path.parents:
                continue
            for entry in getattr(module, "collect_ignore", None) or ():
                if (folder / str(entry)).resolve() == path:
                    ignored.add(str(arg))
            for pattern in getattr(module, "collect_ignore_glob", None) or ():
                if fnmatch.fnmatch(str(path), str(folder / str(pattern))):
                    ignored.add(str(arg))
    return ignored


def _relative_path(value):
    if (not isinstance(value, str) or not value or "\0" in value
            or value.startswith("/") or posixpath.normpath(value) != value
            or ".." in value.split("/")):
        raise ValueError("pbtest collection needs canonical checkout-relative paths")
    return value


def checkout_nodeid(nodeid: str, rootdir: str = ".") -> str:
    """Resolve a pytest node ID against its checkout-relative rootdir."""
    filename, separator, item = nodeid.partition("::")
    return posixpath.normpath(posixpath.join(rootdir, filename)) + separator + item


def node_group_spec(value, files):
    """Validate node partition declarations carried by a sealed selection."""
    if not isinstance(value, dict) or not value:
        raise ValueError("pbtest node groups must be a nonempty object")
    for filename, group in value.items():
        _relative_path(filename)
        if filename not in files:
            raise ValueError("pbtest node group names an unassigned file")
        if (not isinstance(group, dict) or set(group) != {"index", "count", "durations"}
                or type(group["count"]) is not int or group["count"] < 2
                or type(group["index"]) is not int
                or not 0 <= group["index"] < group["count"]
                or not isinstance(group["durations"], dict)):
            raise ValueError("pbtest node group index, count or durations are invalid")
        for nodeid, seconds in group["durations"].items():
            if (not isinstance(nodeid, str) or "\0" in nodeid
                    or nodeid.split("::", 1)[0] != filename
                    or type(seconds) not in (int, float)
                    or not math.isfinite(seconds) or seconds < 0):
                raise ValueError("pbtest node group duration is invalid")
    return value


def partition_nodes(nodeids: list[str], count: int,
                    durations: dict[str, float]) -> list[list[str]]:
    """Partition the current population using only sealed duration estimates."""
    buckets: list[list[str]] = [[] for _ in range(count)]
    known = {node: durations[node] for node in nodeids if node in durations}
    if not known:
        for index, node in enumerate(sorted(nodeids)):
            buckets[index % count].append(node)
        return buckets
    default = statistics.median(known.values())
    predicted = {node: known.get(node, default) for node in nodeids}
    totals = [0.0] * count
    for node in sorted(nodeids, key=lambda node: (-predicted[node], node)):
        index = min(range(count),
                    key=lambda index: (totals[index], len(buckets[index]), index))
        buckets[index].append(node)
        totals[index] += predicted[node]
    return buckets


def selection_plugin(spec):
    """Let pytest collect the original targets within PB's immutable file set."""
    import pytest

    if (not isinstance(spec, dict) or not {"files", "roots"} <= set(spec)
            or set(spec) - {"files", "roots", "capabilities", "node_groups"}):
        raise ValueError("pbtest collection selection fields are invalid")
    if "capabilities" in spec:
        # The sealed capability selection (#1495) is not collection
        # evidence; its full validation is the digest preflight's, which
        # runs before pytest and refuses a malformed seal by name.  Here a
        # non-dict is refused so an unshapeable extra field can never ride
        # into a shard that then ignores it.
        if not isinstance(spec["capabilities"], dict):
            raise ValueError("pbtest collection selection capabilities "
                             "must be an object")
    if not isinstance(spec["files"], list) or not spec["files"]:
        raise ValueError("pbtest collection selection needs assigned files")
    files = [_relative_path(path) for path in spec["files"]]
    if len(set(files)) != len(files):
        raise ValueError("pbtest collection selection repeats an assigned file")
    groups = node_group_spec(spec["node_groups"], files) if "node_groups" in spec else {}
    roots = spec["roots"]
    if not isinstance(roots, list) or not roots:
        raise ValueError("pbtest collection selection needs original request roots")
    for row in roots:
        if not isinstance(row, list) or len(row) != 2 or row[1] not in ("file", "directory"):
            raise ValueError("pbtest collection request root is invalid")
        _relative_path(row[0])
    root = Path.cwd()
    assigned = {root / path: path for path in files}
    # Index lexical containment once, including exact files. Ignore recording
    # then visits only matching assigned names, never the entire assignment.
    descendants = {}
    for path, name in assigned.items():
        for container in (path, *path.parents):
            descendants.setdefault(container, []).append(name)

    class Selection:
        def __init__(self):
            self.ignored = set()
            self.visited = set()
            self.collection_root = root
            self.node_rosters = {filename: [] for filename in groups}

        def pytest_configure(self, config):
            self.collection_root = config.rootpath

        @property
        def targets(self):
            return list(dict.fromkeys(
                path for path, kind in roots
                if any((name == path if kind == "file" else
                        path == "." or name.startswith(path + "/")) for name in files)))

        def report(self):
            result = {"files": files, "ignored": sorted(self.ignored - self.visited)}
            if groups:
                result.update(node_groups=groups, node_rosters=self.node_rosters)
            return result

        @pytest.hookimpl(tryfirst=True)
        def pytest_ignore_collect(self, collection_path, config):
            # File membership is PB's rule. For eligible paths, return None
            # so pytest/project ignore hooks remain the sole population rule.
            if collection_path not in descendants:
                return True
            return None

        @pytest.hookimpl(hookwrapper=True, tryfirst=True, specname="pytest_ignore_collect")
        def pytest_record_ignore(self, collection_path, config):
            outcome = yield
            if outcome.get_result():
                self.ignored.update(descendants.get(collection_path, ()))

        def pytest_collectreport(self, report):
            relative = os.path.relpath(
                self.collection_root / report.nodeid.split("::", 1)[0], root)
            if relative in files:
                self.visited.add(relative)

        @pytest.hookimpl(hookwrapper=True, tryfirst=True)
        def pytest_collection_modifyitems(self, session, config, items):
            yield
            foreign = [item.nodeid for item in items if item.path not in assigned]
            if foreign:
                raise pytest.UsageError("pbtest collection escaped its assigned files: "
                                        + ", ".join(foreign[:10]))
            if groups:
                nodes = {item: checkout_nodeid(item.nodeid,
                         os.path.relpath(config.rootpath, root)) for item in items}
                for filename in groups:
                    self.node_rosters[filename] = sorted(
                        node for node in nodes.values() if node.split("::", 1)[0] == filename)
                selected = set()
                for filename, group in groups.items():
                    selected.update(partition_nodes(self.node_rosters[filename],
                        group["count"], group["durations"])[group["index"]])
                excluded = [item for item in items
                            if nodes[item].split("::", 1)[0] in groups
                            and nodes[item] not in selected]
                excluded_set = set(excluded)
                items[:] = [item for item in items if item not in excluded_set]
                if excluded:
                    config.hook.pytest_deselected(items=excluded)

    selection = Selection()
    if not selection.targets:
        raise ValueError("pbtest collection assigned files have no requested target")
    return selection
