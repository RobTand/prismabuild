"""Pytest collection policy, separate from counted outcome recording."""
from __future__ import annotations

from pathlib import Path
import os
import posixpath


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


def selection_plugin(spec):
    """Let pytest collect the original targets within PB's immutable file set."""
    import pytest

    if not isinstance(spec, dict) or set(spec) not in (
            {"files", "roots"}, {"files", "roots", "capabilities"}):
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
    roots = spec["roots"]
    if not isinstance(roots, list) or not roots:
        raise ValueError("pbtest collection selection needs original request roots")
    for row in roots:
        if not isinstance(row, list) or len(row) != 2 or row[1] not in ("file", "directory"):
            raise ValueError("pbtest collection request root is invalid")
        _relative_path(row[0])
    root = Path.cwd()
    assigned = {root / path: path for path in files}
    ancestors = {parent for path in assigned for parent in path.parents}

    class Selection:
        def __init__(self):
            self.ignored = set()
            self.visited = set()
            self.collection_root = root

        def pytest_configure(self, config):
            self.collection_root = config.rootpath

        @property
        def targets(self):
            return list(dict.fromkeys(
                path for path, kind in roots
                if any((name == path if kind == "file" else
                        path == "." or name.startswith(path + "/")) for name in files)))

        def report(self):
            return {"files": files, "ignored": sorted(self.ignored - self.visited)}

        @pytest.hookimpl(tryfirst=True)
        def pytest_ignore_collect(self, collection_path, config):
            # File membership is PB's rule. For eligible paths, return None
            # so pytest/project ignore hooks remain the sole population rule.
            if collection_path not in assigned and collection_path not in ancestors:
                return True
            return None

        @pytest.hookimpl(hookwrapper=True, tryfirst=True, specname="pytest_ignore_collect")
        def pytest_record_ignore(self, collection_path, config):
            outcome = yield
            if outcome.get_result():
                self.ignored.update(name for path, name in assigned.items()
                                    if path == collection_path or collection_path in path.parents)

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

    selection = Selection()
    if not selection.targets:
        raise ValueError("pbtest collection assigned files have no requested target")
    return selection
