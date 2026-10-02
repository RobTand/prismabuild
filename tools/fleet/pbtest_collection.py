"""Pytest collection policy, separate from counted outcome recording."""
from __future__ import annotations

from pathlib import Path


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

