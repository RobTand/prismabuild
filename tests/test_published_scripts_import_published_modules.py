"""Every tools module a published script imports is itself published (#1284).

The 2026-09-28 generation ``9098f84c872b`` shipped a ``tier_loop.py`` that
imports ``manifest_promotion`` without the module, and the tier role
crash-looped at import on dl380g10.  The publish import probe imports only
``prismabuild.pool``, so nothing refused it.  This reads every published
``tools/`` script's imports, at any depth, and requires each import that
names a module under ``tools/`` or ``tools/fleet/`` of this checkout to be in
the same publication manifest.
"""
from __future__ import annotations

import ast
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
import publish_runtime  # noqa: E402


def _imported_names(source: Path) -> set[str]:
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    return names


def _local_tool(name: str) -> bool:
    return any((ROOT / base / f"{name}.py").is_file()
               for base in ("tools/fleet", "tools"))


def test_every_imported_tools_module_is_published():
    manifest = publish_runtime._publication_manifest()
    missing = []
    for published in sorted(manifest):
        parts = published.split("/")
        if len(parts) != 2 or parts[0] != "tools" or not published.endswith(".py"):
            continue
        source = publish_runtime._source_for(published)
        for name in sorted(_imported_names(source)):
            if _local_tool(name) and f"tools/{name}.py" not in manifest:
                missing.append(f"{published} imports {name}")
    assert missing == []
