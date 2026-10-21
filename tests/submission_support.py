"""Explicit store fixture updates for migrated submission owners."""
from dataclasses import replace
from pathlib import Path
import sys
from types import ModuleType

import pbrun


def repoint_stores(monkeypatch, *, configured_store, store_for):
    """Point every loaded submission entry point at one store for this test.

    The conftest store fixture and ``patch_store`` both go through here, so a
    test's own store reaches every module the fixture pointed (#1931): each
    module in ``sys.modules`` and each module held only as another module's
    attribute (a fleet tool loaded under a private name).
    """
    modules = list(sys.modules.values())
    nested = [value for module in modules if module is not None
              for value in vars(module).values() if isinstance(value, ModuleType)]
    for module in {id(value): value for value in [*modules, *nested] if value is not None}.values():
        filename = getattr(module, "__file__", "") or ""
        if Path(filename).parent.name == "fleet" and hasattr(module, "configured_store"):
            monkeypatch.setattr(module, "configured_store", configured_store)
        if getattr(module, "store_for", None) is not None:
            monkeypatch.setattr(module, "store_for", store_for)


def patch_store(monkeypatch, *, client=pbrun, **changes):
    current = (client.configured_store() if hasattr(client, "configured_store")
               else client.store_for(client.__file__))
    if 'root' in changes:
        root = Path(changes['root'])
        changes.setdefault('queue_root', root / 'pb-queue')
        changes.setdefault('cas_root', root / 'cas')
    updated = replace(current, **changes)
    if hasattr(client, 'configured_store'):
        monkeypatch.setattr(client, 'configured_store', lambda: updated)
    else:
        monkeypatch.setattr(client, 'store_for', lambda *args, **kwargs: updated)
    sync_frontends(monkeypatch, updated)
    return updated


def sync_frontends(monkeypatch, store):
    repoint_stores(monkeypatch, configured_store=lambda: store,
                   store_for=lambda *args, **kwargs: store)


def refusal_value(refusal):
    """The original display-or-status assertion, read from explicit fields."""
    return refusal.reason or refusal.returncode
