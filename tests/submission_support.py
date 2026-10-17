"""Explicit store fixture updates for migrated submission owners."""
from dataclasses import replace
from pathlib import Path
import pbrun


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
    import sys
    for module in list(sys.modules.values()):
        filename = getattr(module, '__file__', '') or ''
        if Path(filename).parent.name not in {'fleet', 'tools'}:
            continue
        if hasattr(module, 'configured_store'):
            monkeypatch.setattr(module, 'configured_store', lambda: store)
        if hasattr(module, 'store_for'):
            monkeypatch.setattr(module, 'store_for', lambda *args, **kwargs: store)


def refusal_value(refusal):
    """The original display-or-status assertion, read from explicit fields."""
    return refusal.reason or refusal.returncode
