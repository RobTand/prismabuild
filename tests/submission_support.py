"""Explicit store fixture updates for migrated submission owners."""
from dataclasses import replace
from pathlib import Path
import pbrun


def patch_store(monkeypatch, *, client=pbrun, **changes):
    current = client.configured_store()
    if 'root' in changes:
        root = Path(changes['root'])
        changes.setdefault('queue_root', root / 'pb-queue')
        changes.setdefault('cas_root', root / 'cas')
    updated = replace(current, **changes)
    monkeypatch.setattr(client, 'configured_store', lambda: updated)
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


def refusal_value(refusal):
    """The original display-or-status assertion, read from explicit fields."""
    return refusal.reason or refusal.returncode
