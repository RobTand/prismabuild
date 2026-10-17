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
    return updated
