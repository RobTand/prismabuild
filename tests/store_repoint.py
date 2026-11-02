"""Point every loaded submission entry point at one store (#1931).

Kept apart from ``submission_support`` so that the conftest store fixture can
use it without importing ``pbrun`` for every test (#2582).
"""
from pathlib import Path
import sys
from types import ModuleType


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

