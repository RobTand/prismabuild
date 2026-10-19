"""PrismaBuild -- deterministic action keys, immutable CAS, and remote dispatch.

Split out of its first client's repository on 2026-08-31 from the branch
`codex/prismabuild-v4-qualified-20260831`, where the implementation had
converged: the whole file set is byte-identical across the seven branches that
carry it, so there was no merge candidate to choose between.

The legacy `prismaquant.prismabuild.*.vN` schema strings are deliberately NOT renamed.
They are baked into already-published receipts and campaign state, and the
identity of those receipts is the value they carry.  The namespace is history,
not a dependency -- this package imports nothing from any client.
"""
from importlib import import_module as _import_module

from . import progress  # noqa: F401
from .progress import commit, report_action_progress

__all__ = [
    "core", "progress", "residency_map", "commit", "report_action_progress",
]
__version__ = "0.1.0"


def __getattr__(name: str):
    if name in ("core", "residency_map"):
        module = _import_module(f".{name}", __name__)
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
