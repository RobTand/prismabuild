"""PrismaBuild -- deterministic action keys, immutable CAS, and remote dispatch.

Split out of the prismaquant repository on 2026-08-31 from
`origin/codex/prismabuild-v4-qualified-20260831`, where the implementation had
converged: the whole file set is byte-identical across the seven branches that
carry it, so there was no merge candidate to choose between.

The `prismaquant.prismabuild.*.vN` schema strings are deliberately NOT renamed.
They are baked into already-published receipts and campaign state, and the
identity of those receipts is the value they carry.  The namespace is history,
not a dependency -- this package imports nothing from prismaquant.
"""
from . import core  # noqa: F401
from .core import report_action_progress

__all__ = ["core", "report_action_progress"]
__version__ = "0.1.0"
