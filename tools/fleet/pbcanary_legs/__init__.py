"""Sealed action payloads for the pbcanary fleet canary (RobTand/prismabuild#688).

Crew A owns legs 1-2; legs 3-4 land from crew B in ``leg3.py`` / ``leg4.py``
under this same contract. The driver (``tools/fleet/pbcanary.py``) discovers
leg modules by name and calls exactly two functions on each.

INTERFACE CONTRACT (exact -- the driver and the integrator depend on it)::

    build() -> dict
    verify(receipt, expected) -> (ok: bool, reason: str)

``build()`` returns an action spec for the existing ``pbrun`` submission
path, with keys:

* ``name``: ``"leg-1"`` .. ``"leg-4"`` (matches the verdict's leg labels).
* ``argv``: command words run after ``pbrun --``. No container or GPU
  flags live here beyond what the command itself spells (leg 2's
  ``docker run --gpus all``); placement demand lives in ``demand``.
* ``demand``: resource demand mapping forwarded as one comma-separated
  ``pbrun --demand k=v,k=v`` aggregate (e.g. ``{"cpu": 1, "mem_gb": 2}``;
  leg 2 adds ``"gpu": 1``), sorted by name and omitted entirely when empty
  so pbrun's own defaults stand.
* ``wait_s``: bounded wait budget for this leg (300 s CPU / 600 s GPU).
* ``container_image``: ``None`` for bare legs; the digest-pinned
  ``name@sha256:<hex>`` ref for container legs. A container leg whose
  image is absent or not digest-pinned raises :class:`LegBuildRefused`
  instead of sealing a floating tag.
* ``expected``: opaque-to-the-driver mapping handed back to
  ``verify`` as ``expected``.

``verify(receipt, expected)`` checks one executed leg's outcome and returns
``(True, <evidence line>)`` or ``(False, <leg + refusing check>)``. It
decides nothing and retries nothing. ``receipt`` is a mapping the driver
assembles from the verified CAS receipt plus the result blob it covers::

    {"action_key": <submitted key>,
     "receipt": {"result": {"sha256": ..., "bytes": ...}, ...},
     "artifact": <result-blob text>}

``expected`` is the ``expected`` sub-dict from this leg's own ``build()``.
Failure reasons name the leg and the refusing check, and avoid the
verdict's precondition markers (``pbcanary_verdict.PRECONDITION_MARKERS``)
unless the leg truly never became a test, so a corrupted digest reads as
exit 1 (failed contract) rather than exit 2 (did not test).
"""

from __future__ import annotations


class LegBuildRefused(Exception):
    """``build()`` could not seal its spec: a precondition the operator fixes.

    The driver records this as a did-not-test leg entry (exit 2), never as
    a passed or failed contract.
    """
