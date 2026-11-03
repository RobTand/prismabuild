"""Operator-local terminal endings only; never a saved or JSON export (#2726).

The public projection remains the only export view. See docs/public_evidence.md
for this view's root-only scope and deliberately public-only envelope inputs.
"""
from __future__ import annotations

import re as _re
import typing as _typing

from . import public_evidence as _public
from . import submission_types as _submission_types


_HOST = _re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$")


def operator_ending_view(record: object) -> dict[str, object]:
    """Return fresh public evidence plus validated root-only terminal details.

    Only an exact built-in dict within the public root size limit can supply
    extras. Already-marked public envelopes are revalidated as public-only.
    No child or list is inspected here; recursion belongs to the public view.
    """
    projected = _public.public_evidence_view(record)
    if type(record) is not dict or len(record) > _public._MAX_ITEMS:
        return projected

    # Derive membership from submission_types.SubmissionRequest.transport, not
    # another copy of the transport vocabulary. That module imports only stdlib
    # collections.abc, dataclasses, pathlib and typing, avoiding the heavy
    # submission/fleet_submit import closures. No I/O runs on this import path.
    transports = _typing.get_args(
        _typing.get_type_hints(_submission_types.SubmissionRequest)["transport"]
    )
    extras = {}
    for key, value in record.items():
        # Never lookup untrusted keys: even non-str keys can compare equal to
        # strings. Exact-type guards also prevent subclass methods being called.
        if type(key) is not str:
            continue
        if key == "schema" and type(value) is str and value == _public._SCHEMA:
            return projected
        if key == "host":
            if type(value) is str and _HOST.fullmatch(value) is not None:
                extras[key] = value
        elif key == "transport":
            if type(value) is str and value in transports:
                extras[key] = value
        elif key == "status":
            if type(value) is str and value == "superseded":
                extras[key] = value

    # Public-only envelope handling above guarantees an ordinary record here.
    # All containers in projected are already fresh; extras contain only str.
    projected["evidence"].update(sorted(extras.items()))
    return projected
