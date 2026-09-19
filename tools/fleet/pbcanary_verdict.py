"""Mechanical verdict for the pbcanary fleet canary (RobTand/prismabuild#688).

The canary driver (crew A) submits four legs through the real interfaces,
verifies their receipts, and hands this module a list of per-leg result
dicts. This module decides nothing at runtime: it applies the fail-closed
verdict semantics from the issue and returns an exit code plus a
machine-readable summary the driver prints.

Input contract (exact -- the driver imports this signature)::

    verdict(results) -> (exit_code: int, summary: dict)

``results`` is a list of per-leg dicts with keys ``leg``, ``ok``,
``reason`` and ``receipt_ref``. Extra keys are allowed and some are read:
leg-4 entries may carry digest fields (``digest``, ``envelope_digest``,
``sha256``, or paired ``digest_a``/``digest_b`` style keys) or an explicit
``envelope_equal`` bool so envelope equality is checked here, not trusted
from a flag alone. Since #690 an ok leg-4 entry must actually carry digest
evidence: a complete digest pair or a single-value digest key. An entry
with none (with or without ``envelope_equal: True``), or with only half a
pair, is treated as not-envelope-equal rather than passing vacuously.
Evidence must be an actual nonempty string: values are never coerced with
``str``, so null, empty, blank and non-string digest fields are malformed
and refuse, and such a field refuses even when another valid digest field
is present on the same entry -- a valid extra digest never masks a
malformed or partial pair.

Exit codes:

*   0 -- every leg (1-4) executed AND verified, including leg-4 envelope
    equality across the two boxes.
*   1 -- any leg failed its contract. The summary names the leg and the
    refusing check, and carries a ``stderr_message`` line the driver prints
    to stderr in that shape.
*   2 -- precondition refused (queue unreachable, generation root absent,
    runner misconfigured) or no leg results at all. "Did not test" is never
    reported as passed.

A precondition refusal anywhere dominates a leg failure: a run that never
became a valid test answers 2, not 1.
"""

from __future__ import annotations


#: Leg numbers the canary must execute and verify for an exit-0 verdict.
REQUIRED_LEGS = (1, 2, 3, 4)

#: Case-insensitive substrings of a leg's ``reason`` that mark the entry as
#: a refused precondition (exit 2) rather than a failed contract (exit 1).
PRECONDITION_MARKERS = (
    "precondition",
    "queue unreachable",
    "unreachable queue",
    "queue_unreachable",
    "generation root",
    "misconfigured",
    "not_run",
    "not run",
    "did not test",
    "did-not-test",
    "never executed",
    "no receipt",
)

#: Single-value digest keys read from leg-4 entries when the two boxes file
#: separate entries (e.g. ``4-sparky`` / ``4-sparklina``).
_DIGEST_KEYS = ("envelope_digest", "digest", "sha256", "artifact_digest")

#: Paired digest keys read from one leg-4 entry that already holds both
#: envelopes (each pair is (first, second)).
_DIGEST_PAIRS = (
    ("digest_a", "digest_b"),
    ("envelope_a", "envelope_b"),
    ("digest_sparky", "digest_sparklina"),
    ("sparky_digest", "sparklina_digest"),
)


def _leg_base(leg: object) -> int | None:
    """Reduce a leg label to its base number 1-4, or None for run-level labels."""
    text = str(leg).strip().lower().replace("_", "-")
    if text.startswith("leg-"):
        text = text[len("leg-"):]
    elif text.startswith("leg"):
        text = text[len("leg"):]
    text = text.lstrip("-")
    if text[:1].isdigit():
        return int(text[:1])
    return None


def _is_precondition(reason: object) -> bool:
    text = str(reason or "").lower()
    return any(marker in text for marker in PRECONDITION_MARKERS)


def _infer_check(reason: object, *, missing: bool = False) -> str:
    """Short check name for a failure, so the summary names leg AND check."""
    if missing:
        return "leg-executed"
    text = str(reason or "").lower()
    if _is_precondition(reason):
        return "precondition"
    if "envelope" in text:
        return "envelope-equality"
    if "digest" in text or "sha256" in text or "bitwise" in text:
        return "artifact-digest"
    if "receipt" in text or "cas" in text:
        return "receipt-verified"
    return "contract"


def _digest_evidence(value: object) -> str | None:
    """Return ``value`` when it is usable digest evidence, else None.

    Evidence is an actual nonempty, non-blank string. Values are never
    coerced with ``str``: ``None``, numbers, containers and empty or blank
    strings are malformed digest fields, not evidence (#690 follow-up).
    """
    if isinstance(value, str) and value.strip():
        return value
    return None


def _leg4_digests(entries: list[dict]) -> tuple[list[str], str | None]:
    """Collect comparable leg-4 digests; return (digests, inequality_detail).

    Fail closed on evidence, not only on equality (issue #690): an ok
    leg-4 entry that carries no digest evidence at all is treated as
    not-envelope-equal, because an empty comparison would pass vacuously.
    ``envelope_equal: False`` fails as before, and ``True`` is read but
    never substitutes for digests — the flag alone is not evidence.

    Evidence must be an actual nonempty string (issue #690 follow-up):
    null, empty, blank and non-string digest fields are malformed and
    refuse, never coerced with ``str``. A malformed pair, a partial pair
    or a malformed single key refuses even when another digest field on the
    same entry is valid, so a valid extra digest cannot mask it.
    """
    digests: list[str] = []
    for entry in entries:
        if isinstance(entry.get("envelope_equal"), bool):
            if not entry["envelope_equal"]:
                return [], "envelope_equal is False"
        evidence: str | None = None
        partial: str | None = None
        for first, second in _DIGEST_PAIRS:
            present = [name for name in (first, second) if name in entry]
            if len(present) == 2:
                first_value = _digest_evidence(entry[first])
                second_value = _digest_evidence(entry[second])
                if first_value is None or second_value is None:
                    return [], (
                        f"{first}/{second} digest pair carries no nonempty "
                        f"string evidence: {first}={entry[first]!r}, "
                        f"{second}={entry[second]!r}"
                    )
                if first_value != second_value:
                    return [], (
                        f"{first} {first_value!r} != {second} {second_value!r}"
                    )
                if evidence is None:
                    evidence = first_value
            elif len(present) == 1:
                if partial is None:
                    partial = present[0]
        for key in _DIGEST_KEYS:
            if key not in entry:
                continue
            value = _digest_evidence(entry[key])
            if value is None:
                return [], (
                    f"{key} carries no nonempty string digest evidence: "
                    f"{entry[key]!r}"
                )
            if evidence is None:
                evidence = value
        if partial is not None:
            return [], (
                f"incomplete digest pair: {partial!r} present without its "
                "partner"
            )
        if evidence is None:
            return [], (
                f"ok leg-4 entry {entry.get('leg', 'leg-4')!r} carries no "
                "digest evidence; envelope equality is unproven, not confirmed"
            )
        digests.append(evidence)
    if len(set(digests)) > 1:
        return [], f"box envelope digests differ: {sorted(set(digests))!r}"
    return digests, None


def verdict(results: list[dict]) -> tuple[int, dict]:
    """Apply the canary verdict semantics; see the module docstring."""
    legs: dict[str, dict] = {}
    if isinstance(results, list):
        for index, entry in enumerate(results):
            if isinstance(entry, dict):
                label = str(entry.get("leg", f"entry-{index}"))
                legs[label] = {
                    "ok": bool(entry.get("ok", False)),
                    "reason": entry.get("reason"),
                    "receipt_ref": entry.get("receipt_ref"),
                }
            else:
                legs[f"entry-{index}"] = {
                    "ok": False,
                    "reason": f"malformed result entry: {entry!r}",
                    "receipt_ref": None,
                }
    else:
        legs = {}

    def summary(exit_code: int, **extra: object) -> tuple[int, dict]:
        base: dict = {
            "exit_code": exit_code,
            "verified": exit_code == 0,
            "legs": legs,
            "failed_leg": None,
            "failed_check": None,
            "detail": None,
            "stderr_message": "",
        }
        base.update(extra)
        return exit_code, base

    if not isinstance(results, list) or not results:
        message = "pbcanary: precondition refused (run): no leg results: did not test"
        return summary(
            2,
            failed_leg="run",
            failed_check="precondition",
            detail="no leg results: did not test",
            stderr_message=message,
        )

    # Exit 2 first: a refused precondition means the run never became a test.
    for label, info in legs.items():
        if not info["ok"] and _is_precondition(info["reason"]):
            return summary(
                2,
                failed_leg=label,
                failed_check="precondition",
                detail=info["reason"],
                stderr_message=(
                    f"pbcanary: precondition refused (leg {label}): {info['reason']}"
                ),
            )

    # Exit 1: any leg whose contract check failed, naming leg and check.
    for label, info in legs.items():
        if not info["ok"]:
            check = _infer_check(info["reason"])
            return summary(
                1,
                failed_leg=label,
                failed_check=check,
                detail=info["reason"],
                stderr_message=(
                    f"pbcanary: leg {label} failed ({check}): {info['reason']}"
                ),
            )

    # Every entry so far is ok. Missing required legs still fail the contract.
    present = set()
    leg4_entries: list[dict] = []
    if isinstance(results, list):
        for entry in results:
            if not isinstance(entry, dict):
                continue
            base = _leg_base(entry.get("leg"))
            if base in REQUIRED_LEGS:
                present.add(base)
                if base == 4:
                    leg4_entries.append(entry)
    for required in REQUIRED_LEGS:
        if required not in present:
            label = f"leg-{required}"
            return summary(
                1,
                failed_leg=label,
                failed_check="leg-executed",
                detail=f"{label} never executed: no result entry",
                stderr_message=(
                    f"pbcanary: {label} failed "
                    f"(leg-executed): no result entry"
                ),
            )

    # Leg-4 envelope equality across the two boxes.
    _, inequality = _leg4_digests(leg4_entries)
    if inequality is not None:
        return summary(
            1,
            failed_leg="leg-4",
            failed_check="envelope-equality",
            detail=inequality,
            stderr_message=(
                f"pbcanary: leg-4 failed (envelope-equality): {inequality}"
            ),
        )

    return summary(
        0,
        stderr_message=(
            "pbcanary: verified -- legs 1-4 executed, receipts ok, "
            "leg-4 envelopes equal"
        ),
    )
