"""Leg 1 -- bare CPU canary action (RobTand/prismabuild#688).

Trivial action, no container, no GPU: prints the sha256 of a fixed payload
to stdout. ``pbrun`` tees the command's output to the result log, so the
CAS receipt's ``result`` digest covers exactly these bytes. Exercises
submission -> admission -> claim -> execution -> CAS ``done/`` receipt.

The expected artifact is computed at build time from the same constant the
action hashes, so a corrupted expectation or a tampered artifact fails
``verify`` with the leg and the refusing check named.
"""

from __future__ import annotations

import hashlib

from . import LegBuildRefused  # noqa: F401  (re-exported for driver uniformity)

NAME = "leg-1"

#: Fixed payload every leg-1 action hashes. Bumped only with the leg's
#: ``PAYLOAD_VERSION`` so an old expectation can never verify a new action.
PAYLOAD = b"pbcanary-leg1:v1"

PAYLOAD_SHA256 = hashlib.sha256(PAYLOAD).hexdigest()

#: Exact stdout the action must produce: "<hex>\\n", nothing else.
ARTIFACT_EXACT = PAYLOAD_SHA256 + "\n"

DEMAND = {"cpu": 1, "mem_gb": 2}

WAIT_S = 300


def build() -> dict:
    """Seal the leg-1 action spec for the ``pbrun`` submission path."""
    python = (
        "import hashlib; "
        f"print(hashlib.sha256({PAYLOAD!r}).hexdigest())"
    )
    return {
        "name": NAME,
        "argv": ["python3", "-c", python],
        "demand": dict(DEMAND),
        "wait_s": WAIT_S,
        "container_image": None,
        "expected": {"leg": NAME, "artifact_exact": ARTIFACT_EXACT},
    }


def _result_of(receipt: object) -> tuple[object, str]:
    if not isinstance(receipt, dict):
        return None, f"{NAME} receipt-verified failed: receipt is not a mapping"
    inner = receipt.get("receipt")
    if not isinstance(inner, dict):
        return None, f"{NAME} receipt-verified failed: receipt['receipt'] absent"
    result = inner.get("result")
    if not isinstance(result, dict):
        return None, f"{NAME} receipt-verified failed: receipt result absent"
    return result, ""


def verify(receipt: object, expected: object) -> tuple[bool, str]:
    """Check the leg-1 artifact and its CAS result binding. No retries."""
    if not isinstance(expected, dict) or expected.get("leg") != NAME:
        return False, f"{NAME} artifact-digest failed: expected block names no leg-1 action"
    want = expected.get("artifact_exact")
    if not isinstance(want, str):
        return False, f"{NAME} artifact-digest failed: expected artifact text absent"
    if not isinstance(receipt, dict):
        return False, f"{NAME} receipt-verified failed: receipt is not a mapping"
    artifact = receipt.get("artifact")
    if not isinstance(artifact, str):
        return False, f"{NAME} artifact-digest failed: artifact text absent"
    if artifact != want:
        return False, (
            f"{NAME} artifact-digest failed: artifact != expected "
            f"(got {hashlib.sha256(artifact.encode()).hexdigest()}, "
            f"want {hashlib.sha256(want.encode()).hexdigest()})"
        )
    result, reason = _result_of(receipt)
    assert isinstance(result, dict) or reason
    if not isinstance(result, dict):
        return False, reason
    if result.get("sha256") != hashlib.sha256(artifact.encode()).hexdigest():
        return False, (
            f"{NAME} receipt-verified failed: CAS result sha256 "
            f"covers different bytes than the artifact"
        )
    if result.get("bytes") != len(artifact.encode()):
        return False, (
            f"{NAME} receipt-verified failed: CAS result byte count "
            f"differs from the artifact"
        )
    return True, f"{NAME} verified: artifact {PAYLOAD_SHA256[:12]} bound to CAS result"
