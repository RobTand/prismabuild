"""pbcanary leg 4 — two-box fanout + join (PB #688).

One action pinned ``--tag sparky``, one pinned ``--tag sparklina``
(deterministic placement, not work-stealing: both boxes must be proven
alive — a down box is a CORRECT failure the nightly run catches). Each
computes the digest of the same shared inputs; the canary requires the
two envelopes' digests bitwise-equal. Miniaturizes the distributed
campaign's core pattern.

Driver flow (the driver owns waiting, placement records, and CAS paths;
this module owns the fixed spec and the mechanical verdict):

  1. ``spec = build()`` — fixed spec; refuses unless the recomputed
     input digest matches the pinned constant below.
  2. Driver submits both ``spec["actions"]`` rows (each: its ``--tag``,
     ``--timeout-s 300 --wait-s 300 --deterministic``, argv
     ``python3 tools/fleet/pbcanary_legs/leg4.py --run-action``, env
     ``PBCANARY_LEG4_TAG`` + ``PBCANARY_LEG4_INPUTS``; the driver injects
     its own ``--priority`` once — issue #690).
  3. Each action prints one canonical-JSON envelope line (see
     ``LEG4_SCHEMA``) and exits 0. The envelope carries NO hostname, NO
     timestamp, NO tag, NO paths — only the shared inputs and their
     digest — so bitwise equality across boxes is achievable; anything
     per-box in the envelope would fail the leg by construction, and
     ``run_action`` is written so it cannot add any.
  4. Driver calls ``verify(receipt_a, receipt_b, spec["expected"])``
     with the sparky receipt first. Exit 1 names the refusing check
     (including which box's envelope refused); exit 2 is the driver's
     precondition refusal.

Interface contract: ``build() -> dict`` as legs 1-2; ``verify`` takes
BOTH envelopes — ``verify(receipt_a, receipt_b, expected) ->
(ok, reason)`` — and requires bitwise equality. No runtime
intelligence: fixed inputs, fixed expectations, fixed check order.

Placement proof split (documented so the driver and the verdict crew
need no guesswork): this module proves both envelopes are byte-identical
and match the pinned digest. The driver proves the two receipts came
from different boxes (distinct ``--tag`` submissions, distinct done/
records); tag/box identity is deliberately NOT in the envelope.

Stdlib only: no PrismaBuild imports, so the module runs unmodified both
in the driver and inside the sealed worker actions.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, str(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))
try:
    from pbcanary_legs.common import (
        canonical_json,
        deterministic_bytes,
        sha256_hex,
    )
except ImportError:  # Worker runs the file directly: sibling import.
    from common import (  # type: ignore[no-redef]
        canonical_json,
        deterministic_bytes,
        sha256_hex,
    )

LEG4_NAME = "leg-4"
LEG4_SCHEMA = "prismabuild.pbcanary.leg4.v1"

#: Deterministic placement. ``sparky`` is the sparky box's hostname tag;
#: ``sparklina``/``gx10-6b77`` are the tags the second GB10 box's loops
#: announce (tools/fleet/fleet_boxes.json). A tag that matches no live
#: box leaves the action queued until ``wait_s`` — that is the CORRECT
#: down-box failure, surfaced by the driver as a failed leg, not a pass.
LEG4_TAGS = ("sparky", "sparklina")

LEG4_WAIT_S = 300  # CPU-leg wait budget from #688.
LEG4_TIMEOUT_S = 300
LEG4_PRIORITY = -10  # Documented default; the driver's --priority governs
# the submission (issue #690 dedupe: specs do not pin --priority).

#: Shared inputs: (seed-suffix, size). Small on purpose — leg 4 tests
#: fanout+join agreement, not volume (volume is leg 3's job).
LEG4_INPUT_SIZES = (65536, 65536, 65536)

#: Fixed expectation (PB #688: "a fixed expected outcome"). ``build()``
#: recomputes from the seeds and refuses on mismatch.
LEG4_EXPECTED_INPUT_DIGEST = (
    "884c2b9e2c9309a13cbc07c24e14695333ced791fd816df205661faf9570d1fc"
)

_ACTION_ENV_TAG = "PBCANARY_LEG4_TAG"
_ACTION_ENV_INPUTS = "PBCANARY_LEG4_INPUTS"


def _seed(index: int) -> bytes:
    return b"/leg4/input%d" % index


def _inputs_descriptor() -> list[dict]:
    return [
        {"seed_hex": _seed(i).hex(), "bytes": n}
        for i, n in enumerate(LEG4_INPUT_SIZES)
    ]


def input_digest() -> str:
    """Digest of the shared inputs: SHA-256 over their concatenation."""
    return sha256_hex(
        b"".join(deterministic_bytes(_seed(i), n) for i, n in enumerate(LEG4_INPUT_SIZES))
    )


def _check_pinned() -> None:
    if input_digest() != LEG4_EXPECTED_INPUT_DIGEST:
        raise RuntimeError(
            "leg4: pinned input-digest mismatch: "
            "regenerate LEG4_EXPECTED_INPUT_DIGEST, do not ship this"
        )


def build() -> dict:
    """Return the fixed leg-4 submission/verification spec.

    Pure data plus the pinned digest; reads nothing from the fleet and
    takes no arguments. ``actions[0]`` is sparky, ``actions[1]`` is
    sparklina; ``verify`` expects the receipts in that order.
    """
    _check_pinned()
    inputs = _inputs_descriptor()
    inputs_json = canonical_json(inputs)
    actions = []
    for tag in LEG4_TAGS:
        actions.append(
            {
                "tag": tag,
                "argv": ["python3", "tools/fleet/pbcanary_legs/leg4.py", "--run-action"],
                "env": {
                    _ACTION_ENV_TAG: tag,
                    _ACTION_ENV_INPUTS: inputs_json,
                },
                "pbrun_flags": [
                    "--tag",
                    tag,
                    "--timeout-s",
                    str(LEG4_TIMEOUT_S),
                    "--wait-s",
                    str(LEG4_WAIT_S),
                    "--deterministic",
                    "--retry-safe",
                    "--max-attempts",
                    "1",
                ],
            }
        )
    return {
        "name": LEG4_NAME,
        "schema": LEG4_SCHEMA,
        "wait_s": LEG4_WAIT_S,
        "timeout_s": LEG4_TIMEOUT_S,
        "priority": LEG4_PRIORITY,
        "inputs": inputs,
        "expected": {"input_digest": LEG4_EXPECTED_INPUT_DIGEST},
        "actions": actions,
    }


def run_action() -> int:
    """Worker entrypoint: digest the shared inputs, print the envelope.

    The envelope is exactly ``{"bytes":[...], "input_digest":...,
    "inputs":[...], "leg":4, "schema":...}`` canonical-encoded — no
    timestamp, hostname, tag, or path. Exits 0 after printing; any
    failure to parse inputs exits 1 with an ``ok:false`` envelope so
    ``verify`` names the check instead of seeing an empty receipt.
    """
    try:
        raw = os.environ.get(_ACTION_ENV_INPUTS, "")
        inputs = json.loads(raw)
        if not isinstance(inputs, list) or not inputs:
            raise ValueError("inputs must be a non-empty list")
        parts = []
        echo = []
        for entry in inputs:
            if not isinstance(entry, dict):
                raise ValueError("each input must be an object")
            seed = bytes.fromhex(entry["seed_hex"])
            size = entry["bytes"]
            if not isinstance(size, int) or size < 0:
                raise ValueError("each input needs a non-negative byte count")
            parts.append(deterministic_bytes(seed, size))
            echo.append({"seed_hex": entry["seed_hex"], "bytes": size})
        envelope: dict = {
            "schema": LEG4_SCHEMA,
            "leg": 4,
            "inputs": echo,
            "input_digest": sha256_hex(b"".join(parts)),
        }
    except Exception as exc:  # Fail-closed: report, never traceback-only.
        envelope = {
            "schema": LEG4_SCHEMA,
            "leg": 4,
            "inputs": [],
            "input_digest": "",
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
        sys.stdout.write(canonical_json(envelope) + "\n")
        sys.stdout.flush()
        return 1
    sys.stdout.write(canonical_json(envelope) + "\n")
    sys.stdout.flush()
    return 0


def _extract_envelope(receipt: object) -> tuple[str | None, dict | None, str]:
    """Find the leg-4 envelope in ``receipt``.

    Returns ``(raw, parsed, where)``; ``raw`` is None when the receipt
    carries only a parsed envelope — leg 4 REQUIRES the raw bytes for
    the bitwise-equality check. Accepted locations, in order:
    ``receipt["envelope"]``, ``receipt["stdout"]``,
    ``receipt["detail"]["stdout"]``. Stdout is scanned for the last line
    that parses as a JSON object with this leg's schema marker.
    """
    if not isinstance(receipt, dict):
        return None, None, ""
    candidate = receipt.get("envelope")
    if isinstance(candidate, str):
        try:
            parsed = json.loads(candidate)
        except ValueError:
            return None, None, "receipt[envelope]"
        if isinstance(parsed, dict) and parsed.get("schema") == LEG4_SCHEMA:
            return candidate.strip(), parsed, "receipt[envelope]"
        return None, None, "receipt[envelope]"
    if isinstance(candidate, dict) and candidate.get("schema") == LEG4_SCHEMA:
        return None, candidate, "receipt[envelope]"
    for where in ("stdout", "detail.stdout"):
        node: object = receipt
        for key in where.split("."):
            node = node.get(key) if isinstance(node, dict) else None
        if not isinstance(node, str):
            continue
        found = None
        for line in node.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                parsed = json.loads(line)
            except ValueError:
                continue
            if isinstance(parsed, dict) and parsed.get("schema") == LEG4_SCHEMA:
                found = (line, parsed)
        if found is not None:
            return found[0], found[1], f"receipt[{where}]"
    return None, None, ""


def _returncode(receipt: dict) -> int | None:
    for where in ("returncode", "detail.returncode", "action_returncode"):
        node: object = receipt
        for key in where.split("."):
            node = node.get(key) if isinstance(node, dict) else None
        if isinstance(node, bool):
            continue
        if isinstance(node, int):
            return node
    return None


def verify(receipt_a: object, receipt_b: object, expected: object) -> tuple[bool, str]:
    """Verify both leg-4 receipts: bitwise-equal envelopes, pinned digest.

    ``receipt_a`` is the sparky (``actions[0]``) receipt, ``receipt_b``
    the sparklina (``actions[1]``) receipt; ``expected`` is
    ``build()["expected"]``. Fail-closed, in fixed order: receipt
    shapes, returncodes, raw envelope presence on each side, bitwise
    equality of the two envelope byte strings, digest match. The first
    refusal names the leg, the side, and the check. A down box surfaces
    here as a missing/failed envelope — a CORRECT failure, never a pass.
    """
    names = LEG4_TAGS  # sparky receipt first, sparklina second (see build()).
    for side, receipt in (("a", receipt_a), ("b", receipt_b)):
        if not isinstance(receipt, dict):
            return False, f"leg4: receipt {side} is not an object"
    assert isinstance(receipt_a, dict) and isinstance(receipt_b, dict)
    if not isinstance(expected, dict):
        return False, "leg4: expected spec is not an object"
    for side, name, receipt in (("a", names[0], receipt_a), ("b", names[1], receipt_b)):
        rc = _returncode(receipt)
        if rc is not None and rc != 0:
            return False, f"leg4: {name} action returncode {rc} != 0"
    raw_a, parsed_a, where_a = _extract_envelope(receipt_a)
    raw_b, parsed_b, where_b = _extract_envelope(receipt_b)
    if raw_a is None or parsed_a is None:
        return False, f"leg4: {names[0]} envelope bytes absent ({where_a or 'nowhere'})"
    if raw_b is None or parsed_b is None:
        return False, f"leg4: {names[1]} envelope bytes absent ({where_b or 'nowhere'})"
    if raw_a != raw_b:
        return False, (
            f"leg4: envelopes not bitwise-equal "
            f"({names[0]} via {where_a} vs {names[1]} via {where_b})"
        )
    want = expected.get("input_digest")
    if not isinstance(want, str):
        return False, "leg4: expected spec lacks input_digest"
    if parsed_a.get("input_digest") != want:
        return False, "leg4: shared-input digest mismatch vs pinned expectation"
    if parsed_a.get("ok") is False:
        return False, "leg4: envelope ok == false"
    return True, (
        f"leg4: {names[0]}+{names[1]} envelopes bitwise-equal, "
        f"input_digest {want[:12]}… verified"
    )


def main(argv: list[str]) -> int:
    if argv == ["--run-action"]:
        return run_action()
    sys.stderr.write("usage: leg4.py --run-action\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
