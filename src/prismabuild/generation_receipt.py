"""Standard-library-only runtime receipt identities and comparison."""
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class RuntimeIdentity:
    commit: str
    generation: str


@dataclass(frozen=True)
class ReceiptUnavailable:
    reason: Literal["missing", "unreadable", "malformed", "incomplete"]


ReceiptObservation = RuntimeIdentity | ReceiptUnavailable


@dataclass(frozen=True)
class Match:
    identity: RuntimeIdentity


@dataclass(frozen=True)
class Drift:
    loaded: RuntimeIdentity
    published: RuntimeIdentity


@dataclass(frozen=True)
class Unknown:
    loaded: ReceiptObservation
    published: ReceiptObservation


ReceiptDecision = Match | Drift | Unknown


def observe(path: Path) -> ReceiptObservation:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return ReceiptUnavailable("missing")
    except OSError:
        return ReceiptUnavailable("unreadable")
    except ValueError:
        return ReceiptUnavailable("malformed")
    if not isinstance(value, dict):
        return ReceiptUnavailable("malformed")
    commit = value.get("commit")
    generation = value.get("generation")
    if not isinstance(commit, str) or not commit or not isinstance(generation, str) or not generation:
        return ReceiptUnavailable("incomplete")
    return RuntimeIdentity(commit, generation)


def compare(loaded: ReceiptObservation, published: ReceiptObservation) -> ReceiptDecision:
    if isinstance(loaded, ReceiptUnavailable) or isinstance(published, ReceiptUnavailable):
        return Unknown(loaded, published)
    return Match(loaded) if loaded == published else Drift(loaded, published)



def read_receipt(path: Path) -> dict:
    """The parsed receipt at ``path``, or ``{}`` when it cannot be read.

    One read serves both identities a receipt carries -- the commit and the
    generation name -- so a claim-boundary handshake costs one file open,
    not one per compared field.  ``{}`` is "unknown", never "absent": the
    callers' existing rule stands, and a receipt that cannot be read does
    not license a refusal any more than it licenses a claim.
    """

    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def commit_at(path: Path) -> str:
    return str(read_receipt(path).get("commit") or "")


def generation_at(path: Path) -> str:
    return str(read_receipt(path).get("generation") or "")


def drift(*, loaded_commit: str, loaded_generation: str,
          published_commit: str, published_generation: str) -> dict | None:
    moved = bool((published_commit and published_commit != loaded_commit) or (
        loaded_generation and published_generation
        and loaded_generation != published_generation))
    if not moved:
        return None
    return {
        "loaded_commit": loaded_commit,
        "loaded_generation": loaded_generation,
        "published_commit": published_commit,
        "published_generation": published_generation,
    }
