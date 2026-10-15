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
