"""Standard-library-only runtime receipt identities and comparison."""
import json
from pathlib import Path



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
