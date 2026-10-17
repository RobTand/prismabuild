"""Pure exact-claim facts, independent of the queue and its record store."""
from __future__ import annotations

from collections.abc import Callable, Mapping

CLAIM_IDENTITY_FIELDS = ("claimed_by", "claimed_unix", "published_unix", "attempts")


def same_claim(
    live: Mapping[str, object], snapshot: Mapping[str, object]
) -> bool:
    """Whether a live claimed record is the claim a worker actually ran.

    Only fields the queue writes once per claim, so the guards that rewrite a
    claimed record in place -- a container-cleanup retry, a withdrawal's
    ``max_attempts`` poison, a pending stop -- do not read as a different
    claim.
    """

    return all(
        live.get(field) == snapshot.get(field) for field in CLAIM_IDENTITY_FIELDS
    )


def check_lease_identity(
    action_key: str, claim: Mapping[str, object], lease: Mapping[str, object] | None,
    *, invalid: Callable[[str], Exception],
) -> None:
    """Refuse conflicting observations before replacing or cleaning ownership.

    Host-level reservations do not distinguish attempts on the same box.
    Missing legacy fields supply no extra proof; jointly stale reads remain
    outside this consistency check.
    """
    if lease is None:
        return
    conflicts = [
        claim_field for claim_field, lease_field in (
            ("claimed_by", "owner"), ("claimed_host", "host"),
            ("claimed_unix", "claimed_unix"),
            ("published_unix", "published_unix"),
        )
        if claim.get(claim_field) is not None
        and lease.get(lease_field) is not None
        and claim[claim_field] != lease[lease_field]
    ]
    if conflicts:
        raise invalid(
            f"contradictory claim and lease identity for {action_key}: "
            f"{', '.join(conflicts)}; claim and reservations retained"
        )
