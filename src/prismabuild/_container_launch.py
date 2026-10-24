"""Private, per-launch container class qualification (not CLI parsing).

The executor supplies the policy from its authoritative launch context. Neither
an action environment verdict nor persisted offer evidence is an input here.
An absent policy is a refusal for container work, not for native execution or
Docker inventory. The shim calls this again for every run/create/owned exec.
"""
from __future__ import annotations

import time

if __package__:
    from . import container_images as ci
else:  # retained runtime's flat module aliases
    import container_images as ci


class LaunchRefused(ValueError):
    """Container-only refusal; callers must leave native admission unchanged."""


def qualify_launch(policy: ci.ClassImagePolicy | None, *, endpoint: str,
                   cache: ci.InventoryCache | None = None) -> dict[str, object]:
    """Evaluate trusted class configuration against fresh host-local evidence.

    ``policy`` must be obtained through pool/core's private runtime mediation,
    never reconstructed from an action-supplied satisfied flag or offer. The
    endpoint is the effective endpoint resolved by the Docker CLI, which the
    caller must pin on the subsequent operation. InventoryCache probes the
    system daemon, so a different local socket is not interchangeable evidence.
    """
    if not isinstance(policy, ci.ClassImagePolicy):
        raise LaunchRefused("class_context_missing: no authoritative container class launch policy")
    if endpoint not in {ci.LOCAL_ENDPOINT, "unix:///run/docker.sock"}:
        raise LaunchRefused("endpoint_mismatch: class inventory covers only the local system Docker endpoint")
    inventory = (cache if cache is not None else ci.InventoryCache()).snapshot(
        max_age_s=ci.CLAIM_FRESHNESS_S)
    verdict = policy.evaluate(inventory, now=time.time(), max_age_s=ci.CLAIM_FRESHNESS_S)
    if verdict.get("status") != "satisfied" or verdict.get("container_work_eligible") is not True:
        raise LaunchRefused(str(verdict.get("reason", "inventory_unknown")))
    return verdict
