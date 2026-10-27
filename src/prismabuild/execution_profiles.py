"""Sealed execution profiles are separate from diagnostic profiler modes.

The hostile contract is opt-in and unavailable until its execution backend,
trusted collector and lane qualification exist. Nothing here enables a host.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import re


SCHEMA = "prismabuild.execution_profile.v1"
PARAM = "execution_profile"
HOSTILE_NAME = "hostile-v1"
CAPABILITY = "hostile-execution-v1"
GUARANTEES = tuple(f"PB-H{number}" for number in range(1, 8))


class ProfileError(ValueError):
    """Invalid profile intent at a submission or sealed-record boundary."""


class ProfileUnavailable(ProfileError):
    """The selected profile has no supported execution and result owner."""


@dataclass(frozen=True)
class Selection:
    policy_sha256: str

    def record(self) -> dict[str, str]:
        return {"schema": SCHEMA, "name": HOSTILE_NAME, "policy_sha256": self.policy_sha256}

    def required_tags(self) -> tuple[str, str]:
        return CAPABILITY, "hostile-policy:" + self.policy_sha256


def parse(value: object) -> Selection:
    if (not isinstance(value, Mapping) or set(value) != {"schema", "name", "policy_sha256"}
            or value["schema"] != SCHEMA or value["name"] != HOSTILE_NAME
            or not isinstance(value["policy_sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", value["policy_sha256"]) is None):
        raise ProfileError("hostile-v1 requires its exact versioned profile and policy SHA-256")
    return Selection(value["policy_sha256"])


def requested(name: str | None, policy_sha256: str | None, *, transport: str) -> Selection | None:
    if name is None and policy_sha256 is None:
        return None
    selected = parse({"schema": SCHEMA, "name": name, "policy_sha256": policy_sha256})
    if transport != "pool":
        raise ProfileError("hostile-v1 requires the pool's trusted execution and cleanup owner")
    return selected


def from_action(action: Mapping[str, object]) -> Selection | None:
    params = action.get("params")
    if not isinstance(params, Mapping) or PARAM not in params:
        return None
    return parse(params[PARAM])


def require_backend(action: Mapping[str, object]) -> None:
    if from_action(action) is not None:
        raise ProfileUnavailable(
            "hostile-v1 execution and authenticated collection are unavailable; "
            "native lane qualification is required before this profile can execute or accept a receipt")


def check_row(action, *, tags, resources) -> Selection | None:
    selected = from_action(action) if action is not None else None
    reserved = {tag for tag in tags if tag == CAPABILITY or str(tag).startswith("hostile-policy:")}
    if selected is None:
        if reserved:
            raise ProfileError("hostile-v1 capability requires a sealed execution profile")
        return None
    if action["params"].get("demand") != resources:
        raise ProfileError("hostile-v1 row demand differs from its sealed request")
    if not set(selected.required_tags()).issubset(set(tags)):
        raise ProfileError("hostile-v1 row erased its exact execution-policy fences")
    return selected
