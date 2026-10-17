"""Queue publication facts projected from one sealed action."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import inspect
from pathlib import Path

from . import pool


@dataclass(frozen=True)
class RowPublication:
    queue_root: Path
    cas_root: Path
    worker_script: Path
    priority: int
    max_attempts: int
    retry_safe: bool
    priority_reason: str | None = None

    def row(self, action: Mapping[str, object], *, queue,
            max_attempts: int | None = None, retry_safe: bool | None = None,
            tested_repository: str | None = None) -> dict[str, object]:
        """The queue projection, with sealed work and explicit publication policy."""
        params = action["params"]
        demand = params["demand"]
        variables = action["environment"]["variables"]
        row: dict[str, object] = {
            "action_key": str(action["action_key"]),
            "cas_root": str(self.cas_root),
            "worker_script": str(self.worker_script),
            "tags": params["placement"]["required_tags"],
            "needs_gpu": bool(demand.get("gpu")),
            "priority": self.priority,
            "resources": demand,
            # ``args`` for the submitter's own work; overridden only by a movement
            # node, whose idempotence is a property of the mover rather than of
            # what the submitter asked for (#603).
            "max_attempts": (self.max_attempts if max_attempts is None
                             else int(max_attempts)),
            "container_owner": str(variables[pool.CONTAINER_OWNER_ENV]),
            "checkout_snapshot": params["checkout_snapshot"],
        }
        priority_reason = self.priority_reason
        if priority_reason is not None:
            parameters = inspect.signature(queue.publish).parameters
            if ("priority_reason" not in parameters
                    and not any(p.kind is inspect.Parameter.VAR_KEYWORD
                                for p in parameters.values())):
                raise pool.record_store.PoolContractError(
                    "loaded queue runtime does not support priority_reason; "
                    "use a matching runtime or omit --priority-reason")
            row["priority_reason"] = pool.normalize_priority_reason(priority_reason)
        if tested_repository is not None:
            # The template's frozen answer for the tree this action seals (#1565).
            # Guarded like the annotation above: a loaded runtime that cannot
            # carry the tag refuses rather than silently untagging the submission.
            parameters = inspect.signature(queue.publish).parameters
            if ("tested_repository" not in parameters
                    and not any(p.kind is inspect.Parameter.VAR_KEYWORD
                                for p in parameters.values())):
                raise pool.record_store.PoolContractError(
                    "loaded queue runtime does not support tested_repository; "
                    "use a matching runtime")
            row["tested_repository"] = pool.normalize_tested_repository(
                tested_repository)
        if params.get("container_images"):
            # Derived from the sealed body, never re-read from the caller: the row
            # describes the action, so the action's own params are the authority.
            row["container_images"] = list(params["container_images"])
        if params.get("interpreter"):
            row["interpreter"] = str(params["interpreter"])
        for field in ("local_dependencies", "dependency_queries"):
            if params.get(field):
                row[field] = dict(params[field])
        if params.get("gang"):
            row["gang"] = dict(params["gang"])
        if params.get("requires_files") and "requires_files" in (
                inspect.signature(queue.publish).parameters):
            # Derived from the sealed body like the interpreter (#1495), and
            # guarded like retry_safe: in a mixed runtime window the row simply
            # omits the field and the capability tag in the row's tags alone
            # still fences every worker that cannot read the requirement.
            row["requires_files"] = [
                {"path": str(entry["path"]), "sha256": str(entry["sha256"])}
                for entry in params["requires_files"]]
        # The repo checkout can advance just before the atomic runtime generation
        # rolls.  The previous PoolQueue already accepts the safety-critical bound,
        # so keep that mixed window usable; add the explanatory annotation once the
        # loaded runtime exposes it.  The sealed action params carry the full
        # contract in both cases.
        if "retry_safe" in inspect.signature(queue.publish).parameters:
            row["retry_safe"] = (self.retry_safe if retry_safe is None
                                 else bool(retry_safe))
        if params.get("resident_set") is not None:
            row["resident_set"] = params["resident_set"]
        return row
