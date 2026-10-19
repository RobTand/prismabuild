"""CLI display and configured-root adapters for package submissions."""
from pathlib import Path
import sys
from prismabuild.runtime_paths import generation_root
from prismabuild import fleet_roster
from prismabuild.submission_types import Display, FleetStore, SubmissionRequest


def store_for(entrypoint, *, root=Path("/mnt/shared/prismabuild-fleet")) -> FleetStore:
    return FleetStore(Path(root), generation_root(entrypoint), fleet_roster.Roster.beside(entrypoint))


def render_submission(event: Display) -> None:
    print(event.text, file=sys.stderr if event.channel == "stderr" else sys.stdout,
          end=event.end, flush=event.flush)


def request_arguments(request, *, order=(), include_defaults=("cwd", "transport", "cpus"), omit=(), scalar_text=None):
    """Render a typed request at the pbrun process boundary."""
    from collections.abc import Sequence
    from dataclasses import fields
    import json
    defaults = SubmissionRequest()
    result = []
    declared = {item.name: item for item in fields(request)}
    ordered = [*dict.fromkeys(order), *(name for name in declared if name not in order)]
    for name in ordered:
        item = declared[name]
        name, value = item.name, getattr(request, item.name)
        if name in {"command", *omit} or value is None:
            continue
        if value == getattr(defaults, name) and name not in include_defaults:
            continue
        flag = "--" + name.replace("_", "-")
        if name == "progress_policy":
            for phase in value["phases"]:
                result.extend(["--progress-phase", f"{phase['name']}={phase['grace_s']}"])
            if value.get("cycle"):
                result.append("--progress-cycle")
        elif name == "awaited_batch":
            result.extend([flag, ":".join(str(value[key]) for key in (
                "parent_key", "plan_key", "controller_state"))])
        elif name == "release_origin_consumer":
            result.extend([flag, *value])
        elif isinstance(value, bool):
            if value:
                result.append(flag)
        elif isinstance(value, Sequence) and not isinstance(value, str):
            for entry in value:
                result.extend([flag, str(entry)])
        else:
            rendered = (scalar_text or {}).get(name, str(value))
            result.extend([flag, rendered])
    return [*result, "--", *request.command]
