"""CLI display and configured-root adapters for package submissions."""
from pathlib import Path
import sys
from prismabuild.runtime_paths import generation_root
from prismabuild import fleet_roster
from prismabuild.submission_types import Display, FleetStore, SubmissionRequest

SUBMISSION_SCHEMA_V1 = "prismabuild.submission.v1"


def store_for(entrypoint, *, root=Path("/mnt/shared/prismabuild-fleet")) -> FleetStore:
    return FleetStore(Path(root), generation_root(entrypoint), fleet_roster.Roster.beside(entrypoint))


def render_submission(event: Display) -> None:
    print(event.text, file=sys.stderr if event.channel == "stderr" else sys.stdout,
          end=event.end, flush=event.flush)


def consumer_submission_display(store: FleetStore, *, render=render_submission):
    """Adapt internal submission diagnostics to the versioned consumer JSON."""
    from dataclasses import replace
    import json
    from . import action_client, core as pb
    from .digest_primitives import raw_sha256

    def display(event: Display) -> None:
        if event.channel == "stdout":
            try:
                value = json.loads(event.text)
            except ValueError:
                value = None
            if (isinstance(value, dict) and value.get("schema") == action_client.DETACH_SCHEMA_V1
                    and value.get("action_key") is not None):
                action = pb.PrismaBuildCAS(store.cas_root).read_action_request(value["action_key"])
                if action is None:
                    raise ValueError("submission acknowledgement has no verified sealed request")
                generation = value.get("published_unix")
                # ``status`` keeps the detach vocabulary (submitted, attached,
                # cache_hit): TrustLadder's live_runs accepts exactly those.
                value.update(schema=SUBMISSION_SCHEMA_V1,
                    request_sha256=raw_sha256(pb._canonical_file_bytes(action)),
                    publication=dict(published_unix=generation, generation=generation))
                event = replace(event, text=json.dumps(value))
        render(event)

    return display


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
