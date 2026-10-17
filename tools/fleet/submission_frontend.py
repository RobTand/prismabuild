"""CLI display and configured-root adapters for package submissions."""
from pathlib import Path
import sys
from runtime_paths import generation_root
from prismabuild import fleet_roster
from prismabuild.submission_types import Display, FleetStore


def store_for(entrypoint, *, root=Path("/mnt/shared/prismabuild-fleet")) -> FleetStore:
    return FleetStore(Path(root), generation_root(entrypoint), fleet_roster.Roster.beside(entrypoint))


def render_submission(event: Display) -> None:
    print(event.text, file=sys.stderr if event.channel == "stderr" else sys.stdout,
          end=event.end, flush=event.flush)
