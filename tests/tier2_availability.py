"""Which profile backends this box can honour, asked of the box itself."""

from __future__ import annotations

import json
from pathlib import Path
import socket
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.core as pb                                       # noqa: E402


def main() -> int:
    table = {}
    for mode, backend in sorted(pb.PROFILE_BACKENDS.items()):
        entry = {"name": getattr(backend, "name", mode)}
        try:
            entry["path"] = backend.locate()
            entry["version"] = backend.version
            entry["backed"] = True
        except pb.ProfileBackendUnavailable as exc:
            entry["backed"] = False
            entry["reason"] = str(exc)[:300]
        table[mode] = entry
    print(json.dumps({"host": socket.gethostname(), "python": sys.executable,
                      "modes": table}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
