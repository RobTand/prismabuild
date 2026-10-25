"""The latest completed pbtest report for each checkout and requested path set."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile


def _report_path(checkout: Path, paths: list[str]) -> Path:
    root = checkout.resolve()
    requested = sorted({(root / path).resolve().relative_to(root).as_posix()
                        for path in paths or ["tests"]})
    scope = json.dumps([str(root), requested], separators=(",", ":")).encode()
    key = hashlib.sha256(scope).hexdigest()
    state = os.environ.get("XDG_STATE_HOME")
    state_root = Path(state) if state and Path(state).is_absolute() else (
        Path.home() / ".local" / "state")
    return state_root / "prismabuild" / "pbtest-history-v1" / f"{key}.json"


def cached_report(checkout: Path, paths: list[str]) -> Path | None:
    """Return the scoped report if present; let the caller report I/O errors."""
    report = _report_path(checkout, paths)
    return report if report.is_file() else None


def remember_report(checkout: Path, paths: list[str], results: list[dict]) -> None:
    """Replace history only after every shard has an observed green verdict.

    The stored bare-list report remains readable by pbtest's explicit history
    reader. I/O errors propagate so its caller can warn without failing tests.
    """
    if not results or any(
            row.get("returncode") != 0 or row.get("ran") is not True
            or row.get("timed_out", False)
            or not isinstance(row.get("verdict"), dict)
            or row["verdict"].get("green") is not True for row in results):
        return
    data = json.dumps(results).encode()
    report = _report_path(checkout, paths)
    report.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
                dir=report.parent, prefix=f".{report.stem}.", suffix=".tmp",
                delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, report)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
