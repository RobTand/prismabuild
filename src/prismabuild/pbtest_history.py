"""The latest completed pbtest report for each repository."""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile

from prismabuild.digest_primitives import raw_sha256


def _repository_identity(checkout: Path) -> str:
    root = checkout.resolve()
    for directory in (root, *root.parents):
        gitdir = directory / ".git"
        if gitdir.is_file():
            pointer = gitdir.read_text().strip()
            if not pointer.startswith("gitdir: ") or not pointer[8:]:
                raise ValueError(f"invalid Git directory pointer: {gitdir}")
            gitdir = directory / pointer[8:]
        if gitdir.is_dir():
            common = gitdir / "commondir"
            if common.is_file():
                gitdir = gitdir / common.read_text().strip()
            return str(gitdir.resolve())
    return str(root)


def _report_path(checkout: Path) -> Path:
    key = raw_sha256(_repository_identity(checkout).encode())
    state = os.environ.get("XDG_STATE_HOME")
    state_root = Path(state) if state and Path(state).is_absolute() else (
        Path.home() / ".local" / "state")
    return state_root / "prismabuild" / "pbtest-history-v2" / f"{key}.json"


def cached_report(checkout: Path, paths: list[str]) -> Path | None:
    """Return repository history if present; let the caller report I/O errors."""
    report = _report_path(checkout)
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
    report = _report_path(checkout)
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
