"""Declared local path evidence on the existing worker-offer/claim boundary."""
from __future__ import annotations

import os
from collections.abc import Mapping

TAG = "local-dependency-v1"


def normalize(requirements: object) -> dict[str, str]:
    if not isinstance(requirements, Mapping):
        raise ValueError("local_dependencies must be a path-to-kind mapping")
    result = {}
    for path, kind in requirements.items():
        if (not isinstance(path, str) or not path.startswith("/")
                or path == "/" or "\x00" in path or kind not in ("path", "executable")):
            raise ValueError(f"invalid local_dependencies entry: {path!r}: {kind!r}")
        result[path] = kind
    return dict(sorted(result.items()))


def observe(requirements: Mapping[str, str]) -> dict[str, str]:
    """One local lookup per declared path; no scanning, SSH or remembered state."""
    answers = {}
    for path in requirements:
        try:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                answers[path] = "executable"
            elif os.path.exists(path):
                answers[path] = "path"
            else:
                answers[path] = "absent"
        except OSError:
            answers[path] = "unknown"
    return answers


def missing(requirements: Mapping[str, str], answers: object) -> list[str]:
    if not isinstance(answers, Mapping):
        return list(requirements)
    return [path for path, kind in requirements.items()
            if answers.get(path) not in (("executable",) if kind == "executable"
                                        else ("path", "executable"))]
