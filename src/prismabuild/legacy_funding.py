"""The retained per-batch funding file, below batch publication and funding."""
from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path

from . import output_contract as oc
from .digest_primitives import sorted_json


class LegacyFunding:
    """A lazy address; read, file and cleanup retain the existing byte recipe."""

    def __init__(self, path: Path):
        self.path = path

    @staticmethod
    def directory(scope: Path) -> Path:
        return scope / "funding"

    def read(self) -> dict[str, object] | None:
        try:
            raw = json.loads(self.path.read_text())
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            raise oc.ProducedOutputError(
                f"funding intent unreadable: {exc}") from None
        if not isinstance(raw, Mapping):
            raise oc.ProducedOutputError("funding intent is corrupt")
        return dict(raw)

    def file(self, record: Mapping[str, object]) -> None:
        path = self.path
        path.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary = tempfile.mkstemp(
            dir=str(path.parent), prefix=".funding.")
        try:
            with os.fdopen(handle, "w") as stream:
                stream.write(sorted_json(dict(record)))
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        except BaseException:
            Path(temporary).unlink(missing_ok=True)
            raise

    def delete(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise oc.ProducedOutputError(
                f"funding intent unreleasable: {exc}") from None
