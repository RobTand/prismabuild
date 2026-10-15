"""Host capability evidence from advisory offers and live observations."""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
import shutil
from types import MappingProxyType

from . import contracts, dependency_digest, local_dependencies


class Presence(str, Enum):
    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class InterpreterAnswers:
    present: frozenset[str] | None = None
    absent: frozenset[str] | None = None

    @classmethod
    def from_offer(cls, offer: Mapping[str, object]) -> InterpreterAnswers:
        present = offer.get("interpreters")
        absent = offer.get("interpreters_absent")
        return cls(
            frozenset(str(path) for path in present) if isinstance(present, list) else None,
            frozenset(str(path) for path in absent) if isinstance(absent, list) else None,
        )

    @classmethod
    def from_observed(
        cls, present: Iterable[str] | None, absent: Iterable[str] | None,
    ) -> InterpreterAnswers:
        return cls(
            frozenset(str(path) for path in present) if present is not None else None,
            frozenset(str(path) for path in absent) if absent is not None else None,
        )

    def answer(self, path: str) -> Presence:
        if self.present is not None and path in self.present:
            return Presence.PRESENT
        if self.absent is not None and path in self.absent:
            return Presence.ABSENT
        return Presence.UNKNOWN

    def offer_fields(self) -> dict[str, object]:
        fields: dict[str, object] = {}
        if self.present is not None:
            fields["interpreters"] = sorted(self.present)
        if self.absent is not None:
            fields["interpreters_absent"] = sorted(self.absent)
        return fields


def normalize_interpreter(value: object) -> str | None:
    if value is None:
        return None
    if (not isinstance(value, str) or not value or "\x00" in value
            or not value.startswith("/") or value == "/"):
        raise ValueError(
            "interpreter must be an absolute path to an executable "
            f"(got {value!r})")
    return value


def interpreter_answer(
    path: str, answers: Sequence[InterpreterAnswers],
) -> Presence:
    observed = [answer.answer(path) for answer in answers]
    if Presence.PRESENT in observed:
        return Presence.PRESENT
    if observed and all(answer is Presence.ABSENT for answer in observed):
        return Presence.ABSENT
    return Presence.UNKNOWN


@dataclass(frozen=True)
class CapabilityRefusal:
    reason: str
    evidence: Mapping[str, object]


class LiveInterpreters:
    """Executable-path observations owned by one claim pass."""

    def __init__(self) -> None:
        self._answers: dict[str, Presence] = {}

    def answer(self, path: str) -> Presence:
        if path not in self._answers:
            self._answers[path] = (
                Presence.PRESENT
                if os.path.isfile(path) and os.access(path, os.X_OK)
                else Presence.ABSENT)
        return self._answers[path]

    def refusal(self, item: Mapping[str, object]) -> CapabilityRefusal | None:
        interpreter = item.get("interpreter")
        if interpreter is None:
            return None
        if not isinstance(interpreter, str) or not interpreter.startswith("/"):
            return CapabilityRefusal(
                "malformed_interpreter", {"interpreter": interpreter})
        if self.answer(interpreter) is not Presence.PRESENT:
            return CapabilityRefusal(
                "interpreter_not_present", {"interpreter": interpreter})
        return None


def observe_interpreters(items: Sequence[object]) -> InterpreterAnswers:
    paths = sorted({
        str(item.get("interpreter"))
        for item in items
        if isinstance(item, dict) and isinstance(item.get("interpreter"), str)})
    host = LiveInterpreters()
    present, absent = set(), set()
    for path in paths:
        (present if host.answer(path) is Presence.PRESENT else absent).add(path)
    return InterpreterAnswers(frozenset(present), frozenset(absent))


@dataclass(frozen=True)
class DependencyAnswers:
    local_paths: Mapping[str, object] | None = None
    files_present: frozenset[str] | None = None
    files_absent: frozenset[str] | None = None

    @classmethod
    def from_offer(cls, offer: Mapping[str, object]) -> DependencyAnswers:
        local = offer.get("local_dependencies")
        present = offer.get("dependency_files")
        absent = offer.get("dependency_files_absent")
        return cls.from_observed(
            local if isinstance(local, Mapping) else None,
            present if isinstance(present, list) else None,
            absent if isinstance(absent, list) else None)

    @classmethod
    def from_observed(
        cls, local: Mapping[str, object] | None,
        present: Iterable[str] | None, absent: Iterable[str] | None,
    ) -> DependencyAnswers:
        return cls(
            MappingProxyType(dict(local)) if local is not None else None,
            frozenset(str(path) for path in present) if present is not None else None,
            frozenset(str(path) for path in absent) if absent is not None else None)

    def files_answer(self, paths: set[str]) -> Presence:
        if self.files_present is not None and paths <= self.files_present:
            return Presence.PRESENT
        if self.files_absent is not None and paths <= self.files_absent:
            return Presence.ABSENT
        return Presence.UNKNOWN

    def matches(
        self, local: Mapping[str, str], files: Sequence[Mapping[str, object]] | None,
    ) -> bool:
        if local_dependencies.missing(local, self.local_paths):
            return False
        return not files or self.files_answer(
            {str(entry.get("path")) for entry in files}) is Presence.PRESENT

    def offer_fields(self) -> dict[str, object]:
        fields: dict[str, object] = {}
        if self.local_paths is not None:
            fields["local_dependencies"] = dict(self.local_paths)
        if self.files_present is not None:
            fields["dependency_files"] = sorted(self.files_present)
        if self.files_absent is not None:
            fields["dependency_files_absent"] = sorted(self.files_absent)
        return fields


def observe_dependencies(items: Sequence[object], *, python: str) -> DependencyAnswers:
    requirements = {python: "executable"}
    shell = shutil.which("bash", path=os.defpath)
    if shell:
        requirements[str(Path(shell).resolve())] = "executable"
    for item in items:
        if not isinstance(item, dict):
            continue
        interpreter = item.get("interpreter")
        if isinstance(interpreter, str):
            requirements[interpreter] = "executable"
        for field in ("dependency_queries", "local_dependencies"):
            try:
                queries = local_dependencies.normalize(item.get(field, {}))
            except ValueError:
                continue
            for path, kind in queries.items():
                if kind == "executable" or path not in requirements:
                    requirements[path] = kind
    local = local_dependencies.observe(requirements)
    paths = sorted({
        str(entry.get("path"))
        for item in items if isinstance(item, dict)
        for entry in (item.get("requires_files") or ())
        if isinstance(entry, dict) and isinstance(entry.get("path"), str)})
    present, absent = dependency_digest.presence(paths)
    return DependencyAnswers.from_observed(local, present, absent)


def dependency_answer(paths: set[str], answers: Sequence[DependencyAnswers]) -> str:
    if not answers:
        return "unknown_capability"
    unanimous_absent = True
    for answer in answers:
        presence = answer.files_answer(paths)
        if presence is Presence.PRESENT:
            return "present"
        if presence is not Presence.ABSENT:
            unanimous_absent = False
    return "absent" if unanimous_absent else "unknown_paths"


class LiveDependencies:
    """Local path and byte verification, with digest caches for one claim pass."""

    def __init__(self) -> None:
        self._present: dict[str, bool] = {}
        self._digests: dict[str, str | None] = {}

    def refusal(self, item: Mapping[str, object]) -> CapabilityRefusal | None:
        try:
            dependencies = local_dependencies.normalize(item.get("local_dependencies", {}))
            if local_dependencies.TAG in (item.get("tags") or []) and not dependencies:
                raise ValueError(f"{local_dependencies.TAG} requires local_dependencies")
            missing = local_dependencies.missing(
                dependencies, local_dependencies.observe(dependencies))
        except (ValueError, OSError) as exc:
            return CapabilityRefusal("local_dependencies_unavailable", {"error": str(exc)})
        if missing:
            return CapabilityRefusal("local_dependency_not_present", {"paths": missing})
        requirements = item.get("requires_files")
        tags = item.get("tags")
        if not requirements and isinstance(tags, list) and contracts.DEPENDENCY_DIGEST_TAG in tags:
            return CapabilityRefusal("dependency_requirement_missing", {"tags": tags})
        if requirements is None:
            return None
        try:
            dependency_digest.validate_requirements(requirements)
        except ValueError as exc:
            return CapabilityRefusal("malformed_requires_files", {"error": str(exc)})
        paths = [str(entry["path"]) for entry in requirements]
        missing = [path for path in paths if not self._present.setdefault(
            path, bool(dependency_digest.presence([path])[0]))]
        if missing:
            return CapabilityRefusal("dependency_not_present", {"paths": missing})
        for entry in requirements:
            path = str(entry["path"])
            if path not in self._digests:
                self._digests[path] = dependency_digest.claim_digest(path)
            observed = self._digests[path]
            if observed is None:
                return CapabilityRefusal("dependency_unreadable", {"path": path})
            if observed != entry["sha256"]:
                return CapabilityRefusal("dependency_digest_mismatch", {
                    "path": path, "expected": entry["sha256"], "observed": observed})
        return None
