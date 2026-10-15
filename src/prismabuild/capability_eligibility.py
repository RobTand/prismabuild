"""Host capability evidence from advisory offers and live observations."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
import shutil
from types import MappingProxyType

from . import container_images, contracts, dependency_digest, local_dependencies


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


@dataclass(frozen=True)
class ImageClassReport:
    _value: object

    @classmethod
    def capture(cls, value: object) -> ImageClassReport:
        return cls(dict(value) if isinstance(value, dict) else value)

    def eligible(self, *, clock: Callable[[], float]) -> bool:
        return container_images.offer_class_image_eligible(self._value, now=clock())

    def offer_value(self) -> object:
        return dict(self._value) if isinstance(self._value, dict) else self._value


@dataclass(frozen=True)
class ImageAnswers:
    references: frozenset[str] | None = None
    class_report: ImageClassReport | None = None

    @classmethod
    def from_offer(cls, offer: Mapping[str, object]) -> ImageAnswers:
        references = offer.get("container_images")
        return cls(
            frozenset(str(ref) for ref in references) if isinstance(references, list) else None,
            ImageClassReport.capture(offer["container_class_verdict"])
            if "container_class_verdict" in offer else None)

    @classmethod
    def from_observed(
        cls, references: Iterable[str] | None, verdict: Mapping[str, object] | None,
    ) -> ImageAnswers:
        return cls(
            frozenset(str(ref) for ref in references) if references is not None else None,
            ImageClassReport.capture(dict(verdict)) if verdict is not None else None)

    def matches(self, required: Sequence[str], *, clock: Callable[[], float]) -> bool:
        if self.class_report is not None and not self.class_report.eligible(clock=clock):
            return False
        return self.references is not None and not container_images.missing(required, self.references)

    def offer_fields(self) -> dict[str, object]:
        fields: dict[str, object] = {}
        if self.references is not None:
            fields["container_images"] = sorted(self.references)
        if self.class_report is not None:
            fields["container_class_verdict"] = self.class_report.offer_value()
        return fields


@dataclass(frozen=True)
class OfferRequirements:
    images: tuple[str, ...] | None
    interpreter: str | None
    local_paths: Mapping[str, str]
    file_paths: frozenset[str] | None

    @classmethod
    def from_item(cls, item: Mapping[str, object]) -> OfferRequirements:
        images = item.get("container_images")
        if images is not None and (
                not isinstance(images, list) or not all(isinstance(ref, str) for ref in images)):
            raise ValueError("pool item container_images must be a list of strings")
        interpreter = item.get("interpreter")
        if interpreter is not None and not isinstance(interpreter, str):
            raise ValueError("pool item interpreter must be an absolute path string")
        local = local_dependencies.normalize(item.get("local_dependencies", {}))
        files = item.get("requires_files")
        if files is not None and (
                not isinstance(files, list) or any(
                    not isinstance(entry, dict) or not isinstance(entry.get("path"), str)
                    for entry in files)):
            raise ValueError("pool item requires_files must be a list of entries naming a path")
        return cls(
            tuple(images) if images is not None else None,
            interpreter, MappingProxyType(local),
            frozenset(str(entry.get("path")) for entry in files) if files is not None else None)


@dataclass(frozen=True)
class OfferCapabilities:
    images: ImageAnswers = ImageAnswers()
    interpreters: InterpreterAnswers = InterpreterAnswers()
    dependencies: DependencyAnswers = DependencyAnswers()

    @classmethod
    def from_offer(cls, offer: Mapping[str, object]) -> OfferCapabilities:
        return cls(ImageAnswers.from_offer(offer), InterpreterAnswers.from_offer(offer),
                   DependencyAnswers.from_offer(offer))

    def matches(self, required: OfferRequirements, *, clock: Callable[[], float]) -> bool:
        if required.images and not self.images.matches(required.images, clock=clock):
            return False
        if (required.interpreter
                and self.interpreters.answer(required.interpreter) is not Presence.PRESENT):
            return False
        if local_dependencies.missing(required.local_paths, self.dependencies.local_paths):
            return False
        return not required.file_paths or self.dependencies.files_answer(
            set(required.file_paths)) is Presence.PRESENT

    def offer_fields(self) -> dict[str, object]:
        return {**self.images.offer_fields(), **self.interpreters.offer_fields(),
                **self.dependencies.offer_fields()}


@dataclass(frozen=True)
class ImageAccepted:
    class_verdict: Mapping[str, object] | None = None


class LiveImages:
    def __init__(
        self, references: Iterable[str] | None, *,
        policy: container_images.ClassImagePolicy | None, inventory: object,
    ) -> None:
        self._references = references
        self._policy = policy
        self._inventory = inventory

    def class_check(self, *, clock: Callable[[], float]) -> ImageAccepted | CapabilityRefusal:
        if self._policy is None:
            return ImageAccepted()
        verdict = self._policy.evaluate(
            self._inventory, now=clock(), max_age_s=container_images.CLAIM_FRESHNESS_S)
        if not verdict["container_work_eligible"]:
            return CapabilityRefusal("container_class_" + str(verdict["reason"]),
                                     {"container_class_verdict": verdict})
        return ImageAccepted(verdict)

    def check(
        self, item: Mapping[str, object], *, clock: Callable[[], float],
    ) -> ImageAccepted | CapabilityRefusal:
        required = item.get("container_images")
        tags = item.get("tags")
        if not required and isinstance(tags, list) and contracts.CONTAINER_IMAGE_TAG in tags:
            return CapabilityRefusal("container_image_requirement_missing", {"tags": tags})
        if not required:
            return ImageAccepted()
        accepted = self.class_check(clock=clock)
        if isinstance(accepted, CapabilityRefusal):
            return accepted
        if not isinstance(required, list) or not all(isinstance(ref, str) for ref in required):
            return CapabilityRefusal("malformed_container_images", {"container_images": required})
        observed = None
        if self._references is not None:
            try:
                observed = {str(ref) for ref in self._references}
            except TypeError:
                pass
        if observed is None:
            return CapabilityRefusal("container_image_presence_unknown", {"required": required})
        absent = container_images.missing(required, observed)
        if absent:
            return CapabilityRefusal("container_image_absent", {
                "absent": list(absent), "required": required})
        return accepted

    def room_eligible(self, item: Mapping[str, object], *, clock: Callable[[], float]) -> bool:
        required = item.get("container_images")
        if required:
            if isinstance(self.class_check(clock=clock), CapabilityRefusal):
                return False
            return (self._references is not None and isinstance(required, list)
                    and not container_images.missing(
                        required, {str(ref) for ref in self._references}))
        return contracts.CONTAINER_IMAGE_TAG not in (item.get("tags") or [])
