"""Host capability evidence from advisory offers and live observations."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
import shutil
from types import MappingProxyType
from typing import Protocol

from . import container_images, contracts, dependency_digest, local_dependencies


class Presence(str, Enum):
    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"


def _advertised_strings(value: object) -> frozenset[str] | None:
    if isinstance(value, list) and all(isinstance(entry, str) for entry in value):
        return frozenset(value)
    return None


@dataclass(frozen=True)
class HostPresence:
    tags: frozenset[str]
    gpu: Presence

    @property
    def enrollment_pins(self) -> frozenset[str]:
        return frozenset(tag.removeprefix("enrolling-only:") for tag in self.tags
                         if tag.startswith("enrolling-only:"))

    @classmethod
    def from_observed(cls, tags: Iterable[str], *, has_gpu: bool) -> HostPresence:
        return cls(frozenset(str(tag) for tag in tags),
                   Presence.PRESENT if has_gpu else Presence.ABSENT)

    @classmethod
    def from_offer(cls, offer: Mapping[str, object]) -> HostPresence:
        value = offer.get("has_gpu")
        gpu = Presence.PRESENT if value is True else Presence.ABSENT if value is False else Presence.UNKNOWN
        return cls(_advertised_strings(offer.get("tags")) or frozenset(), gpu)

    def matches(self, required: Iterable[str], *, needs_gpu: bool) -> bool:
        return ((not needs_gpu or self.gpu is Presence.PRESENT)
                and all(str(tag) in self.tags for tag in required))

    def offer_fields(self) -> dict[str, object]:
        fields: dict[str, object] = {"tags": sorted(self.tags)}
        if self.gpu is not Presence.UNKNOWN:
            fields["has_gpu"] = self.gpu is Presence.PRESENT
        return fields


def placement_matches(
    item: Mapping[str, object], *, tags: frozenset[str], has_gpu: bool,
) -> bool:
    needs_gpu = bool(item.get("needs_gpu"))
    resources = item.get("resources")
    if isinstance(resources, Mapping):
        try:
            needs_gpu = needs_gpu or int(resources.get("gpu", 0)) > 0
        except (TypeError, ValueError, OverflowError):
            pass  # demand validation follows the live capability gates
    presence = HostPresence.from_observed(tags, has_gpu=has_gpu)
    required = item.get("tags") or []
    if not isinstance(required, list):
        raise ValueError("pool item tags must be a list")
    enrolling = presence.enrollment_pins
    if enrolling and not enrolling.intersection(required):
        return False
    return presence.matches(required, needs_gpu=needs_gpu)


@dataclass(frozen=True)
class InterpreterAnswers:
    present: frozenset[str] | None = None
    absent: frozenset[str] | None = None

    @classmethod
    def from_offer(cls, offer: Mapping[str, object]) -> InterpreterAnswers:
        present = offer.get("interpreters")
        absent = offer.get("interpreters_absent")
        return cls(_advertised_strings(present), _advertised_strings(absent))

    @classmethod
    def from_observed(
        cls, present: Iterable[str] | None, absent: Iterable[str] | None,
    ) -> InterpreterAnswers:
        return cls(
            frozenset(present) if present is not None else None,
            frozenset(absent) if absent is not None else None,
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
        return cls(
            MappingProxyType(dict(local)) if isinstance(local, Mapping) else None,
            _advertised_strings(present), _advertised_strings(absent))

    @classmethod
    def from_observed(
        cls, local: Mapping[str, object] | None,
        present: Iterable[str] | None, absent: Iterable[str] | None,
    ) -> DependencyAnswers:
        return cls(
            MappingProxyType(dict(local)) if local is not None else None,
            frozenset(present) if present is not None else None,
            frozenset(absent) if absent is not None else None)

    def files_answer(self, paths: set[str]) -> Presence:
        if self.files_present is not None and paths <= self.files_present:
            return Presence.PRESENT
        if self.files_absent is not None and paths <= self.files_absent:
            return Presence.ABSENT
        return Presence.UNKNOWN

    def local_answers(self, required: Mapping[str, str]) -> Mapping[str, object] | None:
        return self.local_paths

    def file_presence(self, path: str) -> Presence:
        if self.files_present is not None and path in self.files_present:
            return Presence.PRESENT
        if self.files_absent is not None and path in self.files_absent:
            return Presence.ABSENT
        return Presence.UNKNOWN

    def digest(self, path: str) -> DigestEvidence:
        return DigestEvidence.UNOBSERVED

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
        for item in items if isinstance(item, dict) and isinstance(item.get("requires_files"), list)
        for entry in item["requires_files"]
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

    def local_answers(self, required: Mapping[str, str]) -> Mapping[str, object]:
        return local_dependencies.observe(required)

    def file_presence(self, path: str) -> Presence:
        present = self._present.setdefault(path, bool(dependency_digest.presence([path])[0]))
        return Presence.PRESENT if present else Presence.ABSENT

    def digest(self, path: str) -> str | None:
        if path not in self._digests:
            self._digests[path] = dependency_digest.claim_digest(path)
        return self._digests[path]


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
            _advertised_strings(references),
            ImageClassReport.capture(offer["container_class_verdict"])
            if "container_class_verdict" in offer else None)

    @classmethod
    def from_observed(
        cls, references: Iterable[str] | None, verdict: Mapping[str, object] | None,
    ) -> ImageAnswers:
        return cls(
            frozenset(references) if references is not None else None,
            ImageClassReport.capture(dict(verdict)) if verdict is not None else None)

    def class_check(self, *, clock: Callable[[], float]) -> ImageAccepted | CapabilityRefusal:
        if self.class_report is not None and not self.class_report.eligible(clock=clock):
            return CapabilityRefusal("container_class_unavailable", {
                "container_class_verdict": self.class_report.offer_value()})
        return ImageAccepted()

    def image_references(self) -> frozenset[str] | None:
        return self.references

    def offer_fields(self) -> dict[str, object]:
        fields: dict[str, object] = {}
        if self.references is not None:
            fields["container_images"] = sorted(self.references)
        if self.class_report is not None:
            fields["container_class_verdict"] = self.class_report.offer_value()
        return fields


@dataclass(frozen=True)
class FileRequirement:
    path: str
    sha256: str


@dataclass(frozen=True)
class DeclaredCapabilities:
    interpreter: str | None
    images: tuple[str, ...]
    local_paths: Mapping[str, str]
    queries: Mapping[str, str]
    files: list[dict] | None

    @classmethod
    def parse(
        cls, *, interpreter: object, images: object, local_paths: object, queries: object,
        files: object,
    ) -> DeclaredCapabilities:
        try:
            references = container_images.normalize_refs(images) if images is not None else ()
        except ValueError as exc:
            raise ValueError(f"container_images: {exc}") from exc
        named = normalize_interpreter(interpreter)
        local = local_dependencies.normalize({} if local_paths is None else local_paths)
        questions = local_dependencies.normalize({} if queries is None else queries)
        try:
            requirements = dependency_digest.validate_requirements(files) if files is not None else None
        except ValueError as exc:
            raise ValueError(f"requires_files: {exc}") from exc
        return cls(named, tuple(references), MappingProxyType(local), MappingProxyType(questions), requirements)

    def validate_tags(self, tags: Sequence[str]) -> None:
        if contracts.INTERPRETER_TAG in tags and self.interpreter is None:
            raise ValueError(
                f"{contracts.INTERPRETER_TAG} requires an interpreter; an item may "
                "not require the declared-interpreter capability without naming one")
        if contracts.LOCAL_DEPENDENCY_TAG in tags and not self.local_paths:
            raise ValueError(f"{contracts.LOCAL_DEPENDENCY_TAG} requires local_dependencies")
        if contracts.DEPENDENCY_DIGEST_TAG in tags and self.files is None:
            raise ValueError(
                f"{contracts.DEPENDENCY_DIGEST_TAG} requires requires_files; an item may "
                "not require the dependency-digest capability without naming any")
        if contracts.CONTAINER_IMAGE_TAG in tags and not self.images:
            raise ValueError(
                f"{contracts.CONTAINER_IMAGE_TAG} requires container_images; an item may "
                "not require the declared-image capability without declaring one")


@dataclass(frozen=True)
class CapabilityRequirements:
    interpreter: str | CapabilityRefusal | None
    local_paths: Mapping[str, str] | CapabilityRefusal
    files: tuple[FileRequirement, ...] | CapabilityRefusal | None
    images: tuple[str, ...] | CapabilityRefusal
    tags: tuple[str, ...]
    files_declared: bool
    images_declared: bool

    @classmethod
    def from_item(cls, item: Mapping[str, object]) -> CapabilityRequirements:
        interpreter = item.get("interpreter")
        if interpreter is not None and (
                not isinstance(interpreter, str) or not interpreter.startswith("/")):
            interpreter = CapabilityRefusal("malformed_interpreter", {"interpreter": interpreter})
        try:
            local = MappingProxyType(local_dependencies.normalize(item.get("local_dependencies", {})))
        except ValueError as exc:
            local = CapabilityRefusal("local_dependencies_unavailable", {"error": str(exc)})
        raw_files = item.get("requires_files")
        files = None
        if raw_files is not None:
            try:
                dependency_digest.validate_requirements(raw_files)
                files = tuple(FileRequirement(entry["path"], entry["sha256"]) for entry in raw_files)
            except ValueError as exc:
                files = CapabilityRefusal("malformed_requires_files", {"error": str(exc)})
        raw_images = item.get("container_images")
        images = ()
        if raw_images is not None:
            if not isinstance(raw_images, list) or not all(isinstance(ref, str) for ref in raw_images):
                images = CapabilityRefusal("malformed_container_images", {"container_images": raw_images})
            else:
                images = tuple(raw_images)
        return cls(interpreter, local, files, images, tuple(str(tag) for tag in (item.get("tags") or ())),
                   bool(raw_files), bool(raw_images))

    def require_well_formed(self) -> None:
        if isinstance(self.images, CapabilityRefusal):
            raise ValueError("pool item container_images must be a list of strings")
        if isinstance(self.interpreter, CapabilityRefusal):
            raise ValueError("pool item interpreter must be an absolute path string")
        if isinstance(self.local_paths, CapabilityRefusal):
            raise ValueError(str(self.local_paths.evidence["error"]))
        if isinstance(self.files, CapabilityRefusal):
            raise ValueError("pool item requires_files: " + str(self.files.evidence["error"]))


@dataclass(frozen=True)
class OfferCapabilities:
    images: ImageAnswers = ImageAnswers()
    interpreters: InterpreterAnswers = InterpreterAnswers()
    dependencies: DependencyAnswers = DependencyAnswers()

    @classmethod
    def from_offer(cls, offer: Mapping[str, object]) -> OfferCapabilities:
        return cls(ImageAnswers.from_offer(offer), InterpreterAnswers.from_offer(offer),
                   DependencyAnswers.from_offer(offer))

    def check(self, required: CapabilityRequirements, *, clock: Callable[[], float]) -> ImageAccepted | CapabilityRefusal:
        return _check_capabilities(required, self, clock=clock)

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

    def image_references(self) -> frozenset[str] | None:
        if self._references is not None:
            try:
                return frozenset(str(ref) for ref in self._references)
            except TypeError:
                pass
        return None

    def check(
        self, item: Mapping[str, object], *, clock: Callable[[], float],
    ) -> ImageAccepted | CapabilityRefusal:
        return _check_images(CapabilityRequirements.from_item(item), self, clock=clock)


class DigestEvidence(Enum):
    UNOBSERVED = "advisory_presence_only"


class ImageEvidence(Protocol):
    def class_check(self, *, clock: Callable[[], float]) -> ImageAccepted | CapabilityRefusal: ...
    def image_references(self) -> frozenset[str] | None: ...


@dataclass(frozen=True)
class LiveCapabilities:
    interpreters: LiveInterpreters
    dependencies: LiveDependencies
    images: LiveImages

    def check(self, item: Mapping[str, object], *, clock: Callable[[], float]) -> ImageAccepted | CapabilityRefusal:
        return _check_capabilities(CapabilityRequirements.from_item(item), self, clock=clock)


def _check_capabilities(
    required: CapabilityRequirements, host: OfferCapabilities | LiveCapabilities, *, clock: Callable[[], float],
) -> ImageAccepted | CapabilityRefusal:
    interpreter = required.interpreter
    if isinstance(interpreter, CapabilityRefusal):
        return interpreter
    if contracts.INTERPRETER_TAG in required.tags and interpreter is None:
        return CapabilityRefusal("interpreter_requirement_missing", {"tags": list(required.tags)})
    if interpreter is not None and host.interpreters.answer(interpreter) is not Presence.PRESENT:
        return CapabilityRefusal("interpreter_not_present", {"interpreter": interpreter})
    local = required.local_paths
    if isinstance(local, CapabilityRefusal):
        return local
    if contracts.LOCAL_DEPENDENCY_TAG in required.tags and not local:
        return CapabilityRefusal("local_dependencies_unavailable", {
            "error": f"{contracts.LOCAL_DEPENDENCY_TAG} requires local_dependencies"})
    try:
        missing = local_dependencies.missing(local, host.dependencies.local_answers(local))
    except (ValueError, OSError) as exc:
        return CapabilityRefusal("local_dependencies_unavailable", {"error": str(exc)})
    if missing:
        return CapabilityRefusal("local_dependency_not_present", {"paths": missing})
    if contracts.DEPENDENCY_DIGEST_TAG in required.tags and not required.files_declared:
        return CapabilityRefusal("dependency_requirement_missing", {"tags": list(required.tags)})
    files = required.files
    if isinstance(files, CapabilityRefusal):
        return files
    if files is not None:
        missing = [entry.path for entry in files if host.dependencies.file_presence(entry.path) is not Presence.PRESENT]
        if missing:
            return CapabilityRefusal("dependency_not_present", {"paths": missing})
        for entry in files:
            observed = host.dependencies.digest(entry.path)
            if observed is DigestEvidence.UNOBSERVED:
                continue
            if observed is None:
                return CapabilityRefusal("dependency_unreadable", {"path": entry.path})
            if observed != entry.sha256:
                return CapabilityRefusal("dependency_digest_mismatch", {
                    "path": entry.path, "expected": entry.sha256, "observed": observed})
    return _check_images(required, host.images, clock=clock)


def _check_images(
    required: CapabilityRequirements, host: ImageEvidence, *,
    clock: Callable[[], float],
) -> ImageAccepted | CapabilityRefusal:
    if contracts.CONTAINER_IMAGE_TAG in required.tags and not required.images_declared:
        return CapabilityRefusal("container_image_requirement_missing", {"tags": list(required.tags)})
    if not required.images_declared:
        return required.images if isinstance(required.images, CapabilityRefusal) else ImageAccepted()
    accepted = host.class_check(clock=clock)
    if isinstance(accepted, CapabilityRefusal):
        return accepted
    if isinstance(required.images, CapabilityRefusal):
        return required.images
    references = host.image_references()
    images = list(required.images)
    if references is None:
        return CapabilityRefusal("container_image_presence_unknown", {"required": images})
    absent = container_images.missing(images, references)
    if absent:
        return CapabilityRefusal("container_image_absent", {"absent": list(absent), "required": images})
    return accepted
