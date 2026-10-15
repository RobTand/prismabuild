"""Host capability evidence from advisory offers and live observations."""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import os


class InterpreterAnswer(str, Enum):
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

    def answer(self, path: str) -> InterpreterAnswer:
        if self.present is not None and path in self.present:
            return InterpreterAnswer.PRESENT
        if self.absent is not None and path in self.absent:
            return InterpreterAnswer.ABSENT
        return InterpreterAnswer.UNKNOWN

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
) -> InterpreterAnswer:
    observed = [answer.answer(path) for answer in answers]
    if InterpreterAnswer.PRESENT in observed:
        return InterpreterAnswer.PRESENT
    if observed and all(answer is InterpreterAnswer.ABSENT for answer in observed):
        return InterpreterAnswer.ABSENT
    return InterpreterAnswer.UNKNOWN


@dataclass(frozen=True)
class CapabilityRefusal:
    reason: str
    evidence: Mapping[str, object]


class LiveInterpreters:
    """Executable-path observations owned by one claim pass."""

    def __init__(self) -> None:
        self._answers: dict[str, InterpreterAnswer] = {}

    def answer(self, path: str) -> InterpreterAnswer:
        if path not in self._answers:
            self._answers[path] = (
                InterpreterAnswer.PRESENT
                if os.path.isfile(path) and os.access(path, os.X_OK)
                else InterpreterAnswer.ABSENT)
        return self._answers[path]

    def refusal(self, item: Mapping[str, object]) -> CapabilityRefusal | None:
        interpreter = item.get("interpreter")
        if interpreter is None:
            return None
        if not isinstance(interpreter, str) or not interpreter.startswith("/"):
            return CapabilityRefusal(
                "malformed_interpreter", {"interpreter": interpreter})
        if self.answer(interpreter) is not InterpreterAnswer.PRESENT:
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
        (present if host.answer(path) is InterpreterAnswer.PRESENT else absent).add(path)
    return InterpreterAnswers(frozenset(present), frozenset(absent))
