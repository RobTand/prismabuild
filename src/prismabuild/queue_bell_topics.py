"""Names-only topic declarations and bounded namespace adapters."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from .holder_census import HolderCensus
from .adaptive_snapshot import FILES as MIRROR_FILES
from .record_store import PASSES, DENIAL_TRANSITIONS

MAX_TOPIC_FILTERS = 32
MAX_ID_FILTERS = 128
MAX_TOPIC_LENGTH = 64
MAX_ID_LENGTH = 128


@dataclass(frozen=True)
class Topic:
    name: str
    directory: str
    id_pattern: str
    required: bool = False
    legacy: bool = False

    def accepts_id(self, value: str) -> bool:
        return (isinstance(value, str) and 0 < len(value) <= MAX_ID_LENGTH
                and re.fullmatch(self.id_pattern, value) is not None)


# New mutation families need their own bounded adapter and explicit declaration.
OUTCOME_TOPICS = tuple(Topic(name, name, r"[0-9a-f]{64}", required, True)
    for name, required in (("ready", True), ("claimed", True), ("done", True),
        ("failed", True), ("withdrawn", True), ("deferred", True), ("intent", True),
        ("attempts", False), ("publication-endings", False)))
# Host IDs preserve the existing store's spelling, including dotted names.
# They are never split on dots or normalized to action IDs.
HOST_ID_PATTERN = r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
WORKER_TOPICS = (Topic("workers", "workers", HOST_ID_PATTERN),)
HOST_RESERVATION_TOPICS = (Topic("reservations", "reservations", HOST_ID_PATTERN),)
ADAPTIVE_TOPICS = (Topic("adaptive", "reservations", HOST_ID_PATTERN),
                   Topic("claim-denials", "reservations", HOST_ID_PATTERN),
                   Topic(PASSES, PASSES, r"[0-9a-f]{64}"),
                   Topic(DENIAL_TRANSITIONS, DENIAL_TRANSITIONS, r"[0-9a-f]{64}"))
TOPICS = {topic.name: topic for topic in
          (*OUTCOME_TOPICS, *WORKER_TOPICS, *HOST_RESERVATION_TOPICS, *ADAPTIVE_TOPICS)}


class OutcomeNamespaces:
    """Watch shapes and action-name extraction for the original outcome feed."""

    topics = OUTCOME_TOPICS
    by_directory = {topic.directory: topic for topic in OUTCOME_TOPICS}

    @staticmethod
    def allows(relative: Path) -> bool:
        parts = relative.parts
        if not parts:
            return True
        if parts[0] not in OutcomeNamespaces.by_directory:
            return False
        if len(parts) == 1:
            return True
        if parts[0] == "withdrawn":
            return ((len(parts) == 2 and parts[1] in ("decisions", "superseded"))
                    or (len(parts) == 3 and parts[1] == "decisions"))
        return ((parts[0] == "attempts" and len(parts) <= 3)
                or (parts[0] == "publication-endings" and len(parts) == 2))

    @staticmethod
    def hint(relative: Path) -> tuple[str, str] | None:
        parts = relative.parts
        if "." in relative.name and not relative.name.endswith((".json", ".tombstone", ".late-finish")):
            return None
        if len(parts) < 2:
            return None
        name = (parts[2] if parts[:2] == ("withdrawn", "decisions") and len(parts) >= 3
                else parts[1] if parts[0] in ("attempts", "publication-endings")
                else parts[-1])
        key = name.split(".", 1)[0]
        topic = OutcomeNamespaces.by_directory.get(parts[0])
        if topic is not None and topic.accepts_id(key):
            return topic.name, key
        return None


class WorkerOffers:
    """Store.worker_offer_path: one JSON filename per exact host ID."""

    topics = WORKER_TOPICS

    @staticmethod
    def allows(relative: Path) -> bool:
        return relative.parts == ("workers",)

    @staticmethod
    def hint(relative: Path) -> tuple[str, str] | None:
        parts = relative.parts
        if len(parts) == 2 and parts[0] == "workers" and parts[1].endswith(".json"):
            host = parts[1][:-5]
            if WORKER_TOPICS[0].accepts_id(host):
                return "workers", host
        return None


class HostReservations:
    """ResourceLedger shapes only, excluding adaptive mirrors and denials.

    Watch each host, its free/minted token directories, held membership and
    each holder's immediate contents. Private acquisitions and old holder
    spellings have the same shape. No token body establishes capacity here.
    """

    topics = HOST_RESERVATION_TOPICS
    token_directories = frozenset(("free", "held", "minted"))

    @staticmethod
    def hidden_input(relative: Path) -> bool:
        return HostReservations.hint(relative) is not None

    @staticmethod
    def reconstruction_inputs(relative: Path, entries):
        parts = relative.parts
        if len(parts) == 3 and parts[0] == "reservations" and parts[2] == "held":
            return HolderCensus.directory_inputs(entries, include_symlinks=True)
        return entries

    @staticmethod
    def allows(relative: Path) -> bool:
        parts = relative.parts
        if not parts or parts[0] != "reservations":
            return False
        if len(parts) == 1:
            return True
        if not HOST_RESERVATION_TOPICS[0].accepts_id(parts[1]):
            return False
        return (len(parts) == 2
                or (len(parts) == 3 and parts[2] in HostReservations.token_directories)
                or (len(parts) == 4 and parts[2] == "held"))

    @staticmethod
    def hint(relative: Path) -> tuple[str, str] | None:
        parts = relative.parts
        if (len(parts) < 2 or parts[0] != "reservations"
                or not HOST_RESERVATION_TOPICS[0].accepts_id(parts[1])):
            return None
        if (len(parts) == 2
                or (len(parts) == 3 and parts[2] != "adaptive")
                or (len(parts) == 4 and parts[2] in HostReservations.token_directories)
                or (len(parts) == 5 and parts[2] == "held")):
            return "reservations", parts[1]
        return None


class AdaptiveInputs:
    """Fixed mirror filenames and flat per-action sidecars, without body reads."""

    topics = tuple(topic for topic in ADAPTIVE_TOPICS if topic.name != "claim-denials")
    mirror_files = frozenset((*MIRROR_FILES, "publisher-result.json"))
    action_directories = frozenset((PASSES, DENIAL_TRANSITIONS))

    @staticmethod
    def allows(relative: Path) -> bool:
        parts = relative.parts
        if not parts:
            return False
        if parts[0] in AdaptiveInputs.action_directories:
            return len(parts) == 1
        return (parts[0] == "reservations" and (len(parts) == 1
                or (TOPICS["adaptive"].accepts_id(parts[1])
                    and (len(parts) == 2 or (len(parts) == 3 and parts[2] == "adaptive")))))

    @staticmethod
    def hint(relative: Path) -> tuple[str, str] | None:
        parts = relative.parts
        if not parts:
            return None
        if parts[0] in AdaptiveInputs.action_directories and len(parts) == 2:
            key, suffix = relative.stem, relative.suffix
            if suffix == ".json" and TOPICS[parts[0]].accepts_id(key):
                return parts[0], key
        if (parts[0] == "reservations" and len(parts) >= 2
                and TOPICS["adaptive"].accepts_id(parts[1])):
            if len(parts) == 2 or (len(parts) == 3 and parts[2] == "adaptive"):
                return "adaptive", parts[1]
            if (len(parts) == 4 and parts[2] == "adaptive"
                    and parts[3] in AdaptiveInputs.mirror_files and parts[3] != "claim-denials.json"):
                return "adaptive", parts[1]
        return None


class ClaimDenials:
    topics = (TOPICS["claim-denials"],)

    @staticmethod
    def allows(relative: Path) -> bool:
        return False  # AdaptiveInputs installs the shared parent watches.

    @staticmethod
    def hint(relative: Path) -> tuple[str, str] | None:
        parts = relative.parts
        if (len(parts) in (2, 3, 4) and parts[0] == "reservations"
                and TOPICS["claim-denials"].accepts_id(parts[1])
                and (len(parts) == 2 or (parts[2] == "adaptive"
                     and (len(parts) == 3 or parts[3] == "claim-denials.json")))):
            return "claim-denials", parts[1]
        return None


class NamespaceInputs:
    """Per-namespace census rules used by the common watch installer."""

    adapters = (HostReservations,)

    @classmethod
    def hidden_input(cls, relative: Path) -> bool:
        return any(adapter.hidden_input(relative) for adapter in cls.adapters)

    @classmethod
    def reconstruction_inputs(cls, relative: Path, entries):
        for adapter in cls.adapters:
            entries = adapter.reconstruction_inputs(relative, entries)
        return entries


class TopicFilters:
    """Parse bounded query filters without interpreting IDs as filesystem paths."""

    @staticmethod
    def parse(topics: str, ids: str) -> tuple[set[str], set[str]]:
        topic_values = topics.split(",") if topics else []
        id_values = ids.split(",") if ids else []
        if len(topic_values) > MAX_TOPIC_FILTERS or len(id_values) > MAX_ID_FILTERS:
            raise ValueError("bell filter count exceeds limit")
        if any(len(name) > MAX_TOPIC_LENGTH or not re.fullmatch(r"[a-z][a-z0-9-]*", name)
               for name in topic_values):
            raise ValueError("invalid bell topic")
        selected = set(topic_values)
        known = [TOPICS[name] for name in selected if name in TOPICS]
        if not selected:
            known = list(TOPICS.values())
        for value in id_values:
            if (not 0 < len(value) <= MAX_ID_LENGTH
                    or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", value)
                    or (known and not any(topic.accepts_id(value) for topic in known))):
                raise ValueError("invalid bell topic identifier")
        return selected, set(id_values)
