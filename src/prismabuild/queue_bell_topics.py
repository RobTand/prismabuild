"""Names-only topic declarations and bounded outcome namespace adapter (#1939)."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

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
# Only these existing outcome inputs are covered in this slice.
OUTCOME_TOPICS = tuple(Topic(name, name, r"[0-9a-f]{64}", required, True)
    for name, required in (("ready", True), ("claimed", True), ("done", True),
        ("failed", True), ("withdrawn", True), ("deferred", True), ("intent", True),
        ("attempts", False), ("publication-endings", False)))
TOPICS = {topic.name: topic for topic in OUTCOME_TOPICS}


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
