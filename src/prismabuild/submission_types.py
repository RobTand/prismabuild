"""Typed requests, explicit fleet roots and submission diagnostics."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, NoReturn


@dataclass(frozen=True)
class Display:
    text: str
    channel: Literal["stdout", "stderr"] = "stderr"
    end: str = "\n"
    flush: bool = True


type DisplaySink = Callable[[Display], None]


def discard_display(event: Display) -> None:
    pass


