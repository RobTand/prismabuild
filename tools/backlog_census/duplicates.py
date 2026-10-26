"""Title matching shared by the census and ticket intake."""
from difflib import SequenceMatcher
import re

PREFIX = re.compile(r"^\s*\[(P0|P1|P2\+|P2|P3|P4)\]")


def normalize_title(title: str) -> str:
    return re.sub(r"\W+", " ", PREFIX.sub("", title).casefold()).strip()


def likely_duplicate(left: str, right: str) -> bool:
    left, right = normalize_title(left), normalize_title(right)
    return bool(left and right and (
        left == right or (len(left) >= 30 and len(right) >= 30 and
                          SequenceMatcher(None, left, right).ratio() >= .92)
    ))
