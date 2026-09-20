"""The sibling-partial census answers the same set, without the per-dirent stat.

The census decides whether a publish may replace a staged name, so the set it
returns is the contract; the order it asks in is not.  These pin the set --
against the pre-change implementation, entry by entry -- and pin the two
refusals that must stay: an unreadable directory, and this mover's own
temporary never counting as a rival.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools", "fleet"))

import stage_move  # noqa: E402

OWN = "a" * 64
OTHER = "b" * 16


class _Stub:
    mover = OWN


def _previous_implementation(mover: str, destination: Path) -> list[str] | None:
    """The census exactly as it read before the reorder."""

    own = f".{destination.name}.{str(mover)[:16]}.partial"
    try:
        names = [entry.name for entry in os.scandir(destination.parent)
                 if entry.is_file(follow_symlinks=False)]
    except OSError:
        return None
    out = []
    for name in names:
        if name == own:
            continue
        if name == f".{destination.name}.partial":
            out.append(name)
        elif (name.startswith(f".{destination.name}.")
                and name.endswith(".partial")):
            out.append(name)
    return sorted(out)[:5]


def _census(destination: Path) -> list[str] | None:
    return stage_move._StagedPublisher._inflight_partials(_Stub(), destination)


@pytest.fixture()
def staged(tmp_path: Path) -> Path:
    destination = tmp_path / "shard-00001.safetensors"
    destination.write_bytes(b"payload")
    return destination


def _siblings(destination: Path, names: list[str]) -> None:
    for name in names:
        (destination.parent / name).write_bytes(b"")


@pytest.mark.parametrize("siblings, expected", [
    ([], []),
    # The legacy shared temporary, which had its own arm before.
    ([".shard-00001.safetensors.partial"],
     [".shard-00001.safetensors.partial"]),
    # An owner-keyed temporary from another mover: a live rival.
    ([f".shard-00001.safetensors.{OTHER}.partial"],
     [f".shard-00001.safetensors.{OTHER}.partial"]),
    # Both spellings at once.
    ([".shard-00001.safetensors.partial",
      f".shard-00001.safetensors.{OTHER}.partial"],
     [".shard-00001.safetensors.partial",
      f".shard-00001.safetensors.{OTHER}.partial"]),
    # Names that must NOT count: another entry's partial, a bare payload,
    # the prefix without the suffix, and the suffix without the prefix.
    ([".shard-00002.safetensors.partial", "shard-00002.safetensors",
      ".shard-00001.safetensors.inprogress", ".partial"], []),
])
def test_the_census_answers_what_it_always_answered(
        staged: Path, siblings: list[str], expected: list[str]) -> None:
    _siblings(staged, siblings)
    assert _census(staged) == sorted(expected)
    assert _census(staged) == _previous_implementation(OWN, staged)


def test_this_movers_own_temporary_is_never_a_rival(staged: Path) -> None:
    """A mover deferring to itself would never publish anything."""

    _siblings(staged, [f".{staged.name}.{OWN[:16]}.partial"])
    assert _census(staged) == []
    assert _census(staged) == _previous_implementation(OWN, staged)


def test_a_crashed_partial_is_still_seen(staged: Path) -> None:
    """Residue from a crash reads the same as a copy in flight: both defer."""

    residue = staged.parent / f".{staged.name}.{OTHER}.partial"
    residue.write_bytes(b"half a payload")
    assert _census(staged) == [residue.name]
    residue.unlink()
    assert _census(staged) == [], "the sweep's removal must free the name"


def test_an_unreadable_directory_still_fails_closed(staged: Path) -> None:
    """None is a refusal, and it must survive asking the name first."""

    missing = staged.parent / "no-such-dir" / staged.name
    assert _census(missing) is None
    assert _previous_implementation(OWN, missing) is None


def test_a_directory_entry_that_is_not_a_regular_file_is_not_a_partial(
        staged: Path) -> None:
    """A matching name still has to be a regular file to count."""

    (staged.parent / f".{staged.name}.{OTHER}.partial").mkdir()
    assert _census(staged) == []
    assert _census(staged) == _previous_implementation(OWN, staged)


def test_the_answer_is_capped_and_sorted(staged: Path) -> None:
    """The cap and the order are part of the contract callers print."""

    _siblings(staged, [f".{staged.name}.{i:016d}.partial" for i in range(9)])
    got = _census(staged)
    assert got == sorted(got)[:5] and len(got) == 5
    assert got == _previous_implementation(OWN, staged)


def test_only_matching_names_are_stat_ed(staged: Path, monkeypatch) -> None:
    """The point of the reorder: a sibling that cannot match is never stat'ed.

    This is the property that removes the cost, so it is pinned rather than
    left to the benchmark.
    """

    _siblings(staged, [f"bulk-{i:05d}.bin" for i in range(64)]
              + [f".{staged.name}.{OTHER}.partial"])
    stat_ed: list[str] = []
    original = os.DirEntry.is_file

    def counting_is_file(self, *args, **kwargs):
        stat_ed.append(self.name)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(os.DirEntry, "is_file", counting_is_file,
                        raising=False)
    assert _census(staged) == [f".{staged.name}.{OTHER}.partial"]
    assert stat_ed == [f".{staged.name}.{OTHER}.partial"], (
        "only the one candidate name may be stat'ed")
