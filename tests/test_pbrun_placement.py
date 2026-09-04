"""Where an action may run is derived from the checkout, not asked of the caller."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

_SPEC = importlib.util.spec_from_file_location(
    "pbrun", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]

HOST = "sparky"


def _tags(cwd: str, **kw: object) -> list[str]:
    return pbrun.placement_tags(
        Path(cwd),
        explicit=kw.pop("explicit", []),          # type: ignore[arg-type]
        here=bool(kw.pop("here", False)),
        hostname=HOST,
    )


def test_a_shared_checkout_is_free_to_run_on_any_box() -> None:
    """The case that used to need a flag nobody remembered.

    A checkout under the shared mount is at the same path on every box, so
    pinning it to the submitter is a pure loss: the work ran correctly on one
    box while the others sat idle, and nothing anywhere reported that.  An
    empty tag list means the queue places it from the demand alone.
    """

    assert _tags("/mnt/shared/tessera-x86") == []
    assert _tags("/mnt/shared/prismabuild-fleet/repo") == []


def test_a_box_local_checkout_is_pinned_to_that_box() -> None:
    """The pin is a fact about the path, not a preference.

    ``/home/rob/tessera`` exists on every box and holds *different* bytes on
    each; an action that runs there and lands elsewhere does not fail loudly,
    it silently operates on another box's tree.
    """

    assert _tags("/home/rob/tessera") == [HOST]
    assert _tags("/home/rob/tmp/ts50") == [HOST]


def test_here_pins_a_shared_checkout_on_purpose() -> None:
    assert _tags("/mnt/shared/tessera-x86", here=True) == [HOST]


def test_an_explicit_tag_wins_because_only_the_caller_knows_it() -> None:
    """A hardware class the work requires is the one thing the path cannot say."""

    assert _tags("/mnt/shared/tessera-x86", explicit=["x86"]) == ["x86"]
    assert _tags("/home/rob/tessera", explicit=["x86"]) == ["x86"]


def test_a_symlink_into_shared_storage_is_still_shared() -> None:
    """Placement follows the resolved path; a link must not change the answer."""

    assert _tags("/mnt/shared/./tessera-x86/../tessera-x86") == []


@pytest.mark.parametrize("cwd", ["/mnt/shared", "/mnt/shared-other/tree", "/mnt"])
def test_only_paths_under_the_shared_root_count(cwd: str) -> None:
    """``/mnt/shared`` itself is shared; a sibling that merely starts with the
    same characters is not.  ``relative_to`` compares path components, which is
    the reason to use it here rather than a string prefix."""

    assert _tags(cwd) == ([] if cwd == "/mnt/shared" else [HOST])
