"""Two links that each look contained can compose into an escape.

Issue #70: ``require_supported_snapshot_tree`` normalized one link target
string at a time.  With ``a -> .`` sealed alongside it, ``b ->
a/../outside.txt`` normalizes to ``outside.txt`` and was accepted, while the
filesystem resolves ``a`` to the repository root first and then applies ``..``,
so ``b`` names the repository's parent.  Editing that external file changes
what the action reads while the snapshot record, the bundle digest and both
link texts stay identical.

The gate now runs on both legs: the sealed tree's link graph at submit time,
and the materialized tree's links after checkout, because a bundle sealed by
an older ``pbrun`` carries whatever that version accepted.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import materialize  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "pbrun", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]

MAX_BYTES = 16 * 1024 * 1024


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(cwd), *args], check=True,
                   capture_output=True, text=True)


def _source(tmp_path: Path) -> Path:
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "PrismaBuild test")
    _git(source, "config", "user.email", "test@example.invalid")
    (source / "assets").mkdir()
    (source / "assets" / "payload.txt").write_text("sealed bytes\n")
    _git(source, "add", "assets/payload.txt")
    _git(source, "commit", "-qm", "sealed tree")
    return source


def _stamp(source: Path, name: str) -> str:
    stamp_name = f"{pbrun.STAMP_PREFIX}{name}.json"
    (source / stamp_name).write_text(
        json.dumps({"cwd": ".", **pbrun._git_identity(source)})
    )
    return stamp_name


def test_seal_refuses_links_that_compose_into_an_escape(tmp_path: Path) -> None:
    """Each target normalizes inside the tree; the pair does not."""

    source = _source(tmp_path)
    (tmp_path / "outside.txt").write_text("unsealed host bytes\n")
    (source / "a").symlink_to(".", target_is_directory=True)
    (source / "b").symlink_to("a/../outside.txt")
    stamp_name = _stamp(source, "composed-escape")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    with pytest.raises(SystemExit, match="symlink.*outside"):
        pbrun.build_git_checkout_snapshot(
            source, stamp_name=stamp_name, cas=cas, max_bytes=MAX_BYTES
        )


def test_seal_refuses_an_escape_composed_through_a_subdirectory_link(
    tmp_path: Path,
) -> None:
    """The composition does not have to start at the repository root."""

    source = _source(tmp_path)
    (source / "nested").mkdir()
    (source / "nested" / "up").symlink_to("..", target_is_directory=True)
    (source / "nested" / "reach").symlink_to("up/../../outside.txt")
    stamp_name = _stamp(source, "nested-escape")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    with pytest.raises(SystemExit, match="symlink.*outside"):
        pbrun.build_git_checkout_snapshot(
            source, stamp_name=stamp_name, cas=cas, max_bytes=MAX_BYTES
        )


def test_seal_refuses_a_symlink_cycle(tmp_path: Path) -> None:
    """A link graph that cannot be resolved is refused, not walked forever."""

    source = _source(tmp_path)
    (source / "loop-a").symlink_to("loop-b")
    (source / "loop-b").symlink_to("loop-a")
    stamp_name = _stamp(source, "cyclic-links")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    with pytest.raises(SystemExit, match="cycles through"):
        pbrun.build_git_checkout_snapshot(
            source, stamp_name=stamp_name, cas=cas, max_bytes=MAX_BYTES
        )


def test_seal_still_accepts_links_that_stay_in_the_tree(tmp_path: Path) -> None:
    """Containment is the rule; composition through a link is not the offence."""

    source = _source(tmp_path)
    (source / "here").symlink_to(".", target_is_directory=True)
    (source / "data").symlink_to("assets", target_is_directory=True)
    (source / "payload").symlink_to("data/payload.txt")
    (source / "roundabout").symlink_to("here/assets/../assets/payload.txt")
    (source / "missing").symlink_to("assets/absent.txt")
    stamp_name = _stamp(source, "contained-links")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    snapshot = pbrun.build_git_checkout_snapshot(
        source, stamp_name=stamp_name, cas=cas, max_bytes=MAX_BYTES
    )

    assert snapshot["commit"]


def _snapshot_record_carrying(
    tmp_path: Path, links: dict[str, str], cas: pb.PrismaBuildCAS
) -> dict[str, object]:
    """Seal a bundle by hand, the way an older ``pbrun`` would have.

    The point of the materialize-time leg is a bundle the current seal-time
    gate would refuse, so this fixture cannot go through ``pbrun``.
    """

    sealed = tmp_path / "older-pbrun-source"
    sealed.mkdir()
    _git(sealed, "init", "-q")
    _git(sealed, "config", "user.name", "PrismaBuild test")
    _git(sealed, "config", "user.email", "test@example.invalid")
    (sealed / "assets").mkdir()
    (sealed / "assets" / "payload.txt").write_text("sealed bytes\n")
    for name, target in links.items():
        (sealed / name).symlink_to(target)
    _git(sealed, "add", "-A")
    _git(sealed, "commit", "-qm", "sealed by an older pbrun")
    commit = subprocess.run(
        ["git", "-C", str(sealed), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    bundle = tmp_path / f"{'-'.join(sorted(links))}.bundle"
    _git(sealed, "bundle", "create", str(bundle), "HEAD")
    entry, _ = cas.ingest_input(bundle, input_id="pbrun.checkout-snapshot")
    return {
        "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
        "commit": commit,
        "subdirectory": ".",
        "input": entry,
    }


def test_materialize_refuses_a_bundle_whose_links_compose_into_an_escape(
    tmp_path: Path,
) -> None:
    """The worker checks the tree that landed, not the record that described it."""

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    snapshot = _snapshot_record_carrying(
        tmp_path, {"a": ".", "b": "a/../outside.txt"}, cas
    )
    (tmp_path / "outside.txt").write_text("unsealed host bytes\n")
    item = {
        "action_key": "a" * 64,
        "cas_root": str(cas_root),
        "checkout_snapshot": snapshot,
    }

    with pytest.raises(
        materialize.MaterializationContractError, match="points outside"
    ):
        with materialize._execution_checkout(
            item, local_checkout_root=tmp_path / "materialized"
        ):
            pass


def test_materialize_accepts_a_bundle_whose_links_stay_in_the_tree(
    tmp_path: Path,
) -> None:
    """A contained link, including one through another link, still runs."""

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    snapshot = _snapshot_record_carrying(
        tmp_path,
        {"here": ".", "data": "assets", "payload": "data/payload.txt"},
        cas,
    )
    item = {
        "action_key": "b" * 64,
        "cas_root": str(cas_root),
        "checkout_snapshot": snapshot,
    }

    with materialize._execution_checkout(
        item, local_checkout_root=tmp_path / "materialized"
    ) as checkout:
        assert (checkout / "payload").read_text() == "sealed bytes\n"


def test_materialize_reports_an_unresolvable_link_as_a_contract_refusal(
    tmp_path: Path,
) -> None:
    """A link loop inside the tree is refused cleanly, never as a crash.

    The seal-time gate refuses a cycle, so only a bundle already sealed by an
    older ``pbrun`` can carry one.  ``Path.resolve`` reports such a loop as
    ``RuntimeError`` on some Python versions and as ``OSError`` on others, and
    on later versions it returns the link itself, which is contained.  The
    contract is the same in every case: either the checkout runs, or it fails
    with ``MaterializationContractError``.  Nothing else may escape.
    """

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    snapshot = _snapshot_record_carrying(
        tmp_path, {"loop-a": "loop-b", "loop-b": "loop-a"}, cas
    )
    item = {
        "action_key": "c" * 64,
        "cas_root": str(cas_root),
        "checkout_snapshot": snapshot,
    }

    try:
        with materialize._execution_checkout(
            item, local_checkout_root=tmp_path / "materialized"
        ) as checkout:
            assert (checkout / "assets" / "payload.txt").read_text() == (
                "sealed bytes\n"
            )
    except materialize.MaterializationContractError as error:
        assert "cannot be resolved" in str(error)
