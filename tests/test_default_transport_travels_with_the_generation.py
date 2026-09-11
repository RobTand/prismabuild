"""The cutover is a property of the published bytes, not of somebody's shell.

``PRISMABUILD_TRANSPORT`` describes one process.  The fleet is agents started
from a per-user crontab, from systemd user units and from each other on three
boxes; there is no single environment to export into, and a cutover that
depends on one is a cutover that half the fleet does not hear about.

What every one of them does share is the runtime generation it executes.  So
the default rides in that generation's receipt: ``publish_runtime.py
--default-transport slurm`` records it, ``fleet_submit.default_transport``
reads it, and pointing ``repo`` back at the previous generation restores the
previous default in the same atomic namespace operation that rolled it
forward.

The field is optional and absent means ``pool``, because every generation
published before the cutover has no such field and has to keep behaving as it
did.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

import fleet_submit  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "publish_runtime_default_transport",
    ROOT / "tools" / "fleet" / "publish_runtime.py",
)
publish_runtime = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(publish_runtime)  # type: ignore[union-attr]


def _generation(root: Path, **receipt: object) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    body = {
        "schema": "prismaquant.prismabuild.runtime_version.v1",
        "commit": "b" * 40,
        "dirty": False,
        "generation": root.name,
        "files": {},
    }
    body.update(receipt)
    (root / "RUNTIME_VERSION.json").write_text(json.dumps(body), encoding="utf-8")
    return root


# -- reading the default -----------------------------------------------------


def test_a_checkout_with_no_receipt_still_means_the_pull_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(fleet_submit.DEFAULT_TRANSPORT_ENV, raising=False)
    monkeypatch.setattr(fleet_submit, "RUNTIME_ROOT", tmp_path)
    assert fleet_submit.default_transport() == "pool"


def test_a_generation_published_before_the_cutover_still_means_the_pull_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No ``default_transport`` field at all: the behaviour it was published with."""

    monkeypatch.delenv(fleet_submit.DEFAULT_TRANSPORT_ENV, raising=False)
    monkeypatch.setattr(
        fleet_submit, "RUNTIME_ROOT", _generation(tmp_path / "gen")
    )
    assert fleet_submit.default_transport() == "pool"


def test_the_published_generation_carries_the_cutover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(fleet_submit.DEFAULT_TRANSPORT_ENV, raising=False)
    monkeypatch.setattr(
        fleet_submit, "RUNTIME_ROOT",
        _generation(tmp_path / "gen", default_transport="slurm"),
    )
    assert fleet_submit.default_transport() == "slurm"


def test_the_environment_overrules_the_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A person saying where one submission goes is never overruled by a file."""

    monkeypatch.setenv(fleet_submit.DEFAULT_TRANSPORT_ENV, "pool")
    monkeypatch.setattr(
        fleet_submit, "RUNTIME_ROOT",
        _generation(tmp_path / "gen", default_transport="slurm"),
    )
    assert fleet_submit.default_transport() == "pool"


def test_a_transport_this_code_does_not_have_is_not_guessed_at(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A receipt from the future must not route work into a queue nobody drains."""

    monkeypatch.delenv(fleet_submit.DEFAULT_TRANSPORT_ENV, raising=False)
    monkeypatch.setattr(
        fleet_submit, "RUNTIME_ROOT",
        _generation(tmp_path / "gen", default_transport="kubernetes"),
    )
    assert fleet_submit.default_transport() == "pool"


def test_a_damaged_receipt_does_not_take_submission_with_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(fleet_submit.DEFAULT_TRANSPORT_ENV, raising=False)
    generation = tmp_path / "gen"
    generation.mkdir()
    (generation / "RUNTIME_VERSION.json").write_text("{not json", encoding="utf-8")
    monkeypatch.setattr(fleet_submit, "RUNTIME_ROOT", generation)
    assert fleet_submit.default_transport() == "pool"


def test_an_environment_transport_this_code_does_not_have_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typed value is checked, not passed on.

    ``argparse`` applies ``choices=`` to what it parses and never to a
    default, so ``PRISMABUILD_TRANSPORT=slrum`` became ``args.transport`` in
    every producer. Each one branches on ``transport == "slurm"``, so the
    misspelling read as "not slurm" and submitted the work to the pull queue,
    silently, under a name nobody had.
    """

    monkeypatch.setenv(fleet_submit.DEFAULT_TRANSPORT_ENV, "slrum")
    with pytest.raises(SystemExit) as caught:
        fleet_submit.default_transport()
    assert "slrum" in str(caught.value)
    assert fleet_submit.DEFAULT_TRANSPORT_ENV in str(caught.value)


# -- every producer reads it, and reading it is what the tests check ---------
#
# A source-text check that pbrun spells ``default_transport()`` passed while
# pbtest, pbcampaign and tessera_status each kept their own
# ``os.environ.get(...) or "pool"``. These run the producers instead.


def test_an_unflagged_pbtest_shards_onto_the_lane_the_generation_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Twenty shards onto a stopped queue is a day of waiting and no result."""

    from test_pbtest import _dispatch

    monkeypatch.delenv(fleet_submit.DEFAULT_TRANSPORT_ENV, raising=False)
    monkeypatch.setattr(
        fleet_submit, "RUNTIME_ROOT",
        _generation(tmp_path / "gen", default_transport="slurm"),
    )
    command = _dispatch(tmp_path, monkeypatch, [])
    assert command[command.index("--transport") + 1] == "slurm"


def test_the_environment_still_holds_a_suite_back_from_the_lane(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half of the same default: a generation does not overrule a person.

    Someone debugging the lane has to be able to run a suite on the pull queue
    while the published generation says ``slurm``, without republishing.
    """

    from test_pbtest import _dispatch

    monkeypatch.setenv(fleet_submit.DEFAULT_TRANSPORT_ENV, "pool")
    monkeypatch.setattr(
        fleet_submit, "RUNTIME_ROOT",
        _generation(tmp_path / "gen", default_transport="slurm"),
    )
    command = _dispatch(tmp_path, monkeypatch, [])
    assert command[command.index("--transport") + 1] == "pool"


def test_an_unflagged_pbcampaign_dispatches_onto_the_lane_the_generation_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """And a campaign of ``host_class`` rows is refused at manifest load
    before it gets that far, because the class needs the lane."""

    import pbcampaign
    import pbrun

    seen: dict[str, list[str]] = {}

    def fake_main() -> int:
        seen["argv"] = list(sys.argv)
        print(json.dumps({"action_key": "a" * 64, "status": "submitted"}))
        return 0

    monkeypatch.setattr(pbrun, "main", fake_main)
    monkeypatch.delenv(fleet_submit.DEFAULT_TRANSPORT_ENV, raising=False)
    monkeypatch.setattr(
        fleet_submit, "RUNTIME_ROOT",
        _generation(tmp_path / "gen", default_transport="slurm"),
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps([{"argv": ["/bin/bash", "-lc", "true"], "cwd": str(tmp_path)}]),
        encoding="utf-8",
    )

    assert pbcampaign.main(["--detach", str(manifest)]) == 0
    argv = seen["argv"]
    assert argv[argv.index("--transport") + 1] == "slurm"


# -- writing it --------------------------------------------------------------


def _checkout(path: Path) -> Path:
    package = path / "src" / "prismabuild"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "core.py").write_text("GENERATION = 'x'\n")
    (package / "pool.py").write_text("GENERATION = 'x'\n")
    return path


def _publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, argv: list[str],
    *, generation: str = "one",
) -> Path:
    """Publish one generation into ``tmp_path``'s single store, and return the live name."""

    commit = "c" * 40
    checkout = _checkout(tmp_path / f"checkout-{generation}")
    mirror = tmp_path / "mirror" / "repo"
    mirror.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())

    def run(argv_, **_kwargs):
        words = [str(part) for part in argv_]
        if words and words[0] == "git":
            if "rev-parse" in words:
                return SimpleNamespace(returncode=0, stdout=commit + "\n", stderr="")
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout="import ok\n", stderr="")

    monkeypatch.setattr(publish_runtime.subprocess, "run", run)
    monkeypatch.setattr(
        sys, "argv",
        ["publish_runtime.py", "--rollout", "rolling", "--rollout-reason",
         "transport fixture has reviewed mixed-generation compatibility", *argv],
    )
    assert publish_runtime.main() == 0
    return mirror


def test_publishing_without_the_flag_records_no_transport_at_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    mirror = _publish(tmp_path, monkeypatch, [])
    receipt = json.loads((mirror / "RUNTIME_VERSION.json").read_text())
    assert "default_transport" not in receipt


def test_publishing_the_cutover_records_it_in_the_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    mirror = _publish(tmp_path, monkeypatch, ["--default-transport", "slurm"])
    receipt = json.loads((mirror / "RUNTIME_VERSION.json").read_text())
    assert receipt["default_transport"] == "slurm"
    monkeypatch.delenv(fleet_submit.DEFAULT_TRANSPORT_ENV, raising=False)
    monkeypatch.setattr(fleet_submit, "RUNTIME_ROOT", mirror.resolve())
    assert fleet_submit.default_transport() == "slurm"


# -- rolling it back ---------------------------------------------------------


def test_rollback_re_points_the_live_name_at_an_existing_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rollback restores proved bytes; it does not rebuild them from a checkout."""

    mirror = _publish(tmp_path, monkeypatch, ["--default-transport", "slurm"])
    before = mirror.resolve().name

    # A second generation into the same store, cut back to the pull queue.
    # This is the state a rollback starts from in reverse: two proved
    # generations and a live name pointing at one of them.
    again = _publish(tmp_path, monkeypatch, [], generation="two")
    assert again == mirror
    assert mirror.resolve().name != before
    assert "default_transport" not in json.loads(
        (mirror / "RUNTIME_VERSION.json").read_text()
    )

    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    rolling = {
        "rollout": "rolling",
        "rollout_reason": "transport fixture restores a prior compatible generation",
    }
    assert publish_runtime._activate_existing(before, dry_run=True, **rolling) == 0
    assert publish_runtime._activate_existing(before, dry_run=False, **rolling) == 0
    assert mirror.resolve().name == before
    receipt = json.loads((mirror / "RUNTIME_VERSION.json").read_text())
    assert receipt["default_transport"] == "slurm"


def test_activation_refuses_a_name_that_is_not_a_published_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    mirror = _publish(tmp_path, monkeypatch, [])
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    for name in ("../elsewhere", "nope", ""):
        with pytest.raises(SystemExit):
            publish_runtime._activate_existing(
                name, dry_run=False, rollout="rolling",
                rollout_reason="transport fixture validates the target name",
            )
    assert mirror.is_symlink()
