"""A publication refuses a roster that drops live worker args (#1664).

Generation c8daa1be416c dropped ``--gang-admission`` from both Spark
worker shapes. The supervisor adopts the new generation's roster at
re-exec, so it spawned Spark loops without the flag and a native gang
could not publish until the sealed generation was edited by hand.

A fresh publication therefore compares the candidate roster with the
live generation's roster before the live pointer moves. A candidate
that drops any ``--`` option the live roster declares for any box is
refused. Resource values may change; option names may not vanish. An
explicit override names who removes the option and why, and the receipt
records both. Rollback keeps its behavior: it restores a sealed
generation and never carries the override.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "publish_runtime_roster_gate", ROOT / "tools" / "fleet" / "publish_runtime.py"
)
publish_runtime = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(publish_runtime)  # type: ignore[union-attr]

LIVE_ARGS = ["--class", "gb10", "--gpu", "--all-cores", "--gang-admission"]
CANDIDATE_ARGS = ["--class", "gb10", "--gpu", "--all-cores"]


def _roster(args: list[str]) -> dict:
    return {"boxes": {"sparky": {"loops": 5, "args": list(args)}}}


def _sealed_generation(store: Path, name: str, roster: dict) -> Path:
    """A prior generation with the roster the candidate is judged against."""

    root = store / name
    member = root / "tools" / "fleet" / "fleet_boxes.json"
    member.parent.mkdir(parents=True)
    payload = json.dumps(roster, sort_keys=True).encode()
    member.write_bytes(payload)
    mirror_copy = root / "tools" / "fleet_boxes.json"
    mirror_copy.parent.mkdir(parents=True, exist_ok=True)
    mirror_copy.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    (root / "RUNTIME_VERSION.json").write_text(json.dumps({
        "schema": "prismaquant.prismabuild.runtime_version.v1",
        "commit": "b" * 40,
        "rollout": "rolling",
        "rollout_reason": "fixture generation",
        "dirty": False,
        "shape_gate": {"verdict": "waived", "reason": "fixture generation"},
        "generation": name,
        "published_unix": 1.0,
        "published_by": "fixture",
        "files": {"tools/fleet/fleet_boxes.json": digest,
                  "tools/fleet_boxes.json": digest},
    }))
    return root


def _checkout(path: Path, roster: dict) -> Path:
    package = path / "src" / "prismabuild"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "core.py").write_text("GENERATION = 'new'\n")
    fleet = path / "tools" / "fleet"
    fleet.mkdir(parents=True, exist_ok=True)
    (fleet / "pbcanary.py").write_text(
        "def run_canary(*, generation):\n    return 0\n")
    (fleet / "fleet_boxes.json").write_text(json.dumps(roster, sort_keys=True))
    return path


def _publish(tmp_path: Path, monkeypatch, checkout: Path, extra: list[str]) -> Path:
    from types import SimpleNamespace

    mirror = tmp_path / "mirror"
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ("fleet_boxes.json",))

    def run(argv, **_kwargs):
        words = [str(part) for part in argv]
        if words and words[0] == "git":
            if "rev-parse" in words:
                return SimpleNamespace(returncode=0, stdout="a" * 40 + "\n",
                                       stderr="")
            if "status" in words:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if "ls-files" in words:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout="import ok\n", stderr="")

    monkeypatch.setattr(publish_runtime.subprocess, "run", run)
    monkeypatch.setattr(sys, "argv", [
        "publish_runtime.py", "--rollout", "rolling",
        "--rollout-reason", "fixture publication",
        "--shape-gate-waiver", "fixture publication", *extra])
    return mirror


def _live(tmp_path: Path, monkeypatch, roster: dict) -> tuple[Path, Path]:
    """A private mount whose live pointer names a sealed prior generation."""

    fleet = tmp_path / "fleet"
    store = fleet / "runtime-generations"
    prior = _sealed_generation(store, "bbbbbbbbbbbb-100-prior", roster)
    live = fleet / "repo"
    live.symlink_to(prior, target_is_directory=True)
    monkeypatch.setattr(publish_runtime, "MIRROR", live)
    return live, prior


def test_a_candidate_that_drops_a_live_option_is_refused(
        tmp_path, monkeypatch) -> None:
    live, prior = _live(tmp_path, monkeypatch, _roster(LIVE_ARGS))
    checkout = _checkout(tmp_path / "checkout", _roster(CANDIDATE_ARGS))
    mirror = _publish(tmp_path, monkeypatch, checkout, [])
    monkeypatch.setattr(publish_runtime, "MIRROR", live)

    with pytest.raises(SystemExit, match="--gang-admission"):
        publish_runtime.main()

    assert live.resolve() == prior, "the live pointer moved on a refusal"
    receipt = json.loads((prior / "RUNTIME_VERSION.json").read_text())
    assert receipt["generation"] == prior.name
    assert not list((live.parent / "runtime-generations").glob(".*.staging")), \
        "a refused publication left a staging tree behind"


def test_a_candidate_that_keeps_every_live_option_publishes(
        tmp_path, monkeypatch) -> None:
    live, prior = _live(tmp_path, monkeypatch, _roster(LIVE_ARGS))
    checkout = _checkout(tmp_path / "checkout", _roster(LIVE_ARGS))
    _publish(tmp_path, monkeypatch, checkout, [])
    monkeypatch.setattr(publish_runtime, "MIRROR", live)

    assert publish_runtime.main() == 0
    assert live.resolve() != prior


def test_a_changed_value_keeps_an_unchanged_option_name(
        tmp_path, monkeypatch) -> None:
    live, _prior = _live(tmp_path, monkeypatch, _roster(LIVE_ARGS))
    wider = [arg if arg != "--gang-admission" else arg for arg in LIVE_ARGS]
    assert "--gang-admission" in wider
    checkout = _checkout(tmp_path / "checkout", _roster(wider + ["--tag", "x"]))
    _publish(tmp_path, monkeypatch, checkout, [])
    monkeypatch.setattr(publish_runtime, "MIRROR", live)

    assert publish_runtime.main() == 0


def test_an_override_names_who_and_why_and_is_recorded(
        tmp_path, monkeypatch) -> None:
    live, prior = _live(tmp_path, monkeypatch, _roster(LIVE_ARGS))
    checkout = _checkout(tmp_path / "checkout", _roster(CANDIDATE_ARGS))
    _publish(tmp_path, monkeypatch, checkout, [])
    monkeypatch.setattr(publish_runtime, "MIRROR", live)
    monkeypatch.setattr(sys, "argv", [
        "publish_runtime.py", "--rollout", "rolling",
        "--rollout-reason", "fixture publication",
        "--shape-gate-waiver", "fixture publication",
        "--drop-roster-option-by", "rob",
        "--drop-roster-option-reason", "gangs retired from this fleet",
    ])

    assert publish_runtime.main() == 0
    assert live.resolve() != prior
    receipt = json.loads(
        (live.resolve() / "RUNTIME_VERSION.json").read_text())
    override = receipt["roster_option_override"]
    assert override["by"] == "rob"
    assert "gangs retired" in override["reason"]
    assert override["dropped"] == {"sparky": ["--gang-admission"]}


def test_a_half_named_override_is_a_usage_error(tmp_path, monkeypatch) -> None:
    _live(tmp_path, monkeypatch, _roster(LIVE_ARGS))
    checkout = _checkout(tmp_path / "checkout", _roster(CANDIDATE_ARGS))
    mirror = _publish(tmp_path, monkeypatch, checkout, [
        "--drop-roster-option-by", "rob"])
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)

    with pytest.raises(SystemExit) as caught:
        publish_runtime.main()
    assert caught.value.code == 2


def test_rollback_restores_a_prior_roster_without_an_override(
        tmp_path, monkeypatch) -> None:
    live, prior = _live(tmp_path, monkeypatch, _roster(LIVE_ARGS))
    store = live.parent / "runtime-generations"
    newer = _sealed_generation(store, "cccccccccccc-200-newer",
                               _roster(CANDIDATE_ARGS))
    live.unlink()
    live.symlink_to(newer, target_is_directory=True)

    assert publish_runtime._activate_existing(
        prior.name, dry_run=False, rollout="rolling",
        rollout_reason="fixture rollback restores the prior sealed roster",
    ) == 0
    assert live.resolve() == prior
