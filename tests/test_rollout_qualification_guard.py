"""The public rollout API cannot reach a mutating epoch step while guarded."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "rollout_guard_test", ROOT / "tools/fleet/publish_runtime.py")
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


def test_guarded_resume_cannot_reach_the_mutating_step(monkeypatch):
    """The public resume path refuses before `_barrier_step` can run."""
    monkeypatch.setattr(publisher, "_require_external_coordinator", lambda: None)
    monkeypatch.setattr(
        publisher, "_barrier_step",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("MUTATING STEP REACHED")),
    )
    with pytest.raises(SystemExit, match="qualification-guarded"):
        publisher._wait_barrier("a" * 32, wait_s=0)


def test_stage_only_reaches_the_nonmutating_publication_path(monkeypatch):
    """The guard does not prohibit sealing/probing a candidate generation."""
    monkeypatch.setattr(
        publisher, "_require_no_epoch",
        lambda: (_ for _ in ()).throw(AssertionError("STAGE PATH REACHED")),
    )
    args = SimpleNamespace(
        rollout="barrier", dry_run=False, stage_only=True,
        resume_barrier=None, rollback_barrier=None, rollout_reason=None,
        activate_generation=None, allow_dirty=False, migrate_directory=False,
        default_transport=None, barrier_wait_s=0,
    )
    with pytest.raises(AssertionError, match="STAGE PATH REACHED"):
        publisher._run_publication(args)
