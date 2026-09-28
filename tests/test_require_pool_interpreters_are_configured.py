"""The interpreter the hook refuses is configuration, not a client name (#1076).

``require_pool.py`` refused one venv by a literal naming a client project.
It now reads ``gpu_interpreters`` from ``fleet_boxes.json``, the file the
supervisor starts the fleet from. These tests drive the derivation with a
different configuration, so a hook that still carried a hardcoded name would
refuse the old venv and pass the new one.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

import require_pool as hook  # noqa: E402

CONFIGURED = "/home/someone/envs/venvs/" + "gpu-env" + "/bin/python"
FORMER = "/home/rob/dq-runs/venvs/" + "prismaquant-cu130" + "/bin/python"


def test_the_refused_interpreter_is_the_one_configured() -> None:
    pattern = hook.contends_pattern({"gpu_interpreters": [CONFIGURED]})

    assert pattern.search(CONFIGURED + " train.py")
    assert pattern.search("~/envs/venvs/gpu-env/bin/python train.py")
    assert not pattern.search(FORMER + " train.py")


def test_no_configured_interpreter_still_refuses_the_lock_wrappers() -> None:
    pattern = hook.contends_pattern({})

    assert not pattern.search(FORMER + " train.py")
    assert pattern.search("/home/rob/bin/gpuslot.sh python train.py")
    assert pattern.search("flock /home/rob/.gpu.lock python train.py")


def test_the_published_configuration_declares_the_fleet_interpreters() -> None:
    document = json.loads((ROOT / "tools" / "fleet" / "fleet_boxes.json").read_text())
    declared = document[hook.INTERPRETERS_FIELD]

    assert declared and all(Path(path).is_absolute() for path in declared)
    assert hook.CONTENDS.pattern == hook.contends_pattern(document).pattern


def test_the_hook_names_no_client_venv_in_code() -> None:
    source = (ROOT / "tools" / "fleet" / "require_pool.py").read_text()

    assert "prismaquant-" + "cu130" not in source
