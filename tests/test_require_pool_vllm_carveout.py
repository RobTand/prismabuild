"""The GPU-container hook exempts vLLM and sees inside script files (#588).

The standing ruling exempts everything vLLM from admission -- serves,
censuses, routing runs, benchmarks against a live endpoint, its GPU
containers -- but ``require_pool.py`` refused any ``docker --gpus``
invocation, so exempt work routed around the hook through a script file the
lexical scan cannot see inside.  A hook that teaches the one way past it is
advisory for exactly the work it most wants to see.

This file runs DIRECTLY, never through PrismaBuild: it proves exempt work
stays out of the pool, and submitting that proof through the pool would beg
the question.  It is pure CPU -- no GPU, no containers, only command text
and small local scripts.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from test_require_pool import _armed, _verdict  # noqa: E402

CUDA_LINE = "/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python train.py"


def _sightings(tmp_path: Path) -> list[dict]:
    path = tmp_path / "require_pool_sightings.log"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()
            if line.strip()]


# -- the vLLM carve-out -------------------------------------------------------


@pytest.mark.parametrize("command", [
    "docker run --gpus all -p 8000:8000 vllm/vllm-openai:latest --model foo",
    "docker run --gpus all --ipc=host vllm/vllm-openai:v0.9 vllm serve foo",
    "podman run --gpus all img vllm serve --model foo --port 8000",
])
def test_a_vllm_container_passes(tmp_path, command) -> None:
    assert _verdict(_armed(tmp_path, None), command) == 0


@pytest.mark.parametrize("command", [
    "docker run --gpus all image train.py",
    "docker run --gpus all vllm/vllm-openai /opt/nccl-tests/build/all_reduce_perf",
    "docker run --gpus all vllm-image nccl-tests/all_reduce_perf -b 1G -e 8G",
    "docker run --gpus all vllm/vllm-openai pytest /tests",
])
def test_outside_the_carve_out_is_still_refused(tmp_path, command) -> None:
    """No vLLM mention, a bare collective borrowing its image, or a test
    runner inside the container: none of these is a serve, and the NCCL edge
    is decided explicitly rather than by whoever holds the keyboard."""

    assert _verdict(_armed(tmp_path, None), command) == 2


def test_the_carve_out_is_container_only(tmp_path) -> None:
    """The tessera#550 pytest refusal was correct and stays refused."""

    assert _verdict(_armed(tmp_path, None), "python3 -m pytest tests/") == 2


# -- the script-file blind spot -----------------------------------------------


def test_gpu_work_inside_a_script_file_is_refused(
    tmp_path, monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "ab.sh").write_text(
        "#!/bin/bash\n" + CUDA_LINE + "\n", encoding="utf-8")

    module = _armed(tmp_path, None)
    assert _verdict(module, "bash ab.sh") == 2


def test_a_gpu_container_inside_a_script_file_is_refused(
    tmp_path, monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "ab.sh").write_text(
        "#!/bin/bash\ndocker run --gpus all image train.py\n",
        encoding="utf-8")

    module = _armed(tmp_path, None)
    assert _verdict(module, "bash ab.sh") == 2


def test_a_clean_script_passes_and_is_recorded(
    tmp_path, monkeypatch,
) -> None:
    """Allowed, but no longer silent: the sighting names the script."""

    monkeypatch.chdir(tmp_path)
    (tmp_path / "ok.sh").write_text("#!/bin/bash\necho hi\n", encoding="utf-8")

    assert _verdict(_armed(tmp_path, None), "bash ok.sh") == 0
    sightings = _sightings(tmp_path)
    assert len(sightings) == 1
    assert sightings[0]["script"].endswith("ok.sh")


def test_an_unreadable_script_passes_and_is_recorded(
    tmp_path, monkeypatch,
) -> None:
    """What the hook cannot read it cannot judge; the route-around is then
    visible rather than silent."""

    monkeypatch.chdir(tmp_path)
    assert _verdict(_armed(tmp_path, None), "bash nosuch.sh") == 0
    sightings = _sightings(tmp_path)
    assert len(sightings) == 1
    assert sightings[0]["script"].endswith("nosuch.sh")
    assert sightings[0]["reason"] == "unreadable"


def test_only_one_level_is_read(tmp_path, monkeypatch) -> None:
    """A script that only calls another script is allowed and logged; the
    inner file's text is not the outer command's.  The log is what makes
    deeper nesting visible instead of silent."""

    monkeypatch.chdir(tmp_path)
    (tmp_path / "inner.sh").write_text(
        "#!/bin/bash\n" + CUDA_LINE + "\n", encoding="utf-8")
    (tmp_path / "outer.sh").write_text(
        "#!/bin/bash\nbash inner.sh\n", encoding="utf-8")

    assert _verdict(_armed(tmp_path, None), "bash outer.sh") == 0
    sightings = _sightings(tmp_path)
    assert len(sightings) == 1
    assert sightings[0]["script"].endswith("outer.sh")
